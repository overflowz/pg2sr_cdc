//! Durable per-table WAL-event segment buffer.
//!
//! While the DDL poller holds a table's write pause (schema-change job in
//! flight on StarRocks), the CDC flush path routes that table's changes into
//! durable segments here instead of keeping them in memory. The same routing
//! covers per-table flushes that FAIL (Stream Load error, task panic, or
//! flush-timeout abort): the table's unconfirmed rows are buffered here, the
//! flush returns Ok, and the checkpoint advances — no reconnect, no WAL
//! re-stream. Once the DDL finishes (or SR is healthy again), buffered
//! segments are replayed in order through the normal flush path and deleted;
//! the table returns to live flushing.
//!
//! Durability contract: a segment is only acknowledged to the caller after
//! fsync (local) or a confirmed PUT (S3). The checkpoint/LSN ACK advances
//! past durably buffered events — this buffer is authoritative for them.
//! Crash recovery replays leftover segments on startup (`scan_existing`).
//! A segment is never deleted without a successful Stream Load apply
//! (Label-Already-Exists counts as success — SR is idempotent by PK).
//!
//! Backend selection mirrors the checkpoint store exactly (`store_type`:
//! "local" → files under `<checkpoint.path>.buffer/`, "s3" → objects under
//! the `<checkpoint.path>.buffer` key prefix in the same bucket). No new
//! configuration.
//!
//! Wire format (all little-endian, SHA-256 trailer):
//! - magic: 8 bytes `P2SBUF02`
//! - `u32` record count
//! - records: `kind: u8` (1=Upsert, 2=Delete, 3=Truncate) + `oid: u32`
//!   (relation OID; 0 = unknown/wildcard) + `len: u64` + payload (`len`
//!   bytes; row JSON verbatim; zero-length for Truncate)
//! - trailer: 32-byte SHA-256 over all preceding bytes (integrity — a
//!   flipped bit anywhere corrupts the segment, which is then quarantined)

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

use crate::config::CheckpointConfig;

/// Record kinds (wire format).
pub(crate) const BUF_UPSERT: u8 = 1;
pub(crate) const BUF_DELETE: u8 = 2;
pub(crate) const BUF_TRUNCATE: u8 = 3;

/// One buffered change: a row payload (Upsert/Delete) or a Truncate marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufRecord {
    pub kind: u8,
    /// Relation OID the record belongs to (0 = unknown/wildcard).
    pub oid: u32,
    pub payload: Vec<u8>,
}

/// Segment wire-format magic (v2: oid in records + SHA-256 trailer).
const SEGMENT_MAGIC: &[u8; 8] = b"P2SBUF02";

/// Segment file name: `seg_<20-digit zero-padded seq>.bin` (local) / object
/// key suffix (S3). 20 digits cover the full u64 range.
fn segment_file_name(seq: u64) -> String {
    format!("seg_{seq:020}.bin")
}

fn parse_segment_file_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("seg_")?.strip_suffix(".bin")?;
    if rest.len() != 20 || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

/// Minimum encoded record size: kind (1) + oid (4) + len (8).
const MIN_RECORD_BYTES: usize = 13;
/// Header size: magic (8) + count (4).
const HEADER_BYTES: usize = 8 + 4;
/// Trailer size: SHA-256 digest (32).
const TRAILER_BYTES: usize = 32;

fn encode_segment(records: &[BufRecord]) -> Result<Vec<u8>> {
    let count = u32::try_from(records.len())
        .map_err(|_| anyhow::anyhow!("wal buffer segment record count overflows u32"))?;
    let mut buf = Vec::with_capacity(
        HEADER_BYTES
            + TRAILER_BYTES
            + records
                .iter()
                .map(|r| MIN_RECORD_BYTES + r.payload.len())
                .sum::<usize>(),
    );
    buf.extend_from_slice(SEGMENT_MAGIC);
    buf.extend_from_slice(&count.to_le_bytes());
    for r in records {
        buf.push(r.kind);
        buf.extend_from_slice(&r.oid.to_le_bytes());
        let len = u64::try_from(r.payload.len())
            .map_err(|_| anyhow::anyhow!("wal buffer record payload length overflows u64"))?;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&r.payload);
    }
    buf.extend_from_slice(&Sha256::digest(&buf));
    Ok(buf)
}

pub(crate) fn decode_segment(bytes: &[u8]) -> Result<Vec<BufRecord>> {
    if bytes.len() < HEADER_BYTES + TRAILER_BYTES {
        bail!("segment shorter than header + checksum trailer");
    }
    if &bytes[..SEGMENT_MAGIC.len()] != SEGMENT_MAGIC {
        bail!("bad segment magic");
    }
    let trailer_start = bytes.len() - TRAILER_BYTES;
    let expected = Sha256::digest(&bytes[..trailer_start]);
    if expected.as_slice() != &bytes[trailer_start..] {
        bail!("segment checksum mismatch (data corrupt)");
    }
    let count = u32::from_le_bytes(
        bytes[SEGMENT_MAGIC.len()..SEGMENT_MAGIC.len() + 4]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    // Minimum record is 13 bytes (kind + oid + u64 len); bail before
    // allocating when the count is impossible for the segment size.
    if count > bytes.len().saturating_sub(HEADER_BYTES + TRAILER_BYTES) / MIN_RECORD_BYTES {
        bail!(
            "record count {count} impossible for a {}-byte segment",
            bytes.len()
        );
    }
    let mut out = Vec::with_capacity(count);
    let mut pos = HEADER_BYTES;
    for _ in 0..count {
        if bytes.len().saturating_sub(pos) < MIN_RECORD_BYTES {
            bail!("truncated record header at offset {pos}");
        }
        let kind = bytes[pos];
        pos += 1;
        if !(BUF_UPSERT..=BUF_TRUNCATE).contains(&kind) {
            bail!("unknown record kind {kind} at offset {}", pos - 1);
        }
        let oid = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes"));
        pos += 4;
        let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().expect("8 bytes")) as usize;
        pos += 8;
        match kind {
            BUF_TRUNCATE if len != 0 => bail!("truncate record must have zero-length payload"),
            BUF_UPSERT | BUF_DELETE if len == 0 => {
                bail!("row record must have non-empty payload")
            }
            _ => {}
        }
        let end = pos
            .checked_add(len)
            .filter(|&e| e <= trailer_start)
            .ok_or_else(|| anyhow::anyhow!("truncated record payload at offset {pos}"))?;
        out.push(BufRecord {
            kind,
            oid,
            payload: bytes[pos..end].to_vec(),
        });
        pos = end;
    }
    if pos != trailer_start {
        bail!(
            "trailing bytes after records ({} unread)",
            trailer_start - pos
        );
    }
    Ok(out)
}

// ── S3 backend (mirrors checkpoint/s3.rs; no new config) ──────────────────

#[derive(Clone)]
pub(crate) struct S3Backend {
    client: reqwest::Client,
    credentials: rusty_s3::Credentials,
    bucket_url: rusty_s3::Bucket,
    /// Object-key prefix for the whole buffer: `<checkpoint.path>.buffer`
    root_key: String,
}

/// Format a reqwest error WITHOUT the URL (presigned URLs embed a
/// re-usable `X-Amz-Signature` in the query string).
fn reqwest_err_no_url(e: reqwest::Error) -> String {
    format!("{}", e.without_url())
}

impl S3Backend {
    pub(crate) fn new(config: &CheckpointConfig) -> Result<Self> {
        let bucket_name = config.bucket.clone().unwrap_or_default();
        if bucket_name.is_empty() {
            anyhow::bail!("S3 wal buffer requires a non-empty 'bucket' in [checkpoint] config");
        }
        let endpoint = config
            .endpoint
            .clone()
            .unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
        let region = config
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string());
        let url_style = if config.path_style {
            rusty_s3::UrlStyle::Path
        } else {
            rusty_s3::UrlStyle::VirtualHost
        };
        let parsed_endpoint = url::Url::parse(&endpoint)
            .with_context(|| format!("Invalid S3 endpoint URL: {endpoint}"))?;
        let bucket_url = rusty_s3::Bucket::new(parsed_endpoint, url_style, bucket_name, region)
            .map_err(|e| anyhow::anyhow!("Failed to create S3 bucket config: {e}"))?;

        // Env-var credential fallback mirrors the checkpoint store.
        let access_key = config
            .access_key_id
            .clone()
            .or_else(|| {
                std::env::var("AWS_ACCESS_KEY_ID")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_default();
        let secret_key = config
            .secret_access_key
            .clone()
            .or_else(|| {
                std::env::var("AWS_SECRET_ACCESS_KEY")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_default();
        if access_key.is_empty() || secret_key.is_empty() {
            anyhow::bail!(
                "S3 wal buffer backend requires credentials: set [checkpoint] \
                 access_key_id + secret_access_key in config, OR export \
                 AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY."
            );
        }

        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .context("Failed to create HTTP client for wal buffer S3 backend")?,
            credentials: rusty_s3::Credentials::new(access_key, secret_key),
            bucket_url,
            root_key: format!("{}.buffer", config.path),
        })
    }

    /// 300s expiry — long enough to survive retries (same rationale as the
    /// checkpoint store's sign_url).
    fn sign_url<'a>(&self, action: impl rusty_s3::S3Action<'a>) -> url::Url {
        action.sign_with_time(
            std::time::Duration::from_secs(300),
            &time::OffsetDateTime::now_utc(),
        )
    }

    fn object_key(&self, table_key: &str, seq: u64) -> String {
        format!("{}/{}/{}", self.root_key, table_key, segment_file_name(seq))
    }

    fn table_prefix(&self, table_key: &str) -> String {
        format!("{}/{}/seg_", self.root_key, table_key)
    }

    pub(crate) async fn put_object(&self, object_key: &str, body: Vec<u8>) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = crate::util::jitter_duration(std::time::Duration::from_secs(
                    1u64 << (attempt - 1),
                ));
                tracing::warn!(
                    "wal buffer S3 PUT retry {}/{} ({}s backoff) for '{}'",
                    attempt,
                    MAX_RETRIES,
                    backoff.as_secs_f64(),
                    object_key,
                );
                tokio::time::sleep(backoff).await;
            }
            let action = self
                .bucket_url
                .put_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);
            let resp = match self.client.put(url.clone()).body(body.clone()).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = anyhow::anyhow!(
                        "wal buffer S3 PUT request error for '{}': {}",
                        object_key,
                        reqwest_err_no_url(e)
                    );
                    if attempt < MAX_RETRIES {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            if resp.status().is_success() {
                return Ok(());
            }
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            let err = anyhow::anyhow!(
                "wal buffer S3 PUT for '{}' returned HTTP {}: {}",
                object_key,
                status,
                text
            );
            if attempt < MAX_RETRIES && status.is_server_error() {
                last_error = Some(err);
                continue;
            }
            return Err(err);
        }
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "wal buffer S3 PUT for '{}' failed after {} retries",
                object_key,
                MAX_RETRIES
            )
        }))
    }

    pub(crate) async fn get_object(&self, object_key: &str) -> Result<Option<Vec<u8>>> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(crate::util::jitter_duration(
                    std::time::Duration::from_secs(1u64 << (attempt - 1)),
                ))
                .await;
            }
            let action = self
                .bucket_url
                .get_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);
            let resp = match self.client.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = anyhow::anyhow!(
                        "wal buffer S3 GET request error for '{}': {}",
                        object_key,
                        reqwest_err_no_url(e)
                    );
                    if attempt < MAX_RETRIES {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                let err = anyhow::anyhow!(
                    "wal buffer S3 GET for '{}' returned HTTP {}: {}",
                    object_key,
                    status,
                    text
                );
                if attempt < MAX_RETRIES && status.is_server_error() {
                    last_error = Some(err);
                    continue;
                }
                return Err(err);
            }
            let bytes = resp
                .bytes()
                .await
                .context("Failed to read wal buffer S3 response body")?;
            return Ok(Some(bytes.to_vec()));
        }
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "wal buffer S3 GET for '{}' failed after {} retries",
                object_key,
                MAX_RETRIES
            )
        }))
    }

    async fn delete_object(&self, object_key: &str) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(crate::util::jitter_duration(
                    std::time::Duration::from_secs(1u64 << (attempt - 1)),
                ))
                .await;
            }
            let action = self
                .bucket_url
                .delete_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);
            let resp = match self.client.delete(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = anyhow::anyhow!(
                        "wal buffer S3 DELETE request error for '{}': {}",
                        object_key,
                        reqwest_err_no_url(e)
                    );
                    if attempt < MAX_RETRIES {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            // 204 No Content or 404 = success (deleted or doesn't exist).
            if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(());
            }
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            let err = anyhow::anyhow!(
                "wal buffer S3 DELETE for '{}' returned HTTP {}: {}",
                object_key,
                status,
                text
            );
            if attempt < MAX_RETRIES && status.is_server_error() {
                last_error = Some(err);
                continue;
            }
            return Err(err);
        }
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "wal buffer S3 DELETE for '{}' failed after {} retries",
                object_key,
                MAX_RETRIES
            )
        }))
    }

    /// List object keys under `prefix`, following continuation tokens.
    async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action = self.bucket_url.list_objects_v2(Some(&self.credentials));
            action.with_prefix(prefix);
            if let Some(t) = &token {
                action.with_continuation_token(t.clone());
            }
            let url = self.sign_url(action);
            let resp = self.client.get(url).send().await.map_err(|e| {
                anyhow::anyhow!(
                    "wal buffer S3 LIST request error: {}",
                    reqwest_err_no_url(e)
                )
            })?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                bail!(
                    "wal buffer S3 LIST '{}' returned HTTP {status}: {text}",
                    prefix
                );
            }
            let text = resp
                .text()
                .await
                .context("Failed to read wal buffer S3 LIST response body")?;
            let parsed = rusty_s3::actions::ListObjectsV2::parse_response(&text).map_err(|e| {
                anyhow::anyhow!("wal buffer S3 LIST parse error for '{prefix}': {e}")
            })?;
            // rusty_s3 always requests `encoding-type=url`; providers that
            // honor it percent-encode Key values (aggressive encoders turn
            // '/' into %2F). Decode before parsing, or every key is dropped
            // by parse_object_key and the segment becomes invisible to
            // replay/scan (silent wedge — replay spins with zero progress).
            // Keys without percent-escapes decode to themselves (no-op), and
            // a compliant provider encodes a literal '%' as %25, so
            // round-tripping is safe.
            for c in parsed.contents {
                out.push(
                    percent_encoding::percent_decode_str(&c.key)
                        .decode_utf8_lossy()
                        .into_owned(),
                );
            }
            match parsed.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(out)
    }
}

/// Parse a full object key back into `(table_key, seq)`, or `None` if the
/// key isn't a segment under `root_key`. Table keys cannot contain '/'
/// (PG identifiers never contain it), so splitting on "/seg_" is safe.
fn parse_object_key(root_key: &str, key: &str) -> Option<(String, u64)> {
    // Quarantined copies live under `<root>/quarantine/` and must never be
    // picked up by S3 scans (they are not active segments).
    if key.contains("/quarantine/") {
        return None;
    }
    let rest = key.strip_prefix(root_key)?.strip_prefix('/')?;
    let idx = rest.rfind("/seg_")?;
    let table = &rest[..idx];
    let seq = parse_segment_file_name(&rest[idx + 1..])?;
    Some((table.to_string(), seq))
}

// ── Manager ───────────────────────────────────────────────────────────────

enum Backend {
    Local { root: PathBuf },
    S3(Box<S3Backend>),
}

struct State {
    /// Table keys ("schema.table") that currently have ≥1 segment on disk /
    /// in the bucket. Hot-path routing check (`has_segments`) — NO S3 LIST.
    segmented: HashSet<String>,
    /// Table keys with an active replay task (prevents two concurrent
    /// replayers for the same table).
    replay_in_flight: HashSet<String>,
    /// Next segment sequence per table. Populated at startup by
    /// `scan_existing` (max existing + 1) so new appends never collide with
    /// leftover segments; incremented on every append.
    next_seq: HashMap<String, u64>,
    /// In-memory segment count per table — lets `end_replay` / `delete_segment`
    /// drop a table from `segmented` without S3 LIST in the hot path.
    seg_count: HashMap<String, u64>,
    /// Encoded segment file size per (table, seq) — the byte-gauge source.
    seg_size: HashMap<String, HashMap<u64, u64>>,
    /// Total buffered bytes per table (sum of `seg_size`; no S3 LIST needed).
    table_bytes: HashMap<String, u64>,
    /// Tables whose rename sidecar (`<table_key>.renames`) is non-empty on
    /// disk. Append consults this to decide whether an empty rename set must
    /// delete a stale sidecar; end_replay/scan use it for hygiene.
    renames_persisted: HashSet<String>,
}

fn lock_state(mx: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    match mx.lock() {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("WalBufferManager state mutex poisoned — recovering guard");
            e.into_inner()
        }
    }
}

/// Durable per-table WAL-event segment buffer. All segment file/object I/O
/// lives here; the pipeline only routes records in and replays segments out.
pub(crate) struct WalBufferManager {
    backend: Backend,
    state: Mutex<State>,
    /// Number of segments auto-quarantined (corrupt/unreadable) — monotonic
    /// counter, never reset, for the metrics server.
    quarantined: Arc<AtomicU64>,
    /// Warn threshold for total buffered bytes (pipeline max_pending_bytes).
    /// 0 = disabled (test/local-root constructors).
    warn_threshold_bytes: u64,
    /// Hard cap on total buffered bytes across ALL tables (0 = uncapped).
    /// Enforced fail-closed in `append`: exceeding it returns Err, so the
    /// checkpoint does not advance and PG re-sends the WAL once the operator
    /// frees space — a permanently failing table can never fill the disk.
    /// `AtomicU64` so startup can set it after construction (keeps the
    /// constructors' signatures stable for tests).
    max_bytes: AtomicU64,
    /// One-shot latch: a single warning when buffered bytes exceed the
    /// threshold, re-armed when bytes drop back below it.
    growth_warned: AtomicBool,
}

impl WalBufferManager {
    /// Create a manager mirroring the checkpoint store backend. Local:
    /// creates the buffer root directory (fail fast). S3: no I/O at
    /// construction (bucket verified by the checkpoint store's own verify).
    pub(crate) fn new(
        cp_cfg: Option<&CheckpointConfig>,
        warn_threshold_bytes: u64,
    ) -> Result<Arc<Self>> {
        let cfg = cp_cfg.cloned().unwrap_or_default();
        let backend = match cfg.store_type.as_str() {
            "local" => {
                let root = PathBuf::from(format!("{}.buffer", cfg.path));
                std::fs::create_dir_all(&root).with_context(|| {
                    format!("Failed to create wal buffer root {}", root.display())
                })?;
                tracing::info!("wal buffer: local backend root={}", root.display(),);
                Backend::Local { root }
            }
            "s3" => {
                let s3 = S3Backend::new(&cfg)?;
                tracing::info!(
                    "wal buffer: S3 backend root_key={} bucket={}",
                    s3.root_key,
                    s3.bucket_url.name(),
                );
                Backend::S3(Box::new(s3))
            }
            other => bail!(
                "unknown checkpoint store_type '{other}' for wal buffer backend \
                 (supported: local, s3)"
            ),
        };
        Ok(Arc::new(Self {
            backend,
            state: Mutex::new(State {
                segmented: HashSet::new(),
                replay_in_flight: HashSet::new(),
                next_seq: HashMap::new(),
                seg_count: HashMap::new(),
                seg_size: HashMap::new(),
                table_bytes: HashMap::new(),
                renames_persisted: HashSet::new(),
            }),
            quarantined: Arc::new(AtomicU64::new(0)),
            warn_threshold_bytes,
            max_bytes: AtomicU64::new(0),
            growth_warned: AtomicBool::new(false),
        }))
    }

    /// Set the hard byte cap for total buffered segments (0 = uncapped).
    /// Called once at startup from the pipeline config.
    pub(crate) fn set_max_bytes(&self, max_bytes: u64) {
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
    }

    /// Test-only constructor: local backend at an explicit root (tempdir).
    #[cfg(test)]
    pub(crate) fn new_local_root(root: PathBuf) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("Failed to create wal buffer root {}", root.display()))?;
        Ok(Arc::new(Self {
            backend: Backend::Local { root },
            state: Mutex::new(State {
                segmented: HashSet::new(),
                replay_in_flight: HashSet::new(),
                next_seq: HashMap::new(),
                seg_count: HashMap::new(),
                seg_size: HashMap::new(),
                table_bytes: HashMap::new(),
                renames_persisted: HashSet::new(),
            }),
            quarantined: Arc::new(AtomicU64::new(0)),
            warn_threshold_bytes: 0,
            max_bytes: AtomicU64::new(0),
            growth_warned: AtomicBool::new(false),
        }))
    }

    /// Append a batch of records as a new segment for `table_key`. The
    /// segment is fully written + fsynced (local) or PUT-confirmed (S3)
    /// before this returns Ok — the caller may then advance the checkpoint
    /// past these events. Err → flush fails, checkpoint does NOT advance,
    /// events re-deliver from WAL (no loss).
    ///
    /// `renames` is the table's current old→new column-rename map; it is
    /// persisted as a sidecar when non-empty (or when a sidecar already
    /// exists — an empty map then deletes it). The sidecar survives a crash
    /// between DDL registration (checkpoint advanced) and replay drain, so
    /// restart replays remap correctly instead of loading an empty map.
    /// Sidecar write failure propagates as append Err — the segment stays
    /// accounted; WAL re-delivery re-appends and replay drains both.
    pub(crate) async fn append(
        &self,
        table_key: &str,
        records: &[BufRecord],
        renames: &HashMap<String, String>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Reserve the sequence and mark the table as segmented BEFORE the
        // I/O so a concurrently-finishing replay can never observe a count
        // that under-counts a segment already visible on disk.
        let seq = {
            let mut st = lock_state(&self.state);
            let seq = *st.next_seq.entry(table_key.to_string()).or_insert(0);
            st.next_seq.insert(table_key.to_string(), seq + 1);
            st.segmented.insert(table_key.to_string());
            *st.seg_count.entry(table_key.to_string()).or_insert(0) += 1;
            seq
        };
        // Encoded length of the attempted segment, recorded by the publish
        // step below through this explicit `&mut` slot (the `async move` block
        // owns that borrow): `Some(len)` = the segment may exist in the
        // backend even though `append` reports Err, so the reservation and
        // `len` bytes stay accounted. The failure arms clear it back to None
        // wherever they PROVE nothing was published. The length is the
        // deterministic encoded one — never a stat of the entry, which can
        // fail (or report a size that is not the segment's bytes) for an entry
        // that exists, and would re-create the unaccounted leak the arms below
        // exist to remove.
        let mut orphan_len: Option<u64> = None;
        let orphan_slot = &mut orphan_len;
        // ALL fallible work after the reservation lives in this closure,
        // which returns the encoded byte count only on success. Err arms
        // propagate to the rollback below.
        let result: Result<u64> = (|| async move {
            let bytes = encode_segment(records)?;
            let bytes_len = bytes.len();
            // Fail-closed byte cap: a permanently failing table — or a replay
            // that cannot list what it wrote — must not be able to fill the
            // disk. Err propagates to the caller, so the checkpoint does NOT
            // advance and PG re-sends these events once the operator frees
            // space. No row is dropped.
            let max_bytes = self.max_bytes.load(Ordering::Relaxed);
            if max_bytes > 0 {
                let current: u64 = lock_state(&self.state).table_bytes.values().sum();
                if current.saturating_add(bytes_len as u64) > max_bytes {
                    anyhow::bail!(
                        "wal buffer cap exceeded for {table_key}: {current} buffered + \
                         {bytes_len} new > {max_bytes} bytes \
                         (pipeline.wal_buffer_max_mb={}). Replay cannot drain (SR down or \
                         listing wedged) — free SR capacity or raise the cap. The checkpoint \
                         will not advance until the append succeeds; no rows are dropped.",
                        max_bytes / 1024 / 1024,
                    );
                }
            }
            // Only from here on can the segment land in the backend: every
            // failure BEFORE this point (encode, byte cap) published nothing
            // and must roll the reservation back, so the failure arms below
            // must not see a length here.
            *orphan_slot = Some(bytes_len as u64);
            match &self.backend {
                Backend::Local { root } => {
                    let dir = root.join(table_key);
                    let path = dir.join(segment_file_name(seq));
                    let tmp_path = dir.join(format!("{}.tmp", segment_file_name(seq)));
                    let result = (|| -> anyhow::Result<u64> {
                        let dir_existed = dir.exists();
                        std::fs::create_dir_all(&dir)
                            .with_context(|| format!("wal buffer mkdir {}", dir.display()))?;
                        // Durable table-dir creation: fsync the ROOT dir only
                        // when this call actually created `<table>/` (the
                        // entry survives a crash); repeated appends to an
                        // existing dir skip the fsync.
                        if !dir_existed {
                            std::fs::File::open(root)
                                .and_then(|f| f.sync_all())
                                .with_context(|| {
                                    format!("wal buffer fsync root {}", root.display())
                                })?;
                        }
                        std::fs::write(&tmp_path, &bytes)
                            .with_context(|| format!("wal buffer write {}", tmp_path.display()))?;
                        let f = std::fs::File::open(&tmp_path)
                            .with_context(|| format!("wal buffer open {}", tmp_path.display()))?;
                        f.sync_data()
                            .with_context(|| format!("wal buffer fsync {}", tmp_path.display()))?;
                        drop(f);
                        // Atomic publish: a crash mid-write leaves only the .tmp
                        // name, which list_segments ignores (doesn't match
                        // `seg_<20 digits>.bin`).
                        std::fs::rename(&tmp_path, &path).with_context(|| {
                            format!(
                                "wal buffer rename {} → {}",
                                tmp_path.display(),
                                path.display()
                            )
                        })?;
                        // Durable rename: fsync the TABLE dir so the segment
                        // entry survives a crash.
                        std::fs::File::open(&dir)
                            .and_then(|f| f.sync_all())
                            .with_context(|| format!("wal buffer fsync dir {}", dir.display()))?;
                        Ok(bytes_len as u64)
                    })();
                    if result.is_err() {
                        let _ = std::fs::remove_file(&tmp_path); // best-effort tmp cleanup
                    }
                    result
                }
                Backend::S3(s3) => {
                    let key = s3.object_key(table_key, seq);
                    match s3.put_object(&key, bytes).await {
                        Ok(()) => Ok(bytes_len as u64),
                        // PUT errored — verify whether it actually committed
                        // (the failure may have been after the object was
                        // stored). A committed-but-error PUT must NOT roll
                        // back: the checkpoint may advance past an orphaned
                        // segment that would replay stale rows after restart
                        // A confirmed-absent object is a clean
                        // failure → roll back. A failed VERIFY is ambiguous:
                        // the object may exist, so it is deleted best-effort
                        // and the segment stays REGISTERED either way (via
                        // `orphan_len`, set above). Rolling the accounting back
                        // here would hide an object that survived the cleanup
                        // from `segmented` / the byte cap until the next
                        // startup scan — the local arm keeps exactly the same
                        // kind of entry for exactly the same reason: accounting
                        // must match what the backend can list.
                        Err(e) => match s3.get_object(&key).await {
                            Ok(Some(_)) => Ok(bytes_len as u64),
                            Ok(None) => {
                                // Confirmed absent: nothing survives, so the
                                // clean-failure rollback below applies.
                                *orphan_slot = None;
                                Err(e)
                            }
                            Err(e2) => {
                                // Log the DELETE outcome explicitly — a
                                // discarded result used to make the log claim
                                // the object was "deleted if present" even when
                                // it survived.
                                if let Err(de) = s3.delete_object(&key).await {
                                    tracing::error!(
                                        "wal buffer: S3 PUT failed ({e}) and verify failed ({e2}) \
                                         for {key}; cleanup DELETE also failed ({de}) — the object \
                                         may still exist; keeping the segment accounted"
                                    );
                                } else {
                                    tracing::warn!(
                                        "wal buffer: S3 PUT failed ({e}) and verify failed ({e2}) \
                                         for {key}; cleanup DELETE succeeded but the object's \
                                         absence is unconfirmed — keeping the segment accounted"
                                    );
                                }
                                Err(anyhow::anyhow!(
                                    "wal buffer S3 PUT failed: {e}; verify failed: {e2} \
                                     (object may exist; segment kept accounted)"
                                ))
                            }
                        },
                    }
                }
            }
        })()
        .await;
        let bytes_len = match result {
            Err(e) => {
                // Local backend: the rename may have committed while the
                // final dir fsync failed — try to remove the published segment
                // so no orphan can replay stale rows after restart.
                //
                // If that removal ALSO fails the file is visible to
                // `list_segments` but invisible to in-memory accounting (the
                // seg_count rollback below used to drop it on the floor): an
                // unbounded, unaccounted disk leak that only `scan_existing` at
                // the next start would rediscover. Keep it registered instead —
                // accounting must match what the backend can list. `append`
                // still returns Err, so the checkpoint does not advance and the
                // re-delivered rows are appended again (idempotent by PK on
                // replay).
                if let Backend::Local { root } = &self.backend {
                    let path = root.join(table_key).join(segment_file_name(seq));
                    // The removal RESULT decides — not a stat of the path: the
                    // entry is gone when it was never published, was removed,
                    // or was already missing (NotFound), and that is a clean
                    // failure which rolls back below. Any other error means it
                    // survived, and the length kept for it is the encoded one
                    // recorded above; a metadata probe can fail for an entry
                    // that exists (dropping the accounting and re-creating the
                    // invisible leak) or report a size that is not the
                    // segment's bytes.
                    if !path.exists() {
                        orphan_len = None;
                    } else {
                        match std::fs::remove_file(&path) {
                            Ok(()) => orphan_len = None,
                            Err(e2) if e2.kind() == std::io::ErrorKind::NotFound => {
                                orphan_len = None
                            }
                            Err(e2) => {
                                tracing::error!(
                                    "wal buffer: removal of published segment {} failed ({e2}) — \
                                     keeping it REGISTERED (len={:?}) so it is drainable and the \
                                     disk usage is accounted; append still returns Err",
                                    path.display(),
                                    orphan_len,
                                );
                            }
                        }
                    }
                }
                if let Some(len) = orphan_len {
                    let mut st = lock_state(&self.state);
                    st.seg_size
                        .entry(table_key.to_string())
                        .or_default()
                        .insert(seq, len);
                    *st.table_bytes.entry(table_key.to_string()).or_default() += len;
                    return Err(e);
                }
                // Roll back the in-memory accounting so a failed append can't
                // leave the table stuck in `segmented` with no segment behind
                // it. seg_size/table_bytes are written ONLY on the Ok path,
                // so subtracting never-added bytes here would corrupt the
                // gauges — rollback is just the count + set.
                let mut st = lock_state(&self.state);
                if let Some(c) = st.seg_count.get_mut(table_key) {
                    *c = c.saturating_sub(1);
                    if *c == 0 {
                        st.segmented.remove(table_key);
                    }
                }
                return Err(e);
            }
            Ok(n) => n,
        };
        {
            let mut st = lock_state(&self.state);
            st.seg_size
                .entry(table_key.to_string())
                .or_default()
                .insert(seq, bytes_len as u64);
            *st.table_bytes.entry(table_key.to_string()).or_default() += bytes_len as u64;
            // Buffer-growth alert: one warning when
            // total buffered bytes exceed the pipeline max_pending_bytes
            // (replay wedged or a long DDL), re-armed when bytes drop below.
            let total_all = st.table_bytes.values().sum::<u64>();
            if self.warn_threshold_bytes > 0 {
                if total_all > self.warn_threshold_bytes
                    && !self.growth_warned.swap(true, Ordering::Relaxed)
                {
                    tracing::warn!(
                        "wal buffer bytes ({total_all}) exceed pipeline max_pending_bytes ({}) — \
                         replay wedged or long DDL; check SR",
                        self.warn_threshold_bytes,
                    );
                } else if total_all < self.warn_threshold_bytes {
                    self.growth_warned.store(false, Ordering::Relaxed);
                }
            }
        }
        // Rename sidecar: keep it in sync with the map as of this append
        // (non-empty → write; empty but persisted → delete).
        if !renames.is_empty()
            || lock_state(&self.state)
                .renames_persisted
                .contains(table_key)
        {
            self.write_renames_sidecar(table_key, renames).await?;
        }
        tracing::info!(
            table = table_key,
            ops = records.len(),
            bytes = bytes_len,
            seq,
            "wal buffer: appended segment",
        );
        Ok(())
    }

    /// Persist the table's rename map as a sidecar (`<table_key>.renames`
    /// next to the segments) so a crash after the DDL poller registered a
    /// rename (checkpoint advanced) but before replay drained cannot lose
    /// the mapping. Empty map → delete any existing sidecar (idempotent).
    ///
    /// Local: tmp file → `sync_data` → rename. No dir fsync — a lost sidecar
    /// degrades to the pre-fix window (no remap on restart), warn-level only.
    /// S3: plain PUT (delete = DELETE, 404 counts as success).
    pub(crate) async fn write_renames_sidecar(
        &self,
        table_key: &str,
        renames: &HashMap<String, String>,
    ) -> Result<()> {
        if renames.is_empty() {
            match &self.backend {
                Backend::Local { root } => {
                    let p = root.join(format!("{table_key}.renames"));
                    match std::fs::remove_file(&p) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(e).with_context(|| {
                                format!("wal buffer sidecar delete {}", p.display())
                            })
                        }
                    }
                }
                Backend::S3(s3) => {
                    s3.delete_object(&format!("{}/{table_key}.renames", s3.root_key))
                        .await?;
                }
            }
            lock_state(&self.state).renames_persisted.remove(table_key);
            return Ok(());
        }
        let bytes =
            serde_json::to_vec(renames).context("wal buffer rename sidecar serialization")?;
        match &self.backend {
            Backend::Local { root } => {
                let tmp = root.join(format!("{table_key}.renames.tmp"));
                let fin = root.join(format!("{table_key}.renames"));
                std::fs::write(&tmp, &bytes)
                    .with_context(|| format!("wal buffer sidecar write {}", tmp.display()))?;
                let f = std::fs::File::open(&tmp)
                    .with_context(|| format!("wal buffer sidecar open {}", tmp.display()))?;
                f.sync_data()
                    .with_context(|| format!("wal buffer sidecar fsync {}", tmp.display()))?;
                drop(f);
                // No dir fsync (see doc): a lost sidecar degrades to the
                // pre-fix window, warn-level only.
                std::fs::rename(&tmp, &fin).with_context(|| {
                    format!(
                        "wal buffer sidecar rename {} → {}",
                        tmp.display(),
                        fin.display()
                    )
                })?;
            }
            Backend::S3(s3) => {
                s3.put_object(&format!("{}/{table_key}.renames", s3.root_key), bytes)
                    .await?;
            }
        }
        lock_state(&self.state)
            .renames_persisted
            .insert(table_key.to_string());
        Ok(())
    }

    /// Read the table's persisted rename map. Missing sidecar → `Ok(None)`;
    /// corrupt JSON → `Ok(None)` + warn (degrades to no-remap; never fatal).
    pub(crate) async fn read_renames_sidecar(
        &self,
        table_key: &str,
    ) -> Result<Option<HashMap<String, String>>> {
        let bytes = match &self.backend {
            Backend::Local { root } => {
                let p = root.join(format!("{table_key}.renames"));
                match std::fs::read(&p) {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("wal buffer sidecar read {}", p.display()))
                    }
                }
            }
            Backend::S3(s3) => {
                match s3
                    .get_object(&format!("{}/{table_key}.renames", s3.root_key))
                    .await?
                {
                    Some(b) => b,
                    None => return Ok(None),
                }
            }
        };
        match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
            Ok(m) => Ok(Some(m)),
            Err(e) => {
                tracing::warn!(
                    "wal buffer rename sidecar for {table_key} corrupt ({e}) — ignoring (no remap)"
                );
                Ok(None)
            }
        }
    }

    /// Hot-path routing check: does `table_key` have ≥1 segment? In-memory
    /// only — never touches the S3 backend in the flush path.
    pub(crate) fn has_segments(&self, table_key: &str) -> bool {
        lock_state(&self.state).segmented.contains(table_key)
    }

    /// List existing segment sequences for `table_key`, ascending.
    pub(crate) async fn list_segments(&self, table_key: &str) -> Result<Vec<u64>> {
        let mut seqs: Vec<u64> = match &self.backend {
            Backend::Local { root } => {
                let dir = root.join(table_key);
                let entries = match std::fs::read_dir(&dir) {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(e) => {
                        return Err(e).with_context(|| format!("wal buffer list {}", dir.display()))
                    }
                };
                let mut v = Vec::new();
                for entry in entries {
                    let entry =
                        entry.with_context(|| format!("wal buffer read_dir {}", dir.display()))?;
                    if let Some(seq) = parse_segment_file_name(&entry.file_name().to_string_lossy())
                    {
                        v.push(seq);
                    }
                }
                v
            }
            Backend::S3(s3) => {
                let keys = s3
                    .list_keys_with_prefix(&s3.table_prefix(table_key))
                    .await?;
                let mut v = Vec::new();
                for k in keys {
                    if let Some((t, seq)) = parse_object_key(&s3.root_key, &k) {
                        if t == table_key {
                            v.push(seq);
                        }
                    }
                }
                v
            }
        };
        seqs.sort_unstable();
        Ok(seqs)
    }

    /// Read one segment's raw bytes without decoding. Missing → `Ok(None)`
    /// (local NotFound / S3 404 both map to None). Used by scan/replay so
    /// corrupt bytes can be quarantined instead of treated as fatal.
    pub(crate) async fn read_segment_bytes(
        &self,
        table_key: &str,
        seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        match &self.backend {
            Backend::Local { root } => {
                let path = root.join(table_key).join(segment_file_name(seq));
                match std::fs::read(&path) {
                    Ok(b) => Ok(Some(b)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("wal buffer read {}", path.display())),
                }
            }
            Backend::S3(s3) => s3.get_object(&s3.object_key(table_key, seq)).await,
        }
    }

    /// Read and decode one segment. Missing → Err; corrupt → Err (never
    /// skip silently — buffered data is ACKed-past; the caller decides
    /// whether to quarantine or retry).
    /// Used by the wal_buffer unit tests (roundtrip / corruption paths);
    /// production callers use `read_segment_bytes` + `decode_segment` so
    /// corrupt bytes can be quarantined instead of erroring.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn read_segment(&self, table_key: &str, seq: u64) -> Result<Vec<BufRecord>> {
        let bytes = self
            .read_segment_bytes(table_key, seq)
            .await?
            .ok_or_else(|| anyhow::anyhow!("wal buffer segment {table_key}/{seq} not found"))?;
        decode_segment(&bytes)
            .with_context(|| format!("wal buffer segment {table_key}/seg_{seq:020}.bin corrupt"))
    }

    /// Delete one segment (idempotent — replay deletes after apply; a
    /// missing segment is already gone and that is fine).
    pub(crate) async fn delete_segment(&self, table_key: &str, seq: u64) -> Result<()> {
        match &self.backend {
            Backend::Local { root } => {
                let path = root.join(table_key).join(segment_file_name(seq));
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("wal buffer delete {}", path.display()))
                    }
                }
            }
            Backend::S3(s3) => s3.delete_object(&s3.object_key(table_key, seq)).await?,
        }
        let mut st = lock_state(&self.state);
        if let Some(c) = st.seg_count.get_mut(table_key) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                st.segmented.remove(table_key);
            }
        }
        // Drop the byte accounting for the deleted segment (idempotent: a
        // second delete of an already-gone segment finds no size entry).
        if let Some(sizes) = st.seg_size.get_mut(table_key) {
            if let Some(n) = sizes.remove(&seq) {
                if let Some(b) = st.table_bytes.get_mut(table_key) {
                    *b = b.saturating_sub(n);
                    if *b == 0 {
                        st.table_bytes.remove(table_key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Delete every buffered segment (--fresh teardown). The fresh snapshot
    /// re-copies PG's current state, which already contains the buffered
    /// events' effects — leftover segments would resurrect pre-fresh rows.
    pub(crate) async fn purge_all(&self) -> Result<()> {
        match &self.backend {
            Backend::Local { root } => {
                if root.exists() {
                    std::fs::remove_dir_all(root)
                        .with_context(|| format!("wal buffer purge {}", root.display()))?;
                }
                std::fs::create_dir_all(root)
                    .with_context(|| format!("wal buffer recreate {}", root.display()))?;
            }
            Backend::S3(s3) => {
                let keys = s3.list_keys_with_prefix(&s3.root_key).await?;
                for k in keys {
                    s3.delete_object(&k).await?;
                }
            }
        }
        let mut st = lock_state(&self.state);
        st.segmented.clear();
        st.next_seq.clear();
        st.seg_count.clear();
        st.seg_size.clear();
        st.table_bytes.clear();
        st.renames_persisted.clear();
        tracing::info!("wal buffer: purged all segments");
        Ok(())
    }

    /// Claim the replay slot for `table_key`. Returns false if a replay task
    /// for it is already in flight (no second concurrent replayer).
    pub(crate) fn try_begin_replay(&self, table_key: &str) -> bool {
        let mut st = lock_state(&self.state);
        if st.replay_in_flight.contains(table_key) {
            return false;
        }
        st.replay_in_flight.insert(table_key.to_string());
        true
    }

    /// Release the replay slot. Also drops the table from `segmented` when
    /// no segments remain, so the routing condition (`paused || buffered`)
    /// un-buffers the table and it returns to live flushing.
    ///
    /// When the table's last segment is gone, the rename sidecar is deleted
    /// best-effort (a stale sidecar is never loaded at scan — scan only reads
    /// sidecars for tables with segments — so failure only leaks a byte; the
    /// next append rewrites it anyway).
    pub(crate) fn end_replay(&self, table_key: &str) {
        let mut st = lock_state(&self.state);
        st.replay_in_flight.remove(table_key);
        if st.seg_count.get(table_key).copied().unwrap_or(0) == 0 {
            st.segmented.remove(table_key);
            if st.renames_persisted.remove(table_key) {
                match &self.backend {
                    Backend::Local { root } => {
                        let p = root.join(format!("{table_key}.renames"));
                        match std::fs::remove_file(&p) {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => tracing::warn!(
                                "wal buffer: best-effort sidecar delete failed for {table_key}: {e}"
                            ),
                        }
                    }
                    Backend::S3(s3) => {
                        let s3 = s3.clone();
                        let key = format!("{}/{table_key}.renames", s3.root_key);
                        if let Ok(_h) = tokio::runtime::Handle::try_current() {
                            tokio::spawn(async move {
                                if let Err(e) = s3.delete_object(&key).await {
                                    tracing::warn!(
                                        "wal buffer: best-effort S3 sidecar delete failed for {key}: {e:?}"
                                    );
                                }
                            });
                        } else {
                            tracing::warn!(
                                "wal buffer: no runtime for best-effort S3 sidecar delete of {key} — \
                                 stale sidecar skipped at scan (only tables with segments are read)"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Is a replay task currently in flight for `table_key`? Used by the
    /// pipeline's pending_cdc_tables removal to keep a draining table visible
    /// to the DDL poller's wait_for_cdc_drain.
    pub(crate) fn is_replaying(&self, table_key: &str) -> bool {
        lock_state(&self.state).replay_in_flight.contains(table_key)
    }

    /// Tables that currently have ≥1 segment (in-memory set only — used by
    /// the replay kick to decide what to replay without S3 LIST).
    pub(crate) fn segmented_tables(&self) -> Vec<String> {
        lock_state(&self.state).segmented.iter().cloned().collect()
    }

    /// In-memory segment count for `table_key` (0 if unregistered). Lets the
    /// replay path detect an accounting-vs-storage divergence (LIST empty
    /// while segments are counted) instead of exiting silently.
    pub(crate) fn segment_count(&self, table_key: &str) -> u64 {
        lock_state(&self.state)
            .seg_count
            .get(table_key)
            .copied()
            .unwrap_or(0)
    }

    /// Total number of buffered segments across all tables (gauge source).
    pub(crate) fn total_segments(&self) -> u64 {
        lock_state(&self.state).seg_count.values().sum()
    }

    /// Total encoded bytes across all buffered segments (gauge source —
    /// no buffer-size cap by design; external tooling watches this).
    pub(crate) fn total_buffered_bytes(&self) -> u64 {
        lock_state(&self.state).table_bytes.values().sum()
    }

    /// Monotonic count of auto-quarantined (corrupt) segments.
    pub(crate) fn quarantined_count(&self) -> Arc<AtomicU64> {
        self.quarantined.clone()
    }

    /// Move a corrupt/unreadable segment aside, PRESERVING the bytes, and
    /// drop it from active rotation (accounting decremented via
    /// `delete_segment`). Local: rename into `<root>/quarantine/<table>/`
    /// (same filesystem, atomic). S3: PUT a copy under `quarantine/` then
    /// DELETE the original. Errors propagate — quarantine failure is fatal
    /// for the caller (startup scan / replay).
    pub(crate) async fn quarantine_segment(
        &self,
        table_key: &str,
        seq: u64,
        bytes: &[u8],
    ) -> Result<()> {
        match &self.backend {
            Backend::Local { root } => {
                let qdir = root.join("quarantine").join(table_key);
                std::fs::create_dir_all(&qdir)
                    .with_context(|| format!("wal buffer quarantine mkdir {}", qdir.display()))?;
                let src = root.join(table_key).join(segment_file_name(seq));
                let epoch = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let dst = qdir.join(format!("seg_{seq:020}.bin.{epoch}"));
                std::fs::rename(&src, &dst).with_context(|| {
                    format!(
                        "wal buffer quarantine rename {} → {}",
                        src.display(),
                        dst.display()
                    )
                })?;
            }
            Backend::S3(s3) => {
                let qkey = format!("{}/quarantine/{}/seg_{seq:020}.bin", s3.root_key, table_key);
                s3.put_object(&qkey, bytes.to_vec()).await?;
                s3.delete_object(&s3.object_key(table_key, seq)).await?;
            }
        }
        // Decrements seg_count / seg_size / table_bytes (file already moved —
        // delete of the original is a no-op).
        self.delete_segment(table_key, seq).await?;
        self.quarantined.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            table = table_key,
            seq,
            "wal buffer: segment quarantined (corrupt/unreadable) — bytes preserved aside"
        );
        Ok(())
    }

    /// Startup recovery: enumerate tables that have segments, populating
    /// `segmented` / `next_seq` / `seg_count` / `seg_size` / `table_bytes`
    /// so (a) new appends don't collide with leftover segment numbers and
    /// (b) `kick_replays` knows what to replay. A corrupt segment is
    /// AUTO-QUARANTINED (bytes preserved aside, counter bumped, loud error)
    /// and skipped — startup continues with the remaining valid segments;
    /// ACKed data inside a corrupt segment is unrecoverable. Only I/O
    /// errors (read_dir, LIST/GET network) remain fatal.
    ///
    /// Returns `(tables_with_segments, persisted_renames)` — the second is
    /// the per-table rename sidecar content loaded from disk so main.rs can
    /// seed the in-memory rename map before any replay (closes the
    /// crash-after-DDL-registration-before-drain mapping-loss window).
    pub(crate) async fn scan_existing(
        &self,
    ) -> Result<(Vec<String>, HashMap<String, HashMap<String, String>>)> {
        let mut found: Vec<String> = Vec::new();
        match &self.backend {
            Backend::Local { root } => {
                let entries = match std::fs::read_dir(root) {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok((found, HashMap::new()))
                    }
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("wal buffer scan {}", root.display()))
                    }
                };
                for entry in entries {
                    let entry =
                        entry.with_context(|| format!("wal buffer scan {}", root.display()))?;
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !entry.path().is_dir() || name.is_empty() || name == "quarantine" {
                        continue;
                    }
                    let seqs = self.list_segments(&name).await?;
                    // (seq, encoded byte count) for VALID segments only.
                    let mut valid: Vec<(u64, u64)> = Vec::new();
                    for &seq in &seqs {
                        match self.read_segment_bytes(&name, seq).await? {
                            None => continue,
                            Some(bytes) => match decode_segment(&bytes) {
                                Ok(_) => valid.push((seq, bytes.len() as u64)),
                                Err(_) => {
                                    self.quarantine_segment(&name, seq, &bytes).await.with_context(
                                        || {
                                            format!(
                                                "wal buffer startup scan: quarantine failed for {name}/seg_{seq:020}.bin"
                                            )
                                        },
                                    )?;
                                    tracing::error!(
                                        "wal buffer startup scan: segment {name}/seg_{seq:020}.bin corrupt (checksum) — quarantined; data ACKed past this segment is unrecoverable"
                                    );
                                }
                            },
                        }
                    }
                    // A table whose segments were ALL corrupt is not
                    // registered — nothing left to replay.
                    if valid.is_empty() {
                        continue;
                    }
                    let max = valid.last().map(|(s, _)| *s).unwrap();
                    let table_bytes: u64 = valid.iter().map(|(_, b)| *b).sum();
                    let mut st = lock_state(&self.state);
                    st.segmented.insert(name.clone());
                    st.next_seq.insert(name.clone(), max + 1);
                    st.seg_count.insert(name.clone(), valid.len() as u64);
                    st.seg_size
                        .insert(name.clone(), valid.iter().copied().collect());
                    st.table_bytes.insert(name.clone(), table_bytes);
                    found.push(name);
                }
            }
            Backend::S3(s3) => {
                let keys = s3.list_keys_with_prefix(&s3.root_key).await?;
                let mut by_table: HashMap<String, Vec<u64>> = HashMap::new();
                for k in keys {
                    if let Some((table, seq)) = parse_object_key(&s3.root_key, &k) {
                        by_table.entry(table).or_default().push(seq);
                    }
                }
                for (table, mut seqs) in by_table {
                    seqs.sort_unstable();
                    let mut valid: Vec<(u64, u64)> = Vec::new();
                    for &seq in &seqs {
                        match self.read_segment_bytes(&table, seq).await? {
                            None => continue,
                            Some(bytes) => match decode_segment(&bytes) {
                                Ok(_) => valid.push((seq, bytes.len() as u64)),
                                Err(_) => {
                                    self.quarantine_segment(&table, seq, &bytes).await.with_context(
                                        || {
                                            format!(
                                                "wal buffer startup scan: quarantine failed for {table}/seg_{seq:020}.bin"
                                            )
                                        },
                                    )?;
                                    tracing::error!(
                                        "wal buffer startup scan: segment {table}/seg_{seq:020}.bin corrupt (checksum) — quarantined; data ACKed past this segment is unrecoverable"
                                    );
                                }
                            },
                        }
                    }
                    if valid.is_empty() {
                        continue;
                    }
                    let max = valid.last().map(|(s, _)| *s).unwrap();
                    let table_bytes: u64 = valid.iter().map(|(_, b)| *b).sum();
                    let mut st = lock_state(&self.state);
                    st.segmented.insert(table.clone());
                    st.next_seq.insert(table.clone(), max + 1);
                    st.seg_count.insert(table.clone(), valid.len() as u64);
                    st.seg_size
                        .insert(table.clone(), valid.iter().copied().collect());
                    st.table_bytes.insert(table.clone(), table_bytes);
                    found.push(table);
                }
            }
        }
        // Load persisted rename sidecars for every table with valid segments
        // (a sidecar without segments is stale — never loaded).
        let mut renames_out: HashMap<String, HashMap<String, String>> = HashMap::new();
        for name in &found {
            if let Some(renames) = self.read_renames_sidecar(name).await? {
                if !renames.is_empty() {
                    renames_out.insert(name.clone(), renames);
                    lock_state(&self.state)
                        .renames_persisted
                        .insert(name.clone());
                }
            }
        }
        found.sort_unstable();
        Ok((found, renames_out))
    }
}

/// RAII guard for a table's replay slot: releases the slot (and un-buffers
/// the table once no segments remain) on drop — including on task panic or
/// abort, which the explicit `end_replay` calls in every replay return path
/// used to cover.
pub(crate) struct ReplaySlotGuard {
    mgr: Arc<WalBufferManager>,
    table_key: String,
}

impl ReplaySlotGuard {
    pub(crate) fn new(mgr: Arc<WalBufferManager>, table_key: String) -> Self {
        Self { mgr, table_key }
    }
}

impl Drop for ReplaySlotGuard {
    fn drop(&mut self) {
        self.mgr.end_replay(&self.table_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tempfile::tempdir;

    fn rec(kind: u8, payload: &str) -> BufRecord {
        BufRecord {
            kind,
            oid: 0,
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn rec_oid(kind: u8, oid: u32, payload: &str) -> BufRecord {
        BufRecord {
            kind,
            oid,
            payload: payload.as_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn test_roundtrip_all_kinds() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        let records = vec![
            rec(BUF_UPSERT, r#"{"id":1,"v":"a"}"#),
            rec_oid(BUF_DELETE, 42, r#"{"id":2,"v":"b"}"#),
            rec(BUF_TRUNCATE, ""),
        ];
        mgr.append("public.t", &records, &HashMap::new())
            .await
            .unwrap();
        let segs = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(segs, vec![0]);
        let read = mgr.read_segment("public.t", 0).await.unwrap();
        assert_eq!(
            read, records,
            "oid + payload roundtrip through checksummed wire format"
        );
        assert_eq!(
            mgr.total_buffered_bytes(),
            read_segment_file_size(&mgr, "public.t", 0)
        );
    }

    /// Raw encoded size of a segment on disk (test helper for byte gauge).
    fn read_segment_file_size(mgr: &WalBufferManager, table_key: &str, seq: u64) -> u64 {
        let Backend::Local { root } = &mgr.backend else {
            panic!("local backend only");
        };
        std::fs::metadata(root.join(table_key).join(segment_file_name(seq)))
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn test_multi_segment_ordering() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_UPSERT, r#"{"id":1}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_UPSERT, r#"{"id":2}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_DELETE, r#"{"id":1}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        let segs = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(segs, vec![0, 1, 2], "segments listed ascending");
        let s0 = mgr.read_segment("public.t", 0).await.unwrap();
        assert_eq!(s0, vec![rec(BUF_UPSERT, r#"{"id":1}"#)]);
        let s2 = mgr.read_segment("public.t", 2).await.unwrap();
        assert_eq!(s2, vec![rec(BUF_DELETE, r#"{"id":1}"#)]);
        // Delete middle segment; list reflects it.
        mgr.delete_segment("public.t", 1).await.unwrap();
        let segs = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(segs, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_corrupt_magic_errors() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await
            .unwrap();
        // Corrupt the segment on disk (wrong magic).
        let path = root.join("public.t").join(segment_file_name(0));
        std::fs::write(&path, b"NOPE").unwrap();
        let res = mgr.read_segment("public.t", 0).await;
        assert!(res.is_err(), "corrupt segment must error, never skip");
    }

    #[test]
    fn test_decode_corrupt_count_rejected() {
        // Header claiming u32::MAX records must Err, never allocate (also
        // shorter than header + trailer).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_segment(&bytes).is_err());
        // Record with u64::MAX payload length must Err (checked arithmetic,
        // no panic). Trailer slot present so the count sanity check passes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(BUF_UPSERT);
        bytes.extend_from_slice(&0u32.to_le_bytes()); // oid
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        bytes.extend_from_slice(&[0u8; TRAILER_BYTES]);
        assert!(decode_segment(&bytes).is_err());
    }

    #[test]
    fn test_decode_zero_len_row_rejected() {
        // BUF_UPSERT with a zero-length payload is a malformed segment.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(BUF_UPSERT);
        bytes.extend_from_slice(&7u32.to_le_bytes()); // oid
        bytes.extend_from_slice(&0u64.to_le_bytes()); // len == 0
        bytes.extend_from_slice(&Sha256::digest(&bytes)); // valid trailer
        let err = decode_segment(&bytes).unwrap_err().to_string();
        assert!(err.contains("non-empty"), "got: {err}");
    }

    #[test]
    fn test_decode_truncate_with_payload_rejected() {
        // BUF_TRUNCATE must carry zero payload.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(BUF_TRUNCATE);
        bytes.extend_from_slice(&0u32.to_le_bytes()); // oid
        bytes.extend_from_slice(&1u64.to_le_bytes()); // len == 1
        bytes.push(b'x');
        bytes.extend_from_slice(&Sha256::digest(&bytes)); // valid trailer
        let err = decode_segment(&bytes).unwrap_err().to_string();
        assert!(err.contains("zero-length"), "got: {err}");
    }

    #[tokio::test]
    async fn test_checksum_mismatch_detected() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_UPSERT, r#"{"id":1}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        let path = root.join("public.t").join(segment_file_name(0));
        let mut bytes = std::fs::read(&path).unwrap();
        // Flip the first payload byte (12 header + 13 record header → offset
        // 25). Structure stays valid; the SHA-256 trailer must catch it.
        bytes[25] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let err = mgr.read_segment("public.t", 0).await.unwrap_err();
        // anyhow 1.0.x: plain Display shows only the outermost context;
        // the cause chain (checksum mismatch) needs alternate formatting.
        let err = format!("{err:#}");
        assert!(err.contains("checksum"), "got: {err}");
    }

    #[tokio::test]
    async fn test_read_segment_bytes_missing_is_none() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        assert!(
            mgr.read_segment_bytes("public.t", 0)
                .await
                .unwrap()
                .is_none(),
            "missing segment → Ok(None), not Err"
        );
    }

    #[tokio::test]
    async fn test_scan_existing_quarantines_corrupt() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        {
            let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
            mgr.append("public.a", &[rec(BUF_UPSERT, "1")], &HashMap::new())
                .await
                .unwrap();
            mgr.append("public.b", &[rec(BUF_UPSERT, "2")], &HashMap::new())
                .await
                .unwrap();
        }
        // Corrupt public.a's only segment (flip a payload byte).
        let path = root.join("public.a").join(segment_file_name(0));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[25] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let mgr2 = WalBufferManager::new_local_root(root.clone()).unwrap();
        let (found, _renames) = mgr2.scan_existing().await.unwrap();
        assert_eq!(
            found,
            vec!["public.b".to_string()],
            "corrupt-only table not registered"
        );
        assert_eq!(
            mgr2.quarantined_count().load(Ordering::Relaxed),
            1,
            "quarantine counter bumped once"
        );
        // File moved under quarantine/<table>/, bytes preserved.
        let qdir = root.join("quarantine").join("public.a");
        let qfiles: Vec<String> = std::fs::read_dir(&qdir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(qfiles.len(), 1, "one quarantined copy");
        assert!(
            qfiles[0].starts_with("seg_00000000000000000000.bin."),
            "quarantine file keeps segment identity: {}",
            qfiles[0]
        );
        let qbytes = std::fs::read(qdir.join(&qfiles[0])).unwrap();
        assert_eq!(qbytes, bytes, "quarantined bytes preserved verbatim");
        // Accounting excludes the quarantined segment; valid table intact.
        assert_eq!(
            mgr2.total_buffered_bytes(),
            read_segment_file_size(&mgr2, "public.b", 0),
            "only the valid table's bytes count"
        );
        assert!(mgr2.list_segments("public.a").await.unwrap().is_empty());
        assert!(mgr2.has_segments("public.b"));
        assert!(!mgr2.has_segments("public.a"));
    }

    #[tokio::test]
    async fn test_truncated_payload_errors() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "payload")], &HashMap::new())
            .await
            .unwrap();
        // Truncate the file mid-payload.
        let path = root.join("public.t").join(segment_file_name(0));
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        let res = mgr.read_segment("public.t", 0).await;
        assert!(res.is_err(), "truncated segment must error, never skip");
    }

    #[tokio::test]
    async fn test_purge_all() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        mgr.append("public.a", &[rec(BUF_UPSERT, "1")], &HashMap::new())
            .await
            .unwrap();
        mgr.append("public.b", &[rec(BUF_UPSERT, "2")], &HashMap::new())
            .await
            .unwrap();
        assert!(mgr.has_segments("public.a"));
        mgr.purge_all().await.unwrap();
        assert!(!mgr.has_segments("public.a"));
        assert!(!mgr.has_segments("public.b"));
        assert!(mgr.list_segments("public.a").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_flags_and_delete_accounting() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        assert!(!mgr.has_segments("public.t"));
        assert!(mgr.try_begin_replay("public.t"), "first claim succeeds");
        assert!(
            !mgr.try_begin_replay("public.t"),
            "second concurrent claim rejected"
        );
        mgr.end_replay("public.t");
        assert!(
            mgr.try_begin_replay("public.t"),
            "replay slot released after end_replay"
        );
        mgr.end_replay("public.t");

        mgr.append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await
            .unwrap();
        assert!(mgr.has_segments("public.t"));
        // Delete the only segment → has_segments flips false.
        mgr.delete_segment("public.t", 0).await.unwrap();
        assert!(!mgr.has_segments("public.t"));
        // end_replay with no segments must not resurrect it.
        mgr.end_replay("public.t");
        assert!(!mgr.has_segments("public.t"));
    }

    #[tokio::test]
    async fn test_end_replay_keeps_segmented_when_segments_remain() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "1")], &HashMap::new())
            .await
            .unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "2")], &HashMap::new())
            .await
            .unwrap();
        assert!(mgr.try_begin_replay("public.t"));
        mgr.delete_segment("public.t", 0).await.unwrap();
        mgr.end_replay("public.t");
        assert!(
            mgr.has_segments("public.t"),
            "one segment remains → still buffered"
        );
        mgr.delete_segment("public.t", 1).await.unwrap();
        assert!(!mgr.has_segments("public.t"));
    }

    #[tokio::test]
    async fn test_scan_existing_recovers_tables_and_next_seq() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        {
            let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
            mgr.append("public.a", &[rec(BUF_UPSERT, "1")], &HashMap::new())
                .await
                .unwrap();
            mgr.append("public.a", &[rec(BUF_UPSERT, "2")], &HashMap::new())
                .await
                .unwrap();
            mgr.append("public.b", &[rec(BUF_UPSERT, "3")], &HashMap::new())
                .await
                .unwrap();
        }
        // Fresh manager over the same root simulates a restart.
        let mgr2 = WalBufferManager::new_local_root(root.clone()).unwrap();
        let (found, _renames) = mgr2.scan_existing().await.unwrap();
        assert_eq!(found, vec!["public.a".to_string(), "public.b".to_string()]);
        assert!(mgr2.has_segments("public.a"));
        assert!(mgr2.has_segments("public.b"));
        // New append must continue past the recovered max seq (no collision).
        mgr2.append("public.a", &[rec(BUF_UPSERT, "3")], &HashMap::new())
            .await
            .unwrap();
        let segs = mgr2.list_segments("public.a").await.unwrap();
        assert_eq!(segs, vec![0, 1, 2], "new segment gets seq 2 (max+1)");
    }

    #[tokio::test]
    async fn test_scan_existing_empty_root() {
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        let (found, _renames) = mgr.scan_existing().await.unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_segment_name_roundtrip() {
        assert_eq!(segment_file_name(0), "seg_00000000000000000000.bin");
        assert_eq!(segment_file_name(1), "seg_00000000000000000001.bin");
        assert_eq!(segment_file_name(u64::MAX), "seg_18446744073709551615.bin");
        assert_eq!(
            parse_segment_file_name("seg_00000000000000000001.bin"),
            Some(1)
        );
        assert_eq!(parse_segment_file_name("seg_1.bin"), None);
        assert_eq!(parse_segment_file_name("other.bin"), None);
    }

    #[test]
    fn test_parse_object_key() {
        let root = "pg2sr-cdc-checkpoint.json.buffer";
        let key = "pg2sr-cdc-checkpoint.json.buffer/public.t/seg_00000000000000000007.bin";
        assert_eq!(
            parse_object_key(root, key),
            Some(("public.t".to_string(), 7))
        );
        assert_eq!(
            parse_object_key(root, "pg2sr-cdc-checkpoint.json.buffer/other/x"),
            None
        );
        assert_eq!(parse_object_key(root, "unrelated/key"), None);
        // Quarantined copies are never active segments.
        assert_eq!(
            parse_object_key(
                root,
                "pg2sr-cdc-checkpoint.json.buffer/quarantine/public.t/seg_00000000000000000007.bin"
            ),
            None
        );
    }

    /// Test-only S3 config mirroring `S3Backend::new`'s requirements (path-style
    /// addressing so the mock server receives the bucket in the URL path).
    fn make_s3_config(endpoint: String) -> crate::config::CheckpointConfig {
        crate::config::CheckpointConfig {
            store_type: "s3".to_string(),
            path: "ckpt.json".to_string(),
            save_interval_ms: 10000,
            bucket: Some("b".to_string()),
            region: None,
            endpoint: Some(endpoint),
            path_style: true,
            access_key_id: Some("AKIATEST".to_string()),
            secret_access_key: Some("secrettest".to_string()),
        }
    }

    #[tokio::test]
    async fn test_append_mkdir_failure_rolls_back_accounting() {
        // A failed append (mkdir blocked by a file in the way) must roll back
        // the in-memory accounting: no `segmented` entry, no segment/byte
        // gauge residue (the old code subtracted never-added
        // bytes).
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        // A FILE where the table dir should be → create_dir_all fails.
        std::fs::write(root.join("public.t"), b"in the way").unwrap();

        let err = mgr
            .append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await;
        assert!(err.is_err(), "mkdir failure must surface as append Err");
        assert!(!mgr.has_segments("public.t"), "accounting rolled back");
        assert_eq!(mgr.total_segments(), 0);
        assert_eq!(mgr.total_buffered_bytes(), 0);
        // A second failed append must not corrupt accounting further.
        let err2 = mgr
            .append("public.t", &[rec(BUF_UPSERT, "y")], &HashMap::new())
            .await;
        assert!(err2.is_err());
        assert!(!mgr.has_segments("public.t"));
        assert_eq!(mgr.total_segments(), 0);
        assert_eq!(mgr.total_buffered_bytes(), 0);
        // seq reservation must not have leaked a next_seq entry that makes
        // the table look segmented.
        assert!(mgr.segmented_tables().is_empty());
    }

    #[tokio::test]
    async fn test_failed_publish_keeps_surviving_orphan_accounted() {
        // Orphan-length fallback: when the publish fails and the segment entry
        // outlives the cleanup removal, the accounting must keep it with its
        // ENCODED length. The old code decided that on a metadata probe, which
        // can fail for an entry that exists (dropping the segment back to an
        // invisible leak — visible to `list_segments`, invisible to
        // `segmented` / the byte cap until the next startup scan) or report a
        // size that is not the segment's bytes.
        //
        // Driven deterministically, without a real IO error: a DIRECTORY where
        // the segment file belongs makes the publish `rename` fail AND makes
        // the follow-up `remove_file` fail with a non-NotFound error — exactly
        // the "published entry outlived cleanup" state.
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        let records = [rec(BUF_UPSERT, "x")];
        let encoded_len = encode_segment(&records).unwrap().len() as u64;

        // Baseline: a successful append accounts exactly what it wrote, and
        // the accounting agrees with what the backend lists.
        mgr.append("public.ok", &records, &HashMap::new())
            .await
            .unwrap();
        let listed = mgr.list_segments("public.ok").await.unwrap();
        assert_eq!(listed, vec![0]);
        assert_eq!(mgr.segment_count("public.ok"), listed.len() as u64);
        assert_eq!(mgr.total_buffered_bytes(), encoded_len);

        // Occupied segment name: rename → EISDIR, remove_file → non-NotFound.
        std::fs::create_dir_all(root.join("public.t").join(segment_file_name(0))).unwrap();
        let err = mgr.append("public.t", &records, &HashMap::new()).await;
        assert!(err.is_err(), "failed publish surfaces as append Err");
        assert!(
            mgr.has_segments("public.t"),
            "a surviving entry keeps the table segmented (thus drainable)"
        );
        let listed = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(listed, vec![0], "the surviving entry stays listable");
        assert_eq!(
            mgr.segment_count("public.t"),
            listed.len() as u64,
            "accounting never under-reports what list_segments sees"
        );
        assert_eq!(mgr.total_segments(), 2, "reservation kept, not rolled back");
        assert_eq!(
            mgr.total_buffered_bytes(),
            encoded_len * 2,
            "kept bytes are the encoded length, not a stat of the entry"
        );
    }

    #[tokio::test]
    async fn test_byte_cap_fails_closed_and_keeps_accounting() {
        // A permanently failing table must not be able to fill the disk: once
        // the cap is reached, append fails closed (Err → flush Err → checkpoint
        // does NOT advance) and the accounting/segments stay exactly as they
        // were — the failed reservation must not corrupt them.
        let dir = tempdir().unwrap();
        let mgr = WalBufferManager::new_local_root(dir.path().join("buf")).unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "1")], &HashMap::new())
            .await
            .unwrap();
        let used = mgr.total_buffered_bytes();
        assert!(used > 0);
        assert_eq!(mgr.total_segments(), 1);

        // Cap == current usage → the next append must be rejected.
        mgr.set_max_bytes(used);
        let err = mgr
            .append("public.t", &[rec(BUF_UPSERT, "2")], &HashMap::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("cap exceeded"), "got: {err}");
        assert_eq!(mgr.total_segments(), 1, "no segment added on cap rejection");
        assert_eq!(mgr.total_buffered_bytes(), used, "bytes unchanged");
        assert!(
            mgr.has_segments("public.t"),
            "existing segment still drainable"
        );
        assert_eq!(
            mgr.list_segments("public.t").await.unwrap().len(),
            1,
            "only the pre-cap segment is visible"
        );

        // Raising the cap lets the append through — proving the cap (not some
        // other failure) rejected it.
        mgr.set_max_bytes(used * 4);
        mgr.append("public.t", &[rec(BUF_UPSERT, "2")], &HashMap::new())
            .await
            .unwrap();
        assert_eq!(mgr.total_segments(), 2);
    }

    #[tokio::test]
    async fn test_append_s3_put_failure_rolls_back_when_absent() {
        // A PUT failure with a CONFIRMED-absent object (verify
        // GET 404) must roll back — the checkpoint will NOT advance past
        // these events, so no orphan can replay stale rows after restart.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let key = "b/ckpt.json.buffer/public.t/seg_00000000000000000000.bin";
        Mock::given(method("PUT"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        let err = mgr
            .append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await;
        assert!(err.is_err(), "PUT failed + verify 404 → append Err");
        assert!(
            !mgr.has_segments("public.t"),
            "no accounting on clean failure"
        );
        assert_eq!(mgr.total_segments(), 0);
        assert_eq!(mgr.total_buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn test_append_s3_put_failure_ok_when_committed() {
        // A PUT that ERRORED but actually committed (verify GET
        // 200) must NOT roll back — the checkpoint may advance past the
        // object; rolling back would leave an orphan segment that replays
        // stale rows after restart.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let key = "b/ckpt.json.buffer/public.t/seg_00000000000000000000.bin";
        Mock::given(method("PUT"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"committed-bytes"))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        let res = mgr
            .append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await;
        assert!(res.is_ok(), "committed-but-error PUT → Ok (verify GET 200)");
        assert!(mgr.has_segments("public.t"), "segment accounted");
        assert_eq!(mgr.total_segments(), 1);
        assert!(mgr.total_buffered_bytes() > 0);
    }

    #[tokio::test]
    async fn test_append_s3_verify_failure_keeps_accounting() {
        // Ambiguous S3 failure: the PUT errored AND the verify GET errored, so
        // the object's existence is unknown — it may have survived the
        // best-effort cleanup DELETE. The reservation must therefore NOT roll
        // back, or an object the backend can list is invisible to `segmented`
        // / the byte cap until the next startup scan. S3 counterpart of the
        // local arm's "accounting must match what the backend can list".
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let key = "b/ckpt.json.buffer/public.t/seg_00000000000000000000.bin";
        // 403 (not 5xx) for all three verbs → no retry backoff sleeps, so the
        // test stays fast while exercising the same failures.
        Mock::given(method("PUT"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(403).set_body_string("denied"))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        let records = [rec(BUF_UPSERT, "x")];
        let encoded_len = encode_segment(&records).unwrap().len() as u64;
        let err = mgr.append("public.t", &records, &HashMap::new()).await;
        assert!(err.is_err(), "unverifiable PUT failure → append Err");
        assert!(
            mgr.has_segments("public.t"),
            "ambiguous failure leaves the table buffered"
        );
        assert_eq!(mgr.total_segments(), 1, "reservation kept, not rolled back");
        assert_eq!(
            mgr.total_buffered_bytes(),
            encoded_len,
            "kept bytes are the encoded length"
        );
    }

    #[tokio::test]
    async fn test_s3_append_list_roundtrip() {
        // Regression: the replay path depends on `list_segments` finding
        // objects that `append` PUTs under the same prefix. A LIST that
        // silently returns empty (parse/prefix/endpoint quirk) wedges the
        // table forever: every kick spawns a replay that exits Ok at
        // `segs.is_empty()` with zero progress and zero errors.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let key = "b/ckpt.json.buffer/public.t/seg_00000000000000000000.bin";
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>b</Name>
  <Prefix>ckpt.json.buffer/public.t/seg_</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>ckpt.json.buffer/public.t/seg_00000000000000000000.bin</Key>
    <LastModified>2026-08-28T22:42:34.000Z</LastModified>
    <ETag>"abc"</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#
            .to_string();
        Mock::given(method("PUT"))
            .and(path(key))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        // LIST: path-style bucket "b" resolves to URL path "/b/" (trailing
        // slash, rusty_s3 Bucket::base_url) with list-type=2 query.
        Mock::given(method("GET"))
            .and(path("/b/"))
            .and(query_param("list-type", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_string(xml))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_UPSERT, r#"{"id":1}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        let segs = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(segs, vec![0], "LIST must see the segment PUT just wrote");
    }

    #[tokio::test]
    async fn test_append_sidecar_roundtrip() {
        // Rename sidecar: written on append with a non-empty map, recovered
        // by scan_existing (crash window), deleted when replay drains the
        // table, deleted when an append carries an empty map.
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        let renames = HashMap::from([("a".to_string(), "b".to_string())]);
        mgr.append("public.t", &[rec(BUF_UPSERT, "1")], &renames)
            .await
            .unwrap();
        assert!(root.join("public.t.renames").exists());
        assert_eq!(
            mgr.read_renames_sidecar("public.t").await.unwrap(),
            Some(renames.clone())
        );
        // Restart simulation: a fresh manager scans and recovers the sidecar
        // into the returned map + renames_persisted.
        let mgr2 = WalBufferManager::new_local_root(root.clone()).unwrap();
        let (found, renames_out) = mgr2.scan_existing().await.unwrap();
        assert_eq!(found, vec!["public.t".to_string()]);
        assert_eq!(renames_out.get("public.t"), Some(&renames));
        assert!(
            mgr2.read_renames_sidecar("public.t")
                .await
                .unwrap()
                .is_some(),
            "sidecar survives the manager restart"
        );
        // Replay drain: last segment deleted + end_replay → sidecar removed.
        mgr2.delete_segment("public.t", 0).await.unwrap();
        mgr2.end_replay("public.t");
        assert!(
            !root.join("public.t.renames").exists(),
            "sidecar deleted once the table drains"
        );
        assert!(!mgr2.has_segments("public.t"));
        // Append with an empty map deletes an existing sidecar.
        mgr2.append("public.t", &[rec(BUF_UPSERT, "2")], &renames)
            .await
            .unwrap();
        assert!(root.join("public.t.renames").exists());
        mgr2.append("public.t", &[rec(BUF_UPSERT, "3")], &HashMap::new())
            .await
            .unwrap();
        assert!(
            !root.join("public.t.renames").exists(),
            "empty rename map deletes the sidecar"
        );
    }

    #[tokio::test]
    async fn test_quarantine_counter_bumps_once() {
        // regression: the quarantine counter bumps ONLY when the
        // quarantine fully succeeds — a failed quarantine bumps nothing and
        // a retried quarantine bumps exactly once.
        let dir = tempdir().unwrap();
        let root = dir.path().join("buf");
        let mgr = WalBufferManager::new_local_root(root.clone()).unwrap();
        mgr.append("public.t", &[rec(BUF_UPSERT, "x")], &HashMap::new())
            .await
            .unwrap();
        // Block the quarantine destination: a FILE where `quarantine/` should
        // be → create_dir_all fails → quarantine Err before any move →
        // no counter bump, accounting untouched.
        std::fs::write(root.join("quarantine"), b"block").unwrap();
        let bytes = std::fs::read(root.join("public.t").join(segment_file_name(0))).unwrap();
        let err = mgr.quarantine_segment("public.t", 0, &bytes).await;
        assert!(err.is_err(), "blocked quarantine must fail");
        assert_eq!(mgr.quarantined_count().load(Ordering::Relaxed), 0);
        assert!(
            mgr.has_segments("public.t"),
            "accounting untouched on failure"
        );
        // Unblock; the retry succeeds and bumps exactly once.
        std::fs::remove_file(root.join("quarantine")).unwrap();
        mgr.quarantine_segment("public.t", 0, &bytes).await.unwrap();
        assert_eq!(mgr.quarantined_count().load(Ordering::Relaxed), 1);
        assert!(!mgr.has_segments("public.t"));
    }
    #[tokio::test]
    async fn test_s3_list_encoded_keys_still_parse() {
        // Providers honoring encoding-type=url (which rusty_s3 always
        // requests) may percent-encode Key values — including '/' as %2F.
        // The replay path must still resolve them, or every segment becomes
        // invisible to replay (silent wedge).
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>b</Name>
  <Prefix>ckpt.json.buffer/public.t/seg_</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <EncodingType>url</EncodingType>
  <Contents>
    <Key>ckpt.json.buffer%2Fpublic.t%2Fseg_00000000000000000000.bin</Key>
    <LastModified>2026-08-28T22:42:34.000Z</LastModified>
    <ETag>"abc"</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#;
        Mock::given(method("GET"))
            .and(path("/b/"))
            .and(query_param("list-type", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_string(xml))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        let segs = mgr.list_segments("public.t").await.unwrap();
        assert_eq!(
            segs,
            vec![0],
            "URL-encoded keys from encoding-type=url must parse"
        );
    }

    #[tokio::test]
    async fn test_s3_list_empty_while_accounted_is_detectable() {
        // The exact production wedge state: accounting counts a segment but
        // the (broken) LIST sees nothing. The replay path keys its loud
        // divergence warning on `segment_count > 0 && segs.is_empty()` —
        // pin both sides of that condition so a manager regression can't
        // silently un-detect it.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>b</Name>
  <Prefix>ckpt.json.buffer/public.t/seg_</Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <EncodingType>url</EncodingType>
</ListBucketResult>"#;
        // PUT succeeds (like production) but LIST drops the key.
        Mock::given(method("PUT"))
            .and(path(
                "/b/ckpt.json.buffer/public.t/seg_00000000000000000000.bin",
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/b/"))
            .and(query_param("list-type", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_string(xml))
            .mount(&server)
            .await;

        let mgr = WalBufferManager::new(
            Some(&make_s3_config(format!(
                "http://127.0.0.1:{}",
                server.address().port()
            ))),
            0,
        )
        .unwrap();
        mgr.append(
            "public.t",
            &[rec(BUF_UPSERT, r#"{"id":1}"#)],
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            mgr.segment_count("public.t"),
            1,
            "accounting counts the append"
        );
        assert!(
            mgr.list_segments("public.t").await.unwrap().is_empty(),
            "the broken LIST still sees nothing"
        );
        assert!(
            mgr.has_segments("public.t"),
            "table stays buffered — the wedge state"
        );
    }
}
