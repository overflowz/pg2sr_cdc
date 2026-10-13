//! S3/S3-compatible object store checkpoint backend.
//!
//! Uses presigned URLs via `rusty-s3` for authentication and `reqwest` for HTTP.
//! Supports AWS S3 and S3-compatible stores (MinIO, Ceph Object Store, etc.)
//! through the `endpoint` and `path_style` config options.
//!
//! # Atomicity
//! Writes to `{key}.tmp` first, then overwrites `{key}` on success, then
//! deletes `{key}.tmp`. If the process crashes mid-write, the `.tmp` key
//! is cleaned up on the next `save()` call.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use super::{CURRENT_CHECKPOINT_VERSION, Checkpoint, CheckpointStore};
use crate::config::CheckpointConfig;

/// Format a reqwest error WITHOUT the URL. Presigned S3 URLs embed
/// `X-Amz-Signature=...` (a valid request signature) in the query string, and
/// reqwest 0.12 interpolates the full URL into request-error Display — leaking
/// a re-usable signature into logs / `anyhow` error chains. `without_url()`
/// (reqwest 0.12+) returns the error with the URL stripped.
fn reqwest_err_no_url(e: reqwest::Error) -> String {
    format!("{}", e.without_url())
}

/// S3/S3-compatible object store checkpoint backend.
///
/// Uses presigned URLs via `rusty-s3` for authentication, and `reqwest` for HTTP.
/// Supports AWS S3 and S3-compatible stores (MinIO, Ceph Object Store, etc.)
/// through the `endpoint` and `path_style` config options.
///
/// # Atomicity
/// Writes to `{key}.tmp` first, then overwrites `{key}` on success, then
/// deletes `{key}.tmp`. If the process crashes mid-write, the `.tmp` key
/// is cleaned up on the next `save()` call.
pub struct S3Store {
    client: reqwest::Client,
    key: String,
    credentials: rusty_s3::Credentials,
    bucket_url: rusty_s3::Bucket,
    /// Serializes save() calls to prevent concurrent S3 PUT races where a
    /// stale (lower LSN/ddl_id) write lands after a newer one. S3 has no
    /// cross-PUT ordering guarantee, so last-PUT-wins can regress the
    /// checkpoint — causing DDL re-execution (data loss for non-idempotent
    /// DDLs like DROP TABLE) or WAL replay (waste).
    save_lock: tokio::sync::Mutex<()>,
}

impl S3Store {
    pub fn new(config: &CheckpointConfig) -> Result<Self> {
        let bucket_name = config.bucket.clone().unwrap_or_default();
        if bucket_name.is_empty() {
            anyhow::bail!("S3 checkpoint requires a non-empty 'bucket' in [checkpoint] config");
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

        // Fall back to env vars (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
        // when not set in config — mirrors the AWS SDK default credential chain
        // and matches the config doc. If STILL empty, bail fast instead of
        // signing requests with empty creds (a confusing S3 403 the caller
        // can't easily diagnose). IRSA/IMDS web-identity fallback is NOT
        // implemented (rusty_s3 has no credentials provider); document/env-gate.
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
                "S3 checkpoint backend requires credentials: set [checkpoint] \
                 access_key_id + secret_access_key in config, OR export \
                 AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY. \
                 (IRSA/IMDS web-identity fallback is not implemented.)"
            );
        }
        let credentials = rusty_s3::Credentials::new(access_key, secret_key);

        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .context("Failed to create HTTP client for S3")?,
            key: config.path.clone(),
            credentials,
            bucket_url,
            save_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Get a presigned URL for an S3 action with a generous expiry.
    /// 300s expiry — long enough to survive retries (was 60s, which caused
    /// HTTP 403 "Request has expired" on slow MinIO after 2-3 timeout retries).
    fn sign_url<'a>(&self, action: impl rusty_s3::S3Action<'a>) -> url::Url {
        action.sign_with_time(Duration::from_secs(300), &time::OffsetDateTime::now_utc())
    }

    /// Check if an S3 object exists by sending a HEAD request.
    #[allow(dead_code)]
    async fn object_exists(&self, object_key: &str) -> Result<bool> {
        let action = self
            .bucket_url
            .head_object(Some(&self.credentials), object_key.as_ref());
        let url = self.sign_url(action);
        let resp = self.client.head(url).send().await;
        match resp {
            Ok(r) if r.status().is_success() => Ok(true),
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => Ok(false),
            Ok(r) => Err(anyhow::anyhow!(
                "S3 HEAD failed for '{}': HTTP {}",
                object_key,
                r.status()
            )),
            Err(e) => Err(anyhow::anyhow!(
                "S3 HEAD request error for '{}': {}",
                object_key,
                reqwest_err_no_url(e)
            )),
        }
    }

    /// Verify the configured bucket exists and is reachable. Single HEAD-bucket
    /// request — no retry loop because failures here are almost always config
    /// errors (wrong bucket/region/creds/unreachable endpoint) that no amount
    /// of retrying will fix. Called once at startup via `verify()`.
    async fn verify_bucket(&self) -> Result<()> {
        let bucket_name = self.bucket_url.name();
        let region = self.bucket_url.region();
        let endpoint = self.bucket_url.base_url();
        let action = self.bucket_url.head_bucket(Some(&self.credentials));
        let url = self.sign_url(action);
        match self.client.head(url).send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(
                    "S3 checkpoint bucket '{bucket_name}' reachable \
                     (region={region}, endpoint={endpoint})",
                );
                Ok(())
            }
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => bail!(
                "S3 checkpoint bucket '{bucket_name}' does not exist \
                 (region={region}, endpoint={endpoint}). \
                 Create it first, or fix the 'bucket' / 'region' / 'endpoint' \
                 fields under [checkpoint] in your config."
            ),
            Ok(r) if r.status() == reqwest::StatusCode::FORBIDDEN => bail!(
                "Access denied (HTTP 403) to S3 checkpoint bucket '{bucket_name}' \
                 (region={region}, endpoint={endpoint}). \
                 Verify 'access_key_id' / 'secret_access_key' and the bucket policy."
            ),
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                bail!(
                    "S3 HEAD bucket '{bucket_name}' returned HTTP {status}: {body} \
                     (region={region}, endpoint={endpoint})"
                )
            }
            // Strip the request URL from the reqwest error: the URL is
            // presigned (X-Amz-Signature, 300s expiry) and reqwest's Display
            // would embed it, leaking a re-usable credential into WARN/ERROR
            // logs and the rotating log file.
            Err(e) => bail!(
                "Cannot reach S3 endpoint {endpoint} for bucket '{bucket_name}' \
                 (region={region}): {}. \
                 (Request URL omitted deliberately — it carries a presigned \
                 signature.) \
                 Check 'endpoint', 'region', DNS, and TLS configuration.",
                reqwest_err_no_url(e)
            ),
        }
    }

    /// Upload content to an S3 object key with exponential backoff retry.
    /// Transient S3 failures (5xx, network bounce) are retried up to 3 times
    /// before the error is propagated to the caller.
    async fn put_object(&self, object_key: &str, body: Vec<u8>) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = std::time::Duration::from_secs(1u64 << (attempt - 1));
                let jittered = crate::util::jitter_duration(backoff);
                tracing::warn!(
                    "S3 PUT retry {}/{} ({}s backoff) for '{}'",
                    attempt,
                    MAX_RETRIES,
                    jittered.as_secs_f64(),
                    object_key,
                );
                tokio::time::sleep(jittered).await;
            }

            // Re-sign the URL on each attempt so the presigned URL is always
            // fresh. Previously the URL was signed once before the retry loop,
            // which meant later retries could fail with HTTP 403 "Request has
            // expired" after the 60s (now 300s) expiry elapsed.
            let action = self
                .bucket_url
                .put_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);

            let resp = match self.client.put(url.clone()).body(body.clone()).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = anyhow::anyhow!("S3 PUT request error for '{}': {}", object_key, reqwest_err_no_url(e));
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
                "S3 PUT for '{}' returned HTTP {}: {}",
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
                "S3 PUT for '{}' failed after {} retries",
                object_key,
                MAX_RETRIES
            )
        }))
    }

    /// Download content from an S3 object key with retry on transient failures.
    async fn get_object(&self, object_key: &str) -> Result<Option<Vec<u8>>> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = std::time::Duration::from_secs(1u64 << (attempt - 1));
                tokio::time::sleep(crate::util::jitter_duration(backoff)).await;
            }
            // Re-sign on each attempt (URL may expire across retries).
            let action = self
                .bucket_url
                .get_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);
            let resp = match self.client.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = anyhow::anyhow!("S3 GET request error for '{}': {}", object_key, reqwest_err_no_url(e));
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
                    "S3 GET for '{}' returned HTTP {}: {}",
                    object_key, status, text
                );
                if attempt < MAX_RETRIES && status.is_server_error() {
                    last_error = Some(err);
                    continue;
                }
                return Err(err);
            }
            // Same no-URL treatment as the send() errors above: a body-read
            // failure is still a reqwest error carrying the presigned URL, and
            // `.context()` would retain it as the anyhow error's source —
            // visible wherever the chain is printed with `{:?}`.
            let bytes = resp.bytes().await.map_err(|e| {
                anyhow::anyhow!("Failed to read S3 response body: {}", reqwest_err_no_url(e))
            })?;
            return Ok(Some(bytes.to_vec()));
        }
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("S3 GET for '{}' failed after {} retries", object_key, MAX_RETRIES)
        }))
    }

    /// Delete an S3 object key (idempotent) with retry on transient failures.
    async fn delete_object(&self, object_key: &str) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = std::time::Duration::from_secs(1u64 << (attempt - 1));
                tokio::time::sleep(crate::util::jitter_duration(backoff)).await;
            }
            let action = self
                .bucket_url
                .delete_object(Some(&self.credentials), object_key.as_ref());
            let url = self.sign_url(action);
            let resp = match self.client.delete(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    let err =
                        anyhow::anyhow!("S3 DELETE request error for '{}': {}", object_key, reqwest_err_no_url(e));
                    if attempt < MAX_RETRIES {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            // 204 No Content or 404 = success (deleted or doesn't exist)
            if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(());
            }
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            let err = anyhow::anyhow!(
                "S3 DELETE for '{}' returned HTTP {}: {}",
                object_key, status, text
            );
            if attempt < MAX_RETRIES && status.is_server_error() {
                last_error = Some(err);
                continue;
            }
            return Err(err);
        }
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("S3 DELETE for '{}' failed after {} retries", object_key, MAX_RETRIES)
        }))
    }
}

/// Result of loading one checkpoint object (main or .tmp).
///
/// - `Valid((cp, bytes))`: object existed, parsed, and passed the version check.
///   `bytes` is the raw object body, retained verbatim so `promote_tmp_to_main`
///   can re-write it without re-serializing (avoiding field-ordering drift).
/// - `Corrupt`: object existed but failed to parse (recoverable — try the
///   other key, clean up the corrupt one).
/// - `Absent`: object did not exist.
///
/// Fatal conditions (UTF-8 failure, empty body, version < 2) propagate as `Err`
/// from `try_load_checkpoint` — they halt startup rather than falling through,
/// matching the previous behavior.
enum LoadResult {
    Valid((Checkpoint, Vec<u8>)),
    Corrupt,
    Absent,
}

impl S3Store {
    /// Load and parse a single checkpoint object (main or `.tmp`).
    ///
    /// Centralizes the version check + empty check + UTF-8 check that the old
    /// `load()` inlined twice (once for main, once for tmp). Bails (returns
    /// `Err`) on fatal conditions; returns `LoadResult::Corrupt` for a parse
    /// error so the caller can fall through to the other key.
    async fn try_load_checkpoint(
        &self,
        key: &str,
        what: &'static str,
    ) -> Result<LoadResult> {
        let bytes = match self.get_object(key).await? {
            Some(b) => b,
            None => return Ok(LoadResult::Absent),
        };
        let content = String::from_utf8(bytes.clone())
            .with_context(|| format!("S3 {} checkpoint '{}' is not valid UTF-8", what, key))?;
        if content.trim().is_empty() {
            bail!(
                "S3 {} checkpoint '{}' is empty — remove it manually and restart",
                what, key
            );
        }
        match serde_json::from_str::<Checkpoint>(&content) {
            Ok(cp) => {
                // Version check: old checkpoints (version < 2) may have
                // last_ddl_id = 0 (missing field defaulted), which would
                // cause the DDL poller to re-apply ALL DDLs from id 1 —
                // including non-idempotent DDLs (DROP TABLE, RENAME).
                // Refuse to start; require --fresh for safety.
                if cp.version < CURRENT_CHECKPOINT_VERSION {
                    bail!(
                        "S3 {} checkpoint '{}' is version {} (current is {}). \
                         This is likely a checkpoint from an older pipeline version. \
                         The last_ddl_id field may be unreliable. \
                         Run with --fresh to re-snapshot from scratch, or \
                         manually set version={} in the checkpoint JSON if you \
                         are certain no DDL re-execution will occur.",
                        what, key, cp.version, CURRENT_CHECKPOINT_VERSION,
                        CURRENT_CHECKPOINT_VERSION,
                    );
                }
                Ok(LoadResult::Valid((cp, bytes)))
            }
            Err(e) => {
                tracing::error!(
                    "S3 {} checkpoint '{}' is corrupted (parse error: {})",
                    what, key, e
                );
                Ok(LoadResult::Corrupt)
            }
        }
    }

    /// Promote a `.tmp` checkpoint to the main key: PUT main (overwriting any
    /// stale one), then DELETE `.tmp`. The PUT uses the original raw bytes so
    /// the promoted main is byte-identical to tmp. If the PUT fails, the tmp is
    /// left in place so the next `load()` can retry recovery (non-fatal — the
    /// returned Checkpoint is still valid in-memory).
    async fn promote_tmp_to_main(&self, tmp_bytes: &[u8], tmp_key: &str) {
        match self.put_object(&self.key, tmp_bytes.to_vec()).await {
            Ok(()) => {
                let _ = self.delete_object(tmp_key).await;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to promote tmp checkpoint to main (left in place for retry): {e}"
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl CheckpointStore for S3Store {
    fn store_name(&self) -> &str {
        "s3"
    }

    async fn verify(&self) -> Result<()> {
        self.verify_bucket().await
    }

    async fn load(&self) -> Result<Checkpoint> {
        // A crash between save()'s `.tmp` PUT and the `main` overwrite
        // leaves `main` older than `.tmp`. The old `load()` returned `main`
        // whenever it parsed, ignoring the newer `.tmp`. Now we load both,
        // compare via Checkpoint::is_newer_than, and promote the newer one.
        // Fatal conditions (UTF-8/empty/version<2) still bail; corrupt-but-
        // recoverable (parse err) falls through to the other key, mirroring
        // the previous recovery semantics.

        // Returns (existed, parsed). `parsed` is None if the object was absent
        // OR existed-but-corrupt (recoverable); Err is reserved for fatal cases.
        let main = self.try_load_checkpoint(&self.key, "main").await?;
        let tmp_key = format!("{}.tmp", self.key);
        let tmp = self.try_load_checkpoint(&tmp_key, "tmp").await?;

        match (main, tmp) {
            (LoadResult::Valid(m), LoadResult::Absent) => {
                tracing::info!(
                    "Loaded S3 checkpoint: lsn={}, snapshot={}, ddl_id={}",
                    m.0.last_committed_lsn, m.0.snapshot_done, m.0.last_ddl_id
                );
                Ok(m.0)
            }
            (LoadResult::Valid(m), LoadResult::Corrupt) => {
                // tmp was corrupt — clean it up, return main.
                tracing::warn!(
                    "S3 tmp checkpoint '{}' is corrupt — cleaning up, returning main",
                    tmp_key
                );
                let _ = self.delete_object(&tmp_key).await;
                tracing::info!(
                    "Loaded S3 checkpoint: lsn={}, snapshot={}, ddl_id={}",
                    m.0.last_committed_lsn, m.0.snapshot_done, m.0.last_ddl_id
                );
                Ok(m.0)
            }
            (LoadResult::Valid(m), LoadResult::Valid(t)) => {
                // Both main and tmp are valid. Pick the newer.
                if t.0.is_newer_than(&m.0) {
                    tracing::warn!(
                        "S3 tmp checkpoint is newer than main (crash between tmp-PUT and \
                         main-overwrite in save()) — promoting tmp. \
                         main: lsn={}, ddl_id={}; tmp: lsn={}, ddl_id={}",
                        m.0.last_committed_lsn, m.0.last_ddl_id,
                        t.0.last_committed_lsn, t.0.last_ddl_id,
                    );
                    self.promote_tmp_to_main(&t.1, &tmp_key).await;
                    tracing::info!(
                        "Loaded S3 checkpoint (from tmp): lsn={}, ddl_id={}",
                        t.0.last_committed_lsn, t.0.last_ddl_id
                    );
                    Ok(t.0)
                } else {
                    // tmp is older or equal — stale leftover from a prior save()
                    // that already overwrote main. Clean up and return main.
                    tracing::info!(
                        "S3 tmp checkpoint is not newer than main (lsn main={},tmp={}, \
                         ddl_id main={},tmp={}) — cleaning up stale tmp, returning main",
                        m.0.last_committed_lsn, t.0.last_committed_lsn,
                        m.0.last_ddl_id, t.0.last_ddl_id,
                    );
                    let _ = self.delete_object(&tmp_key).await;
                    Ok(m.0)
                }
            }
            (LoadResult::Absent, LoadResult::Valid(t)) => {
                // Main never written (e.g. first save after --fresh crashed mid-save).
                tracing::info!(
                    "Recovered checkpoint from S3 tmp key '{}': lsn={}",
                    tmp_key, t.0.last_committed_lsn
                );
                self.promote_tmp_to_main(&t.1, &tmp_key).await;
                Ok(t.0)
            }
            (LoadResult::Absent, LoadResult::Absent) => {
                // Both absent — genuinely fresh start.
                tracing::info!(
                    "No S3 checkpoint at '{}' (main + tmp absent), starting fresh",
                    self.key,
                );
                Ok(Checkpoint::default())
            }
            (LoadResult::Corrupt, LoadResult::Valid(t)) => {
                // Main corrupt, tmp valid — recover from tmp.
                tracing::warn!(
                    "S3 main checkpoint '{}' is corrupt — recovering from tmp",
                    self.key
                );
                self.promote_tmp_to_main(&t.1, &tmp_key).await;
                Ok(t.0)
            }
            (LoadResult::Corrupt, LoadResult::Absent) | (LoadResult::Corrupt, LoadResult::Corrupt) => {
                // Main existed but was corrupt, and tmp is absent/corrupt.
                bail!(
                    "S3 checkpoint '{}' is corrupted and no valid tmp fallback found. \
                     Restore from backup or run with --fresh.",
                    self.key,
                )
            }
            (LoadResult::Absent, LoadResult::Corrupt) => {
                // Main absent, tmp corrupt. Clean up tmp and start fresh.
                tracing::warn!(
                    "S3 main absent and tmp '{}' is corrupt — cleaning up tmp, starting fresh",
                    tmp_key
                );
                let _ = self.delete_object(&tmp_key).await;
                Ok(Checkpoint::default())
            }
        }
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        // Acquire the save_lock to serialize concurrent save() calls.
        // Without this, two concurrent saves (e.g. background saver + DDL poller)
        // can race: the stale (lower LSN/ddl_id) PUT might land AFTER the
        // newer one on S3, causing checkpoint regression.
        let _guard = self.save_lock.lock().await;
        self.save_locked(checkpoint).await
    }

    /// Capture-and-save atomically: the checkpoint is cloned while the store
    /// write lock is held, so a concurrent newer save can never be
    /// overwritten by this one's older capture (the plain `save` path's
    /// clone-then-lock window regresses the durable checkpoint when the
    /// background saver's capture is preempted by the DDL poller's save).
    async fn save_captured<'a>(
        &self,
        capture: Box<dyn FnOnce() -> Checkpoint + Send + 'a>,
    ) -> Result<Checkpoint> {
        let _guard = self.save_lock.lock().await;
        let cp = capture();
        self.save_locked(&cp).await?;
        Ok(cp)
    }

    async fn delete(&self) -> Result<()> {
        self.delete_object(&self.key).await?;
        // Also clean up any orphaned tmp key
        let tmp_key = format!("{}.tmp", self.key);
        let _ = self.delete_object(&tmp_key).await;
        Ok(())
    }
}

impl S3Store {
    /// Checkpoint write body, run with `save_lock` held (by `save` or
    /// `save_captured`).
    async fn save_locked(&self, checkpoint: &Checkpoint) -> Result<()> {
        // Ensure version is current before writing.
        if checkpoint.version < CURRENT_CHECKPOINT_VERSION {
            // This should not happen in normal operation (version is set on
            // save), but if an old Checkpoint struct is passed, upgrade it.
            let mut cp = checkpoint.clone();
            cp.version = CURRENT_CHECKPOINT_VERSION;
            let content = serde_json::to_string_pretty(&cp)
                .context("Failed to serialize checkpoint")?;
            let bytes = content.into_bytes();
            let tmp_key = format!("{}.tmp", self.key);
            self.put_object(&tmp_key, bytes.clone()).await?;
            self.put_object(&self.key, bytes).await?;
            let _ = self.delete_object(&tmp_key).await;
            tracing::debug!(
                "S3 checkpoint saved to '{}' (lsn={}, version={})",
                self.key, cp.last_committed_lsn, cp.version
            );
            return Ok(());
        }

        let content =
            serde_json::to_string_pretty(checkpoint).context("Failed to serialize checkpoint")?;
        let bytes = content.into_bytes();

        // Write to temp key first
        let tmp_key = format!("{}.tmp", self.key);
        self.put_object(&tmp_key, bytes.clone()).await?;

        // Promote to main key (overwrite)
        self.put_object(&self.key, bytes).await?;

        // Clean up temp key
        let _ = self.delete_object(&tmp_key).await;

        tracing::debug!(
            "S3 checkpoint saved to '{}' (lsn={})",
            self.key,
            checkpoint.last_committed_lsn
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the S3 checkpoint backend's startup `verify()` (HEAD-bucket).
    //!
    //! We mock the S3 server with `wiremock` — it doesn't validate AWS
    //! signatures, but for these tests we only care that the right HTTP
    //! method is dispatched against the right path and that the response
    //! status is classified correctly by `verify_bucket`.
    use super::*;
    use crate::config::CheckpointConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Build a CheckpointConfig pointing at `endpoint` with a fixed test
    /// bucket / region / credentials. Uses path-style addressing so the
    /// mock server receives the bucket name as the URL path.
    fn make_s3_config(endpoint: String) -> CheckpointConfig {
        CheckpointConfig {
            store_type: "s3".to_string(),
            path: "cp.json".to_string(),
            save_interval_ms: 10000,
            bucket: Some("test-bucket".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some(endpoint),
            path_style: true,
            access_key_id: Some("AKIATEST".to_string()),
            secret_access_key: Some("secrettest".to_string()),
        }
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_ok() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let config = make_s3_config(server.uri());
        let store = S3Store::new(&config).expect("S3Store::new should succeed");
        // Should be Ok — bucket "exists".
        store
            .verify()
            .await
            .expect("verify() should succeed on HTTP 200");
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let config = make_s3_config(server.uri());
        let store = S3Store::new(&config).unwrap();
        let err = store.verify().await.unwrap_err().to_string();
        assert!(
            err.contains("does not exist"),
            "404 should be classified as missing bucket, got: {err}"
        );
        assert!(
            err.contains("test-bucket"),
            "error should mention the bucket name, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_forbidden() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let config = make_s3_config(server.uri());
        let store = S3Store::new(&config).unwrap();
        let err = store.verify().await.unwrap_err().to_string();
        assert!(
            err.contains("Access denied"),
            "403 should be classified as access denied, got: {err}"
        );
        assert!(
            err.contains("access_key_id") || err.contains("secret_access_key"),
            "error should hint at credentials, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;
        let config = make_s3_config(server.uri());
        let store = S3Store::new(&config).unwrap();
        let err = store.verify().await.unwrap_err().to_string();
        assert!(
            err.contains("HTTP 500"),
            "5xx should surface status code, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_unreachable() {
        // Use port 1 on loopback — ports <1024 are privileged, so on any
        // non-root test process this returns ECONNREFUSED immediately. This
        // is the same error class we'd see if a real S3 endpoint were
        // unreachable (DNS failure, wrong host, etc.).
        let config = make_s3_config("http://127.0.0.1:1".to_string());
        let store = S3Store::new(&config).unwrap();
        let err = store.verify().await.unwrap_err().to_string();
        assert!(
            err.contains("Cannot reach"),
            "network error should be classified as unreachable, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_s3_verify_bucket_method_is_head() {
        // Sanity check: ensure verify() actually issues a HEAD (not GET/PUT).
        // If a future refactor accidentally changes the verb, the path-matched
        // mock below will not match and we'll get a 404 from wiremock's
        // default fallback, surfacing the bug.
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let config = make_s3_config(server.uri());
        let store = S3Store::new(&config).unwrap();
        store.verify().await.expect("HEAD should hit the mock");
    }
}
