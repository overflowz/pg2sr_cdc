//! pg2sr_cdc — PostgreSQL to StarRocks CDC pipeline.

mod cdc;
mod checkpoint;
mod config;
mod ddl;
mod ddl_poller;
mod metrics_server;
mod pg_catalog;
mod pg_lock;
mod pipeline;
mod sink;
mod snapshot;
mod sr_connection;
mod sr_sql;
mod types;
mod util;
mod wal_buffer;

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use fs2::FileExt;
use tokio::sync::watch;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::cdc::CdcConsumer;
use crate::checkpoint::create_checkpoint_store;
use crate::checkpoint::lock_checkpoint;
use crate::checkpoint::Checkpoint;
use crate::checkpoint::CheckpointStore;
use crate::config::{Config, PgTlsConnector};
use crate::pipeline::Pipeline;
use crate::sink::SrSink;
use crate::util::AbortOnDrop;

/// Install the ring CryptoProvider at process start so rustls doesn't panic
/// when both aws-lc-rs and ring are compiled in (cargo feature unification from
/// transitive deps like pgwire-replication → tokio-rustls default features).
/// Idempotent — subsequent calls are no-ops.
fn init_tls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

// ── Size-capped rotating log file writer ───────────────────────────────────

/// Resolve a relative path against the config file's parent directory.
///
/// A systemd unit with `WorkingDirectory=/` and a relative `logs/{name}.log`
/// or default `pg2sr_cdc_checkpoint.json` would otherwise resolve against `/`
/// — writing logs to `/logs/<name>.log` and the checkpoint to the filesystem
/// root (or failing with permission denied). Absolute paths pass through
/// unchanged. If the config path has no usable parent (e.g. `config.toml`
/// without a directory), the path stays relative (resolves against CWD) —
/// this matches the prior behavior for the legacy bare-filename config case.
fn resolve_against_config_dir(path: &str, config_path: &str) -> PathBuf {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    let cfg = std::path::Path::new(config_path);
    if let Some(parent) = cfg.parent() {
        if !parent.as_os_str().is_empty() {
            return parent.join(p);
        }
    }
    p.to_path_buf()
}

/// A `MakeWriter` implementation that writes WARN+ERROR logs to a file,
/// rotating it when the current file exceeds `max_size` bytes.
///
/// Rotation policy: when a write would exceed `max_size`, the current file
/// (`logs/pg2sr_cdc.log`) is renamed to `.log.1` (overwriting any previous
/// `.log.1`), and a fresh `.log` file starts. This caps total disk usage at
/// approximately 2 × `max_size` (current + one rotated backup).
#[derive(Clone)]
struct SizeCappedLogWriter {
    path: PathBuf,
    max_size: u64,
    written: Arc<AtomicU64>,
    file: Arc<Mutex<Option<std::fs::File>>>,
}

impl SizeCappedLogWriter {
    fn new(path: impl Into<PathBuf>, max_size: u64) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Probe current file size, tolerating a missing file (fresh start).
        let current_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path,
            max_size,
            written: Arc::new(AtomicU64::new(current_size)),
            file: Arc::new(Mutex::new(None)),
        })
    }

    /// Open or reopen the log file handle for appending.
    fn open_file(&self) -> io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
    }

    /// Rename the current log to `.log.1` and reset the size counter.
    /// Called when the current file is about to exceed `max_size`.
    fn rotate(&self) -> io::Result<()> {
        // Close the cached handle before renaming.
        {
            let mut guard = self.file.lock().unwrap_or_else(|e| {
                eprintln!("Log mutex poisoned in rotate, recovering");
                e.into_inner()
            });
            *guard = None;
        }
        let backup = self.path.with_extension("log.1");
        let _ = std::fs::remove_file(&backup);
        if self.path.exists() {
            std::fs::rename(&self.path, &backup)?;
        }
        self.written.store(0, Ordering::Relaxed);
        Ok(())
    }
}

impl Write for SizeCappedLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let prev = self.written.load(Ordering::Relaxed);
        // If this write would push us over the cap, rotate first.
        if prev.saturating_add(buf.len() as u64) > self.max_size {
            if let Err(e) = self.rotate() {
                // Rotation failed — try to write anyway, don't lose the log.
                eprintln!("Failed to rotate log file: {e}");
            }
        }

        // Get or lazily open the cached file handle.
        let mut guard = self.file.lock().unwrap_or_else(|e| {
            eprintln!("Log mutex poisoned in write, recovering");
            e.into_inner()
        });
        if guard.is_none() {
            *guard = Some(self.open_file()?);
        }
        let file = guard.as_mut().unwrap();
        let n = file.write(buf)?;
        file.flush()?;
        self.written.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut file) = *self.file.lock().unwrap_or_else(|e| {
            eprintln!("Log mutex poisoned in flush, recovering");
            e.into_inner()
        }) {
            file.flush()?;
        }
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SizeCappedLogWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Check whether a `pg_create_logical_replication_slot` error is just
/// "already exists" (non-fatal on non-fresh starts) vs a real failure.
/// Traverses the error source chain since `tokio_postgres::Error::Display`
/// only outputs "db error" for Db variants — the actual PG message lives
/// in the source chain.
fn slot_already_exists_err(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: &dyn std::error::Error = err;
    loop {
        let msg = format!("{current}");
        if msg.contains("42710") || msg.contains("already exists") {
            return true;
        }
        match current.source() {
            Some(src) => current = src,
            None => return false,
        }
    }
}

/// Detect PostgreSQL "replication slot is active for PID X" errors.
///
/// SQLSTATE 55006 (`object_not_in_prerequisite_state`) is returned when a
/// second client tries to attach to an already-active logical replication
/// slot — i.e. another pipeline instance is already streaming from the
/// same slot. This mirrors the source-chain-walking pattern of
/// `slot_already_exists_err` because tokio-postgres buries the SQLSTATE and
/// human-readable message at varying depths of the error chain.
fn is_slot_in_use_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: &dyn std::error::Error = err;
    loop {
        let msg = format!("{current}");
        if msg.contains("55006")
            || msg.contains("is active for PID")
            || msg.contains("is already in use")
        {
            return true;
        }
        match current.source() {
            Some(src) => current = src,
            None => return false,
        }
    }
}

/// Detect PostgreSQL "publication ... does not exist" errors.
///
/// SQLSTATE 42704 (`undefined_object`) is returned by `START_REPLICATION ...
/// (publication 'X')` when the named publication is absent from the database
/// the replication connection is attached to. This mirrors the source-chain-walking
/// pattern of `slot_already_exists_err` because tokio-postgres buries the
/// SQLSTATE and human-readable message at varying depths of the error chain.
///
/// To avoid false positives on unrelated 42704 errors (e.g. `relation "Y" does
/// not exist`), this matcher requires *both* the SQLSTATE *and* the word
/// `publication` to be present somewhere in the chain.
fn is_publication_missing_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: &dyn std::error::Error = err;
    loop {
        let msg = format!("{current}");
        if msg.contains("42704") && msg.contains("publication") {
            return true;
        }
        // Also catch the text-only form some pgwire versions surface without
        // the SQLSTATE code, as long as it names a publication.
        if msg.contains("publication")
            && (msg.contains("does not exist") || msg.contains("not found"))
        {
            return true;
        }
        match current.source() {
            Some(src) => current = src,
            None => return false,
        }
    }
}

/// Build an actionable error message for the missing-publication case.
///
/// Publications are **database-local** objects in PostgreSQL: a publication
/// created in `postgres` is invisible to a replication connection that attaches
/// to `example_db`. The message therefore names the configured database
/// and gives the exact `CREATE PUBLICATION` command to run *in that database*,
/// plus the `manage_publication = true` config alternative.
fn missing_publication_error_msg(pub_name: &str, db_name: &str) -> String {
    format!(
        "publication \"{pub_name}\" does not exist in database \"{db_name}\". \
         PostgreSQL publications are database-local objects — a publication created \
         in a different database (e.g. `postgres`) is invisible here. \
         Fix ONE of the following:\n  \
         (a) Create the publication in the configured database:\n      \
             CREATE PUBLICATION {pub_name} FOR ALL TABLES \
             WITH (publish='insert, update, delete, truncate');\n  \
         (b) Or let the pipeline manage it: set manage_publication = true \
             under [postgres] in the config."
    )
}

/// Check whether a PG error is a permanent (non-recoverable) error that should
/// cause immediate process exit instead of retrying. These include:
/// - 28P01: invalid_password (wrong password)
/// - 28000: invalid_authorization_specification (unknown user)
/// - 42501: insufficient_privilege (permission denied / not superuser)
/// - 0A000: feature_not_supported (missing superuser capability)
fn is_permanent_pg_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let error_codes = ["28P01", "28000", "42501", "0A000"];
    let mut current: &dyn std::error::Error = err;
    loop {
        let msg = format!("{current}");
        for code in &error_codes {
            if msg.contains(code) {
                return true;
            }
        }
        match current.source() {
            Some(src) => current = src,
            None => return false,
        }
    }
}

/// Compute the LSN to ACK to PostgreSQL during an idle period (empty batch).
///
/// Two concepts must be kept separate:
///
/// **Checkpoint LSN** — the LSN we resume from on crash. Must only advance past
///   flushed data. The caller advances it to `checkpoint_lsn` (which excludes
/// KeepAlive `wal_end` per the fix).
///
/// **PG ACK LSN** (`confirmed_flush_lsn`) — tells PG it can discard WAL. Can
///   safely advance to `wal_end` during idle **when no buffered events are
///   pending** (`!has_pending`), because the walsender has already decoded ALL
///   WAL up to `wal_end` and delivered any replicated-table transactions. The
///   gap between `checkpoint_lsn` and `wal_end` contained only non-replicated
///   system WAL → no data loss on crash (the pipeline resumes from
///   `checkpoint_lsn` and PG re-sends from `confirmed_flush_lsn = wal_end`; the
///   skipped range has no replicated data).
///
/// When `has_pending` is true (events buffered, not yet flushed), ACK gates on
/// `saved` (the durably-saved checkpoint) to protect un-flushed data.
fn idle_ack_lsn(checkpoint_lsn: i64, wal_end: i64, saved: u64, has_pending: bool) -> u64 {
    if !has_pending && wal_end > 0 {
        // Pipeline is idle (no buffered events). ACK to the WAL head — the
        // walsender has decoded everything up to here; only system WAL
        // (autovacuum, checkpoints) would be discarded. No data loss.
        wal_end as u64
    } else {
        // Events are buffered but not yet flushed (e.g. a transaction arrived
        // in the same batch as the KeepAlive, or a previous batch is still in
        // the pipeline). Gate the ACK on the durable checkpoint so PG doesn't
        // discard WAL that the pipeline still needs to re-deliver on crash.
        //
        // Use the checkpoint LSN (which may be ahead of `saved` if the saver
        // hasn't persisted the latest advance yet) — ACK only up to what's
        // been durably saved.
        (checkpoint_lsn as u64).min(saved)
    }
}

/// Outcome of a flush attempt.
enum FlushOutcome {
    /// Flush succeeded (every table either stream-loaded or durably
    /// appended to the wal buffer — checkpoint may advance).
    Flushed,
    /// A failed wal-buffer APPEND (local disk full / S3 down) — data could
    /// not be made durable, so reconnect is the only option. Shutdown
    /// mid-flush also lands here; the main loop sees the shutdown flag and
    /// exits instead of reconnecting (the dropped batch replays from the
    /// checkpoint on next start). Ordinary per-table Stream Load failures
    /// NEVER reach here: they are durably buffered and the flush returns Ok.
    Failed(anyhow::Error),
}

/// Thin wrapper over `flush_transaction`. Per-table flush failures (Stream
/// Load error, task panic, flush-timeout abort) no longer defer or fail:
/// the failed table's unconfirmed rows are durably appended to the
/// wal_buffer (fsynced/PUT-confirmed) and `flush_transaction` returns Ok —
/// there is nothing to retry and no bounded wait. Err is reserved for a
/// failed buffer APPEND (→ reconnect). Shutdown mid-flush also returns Err;
/// the main loop exits instead of reconnecting.
async fn flush_batch(pipeline: &mut Pipeline, commit_lsn: i64) -> FlushOutcome {
    match pipeline.flush_transaction(commit_lsn).await {
        Ok(()) => FlushOutcome::Flushed,
        Err(e) => FlushOutcome::Failed(e),
    }
}

/// Runtime metrics for observability.
struct Metrics {
    total_rows: Arc<AtomicU64>,
    total_batches: Arc<AtomicU64>,
    total_flush_ms: Arc<AtomicU64>,
    last_lsn: Arc<AtomicU64>,
    reconnect_count: Arc<AtomicU64>,
    /// Shared with Pipeline to track current pending memory bytes.
    pending_bytes: Arc<AtomicUsize>,
    /// DDL events applied (from DDL poller, via main.rs shared state).
    ddl_applied: Arc<AtomicU64>,
    /// DDL events skipped (from DDL poller).
    ddl_skipped: Arc<AtomicU64>,
    /// Error count (from flush failures, DDL failures, checkpoint-save failures, etc.)
    error_count: Arc<AtomicU64>,
    /// Epoch seconds of the last completed CDC batch (0 = none yet). /health
    /// uses it to distinguish "idle" from "wedged": a pipeline stuck in the
    /// reconnect loop or with every replay failing never advances this.
    last_progress_at: Arc<AtomicU64>,
    /// Epoch seconds of the last successful checkpoint save.
    /// Updated by the background checkpoint saver.
    last_saved_at: Arc<AtomicU64>,
    // New: snapshot phase tracking
    snapshot_in_progress: Arc<AtomicU64>,
    snapshot_tables_done: Arc<AtomicU64>,
    // New: PG WAL lag tracking (queried periodically, cached)
    pg_wal_lag_bytes: Arc<AtomicU64>,
    confirmed_flush_lsn: Arc<AtomicU64>,
    checkpoint_lag_bytes: Arc<AtomicU64>,
    // Wal-buffer observability (durable write-pause storage)
    wal_buffered_events: Arc<AtomicU64>,
    wal_buffered_bytes: Arc<AtomicU64>,
    wal_replayed_events: Arc<AtomicU64>,
    wal_replay_failures: Arc<AtomicU64>,
    /// Current segment count across all buffered tables (gauge, manager-owned).
    wal_buffer_segments: Arc<AtomicU64>,
    /// Current buffered byte count across all segments (gauge, manager-owned).
    wal_buffer_bytes: Arc<AtomicU64>,
    /// Segments auto-quarantined (corrupt) — monotonic counter (manager-owned).
    wal_buffer_quarantined_segments: Arc<AtomicU64>,
    // Throughput interval-rate tracking
    prev_rows: AtomicU64,
    prev_summary_at: std::sync::Mutex<Instant>,
}

impl Metrics {
    #[allow(clippy::too_many_arguments)]
    fn new(
        pending_bytes: Arc<AtomicUsize>,
        ddl_applied: Arc<AtomicU64>,
        ddl_skipped: Arc<AtomicU64>,
        error_count: Arc<AtomicU64>,
        last_saved_at: Arc<AtomicU64>,
        snapshot_in_progress: Arc<AtomicU64>,
        snapshot_tables_done: Arc<AtomicU64>,
        pg_wal_lag_bytes: Arc<AtomicU64>,
        confirmed_flush_lsn: Arc<AtomicU64>,
        checkpoint_lag_bytes: Arc<AtomicU64>,
        wal_buffered_events: Arc<AtomicU64>,
        wal_buffered_bytes: Arc<AtomicU64>,
        wal_replayed_events: Arc<AtomicU64>,
        wal_replay_failures: Arc<AtomicU64>,
        wal_buffer_segments: Arc<AtomicU64>,
        wal_buffer_bytes: Arc<AtomicU64>,
        wal_buffer_quarantined_segments: Arc<AtomicU64>,
        started: Instant,
    ) -> Self {
        Self {
            total_rows: Arc::new(AtomicU64::new(0)),
            total_batches: Arc::new(AtomicU64::new(0)),
            total_flush_ms: Arc::new(AtomicU64::new(0)),
            last_lsn: Arc::new(AtomicU64::new(0)),
            reconnect_count: Arc::new(AtomicU64::new(0)),
            pending_bytes,
            ddl_applied,
            ddl_skipped,
            error_count,
            last_saved_at,
            last_progress_at: Arc::new(AtomicU64::new(0)),
            snapshot_in_progress,
            snapshot_tables_done,
            pg_wal_lag_bytes,
            confirmed_flush_lsn,
            checkpoint_lag_bytes,
            wal_buffered_events,
            wal_buffered_bytes,
            wal_replayed_events,
            wal_replay_failures,
            wal_buffer_segments,
            wal_buffer_bytes,
            wal_buffer_quarantined_segments,
            prev_rows: AtomicU64::new(0),
            prev_summary_at: std::sync::Mutex::new(started),
        }
    }

    fn record_batch(&self, rows: u64, flush_ms: u64, lsn: u64) {
        self.total_rows.fetch_add(rows, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_flush_ms.fetch_add(flush_ms, Ordering::Relaxed);
        self.last_lsn.store(lsn, Ordering::Relaxed);
        // Batch completed → the consumer is making progress (used by /health
        // to tell a wedged reconnect loop from an idle pipeline).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_progress_at.store(now, Ordering::Relaxed);
    }

    fn report(&self, started: Instant) {
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let rows = self.total_rows.load(Ordering::Relaxed);
        let batches = self.total_batches.load(Ordering::Relaxed);
        let total_ms = self.total_flush_ms.load(Ordering::Relaxed);
        let lsn = self.last_lsn.load(Ordering::Relaxed);
        let pb = self.pending_bytes.load(Ordering::Relaxed);
        let da = self.ddl_applied.load(Ordering::Relaxed);
        let ds = self.ddl_skipped.load(Ordering::Relaxed);
        let pb_mb = pb as f64 / (1024.0 * 1024.0);
        let reconnects = self.reconnect_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let wal_lag = self.pg_wal_lag_bytes.load(Ordering::Relaxed);
        // Unified: include errors + wal_lag that were previously missing.
        tracing::debug!(
            "stats rows={rows} batches={batches} rows/s={:.1} avg_flush={:.1}ms \
             lsn={lsn} reconnects={reconnects} pending_mem={pb_mb:.1}MB \
             ddl_applied={da} ddl_skipped={ds} errors={errors} wal_lag_bytes={wal_lag}",
            rows as f64 / elapsed,
            if batches > 0 {
                total_ms as f64 / batches as f64
            } else {
                0.0
            },
        );
    }

    /// Periodic health summary (every 5min) with cumulative + interval stats.
    fn report_summary(&self, started: Instant) {
        let elapsed = started.elapsed();
        let rows = self.total_rows.load(Ordering::Relaxed);
        let batches = self.total_batches.load(Ordering::Relaxed);
        let total_ms = self.total_flush_ms.load(Ordering::Relaxed);
        let da = self.ddl_applied.load(Ordering::Relaxed);
        let ds = self.ddl_skipped.load(Ordering::Relaxed);
        let reconnects = self.reconnect_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let lsn = self.last_lsn.load(Ordering::Relaxed);
        let pb = self.pending_bytes.load(Ordering::Relaxed);
        let pb_mb = pb as f64 / (1024.0 * 1024.0);
        let wal_lag = self.pg_wal_lag_bytes.load(Ordering::Relaxed);
        let snap_ip = self.snapshot_in_progress.load(Ordering::Relaxed);
        let snap_td = self.snapshot_tables_done.load(Ordering::Relaxed);
        let last_saved = self.last_saved_at.load(Ordering::Relaxed);
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let save_lag = if last_saved > 0 && now_epoch > last_saved {
            now_epoch - last_saved
        } else {
            0
        };
        // Throughput: true interval rate (delta rows / delta time) instead of
        // the prior lifetime-average (total_rows / total_uptime) which was
        // always 0 during snapshot / first tick / low-volume CDC.
        let now = Instant::now();
        let prev_rows = self.prev_rows.swap(rows, Ordering::Relaxed);
        let prev_time = self
            .prev_summary_at
            .lock()
            .map(|mut t| {
                let old = *t;
                *t = now;
                old
            })
            .unwrap_or(started);
        let dt = now.duration_since(prev_time).as_secs_f64().max(0.001);
        let delta_rows = rows.saturating_sub(prev_rows);
        let throughput = delta_rows as f64 / dt;

        let avg_flush = if batches > 0 {
            total_ms as f64 / batches as f64
        } else {
            0.0
        };
        let wal_segs = self.wal_buffer_segments.load(Ordering::Relaxed);
        tracing::info!(
            "HEALTH SUMMARY uptime={} rows={} batches={} throughput={:.1} rows/s \
             avg_flush={:.1}ms ddl_applied={} ddl_skipped={} reconnects={} errors={} \
             checkpoint_save_lag={}s lsn={} pending_mem={:.1}MB wal_lag_bytes={} \
             snapshot_in_progress={} snapshot_tables_done={} wal_buffer_segments={}",
            crate::util::format_duration(elapsed.as_secs()),
            rows,
            batches,
            throughput,
            avg_flush,
            da,
            ds,
            reconnects,
            errors,
            save_lag,
            lsn,
            pb_mb,
            wal_lag,
            snap_ip,
            snap_td,
            wal_segs,
        );
    }
}

#[derive(Parser, Debug)]
#[command(name = "pg2sr_cdc", about = "PostgreSQL → StarRocks CDC pipeline")]
struct Args {
    #[arg(short = 'c', long, default_value = "config.toml")]
    config: String,
    #[arg(long)]
    fresh: bool,
    /// Override the pipeline name from config. Sets application_name, lock file,
    /// and log file name.
    #[arg(long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Must be called before any rustls-using code to select ring when both
    // aws-lc-rs and ring are compiled into the binary.
    init_tls();

    let args = Args::parse();

    // Load config first so we have the pipeline name for logging setup.
    let config_path = &args.config;
    let mut config = Config::from_file(config_path).context("Failed to load config")?;

    // Determine effective pipeline name: CLI --name > config name > default
    let pipeline_name = args
        .name
        .clone()
        .unwrap_or_else(|| config.pipeline.name.clone());
    crate::config::validate_pipeline_name(&pipeline_name).context("Invalid pipeline name")?;

    // Set application_name on PgConfig so all tokio-postgres connections
    // (DDL setup, snapshot, catalog queries) identify themselves.
    config.postgres.app_name = pipeline_name.clone();

    // Thread [pipeline].query_timeout_ms into the PG catalog query helpers
    // (previously a hardcoded 30s).
    crate::pg_catalog::set_catalog_query_timeout(config.pipeline.query_timeout_ms);

    // ── Single-instance lock via flock (keyed on SR database) ──
    // The lock is held for the process lifetime; the kernel releases it
    // automatically on exit/crash. No PID file, no stale cleanup needed.
    let lock_path = std::env::temp_dir().join(format!("pg2sr_{}.lock", config.starrocks.database));
    let lock_fd = std::fs::File::create(&lock_path).context(format!(
        "Failed to create lock file at {}",
        lock_path.display()
    ))?;
    lock_fd.try_lock_exclusive().context(format!(
        "Another pipeline instance is already replicating to SR database '{}'. Lock file: {}",
        config.starrocks.database,
        lock_path.display(),
    ))?;
    // fd stays open for the process lifetime — kernel releases flock on exit.

    // Set up dual-layer tracing:
    // - Console (stdout): INFO and above, controlled by RUST_LOG env
    // - File (logs/{name}.log): WARN and ERROR, size-capped rotation
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Size-capped rotating log file writer.
    // When the current log exceeds max_size (~50 MB), it's renamed to
    // .log.1 and a fresh file starts. Old .log.1 is deleted. Total cap ≈ 100 MB.
    // Resolve the relative "logs/{name}.log" against the config file's
    // parent dir so a systemd WorkingDirectory=/ doesn't write to /logs/<name>.log.
    let log_path = resolve_against_config_dir(&format!("logs/{pipeline_name}.log"), config_path);
    let log_writer = SizeCappedLogWriter::new(&log_path, 50 * 1024 * 1024).with_context(|| {
        format!(
            "Failed to create log directory for {} — check permissions/ownership of the \
             config directory",
            log_path.display()
        )
    })?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(log_writer);
    // Keep the guard alive for the program's lifetime — dropping it flushes
    // the non-blocking writer and shuts down the worker thread.
    let _log_guard = _guard;

    let console_layer = tracing_subscriber::fmt::layer().with_filter(env_filter);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(false)
        .with_filter(tracing_subscriber::filter::LevelFilter::WARN);

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    let cp_cfg = config.checkpoint.clone().unwrap_or_default();
    // The checkpoint path is user-configured and resolved relative to the
    // process CWD (as documented) — we do NOT re-resolve it against the config
    // dir, because that would change its meaning when the config lives in a
    // subdirectory (e.g. a config under `dev/` with path "x.json" →
    // "dev/x.json", breaking tooling that reads the documented key). Instead
    // warn loudly when it's relative so a systemd unit with WorkingDirectory=/
    // is caught at startup. (The auto-generated "logs/{name}.log" path above IS
    // resolved against the config dir, since it's pipeline-internal and benefits.)
    if !cp_cfg.path.is_empty() {
        let p = std::path::Path::new(&cp_cfg.path);
        if p.is_relative() {
            tracing::warn!(
                "checkpoint.path '{}' is relative — it resolves against the process \
                 working directory (CWD). For a systemd unit with WorkingDirectory=/, \
                 set an absolute path to avoid writing to the filesystem root.",
                cp_cfg.path,
            );
        }
    }
    let checkpoint_store: Arc<dyn CheckpointStore> =
        create_checkpoint_store(&cp_cfg).context("Failed to create checkpoint store")?;
    // Fail fast on misconfigured checkpoint backend (missing S3 bucket, wrong
    // creds, unreachable endpoint) BEFORE we touch Postgres / StarRocks. Without
    // this, a bad S3 config surfaces only at checkpoint_store.load() below — long
    // after tracing, flock, and possibly the --fresh teardown have run.
    checkpoint_store
        .verify()
        .await
        .context("Checkpoint store verification failed — refusing to start CDC")?;
    tracing::info!(
        "pg2sr_cdc starting (checkpoint store: {}, path: {})",
        checkpoint_store.store_name(),
        cp_cfg.path,
    );

    // Durable write-pause buffer: mirrors the checkpoint backend (same
    // store_type / bucket / creds — no new config). Root is
    // `<checkpoint.path>.buffer` (local dir or S3 key prefix). scan_existing
    // enumerates leftover segments from a previous run (crash mid-pause);
    // corrupt segments are AUTO-QUARANTINED (bytes preserved aside, counter
    // bumped) and only I/O errors are fatal.
    let wal_buffer = crate::wal_buffer::WalBufferManager::new(
        config.checkpoint.as_ref(),
        (config.pipeline.max_pending_memory_mb * 1024 * 1024) as u64,
    )
    .context("Failed to create wal buffer manager")?;
    // Hard byte cap for the durable buffer (0 = uncapped). Fail-closed in
    // append(): a permanently failing table can never fill the disk.
    wal_buffer.set_max_bytes(
        config
            .pipeline
            .wal_buffer_max_mb
            .saturating_mul(1024 * 1024),
    );
    // Monotonic quarantined-segment counter (manager-owned).
    let wal_buffer_quarantined = wal_buffer.quarantined_count();
    // --fresh tears the buffer down right after (purge_all below) — skip the
    // leftover scan entirely so the startup path never quarantines segments
    // that are about to be purged anyway.
    // Leftover rename sidecars from a previous run, seeded into the in-memory
    // rename map later (D3) so crash-recovered replays remap correctly.
    let leftover_renames: std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    > = if !args.fresh {
        let (leftover, leftover_renames) = wal_buffer
            .scan_existing()
            .await
            .context("wal buffer scan_existing failed — refusing to start CDC")?;
        if !leftover.is_empty() {
            tracing::warn!(
                "wal buffer: {} table(s) have leftover segments from a previous run: {:?} — \
                 they will be replayed once the DDL poller re-establishes write pauses \
                 (or purged immediately by --fresh)",
                leftover.len(),
                leftover,
            );
        }
        leftover_renames
    } else {
        // --fresh: nothing to restore (buffer was just purged).
        std::collections::HashMap::new()
    };

    // Build TLS connector for PG direct connections (DDL setup, snapshot, etc.)
    let pg_tls = PgTlsConnector::from_config(&config.postgres.tls)
        .context("Failed to configure PostgreSQL TLS")?;

    // ── Distributed single-instance guard (PG advisory locks) ──
    // The flock above is host-local; advisory locks extend the same
    // exclusion across hosts, keyed on the artifacts that are actually
    // shared: the SR database (mirroring the flock) and the checkpoint
    // store (its local path / S3 bucket+key identity). Session-scoped: a
    // crash or connection drop releases the lock automatically. Acquired
    // BEFORE the --fresh teardown below, so a cross-host --fresh cannot
    // stomp a live instance; zombie takeover still works (a dead session
    // released the lock). The replication-slot guard remains the
    // authoritative backstop for same-slot contention (PG 55006).
    let pg_lock_guard = crate::pg_lock::acquire(
        &config.postgres,
        &pg_tls,
        &cp_cfg,
        &[
            (
                "StarRocks database",
                format!("pg2sr_cdc_sr:{}", config.starrocks.database),
            ),
            (
                "checkpoint store",
                format!(
                    "pg2sr_cdc_ckpt:{}:{}:{}:{}",
                    cp_cfg.store_type,
                    cp_cfg.endpoint.clone().unwrap_or_default(),
                    cp_cfg.bucket.clone().unwrap_or_default(),
                    cp_cfg.path,
                ),
            ),
        ],
    )
    .await
    .context("Failed to acquire distributed single-instance lock")?;

    // ── Shutdown signaling via watch channel ──
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // ── Single-instance lock watchdog ──
    // The advisory lock lives on one PG session. If that session dies mid-run
    // PG releases the locks immediately, so cross-host exclusion disappears
    // while this process keeps writing — the exact scenario the guard exists
    // to prevent. Fail closed: log loudly and begin graceful shutdown instead
    // of running on as a second writer. Spawned here — before the snapshot
    // phase, which can run for hours — so a lock session that dies mid-
    // snapshot is detected at once instead of going unnoticed until CDC
    // starts (or forever, for a pipeline that never finishes its snapshot).
    {
        let lock_watcher = pg_lock_guard.watcher();
        let lock_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            lock_watcher.wait_lost().await;
            if *lock_shutdown_tx.borrow() {
                return; // already shutting down for another reason
            }
            tracing::error!(
                "Distributed single-instance lock session ended (PG connection dropped) — \
                 another instance may now start against the same StarRocks database/checkpoint. \
                 Shutting down to fail closed; restart once the lock holder situation is clear."
            );
            let _ = lock_shutdown_tx.send(true);
        });
    }

    // StarRocks HA connection manager
    let sr_mgr = Arc::new(
        crate::sr_connection::SrConnectionManager::new(
            &config.starrocks,
            config.pipeline.max_retries,
            config.pipeline.sink_flush_timeout_ms,
            config.pipeline.mysql_query_timeout_ms,
        )
        .context("Failed to create SR connection manager")?,
    );

    // Ensure the StarRocks database exists. CREATE DATABASE IF NOT EXISTS is
    // idempotent — safe to run on every start. Without this, all downstream
    // operations (DDL poller, snapshot, CDC) fail with "Unknown database".
    // Must connect without selecting a database since the target DB doesn't
    // exist yet — MySQL handshake fails if db_name is set to a non-existent DB.
    {
        let create_db = format!("CREATE DATABASE IF NOT EXISTS `{}`", sr_mgr.database());
        tracing::info!("Ensuring SR database `{}` exists", sr_mgr.database());
        if let Err(e) = sr_mgr.execute_sql_without_db(&create_db).await {
            tracing::error!(
                "Failed to ensure SR database `{}` exists: {:?}",
                sr_mgr.database(),
                e,
            );
            return Err(e).context("Failed to create SR database");
        }
    }

    // Parse DDL capture table for DDL capture infrastructure setup.
    let (ddl_schema, ddl_table) =
        config::parse_ddl_capture_table(&config.pipeline.ddl_capture_table)
            .context("Invalid ddl_capture_table config — must be in 'schema.table' format")?;

    // ── Fresh start ──
    if args.fresh {
        // --fresh is destructive: it deletes the checkpoint, drops &
        // recreates the replication slot + publication, tears down DDL capture
        // infra, and re-snapshots all matching tables from PG into StarRocks
        // (replacing SR data via per-table shadow-table swap). It does NOT drop
        // SR tables directly. The log message documents the exact teardown so
        // operators know what `--fresh` touches.
        tracing::warn!(
            "FRESH START: will delete the checkpoint, drop & recreate the \
             replication slot + publication, tear down DDL capture infra, \
             and re-snapshot all matching tables from PG into StarRocks \
             (replacing SR data via per-table shadow-table swap). It does NOT \
             drop SR tables directly."
        );
        checkpoint_store.delete().await?;
        // The fresh snapshot re-copies PG's current state, which already
        // contains the buffered events' effects — leftover segments would
        // resurrect pre-fresh rows on replay. Purge before re-snapshotting.
        wal_buffer.purge_all().await?;
        reset_replication_slot(&config.postgres).await?;
    }

    // Clean DDL infrastructure on fresh start
    if args.fresh {
        ddl::teardown_ddl_capture(&config.postgres, ddl_schema, ddl_table, &pg_tls).await?;
    }

    let checkpoint = if args.fresh {
        // On fresh start, get the new slot's starting LSN so CDC resumes from
        // the correct WAL position. If we start at LSN=0, pgwire-replication's
        // internal position gets out of sync with the actual slot position.
        let mut cp = Checkpoint::default();
        match query_slot_lsn(&config.postgres, &config.postgres.slot_name, &pg_tls).await {
            Some(lsn) if lsn > 0 => {
                cp.last_committed_lsn = lsn as u64;
                tracing::info!("Fresh start: initial LSN set to {lsn}");
            }
            _ => tracing::warn!("Fresh start: could not query slot LSN, starting from 0"),
        }
        Arc::new(Mutex::new(cp))
    } else {
        Arc::new(Mutex::new(checkpoint_store.load().await?))
    };

    // ── DDL capture (with retry for transient PG unavailability) ──
    {
        let mut retry_delay = Duration::from_secs(1);
        const MAX_DDL_SETUP_RETRY: Duration = Duration::from_secs(30);
        // On --fresh: create from scratch (teardown already happened above).
        // On restart: ensure infrastructure exists without dropping anything.
        loop {
            let result: Result<()> = if args.fresh {
                ddl::create_ddl_capture(&config.postgres, ddl_schema, ddl_table, &pg_tls).await
            } else {
                ddl::ensure_ddl_capture(&config.postgres, ddl_schema, ddl_table, &pg_tls).await
            };
            match result {
                Ok(()) => break,
                Err(e) => {
                    if is_permanent_pg_error(&*e) {
                        tracing::error!(
                            "DDL setup failed with permanent error (auth/permission) — exiting: {:?}",
                            e,
                        );
                        return Err(e).context("Permanent DDL setup failure (auth/permission)");
                    }
                    let fn_name = if args.fresh {
                        "create_ddl_capture"
                    } else {
                        "ensure_ddl_capture"
                    };
                    tracing::error!(
                        "DDL {} failed: {:?} — retrying in {:?}...",
                        fn_name,
                        e,
                        retry_delay,
                    );
                    tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                    retry_delay = (retry_delay * 2).min(MAX_DDL_SETUP_RETRY);
                }
            }
        }
    }

    // Reset DDL checkpoint if the DDL capture table was recreated.
    // This handles the case where the pipeline restarts after DDL infra
    // was deleted (e\.g. after the capture table is recreated). Without this, the DDL poller
    // would query WHERE id > $1 with the stale last_ddl_id and skip all
    // new entries (which start from id=1 after the table was recreated).
    //
    // Note: ensure_ddl_capture above guarantees the table exists at this
    // point — a MAX(id) failure here indicates a bug (e.g. concurrent
    // DROP TABLE by a DBA between the ensure and this query) rather than
    // a tolerable startup condition. Hard-fail rather than silently
    // leave a stale last_ddl_id in place.
    {
        let conn_str = config.postgres.conn_str();
        let (client, conn) = pg_tls
            .connect(&conn_str)
            .await
            .context("Failed to connect PG for DDL checkpoint reset")?;
        // Wrap the connection-driver spawn in AbortOnDrop (consistent with
        // the catalog/snapshot/connection guards). Currently safe (the task
        // ends when `client` drops) but inconsistent; AbortOnDrop future-proofs
        // against a detached zombie task if this code is restructured.
        let _conn_driver = AbortOnDrop(tokio::spawn(async move {
            let _ = conn.await;
        }));
        let row = client
            .query_one(
                &format!(
                    "SELECT COALESCE(MAX(id), 0)::bigint FROM {}.{}",
                    crate::pg_catalog::pg_quote(ddl_schema),
                    crate::pg_catalog::pg_quote(ddl_table)
                ),
                &[],
            )
            .await
            .context(
                "Failed to query MAX(id) from DDL capture table \
                      — unexpected immediately after ensure_ddl_capture succeeded",
            )?;
        let max_id: i64 = row.try_get(0).unwrap_or(0);
        let cp = {
            let mut guard = lock_checkpoint(&checkpoint);
            if max_id < guard.last_ddl_id {
                tracing::info!(
                    "DDL capture table recreated (max_id={max_id} < last_ddl_id={}), resetting DDL checkpoint",
                    guard.last_ddl_id,
                );
                guard.last_ddl_id = max_id;
                Some(guard.clone())
            } else {
                None
            }
        };
        if let Some(cp) = cp {
            if let Err(e) = checkpoint_store.save(&cp).await {
                tracing::warn!("Failed to save DDL checkpoint reset: {e}");
            }
        }
    }

    // ── Validate PG table schemas before snapshot ──
    // Fail fast: tables without PRIMARY KEY or with incompatible PK types
    // (like JSON) can't be replicated to StarRocks. Check all tables upfront
    // instead of discovering the error during snapshot's infinite retry loop.
    {
        let violations = validate_replicated_tables(
            &config.postgres,
            &config.postgres.table_filter,
            &config.postgres.table_exclude,
            ddl_schema,
            ddl_table,
            &pg_tls,
        )
        .await;
        if let Err(e) = violations {
            tracing::error!("{e}");
            return Err(e);
        }
    }

    // ── Table filter change detection ──
    {
        let cp = lock_checkpoint(&checkpoint);
        let normalized_filter = config.postgres.table_filter.trim().to_string();
        let prev_filter = cp.table_filter.as_deref().unwrap_or("");
        if prev_filter.is_empty() && normalized_filter != "*" {
            tracing::info!(
                "table_filter='{}' (first start — will snapshot all matching tables)",
                normalized_filter,
            );
        } else if prev_filter != normalized_filter && !prev_filter.is_empty() {
            tracing::info!(
                "table_filter changed: '{}' → '{}' — gap detection will snapshot newly matching tables",
                prev_filter, normalized_filter,
            );
        }
    }

    // ── Ensure replication slot exists BEFORE snapshot ──
    // On fresh start, reset_replication_slot() above already created the slot
    // and publication ~100ms earlier — calling ensure_replication_slot here
    // would just re-run pg_create_logical_replication_slot and emit a spurious
    // "already exists" WARN. Skip it on --fresh; the slot LSN query below still
    // runs because --fresh also needs to capture the new slot's start LSN.
    // On restart, the slot should still exist. If it was accidentally dropped,
    // we recreate it and advance the checkpoint to the new slot's LSN so CDC
    // doesn't try to resume from a stale LSN the new slot doesn't cover.
    {
        if !args.fresh {
            ensure_replication_slot(&config.postgres, &pg_tls)
                .await
                .context("Failed to ensure replication slot before snapshot")?;
        }

        // If the slot was recreated (dropped between runs), its LSN will be
        // ahead of the checkpoint's last_committed_lsn. Advance to match.
        if let Some(lsn) =
            query_slot_lsn(&config.postgres, &config.postgres.slot_name, &pg_tls).await
        {
            if lsn > 0 {
                let mut cp = lock_checkpoint(&checkpoint);
                if (lsn as u64) > cp.last_committed_lsn {
                    tracing::info!(
                        "Replication slot LSN ({lsn}) ahead of checkpoint LSN ({}), \
                         advancing checkpoint to match slot",
                        cp.last_committed_lsn,
                    );
                    cp.last_committed_lsn = lsn as u64;
                }
            }
        }
    }

    // ── Cross-host single-instance guard: refuse to run while another
    //    pipeline is actively streaming from the same replication slot.
    //    The local flock above only protects against same-host duplicates;
    //    two instances on different machines would both pass the flock and
    //    race through the snapshot phase into the CDC attach. PG's
    //    `pg_replication_slots.active` flag is the authoritative source of
    //    truth here — `active = true` means a live WAL sender is attached.
    //
    //    This is an early-fail optimization: the real atomic guard is
    //    PG's 55006 "replication slot is active for PID X" rejection at
    //    CdcConsumer::connect time (handled as fatal-exit below). The
    //    pre-check just skips the expensive snapshot phase when we can
    //    already see the slot is busy.
    //
    //    On --fresh starts, reset_replication_slot() above already
    //    terminated any zombie WAL sender, so active_pid is NULL here
    //    and the check passes. (Note: snapshot-phase TOCTOU — two
    //    instances starting within ~10ms of each other — is not closed
    //    by this check; it relies on the 55006 fatal-exit as backup.
    //    This mirrors SeaTunnel's posture of punting snapshot-phase
    //    coordination to operator responsibility.)
    //
    //    Instances sharing the SR database or the checkpoint store are
    //    additionally excluded by the PG advisory lock acquired before
    //    the snapshot phase (see the acquire above), which closes the
    //    TOCTOU for exactly the configurations that would share
    //    downstream state. Same-slot contention is still enforced here
    //    and by PG's 55006 at CDC attach.
    if let Some(pid) =
        slot_active_pid(&config.postgres, &pg_tls, &config.postgres.slot_name).await?
    {
        return Err(anyhow::anyhow!(
            "Replication slot '{}' is already in use by PID {} — refusing to start. \
             Another pipeline instance is actively streaming from this slot. \
             Stop the other instance first, or use --fresh to terminate any \
             zombie WAL sender.",
            config.postgres.slot_name,
            pid,
        ));
    }

    // Snapshot + WAL-lag gauges created early (before snapshot phase) so they
    // can be set during snapshot and passed into Metrics::new later.
    let metrics_snapshot_in_progress = Arc::new(AtomicU64::new(0));
    let metrics_snapshot_tables_done = Arc::new(AtomicU64::new(0));
    let metrics_pg_wal_lag_bytes = Arc::new(AtomicU64::new(0));
    let metrics_confirmed_flush_lsn = Arc::new(AtomicU64::new(0));
    let metrics_checkpoint_lag_bytes = Arc::new(AtomicU64::new(0));

    // ── Snapshot (always runs; skip-already-done tables for gap detection) ──
    {
        // Set snapshot_in_progress gauge = 1 for the duration of the snapshot phase.
        metrics_snapshot_in_progress.store(1, Ordering::Relaxed);
        // Clean up orphaned snapshot temp/backup tables from prior crashes
        snapshot::cleanup_orphan_tables(&sr_mgr).await;

        let mut retry_delay = Duration::from_millis(500);
        const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
        loop {
            let table_props = config.starrocks.build_table_properties();
            match snapshot::run_snapshot(
                &config.postgres,
                &sr_mgr,
                checkpoint.clone(),
                &*checkpoint_store,
                &config.postgres.table_filter,
                &config.postgres.table_exclude,
                &table_props,
                config.pipeline.snapshot_page_size,
                config.pipeline.max_retries,
                &pg_tls,
                ddl_schema,
                ddl_table,
                config.pipeline.query_timeout_ms,
            )
            .await
            {
                Ok(()) => {
                    // Save the current checkpoint state (snapshot progress)
                    let cp = lock_checkpoint(&checkpoint).clone();
                    // MutexGuard dropped — safe to await
                    if let Err(e) = checkpoint_store.save(&cp).await {
                        tracing::warn!(
                            "Snapshot phase save failed (non-fatal, will retry): {:?}",
                            e,
                        );
                    }
                    // Save the current table_filter to checkpoint for change detection
                    // on subsequent restarts.
                    {
                        let cp = {
                            let mut guard = lock_checkpoint(&checkpoint);
                            guard.table_filter =
                                Some(config.postgres.table_filter.trim().to_string());
                            guard.clone()
                        };
                        if let Err(e) = checkpoint_store.save(&cp).await {
                            tracing::warn!("Failed to save table_filter to checkpoint: {e}");
                        }
                    }
                    break;
                }
                Err(e) => {
                    if is_permanent_pg_error(&*e) {
                        tracing::error!(
                            "Snapshot failed with permanent error (auth/permission) — exiting: {:?}",
                            e,
                        );
                        metrics_snapshot_in_progress.store(0, Ordering::Relaxed);
                        return Err(e).context("Permanent snapshot failure (auth/permission)");
                    }
                    tracing::error!(
                        "Snapshot failed: {:?} — retrying in {:?}...",
                        e,
                        retry_delay
                    );
                    tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                    retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
        // Snapshot phase complete — set gauge to 0 (CDC streaming starting).
        metrics_snapshot_in_progress.store(0, Ordering::Relaxed);
    }

    // Ctrl+C handler with graceful shutdown.
    // First Ctrl+C: begin graceful shutdown (flush pending data, save checkpoint).
    // Second Ctrl+C (at any point during the stall-prone graceful path): force
    // `std::process::exit(130)`. README/PROJECT.md promise this escape hatch
    // because the graceful flush can take ~340s when SR is unresponsive, during
    // which tokio's signal driver would otherwise swallow further SIGINTs — the
    // operator had no way out for minutes.
    let shutdown_tx_ctrl = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::error!("Failed to register SIGINT handler — signal handling disabled");
            return;
        }
        tracing::info!(
            "SIGINT received, initiating graceful shutdown (press Ctrl+C again to force-exit)..."
        );
        let _ = shutdown_tx_ctrl.send(true);
        // The first ctrl_c() consumed SIGINT; this second await catches a
        // subsequent SIGINT during graceful shutdown and exits immediately. The
        // future is dropped (no force-exit) when the process exits via the
        // graceful path, so it never lingers past shutdown.
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::warn!("Second SIGINT received — force-exiting (code 130)");
            std::process::exit(130);
        }
    });

    // SIGTERM handler — container orchestration (Docker/K8s) sends SIGTERM first.
    // Without this, the pipeline gets killed hard mid-flush in containerized
    // environments, losing the checkpoint.
    #[cfg(unix)]
    {
        let shutdown_tx_term = shutdown_tx.clone();
        tokio::spawn(async move {
            use tokio::signal::unix;
            match unix::signal(unix::SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                    tracing::info!("SIGTERM received, shutting down (send again to force-exit)...");
                    let _ = shutdown_tx_term.send(true);
                    // Second SIGTERM → force exit(143) (128 + SIGTERM=15).
                    if stream.recv().await.is_some() {
                        tracing::warn!("Second SIGTERM received — force-exiting (code 143)");
                        std::process::exit(143);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to register SIGTERM handler: {:?}", e);
                }
            }
        });
    }

    // ── Pipeline state ──
    let sr_sink = SrSink::new(sr_mgr.clone()).context("Failed to create StarRocks sink")?;
    let table_props_pipeline = config.starrocks.build_table_properties();

    // Shared state for metrics heartbeat
    let pending_bytes = Arc::new(AtomicUsize::new(0));
    let ddl_applied = Arc::new(AtomicU64::new(0));
    let ddl_skipped = Arc::new(AtomicU64::new(0));
    // Wal-buffer metrics — shared with the Pipeline (append/replay accounting).
    let wal_buffered_events = Arc::new(AtomicU64::new(0));
    let wal_buffered_bytes = Arc::new(AtomicU64::new(0));
    let wal_replayed_events = Arc::new(AtomicU64::new(0));
    let wal_replay_failures = Arc::new(AtomicU64::new(0));
    let wal_buffer_segments = Arc::new(AtomicU64::new(0));
    let wal_buffer_bytes = Arc::new(AtomicU64::new(0));
    // Manager-owned totals: these gauges are refreshed from the manager's
    // in-memory accounting by the wal-lag metrics loop (once per tick) —
    // never mutated by the pipeline. Initialize from the startup scan/purge
    // so the first metrics tick is already correct.
    wal_buffer_segments.store(wal_buffer.total_segments(), Ordering::Relaxed);
    wal_buffer_bytes.store(wal_buffer.total_buffered_bytes(), Ordering::Relaxed);
    // Metrics error_count is created early so it can be shared with the DDL
    // poller and the checkpoint saver (both bumped on non-fatal failures).
    let error_count = Arc::new(AtomicU64::new(0));
    // Snapshot + WAL-lag gauges were created earlier (before the snapshot phase).

    let table_filter = config.postgres.table_filter.clone();
    let table_filter_re = Some(
        crate::config::compile_table_filter(&table_filter)
            .context("Failed to compile table_filter regex at startup")?,
    );

    let pending_cdc_tables: Arc<Mutex<std::collections::HashSet<String>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));
    // DDL/CDC drain-and-pause coordination: tables currently being DDL'd by the
    // DDL poller. The CDC flush path routes these tables' changes into the
    // durable wal_buffer (flush_transaction routing condition) — no in-memory
    // deferral.
    let ddl_active: Arc<Mutex<std::collections::HashSet<String>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));
    // Tables dropped by the DDL poller since the last batch; the pipeline
    // drains this in begin_transaction to evict stale relation/PK cache
    // entries (bounds memory under DDL churn, prevents OID-reuse mis-routing).
    let dropped_relations: Arc<Mutex<std::collections::HashSet<String>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));
    // Column-rename map (old→new per table), registered by the DDL poller on
    // RENAME COLUMN so buffered rows replay with live column names. Seeded
    // from leftover sidecars (crash recovery) so replay remaps immediately;
    // the DDL poller re-registers any rename it applies after restart.
    let rename_map: crate::pipeline::RenameMap =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    for (tk, entries) in leftover_renames {
        crate::pipeline::lock_or_recover(&rename_map)
            .entry(tk)
            .or_default()
            .extend(entries);
    }

    let mut pipeline = Pipeline::new(
        sr_sink,
        sr_mgr.clone(),
        table_props_pipeline,
        config.pipeline.max_pending_memory_mb,
        config.postgres.clone(),
        pending_bytes.clone(),
        table_filter_re,
        config.pipeline.ddl_race_retry_ms,
        config.pipeline.max_concurrent_flushes,
        pending_cdc_tables.clone(),
        ddl_active.clone(),
        dropped_relations.clone(),
        shutdown_rx.clone(),
        wal_buffer.clone(),
        wal_buffered_events.clone(),
        wal_buffered_bytes.clone(),
        wal_replayed_events.clone(),
        wal_replay_failures.clone(),
        rename_map.clone(),
    );

    // ── Metrics ──
    let last_saved_at: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let metrics = Arc::new(Metrics::new(
        pending_bytes,
        ddl_applied.clone(),
        ddl_skipped.clone(),
        error_count.clone(),
        last_saved_at.clone(),
        metrics_snapshot_in_progress.clone(),
        metrics_snapshot_tables_done.clone(),
        metrics_pg_wal_lag_bytes.clone(),
        metrics_confirmed_flush_lsn.clone(),
        metrics_checkpoint_lag_bytes.clone(),
        wal_buffered_events.clone(),
        wal_buffered_bytes.clone(),
        wal_replayed_events.clone(),
        wal_replay_failures.clone(),
        wal_buffer_segments.clone(),
        wal_buffer_bytes.clone(),
        wal_buffer_quarantined.clone(),
        started,
    ));

    // Install the `metrics` crate Prometheus recorder so
    // flush-latency histograms with p50/p95/p99 quantiles are exposed via the
    // /metrics endpoint below — closing the "no histogram support / only an
    // average" observability gap. Failure to install is non-fatal: the
    // `metrics::histogram!` calls become no-ops and /metrics omits the section.
    let prom_handle: Option<metrics_exporter_prometheus::PrometheusHandle> =
        if config.metrics.enabled {
            metrics_exporter_prometheus::PrometheusBuilder::new()
                .set_quantiles(&[0.5, 0.95, 0.99])
                .and_then(|b| b.install_recorder())
                .ok()
        } else {
            None
        };

    // Metric reporter (every 5s) with auto-restart on panic
    let metrics_clone = metrics.clone();
    let shutdown_for_metrics = shutdown_rx.clone();
    let mut metrics_handle = tokio::spawn(async move {
        let mut restart_count: u32 = 0;
        const MAX_METRICS_RESTARTS: u32 = 10;
        loop {
            if *shutdown_for_metrics.borrow() {
                break;
            }

            let m = metrics_clone.clone();
            let mut rx = shutdown_for_metrics.clone();

            let task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        biased;
                        _ = rx.changed() => {
                            break;
                        }
                        _ = interval.tick() => {
                            m.report(started);
                        }
                    }
                }
            });

            match task.await {
                Ok(()) => break,
                Err(e) => {
                    if e.is_panic() {
                        restart_count += 1;
                        if restart_count > MAX_METRICS_RESTARTS {
                            tracing::error!(
                                "Metrics reporter panicked {} times (>{} cap) — \
                                 giving up. Metrics will no longer be reported.",
                                restart_count,
                                MAX_METRICS_RESTARTS,
                            );
                            break;
                        }
                        tracing::error!(
                            "Metrics reporter panicked ({}/{}): {:?} — restarting in 5s",
                            restart_count,
                            MAX_METRICS_RESTARTS,
                            e.into_panic()
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    } else {
                        break;
                    }
                }
            }
        }
    });

    // ── PG WAL lag query task (every 60s) ──
    // Periodically queries pg_current_wal_lsn and confirmed_flush_lsn from PG,
    // computes WAL lag and checkpoint lag, and caches into shared atomics for
    // /metrics and /health to expose.
    {
        let wal_lag_pg_cfg = config.postgres.clone();
        let wal_lag_tls = PgTlsConnector::from_config(&config.postgres.tls)
            .context("Failed to create TLS connector for WAL-lag task")?;
        let wal_lag_slot = config.postgres.slot_name.clone();
        let mut wal_lag_shutdown = shutdown_rx.clone();
        let wal_lag_wal_lag_bytes = metrics_pg_wal_lag_bytes.clone();
        let wal_lag_confirmed_flush_lsn = metrics_confirmed_flush_lsn.clone();
        let wal_lag_checkpoint_lag_bytes = metrics_checkpoint_lag_bytes.clone();
        let wal_lag_snapshot_tables_done = metrics_snapshot_tables_done.clone();
        let wal_lag_checkpoint = checkpoint.clone();
        let wal_lag_wbuf = wal_buffer.clone();
        let wal_lag_wbuf_segments = wal_buffer_segments.clone();
        let wal_lag_wbuf_bytes = wal_buffer_bytes.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip immediate first tick
            loop {
                tokio::select! {
                    biased;
                    _ = wal_lag_shutdown.changed() => { break; }
                    _ = interval.tick() => {
                        // Query PG for WAL head and confirmed_flush_lsn.
                        let current_wal_lsn = query_pg_current_wal_lsn(&wal_lag_pg_cfg, &wal_lag_tls).await;
                        let flush_lsn = query_slot_lsn(&wal_lag_pg_cfg, &wal_lag_slot, &wal_lag_tls).await;
                        // Read checkpoint's last_committed_lsn + snapshot progress.
                        let (committed, snap_done) = {
                            let cp = lock_checkpoint(&wal_lag_checkpoint);
                            (cp.last_committed_lsn as i64, cp.snapshotted_tables.len() as u64)
                        };
                        wal_lag_snapshot_tables_done.store(snap_done, Ordering::Relaxed);
                        if let (Some(wal), Some(flush)) = (current_wal_lsn, flush_lsn) {
                            let lag = (wal - flush).max(0) as u64;
                            wal_lag_wal_lag_bytes.store(lag, Ordering::Relaxed);
                            wal_lag_confirmed_flush_lsn.store(flush as u64, Ordering::Relaxed);
                            let ckpt_lag = (flush - committed).max(0) as u64;
                            wal_lag_checkpoint_lag_bytes.store(ckpt_lag, Ordering::Relaxed);
                        }
                        // Refresh the manager-owned wal-buffer gauges (segments
                        // + bytes) from the manager's in-memory accounting.
                        wal_lag_wbuf_segments.store(wal_lag_wbuf.total_segments(), Ordering::Relaxed);
                        wal_lag_wbuf_bytes.store(wal_lag_wbuf.total_buffered_bytes(), Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // ── 5-minute health summary ──
    let summary_metrics = metrics.clone();
    let summary_started = started;
    let mut shutdown_for_summary = shutdown_rx.clone();
    let mut summary_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            tokio::select! {
                biased;
                _ = shutdown_for_summary.changed() => {
                    break;
                }
                _ = interval.tick() => {
                    summary_metrics.report_summary(summary_started);
                }
            }
        }
    });

    // ── Prometheus /metrics HTTP endpoint ──
    if config.metrics.enabled {
        let port = config.metrics.port;
        let host = config.metrics.host.clone();
        let metrics_snap = metrics_server::MetricsSnapshot {
            total_rows: metrics.total_rows.clone(),
            total_batches: metrics.total_batches.clone(),
            total_flush_ms: metrics.total_flush_ms.clone(),
            last_lsn: metrics.last_lsn.clone(),
            last_progress_at: metrics.last_progress_at.clone(),
            reconnect_count: metrics.reconnect_count.clone(),
            pending_bytes: metrics.pending_bytes.clone(),
            ddl_applied: metrics.ddl_applied.clone(),
            ddl_skipped: metrics.ddl_skipped.clone(),
            error_count: metrics.error_count.clone(),
            last_saved_at: metrics.last_saved_at.clone(),
            snapshot_in_progress: metrics.snapshot_in_progress.clone(),
            snapshot_tables_done: metrics.snapshot_tables_done.clone(),
            pg_wal_lag_bytes: metrics.pg_wal_lag_bytes.clone(),
            confirmed_flush_lsn: metrics.confirmed_flush_lsn.clone(),
            checkpoint_lag_bytes: metrics.checkpoint_lag_bytes.clone(),
            wal_buffered_events: metrics.wal_buffered_events.clone(),
            wal_buffered_bytes: metrics.wal_buffered_bytes.clone(),
            wal_replayed_events: metrics.wal_replayed_events.clone(),
            wal_replay_failures: metrics.wal_replay_failures.clone(),
            wal_buffer_segments: metrics.wal_buffer_segments.clone(),
            wal_buffer_bytes: metrics.wal_buffer_bytes.clone(),
            wal_buffer_quarantined_segments: metrics.wal_buffer_quarantined_segments.clone(),
            wal_buffer: Some(wal_buffer.clone()),
            started,
            shutdown_rx: shutdown_rx.clone(),
            save_interval_ms: cp_cfg.save_interval_ms,
            prom_handle,
        };
        tokio::spawn(async move {
            metrics_server::run_metrics_server(metrics_snap, &host, port).await;
        });
    }

    // ── DDL polling (supervised: restart on panic, FATAL after 3 failures) ──
    let ddl_handle: Option<tokio::task::JoinHandle<()>>;
    if config.pipeline.schema_sync {
        let ddl_pg_config = config.postgres.clone();
        let ddl_sr_mgr = sr_mgr.clone();
        let ddl_schema_owned = ddl_schema.to_string();
        let ddl_table_owned = ddl_table.to_string();
        let ddl_checkpoint = checkpoint.clone();
        let ddl_checkpoint_store = checkpoint_store.clone();
        let ddl_shutdown_rx = shutdown_rx.clone();
        let ddl_shutdown_tx = shutdown_tx.clone();
        let ddl_poll_interval = config.pipeline.ddl_poll_interval_ms;
        let ddl_table_props = config.starrocks.build_table_properties();
        let ddl_table_filter = config.postgres.table_filter.clone();
        let ddl_table_exclude = config.postgres.table_exclude.clone();
        let ddl_pending_cdc_tables = pending_cdc_tables.clone();
        let ddl_ddl_active = ddl_active.clone();
        let ddl_dropped_relations = dropped_relations.clone();
        let ddl_error_count = error_count.clone();

        // Supervisor: spawns run_ddl_poller_loop, restarts on panic.
        // After MAX_DDL_RESTARTS panics, FATAL-exits (sends shutdown signal).
        // This mirrors the checkpoint saver supervisor pattern at line 1032.
        ddl_handle = Some(tokio::spawn(async move {
            const MAX_DDL_RESTARTS: u32 = 3;
            let mut restart_count: u32 = 0;
            let mut ddl_backoff = Duration::from_secs(5);
            const DDL_MAX_BACKOFF: Duration = Duration::from_secs(30);

            loop {
                if *ddl_shutdown_rx.borrow() {
                    break;
                }

                tracing::info!(
                    "Starting DDL poller (attempt {}/{})",
                    restart_count + 1,
                    MAX_DDL_RESTARTS + 1,
                );

                // Clone all inputs for this spawn iteration — Arcs are cheap,
                // owned types (PgConfig, String, HashMap) get cloned per-spawn.
                let pg = ddl_pg_config.clone();
                let sr = ddl_sr_mgr.clone();
                let schema = ddl_schema_owned.clone();
                let table = ddl_table_owned.clone();
                let cp = ddl_checkpoint.clone();
                let store = ddl_checkpoint_store.clone();
                let rx = ddl_shutdown_rx.clone();
                let tx = ddl_shutdown_tx.clone();
                let props = ddl_table_props.clone();
                let tf = ddl_table_filter.clone();
                let tex = ddl_table_exclude.clone();
                let pct = ddl_pending_cdc_tables.clone();
                let dda = ddl_ddl_active.clone();
                let dr = ddl_dropped_relations.clone();
                let da = ddl_applied.clone();
                let ds = ddl_skipped.clone();
                let ec = ddl_error_count.clone();
                let rm = rename_map.clone();
                let wb = wal_buffer.clone();

                // Wrap the inner DDL poller task in AbortOnDrop so
                // an outer-task abort (e.g. via `tokio::time::timeout` + `.abort()`
                // for shutdown at main.rs:2275-2276) propagates down to the inner
                // `run_ddl_poller_loop` instead of detaching it. WITHOUT this
                // wrapper, dropping the local `inner_handle: JoinHandle` at the
                // outer-abort point DETACHES the inner task (rather than aborting
                // it) — the inner DDL poller would continue executing SR DDLs
                // concurrently with `sr_mgr.shutdown()` (which disconnects
                // pools), risking panics in `mysql_async` internals. Mirrors the
                // existing AbortOnDrop pattern at `ddl_poller.rs:158` (the
                // `run_ddl_poller_loop` → `run_ddl_poller` boundary). The drop
                // chain on outer-abort: outer task cancelled → inner_guard drops
                // → `.abort()` on `run_ddl_poller_loop` → its `_task_guard`
                // (ddl_poller.rs:158) drops → `.abort()` on `run_ddl_poller` →
                // the in-flight `conn.query_drop` future (sr_connection.rs:585)
                // drops in place (mysql_async supports this; the per-query
                // `tokio::time::timeout` wrapper already relies on this). Chain
                // cancellation, 3 levels deep.
                let mut inner_guard = AbortOnDrop(tokio::spawn(async move {
                    ddl_poller::run_ddl_poller_loop(
                        pg,
                        sr,
                        schema,
                        table,
                        cp,
                        store,
                        rx,
                        tx,
                        ddl_poll_interval,
                        props,
                        config.pipeline.snapshot_page_size,
                        config.pipeline.max_retries,
                        da,
                        ds,
                        tf,
                        tex,
                        config.pipeline.query_timeout_ms,
                        config.pipeline.ddl_failure_abort,
                        config.pipeline.schema_change_retry_ms,
                        pct,
                        dda,
                        dr,
                        ec,
                        rm,
                        wb,
                    )
                    .await;
                }));

                match (&mut inner_guard.0).await {
                    Ok(()) => {
                        tracing::info!("DDL poller exited normally");
                        break;
                    }
                    Err(e) if e.is_panic() => {
                        restart_count += 1;
                        if restart_count > MAX_DDL_RESTARTS {
                            tracing::error!(
                                "FATAL: DDL poller task panicked {} times (>{} cap). \
                                 Schema sync has stopped permanently. \
                                 Sending shutdown signal — pipeline will exit.",
                                restart_count,
                                MAX_DDL_RESTARTS,
                            );
                            let _ = ddl_shutdown_tx.send(true);
                            break;
                        }
                        let panic_msg = e.into_panic();
                        let panic_str = panic_msg
                            .downcast_ref::<String>()
                            .map(|s| s.as_str())
                            .unwrap_or("(non-string panic)");
                        tracing::error!(
                            "DDL poller panicked ({}/{}): {} — restarting in {:?}",
                            restart_count,
                            MAX_DDL_RESTARTS,
                            panic_str,
                            ddl_backoff,
                        );
                        tokio::time::sleep(crate::util::jitter_duration(ddl_backoff)).await;
                        ddl_backoff = (ddl_backoff * 2).min(DDL_MAX_BACKOFF);
                    }
                    Err(_) => {
                        // Cancelled (not panic) — likely shutdown
                        tracing::info!("DDL poller task cancelled (shutdown)");
                        break;
                    }
                }
            }
        }));
    } else {
        ddl_handle = None;
        tracing::info!("Schema evolution disabled (schema_sync=false)");
    }

    // Background checkpoint saver: persists the in-memory checkpoint on a
    // fixed interval instead of inline on every batch/idle-ACK. Avoids
    // spamming S3 with PUTs and keeps hot paths fast. Final save on shutdown.
    // Wrapped in a supervisor that restarts on panic (mirroring metrics reporter).
    let save_interval = Duration::from_millis(cp_cfg.save_interval_ms);
    let last_saved_lsn: Arc<AtomicU64> = {
        let cp = lock_checkpoint(&checkpoint);
        Arc::new(AtomicU64::new(cp.last_committed_lsn))
    };
    let last_saved_ddl_id: Arc<std::sync::atomic::AtomicI64> = {
        let cp = lock_checkpoint(&checkpoint);
        Arc::new(std::sync::atomic::AtomicI64::new(cp.last_ddl_id))
    };
    let cp_saver_handle = {
        let bg_checkpoint = checkpoint.clone();
        let bg_store = checkpoint_store.clone();
        let bg_shutdown = shutdown_rx.clone();
        let bg_last_saved = last_saved_lsn.clone();
        let bg_last_saved_ddl = last_saved_ddl_id.clone();
        let bg_saved_at = last_saved_at.clone();
        let bg_error_count = error_count.clone();
        tokio::spawn(async move {
            let mut restart_count: u32 = 0;
            const MAX_SAVER_RESTARTS: u32 = 10; // was 100 (~8min in a panic loop); 10 bounds recovery
            loop {
                if *bg_shutdown.borrow() {
                    break;
                }

                let ckpt = bg_checkpoint.clone();
                let store = bg_store.clone();
                let mut shutdown = bg_shutdown.clone();
                let last_saved = bg_last_saved.clone();
                let last_saved_ddl = bg_last_saved_ddl.clone();
                let saved_at = bg_saved_at.clone();
                let saver_errors = bg_error_count.clone();

                let task = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(save_interval);
                    interval.tick().await; // skip immediate first tick
                    loop {
                        tokio::select! {
                            biased;
                            _ = shutdown.changed() => {
                                // Final save on shutdown — always unconditional.
                                // Capture under the store lock so the saved
                                // state is the newest in-memory state at write
                                // time (no stale-clone regression). The
                                // returned cp is the DUURABLY written state —
                                // bookkeeping must use it, not a re-clone.
                                match store
                                    .save_captured(Box::new(|| lock_checkpoint(&ckpt).clone()))
                                    .await
                                {
                                    Ok(cp) => {
                                        tracing::info!(
                                            "Final checkpoint saved on shutdown: lsn={} ddl_id={}",
                                            cp.last_committed_lsn, cp.last_ddl_id,
                                        );
                                        last_saved.store(cp.last_committed_lsn, Ordering::Release);
                                        last_saved_ddl.store(cp.last_ddl_id, std::sync::atomic::Ordering::Release);
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_secs())
                                            .unwrap_or(0);
                                        saved_at.store(now, Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        let cp = lock_checkpoint(&ckpt).clone();
                                        tracing::error!(
                                            "Final checkpoint save on shutdown failed: {:?} (lsn={}, ddl_id={})",
                                            e, cp.last_committed_lsn, cp.last_ddl_id,
                                        );
                                        saver_errors.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                break;
                            }
                            _ = interval.tick() => {
                                // Skip-on-unchanged: if both LSN and DDL_ID are
                                // the same as the last durable save AND we're past
                                // the snapshot phase, skip the PUT. Still update
                                // saved_at so the "saver dead" watchdog doesn't
                                // false-alarm.
                                let saved_lsn = last_saved.load(Ordering::Acquire);
                                let saved_ddl = last_saved_ddl.load(std::sync::atomic::Ordering::Acquire);
                                {
                                    let cp = lock_checkpoint(&ckpt).clone();
                                    if cp.snapshot_done
                                        && cp.last_committed_lsn == saved_lsn
                                        && cp.last_ddl_id == saved_ddl
                                    {
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_secs())
                                            .unwrap_or(0);
                                        saved_at.store(now, Ordering::Relaxed);
                                        tracing::trace!(
                                            "Checkpoint unchanged (lsn={}, ddl_id={}) — skipping S3 PUT",
                                            cp.last_committed_lsn, cp.last_ddl_id,
                                        );
                                        continue;
                                    }
                                }
                                // Capture under the store lock: the saved state
                                // is the newest in-memory state at WRITE time,
                                // so an older capture can never overwrite a
                                // concurrent newer save (stale-clone race).
                                // The returned cp is the durably written state.
                                match store
                                    .save_captured(Box::new(|| lock_checkpoint(&ckpt).clone()))
                                    .await
                                {
                                    Ok(cp) => {
                                        tracing::info!(
                                            "Checkpoint saved (bg): lsn={} ddl_id={}",
                                            cp.last_committed_lsn, cp.last_ddl_id,
                                        );
                                        last_saved.store(cp.last_committed_lsn, Ordering::Release);
                                        last_saved_ddl.store(cp.last_ddl_id, std::sync::atomic::Ordering::Release);
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_secs())
                                            .unwrap_or(0);
                                        saved_at.store(now, Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        let cp = lock_checkpoint(&ckpt).clone();
                                        tracing::error!(
                                            "Background checkpoint save failed: {:?} (lsn={}, ddl_id={})",
                                            e, cp.last_committed_lsn, cp.last_ddl_id,
                                        );
                                        saver_errors.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                });

                match task.await {
                    Ok(()) => break, // shutdown path broke the inner loop
                    Err(e) => {
                        if e.is_panic() {
                            restart_count += 1;
                            if restart_count > MAX_SAVER_RESTARTS {
                                tracing::error!(
                                    "Background checkpoint saver panicked {} times (>{} cap) — \
                                 giving up. Checkpoint will only be saved on shutdown.",
                                    restart_count,
                                    MAX_SAVER_RESTARTS,
                                );
                                bg_error_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            tracing::error!(
                            "Background checkpoint saver panicked ({}/{}): {:?} — restarting in 5s",
                            restart_count, MAX_SAVER_RESTARTS, e.into_panic()
                        );
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        } else {
                            break;
                        }
                    }
                }
            }
        })
    };

    tracing::info!("CDC streaming started");

    // ── CDC: outer reconnect loop with exponential backoff ──
    let mut reconnect_count = 0u64;
    // Set when a CDC cycle ends via the event-error path. The post-connect
    // backoff reset below must not fire for such a cycle: a recurring event
    // error (e.g. a schema mismatch the relation reload cannot fix) would
    // otherwise reconnect at full speed forever with a reset retry_delay.
    let mut last_cycle_event_error = false;
    let mut retry_delay = Duration::from_millis(config.pipeline.reconnect_delay_ms);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
    let mut max_processed_lsn: u64 = 0;
    // Highest LSN of a *successfully* flushed batch (set only after
    // `flush_transaction` returns Ok and the in-memory checkpoint is advanced).
    // Used at shutdown so we never advance the persisted checkpoint past
    // what StarRocks actually received — if the final pending flush fails, we
    // fall back to this safe LSN so the failed batch replays on restart
    // (at-least-once) instead of being permanently lost.
    let mut last_flushed_lsn: u64 = 0;
    // When true, the memory limit check is suppressed for the first batch
    // after a reconnect. This prevents an infinite reconnect loop when
    // replayed events (from a prior mid-batch break) trigger the limit
    // again. The flag is cleared after the first full batch completes.
    let mut after_mid_batch_reconnect = false;
    loop {
        // Ensure the replication slot and publication exist on every
        // reconnection attempt. This handles the case where a user
        // accidentally dropped the slot or publication while the
        // pipeline was running — they'll be recreated automatically.
        //
        // Skip on the first iteration (reconnect_count == 0): the slot
        // was already ensured (or freshly created via --fresh) just above
        // in the startup path. Re-running pg_create_logical_replication_slot
        // here would emit a spurious "already exists" WARN ~1.5s after
        // startup for no benefit.
        if reconnect_count > 0 {
            if let Err(e) = ensure_replication_slot(&config.postgres, &pg_tls).await {
                if is_permanent_pg_error(&*e) {
                    tracing::error!(
                        "Failed to ensure replication slot with permanent error (auth/permission) — exiting: {:?}",
                        e,
                    );
                    return Err(e).context("Permanent replication slot failure (auth/permission)");
                }
                tracing::error!("Failed to ensure replication slot (will retry): {:?}", e,);
                tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                continue;
            }
        }

        // Cross-host guard (Point B): re-check slot active_pid before
        // attempting to attach. This catches the case where another
        // instance started during our snapshot phase (which can take
        // minutes to hours for large databases). The Point A pre-check
        // before the snapshot only covered the start; this check covers
        // the entire pre-connect window. PG's 55006 rejection at
        // CdcConsumer::connect (Point C below) is the final atomic
        // fallback for any residual TOCTOU race between this check and
        // the connect call.
        if let Some(pid) =
            slot_active_pid(&config.postgres, &pg_tls, &config.postgres.slot_name).await?
        {
            return Err(anyhow::anyhow!(
                "Replication slot '{}' became active (PID={}) while this \
                 instance was running — another pipeline instance has taken \
                 over the slot. Exiting to prevent silent duplicate processing. \
                 To resume, stop the other instance and restart.",
                config.postgres.slot_name,
                pid,
            ));
        }

        let resume_lsn = lock_checkpoint(&checkpoint).last_committed_lsn as i64;

        // Clamp max_event_bytes to SR's Stream Load body limit. SR's
        // `streaming_load_max_batch_size_mb` defaults to 100MB and the flush
        // path chunks batches to ≤90MB (`SR_STREAM_LOAD_MAX_BYTES`), but a
        // SINGLE row larger than 90MB forms its own chunk. Without this clamp
        // (default 512MB pending → 128MB event cap), a single event in
        // (100MB, 128MB) would pass the pipeline's poison-pill guard yet be
        // deterministically rejected by SR — an infinite flush→reconnect→
        // replay loop with no FATAL exit. Capping at 90MB routes such events
        // to the existing "exceeds max_event_bytes" FATAL path in cdc.rs.
        const MAX_EVENT_BYTES_SR_CAP: usize = 90 * 1024 * 1024;
        let max_event_bytes = (config.pipeline.max_pending_memory_mb * 1024 * 1024 / 4)
            .clamp(1024 * 1024, MAX_EVENT_BYTES_SR_CAP);
        let mut consumer = match CdcConsumer::connect(
            &config.postgres,
            resume_lsn,
            ddl_table,
            config.pipeline.max_tx_events,
            max_event_bytes,
        )
        .await
        {
            Ok(c) => {
                tracing::info!("Connected to PG replication (resume LSN={})", resume_lsn);
                c
            }
            Err(e) => {
                if is_permanent_pg_error(&*e) {
                    tracing::error!(
                        "CDC connect failed with permanent error (auth/permission) — exiting: {:?}",
                        e,
                    );
                    return Err(e).context("Permanent CDC connection failure (auth/permission)");
                }
                // Point C: PG's 55006 "replication slot is active for PID X"
                // is the atomic fallback for the cross-host single-instance
                // guard. The Point A (pre-snapshot) and Point B (pre-connect)
                // active_pid checks are best-effort; PG's slot invariant is
                // the real lock. Previously this case fell through to the
                // transient-retry path and spun forever (3s→30s backoff)
                // because the slot would never become free while the other
                // instance kept streaming. Treat it as fatal misconfiguration
                // instead.
                if is_slot_in_use_error(&*e) {
                    tracing::error!(
                        "Replication slot '{}' is already in use by another \
                         process (55006). Another pipeline instance is \
                         actively streaming from this slot — exiting instead \
                         of spinning. Use a unique slot_name, or stop the \
                         other instance and restart.",
                        config.postgres.slot_name,
                    );
                    return Err(e).context(
                        "Replication slot already active for another PID (55006) \
                         — refusing to retry forever",
                    );
                }
                tracing::error!("Connect failed: {:?}, retrying in {:?}...", e, retry_delay);
                tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                continue;
            }
        };

        // Load relation cache with PK metadata from PG catalog.
        // Needed on resume (PG doesn't re-send Relation messages) and on fresh
        // start (WAL Relation events don't carry PK metadata). Without this,
        // pk_columns would be empty and DELETE for PRIMARY KEY tables would
        // be incorrectly skipped.
        if let Err(e) = consumer.load_relations().await {
            if is_permanent_pg_error(&*e) {
                tracing::error!(
                        "Failed to load relations with permanent error (auth/permission) — exiting: {:?}",
                        e,
                    );
                return Err(e).context("Permanent relation load failure (auth/permission)");
            }
            tracing::error!(
                "Failed to load relations from PG: {:?} — reconnecting in {:?}",
                e,
                retry_delay
            );
            reconnect_count += 1;
            metrics
                .reconnect_count
                .store(reconnect_count, Ordering::Relaxed);
            tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
            retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
            continue;
        }

        // Inject synthetic Relation events from load_relations() into the
        // pipeline's cache so pk_columns are populated before CDC starts.
        // Errors are logged (not fatal) — if cache priming fails, the first
        // CDC Relation event will repopulate it.
        for event in consumer.drain_tx_events() {
            if let Err(e) = pipeline.process_event(&event).await {
                tracing::warn!("Failed to prime relation cache with event (will retry on first CDC event): {:?}", e);
            }
        }

        // Startup recovery: replay leftover wal-buffer segments from a
        // previous run (crash mid-pause). The relation cache is now loaded,
        // so replay can resolve current schemas; a re-running DDL poller will
        // re-acquire write pauses for tables whose jobs are still active, and
        // the replayer's per-iteration ddl_active check yields to them.
        pipeline.kick_replays();

        // Only reset backoff after both connect AND relation cache load succeed
        // — and only when the previous cycle did not end in an event error, so
        // the backoff keeps growing across a recurring event-error reconnect
        // loop instead of snapping back to the floor on every fresh attach.
        if !last_cycle_event_error {
            retry_delay = Duration::from_millis(config.pipeline.reconnect_delay_ms);
        }
        last_cycle_event_error = false;

        // ── CDC: inner event loop (batched for throughput) ──
        let batch_rows = config.pipeline.batch_rows;
        let batch_timeout = Duration::from_millis(config.pipeline.batch_timeout_ms);
        let max_batch_bytes = config.pipeline.max_pending_memory_mb * 1024 * 1024;
        // DDL poller watchdog: check liveness every 30s. If the supervisor
        // task itself died (not just an inner restart — the supervisor handles
        // those), the pipeline must exit rather than silently continue without
        // schema sync. Uses is_finished() which is non-blocking + zero-cost.
        let mut ddl_watchdog_timer = std::time::Instant::now();

        loop {
            // DDL poller supervisor watchdog check
            if ddl_watchdog_timer.elapsed() >= Duration::from_secs(30) {
                ddl_watchdog_timer = std::time::Instant::now();
                if let Some(ref h) = ddl_handle {
                    if h.is_finished() && !*shutdown_rx.borrow() {
                        // Supervisor task died unexpectedly — not a clean shutdown.
                        // Schema sync has stopped silently. Continuing would cause
                        // silent schema drift (DML keeps flowing but no new DDLs
                        // propagate). FATAL exit is the safe choice.
                        tracing::error!(
                            "FATAL: DDL poller supervisor task died. \
                             Schema sync has stopped — pipeline will exit \
                             to prevent silent schema drift."
                        );
                        return Err(anyhow::anyhow!(
                            "DDL poller supervisor task died unexpectedly"
                        ));
                    }
                }
                // Checkpoint-saver supervisor watchdog. If the saver
                // exhausted its (now ~10) restart budget after panics, the
                // checkpoint won't be persisted — PG WAL grows unbounded and a
                // crash loses everything since the last successful save. Only
                // detectable via this liveness check (the saver has no metrics
                // error of its own). Bail out so the operator restarts fresh.
                if cp_saver_handle.is_finished() && !*shutdown_rx.borrow() {
                    tracing::error!(
                        "FATAL: checkpoint saver supervisor task died (likely exhausted \
                         MAX_SAVER_RESTARTS after repeated panics). Checkpoints are no \
                         longer being persisted — pipeline will exit to prevent silent \
                         WAL growth and crash data loss."
                    );
                    return Err(anyhow::anyhow!(
                        "Checkpoint saver supervisor task died unexpectedly"
                    ));
                }
            }
            // Read a batch of transactions
            let batches = match consumer
                .read_transaction_batch(
                    shutdown_rx.clone(),
                    batch_rows,
                    batch_timeout,
                    max_batch_bytes,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    // Irrecoverable poison-pill conditions — exit the process
                    // instead of reconnecting into an infinite loop.
                    //
                    // 1. WAL corruption (parse errors) — operator must advance
                    //    the slot or restore from backup.
                    // 2. Single event exceeds max_event_bytes — operator must
                    //    increase max_pending_memory_mb and restart, OR remove
                    //    the offending row(s) from PG. Reconnecting just re-reads
                    //    the same oversized event from the same LSN forever.
                    //    This is a poison pill: RECONNECT CANNOT ADVANCE PAST IT.
                    let is_fatal_parse_err =
                        err_str.contains("Too many consecutive pgoutput parse errors");
                    let is_fatal_oversize = err_str.contains("exceeds max_event_bytes limit");
                    // 3. Publication dropped mid-run (or TOCTOU between the
                    //    early publication_exists() check and START_REPLICATION).
                    //    SQLSTATE 42704 from START_REPLICATION is not transient —
                    //    reconnecting just re-issues the same START_REPLICATION
                    //    against the same missing publication forever. Exit so
                    //    the operator can recreate it and restart.
                    let is_fatal_pub_missing = is_publication_missing_error(&*e);
                    if is_fatal_parse_err {
                        tracing::error!(
                            "FATAL: corrupt WAL stream — {} — exiting. \
                             Manually advance the replication slot past the corrupt record \
                             or restore from backup before restarting.",
                            e,
                        );
                        metrics.error_count.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                    if is_fatal_oversize {
                        tracing::error!(
                            "FATAL: poison-pill WAL event — {} — exiting. \
                             The pipeline cannot advance past a single WAL event larger than \
                             max_event_bytes (capped at 90MB = StarRocks' Stream Load body \
                             limit margin). If the event is BELOW 90MB, increase [pipeline] \
                             max_pending_memory_mb (current value: {} MB) and restart. If it \
                             exceeds 90MB, remove the oversized row(s) from PostgreSQL, OR \
                             manually advance the replication slot past this LSN with \
                             `SELECT pg_replication_slot_advance('pg2sr_cdc_slot', 'X/Y')` \
                             (data loss — use with caution) before restarting.",
                            e,
                            config.pipeline.max_pending_memory_mb,
                        );
                        metrics.error_count.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                    if is_fatal_pub_missing {
                        // Use the same actionable message as the early
                        // publication_exists() check — names the database
                        // (database-local hint), the CREATE PUBLICATION command,
                        // and the manage_publication config alternative.
                        let msg = missing_publication_error_msg(
                            &config.postgres.publication,
                            &config.postgres.database,
                        );
                        tracing::error!(
                            "FATAL: publication '{}' missing — {}. Exiting.",
                            config.postgres.publication,
                            msg,
                        );
                        metrics.error_count.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                    tracing::error!(
                        "read_transaction_batch: {:?}, reconnecting in {:?}...",
                        e,
                        retry_delay
                    );
                    reconnect_count += 1;
                    metrics
                        .reconnect_count
                        .store(reconnect_count, Ordering::Relaxed);
                    tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                    retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                    break;
                }
            };

            // Empty means shutdown or idle — advance the slot so PG can
            // discard WAL that the pipeline no longer needs.
            //
            // Two LSNs are tracked separately (see idle_ack_lsn):
            //  - **Checkpoint** — advances to max_known_lsn() (excludes
            //    KeepAlive wal_end per the LSN-gating rule — must not crash-skip un-flushed txns).
            //  - **PG ACK** (confirmed_flush_lsn) — can safely advance to the
            //    KeepAlive wal_end when the pipeline has no buffered pending
            //    events, because the walsender has already decoded all WAL up
            //    to wal_end and delivered any replicated-table transactions.
            //    This keeps replication lag bounded during idle periods —
            //    without it, PG accumulates system WAL (autovacuum, checkpoints)
            //    unboundedly between data events.
            if batches.is_empty() {
                if *shutdown_rx.borrow() {
                    tracing::info!("CDC stream ended (shutdown)");
                    break;
                }
                // Replay kick for idle streams: leftover buffered segments
                // (DDL pause ended while the stream was quiet) must still be
                // drained. flush_transaction only kicks when batches exist.
                pipeline.kick_replays();
                // Skip idle ACK during mid-batch reconnect: un-processed events
                // from the memory limit break are still in the WAL. If we ACK
                // past them, PG discards the WAL and we lose data on restart.
                if after_mid_batch_reconnect {
                    tracing::debug!(
                        "Skipping idle ACK during mid-batch reconnect (max_processed_lsn={})",
                        max_processed_lsn
                    );
                    continue;
                }
                // Advance the in-memory checkpoint to the latest commit or
                // filtered LSN.
                // Use the checkpoint's last_committed_lsn as a floor: on a
                // fresh connect, consumer.last_commit_lsn starts at 0 (no
                // Commit events processed yet), but the checkpoint may have a
                // non-zero LSN from a prior run. Without this floor, idle_lsn
                // would be 0 → the entire idle ACK is skipped → confirmed_flush_lsn
                // never advances → lag grows unboundedly.
                let idle_lsn = consumer
                    .max_known_lsn()
                    .max(lock_checkpoint(&checkpoint).last_committed_lsn as i64);
                let wal_end = consumer.last_wal_end();
                // has_pending must ALSO cover a transaction open in the
                // consumer: the batch-timeout arm returns an empty batch while
                // a txn is mid-flight (its changes are in consumer RAM, not
                // the pipeline), and PG's keepalives advance wal_end THROUGH
                // such transactions. ACKing to wal_end in that window would
                // let PG discard WAL the pipeline still needs — at-least-once
                // violation (crash = lost txn, graceful restart = silently
                // skipped via the slot-ahead auto-advance).
                let has_pending = pipeline.has_pending() || consumer.has_in_flight_tx();
                if idle_lsn > 0 || wal_end > 0 {
                    if idle_lsn > 0 {
                        let mut guard = lock_checkpoint(&checkpoint);
                        if idle_lsn > guard.last_committed_lsn as i64 {
                            guard.last_committed_lsn = idle_lsn as u64;
                        }
                    }
                    // Acquire: pairs with the saver's Release stores so the
                    // durable checkpoint write is visible before we ACK to PG.
                    let saved = last_saved_lsn.load(Ordering::Acquire);
                    let ack_lsn = idle_ack_lsn(idle_lsn, wal_end, saved, has_pending);
                    if ack_lsn > 0 {
                        tracing::debug!(
                            "Idle ACK: advancing slot to LSN={} (checkpoint={}, wal_end={}, \
                             has_pending={}, saved={})",
                            ack_lsn,
                            idle_lsn,
                            wal_end,
                            has_pending,
                            saved,
                        );
                        consumer.acknowledge_lsn(ack_lsn);
                    }
                }
                continue;
            }

            let batch_count = batches.len();
            let batch_max_lsn = batches.iter().map(|(lsn, _)| *lsn).max().unwrap_or(0) as u64;

            // Process all transactions — accumulate into single pipeline buffer
            let flush_start = Instant::now();
            let mut event_error = false;
            let mut processed_event_count: usize = 0;
            let mut hard_limit_hit: bool = false;

            pipeline.begin_transaction();
            for (_commit_lsn, events) in &batches {
                for event in events {
                    if let Err(e) = pipeline.process_event(event).await {
                        tracing::error!("process_event: {:?}, aborting batch", e);
                        // Surface event-processing errors in /metrics error_count
                        metrics.error_count.fetch_add(1, Ordering::Relaxed);
                        event_error = true;
                        break;
                    }
                    processed_event_count += 1;
                    // Yield to the Tokio reactor so shutdown signals, keepalive status
                    // updates, and background tasks are not starved during large batches
                    // with heavy CPU-bound JSON conversion (columns_to_json).
                    tokio::task::yield_now().await;
                    // If the memory limit was exceeded during this event, force
                    // a flush immediately instead of reading more WAL events.
                    // Two tiers:
                    //   1. Soft limit (flush_requested) — suppressed during replay
                    //      batches to avoid infinite reconnect loops.
                    //   2. Hard limit (hard_flush_requested) — always enforced to
                    //      prevent OOM even on replay batches.  Set at 2× the
                    //      configured max_pending_bytes.
                    if pipeline.hard_flush_requested() {
                        hard_limit_hit = true;
                        tracing::error!(
                            "Pipeline HARD memory limit hit ({} bytes, limit {}) — forcing flush",
                            pipeline.pending_bytes(),
                            config.pipeline.max_pending_memory_mb * 1024 * 1024,
                        );
                        break;
                    }
                    if !after_mid_batch_reconnect && pipeline.flush_requested() {
                        tracing::warn!(
                            "Pipeline SOFT memory limit hit ({} bytes), flushing early",
                            config.pipeline.max_pending_memory_mb * 1024 * 1024
                        );
                        break;
                    }
                }
                // Only advance max_processed_lsn if ALL events in this
                // transaction were processed. A memory-limit break mid-transaction
                // means the transaction is incomplete — don't tell PG we've
                // consumed it, so the mid-batch break check triggers a reconnect
                // to replay the full transaction.
                let broke_mid_transaction = !event_error
                    && (pipeline.hard_flush_requested()
                        || (!after_mid_batch_reconnect && pipeline.flush_requested()));
                if !event_error && !broke_mid_transaction {
                    // During a replay batch (after_mid_batch_reconnect), the
                    // soft limit is suppressed in the inner loop (above) to
                    // avoid infinite reconnect loops. Without this incremental
                    // flush, committed transactions accumulate until the 2×
                    // HARD limit, which trips the FATAL exit below and loses
                    // the un-flushed tail — even when every individual
                    // transaction is well under the limit. Flush the committed
                    // backlog here, at a transaction boundary, to drain the
                    // replay in bounded memory *without* reconnecting (so it
                    // can't loop). A single genuinely-oversized transaction
                    // (> 2× cap) still breaks via the hard-limit check above.
                    //
                    // CRITICAL: advance max_processed_lsn ONLY after the flush
                    // succeeds. If we advance first and the flush fails, the
                    // mid-batch ACK path would persist + ACK past the failed
                    // transaction, and PG would discard the WAL — permanent
                    // data loss.
                    if after_mid_batch_reconnect && pipeline.flush_requested() {
                        // mark_shutdown_seen() prevents flush_transaction's
                        // shutdown_rx.changed() select arm from bailing and
                        // dropping the in-flight buffer (which is already
                        // moved out via std::mem::take inside flush_transaction).
                        if *shutdown_rx.borrow() {
                            pipeline.mark_shutdown_seen();
                        }
                        // Use the same flush wrapper as the main flush path —
                        // DDL-paused tables durably buffer instead of
                        // deferring, so there is no retry loop.
                        match flush_batch(&mut pipeline, *_commit_lsn).await {
                            FlushOutcome::Flushed => {}
                            FlushOutcome::Failed(e) => {
                                tracing::error!(
                                    "Incremental replay flush failed at LSN {} (max_processed_lsn={}): {:?}, reconnecting...",
                                    *_commit_lsn, max_processed_lsn, e
                                );
                                break;
                            }
                        }
                    }
                    max_processed_lsn = (*_commit_lsn as u64).max(max_processed_lsn);
                }
                if event_error {
                    break;
                }
                if pipeline.hard_flush_requested() {
                    break;
                }
                if !after_mid_batch_reconnect && pipeline.flush_requested() {
                    break;
                }
            }

            if event_error {
                // Pace this like the sibling error arms. Without the sleep and
                // backoff growth, a recurring event error (e.g. a schema
                // mismatch the relation reload cannot fix) loops at full speed
                // — several new PG connections per iteration, no checkpoint
                // progress — and stays invisible in /metrics because this path
                // never touched reconnect_count.
                reconnect_count += 1;
                metrics
                    .reconnect_count
                    .store(reconnect_count, Ordering::Relaxed);
                tracing::warn!(
                    "Batch aborted due to event error, reconnecting in {:?}...",
                    retry_delay
                );
                tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                last_cycle_event_error = true;
                break;
            }

            // Refresh PK info for new relations that arrived via WAL Relation events
            // (which don't carry PK metadata). Without this, tables created after
            // startup would have empty pk_columns in the pipeline's cache.
            if let Err(e) = pipeline.refresh_relation_pks().await {
                tracing::warn!("Failed to refresh relation PKs: {:?}", e);
            }

            // Single flush for entire batch
            let commit_lsn = max_processed_lsn as i64;

            // mark_shutdown_seen() prevents flush_transaction's
            // shutdown_rx.changed() select arm from bailing and dropping the
            // buffer (already moved via std::mem::take) when shutdown arrives
            // between read_transaction_batch() and flush_transaction().
            if *shutdown_rx.borrow() {
                pipeline.mark_shutdown_seen();
            }
            // Durable buffering: tables with an in-flight SR schema-change
            // job, leftover buffered segments, OR a failed per-table flush
            // (Stream Load error / task panic / flush-timeout abort) have
            // their changes durably appended to the wal_buffer inside
            // flush_transaction — the flush returns Ok and the checkpoint
            // advances past durably buffered events. No in-memory deferral,
            // no retry loop, no reconnect: the replay path drains segments
            // once SR is healthy again.
            match flush_batch(&mut pipeline, commit_lsn).await {
                FlushOutcome::Flushed => {
                    // Fall through to the mid-batch-break checks below.
                }
                FlushOutcome::Failed(e) => {
                    tracing::warn!(
                        "Flush failed at LSN {}: {:?}, reconnecting...",
                        max_processed_lsn,
                        e
                    );
                    // Surface flush failures in /metrics error_count
                    metrics.error_count.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }

            // If the memory limit triggered a mid-batch break, some transactions
            // were consumed from the WAL stream but not processed. Reconnect so
            // PG re-sends them from the checkpoint LSN — without this, the next
            // batch would read past them and they'd be lost.
            if max_processed_lsn < batch_max_lsn {
                // If the hard memory limit triggered during a replay batch
                // (after_mid_batch_reconnect was already true), reconnecting
                // will re-send the same oversized events and loop forever.
                // There is no recovery — the transaction exceeds 2× the
                // configured max_pending_memory_mb. Exit so the operator
                // can raise the limit or split the transaction.
                if hard_limit_hit && after_mid_batch_reconnect {
                    tracing::error!(
                        "FATAL: HARD memory limit ({} bytes, limit {}) hit during replay batch — \
                         the replayed transaction exceeds 2× max_pending_memory_mb. \
                         Increase max_pending_memory_mb or split the oversized transaction. \
                         Exiting.",
                        pipeline.pending_bytes(),
                        config.pipeline.max_pending_memory_mb * 1024 * 1024,
                    );
                    return Err(anyhow::anyhow!(
                        "HARD memory limit during replay batch — exiting to prevent infinite reconnect loop"
                    ));
                }
                after_mid_batch_reconnect = true;
                tracing::debug!(
                    "Mid-batch break (processed LSN={max_processed_lsn} < batch_max={batch_max_lsn}) — reconnecting to replay skipped events"
                );
                // Durably advance the replication slot past the transactions we
                // just flushed BEFORE reconnecting. Without this, PG re-sends
                // the already-committed events on the replay batch; if their
                // cumulative size exceeds the 2× hard limit, the replay trips
                // the FATAL exit above and the genuinely-unprocessed tail is
                // lost. flush_transaction() already persisted these rows to
                // StarRocks, so ACKing past the saved checkpoint is safe — a
                // crash before the next periodic save replays from this LSN
                // (at-least-once, idempotent by PRIMARY KEY).
                if max_processed_lsn > 0 {
                    {
                        let mut guard = lock_checkpoint(&checkpoint);
                        if max_processed_lsn > guard.last_committed_lsn {
                            guard.last_committed_lsn = max_processed_lsn;
                        }
                    }
                    let cp = lock_checkpoint(&checkpoint).clone();
                    match checkpoint_store.save(&cp).await {
                        Ok(()) => {
                            last_saved_lsn.store(max_processed_lsn, Ordering::Release);
                            consumer.acknowledge_lsn(max_processed_lsn);
                            last_flushed_lsn = max_processed_lsn;
                            tracing::info!(
                                "Mid-batch break: saved checkpoint + ACKed LSN {} before reconnect",
                                max_processed_lsn
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Checkpoint save before mid-batch reconnect failed (non-fatal, \
                                 committed events will replay): {:?}",
                                e
                            );
                        }
                    }
                }
                // On reconnect, the memory limit check is suppressed for the
                // first batch to let the replay events through without another
                // mid-batch break (which would cause an infinite loop). The
                // flag resets after the replay batch completes successfully.
                break;
            }
            if after_mid_batch_reconnect {
                after_mid_batch_reconnect = false;
                tracing::debug!("Replay batch completed — re-enabling memory limit checks");
            }

            // Include any filtered-only LSNs so WAL progresses past
            // events from tables outside table_filter.
            let filtered_lsn = consumer.drain_filtered_lsn() as u64;
            let max_lsn = max_processed_lsn.max(filtered_lsn);

            // Update in-memory checkpoint + acknowledge WAL once per batch.
            // ACK gated on last durable save: never tell PG we've consumed
            // WAL past what's been durably checkpointed. The full max_lsn is
            // saved to in-memory checkpoint; the background saver persists it
            // on the next tick, and subsequent ACKs catch up.
            //
            // Record `last_flushed_lsn` ONLY after the batch flush succeeded
            // (we reached here past `flush_transaction`), so the shutdown
            // path can safely fall back to it if the final pending flush fails.
            let flush_ms = flush_start.elapsed().as_millis() as u64;
            if max_lsn > 0 {
                last_flushed_lsn = max_lsn;
                {
                    let mut guard = lock_checkpoint(&checkpoint);
                    if max_lsn > guard.last_committed_lsn {
                        guard.last_committed_lsn = max_lsn;
                    }
                }
                // Acquire: pairs with the saver's Release stores so the
                // durable checkpoint write is visible before we ACK to PG.
                let saved = last_saved_lsn.load(Ordering::Acquire);
                let safe_ack = max_lsn.min(saved);
                if safe_ack > 0 {
                    consumer.acknowledge_lsn(safe_ack);
                }
            }

            metrics.record_batch(processed_event_count as u64, flush_ms, max_lsn);

            // Record the per-batch flush latency in the
            // `metrics`-crate histogram so p50/p95/p99 are exposed via
            // /metrics (in addition to the existing cumulative average gauge).
            metrics::histogram!("pg2sr_cdc_flush_latency_ms").record(flush_ms as f64);

            if batch_count > 0 {
                tracing::debug!(
                    batch_count = batch_count,
                    rows = processed_event_count,
                    flush_ms = flush_ms,
                    lsn = max_lsn,
                    "Flushed batch",
                );
            }
        }

        // Check if we should exit (not just reconnect)
        if *shutdown_rx.borrow() {
            break;
        }
    }

    // ── Shutdown ──
    // Conservative LSN advance: set the in-memory checkpoint to the highest
    // LSN we know was *successfully flushed* to StarRocks (not max_processed_lsn,
    // which may include events from a batch that was processed in-memory but
    // never flushed — e.g. shutdown arrived mid-batch before flush_transaction).
    // Advancing to max_processed_lsn here would let the background saver persist
    // a checkpoint past un-flushed events; on restart the pipeline would resume
    // past them → permanent data loss. We'll re-advance after the final
    // pending flush below, and only if it succeeds.
    {
        let mut guard = lock_checkpoint(&checkpoint);
        if last_flushed_lsn > guard.last_committed_lsn {
            guard.last_committed_lsn = last_flushed_lsn;
        }
    }

    // Flush any pending events that were buffered but not yet committed to SR.
    // This handles the edge case where shutdown arrives between begin_transaction()
    // and flush_transaction() — e.g., SIGTERM during event processing.
    // If the flush fails, the events will be replayed from the last checkpoint
    // (at-least-once semantics).
    if pipeline.has_pending() {
        tracing::info!("Flushing pending CDC events before shutdown...");
        // Mark the shutdown signal as "seen" so flush_transaction() won't
        // immediately bail on shutdown_rx.changed(). Without this, the common
        // case (shutdown arrived between flushes) would cause the final flush
        // to fail because changed() fires immediately.
        pipeline.mark_shutdown_seen();
        match pipeline.flush_transaction(0).await {
            Ok(()) => {
                // The final flush succeeded — the pending events are now in
                // StarRocks, so it is safe to advance the checkpoint to
                // max_processed_lsn (reducing replay on restart).
                {
                    let mut guard = lock_checkpoint(&checkpoint);
                    if max_processed_lsn > guard.last_committed_lsn {
                        guard.last_committed_lsn = max_processed_lsn;
                    }
                }
                // Persist the checkpoint after a successful final flush so the
                // LSN advances past the flushed events, reducing replay on
                // restart. Capture under the store lock (stale-clone race).
                match checkpoint_store
                    .save_captured(Box::new(|| lock_checkpoint(&checkpoint).clone()))
                    .await
                {
                    Ok(cp) => {
                        // Keep last_saved_lsn fresh (no ACK follows, but a
                        // stale value is a future footgun) — Release
                        // pairs with the (absent here) Acquire loads in the
                        // ACK paths. Uses the DURABLY saved cp.
                        last_saved_lsn.store(cp.last_committed_lsn, Ordering::Release);
                        tracing::info!(
                            "Checkpoint saved after final shutdown flush (lsn={})",
                            cp.last_committed_lsn
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to save checkpoint after final shutdown flush: {:?}",
                            e,
                        );
                    }
                }
                // Note: We cannot ACK to PG here because the CdcConsumer was
                // dropped when the CDC loop broke. The PG slot's
                // confirmed_flush_lsn will lag behind the checkpoint until the
                // next pipeline start. This is harmless: PG retains the WAL
                // between confirmed_flush_lsn and the checkpoint, and on
                // restart the pipeline resumes from the saved checkpoint
                // (at-least-once semantics, no data loss).
            }
            Err(e) => {
                // The final flush FAILED — do NOT leave the checkpoint advanced
                // past already-flushed events. Revert to last_flushed_lsn so the
                // background saver persists only the safe LSN and the failed
                // batch replays on restart (at-least-once) instead of being
                // permanently lost.
                {
                    let mut guard = lock_checkpoint(&checkpoint);
                    if last_flushed_lsn < guard.last_committed_lsn {
                        guard.last_committed_lsn = last_flushed_lsn;
                    }
                }
                tracing::warn!(
                    "Final pending flush failed (non-fatal, events will replay): {:?}",
                    e,
                );
            }
        }
    }

    // Drain in-flight wal-buffer replay tasks (runs regardless of
    // has_pending — replay tasks are not part of the pipeline's pending
    // batch). Tasks that don't finish within the flush timeout leave their
    // segments on disk; startup scan re-kicks replay (at-least-once).
    tracing::info!("Draining wal buffer replay tasks before shutdown...");
    pipeline.drain_replays().await;

    metrics.report(started);

    // Await the background checkpoint saver's final save. It already received
    // the shutdown signal and is saving; we wait up to 10s for it to complete.
    // On timeout we log a warning — the next restart replays from the last
    // periodic save (at-least-once, no data loss).
    match tokio::time::timeout(Duration::from_secs(10), cp_saver_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("Background checkpoint saver panicked: {:?}", e);
        }
        Err(_) => {
            tracing::warn!("Background checkpoint saver did not finish within 10s — final save may be incomplete");
        }
    }

    // Abort background tasks (metrics, summary, DDL poller).
    // The
    // outer DDL supervisor task is wrapped in `AbortOnDrop(inner_handle)` at
    // main.rs:1270, mirroring `ddl_poller.rs:158`. The unload chain on
    // outer abort (here at :2294): outer task is cancelled → its
    // `inner_guard: AbortOnDrop` drops → `.abort()` on
    // `run_ddl_poller_loop` → its `_task_guard` (`ddl_poller.rs:158`)
    // drops → `.abort()` on `run_ddl_poller` → the in-flight
    // `conn.query_drop` future (`sr_connection.rs:585`) drops in place
    // (mysql_async supports mid-query drop — the per-query
    // `tokio::time::timeout` wrapper already relies on this). So the
    // detach race is closed. The 2s `tokio::time::timeout` below remains
    // as a graceful-drain window before the forceful abort + sr_mgr.shutdown().
    // JoinHandle is Unpin, so &mut handle is a valid Future (consumed only
    // on poll; we keep the handle alive to abort() if the timeout fires).
    let _ = tokio::time::timeout(Duration::from_secs(2), &mut metrics_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), &mut summary_handle).await;
    metrics_handle.abort();
    summary_handle.abort();
    if let Some(mut h) = ddl_handle {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut h).await;
        h.abort();
    }

    // Gracefully shut down MySQL connection pools
    sr_mgr.shutdown().await;

    // Release the distributed single-instance lock (explicit unlock;
    // the session close releases it regardless).
    pg_lock_guard.release().await;

    tracing::info!("Shutdown complete");

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// Table schema validation (fail-fast startup check)
// ─────────────────────────────────────────────────────────────────────

const BYTEA_OID: u32 = 17;

fn has_bytea_columns(
    all_columns: &std::collections::HashMap<u32, Vec<pg_catalog::CatalogColumn>>,
) -> bool {
    all_columns
        .values()
        .any(|cols| cols.iter().any(|c| c.type_oid == BYTEA_OID))
}

/// Validate that all tables matching the filter have a PRIMARY KEY with
/// SR-compatible column types. Uses batch PG catalog queries so O(1) queries
/// regardless of table count, instead of O(N) queries per table.
async fn validate_replicated_tables(
    pg_cfg: &config::PgConfig,
    table_filter: &str,
    table_exclude: &str,
    ddl_schema: &str,
    ddl_table: &str,
    tls: &PgTlsConnector,
) -> Result<()> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for table validation")?;
    let _conn_guard = AbortOnDrop(conn);

    let filter_re = config::compile_table_filter(table_filter)
        .context("Failed to compile table_filter regex for table validation")?;
    let exclude_re = config::compile_table_exclude(table_exclude)
        .context("Failed to compile table_exclude regex for table validation")?;

    // Batch 1: get all tables with their OIDs in one query
    let all_tables = crate::pg_catalog::fetch_bulk_table_oids(&client, ddl_schema, ddl_table)
        .await
        .context("Failed to query PG tables for validation")?;

    // Filter and collect OIDs for matching tables
    let mut tables_to_check: Vec<(String, String, u32)> = Vec::new();
    let mut filtered_out: usize = 0;

    for (schema, table, oid) in &all_tables {
        let qualified = format!("{}.{}", schema, table);
        // Single-schema enforcement: skip tables in other schemas BEFORE the
        // filter checks, so a no-PK table in an unrelated schema can't block
        // startup with a confusing validation error.
        if schema != &pg_cfg.schema {
            tracing::debug!(
                "TABLE SKIPPED (other schema): {} — postgres.schema='{}'",
                qualified,
                pg_cfg.schema,
            );
            filtered_out += 1;
        } else if !config::table_matches_filter(&filter_re, schema, table) {
            tracing::warn!(
                "TABLE FILTERED OUT: {} — skipped (table_filter='{}' did not match)",
                qualified,
                table_filter,
            );
            filtered_out += 1;
        } else if config::is_table_excluded(&exclude_re, schema, table) {
            tracing::warn!(
                "TABLE EXCLUDED: {} — skipped (matches table_exclude='{}')",
                qualified,
                table_exclude,
            );
            filtered_out += 1;
        } else {
            tables_to_check.push((schema.clone(), table.clone(), *oid));
        }
    }

    if tables_to_check.is_empty() {
        tracing::info!(
            "Table schema validation: no tables matched filter ({} filtered out)",
            filtered_out,
        );
        return Ok(());
    }

    // Batch 2: get ALL columns for ALL matching tables in one query
    let check_oids: Vec<u32> = tables_to_check.iter().map(|t| t.2).collect();
    let all_columns = crate::pg_catalog::fetch_bulk_columns(&client, &check_oids)
        .await
        .context("Failed to query PG columns for validation")?;

    // Batch 3: get ALL primary keys for ALL matching tables in one query
    let all_pks = crate::pg_catalog::fetch_bulk_primary_keys(&client, &check_oids)
        .await
        .context("Failed to query PG primary keys for validation")?;

    // Batch 4: get ALL replica identities for ALL matching tables in one
    // query. pgoutput's UPDATE/DELETE old-key ('K') tuples are only usable
    // with 'd' (DEFAULT → PK) or 'f' (FULL). 'n' (NOTHING) sends no old key,
    // and 'i' (INDEX) sends the identity index's columns — which may not be
    // the PK the pipeline extracts old keys by — so both would silently
    // misbehave (skipped PK-change deletes, no-op DELETEs). Fail startup.
    let all_replidents = crate::pg_catalog::fetch_bulk_replica_identities(&client, &check_oids)
        .await
        .context("Failed to query PG replica identities for validation")?;

    // Check bytea_output setting if any replicated table has BYTEA columns.
    // CDC path assumes hex encoding (pgoutput delivers hex-encoded bytes).
    // escape mode would silently corrupt data.
    if has_bytea_columns(&all_columns) {
        let row = client
            .query_one("SHOW bytea_output", &[])
            .await
            .context("Failed to query bytea_output setting")?;
        let bytea_output: String = row.get(0);
        if bytea_output != "hex" {
            anyhow::bail!(
                "BYTEA columns require `bytea_output = hex` (PG default since 9.0). \
                 Current value: '{}'. Set `bytea_output = hex` in postgresql.conf \
                 or remove BYTEA tables from table_filter.",
                bytea_output,
            );
        }
        tracing::info!("bytea_output verified: hex (BYTEA columns present)");
    }

    // Validate in memory
    let mut errors: Vec<String> = Vec::new();
    let mut no_pk_skipped: usize = 0;
    let mut incompatible_pk_skipped: usize = 0;
    let mut bad_replident_skipped: usize = 0;
    let mut will_process: usize = 0;

    for (schema, table, oid) in &tables_to_check {
        let qualified = format!("{}.{}", schema, table);

        let pk_cols = match all_pks.get(oid) {
            Some(pks) if !pks.is_empty() => pks.clone(),
            _ => {
                // A no-PK table is silently dropped by flush_table (returns
                // Ok without writing) — silent data loss for any writes to it.
                // Refuse to start so the operator adds a PRIMARY KEY or excludes
                // the table via table_filter/table_exclude. Previously this only
                // warned, so the pipeline ran and no-PK writes were lost.
                errors.push(format!(
                    "{}.{}: no PRIMARY KEY — StarRocks requires a PK for CDC. \
                     Add a PRIMARY KEY in Postgres or exclude this table via \
                     [postgres] table_filter / table_exclude",
                    schema, table,
                ));
                no_pk_skipped += 1;
                continue;
            }
        };

        let cols = match all_columns.get(oid) {
            Some(c) => c.clone(),
            None => {
                errors.push(format!("{}.{}: no columns found in catalog", schema, table));
                continue;
            }
        };

        let pk_names: std::collections::HashSet<&str> =
            pk_cols.iter().map(|c| c.as_str()).collect();

        let mut incompatible = false;
        for col in &cols {
            if !pk_names.contains(col.name.as_str()) {
                continue;
            }
            if !crate::types::is_pg_type_sr_pk_compatible(col.type_oid, col.type_modifier) {
                let sr_type = crate::types::pg_type_to_sr(col.type_oid, col.type_modifier);
                // Log the resolved PG type NAME (e.g. "json"), not the
                // numeric OID duplicated twice.
                let pg_type_name = crate::types::pg_type_oid_to_name(col.type_oid);
                tracing::warn!(
                    "TABLE SKIPPED (INCOMPATIBLE PK): {}.{} — PK column '{}' has PG type {} \
                     (OID={}) which maps to SR type '{}' and cannot be a PRIMARY KEY in StarRocks. \
                     Change the PK column type or exclude this table via table_filter",
                    schema,
                    table,
                    col.name,
                    pg_type_name,
                    col.type_oid,
                    sr_type,
                );
                incompatible_pk_skipped += 1;
                incompatible = true;
            }
        }

        if !incompatible {
            // REPLICA IDENTITY must be 'd' (DEFAULT — old key = PK, sufficient
            // for PK tables) or 'f' (FULL). 'n' (NOTHING) → pgoutput sends no
            // old key at all: PK-change DELETEs are silently skipped (orphan
            // rows on SR) and DELETEs become no-ops. 'i' (INDEX) → the old key
            // carries the identity index's columns; unless that index IS the
            // PK, the pipeline's PK-position extraction gets NULLs ("?" keys),
            // emitting spurious deletes. Both are startup errors.
            let replident = all_replidents.get(oid).map(|v| v.as_str()).unwrap_or("?");
            if replident != "d" && replident != "f" {
                errors.push(format!(
                    "{}.{}: REPLICA IDENTITY is '{}' — must be 'd' (DEFAULT) or 'f' (FULL). \
                     Old-key data for UPDATE/DELETE is missing ('n') or not the primary key ('i'), \
                     so PK-change deletes and DELETEs would be wrong. \
                     Fix with `ALTER TABLE {} REPLICA IDENTITY DEFAULT;` \
                     or exclude this table via [postgres] table_filter / table_exclude",
                    schema, table, replident, qualified,
                ));
                bad_replident_skipped += 1;
                incompatible = true;
            }
        }

        if !incompatible {
            tracing::info!(
                "TABLE WILL BE REPLICATED: {} — {} PK column(s): {:?}",
                qualified,
                pk_cols.len(),
                pk_cols,
            );
            will_process += 1;
        }
    }

    tracing::info!(
        "Table validation summary: {} filtered out, {} no-PK skipped, {} incompatible PK skipped, {} bad REPLICA IDENTITY skipped, {} will be processed ({} errors)",
        filtered_out, no_pk_skipped, incompatible_pk_skipped, bad_replident_skipped, will_process, errors.len(),
    );

    if errors.is_empty() {
        tracing::info!(
            "Table schema validation passed: {} tables will be replicated ({} filtered out, {} no-PK skipped, {} incompatible PK skipped, {} bad REPLICA IDENTITY skipped)",
            will_process, filtered_out, no_pk_skipped, incompatible_pk_skipped, bad_replident_skipped,
        );
        Ok(())
    } else {
        let summary = format!(
            "Table schema validation FAILED — {} issue(s) found:\n  - {}",
            errors.len(),
            errors.join("\n  - "),
        );
        Err(anyhow::anyhow!("{}", summary))
    }
}

// ─────────────────────────────────────────────────────────────────────
// Replication slot & publication management
// ─────────────────────────────────────────────────────────────────────

/// Drop and recreate the replication slot and publication on --fresh.
/// The publication is created FOR ALL TABLES so all existing and future
/// tables are automatically included in CDC replication.
async fn reset_replication_slot(pg_cfg: &config::PgConfig) -> Result<()> {
    let conn_str = pg_cfg.conn_str();

    let tls = PgTlsConnector::from_config(&pg_cfg.tls)
        .context("Failed to configure PG TLS for slot reset")?;
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect for slot reset")?;
    let _conn_guard = AbortOnDrop(conn);

    // Terminate any active WAL sender holding the slot from a prior
    // pipeline instance that wasn't cleaned up (zombie after SIGKILL).
    // Retry loop: PG's backend termination is asynchronous — the backend
    // may still be releasing the slot when we try to drop it.
    let term_sql = "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active";
    let drop_sql =
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1";

    for attempt in 0..5 {
        // Terminate any active backend
        match client.execute(term_sql, &[&pg_cfg.slot_name]).await {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(
                        "Terminated {n} zombie WAL sender(s) for slot '{}' (attempt {})",
                        pg_cfg.slot_name,
                        attempt + 1,
                    );
                }
            }
            Err(e) => tracing::debug!("No active WAL sender to terminate: {e}"),
        }

        // Small wait for termination to take effect
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        // Try to drop the slot
        match client.execute(drop_sql, &[&pg_cfg.slot_name]).await {
            Ok(_) => {
                tracing::info!("Dropped existing replication slot '{}'", pg_cfg.slot_name);
                break; // drop succeeded, exit retry loop
            }
            Err(e) => {
                let err_str = format!("{}", e);
                if err_str.contains("active") && attempt < 4 {
                    tracing::warn!(
                        "Slot '{}' still active after terminate (attempt {}), retrying...",
                        pg_cfg.slot_name,
                        attempt + 1,
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                } else {
                    tracing::debug!("Slot '{}' may not exist: {:?}", pg_cfg.slot_name, e);
                    break; // non-active error or exhausted retries
                }
            }
        }
    }

    let create_slot = "SELECT pg_create_logical_replication_slot($1, 'pgoutput')";
    client
        .execute(create_slot, &[&pg_cfg.slot_name])
        .await
        .context(format!(
            "Failed to create replication slot '{}' during --fresh reset",
            pg_cfg.slot_name,
        ))?;
    tracing::info!("Created fresh replication slot '{}'", pg_cfg.slot_name);

    if pg_cfg.manage_publication {
        let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", pg_cfg.publication);
        client.execute(&drop_pub, &[]).await.context(format!(
            "Failed to drop publication '{}' during --fresh reset",
            pg_cfg.publication,
        ))?;

        // WITH (publish=...) includes truncate so TRUNCATE TABLE events
        // are replicated via the WAL stream. Without this, PG's default
        // publication (publish='insert,update,delete') omits truncate,
        // causing silent permanent divergence: PG is empty, SR retains
        // all old rows, and the CDC pipeline's Truncate handler is dead code.
        let create_pub = format!(
            "CREATE PUBLICATION {} FOR ALL TABLES WITH (publish='insert, update, delete, truncate')",
            pg_cfg.publication,
        );
        client.execute(&create_pub, &[]).await.context(format!(
            "Failed to create publication '{}' during --fresh reset",
            pg_cfg.publication,
        ))?;
        tracing::info!(
            "Created publication '{}' for all tables (includes truncate)",
            pg_cfg.publication,
        );
    } else {
        tracing::info!(
            "manage_publication is false — skipping publication setup (assumes DBA-managed)"
        );
        // Verify the publication exists in the configured database. On
        // --fresh, ensure_replication_slot() is skipped (see main.rs startup
        // path), so without this check a missing publication would sail
        // through startup, complete the snapshot, then spin forever in the
        // CDC reconnect loop when START_REPLICATION returns 42704.
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                &[&pg_cfg.publication],
            )
            .await
            .context(format!(
                "Failed to query pg_publication for '{}' during --fresh reset",
                pg_cfg.publication,
            ))?;
        if row.is_none() {
            return Err(anyhow::anyhow!(
                "{}",
                missing_publication_error_msg(&pg_cfg.publication, &pg_cfg.database)
            ));
        }
        tracing::debug!(
            "Publication '{}' verified present in database '{}'",
            pg_cfg.publication,
            pg_cfg.database,
        );
    }

    Ok(())
}

/// Create a replication slot + publication if they don't yet exist.
/// Idempotent — safe to call on every start without --fresh.
/// The publication is created FOR ALL TABLES so all existing and future
/// tables are automatically included in CDC replication.
pub(crate) async fn ensure_replication_slot(
    pg_cfg: &config::PgConfig,
    tls: &PgTlsConnector,
) -> Result<()> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for slot creation")?;
    let _conn_guard = AbortOnDrop(conn);

    // Create slot if it doesn't exist.  Already-exists is the normal case on
    // non-fresh starts and is non-fatal. All other failures are fatal.
    let create_slot = "SELECT pg_create_logical_replication_slot($1, 'pgoutput')";
    match client.execute(create_slot, &[&pg_cfg.slot_name]).await {
        Ok(_) => tracing::info!("Created replication slot '{}'", pg_cfg.slot_name),
        Err(e) => {
            if slot_already_exists_err(&e) {
                tracing::info!("Replication slot '{}' already exists", pg_cfg.slot_name);
            } else {
                return Err(e).context(format!(
                    "Failed to create replication slot '{}'",
                    pg_cfg.slot_name,
                ));
            }
        }
    }

    // Create empty publication if it doesn't exist — only when managing
    // our own publication. When manage_publication is false, the DBA
    // handles it externally.
    if pg_cfg.manage_publication {
        // WITH (publish=...) includes truncate so TRUNCATE TABLE events
        // are replicated via the WAL stream. See --fresh path above for rationale.
        let create_pub = format!(
            "CREATE PUBLICATION {} FOR ALL TABLES WITH (publish='insert, update, delete, truncate')",
            pg_cfg.publication,
        );
        match &client.execute(&create_pub, &[]).await {
            Ok(_) => {
                tracing::info!(
                    "Created publication '{}' for all tables (includes truncate)",
                    pg_cfg.publication,
                );
            }
            Err(e) => {
                if slot_already_exists_err(e) {
                    tracing::info!("Publication '{}' already exists", pg_cfg.publication);
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to create publication '{}': {:?}",
                        pg_cfg.publication,
                        e
                    ));
                }
            }
        };
    } else {
        tracing::debug!(
            "manage_publication is false — skipping publication creation (assumes DBA-managed)"
        );
        // Verify the publication actually exists in the configured database.
        // Publications are database-local: a publication created in a different
        // DB (e.g. `postgres`) is invisible to the replication connection that
        // attaches to pg_cfg.database. Without this check, the pipeline would
        // pass startup, complete the snapshot, then fail at START_REPLICATION
        // with SQLSTATE 42704 and spin forever in the reconnect loop (42704 is
        // treated as transient by is_permanent_pg_error). Fast-fail here with
        // an actionable message instead.
        match publication_exists(pg_cfg, tls, &pg_cfg.publication).await {
            Ok(true) => {
                tracing::debug!(
                    "Publication '{}' verified present in database '{}'",
                    pg_cfg.publication,
                    pg_cfg.database,
                );
            }
            Ok(false) => {
                return Err(anyhow::anyhow!(
                    "{}",
                    missing_publication_error_msg(&pg_cfg.publication, &pg_cfg.database)
                ));
            }
            Err(e) => {
                // Transient catalog-query failure — don't block startup; the
                // CDC-loop backstop (is_publication_missing_error) will catch
                // a genuinely missing publication at recv() time.
                tracing::warn!(
                    "Could not verify publication '{}' existence (will retry at CDC start): {:?}",
                    pg_cfg.publication,
                    e,
                );
            }
        }
    }

    Ok(())
}

/// Query the confirmed_flush_lsn of a replication slot.
/// Returns None if the slot doesn't exist or the query fails.
async fn query_slot_lsn(
    pg_cfg: &config::PgConfig,
    slot_name: &str,
    tls: &PgTlsConnector,
) -> Option<i64> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls.connect(&conn_str).await.ok()?;
    let _conn_guard = AbortOnDrop(conn);

    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .ok()??;

    let lsn_str: String = row.get(0);
    // Parse LSN in "XX/XXXXXXXX" format
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let hi = i64::from_str_radix(parts[0], 16).ok()?;
    let lo = i64::from_str_radix(parts[1], 16).ok()?;
    Some((hi << 32) | lo)
}

/// Query `pg_current_wal_lsn()` — the PG WAL head (what PG has durably
/// written to disk). Used together with `confirmed_flush_lsn` (from
/// `query_slot_lsn`) to compute `pg_wal_lag_bytes` = how far the slot
/// is behind the WAL head (bytes PG can't discard yet).
async fn query_pg_current_wal_lsn(pg_cfg: &config::PgConfig, tls: &PgTlsConnector) -> Option<i64> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls.connect(&conn_str).await.ok()?;
    let _conn_guard = AbortOnDrop(conn);
    let row = client
        .query_opt("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .ok()?;
    let row = row?;
    let lsn_str: String = row.get(0);
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let hi = i64::from_str_radix(parts[0], 16).ok()?;
    let lo = i64::from_str_radix(parts[1], 16).ok()?;
    Some((hi << 32) | lo)
}

/// Check whether a publication exists in the configured database.
///
/// Publications are database-local objects in PostgreSQL: this query runs
/// against the same database the replication connection will attach to
/// (via `pg_cfg.conn_str()`), so a publication created in a *different*
/// database (e.g. `postgres`) correctly returns `false` here.
///
/// Returns `Ok(true)` if the publication exists in the configured database,
/// `Ok(false)` if it does not, or `Err` if the catalog query itself fails
/// (treated as transient by callers — they'll retry and re-check).
async fn publication_exists(
    pg_cfg: &config::PgConfig,
    tls: &PgTlsConnector,
    pub_name: &str,
) -> Result<bool> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for publication existence check")?;
    let _conn_guard = AbortOnDrop(conn);

    let row = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&pub_name],
        )
        .await
        .context(format!("Failed to query pg_publication for '{}'", pub_name,))?;
    Ok(row.is_some())
}

/// Query the `active_pid` of a replication slot.
///
/// Returns `Ok(Some(pid))` if another process is actively streaming from the
/// slot, `Ok(None)` if the slot is free (or doesn't exist), or `Err` if the
/// query itself fails. PG maintains the `active` flag and `active_pid`
/// column authoritatively — `active = true` means a live WAL sender process
/// is attached. There is no stale-PID risk under normal conditions: PG
/// clears `active` when the WAL sender exits (including abnormal exit via
/// SIGKILL, modulo TCP keepalive timeout). The local `flock` only guards
/// same-host duplicates; this check is the cross-host guard.
async fn slot_active_pid(
    pg_cfg: &config::PgConfig,
    tls: &PgTlsConnector,
    slot_name: &str,
) -> Result<Option<i64>> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for slot active_pid check")?;
    let _conn_guard = AbortOnDrop(conn);

    let row = client
        .query_opt(
            "SELECT active_pid::bigint FROM pg_replication_slots WHERE slot_name = $1 AND active",
            &[&slot_name],
        )
        .await
        .context("Failed to query active_pid from pg_replication_slots")?;

    Ok(row.map(|r| r.get::<_, i64>(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_writer_rotate_on_size() {
        let tmp = std::env::temp_dir().join("pg2sr_cdc_test_rotation");
        let log_path = tmp.join("test.log");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // Create writer with tiny cap so rotation triggers quickly.
        let mut w = SizeCappedLogWriter::new(&log_path, 50).unwrap();

        // Write 30 bytes — fits.
        w.write_all(b"line one: fits under cap\n").unwrap();
        assert_eq!(
            std::fs::read_to_string(&log_path).unwrap(),
            "line one: fits under cap\n"
        );

        // Write another 30 bytes — 60 > 50, must rotate.
        w.write_all(b"line two: triggers rotate\n").unwrap();

        // Current file has only line two.
        assert_eq!(
            std::fs::read_to_string(&log_path).unwrap(),
            "line two: triggers rotate\n"
        );

        // Backup file has line one.
        let backup_path = tmp.join("test.log.1");
        assert!(backup_path.exists(), "backup file must exist");
        assert_eq!(
            std::fs::read_to_string(&backup_path).unwrap(),
            "line one: fits under cap\n"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_init_tls_idempotent() {
        init_tls();
        init_tls();
    }

    #[test]
    fn test_slot_already_exists_err_detects_42710() {
        let e = std::io::Error::other(
            "ERROR: duplicate_object (42710): replication slot \"foo\" already exists",
        );
        assert!(slot_already_exists_err(&e));
        let e = std::io::Error::other("42710");
        assert!(slot_already_exists_err(&e));
    }

    #[test]
    fn test_slot_already_exists_err_detects_already_exists_text() {
        let e = std::io::Error::other("replication slot \"pg2sr_cdc_slot\" already exists");
        assert!(slot_already_exists_err(&e));
        let e = std::io::Error::other("already exists");
        assert!(slot_already_exists_err(&e));
    }

    #[test]
    fn test_slot_already_exists_err_rejects_other_errors() {
        let e = std::io::Error::other("ERROR: must be superuser to create replication slot");
        assert!(!slot_already_exists_err(&e));
        let e = std::io::Error::other("connection refused");
        assert!(!slot_already_exists_err(&e));
    }

    #[test]
    fn test_slot_already_exists_err_case_sensitivity() {
        let e = std::io::Error::other("already exists");
        assert!(slot_already_exists_err(&e));
        let e = std::io::Error::other("Already Exists");
        assert!(!slot_already_exists_err(&e));
    }

    #[test]
    fn test_slot_already_exists_err_traverses_source_chain() {
        let inner = std::io::Error::other("ERROR: replication slot \"x\" already exists");
        let outer = std::io::Error::other(inner);
        assert!(slot_already_exists_err(&outer));
    }

    // ── is_slot_in_use_error tests ─────────────────────────────

    #[test]
    fn test_is_slot_in_use_error_detects_sqlstate_55006() {
        let e = std::io::Error::other(
            "ERROR: object_not_in_prerequisite_state (55006): \
             replication slot \"foo\" is active for PID 12345",
        );
        assert!(is_slot_in_use_error(&e));
        let e = std::io::Error::other("55006");
        assert!(is_slot_in_use_error(&e));
    }

    #[test]
    fn test_is_slot_in_use_error_detects_active_for_pid_text() {
        let e = std::io::Error::other("replication slot \"pg2sr_cdc_slot\" is active for PID 4242");
        assert!(is_slot_in_use_error(&e));
    }

    #[test]
    fn test_is_slot_in_use_error_detects_already_in_use_text() {
        let e = std::io::Error::other("replication slot is already in use");
        assert!(is_slot_in_use_error(&e));
    }

    #[test]
    fn test_is_slot_in_use_error_rejects_already_exists() {
        // 42710 "already exists" must NOT be treated as slot-in-use — it
        // means the slot exists but is free; the caller should attach, not
        // exit. This is the critical distinction that prevents the new guard
        // from interfering with normal restart behavior.
        let e = std::io::Error::other("replication slot \"foo\" already exists");
        assert!(!is_slot_in_use_error(&e));
        let e = std::io::Error::other("42710");
        assert!(!is_slot_in_use_error(&e));
    }

    #[test]
    fn test_is_slot_in_use_error_rejects_unrelated_errors() {
        let e = std::io::Error::other("connection refused");
        assert!(!is_slot_in_use_error(&e));
        let e = std::io::Error::other("must be superuser to create replication slot");
        assert!(!is_slot_in_use_error(&e));
        let e = std::io::Error::other("role \"pg2sr\" does not exist");
        assert!(!is_slot_in_use_error(&e));
    }

    #[test]
    fn test_is_slot_in_use_error_traverses_source_chain() {
        let inner = std::io::Error::other("replication slot \"x\" is active for PID 99");
        let outer = std::io::Error::other(inner);
        assert!(is_slot_in_use_error(&outer));
    }

    // ── is_publication_missing_error tests ─────────────────────

    #[test]
    fn test_is_publication_missing_err_detects_42704() {
        // The exact error shape surfaced by pgwire-replication when
        // START_REPLICATION references a non-existent publication.
        let e = std::io::Error::other(
            "ERROR: undefined_object (42704): \
             publication \"pg2sr_cdc_publication\" does not exist",
        );
        assert!(is_publication_missing_error(&e));
    }

    #[test]
    fn test_is_publication_missing_err_detects_text_only_form() {
        // Some pgwire versions surface the message without the SQLSTATE code.
        let e = std::io::Error::other("publication \"pg2sr_cdc_publication\" does not exist");
        assert!(is_publication_missing_error(&e));
        let e = std::io::Error::other("publication my_pub not found");
        assert!(is_publication_missing_error(&e));
    }

    #[test]
    fn test_is_publication_missing_err_rejects_unrelated_42704() {
        // A 42704 for a *relation* (not a publication) must NOT match —
        // that's a different failure mode (table dropped mid-stream) and
        // should remain a transient reconnect, not a fatal exit.
        let e = std::io::Error::other(
            "ERROR: undefined_object (42704): relation \"orders\" does not exist",
        );
        assert!(!is_publication_missing_error(&e));
        // 42704 alone, without the word "publication", must not match.
        let e = std::io::Error::other("42704");
        assert!(!is_publication_missing_error(&e));
    }

    #[test]
    fn test_is_publication_missing_err_rejects_other_errors() {
        let e = std::io::Error::other("connection refused");
        assert!(!is_publication_missing_error(&e));
        let e = std::io::Error::other("must be superuser to create replication slot");
        assert!(!is_publication_missing_error(&e));
        let e = std::io::Error::other("replication slot \"x\" already exists");
        assert!(!is_publication_missing_error(&e));
    }

    #[test]
    fn test_is_publication_missing_err_traverses_source_chain() {
        let inner = std::io::Error::other(
            "publication \"pg2sr_cdc_publication\" does not exist (SQLSTATE 42704)",
        );
        let outer = std::io::Error::other(inner);
        assert!(is_publication_missing_error(&outer));
    }

    // ── missing_publication_error_msg tests ─────────────────────

    #[test]
    fn test_missing_publication_error_msg_is_actionable() {
        let msg = missing_publication_error_msg("pg2sr_cdc_publication", "example_db");
        // Must name the publication and the configured database so the
        // operator knows WHERE the publication is missing.
        assert!(
            msg.contains("pg2sr_cdc_publication"),
            "msg must name the publication"
        );
        assert!(
            msg.contains("example_db"),
            "msg must name the configured database"
        );
        // Must give the exact SQL command to fix it.
        assert!(
            msg.contains("CREATE PUBLICATION"),
            "msg must include CREATE PUBLICATION fix"
        );
        // Must mention the database-local nature of publications — this is
        // the cause of the error (publication created in the wrong DB).
        assert!(
            msg.contains("database-local"),
            "msg must explain database-local publications"
        );
        // Must offer the config alternative.
        assert!(
            msg.contains("manage_publication"),
            "msg must mention manage_publication"
        );
    }

    #[test]
    fn test_has_bytea_columns_no_bytea() {
        let mut cols: std::collections::HashMap<u32, Vec<crate::pg_catalog::CatalogColumn>> =
            std::collections::HashMap::new();
        cols.insert(
            1,
            vec![
                crate::pg_catalog::CatalogColumn {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                crate::pg_catalog::CatalogColumn {
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        );
        assert!(
            !has_bytea_columns(&cols),
            "INTEGER + TEXT columns should not trigger bytea check"
        );
    }

    #[test]
    fn test_has_bytea_columns_with_bytea() {
        let mut cols: std::collections::HashMap<u32, Vec<crate::pg_catalog::CatalogColumn>> =
            std::collections::HashMap::new();
        cols.insert(
            1,
            vec![
                crate::pg_catalog::CatalogColumn {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                crate::pg_catalog::CatalogColumn {
                    name: "data".into(),
                    type_oid: BYTEA_OID,
                    type_modifier: -1,
                },
            ],
        );
        assert!(
            has_bytea_columns(&cols),
            "BYTEA column (oid=17) should trigger bytea check"
        );
    }

    // ── DDL poller supervisor tests ───────────────────────────

    #[tokio::test]
    async fn test_ddl_supervisor_restarts_on_panic_and_fatals_after_max() {
        // The supervisor restarts a panicking task MAX_DDL_RESTARTS times
        // (3), then sends shutdown. We simulate by spawning tasks that
        // immediately panic, and checking the supervisor's final state.
        const MAX_DDL_RESTARTS: u32 = 3;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let shutdown_tx = std::sync::Arc::new(shutdown_tx);
        let mut restart_count: u32 = 0;
        let mut backoff = Duration::from_millis(1); // fast for tests

        let supervisor = tokio::spawn(async move {
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
                let inner = tokio::spawn(async {
                    panic!("simulated DDL poller panic");
                });
                match inner.await {
                    Ok(()) => break,
                    Err(e) if e.is_panic() => {
                        restart_count += 1;
                        if restart_count > MAX_DDL_RESTARTS {
                            let _ = shutdown_tx.send(true);
                            return restart_count;
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                    Err(_) => break,
                }
            }
            restart_count
        });

        let total_restarts = supervisor.await.unwrap();
        // Exactly MAX+1 panics before sending shutdown.
        assert_eq!(
            total_restarts,
            MAX_DDL_RESTARTS + 1,
            "Supervisor should restart {} times then FATAL",
            MAX_DDL_RESTARTS + 1,
        );
    }

    #[tokio::test]
    async fn test_ddl_supervisor_exits_cleanly_on_normal_return() {
        // If the inner task returns Ok(()), the supervisor breaks without restart.
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let supervisor = tokio::spawn(async move {
            let inner = tokio::spawn(async {
                // Task completes normally (simulates clean shutdown signal)
            });
            match inner.await {
                Ok(()) => "clean",
                Err(_) => "panic",
            }
        });

        let result: &str = supervisor.await.unwrap();
        assert_eq!(result, "clean");
        assert!(
            !*shutdown_rx.borrow(),
            "Shutdown should NOT be sent on clean exit"
        );
    }

    // ── idle_ack_lsn (idle replication lag fix) ───────────────────────

    #[test]
    fn test_idle_ack_advances_to_wal_end_when_idle() {
        // When the pipeline is idle (no pending buffered events), the ACK LSN
        // should advance to the KeepAlive wal_end — this lets PG discard
        // system WAL (autovacuum, checkpoints) and keeps replication lag bounded.
        // The checkpoint LSN (100) is behind wal_end (200), but that's safe:
        // the gap contains only system WAL, not replicated data.
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 100, /* wal_end       */ 200,
            /* saved         */ 100, /* has_pending   */ false,
        );
        assert_eq!(ack, 200, "idle + no pending → ACK to wal_end");
    }

    #[test]
    fn test_idle_ack_gates_on_saved_when_pending() {
        // When the pipeline has pending (buffered, unflushed) events, the ACK
        // must NOT advance to wal_end — it gates on the durable checkpoint
        // (saved) to protect un-flushed data from being discarded.
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 100, /* wal_end       */ 200,
            /* saved         */ 90, /* has_pending   */ true,
        );
        assert_eq!(ack, 90, "pending → ACK gated on min(checkpoint, saved)");
    }

    #[test]
    fn test_idle_ack_checkpoint_ahead_of_saved_when_pending() {
        // If the checkpoint LSN is ahead of the durable save (saver hasn't
        // persisted the latest advance yet), and we have pending events,
        // ACK only up to the durable save — never past it.
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 150, /* wal_end       */ 200,
            /* saved         */ 100, /* has_pending   */ true,
        );
        assert_eq!(ack, 100, "pending + checkpoint ahead → ACK to saved only");
    }

    #[test]
    fn test_idle_ack_wal_end_zero_when_idle_falls_back_to_saved() {
        // If wal_end is 0 (no KeepAlive received yet) and the pipeline is idle,
        // fall back to gating on the durable checkpoint — still safe and lets
        // the slot advance to the last durably-saved LSN.
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 100, /* wal_end       */ 0, /* saved         */ 90,
            /* has_pending   */ false,
        );
        assert_eq!(
            ack, 90,
            "wal_end=0 + idle → fall back to min(checkpoint, saved)"
        );
    }

    #[test]
    fn test_idle_ack_wal_end_below_checkpoint_when_idle_uses_checkpoint() {
        // Edge case: if wal_end < checkpoint (shouldn't happen given the
        // monotonic KeepAlive, but defense-in-depth), don't rewind the ACK.
        // When idle (no pending), use max(wal_end, saved) semantics by checking
        // wal_end > 0 — the function returns wal_end even if it's behind the
        // checkpoint. This is acceptable because the caller advances the
        // checkpoint FIRST, then ACKs; if wal_end is behind the checkpoint,
        // the ACK is behind the checkpoint — always safe (PG retains MORE WAL,
        // never less).
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 200, /* wal_end       */ 150,
            /* saved         */ 200, /* has_pending   */ false,
        );
        assert_eq!(
            ack, 150,
            "wal_end < checkpoint + idle → ACK to wal_end (safe: PG keeps more)"
        );
    }

    #[test]
    fn test_idle_ack_pending_with_zero_saved_returns_zero() {
        // Startup edge case: saved=0 (first tick, no checkpoint saved yet) +
        // pending events → don't ACK anything (let the first flush complete).
        let ack = idle_ack_lsn(
            /* checkpoint_lsn */ 100, /* wal_end       */ 200,
            /* saved         */ 0, /* has_pending   */ true,
        );
        assert_eq!(ack, 0, "pending + saved=0 → no ACK");
    }
}
