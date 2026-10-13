//! Prometheus /metrics and /health HTTP endpoint.
//!
//! A lightweight raw TCP listener (no hyper/axum/prometheus crate needed).
//! Serves Prometheus text exposition format from the pipeline's existing
//! `AtomicU64` counters. Two routes:
//!
//! - `GET /metrics` — Prometheus text format (13 metrics)
//! - `GET /health`  — JSON `{"status":"ok","uptime":"3h 12m"}` or 503
//!
//! The listener is spawned as a supervised tokio task in main.rs.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

// PrometheusHandle from the `metrics` crate lets us render
// histograms (with p50/p95/p99 quantiles) alongside the existing AtomicU64
// counters in the /metrics endpoint.
use metrics_exporter_prometheus::PrometheusHandle;

/// All counters needed by the /metrics endpoint.
/// Cloned cheaply (all fields are Arc or small values).
#[derive(Clone)]
pub struct MetricsSnapshot {
    pub total_rows: Arc<AtomicU64>,
    pub total_batches: Arc<AtomicU64>,
    pub total_flush_ms: Arc<AtomicU64>,
    pub last_lsn: Arc<AtomicU64>,
    /// Epoch seconds of the last completed CDC batch (0 = none yet). Lets
    /// /health tell "idle" (lag 0) from "wedged" (lag > 0, no progress).
    pub last_progress_at: Arc<AtomicU64>,
    pub reconnect_count: Arc<AtomicU64>,
    pub pending_bytes: Arc<AtomicUsize>,
    pub ddl_applied: Arc<AtomicU64>,
    pub ddl_skipped: Arc<AtomicU64>,
    pub error_count: Arc<AtomicU64>,
    pub last_saved_at: Arc<AtomicU64>,
    // New: snapshot phase tracking
    pub snapshot_in_progress: Arc<AtomicU64>,
    pub snapshot_tables_done: Arc<AtomicU64>,
    // New: PG WAL lag tracking (queried periodically, cached)
    pub pg_wal_lag_bytes: Arc<AtomicU64>,
    pub confirmed_flush_lsn: Arc<AtomicU64>,
    pub checkpoint_lag_bytes: Arc<AtomicU64>,
    pub wal_buffered_events: Arc<AtomicU64>,
    pub wal_buffered_bytes: Arc<AtomicU64>,
    pub wal_replayed_events: Arc<AtomicU64>,
    pub wal_replay_failures: Arc<AtomicU64>,
    pub wal_buffer_segments: Arc<AtomicU64>,
    pub wal_buffer_bytes: Arc<AtomicU64>,
    pub wal_buffer_quarantined_segments: Arc<AtomicU64>,
    /// Manager reference for per-scrape gauge refresh: the wal-buffer segment and
    /// byte gauges are refreshed per /metrics request instead of ≤60s stale.
    /// `None` when the snapshot is built without the manager
    /// (tests).
    pub wal_buffer: Option<Arc<crate::wal_buffer::WalBufferManager>>,
    pub started: Instant,
    pub shutdown_rx: watch::Receiver<bool>,
    /// Configured checkpoint save interval — used by /health to scale the
    /// "never saved" threshold so a large save_interval doesn't false-positive
    /// a 503 before the first tick fires.
    pub save_interval_ms: u64,
    /// SLO: handle to the `metrics` crate recorder. `None` when metrics are
    /// disabled or the recorder failed to install — /metrics then omits the
    /// histogram/quantile section. When `Some`, `.render()` produces the
    /// flush-latency histogram with p50/p95/p99 gauges.
    pub prom_handle: Option<PrometheusHandle>,
}

impl MetricsSnapshot {
    /// Refresh the wal-buffer gauges from the manager's in-memory accounting
    /// (per-scrape, so /metrics never serves ≤60s-stale segment/byte counts —
    /// The main.rs 60s tick refresh is kept: it feeds the
    /// HEALTH SUMMARY log reading the same Arcs; redundant writes are
    /// harmless.
    pub fn refresh_wal_gauges(&self) {
        if let Some(m) = &self.wal_buffer {
            self.wal_buffer_segments
                .store(m.total_segments(), Ordering::Relaxed);
            self.wal_buffer_bytes
                .store(m.total_buffered_bytes(), Ordering::Relaxed);
        }
    }
}

/// Run the metrics HTTP server until shutdown is signaled.
pub async fn run_metrics_server(mut snap: MetricsSnapshot, host: &str, port: u16) {
    // Bind to the configured host (default 127.0.0.1) — was hardcoded
    // 0.0.0.0, exposing LSN/throughput/reconnect metrics to the network.
    let addr = format!("{host}:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!("Metrics server listening on http://{addr}/metrics");
            l
        }
        Err(e) => {
            tracing::error!("Failed to bind metrics {addr}: {e:?}");
            return;
        }
    };

    loop {
        tokio::select! {
            biased;
            _ = snap.shutdown_rx.changed() => {
                tracing::info!("Metrics server shutting down");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((socket, _peer)) => {
                        let s = snap.clone();
                        tokio::spawn(async move {
                            handle_request(socket, s).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!("Metrics server accept error: {e:?}");
                    }
                }
            }
        }
    }
}

/// Bound reads and writes in the metrics HTTP handler so a slow/silent client
/// (slow-loris) cannot pin a tokio task forever, which would accumulate
/// orphaned tasks over time under adversarial or buggy probe traffic.
const METRICS_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Handle a single HTTP request.
async fn handle_request(mut socket: tokio::net::TcpStream, snap: MetricsSnapshot) {
    let mut buf = [0u8; 1024];
    let n = match tokio::time::timeout(METRICS_IO_TIMEOUT, socket.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return,
        Ok(Ok(n)) => n,
    };

    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        "/metrics" => {
            // Per-scrape gauge refresh BEFORE the atomics are read below.
            snap.refresh_wal_gauges();
            let body = format_prometheus_metrics(&snap);
            ("200 OK", "text/plain; version=0.0.4", body)
        }
        "/health" => {
            let is_shutting_down = *snap.shutdown_rx.borrow();
            let uptime_secs = snap.started.elapsed().as_secs();
            let uptime_str = crate::util::format_duration(uptime_secs);
            let last_lsn = snap.last_lsn.load(Ordering::Relaxed);
            let reconnect_count = snap.reconnect_count.load(Ordering::Relaxed);
            let pending_bytes = snap.pending_bytes.load(Ordering::Relaxed);
            let last_saved_at = snap.last_saved_at.load(Ordering::Relaxed);
            let pg_wal_lag = snap.pg_wal_lag_bytes.load(Ordering::Relaxed);
            let last_progress_at = snap.last_progress_at.load(Ordering::Relaxed);
            // Manager-owned gauge (segments currently on disk), NOT the
            // monotonic wal_buffered_bytes append counter: the counter never
            // returns to 0, so using it would latch has_work/buffered_stalled
            // on forever after the first buffered byte.
            let wal_buffer_bytes = snap.wal_buffer_bytes.load(Ordering::Relaxed);
            let wal_replay_failures = snap.wal_replay_failures.load(Ordering::Relaxed);
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let checkpoint_save_lag_secs = if last_saved_at > 0 {
                now_secs.saturating_sub(last_saved_at)
            } else {
                0
            };
            // Stall signals (corrected — cluster lag alone is NOT a stall
            // signal). `pg_wal_lag_bytes` is a CLUSTER-WIDE delta
            // (pg_current_wal_lsn() - confirmed_flush_lsn) refreshed by a 60s
            // poller: any WAL written anywhere in the cluster keeps it > 0,
            // while `last_progress_at` only moves when a CDC batch completes,
            // so a sparse-traffic but healthy pipeline was reported degraded.
            // `has_work` (our own pending rows or durably buffered WAL) is
            // therefore required: lag caused by foreign WAL with nothing of
            // ours outstanding is benign.
            //
            // The saver keeps ticking even when nothing changes, so without
            // these a wedge reported "ok" and a k8s liveness probe would never
            // restart it. Three distinct wedges:
            //   stalled          — our work is outstanding, PG has WAL past
            //                      confirmed_flush, no batch completed >300s.
            //   buffered_stalled — replay failing >300s while rows sit durably
            //                      buffered: SR down, flushes fail but the
            //                      checkpoint keeps advancing (cluster lag can
            //                      read 0), so the lag-only rule missed it.
            //   never_progressed — no batch EVER completed while work exists
            //                      (`stalled` requires last_progress_at > 0, so
            //                      a pipeline wedged before its first batch
            //                      was "ok" forever).
            let stalled_secs = if last_progress_at > 0 {
                now_secs.saturating_sub(last_progress_at)
            } else {
                0
            };
            let has_work = pending_bytes > 0 || wal_buffer_bytes > 0;
            let stalled = !is_shutting_down && pg_wal_lag > 0 && has_work && stalled_secs > 300;
            let buffered_stalled = !is_shutting_down
                && wal_buffer_bytes > 0
                && wal_replay_failures > 0
                && stalled_secs > 300;
            let never_progressed =
                !is_shutting_down && last_progress_at == 0 && uptime_secs > 600 && has_work;
            // Liveness: 503 on (a) shutdown, (b) saver dead (stale >300s while
            // not shutting down), or (c) never-saved past the expected first-save
            // deadline (the first save should have happened by now — saver may
            // be stuck). The deadline scales with the configured save_interval
            // so a large interval doesn't false-positive a 503 before the first
            // tick fires (floor 30s for the default 10s interval).
            let first_save_deadline_secs = 30u64.max((snap.save_interval_ms / 1000) * 2);
            let saver_dead =
                !is_shutting_down && last_saved_at > 0 && checkpoint_save_lag_secs > 300;
            let never_saved_and_should_have =
                !is_shutting_down && last_saved_at == 0 && uptime_secs > first_save_deadline_secs;
            // Any degraded signal → "degraded" + 503. First true flag (in the
            // enum order) names the reason so an operator can tell the wedges
            // apart; "none" also covers shutdown (status says so).
            let degraded = saver_dead
                || never_saved_and_should_have
                || stalled
                || buffered_stalled
                || never_progressed;
            let status_label = if is_shutting_down {
                "shutting down"
            } else if degraded {
                "degraded"
            } else {
                "ok"
            };
            let http_status = if is_shutting_down || degraded {
                "503 Service Unavailable"
            } else {
                "200 OK"
            };
            let degraded_reason = if saver_dead {
                "saver_dead"
            } else if never_saved_and_should_have {
                "never_saved"
            } else if stalled {
                "stalled"
            } else if buffered_stalled {
                "buffered_stalled"
            } else if never_progressed {
                "never_progressed"
            } else {
                "none"
            };
            let body = format!(
                r#"{{"status":"{status_label}","degraded_reason":"{degraded_reason}","uptime":"{uptime_str}","checkpoint_save_lag_secs":{checkpoint_save_lag_secs},"stalled_secs":{stalled_secs},"reconnect_count":{reconnect_count},"pending_bytes":{pending_bytes},"last_lsn":{last_lsn},"pg_wal_lag_bytes":{pg_wal_lag},"wal_buffer_bytes":{wal_buffer_bytes},"wal_replay_failures":{wal_replay_failures}}}"#,
            );
            (http_status, "application/json", body)
        }
        _ => {
            let body = "Not Found\n".to_string();
            ("404 Not Found", "text/plain", body)
        }
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let _ = tokio::time::timeout(METRICS_IO_TIMEOUT, socket.write_all(response.as_bytes())).await;
    let _ = tokio::time::timeout(METRICS_IO_TIMEOUT, socket.flush()).await;
}

/// Format all metrics in Prometheus text exposition format.
fn format_prometheus_metrics(s: &MetricsSnapshot) -> String {
    let rows = s.total_rows.load(Ordering::Relaxed);
    let batches = s.total_batches.load(Ordering::Relaxed);
    let flush_ms = s.total_flush_ms.load(Ordering::Relaxed);
    let lsn = s.last_lsn.load(Ordering::Relaxed);
    let reconnects = s.reconnect_count.load(Ordering::Relaxed);
    let pending = s.pending_bytes.load(Ordering::Relaxed);
    let ddl_a = s.ddl_applied.load(Ordering::Relaxed);
    let ddl_s = s.ddl_skipped.load(Ordering::Relaxed);
    let errors = s.error_count.load(Ordering::Relaxed);
    let uptime = s.started.elapsed().as_secs_f64().max(0.001);
    let avg_flush = if batches > 0 {
        flush_ms as f64 / batches as f64
    } else {
        0.0
    };

    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last_saved = s.last_saved_at.load(Ordering::Relaxed);
    let cp_lag = if last_saved > 0 && now_epoch > last_saved {
        now_epoch - last_saved
    } else {
        0
    };

    let snapshot_ip = s.snapshot_in_progress.load(Ordering::Relaxed);
    let snapshot_td = s.snapshot_tables_done.load(Ordering::Relaxed);
    let wal_lag = s.pg_wal_lag_bytes.load(Ordering::Relaxed);
    let flush_lsn = s.confirmed_flush_lsn.load(Ordering::Relaxed);
    let ckpt_lag_bytes = s.checkpoint_lag_bytes.load(Ordering::Relaxed);
    let wbuf_events = s.wal_buffered_events.load(Ordering::Relaxed);
    let wbuf_bytes = s.wal_buffered_bytes.load(Ordering::Relaxed);
    let wrep_events = s.wal_replayed_events.load(Ordering::Relaxed);
    let wrep_fail = s.wal_replay_failures.load(Ordering::Relaxed);
    let wsegs = s.wal_buffer_segments.load(Ordering::Relaxed);
    let wbytes = s.wal_buffer_bytes.load(Ordering::Relaxed);
    let wquar = s.wal_buffer_quarantined_segments.load(Ordering::Relaxed);
    let mut body = format!(
        "\
# HELP pg2sr_cdc_total_rows Total rows replicated to StarRocks
# TYPE pg2sr_cdc_total_rows counter
pg2sr_cdc_total_rows {rows}

# HELP pg2sr_cdc_total_batches Total flush batches processed
# TYPE pg2sr_cdc_total_batches counter
pg2sr_cdc_total_batches {batches}

# HELP pg2sr_cdc_total_flush_ms Cumulative flush latency in milliseconds
# TYPE pg2sr_cdc_total_flush_ms counter
pg2sr_cdc_total_flush_ms {flush_ms}

# HELP pg2sr_cdc_last_lsn Highest WAL LSN flushed to the in-memory checkpoint (may be ahead of the durable/ACK'd LSN)
# TYPE pg2sr_cdc_last_lsn gauge
pg2sr_cdc_last_lsn {lsn}

# HELP pg2sr_cdc_reconnect_count PG replication reconnection count
# TYPE pg2sr_cdc_reconnect_count counter
pg2sr_cdc_reconnect_count {reconnects}

# HELP pg2sr_cdc_pending_bytes Current pending memory in bytes
# TYPE pg2sr_cdc_pending_bytes gauge
pg2sr_cdc_pending_bytes {pending}

# HELP pg2sr_cdc_ddl_applied DDL events applied to StarRocks
# TYPE pg2sr_cdc_ddl_applied counter
pg2sr_cdc_ddl_applied {ddl_a}

# HELP pg2sr_cdc_ddl_skipped DDL events skipped (unsupported / managed elsewhere)
# TYPE pg2sr_cdc_ddl_skipped counter
pg2sr_cdc_ddl_skipped {ddl_s}

# HELP pg2sr_cdc_error_count Total pipeline errors (flush, event-processing, DDL, checkpoint-save)
# TYPE pg2sr_cdc_error_count counter
pg2sr_cdc_error_count {errors}

# HELP pg2sr_cdc_uptime_seconds Pipeline uptime in seconds
# TYPE pg2sr_cdc_uptime_seconds gauge
pg2sr_cdc_uptime_seconds {uptime}

# HELP pg2sr_cdc_avg_flush_ms Average flush latency in milliseconds
# TYPE pg2sr_cdc_avg_flush_ms gauge
pg2sr_cdc_avg_flush_ms {avg_flush}

# HELP pg2sr_cdc_checkpoint_lag_seconds Seconds since last checkpoint save
# TYPE pg2sr_cdc_checkpoint_lag_seconds gauge
pg2sr_cdc_checkpoint_lag_seconds {cp_lag}

# HELP pg2sr_cdc_snapshot_in_progress 1 while the snapshot phase is running, 0 during CDC streaming
# TYPE pg2sr_cdc_snapshot_in_progress gauge
pg2sr_cdc_snapshot_in_progress {snapshot_ip}

# HELP pg2sr_cdc_snapshot_tables_done Number of tables added to the snapshotted set
# TYPE pg2sr_cdc_snapshot_tables_done counter
pg2sr_cdc_snapshot_tables_done {snapshot_td}

# HELP pg2sr_cdc_pg_wal_lag_bytes PG WAL lag in bytes (pg_current_wal_lsn - confirmed_flush_lsn)
# TYPE pg2sr_cdc_pg_wal_lag_bytes gauge
pg2sr_cdc_pg_wal_lag_bytes {wal_lag}

# HELP pg2sr_cdc_confirmed_flush_lsn The PG replication slot's confirmed_flush_lsn
# TYPE pg2sr_cdc_confirmed_flush_lsn gauge
pg2sr_cdc_confirmed_flush_lsn {flush_lsn}

# HELP pg2sr_cdc_checkpoint_lag_bytes WAL bytes ACK'd to PG but not yet durably checkpointed (confirmed_flush_lsn - last_committed_lsn)
# TYPE pg2sr_cdc_checkpoint_lag_bytes gauge
pg2sr_cdc_checkpoint_lag_bytes {ckpt_lag_bytes}

# HELP pg2sr_cdc_wal_buffered_events Events appended to the durable wal buffer during DDL write pauses
# TYPE pg2sr_cdc_wal_buffered_events counter
pg2sr_cdc_wal_buffered_events {wbuf_events}

# HELP pg2sr_cdc_wal_buffered_bytes Payload bytes appended to the durable wal buffer
# TYPE pg2sr_cdc_wal_buffered_bytes counter
pg2sr_cdc_wal_buffered_bytes {wbuf_bytes}

# HELP pg2sr_cdc_wal_replayed_events Events replayed from the wal buffer into StarRocks
# TYPE pg2sr_cdc_wal_replayed_events counter
pg2sr_cdc_wal_replayed_events {wrep_events}

# HELP pg2sr_cdc_wal_replay_failures Replay tasks that failed (segment kept, retried on next kick)
# TYPE pg2sr_cdc_wal_replay_failures counter
pg2sr_cdc_wal_replay_failures {wrep_fail}

# HELP pg2sr_cdc_wal_buffer_segments Current buffered segment count across all tables
# TYPE pg2sr_cdc_wal_buffer_segments gauge
pg2sr_cdc_wal_buffer_segments {wsegs}

# HELP pg2sr_cdc_wal_buffer_bytes Current buffered bytes across all segments (no cap — external tooling watches this)
# TYPE pg2sr_cdc_wal_buffer_bytes gauge
pg2sr_cdc_wal_buffer_bytes {wbytes}

# HELP pg2sr_cdc_wal_buffer_quarantined_segments Segments auto-quarantined (corrupt/unreadable, bytes preserved aside)
# TYPE pg2sr_cdc_wal_buffer_quarantined_segments counter
pg2sr_cdc_wal_buffer_quarantined_segments {wquar}
"
    );

    // SLO: append the `metrics` crate histograms with HELP/TYPE headers.
    if let Some(handle) = &s.prom_handle {
        let rendered = handle.render();
        if !rendered.trim().is_empty() {
            body.push('\n');
            body.push_str(
                "# HELP pg2sr_cdc_flush_latency_ms Flush latency in milliseconds (histogram)\n\
                 # TYPE pg2sr_cdc_flush_latency_ms histogram\n",
            );
            body.push_str(&rendered);
        }
    }

    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    /// The `metrics` crate's global recorder can only be installed once per
    /// process. Use a OnceLock so multiple tests share the same handle safely.
    static TEST_HANDLE: OnceLock<Option<metrics_exporter_prometheus::PrometheusHandle>> =
        OnceLock::new();

    fn test_handle() -> Option<metrics_exporter_prometheus::PrometheusHandle> {
        TEST_HANDLE
            .get_or_init(|| {
                metrics_exporter_prometheus::PrometheusBuilder::new()
                    .set_quantiles(&[0.5, 0.95, 0.99])
                    .and_then(|b| b.install_recorder())
                    .ok()
            })
            .clone()
    }

    fn dummy_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            total_rows: Arc::new(AtomicU64::new(100)),
            total_batches: Arc::new(AtomicU64::new(5)),
            total_flush_ms: Arc::new(AtomicU64::new(250)),
            last_lsn: Arc::new(AtomicU64::new(42)),
            last_progress_at: Arc::new(AtomicU64::new(0)),
            reconnect_count: Arc::new(AtomicU64::new(0)),
            pending_bytes: Arc::new(AtomicUsize::new(0)),
            ddl_applied: Arc::new(AtomicU64::new(3)),
            ddl_skipped: Arc::new(AtomicU64::new(1)),
            error_count: Arc::new(AtomicU64::new(0)),
            last_saved_at: Arc::new(AtomicU64::new(0)),
            snapshot_in_progress: Arc::new(AtomicU64::new(0)),
            snapshot_tables_done: Arc::new(AtomicU64::new(0)),
            pg_wal_lag_bytes: Arc::new(AtomicU64::new(0)),
            confirmed_flush_lsn: Arc::new(AtomicU64::new(0)),
            checkpoint_lag_bytes: Arc::new(AtomicU64::new(0)),
            wal_buffered_events: Arc::new(AtomicU64::new(7)),
            wal_buffered_bytes: Arc::new(AtomicU64::new(1024)),
            wal_replayed_events: Arc::new(AtomicU64::new(6)),
            wal_replay_failures: Arc::new(AtomicU64::new(1)),
            wal_buffer_segments: Arc::new(AtomicU64::new(2)),
            wal_buffer_bytes: Arc::new(AtomicU64::new(4096)),
            wal_buffer_quarantined_segments: Arc::new(AtomicU64::new(3)),
            wal_buffer: None,
            started: Instant::now(),
            shutdown_rx: watch::channel(false).1,
            save_interval_ms: 10_000,
            prom_handle: None,
        }
    }

    #[test]
    fn test_format_prometheus_metrics_includes_core_counters() {
        let snap = dummy_snapshot();
        let body = format_prometheus_metrics(&snap);
        assert!(body.contains("pg2sr_cdc_total_rows 100"));
        assert!(body.contains("pg2sr_cdc_total_batches 5"));
        assert!(body.contains("pg2sr_cdc_ddl_applied 3"));
        assert!(body.contains("pg2sr_cdc_ddl_skipped 1"));
        // Wal-buffer observability
        assert!(body.contains("pg2sr_cdc_wal_buffered_events 7"));
        assert!(body.contains("pg2sr_cdc_wal_buffered_bytes 1024"));
        assert!(body.contains("pg2sr_cdc_wal_replayed_events 6"));
        assert!(body.contains("pg2sr_cdc_wal_replay_failures 1"));
        assert!(body.contains("pg2sr_cdc_wal_buffer_segments 2"));
        assert!(body.contains("pg2sr_cdc_wal_buffer_bytes 4096"));
        assert!(body.contains("pg2sr_cdc_wal_buffer_quarantined_segments 3"));
    }

    // ── SLO /metrics renders flush-latency histogram ──

    #[test]
    fn test_metrics_renders_flush_latency_histogram_with_quantiles() {
        // SLO: install a Prometheus recorder with p50/p95/p99 quantiles,
        // record several flush-latency samples, and verify /metrics renders
        // the histogram with p50/p95/p99 quantile gauges.
        let handle = test_handle().expect("recorder should be installed");

        // Record flush latencies: 10ms, 20ms, 30ms, ..., 100ms
        let hist = metrics::histogram!("pg2sr_cdc_flush_latency_ms");
        for ms in 10..=100 {
            hist.record(ms as f64);
        }

        let rendered = handle.render();
        assert!(
            rendered.contains("pg2sr_cdc_flush_latency_ms"),
            "rendered metrics must include the flush-latency histogram: {rendered}"
        );
        // The quantile labels appear as quantile="0.5", "0.95", "0.99"
        assert!(
            rendered.contains("quantile=\"0.5\""),
            "p50 quantile must be present: {rendered}"
        );
        assert!(
            rendered.contains("quantile=\"0.95\""),
            "p95 quantile must be present: {rendered}"
        );
        assert!(
            rendered.contains("quantile=\"0.99\""),
            "p99 quantile must be present: {rendered}"
        );
    }

    #[test]
    fn test_format_prometheus_metrics_appends_histogram_when_handle_present() {
        // SLO: when prom_handle is Some, the histogram section is appended.
        let handle = test_handle().expect("recorder should be installed");

        // Record a sample so the histogram has a non-empty render.
        metrics::histogram!("pg2sr_cdc_flush_latency_ms").record(42.0);

        let mut snap = dummy_snapshot();
        snap.prom_handle = Some(handle);
        let body = format_prometheus_metrics(&snap);
        assert!(
            body.contains("pg2sr_cdc_flush_latency_ms"),
            "body must include histogram section when prom_handle is Some: {body}"
        );
        assert!(
            body.contains("quantile=\"0.5\""),
            "p50 quantile must appear in /metrics body: {body}"
        );
    }

    #[test]
    fn test_format_prometheus_metrics_omits_histogram_when_handle_none() {
        // When prom_handle is None (metrics disabled), the histogram section
        // must be absent — the core counters still render.
        let snap = dummy_snapshot();
        let body = format_prometheus_metrics(&snap);
        assert!(
            !body.contains("pg2sr_cdc_flush_latency_ms"),
            "body must NOT include histogram section when prom_handle is None"
        );
        assert!(
            body.contains("pg2sr_cdc_total_rows"),
            "core counters must still render"
        );
    }
}
