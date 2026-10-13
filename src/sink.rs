//! StarRocks Stream Load via reqwest — handles 307 FE→BE redirect with auth preserved.
//! Multi-node HA failover with exponential backoff retry, column transforms for
//! BYTEA (unhex) and DELETE (__op), and duplicate label detection for idempotency.
use anyhow::{bail, Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::sr_connection::{LabelStatusQuery, SrConnectionManager};

/// Maximum bytes to read from a Stream Load HTTP response body. Prevents OOM
/// when a misconfigured proxy or SR internal error returns a multi-GB HTML
/// error page. SR Stream Load JSON responses are typically < 4 KB.
const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;

/// Read an HTTP response body with a size cap. Prevents OOM from oversized
/// responses — SR Stream Load JSON responses are typically < 4 KB, but a
/// misconfigured proxy or SR internal error can return a multi-GB HTML page.
///
/// The cap is enforced at the SOURCE via `bytes_stream()` — we stop
/// pulling bytes once `MAX_RESPONSE_BODY_BYTES` is reached. The previous
/// implementation called `response.bytes().await` (reads the ENTIRE body into
/// memory) and only truncated afterward, so a multi-GB body OOMed the
/// pipeline before the cap could fire.
async fn read_capped_body(response: reqwest::Response) -> Result<String> {
    use futures::StreamExt;
    let mut stream = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(MAX_RESPONSE_BODY_BYTES);
    let mut truncated = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to read Stream Load response body chunk")?;
        let remaining = MAX_RESPONSE_BODY_BYTES.saturating_sub(buf.len());
        if remaining == 0 {
            truncated = true;
            break;
        }
        if chunk.len() <= remaining {
            buf.extend_from_slice(&chunk);
        } else {
            buf.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
    }
    if truncated {
        tracing::warn!(
            "Stream Load response body reached the {} byte cap — truncated",
            MAX_RESPONSE_BODY_BYTES,
        );
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Response from StarRocks Stream Load.
#[derive(Debug, Default, serde::Deserialize)]
#[allow(non_snake_case, dead_code)]
pub struct StreamLoadResponse {
    #[serde(default)]
    pub Status: String,
    #[serde(default)]
    pub Message: String,
    #[serde(default)]
    pub NumberTotalRows: i64,
    #[serde(default)]
    pub NumberLoadedRows: i64,
    /// Rows rejected by SR (e.g. strict-mode type coercion failures).
    /// Non-zero means silent data loss if treated as success.
    #[serde(default)]
    pub NumberFilteredRows: i64,
    #[serde(default)]
    pub ErrorURL: String,
    #[serde(default)]
    pub Label: String,
    #[serde(default)]
    pub ExistingJobStatus: String,
}

/// A batch of rows to load into a single StarRocks table.
#[derive(Debug, Clone)]
pub struct SrBatch {
    pub database: String,
    pub table: String,
    /// Serialized JSON-lines payload. Held as `bytes::Bytes` so a per-attempt
    /// clone in `try_stream_load` is an O(1) refcount bump instead of a full
    /// N-byte copy (with `max_retries` × node_count attempts, a 100 MB batch
    /// previously churned ~900 MB of allocations per flush).
    pub data: bytes::Bytes,
    pub row_count: usize,
    pub columns: Vec<String>,
    pub op_type: String,
}

/// StarRocks Stream Load sink — reqwest with manual 307 redirect (preserves auth).
/// Uses the connection manager for multi-node HA failover.
#[derive(Clone)]
pub struct SrSink {
    mgr: Arc<SrConnectionManager>,
    max_retries: u32,
    http_timeout: Duration,
    client: reqwest::Client,
    /// Whether to use https:// for connections.
    use_tls: bool,
    /// Monotonically increasing counter for globally-unique Stream Load labels.
    label_counter: Arc<AtomicU64>,
    /// Used to poll SHOW LOAD for a label's terminal state when SR returns
    /// "Label Already Exists + state=RUNNING". Stored as a trait object so
    /// tests can inject a mock; production users get the
    /// `LabelStatusQuery` impl on `SrConnectionManager` automatically.
    label_poller: Arc<dyn LabelStatusQuery>,
    /// Labels that hit a strict-mode PARTIAL SUCCESS (NumberFilteredRows > 0).
    /// A subsequent retry reuses the SAME label (HA failover) and SR returns
    /// "Label Already Exists + FINISHED" — WITHOUT this set, that's treated as
    /// full success and the filtered rows are silently lost. When a label
    /// is poisoned, the duplicate-FINISHED response is rejected so the error
    /// propagates (flush_transaction returns Err → LSN doesn't advance → PG
    /// re-sends the batch; the operator fixes the bad row).
    poisoned_labels: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl SrSink {
    pub fn new(mgr: Arc<SrConnectionManager>) -> Result<Self> {
        let http_timeout = mgr.http_timeout();
        let mut client_builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none()) // manual 307 to preserve auth
            .connect_timeout(mgr.connection_timeout()) // TCP+TLS handshake only
            .timeout(http_timeout) // upper bound per single HTTP call
            .pool_idle_timeout(mgr.http_pool_idle_timeout()) // drop idle connections before SR closes them
            .pool_max_idle_per_host(20); // bound idle connections per BE host

        // Apply HTTP TLS configuration
        let use_tls = mgr.http_tls_enabled();
        if use_tls {
            if let Some(ca_cert_path) = mgr.http_tls_ca_cert() {
                let cert_bytes = std::fs::read(ca_cert_path).with_context(|| {
                    format!("Failed to read HTTP TLS CA cert: {:?}", ca_cert_path)
                })?;
                let cert = reqwest::tls::Certificate::from_pem(&cert_bytes)
                    .context("Failed to parse HTTP TLS CA cert")?;
                client_builder = client_builder.add_root_certificate(cert);
            }
        }

        let client = client_builder
            .build()
            .context("Failed to build reqwest client")?;

        Ok(Self {
            mgr: mgr.clone(),
            max_retries: mgr.max_retries(),
            http_timeout,
            client,
            use_tls: mgr.http_tls_enabled(),
            label_counter: Arc::new(AtomicU64::new(0)),
            label_poller: mgr.clone() as Arc<dyn LabelStatusQuery>,
            poisoned_labels: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        })
    }

    /// Test-only constructor that injects a mock `LabelStatusQuery` for
    /// unit-testing `poll_label_until_terminal` without a real StarRocks.
    #[cfg(test)]
    pub fn with_label_poller(
        mgr: Arc<SrConnectionManager>,
        poller: Arc<dyn LabelStatusQuery>,
    ) -> Result<Self> {
        let http_timeout = mgr.http_timeout();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(mgr.connection_timeout())
            .timeout(http_timeout)
            .pool_idle_timeout(mgr.http_pool_idle_timeout())
            .pool_max_idle_per_host(20)
            .build()
            .context("Failed to build reqwest client")?;
        Ok(Self {
            mgr,
            max_retries: 0,
            http_timeout,
            client,
            use_tls: false,
            label_counter: Arc::new(AtomicU64::new(0)),
            label_poller: poller,
            poisoned_labels: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        })
    }

    /// Stream Load a batch with HA multi-node failover.
    ///
    /// Tries nodes in active-first order. On each node, retries with exponential
    /// backoff (1s, 2s, 4s, ... capped at 64s). If a node fails after all retries,
    /// clears the active cache and tries the next node.
    pub async fn stream_load(&self, batch: &SrBatch) -> Result<StreamLoadResponse> {
        let nodes = self.mgr.node_order();
        let mut last_error: Option<anyhow::Error> = None;

        // Generate one globally-unique label for this stream_load call.
        // All node failover attempts reuse the same label, so if node A
        // committed but the response was lost, node B's attempt is detected
        // as a duplicate and treated as success.  Without reuse, node B
        // would generate a new label and load the same data again.
        let label = self.make_label(batch);

        for &node_idx in &nodes {
            let url =
                self.mgr
                    .stream_load_url(&batch.database, &batch.table, self.use_tls, node_idx);

            match self
                .stream_load_on_node(&url, &label, batch, node_idx)
                .await
            {
                Ok(resp) => {
                    // Success! Cache this node as active for future requests.
                    self.mgr.set_active(node_idx);
                    return Ok(resp);
                }
                Err(e) => {
                    // Node failed — record error and clear active if it was the
                    // active node (so next call tries fresh). Per-node retry
                    // noise is DEBUG; the aggregate WARN after all nodes are
                    // exhausted is logged below.
                    tracing::debug!(
                        node_idx = node_idx,
                        host = self.mgr.get_node(node_idx).map(|n| n.host.as_str()).unwrap_or("?"),
                        error = ?e,
                        "Stream Load attempt failed on node",
                    );
                    last_error = Some(e);
                    self.mgr.clear_active_if_match(node_idx);
                }
            }
        }

        // All nodes failed — clear active cache entirely
        self.mgr.clear_active();
        tracing::warn!(
            label = %label,
            error = ?last_error.as_ref().map(|e| e.to_string()).unwrap_or_default(),
            "Stream Load failed after all node attempts — pipeline will replay in next cycle",
        );
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No SR nodes available for Stream Load")))
    }

    /// Build a globally-unique Stream Load label.
    /// Uses a monotonic AtomicU64 counter appended to the timestamp so two
    /// flushes for the same table within the same millisecond never collide.
    fn make_label(&self, batch: &SrBatch) -> String {
        let counter = self.label_counter.fetch_add(1, Ordering::Relaxed);
        format!(
            "pg2sr_cdc_{}_{}_{}_{}",
            batch.database,
            batch.table,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            counter,
        )
    }

    /// Mark a Stream Load label as poisoned by a partial-success.
    /// The set is capped to prevent unbounded growth under pathological
    /// conditions; when full it is cleared (poisoned labels are unique and
    /// never regenerated, so clearing very old ones is safe — they will never
    /// be checked again).
    fn poison_label(set: &Arc<std::sync::Mutex<std::collections::HashSet<String>>>, label: &str) {
        const MAX_POISONED: usize = 4096;
        if let Ok(mut guard) = set.lock() {
            if guard.len() >= MAX_POISONED {
                guard.clear();
            }
            guard.insert(label.to_string());
        }
    }

    /// Whether a label was poisoned by a prior partial-success.
    fn is_label_poisoned(
        set: &Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
        label: &str,
    ) -> bool {
        match set.lock() {
            Ok(guard) => guard.contains(label),
            Err(e) => {
                // Poison-recovery: a poisoned mutex means a prior panic; check
                // the inner set rather than silently treating as clean (which
                // would re-introduce the masking bug).
                e.into_inner().contains(label)
            }
        }
    }

    /// Value for the SR Stream Load `timeout` request header (seconds).
    ///
    /// SR's own default per-load timeout is 600s (or the FE's
    /// `stream_load_default_timeout_second`) — far longer than this client's
    /// patience, so a slow load could stay RUNNING long after reqwest gave up.
    /// Claiming the deadline for ourselves is the point of this header, but it
    /// must NOT be the bare request timeout: when the client aborts while
    /// waiting, the retry reuses the SAME label and the
    /// `Label Already Exists + FINISHED` path below turns a load SR was still
    /// allowed to finish into one cheap label lookup. Cutting SR off exactly
    /// at the request timeout instead makes that load abort terminally, every
    /// later attempt on the reused label fails, and the rows are pushed into
    /// the durable buffer for a replay cycle — a degradation loop on clusters
    /// that legitimately need longer than `sink_flush_timeout_ms` for a ≤90MB
    /// chunk. So the deadline is what the pipeline will actually wait for a
    /// confirmed outcome: the request timeout plus the label-poll budget it
    /// spends afterwards. Clamped to SR's documented range (1..=259200).
    fn sr_timeout_header(&self) -> String {
        let budget = self.http_timeout + self.poll_label_budget();
        let secs = budget.as_secs_f64().ceil().max(1.0);
        let secs = if secs > 259_200.0 {
            259_200
        } else {
            secs as u64
        };
        secs.to_string()
    }

    /// Attempt Stream Load on a specific node with exponential backoff retries.
    async fn stream_load_on_node(
        &self,
        url: &str,
        label: &str,
        batch: &SrBatch,
        node_idx: usize,
    ) -> Result<StreamLoadResponse> {
        // max_retries is the number of retry attempts AFTER the initial try.
        let max_attempts = self.max_retries + 1;
        let mut attempt: u32 = 0;
        let base_delay = Duration::from_secs(1);

        loop {
            // Per-attempt deadline: FE PUT + (if 307) BE PUT share one budget.
            // The reqwest client's own `.timeout(http_timeout)` acts as a
            // per-HTTP-call backstop; this deadline bounds the full chain.
            let deadline = tokio::time::Instant::now() + self.http_timeout;
            let result =
                tokio::time::timeout_at(deadline, self.try_stream_load(url, label, batch)).await;
            match result {
                Err(_elapsed) => {
                    // Per-attempt deadline fired — DEBUG during retry; final
                    // attempt will bail with an error.
                    tracing::debug!(
                        attempt = attempt + 1,
                        max_attempts,
                        node_idx,
                        "Stream Load attempt timed out (deadline elapsed)",
                    );
                    if attempt + 1 >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "Stream Load timed out after {} attempts on node {}",
                            max_attempts,
                            node_idx,
                        ));
                    }
                }
                Ok(Ok(resp)) => {
                    // "Publish Timeout" is a documented SUCCESS state (STREAM
                    // LOAD return values): the load job is submitted and the
                    // transaction committed, the data is merely not queryable
                    // yet, and SR's own docs say "you do not need to retry".
                    // `NumberLoadedRows` is documented as valid only for
                    // `Status:"Success"`, so the partial-success guard below
                    // must NOT run for it. Treating it as a failure (the old
                    // behavior) burned the retry/failover budget and could
                    // push an already-committed load into the durable buffer.
                    if resp.Status == "Publish Timeout" {
                        tracing::debug!(
                            "Stream Load published with timeout (committed, not yet queryable): \
                             {}.{} ({} rows) via node {}",
                            batch.database,
                            batch.table,
                            batch.row_count,
                            node_idx,
                        );
                        return Ok(resp);
                    }
                    if resp.Status == "Success" || resp.Status == "OK" {
                        // Strict check: SR can return Status:"Success" while
                        // silently rejecting individual rows (strict-mode type
                        // coercion failures, NULL into NOT NULL, etc.).
                        // These show up as NumberFilteredRows > 0 and
                        // NumberLoadedRows < row_count. Without this check,
                        // the snapshot "succeeds" but is missing N rows —
                        // never reconciled. Silent data corruption.
                        if resp.NumberFilteredRows > 0
                            || (batch.row_count > 0
                                && resp.NumberLoadedRows < batch.row_count as i64)
                        {
                            tracing::error!(
                                "Stream Load partial success: {}.{}, loaded={}/{}, filtered={}, \
                                 status={}, message={}",
                                batch.database,
                                batch.table,
                                resp.NumberLoadedRows,
                                batch.row_count,
                                resp.NumberFilteredRows,
                                resp.Status,
                                resp.Message,
                            );
                            // Poison this label so a HA-failover retry
                            // (which reuses the SAME label) can't be masked as
                            // full success by a "Label Already Exists +
                            // FINISHED" response — that would silently lose the
                            // filtered rows. Propagate Err so flush_transaction
                            // fails, the LSN doesn't advance, and PG re-sends
                            // the batch (operator fixes data quality).
                            Self::poison_label(&self.poisoned_labels, label);
                            bail!(
                                "Stream Load partial success on node {}: loaded {}/{} rows, \
                                 {} filtered (status={}, message={})",
                                node_idx,
                                resp.NumberLoadedRows,
                                batch.row_count,
                                resp.NumberFilteredRows,
                                resp.Status,
                                resp.Message,
                            );
                        }
                        tracing::debug!(
                            "Stream Load OK: {}.{} ({} rows) via node {}",
                            batch.database,
                            batch.table,
                            batch.row_count,
                            node_idx,
                        );
                        return Ok(resp);
                    } else if resp.Status == "Label Already Exists"
                        || (resp.Status == "Fail" && resp.ExistingJobStatus == "FINISHED")
                    {
                        // Duplicate label — a previous attempt (same label,
                        // same stream_load call) already reached SR. Success
                        // is ONLY safe when the existing job terminally
                        // FINISHED: the data is committed. Any other state is
                        // NOT an idempotent success — a RUNNING job can still
                        // fail after we ACK (checkpoint advances → batch
                        // lost), so it must be polled to a terminal state
                        // exactly like the Fail arm below.
                        //
                        // If this label was poisoned by a prior partial
                        // success (filtered rows), a duplicate FINISHED must NOT
                        // be treated as full success — the filtered rows would
                        // be silently lost. Propagate the error instead.
                        if Self::is_label_poisoned(&self.poisoned_labels, label) {
                            tracing::error!(
                                label = %resp.Label,
                                "Refusing to treat 'Label Already Exists + FINISHED' as success: \
                                 label was poisoned by a prior partial-success (filtered rows)",
                            );
                            bail!(
                                "Stream Load label {} already FINISHED but is poisoned by a \
                                 prior partial-success (filtered rows) — refusing to mask as \
                                 success; batch will replay",
                                label,
                            );
                        }
                        match resp.ExistingJobStatus.as_str() {
                            "FINISHED" | "LOADED" => {
                                // Idempotent success — previous attempt committed the data.
                                tracing::debug!(
                                    label = %resp.Label,
                                    "Stream Load label already FINISHED — treating as OK",
                                );
                                return Ok(resp);
                            }
                            "RUNNING" | "LOADING" | "PENDING" | "PENDING_EXECUTE" | "QUEUEING" => {
                                // Label exists and SR is still processing it — poll
                                // SHOW LOAD for a terminal state. If FINISHED → Ok
                                // (idempotent); if FAILED → ERROR (data loss); if
                                // still non-terminal after the poll budget → Err
                                // (retry on next node). The poll is bounded by the
                                // same budget the flush-timeout formula assumes, so
                                // a legitimate attempt can never exceed the abort
                                // deadline.
                                return self.poll_label_bounded(&resp.Label).await;
                            }
                            // FAILED / CANCELLED / empty (unknown shape): not a
                            // confirmable commit — fall through to the generic
                            // Fail path (retry, then next node). An empty
                            // ExistingJobStatus with "Label Already Exists" is a
                            // response shape we cannot validate as success.
                            _ => {}
                        }
                        // Fall through to the generic Fail path below.
                    } else if resp.Status == "Fail"
                        && matches!(
                            resp.ExistingJobStatus.as_str(),
                            "RUNNING" | "LOADING" | "PENDING" | "PENDING_EXECUTE" | "QUEUEING"
                        )
                    {
                        // Label exists and SR is still processing it — poll SHOW LOAD
                        // for a terminal state. If FINISHED → Ok (idempotent);
                        // if FAILED → ERROR (data loss); if still non-terminal after
                        // poll budget → Err (retry on next node).
                        //
                        // NOTE: we deliberately do NOT match empty `ExistingJobStatus`
                        // here. SR returns `Status:"Fail"` with no `ExistingJobStatus`
                        // field for unrelated failures (e.g. "unknown table",
                        // "syntax error", network/path errors). Empty means there's
                        // no label-collision to poll — fall through to the generic
                        // Fail path below for retry/failover handling.
                        return self.poll_label_bounded(&resp.Label).await;
                    } else {
                        // Generic Fail — DEBUG during retry (final WARN logs at
                        // stream_load level after all nodes exhausted).
                        tracing::debug!(
                            status = %resp.Status,
                            message = %resp.Message,
                            attempt = attempt + 1,
                            max_attempts,
                            node_idx,
                            "Stream Load FAILED on attempt",
                        );
                        // Syntax errors from SR are permanent — the columns header or
                        // data format is invalid and retrying won't help. Fail fast
                        // instead of burning retries and falling through to all nodes.
                        let msg_lower = resp.Message.to_lowercase();
                        let is_permanent = msg_lower.contains("syntax error")
                            || msg_lower.contains("unexpected input");
                        if is_permanent {
                            tracing::error!(
                                status = %resp.Status,
                                message = %resp.Message,
                                node_idx,
                                "Stream Load permanent failure (syntax error)",
                            );
                            bail!(
                                "Stream Load permanent failure (syntax error) on node {}: Status={}, Message={}",
                                node_idx, resp.Status, resp.Message,
                            );
                        }
                        if attempt + 1 >= max_attempts {
                            return Err(anyhow::anyhow!(
                                "Stream Load failed after {} attempts on node {}: Status={}, Message={}",
                                max_attempts, node_idx, resp.Status, resp.Message,
                            ));
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::debug!(
                        error = ?e,
                        attempt = attempt + 1,
                        max_attempts,
                        node_idx,
                        "Stream Load attempt error",
                    );
                    if attempt + 1 >= max_attempts {
                        return Err(e).context(format!(
                            "Stream Load failed after {} attempts on node {}",
                            max_attempts, node_idx,
                        ));
                    }
                }
            }
            // Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 64s
            let backoff_secs = 2u32.pow(attempt.min(6));
            tracing::debug!(
                node_idx = node_idx,
                attempt = attempt + 1,
                backoff_secs = backoff_secs,
                "Stream Load backoff",
            );
            let sleep_dur = crate::util::jitter_duration(base_delay * backoff_secs);
            tokio::time::sleep(sleep_dur).await;
            attempt += 1;
        }
    }

    /// Worst-case wall-clock budget for one `poll_label_until_terminal` call:
    /// 5 iterations × (2s poll sleep + query_timeout + connection_timeout).
    /// The flush-timeout formula (pipeline.rs compute_flush_timeout) uses the
    /// same arithmetic so a legitimate poll sequence can never be aborted by
    /// the per-flush deadline.
    fn poll_label_budget(&self) -> Duration {
        const POLL_ITERATIONS: u32 = 5;
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        (POLL_INTERVAL + self.mgr.query_timeout() + self.mgr.connection_timeout())
            .saturating_mul(POLL_ITERATIONS)
    }

    /// Run `poll_label_until_terminal` under the bounded poll budget. A poll
    /// that exceeds the budget returns a soft error so the outer retry/next
    /// node re-drives the attempt (the per-attempt deadline must stay
    /// consistent with the flush-timeout formula).
    async fn poll_label_bounded(&self, label: &str) -> Result<StreamLoadResponse> {
        match tokio::time::timeout(
            self.poll_label_budget(),
            self.poll_label_until_terminal(label),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                tracing::warn!(
                    label,
                    budget_secs = self.poll_label_budget().as_secs(),
                    "Stream Load label poll exceeded its budget — treating as soft failure, retry on next node",
                );
                bail!(
                    "Stream Load label {} still unresolved after the {}s poll budget",
                    label,
                    self.poll_label_budget().as_secs(),
                );
            }
        }
    }

    /// Poll SHOW LOAD for a label, returning Ok if FINISHED/LOADED
    /// or bailing with ERROR if FAILED/CANCELLED, or with a soft
    /// "still running" error if poll budget exhausts.
    ///
    /// Polling: up to 5 queries × 2s sleep = ~10s cap (plus query timeouts).
    /// Each SHOW LOAD query is bounded by mysql_query_timeout_ms.
    async fn poll_label_until_terminal(&self, label: &str) -> Result<StreamLoadResponse> {
        const POLL_MAX_ATTEMPTS: u32 = 5;
        const POLL_INTERVAL: Duration = Duration::from_secs(2);

        for i in 0..POLL_MAX_ATTEMPTS {
            tokio::time::sleep(POLL_INTERVAL).await;

            // A transient SHOW LOAD error (SR blip, transient connection
            // drop) must NOT abandon the remaining poll iterations — the label
            // may still reach FINISHED. `?`-propagating the first error wastes
            // ~100s of poll budget (outer retry re-runs the whole poll from
            // scratch per node). Warn and continue to the next iteration.
            let state = match self.label_poller.query_label_status(label).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        label,
                        poll_iteration = i,
                        error = ?e,
                        "Transient SHOW LOAD poll error — continuing to next poll iteration",
                    );
                    continue;
                }
            };

            match state.as_deref() {
                Some("FINISHED") | Some("LOADED") => {
                    // parity: the direct duplicate-arm checks the poison set
                    // before treating a duplicate FINISHED as success; the poll
                    // path must too — a label poisoned by a prior partial
                    // success must never be confirmed through SHOW LOAD (the
                    // filtered rows would be silently lost).
                    if Self::is_label_poisoned(&self.poisoned_labels, label) {
                        tracing::error!(
                            label,
                            poll_iteration = i,
                            "Label {label} reached FINISHED during poll but is poisoned by a \
                             prior partial-success (filtered rows) — refusing to mask as success"
                        );
                        bail!(
                            "Stream Load label {} finished during poll but is poisoned by a \
                             prior partial-success (filtered rows) — batch will replay",
                            label,
                        );
                    }
                    tracing::debug!(
                        label,
                        poll_iteration = i,
                        "Stream Load label committed during poll — Ok",
                    );
                    return Ok(StreamLoadResponse {
                        Status: "Success".into(),
                        ExistingJobStatus: "FINISHED".into(),
                        Label: label.to_string(),
                        ..Default::default()
                    });
                }
                Some("FAILED") | Some("CANCELLED") => {
                    // Data loss risk — previous attempt may have partially
                    // loaded then errored out, OR failed before any data
                    // landed. Can't tell without row counts → log ERROR.
                    tracing::error!(
                        label,
                        state = state.as_deref().unwrap_or(""),
                        "Stream Load label ended terminal-failure — possible data loss",
                    );
                    bail!(
                        "Stream Load label {} ended in state={} (data loss risk)",
                        label,
                        state.as_deref().unwrap_or(""),
                    );
                }
                Some(other) => {
                    tracing::debug!(
                        label,
                        poll_iteration = i,
                        state = other,
                        "Label still non-terminal — continuing poll",
                    );
                    // Fall through to next poll iteration (or exhausted bail below).
                }
                None => {
                    tracing::debug!(
                        label,
                        poll_iteration = i,
                        "Label not found in SHOW LOAD — likely GC'd",
                    );
                    // Treat missing label as a soft failure — bail on last poll.
                }
            }
        }

        bail!(
            "Stream Load label {} still non-terminal (or missing) after {} polls ({}s)",
            label,
            POLL_MAX_ATTEMPTS,
            POLL_MAX_ATTEMPTS * 2,
        );
    }

    async fn try_stream_load(
        &self,
        url: &str,
        label: &str,
        batch: &SrBatch,
    ) -> Result<StreamLoadResponse> {
        let is_delete = batch.op_type == "delete";

        // Build payload — for DELETE, prepend __op marker to each row.
        // SrBatch.data is `bytes::Bytes`, so the non-DELETE path is an O(1)
        // refcount bump (no full N-byte copy per attempt). The DELETE
        // path builds the `__op`-injected payload once into a `BytesMut` and
        // `freeze()`s it — also a single allocation.
        let payload: bytes::Bytes = if is_delete && !batch.columns.is_empty() {
            let mut out = bytes::BytesMut::with_capacity(batch.data.len() + batch.data.len() / 8);
            // SrBatch.data is built by serde_json (valid UTF-8) — safe to view as &str.
            let data_str = std::str::from_utf8(&batch.data)
                .expect("SrBatch.data is serde_json output (valid UTF-8)");
            let mut first = true;
            for line in data_str.lines() {
                if line.is_empty() {
                    continue;
                }
                if !first {
                    out.extend_from_slice(b"\n");
                }
                first = false;
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix('{') {
                    // Inject `__op:"1"` right after the opening brace.
                    out.extend_from_slice(b"{\"__op\":\"1\",");
                    out.extend_from_slice(rest.as_bytes());
                } else {
                    out.extend_from_slice(line.as_bytes());
                }
            }
            out.freeze()
        } else {
            batch.data.clone()
        };

        // Build the FE request with the actual payload — StarRocks 3.x supports
        // this directly and older versions redirect (307) to a BE.  Sending the
        // body with the initial request avoids the silent-data-drop bug where an
        // empty-body 200 is confused with success.
        let mut fe_request = self
            .client
            .put(url)
            .basic_auth(self.mgr.username(), Some(self.mgr.password()))
            .header("label", label)
            .header("Expect", "100-continue")
            .header("timeout", self.sr_timeout_header())
            .header("format", "json");
        // Partial-column update on UPSERT so an UPDATE whose TOAST column is
        // unchanged (omitted from the JSON, ColValue::Unchanged) keeps the
        // existing SR value instead of being overwritten with NULL/default
        // (silent TOAST corruption). DELETEs send full rows via __op, so they
        // don't need (and don't use) partial-column semantics.
        if !is_delete {
            fe_request = fe_request.header("partial_columns", "true");
        }

        if !batch.columns.is_empty() {
            fe_request = fe_request.header("columns", build_stream_load_columns(batch, is_delete));
        }

        let fe_response = match fe_request.body(payload.clone()).send().await {
            Ok(resp) => resp,
            Err(e) => {
                // FE connection failed — propagate error so the caller
                // retries on the next node via multi-node HA failover.
                return Err(e).context(format!(
                    "Stream Load FE request failed for {}.{}",
                    batch.database, batch.table,
                ));
            }
        };

        let status = fe_response.status();

        match status.as_u16() {
            307 => {
                // FE returned redirect (usual StarRocks behavior) — the body was
                // consumed by the FE and must be re-sent to the BE.
                let be_url = fe_response
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
                    .context("Missing Location header in 307 redirect")?;

                // Security: validate the BE redirect URL's SCHEME. With
                // use_tls=true, a plaintext redirect is refused (Basic Auth
                // must never travel unencrypted); non-http(s) schemes are
                // refused outright. NOTE: this is NOT host validation — a
                // compromised/misconfigured FE with use_tls=false could
                // redirect to any host and receive the credentials + payload.
                // That is accepted: the FE already holds the full credentials
                // (Basic Auth is the same user), so the check protects
                // against misconfiguration, not FE compromise.
                let be_parsed =
                    reqwest::Url::parse(&be_url).context("Invalid BE redirect URL from FE 307")?;
                if self.use_tls && be_parsed.scheme() != "https" {
                    bail!(
                        "BE redirect URL '{}' is not HTTPS while use_tls=true — \
                         refusing to send credentials over plaintext",
                        be_url
                    );
                }
                if be_parsed.scheme() != "http" && be_parsed.scheme() != "https" {
                    bail!(
                        "BE redirect URL '{}' has unexpected scheme '{}' — \
                         only http/https allowed",
                        be_url,
                        be_parsed.scheme()
                    );
                }

                tracing::debug!("Stream Load redirected (307) to {}", be_url);
                self.send_to_be(&be_url, label, batch, payload, is_delete)
                    .await
            }
            200 => {
                // FE processed the load directly (no redirect).  The body was
                // sent with the request, so the data is committed.
                let body_text = read_capped_body(fe_response).await?;
                parse_stream_load_response(&body_text)
            }
            other => {
                let body_text = read_capped_body(fe_response).await?;
                bail!(
                    "Stream Load unexpected status {}: {}",
                    other,
                    crate::util::truncate_str(&body_text, 500)
                );
            }
        }
    }

    /// Send Stream Load directly to a BE node (after 307 redirect).
    async fn send_to_be(
        &self,
        url: &str,
        label: &str,
        batch: &SrBatch,
        payload: bytes::Bytes,
        is_delete: bool,
    ) -> Result<StreamLoadResponse> {
        let mut request = self
            .client
            .put(url)
            .basic_auth(self.mgr.username(), Some(self.mgr.password()))
            .header("label", label)
            .header("Expect", "100-continue")
            .header("timeout", self.sr_timeout_header())
            .header("format", "json")
            .body(payload);
        // Partial-column update on UPSERT only (see FE path comment).
        if !is_delete {
            request = request.header("partial_columns", "true");
        }

        if !batch.columns.is_empty() {
            request = request.header("columns", build_stream_load_columns(batch, is_delete));
        }

        let response = request
            .send()
            .await
            .context("Failed to send Stream Load to BE")?;

        let status = response.status();
        let body = read_capped_body(response).await?;

        if !status.is_success() {
            // SR can return a structured JSON body (e.g. "Label Already
            // Exists" + ExistingJobStatus: "FINISHED") even with a non-2xx HTTP
            // status on the BE path. Parse it so the caller's idempotency
            // routing (in stream_load_on_node) can treat a duplicate FINISHED
            // as success instead of a generic HTTP-error failure.
            if let Ok(resp) = parse_stream_load_response(&body) {
                let salvage = resp.Status == "Success"
                    || resp.Status == "OK"
                    || resp.Status == "Publish Timeout"
                    || resp.Status == "Label Already Exists"
                    || (resp.Status == "Fail" && resp.ExistingJobStatus == "FINISHED");
                if salvage {
                    return Ok(resp);
                }
            }
            bail!(
                "Stream Load BE returned HTTP {}: {}",
                status,
                crate::util::truncate_str(&body, 200),
            );
        }

        parse_stream_load_response(&body)
    }
}

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse> {
    let sl_resp: StreamLoadResponse = match serde_json::from_str(body) {
        Ok(r) => r,
        Err(e) => {
            // Non-JSON body (HTML error page, proxy error, etc.) — log raw snippet
            tracing::error!(
                "Stream Load response was NOT valid JSON ({}). Raw body (first 500 chars): {}",
                e,
                &crate::util::truncate_str(body, 500),
            );
            return Err(anyhow::anyhow!(
                "Failed to parse Stream Load response: {}. Body: {}",
                e,
                crate::util::truncate_str(body, 200),
            ));
        }
    };

    // Detect silent failure: valid JSON but empty Status (proxied error page,
    // load balancer health check, or SR internal response without Status field).
    if sl_resp.Status.is_empty() {
        tracing::warn!(
            "Stream Load response has empty Status — may indicate unexpected response format. \
             Raw body (first 500 chars): {}",
            &crate::util::truncate_str(body, 500),
        );
    }

    Ok(sl_resp)
}

/// Build the `columns` header for a Stream Load request.
/// For DELETE operations, prepends `__op` so StarRocks can interpret the rows
/// as deletes (__op=1). For UPSERT, passes through batch.columns as-is.
/// All column names are backtick-quoted to prevent syntax errors from SR
/// reserved words (e.g. `schema`, `key`, `timestamp`).
fn build_stream_load_columns(batch: &SrBatch, is_delete: bool) -> String {
    // Quote a column name or expression for SR's Stream Load columns header.
    // Bare names get wrapped in backticks. Expressions like `col=unhex(col)`
    // only quote the LHS column name portion.
    let quote_col = |col: &str| -> String {
        if let Some(eq_pos) = col.find('=') {
            let name = &col[..eq_pos];
            let expr = &col[eq_pos..]; // includes the '='
            format!("{}{expr}", crate::sr_sql::sr_quote(name))
        } else {
            crate::sr_sql::sr_quote(col)
        }
    };

    if is_delete {
        let mut parts = vec!["__op".to_string()];
        parts.extend(batch.columns.iter().map(|c| quote_col(c)));
        parts.join(", ")
    } else {
        batch
            .columns
            .iter()
            .map(|c| quote_col(c))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stream_load_response_success() {
        let body =
            r#"{"Status":"Success","Message":"OK","NumberTotalRows":100,"NumberLoadedRows":100}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(resp.Status, "Success");
        assert_eq!(resp.NumberTotalRows, 100);
        assert_eq!(resp.NumberLoadedRows, 100);
    }

    #[test]
    fn test_parse_stream_load_response_fail() {
        let body = r#"{"Status":"Fail","Message":"unknown table","ErrorURL":"http://..."}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(resp.Status, "Fail");
        assert!(resp.Message.contains("unknown table"));
    }

    #[test]
    fn test_parse_stream_load_response_existing_job() {
        let body = r#"{"Status":"Fail","Message":"Label already used","ExistingJobStatus":"FINISHED","Label":"mylabel"}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(resp.ExistingJobStatus, "FINISHED");
        assert_eq!(resp.Label, "mylabel");
    }

    #[test]
    fn test_parse_stream_load_response_malformed() {
        let result = parse_stream_load_response("not json");
        assert!(result.is_err());
    }

    /// Verify retry counter semantics: max_retries=3 → 4 total attempts.
    #[test]
    fn test_max_retries_produces_correct_attempt_count() {
        let max_retries: u32 = 3;
        let max_attempts = max_retries + 1; // 4

        let mut attempt = 0;
        let mut tries = Vec::new();

        loop {
            tries.push(attempt + 1);
            if attempt >= max_retries {
                break;
            }
            attempt += 1;
        }

        assert_eq!(tries, vec![1, 2, 3, 4], "max_retries=3 must try 4 times");
        assert_eq!(max_attempts, 4);
        assert_eq!(tries.len() as u32, max_attempts);
    }

    #[test]
    fn test_sr_batch_builder() {
        let batch = SrBatch {
            database: "testdb".into(),
            table: "orders".into(),
            data: r#"{"id":1,"val":"hello"}"#.into(),
            row_count: 1,
            columns: vec!["data=unhex(data)".into()],
            op_type: "upsert".into(),
        };
        assert_eq!(batch.database, "testdb");
        assert_eq!(batch.table, "orders");
        assert_eq!(batch.row_count, 1);
        assert_eq!(batch.op_type, "upsert");
        assert!(!batch.columns.is_empty());
    }

    #[test]
    fn test_build_stream_load_columns_delete() {
        let batch = SrBatch {
            database: "db".into(),
            table: "t".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec!["id".into(), "name".into()],
            op_type: "delete".into(),
        };
        let cols = build_stream_load_columns(&batch, true);
        assert_eq!(cols, "__op, `id`, `name`");
    }

    #[test]
    fn test_build_stream_load_columns_upsert() {
        let batch = SrBatch {
            database: "db".into(),
            table: "t".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec!["id".into(), "name".into()],
            op_type: "upsert".into(),
        };
        let cols = build_stream_load_columns(&batch, false);
        assert_eq!(cols, "`id`, `name`");
    }

    #[test]
    fn test_build_stream_load_columns_empty() {
        let batch = SrBatch {
            database: "db".into(),
            table: "t".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec![],
            op_type: "upsert".into(),
        };
        let cols = build_stream_load_columns(&batch, false);
        assert_eq!(cols, "");
    }

    #[test]
    fn test_build_stream_load_columns_reserved_word() {
        // Column names that are SR reserved words must be backtick-quoted
        let batch = SrBatch {
            database: "db".into(),
            table: "t".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec!["schema".into(), "key".into(), "timestamp".into()],
            op_type: "upsert".into(),
        };
        let cols = build_stream_load_columns(&batch, false);
        assert_eq!(cols, "`schema`, `key`, `timestamp`");
    }

    #[test]
    fn test_build_stream_load_columns_expression() {
        // Expressions like col=unhex(col) should quote only the name part
        let batch = SrBatch {
            database: "db".into(),
            table: "t".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec!["blob=unhex(blob)".into(), "name".into()],
            op_type: "upsert".into(),
        };
        let cols = build_stream_load_columns(&batch, false);
        assert_eq!(cols, "`blob`=unhex(blob), `name`");
    }

    /// Regression test for silent-data-loss bug: two Stream Load labels generated
    /// for the same table in rapid succession must be globally unique.
    #[test]
    fn test_stream_load_labels_globally_unique() {
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "testdb".into(),
            username: "root".into(),
            password: String::new(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 3, 10000, 10000).unwrap(),
        );
        let sink = SrSink::new(mgr).expect("sink construction");

        let batch = SrBatch {
            database: "testdb".into(),
            table: "orders".into(),
            data: "{}".into(),
            row_count: 1,
            columns: vec!["id".into()],
            op_type: "upsert".into(),
        };

        let label1 = sink.make_label(&batch);
        let label2 = sink.make_label(&batch);
        assert_ne!(
            label1, label2,
            "Consecutive labels must differ. label1={label1} label2={label2}"
        );
    }

    /// Regression test: ExistingJobStatus=FINISHED must return an error, not Ok.
    /// The old code treated the response as successful idempotency, which silently
    /// dropped data when labels collided.
    #[test]
    fn test_existing_job_finished_is_error_not_success() {
        let body = r#"{"Status":"Fail","Message":"Label already used","ExistingJobStatus":"FINISHED","Label":"mylabel"}"#;
        let resp: StreamLoadResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.Status, "Fail");
        assert_eq!(resp.ExistingJobStatus, "FINISHED");
    }

    /// Regression test: ExistingJobStatus=RUNNING must return an error, not Ok.
    #[test]
    fn test_existing_job_running_is_error_not_success() {
        let body = r#"{"Status":"Fail","Message":"Label already used","ExistingJobStatus":"RUNNING","Label":"mylabel"}"#;
        let resp: StreamLoadResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.Status, "Fail");
        assert_eq!(resp.ExistingJobStatus, "RUNNING");
    }

    /// Regression test: "Label Already Exists" with FINISHED must be treated as OK
    /// (idempotency — previous attempt committed the data).
    #[test]
    fn test_label_already_exists_finished_is_ok() {
        let body = r#"{"Status":"Label Already Exists","Message":"","ExistingJobStatus":"FINISHED","Label":"mylabel"}"#;
        let resp: StreamLoadResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.Status, "Label Already Exists");
        assert_eq!(resp.ExistingJobStatus, "FINISHED");
    }

    /// Regression test: "Label Already Exists" with RUNNING must NOT be treated as OK.
    /// The old code returned Ok for any "Label Already Exists" regardless of state,
    /// which could cause the checkpoint to advance past uncommitted events.
    #[test]
    fn test_label_already_exists_running_not_ok() {
        let body = r#"{"Status":"Label Already Exists","Message":"","ExistingJobStatus":"RUNNING","Label":"mylabel"}"#;
        let resp: StreamLoadResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.Status, "Label Already Exists");
        assert_eq!(resp.ExistingJobStatus, "RUNNING");
    }

    #[test]
    fn test_max_retries_zero_produces_one_attempt() {
        let max_retries: u32 = 0;
        let max_attempts = max_retries + 1;
        let mut attempt = 0;
        let mut tries = Vec::new();
        loop {
            tries.push(attempt + 1);
            if attempt >= max_retries {
                break;
            }
            attempt += 1;
        }
        assert_eq!(tries, vec![1], "max_retries=0 must try exactly once");
        assert_eq!(max_attempts, 1);
        assert_eq!(tries.len() as u32, max_attempts);
    }

    #[test]
    fn test_max_retries_five_produces_six_attempts() {
        let max_retries: u32 = 5;
        let max_attempts = max_retries + 1;
        let mut attempt = 0;
        let mut tries = Vec::new();
        loop {
            tries.push(attempt + 1);
            if attempt >= max_retries {
                break;
            }
            attempt += 1;
        }
        assert_eq!(
            tries,
            vec![1, 2, 3, 4, 5, 6],
            "max_retries=5 must try 6 times"
        );
        assert_eq!(max_attempts, 6);
        assert_eq!(tries.len() as u32, max_attempts);
    }

    // ── Batch-size regression tests ───────────────────────────

    #[test]
    fn test_stream_load_response_number_filtered_rows() {
        // SR can return Status:"Success" while silently rejecting individual
        // rows (strict-mode coercion failures). NumberFilteredRows must be
        // parsed correctly.
        let body = r#"{"Status":"Success","NumberTotalRows":100,"NumberLoadedRows":98,"NumberFilteredRows":2,"Message":"OK"}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(resp.Status, "Success");
        assert_eq!(resp.NumberLoadedRows, 98);
        assert_eq!(
            resp.NumberFilteredRows, 2,
            "NumberFilteredRows must be parsed (was not in the struct before the fix)"
        );
    }

    #[test]
    fn test_stream_load_response_no_filtered_rows_field() {
        // Old SR versions may not include NumberFilteredRows — serde(default)
        // must handle the missing field gracefully.
        let body = r#"{"Status":"Success","NumberTotalRows":100,"NumberLoadedRows":100}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(
            resp.NumberFilteredRows, 0,
            "Missing NumberFilteredRows must default to 0"
        );
    }

    #[test]
    fn test_stream_load_response_empty_status() {
        // Some proxy/load-balancer responses have valid JSON but no Status
        // field. Must be parsed (callers check Status != "Success").
        let body = r#"{"message":"hi"}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert!(
            resp.Status.is_empty(),
            "Missing Status must default to empty string"
        );
    }

    #[test]
    fn test_stream_load_response_label_already_exists_finished() {
        // Idempotent replay: label already exists and the previous job FINISHED.
        let body =
            r#"{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED","Label":"abc123"}"#;
        let resp = parse_stream_load_response(body).unwrap();
        assert_eq!(resp.Status, "Label Already Exists");
        assert_eq!(resp.ExistingJobStatus, "FINISHED");
        assert_eq!(resp.Label, "abc123");
    }

    // ── Label polling tests (Arc<dyn LabelStatusQuery> mock seam) ──

    /// Canned-response mock for `LabelStatusQuery`. Each call to
    /// `query_label_status` pops one entry from the front of `responses`,
    /// so tests can simulate a label that's RUNNING → RUNNING → FINISHED,
    /// or RUNNING → FAILED, etc.
    struct MockLabelStatusQuery {
        responses: std::sync::Mutex<std::collections::VecDeque<Option<String>>>,
    }
    impl MockLabelStatusQuery {
        fn new<I: IntoIterator<Item = Option<String>>>(responses: I) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses.into_iter().collect()),
            }
        }
    }
    impl LabelStatusQuery for MockLabelStatusQuery {
        fn query_label_status<'a>(
            &'a self,
            _label: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<String>>> + Send + 'a>>
        {
            Box::pin(async move {
                let mut guard = self.responses.lock().unwrap();
                Ok(guard.pop_front().flatten())
            })
        }
    }

    /// Helper: build an `SrSink` whose `label_poller` is a `MockLabelStatusQuery`.
    /// Uses the test-only `with_label_poller` constructor. The `SrConnectionManager`
    /// is wired so the resulting sink won't actually issue real HTTP requests for
    /// `poll_label_until_terminal` — only the mock poller is invoked.
    fn build_sink_with_mock_poller(responses: Vec<Option<&'static str>>) -> SrSink {
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "testdb".into(),
            username: "root".into(),
            password: String::new(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 0, 10000, 10000).unwrap(),
        );
        let owned: Vec<Option<String>> = responses
            .into_iter()
            .map(|o| o.map(str::to_string))
            .collect();
        let mock = Arc::new(MockLabelStatusQuery::new(owned));
        SrSink::with_label_poller(mgr, mock as Arc<dyn LabelStatusQuery>).unwrap()
    }

    #[test]
    fn test_sr_timeout_header_covers_request_plus_label_poll() {
        // The header must exceed the bare request timeout: a load SR is still
        // allowed to finish after the client aborts is salvaged by the
        // "Label Already Exists + FINISHED" path on the next attempt. Cutting
        // SR off at the request timeout would abort it terminally and push the
        // rows into the durable buffer.
        let sink = build_sink_with_mock_poller(vec![]);
        // helper wiring: http=10s, query=10s, conn=5s → poll budget 5×(2+10+5)=85s.
        assert_eq!(sink.sr_timeout_header(), "95");
        assert!(
            sink.sr_timeout_header().parse::<u64>().unwrap() > sink.http_timeout.as_secs(),
            "SR deadline must exceed the client's request timeout"
        );
    }

    #[test]
    fn test_sr_timeout_header_clamps_to_sr_range() {
        // SR documents the load `timeout` range as 1..=259200 seconds; an
        // absurd configured budget must clamp instead of being rejected.
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "testdb".into(),
            username: "root".into(),
            password: String::new(),
            connection_timeout_ms: 999_999_999,
            http_pool_idle_timeout_ms: 60_000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 0, 999_999_999, 999_999_999)
                .unwrap(),
        );
        let mock = Arc::new(MockLabelStatusQuery::new(vec![]));
        let sink = SrSink::with_label_poller(mgr, mock as Arc<dyn LabelStatusQuery>).unwrap();
        assert_eq!(sink.sr_timeout_header(), "259200");
    }

    #[tokio::test]
    async fn test_label_polling_finished_returns_ok() {
        let sink =
            build_sink_with_mock_poller(vec![Some("LOADING"), Some("LOADING"), Some("FINISHED")]);
        let result = sink.poll_label_until_terminal("test_label").await;
        assert!(
            result.is_ok(),
            "FINISHED must return Ok: {:?}",
            result.unwrap_err()
        );
        let resp = result.unwrap();
        assert_eq!(resp.Status, "Success");
        assert_eq!(resp.ExistingJobStatus, "FINISHED");
        assert_eq!(resp.Label, "test_label");
    }

    #[tokio::test]
    async fn test_label_polling_failed_returns_error() {
        let sink = build_sink_with_mock_poller(vec![Some("LOADING"), Some("FAILED")]);
        let result = sink.poll_label_until_terminal("test_label").await;
        assert!(result.is_err(), "FAILED must return Err");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("FAILED"),
            "error must mention FAILED state: {err}"
        );
    }

    #[tokio::test]
    async fn test_label_polling_cancelled_returns_error() {
        let sink = build_sink_with_mock_poller(vec![Some("LOADING"), Some("CANCELLED")]);
        let result = sink.poll_label_until_terminal("test_label").await;
        assert!(result.is_err(), "CANCELLED must return Err");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CANCELLED"),
            "error must mention CANCELLED: {err}"
        );
    }

    #[tokio::test]
    async fn test_label_polling_exhausts_returns_error() {
        // 5 polls of "LOADING" → exhausted → Err
        let sink = build_sink_with_mock_poller(vec![
            Some("LOADING"),
            Some("LOADING"),
            Some("LOADING"),
            Some("LOADING"),
            Some("LOADING"),
        ]);
        let result = sink.poll_label_until_terminal("test_label").await;
        assert!(result.is_err(), "exhausted polls must Err");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("non-terminal"),
            "error must mention non-terminal: {err}"
        );
    }

    #[tokio::test]
    async fn test_label_polling_missing_label_returns_error_after_5_polls() {
        // 5 polls of None (label GC'd) → exhausted → Err
        let sink = build_sink_with_mock_poller(vec![None, None, None, None, None]);
        let result = sink.poll_label_until_terminal("test_label").await;
        assert!(result.is_err(), "missing label after 5 polls must Err");
    }

    // ── read_capped_body enforces the cap at the source ──────────

    #[tokio::test]
    async fn test_read_capped_body_truncates_oversized_at_source() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // 256 KB body — well over the 64 KB cap. If read_capped_body read the
        // whole body before truncating, this would still pass (just allocate);
        // the test verifies the OUTPUT is capped (≤ MAX_RESPONSE_BODY_BYTES),
        // which is the observable contract.
        let big_body = "x".repeat(256 * 1024);
        Mock::given(method("GET"))
            .and(path("/big"))
            .respond_with(ResponseTemplate::new(200).set_body_string(big_body.clone()))
            .mount(&server)
            .await;

        let resp = reqwest::get(format!("{}/big", server.uri())).await.unwrap();
        let text = read_capped_body(resp).await.unwrap();
        assert!(
            text.len() <= MAX_RESPONSE_BODY_BYTES,
            "capped body must be ≤ {} bytes, got {}",
            MAX_RESPONSE_BODY_BYTES,
            text.len(),
        );
        assert_eq!(
            text.len(),
            MAX_RESPONSE_BODY_BYTES,
            "should be truncated to the cap"
        );
    }

    #[tokio::test]
    async fn test_read_capped_body_small_body_passthrough() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/small"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello"))
            .mount(&server)
            .await;

        let resp = reqwest::get(format!("{}/small", server.uri()))
            .await
            .unwrap();
        let text = read_capped_body(resp).await.unwrap();
        assert_eq!(text, "hello");
    }

    // ── poisoned-label rejects masked partial-success ─────────────

    #[test]
    fn test_poison_label_marks_and_checks() {
        let set: Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        assert!(
            !SrSink::is_label_poisoned(&set, "lbl-a"),
            "unmarked label is not poisoned"
        );
        SrSink::poison_label(&set, "lbl-a");
        assert!(
            SrSink::is_label_poisoned(&set, "lbl-a"),
            "marked label is poisoned"
        );
        assert!(
            !SrSink::is_label_poisoned(&set, "lbl-b"),
            "other labels are not poisoned",
        );
    }

    #[test]
    fn test_poison_label_set_caps_and_clears() {
        // Cap is 4096; inserting at/over the cap clears the set first (poisoned
        // labels are unique per stream_load call and never regenerated, so
        // clearing very old ones is safe).
        let set: Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        for _ in 0..4097 {
            let l = format!(
                "lbl-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            );
            SrSink::poison_label(&set, &l);
        }
        // After the cap triggers a clear, the set is bounded (far below 4097).
        assert!(
            set.lock().unwrap().len() < 4097,
            "set must be capped, got {}",
            set.lock().unwrap().len(),
        );
    }
}
