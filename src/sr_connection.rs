//! StarRocks connection manager with multi-node HA support.
//!
//! Manages a list of FE nodes and tracks which node last succeeded for writes.
//! On write failure, clears the cached node and tries all nodes in round-robin.
//! Supports configurable per-node connect timeout.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use mysql_async::prelude::*;

use crate::config::{SrConfig, SrNodeConfig, SrTlsConfig};

/// Trait object seam for unit-testing `poll_label_until_terminal` in sink.rs
/// without spinning a real StarRocks. Production code uses
/// `SrConnectionManager` directly; tests pass a `MockLabelStatusQuery`
/// returning canned `Option<State>` responses.
pub trait LabelStatusQuery: Send + Sync {
    /// Poll SHOW LOAD for a label and return its current State column value.
    /// Returns `Ok(None)` if the label has been GC'd (no rows) and
    /// `Ok(Some(state))` for any of SR's terminal or in-progress states.
    fn query_label_status<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + 'a>>;
}

impl LabelStatusQuery for SrConnectionManager {
    fn query_label_status<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + 'a>> {
        Box::pin(Self::query_label_status(self, label))
    }
}

/// URL-encode a string for safe inclusion in HTTP path segments.
///
/// `form_urlencoded` encodes space as `+`, which in a
/// URL path segment is a literal plus, not a space — StarRocks would look for
/// a table literally named `my+table`. Use `percent_encoding` so space becomes
/// `%20` (unambiguous in path components). Unreserved chars (`- . _ ~`) and
/// alphanumerics are left intact.
const SR_PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn urlencode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, SR_PATH_SEGMENT).collect()
}

/// Detect the benign "source table not found" case for a snapshot backup-rename.
///
/// `swap_shadow_to_main` (snapshot.rs:304) performs:
///   1. `ALTER TABLE \`db\`.\`table\` RENAME \`table\`_drop_<millis>_<counter>`
///   2. `ALTER TABLE \`db\`.\`shadow\` RENAME \`table\``
///   3. `DROP TABLE IF EXISTS \`table\`_drop_<millis>_<counter>`
///
/// Step 1 fails with "Table not found" on the first snapshot of a new table
/// (no pre-existing main to back up). The caller (snapshot.rs:330) already logs
/// this at `debug!`. This helper prevents the generic executor from also
/// emitting `warn!` on every FE node (3× log spam per first snapshot).
///
/// Scope: only matches when the rename target contains `_DROP_<13+ digit
/// millis>` — the format produced by `swap_shadow_to_main` (snapshot.rs:315).
/// This avoids masking real rename errors in step 2 (shadow→main) or the DDL
/// poller's table-rename path.
fn is_snapshot_backup_rename_not_found(stmt_upper: &str, msg: &str) -> bool {
    if !stmt_upper.starts_with("ALTER TABLE") {
        return false;
    }
    // Must be a table-rename, not a column-rename.
    if !stmt_upper.contains(" RENAME ") || stmt_upper.contains("RENAME COLUMN") {
        return false;
    }
    // StarRocks "Table X is not found." phrasing; also accept Pg-isms just in case.
    if !msg.contains("is not found")
        && !msg.contains("does not exist")
        && !msg.contains("doesn't exist")
    {
        return false;
    }
    // Recognize the backup-rename target: `<prefix>_DROP_<millis:>=13 digits>[_<counter>]`
    if let Some(pos) = stmt_upper.rfind("_DROP_") {
        let after = &stmt_upper[pos + "_DROP_".len()..];
        let first_segment: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        return first_segment.len() >= 13;
    }
    false
}

/// Split a (possibly multi-statement) SQL string into individual statements
/// via polyglot_sql. On parse failure, pass the raw SQL through as a single
/// statement. On `generate` failure for one statement, log + drop it (preserves
/// the existing out-of-scope behavior).
///
/// Invoked via `spawn_blocking` so a polyglot_sql panic
/// (deeply nested DDL) is isolated into a `JoinError` instead of propagating
/// through async executors when `execute_sql` is called outside a supervised task.
fn polyglot_parse_to_stmts(sql: &str) -> Vec<String> {
    match polyglot_sql::parse(sql, polyglot_sql::DialectType::StarRocks) {
        Ok(exprs) if exprs.len() <= 1 => {
            vec![sql.trim().to_string()]
        }
        Ok(exprs) => {
            let mut stmts = Vec::with_capacity(exprs.len());
            for e in &exprs {
                match polyglot_sql::generate(e, polyglot_sql::DialectType::StarRocks) {
                    Ok(s) => stmts.push(s),
                    Err(err) => {
                        tracing::warn!(
                            "polyglot_sql::generate failed for expression — \
                             schema change may be incomplete. \
                             Error: {err}. Expression: {e:?}"
                        );
                    }
                }
            }
            stmts
        }
        Err(_) => {
            vec![sql.trim().to_string()]
        }
    }
}

/// Manages connections to a cluster of StarRocks FE nodes.
///
/// Maintains an in-memory "active node" — the last FE node that successfully
/// handled a write (Stream Load or SQL). Subsequent operations try the active
/// node first; if it fails, the cache is cleared and all nodes are tried in
/// order until one succeeds.
///
/// # Thread safety
/// Uses `std::sync::Mutex` for the active node index — locks are held for
/// microseconds (just read/write a `usize`), so `std::sync::Mutex` is fine
/// in async contexts.
pub struct SrConnectionManager {
    nodes: Vec<SrNodeConfig>,
    connection_timeout: Duration,
    http_timeout: Duration,
    query_timeout: Duration,
    max_retries: u32,
    http_pool_idle_timeout: Duration,
    mysql_max_pool_connections: usize,
    http_tls: SrTlsConfig,
    mysql_tls: SrTlsConfig,
    username: String,
    password: String,
    database: String,
    /// Index of the last node that succeeded a write.
    /// `None` means no active node — will try all nodes.
    active_node: Mutex<Option<usize>>,
    /// Pre-built MySQL connection pool per node. Reused across all
    /// `execute_sql` calls so that DDL churn does not leak pool
    /// background tasks. Disconnected on `shutdown()`.
    pools: Vec<mysql_async::Pool>,
    /// Per-node failure tracking for circuit breaker: last time each node
    /// failed. Nodes that failed within the cooldown period are pushed to the
    /// back of `node_order()` so we don't pay the full connect_timeout on
    /// every operation against a dead node.
    last_failure_at: Mutex<Vec<Option<std::time::Instant>>>,
    /// Cooldown period: a node that failed is deprioritized for this long.
    const_node_failback: Duration,
    /// Set when `shutdown()` begins so concurrent
    /// `execute_sql` / `query_col` operations fail fast instead of racing
    /// pool disconnection. Callers must still quiesce in-flight operations
    /// before invoking `shutdown()` (main.rs awaits CDC + flush + checkpoint
    /// saver first), but this guard tightens the residual race.
    shutting_down: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for SrConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SrConnectionManager")
            .field("nodes", &self.nodes)
            .field("connection_timeout", &self.connection_timeout)
            .field("http_timeout", &self.http_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("max_retries", &self.max_retries)
            .field("http_pool_idle_timeout", &self.http_pool_idle_timeout)
            .field("mysql_max_pool_connections", &self.mysql_max_pool_connections)
            .field("active_node", &self.lock_active())
            .field("pool_count", &self.pools.len())
            .finish()
    }
}

/// Bracket an IPv6 host literal for URL embedding.
/// `::1` → `[::1]`; `192.0.2.1` → `192.0.2.1` (IPv4/hostname unchanged).
fn bracket_ipv6_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{}]", host)
    } else {
        host.to_string()
    }
}

/// Find the index of a column by name (case-insensitive) in a slice of
/// column names. Used to resolve SHOW LOAD's `State`
/// column by name instead of a hardcoded index (2), so a future SR version
/// reordering columns doesn't silently read the wrong value.
fn find_col_index<S: AsRef<str>>(names: &[S], target: &str) -> Result<usize> {
    names
        .iter()
        .position(|n| n.as_ref().eq_ignore_ascii_case(target))
        .ok_or_else(|| anyhow::anyhow!("Column '{target}' not found in result"))
}

impl SrConnectionManager {
    /// Lock the active node mutex, recovering from poison if another
    /// thread panicked while holding the lock. We only store an
    /// `Option<usize>`, so the poisoned value is safe to use.
    fn lock_active(&self) -> std::sync::MutexGuard<'_, Option<usize>> {
        self.active_node.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Create a new connection manager from the SR config.
    pub fn new(
        config: &SrConfig,
        max_retries: u32,
        sink_flush_timeout_ms: u64,
        mysql_query_timeout_ms: u64,
    ) -> Result<Self> {
        let nodes = config.parse_hosts().with_context(|| {
            format!("Invalid SR config (node_urls='{}')", config.node_urls)
        })?;
        let connection_timeout = Duration::from_millis(config.connection_timeout_ms);
        let http_timeout = Duration::from_millis(sink_flush_timeout_ms);
        let query_timeout = Duration::from_millis(mysql_query_timeout_ms);
        tracing::info!(
            "SR connection manager: {} node(s), connection_timeout={:?}, http_timeout={:?}, \
             query_timeout={:?}, max_retries={}, nodes={:?}",
            nodes.len(),
            connection_timeout,
            http_timeout,
            query_timeout,
            max_retries,
            nodes.iter().map(|n| &n.host).collect::<Vec<_>>(),
        );
        let opts_cache: Vec<mysql_async::Opts> = nodes
            .iter()
            .map(|node| -> Result<mysql_async::Opts> {
                let pool_opts = mysql_async::PoolOpts::default()
                    .with_inactive_connection_ttl(Duration::from_secs(300))
                    .with_constraints(
                        mysql_async::PoolConstraints::new(1, config.mysql_max_pool_connections)
                            .context("mysql_max_pool_connections must be >= 1")?,
                    );
                let mut builder = mysql_async::OptsBuilder::default()
                    .ip_or_hostname(&node.host)
                    .tcp_port(node.fe_jdbc_port)
                    .user(Some(&config.username))
                    .pass(Some(&config.password))
                    .db_name(Some(&config.database))
                    .prefer_socket(false)
                    .stmt_cache_size(0)
                    .pool_opts(pool_opts);
                if config.mysql_tls.enable {
                    let mut ssl_opts = mysql_async::SslOpts::default();
                    if let Some(ref ca_path) = config.mysql_tls.ca_cert {
                        let root_certs: Vec<_> = vec![ca_path.clone()]
                            .into_iter()
                            .map(|p| p.into())
                            .collect();
                        ssl_opts = ssl_opts.with_root_certs(root_certs);
                    }
                    builder = builder.ssl_opts(Some(ssl_opts));
                }
                Ok(builder.into())
            })
            .collect::<Result<Vec<_>>>()?;
        let pools = opts_cache
            .iter()
            .map(|opts| mysql_async::Pool::new(opts.clone()))
            .collect();
        let node_count = nodes.len();
        Ok(Self {
            nodes,
            connection_timeout,
            http_timeout,
            query_timeout,
            max_retries,
            http_pool_idle_timeout: Duration::from_millis(config.http_pool_idle_timeout_ms),
            mysql_max_pool_connections: config.mysql_max_pool_connections,
            http_tls: config.http_tls.clone(),
            mysql_tls: config.mysql_tls.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            database: config.database.clone(),
            active_node: Mutex::new(None),
            pools,
            last_failure_at: Mutex::new(vec![None; node_count]),
            const_node_failback: Duration::from_secs(30),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Number of configured FE nodes.
    #[allow(dead_code)]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Return the active node index if one is cached, otherwise `None`.
    pub fn active_node_index(&self) -> Option<usize> {
        *self.lock_active()
    }

    /// Return the active node's host string, if one is cached.
    #[allow(dead_code)]
    pub fn active_host(&self) -> Option<String> {
        self.active_node_index()
            .and_then(|idx| self.nodes.get(idx))
            .map(|n| n.host.clone())
    }

    /// Build a connection-ordered list of node indices.
    ///
    /// If an active node is cached, it appears first; all others follow in
    /// their original order. If no active node is cached, returns all nodes
    /// in config order. Nodes that failed within the cooldown period are
    /// pushed to the back to avoid paying the full connect_timeout on every
    /// operation against a dead node (circuit breaker).
    pub fn node_order(&self) -> Vec<usize> {
        // Copy the active-node value out of the guard so
        // the Mutex is released immediately, instead of holding it through the
        // healthy/cool partitioning loops below (which blocked set/clear_active).
        let active = *self.lock_active();
        let now = std::time::Instant::now();
        let failures = self.last_failure_at.lock().unwrap_or_else(|e| e.into_inner());
        let healthy: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| {
                match failures.get(i) {
                    Some(Some(t)) => now.duration_since(*t) > self.const_node_failback,
                    _ => true, // no failure recorded → healthy
                }
            })
            .collect();
        let cool: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| {
                match failures.get(i) {
                    Some(Some(t)) => now.duration_since(*t) <= self.const_node_failback,
                    _ => false,
                }
            })
            .collect();
        // Active node first (if healthy), then other healthy, then cooling
        match active {
            Some(idx) if idx < self.nodes.len() => {
                let mut order = healthy.clone();
                if let Some(pos) = order.iter().position(|&x| x == idx) {
                    order.remove(pos);
                    order.insert(0, idx);
                }
                order.extend(cool);
                order
            }
            _ => {
                let mut order = healthy;
                order.extend(cool);
                order
            }
        }
    }

    /// Mark a node as the active (successful) node.
    pub fn set_active(&self, node_idx: usize) {
        if node_idx < self.nodes.len() {
            *self.lock_active() = Some(node_idx);
            // Clear failure state on success
            let mut failures = self.last_failure_at.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(slot) = failures.get_mut(node_idx) {
                *slot = None;
            }
        }
    }

    /// Clear the active node cache, e.g. after a write failure.
    /// Call this when the currently active node fails so subsequent
    /// operations try all nodes again.
    pub fn clear_active(&self) {
        *self.lock_active() = None;
    }

    /// Update the active node only if the given index matches the current
    /// active node (i.e., the failing node was the one we were using).
    /// This avoids clearing the active node if a background task or DDL
    /// poller failed on a different node.
    pub fn clear_active_if_match(&self, node_idx: usize) {
        let mut active = self.lock_active();
        if *active == Some(node_idx) {
            *active = None;
        }
        // Record failure timestamp for circuit breaker
        if node_idx < self.nodes.len() {
            let mut failures = self.last_failure_at.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(slot) = failures.get_mut(node_idx) {
                *slot = Some(std::time::Instant::now());
            }
        }
    }

    // ── MySQL/SQL connection ──

    /// Build mysql_async connection opts for a specific node, with an
    /// optional database name override. Pass `None` to connect without
    /// selecting a database (needed for CREATE DATABASE when the target
    /// database doesn't exist yet).
    fn build_mysql_opts_with_db(
        &self,
        node: &SrNodeConfig,
        db_name: Option<&str>,
    ) -> Result<mysql_async::Opts> {
        // Keep the one-off pool's bounds consistent
        // with the pre-built pools (min(4, config)) instead of a hardcoded 4.
        let one_off_max = self.mysql_max_pool_connections.min(4);
        let pool_opts = mysql_async::PoolOpts::default()
            .with_inactive_connection_ttl(Duration::from_secs(300))
            .with_constraints(
                mysql_async::PoolConstraints::new(1, one_off_max)
                    .context("mysql_max_pool_connections must be >= 1")?,
            );
        let mut builder = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&node.host)
            .tcp_port(node.fe_jdbc_port)
            .user(Some(&self.username))
            .pass(Some(&self.password))
            .db_name(db_name)
            .prefer_socket(false)
            .stmt_cache_size(0)
            .pool_opts(pool_opts);

        if self.mysql_tls.enable {
            let mut ssl_opts = mysql_async::SslOpts::default();
            if let Some(ref ca_path) = self.mysql_tls.ca_cert {
                let path_buf = ca_path.clone();
                let root_certs: Vec<_> = vec![path_buf].into_iter().map(|p| p.into()).collect();
                ssl_opts = ssl_opts.with_root_certs(root_certs);
            } else {
                tracing::warn!(
                    "MySQL TLS enabled but no ca_cert configured — \
                     set starrocks.mysql_tls.ca_cert"
                );
            }
            builder = builder.ssl_opts(Some(ssl_opts));
        }

        Ok(builder.into())
    }

    /// Execute SQL on StarRocks with node failover.
    ///
    /// Tries nodes in order (active first). On success, caches the node.
    /// On failure with active node, clears the cache and retries all nodes.
    /// On failure with all nodes, returns the last error.
    pub async fn execute_sql(&self, sql: &str) -> Result<String> {
        self.execute_sql_with_db(sql, Some(&self.database)).await
    }

    /// Execute SQL on StarRocks with node failover, without selecting a
    /// database. Used for bootstrap commands like CREATE DATABASE where
    /// the target database doesn't exist yet.
    pub async fn execute_sql_without_db(&self, sql: &str) -> Result<String> {
        self.execute_sql_with_db(sql, None).await
    }

    /// (Tier 1) optimization: execute pre-split SQL statements on
    /// StarRocks with node failover. Mirrors [`execute_sql`] but skips
    /// the redundant third polyglot parse call — used by the DDL poller
    /// where `post_process_ddl_stmts` already produces `Vec<String>`.
    pub async fn execute_sql_stmts(&self, stmts: Vec<String>) -> Result<String> {
        self.execute_sql_with_db_stmts(stmts, Some(&self.database)).await
    }

    /// Execute SQL on StarRocks with node failover, optionally selecting
    /// a database. Internal helper used by both `execute_sql` and
    /// `execute_sql_without_db`.
    async fn execute_sql_with_db(&self, sql: &str, db_name: Option<&str>) -> Result<String> {
        if self.shutting_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow::anyhow!(
                "SrConnectionManager shutting down — rejecting SQL: {}",
                crate::util::truncate_str(sql.trim(), 120)
            ));
        }
        // (Tier 1): pre-split statements via polyglot_parse_to_stmts once.
        let sql_owned = sql.to_string();
        let stmts: Vec<String> =
            match tokio::task::spawn_blocking(move || polyglot_parse_to_stmts(&sql_owned)).await {
                Ok(stmts) => stmts,
                Err(join_err) => {
                    tracing::error!(
                        "polyglot_sql parse panicked in spawn_blocking: {join_err:?} — passing raw SQL through"
                    );
                    vec![sql.trim().to_string()]
                }
            };
        self.execute_sql_with_db_stmts(stmts, db_name).await
    }

    /// (Tier 1) optimization: like [`execute_sql_with_db`] but accepts
    /// pre-split statements (`Vec<String>`) so the third polyglot parse round-
    /// trip (parse → generate per-expr) is skipped. Used by the DDL path
    /// where `post_process_ddl_stmts` already produces `Vec<String>`.
    ///
    /// HA-failover resume is preserved: each successful statement advances
    /// `applied_count`; on node failover, statements before `applied_count`
    /// are skipped (their effect is replicated across all FE nodes via the
    /// meta-service, so skipping is correct and avoids double-applying non-
    /// idempotent statements).
    async fn execute_sql_with_db_stmts(
        &self,
        stmts: Vec<String>,
        db_name: Option<&str>,
    ) -> Result<String> {
        if self.shutting_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow::anyhow!(
                "SrConnectionManager shutting down — rejecting {} pre-split statements",
                stmts.len()
            ));
        }
        let order = self.node_order();
        let mut last_error = None;
        let mut applied_count: usize = 0;

        for &node_idx in &order {
            let node = &self.nodes[node_idx];

            // Use the pre-built pool for the common case (target database
            // already exists). For without-db bootstrap (CREATE DATABASE
            // before the target DB exists), build a one-off Pool — this
            // path is called only once at startup, so the extra pool
            // allocation is acceptable.
            let mut one_off_pool: Option<mysql_async::Pool>;
            let pool_ref: &mysql_async::Pool = if db_name == Some(self.database.as_str()) {
                one_off_pool = None;
                &self.pools[node_idx]
            } else {
                let opts = match self.build_mysql_opts_with_db(node, db_name) {
                    Ok(opts) => opts,
                    Err(e) => {
                        tracing::warn!(
                            "SR node {}:{} MySQL opts error: {e}",
                            node.host,
                            node.fe_jdbc_port
                        );
                        last_error = Some(e);
                        continue;
                    }
                };
                one_off_pool = Some(mysql_async::Pool::new(opts));
                // unwrap() is infallible — `one_off_pool` was just assigned Some(_).
                // Using .as_ref().unwrap() instead of a let-else keeps the borrow
                // checker happy without an intermediate binding.
                one_off_pool.as_ref().unwrap()
            };

            let conn_result = tokio::time::timeout(self.connection_timeout, pool_ref.get_conn()).await;

            let mut conn = match conn_result {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    let msg = format!(
                        "SR node {}:{} MySQL connect error: {e}",
                        node.host, node.fe_jdbc_port
                    );
                    tracing::warn!("{msg}");
                    last_error = Some(anyhow::anyhow!("{msg}"));
                    self.clear_active_if_match(node_idx);
                    // Disconnect the one-off pool if it was created
                    if let Some(p) = one_off_pool.take() {
                        let _ = tokio::time::timeout(Duration::from_secs(5), p.disconnect()).await;
                    }
                    continue;
                }
                Err(_elapsed) => {
                    let msg = format!(
                        "SR node {}:{} MySQL connect timeout after {:?}",
                        node.host, node.fe_jdbc_port, self.connection_timeout
                    );
                    tracing::warn!("{msg}");
                    last_error = Some(anyhow::anyhow!("{msg}"));
                    self.clear_active_if_match(node_idx);
                    // Disconnect the one-off pool if it was created
                    if let Some(p) = one_off_pool.take() {
                        let _ = tokio::time::timeout(Duration::from_secs(5), p.disconnect()).await;
                    }
                    continue;
                }
            };

            // Parse to detect multi-statement SQL (same logic as sr_sql::execute_sql).
            // (this was previously parsed per-node inside this loop; now
            // parsed once above so applied_count indices are stable across nodes.)
            let mut exec_ok = true;
            for (i, raw_stmt) in stmts.iter().enumerate() {
                // Skip statements already applied on a prior node.
                if i < applied_count {
                    continue;
                }
                let stmt = raw_stmt.trim();
                if stmt.is_empty() {
                    continue;
                }
                match tokio::time::timeout(self.query_timeout, conn.query_drop(stmt)).await {
                    Ok(Ok(_)) => {
                        // Statement applied successfully on this node — record
                        // progress so a subsequent node failover resumes here.
                        applied_count = i + 1;
                        // Log the executed SR SQL at DEBUG so the success path
                        // is observable (the backtick-quoted form is what tooling matches
                        // on; previously only the error
                        // paths logged the stmt, leaving successful DDLs
                        // invisible in logs).
                        tracing::debug!(
                            "SR SQL ok on {}:{} — stmt: {}",
                            node.host, node.fe_jdbc_port,
                            crate::util::truncate_str(stmt, 200),
                        );
                    }
                    Ok(Err(e)) => {
                        let msg = e.to_string().to_lowercase();
                        let stmt_upper = stmt.to_uppercase();
                        let is_idempotent = stmt_upper.starts_with("CREATE TABLE")
                            || stmt_upper.starts_with("CREATE DATABASE")
                            || (stmt_upper.starts_with("ALTER TABLE")
                                && stmt_upper.contains("ADD COLUMN")
                                && !stmt_upper.contains("DROP COLUMN"));
                        if is_idempotent
                            && (msg.contains("already exists") || msg.contains("duplicate"))
                        {
                            tracing::debug!(
                                "SR SQL: ignoring: {}",
                                crate::util::truncate_str(&msg, 120)
                            );
                        } else if msg.contains("unknown database")
                            && stmt.to_lowercase().contains("use ")
                        {
                            tracing::debug!(
                                "SR SQL: ignoring USE: {}",
                                crate::util::truncate_str(&msg, 120)
                            );
                        } else if is_snapshot_backup_rename_not_found(&stmt_upper, &msg) {
                            tracing::debug!(
                                "SR SQL: ignoring rename of non-existent table (snapshot backup): {}",
                                crate::util::truncate_str(&msg, 120)
                            );
                        } else {
                            // "schema change in progress" is transient — SR
                            // is finishing a prior ALTER TABLE. The DDL poller
                            // handles this with retry+sleep. Log at INFO to
                            // avoid WARN noise during routine schema changes.
                            if msg.contains("schema change") {
                                tracing::info!(
                                    "SR SQL transient (schema change in progress) on node {}:{} — stmt: {}",
                                    node.host,
                                    node.fe_jdbc_port,
                                    crate::util::truncate_str(stmt, 200)
                                );
                            } else {
                                tracing::warn!(
                                    "SR SQL failed on node {}:{} — {} — stmt: {}",
                                    node.host,
                                    node.fe_jdbc_port,
                                    e,
                                    crate::util::truncate_str(stmt, 200)
                                );
                            }
                            exec_ok = false;
                            self.clear_active_if_match(node_idx);
                            last_error = Some(anyhow::anyhow!(
                                "SR SQL failed on {}:{}: {}",
                                node.host,
                                node.fe_jdbc_port,
                                e
                            ));
                            break;
                        }
                    }
                    Err(_elapsed) => {
                        // Query timed out — treat as a node failure and fall through.
                        tracing::warn!(
                            "SR SQL query timed out after {:?} on node {}:{} — stmt: {}",
                            self.query_timeout,
                            node.host,
                            node.fe_jdbc_port,
                            crate::util::truncate_str(stmt, 200),
                        );
                        exec_ok = false;
                        self.clear_active_if_match(node_idx);
                        last_error = Some(anyhow::anyhow!(
                            "SR SQL query timed out after {:?} on {}:{}",
                            self.query_timeout,
                            node.host,
                            node.fe_jdbc_port
                        ));
                        break;
                    }
                }
            }

            // Close connection (returns to pool). Pool stays alive for reuse.
            drop(conn);

            // If this was a one-off bootstrap pool, disconnect it cleanly to
            // free background tasks. Pre-built pools are disconnected on shutdown().
            if let Some(p) = one_off_pool {
                let _ = tokio::time::timeout(Duration::from_secs(5), p.disconnect()).await;
            }

            if exec_ok {
                // Success! Cache this node and return.
                self.set_active(node_idx);
                return Ok("OK".to_string());
            }
        }

        // All nodes failed — clear active cache
        self.clear_active();
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No SR nodes available")))
    }

    /// Execute a query that returns rows (e.g., SHOW TABLES).
    /// Returns the first column of each row as a String.
    /// Used by snapshot gap detection to check SR-side table existence.
    pub async fn query_first_col(&self, sql: &str) -> Result<Vec<String>> {
        self.query_col(sql, 0).await
    }

    /// Execute a query that returns rows, returning the value of the column
    /// at `col_idx` from each row. Used by `query_first_col` (col 0), and by
    /// SHOW LOAD label polling (col 2 is the State column in SR's SHOW LOAD
    /// result set).
    /// Execute a query that returns rows, with node failover. Returns raw rows
    /// so callers can resolve columns by name rather than a fragile index.
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<mysql_async::Row>> {
        use mysql_async::prelude::Queryable;
        if self.shutting_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(anyhow::anyhow!(
                "SrConnectionManager shutting down — rejecting query: {}",
                crate::util::truncate_str(sql, 120)
            ));
        }
        let order = self.node_order();
        let mut last_error = None;
        for &node_idx in &order {
            let node = &self.nodes[node_idx];
            let pool_ref = &self.pools[node_idx];
            let conn_result =
                tokio::time::timeout(self.connection_timeout, pool_ref.get_conn()).await;
            let mut conn = match conn_result {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    last_error = Some(anyhow::anyhow!(
                        "SR node {}:{} MySQL connect error: {e}",
                        node.host,
                        node.fe_jdbc_port
                    ));
                    self.clear_active_if_match(node_idx);
                    continue;
                }
                Err(_) => {
                    last_error = Some(anyhow::anyhow!(
                        "SR node {}:{} MySQL connect timeout",
                        node.host,
                        node.fe_jdbc_port
                    ));
                    self.clear_active_if_match(node_idx);
                    continue;
                }
            };
            let result: mysql_async::Result<Vec<mysql_async::Row>> =
                match tokio::time::timeout(self.query_timeout, conn.query(sql)).await {
                    Ok(r) => r,
                    Err(_elapsed) => {
                        last_error = Some(anyhow::anyhow!(
                            "SR query timed out after {:?} on {}:{}",
                            self.query_timeout,
                            node.host,
                            node.fe_jdbc_port
                        ));
                        self.clear_active_if_match(node_idx);
                        continue;
                    }
                };
            drop(conn);
            match result {
                Ok(rows) => {
                    self.set_active(node_idx);
                    return Ok(rows);
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!(
                        "SR query failed on {}:{}: {e}",
                        node.host,
                        node.fe_jdbc_port
                    ));
                    self.clear_active_if_match(node_idx);
                    continue;
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| anyhow::anyhow!("No SR nodes available for query")))
    }

    /// Execute a query that returns rows, returning the value of the column
    /// at `col_idx` from each row. Used by `query_first_col` (col 0), and by
    /// callers that don't need name-based column resolution.
    pub async fn query_col(&self, sql: &str, col_idx: usize) -> Result<Vec<String>> {
        let rows = self.query_rows(sql).await?;
        let mut skipped = 0usize;
        let values: Vec<String> = rows
            .iter()
            .filter_map(|r| match r.get::<String, _>(col_idx) {
                Some(v) => Some(v),
                None => {
                    skipped += 1;
                    None
                }
            })
            .collect();
        if skipped > 0 {
            // Never drop rows silently: a NULL or non-String value here would
            // otherwise surface as a short list — e.g. `sr_table_exists`
            // reporting the table as missing and failing the swap with a
            // misleading diagnosis.
            tracing::warn!(
                "query_col: skipped {skipped} row(s) whose column {col_idx} \
                 is not a String — SQL: {sql}"
            );
        }
        Ok(values)
    }

    /// Poll `SHOW LOAD WHERE Label = 'xxx'` and return the State column
/// value (`FINISHED`, `FAILED`, `LOADING`, etc.) for the given label.
/// Returns `None` if no row exists in the SHOW LOAD result (label GC'd
/// by SR or never registered). Single quotes in `label` are SQL-escaped
/// by doubling (`'` → `''`) to prevent SQL injection.
    pub async fn query_label_status(&self, label: &str) -> Result<Option<String>> {
        let safe_label = label.replace('\'', "''");
        let sql = format!("SHOW LOAD WHERE `Label` = '{}'", safe_label);
        let rows = self.query_rows(&sql).await?;
        let row = match rows.into_iter().next() {
            Some(r) => r,
            None => return Ok(None),
        };
        // Resolve the State column by NAME instead of a
        // hardcoded index (2), so a future SR version reordering SHOW LOAD
        // columns doesn't silently read the wrong value.
        let col_names: Vec<String> = row
            .columns()
            .iter()
            .map(|c| c.name_str().to_string())
            .collect();
        let idx = find_col_index(&col_names, "State")?;
        Ok(row.get::<String, _>(idx))
    }


    /// Build the Stream Load URL for a given node and table.
    pub fn stream_load_url(
        &self,
        database: &str,
        table: &str,
        use_tls: bool,
        node_idx: usize,
    ) -> String {
        let node = match self.nodes.get(node_idx) {
            Some(n) => n,
            None => {
                tracing::error!(
                    "stream_load_url: node_idx={} out of bounds (nodes.len={}) — using node 0",
                    node_idx,
                    self.nodes.len(),
                );
                &self.nodes[0]
            }
        };
        let scheme = if use_tls { "https" } else { "http" };
        // Bracket IPv6 host literals so the URL is
        // well-formed (e.g. `http://[::1]:8030/...` instead of `http://::1:8030`).
        let host = bracket_ipv6_host(&node.host);
        // URL-encode db/table names to handle special characters
        let encoded_db = urlencode(database);
        let encoded_tbl = urlencode(table);
        format!(
            "{}://{}:{}/api/{}/{}/_stream_load",
            scheme, host, node.fe_http_port, encoded_db, encoded_tbl,
        )
    }

    /// Get the active node's Stream Load URL, or the first node if none active.
    /// Returns (url, node_idx).
    #[allow(dead_code)]
    pub fn stream_load_url_active(
        &self,
        database: &str,
        table: &str,
        use_tls: bool,
    ) -> (String, usize) {
        let idx = self.active_node_index().unwrap_or(0);
        (self.stream_load_url(database, table, use_tls, idx), idx)
    }

    /// Access to node configs for external HTTP clients.
    pub fn get_node(&self, idx: usize) -> Option<&SrNodeConfig> {
        self.nodes.get(idx)
    }

    /// Reference to the full node list.
    #[allow(dead_code)]
    pub fn nodes(&self) -> &[SrNodeConfig] {
        &self.nodes
    }

    /// The configured database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The configured username for basic auth.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The configured password for basic auth.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Whether HTTP TLS is enabled.
    pub fn http_tls_enabled(&self) -> bool {
        self.http_tls.enable
    }

    /// Optional custom CA certificate path for HTTP TLS connections.
    pub fn http_tls_ca_cert(&self) -> Option<&std::path::PathBuf> {
        self.http_tls.ca_cert.as_ref()
    }

    /// Initial TCP/auth connection timeout for both HTTP and MySQL.
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// Per-attempt HTTP Stream Load timeout (FE PUT + 307 BE chain).
    pub fn http_timeout(&self) -> Duration {
        self.http_timeout
    }

    /// Per-query MySQL execution timeout (DDL, catalog, SHOW LOAD polling).
    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Max retry attempts per node after the initial attempt fails.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// HTTP connection pool idle timeout.
    pub fn http_pool_idle_timeout(&self) -> Duration {
        self.http_pool_idle_timeout
    }

    /// Maximum MySQL connections per FE node in the pool.
    #[allow(dead_code)]
    pub fn mysql_max_pool_connections(&self) -> usize {
        self.mysql_max_pool_connections
    }

    /// Gracefully shut down all MySQL connection pools.
    /// Called during pipeline shutdown to release pool resources.
    ///
    /// Sets a fail-fast flag so concurrent operations
    /// abort promptly instead of racing pool disconnection. Callers MUST quiesce
    /// all in-flight operations (CDC loop, flush, checkpoint saver) before
    /// calling this — main.rs honors that contract.
    pub async fn shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let futs: Vec<_> = self.pools.iter().map(|pool| {
            tokio::time::timeout(Duration::from_secs(5), pool.clone().disconnect())
        }).collect();
        futures::future::join_all(futs).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SrConfig;

    // ── Pure-logic tests for `is_snapshot_backup_rename_not_found` ─────────
    // These don't need a tokio runtime or a real StarRocks connection — they
    // just verify the SQL/error pattern classification that drives the
    // benign-case filter in `execute_sql`.

    #[test]
    fn test_backup_rename_not_found_matches_prod_log() {
        // Verbatim from prod log (uppercased). 1782081437313 = 13 digits.
        let stmt = "ALTER TABLE `EXAMPLE_DB`.`ADDRESSES` RENAME `ADDRESSES_DROP_1782081437313_2`";
        let msg = "table addresses is not found.";
        assert!(is_snapshot_backup_rename_not_found(stmt, msg));
    }

    #[test]
    fn test_backup_rename_not_found_doesnt_match_shadow_to_main() {
        // Step 2 of swap: shadow -> main. This is the actual swap; it must
        // propagate as a real error if it fails (would indicate a real bug).
        let stmt = "ALTER TABLE `EXAMPLE_DB`.`ADDRESSES_SNAPSHOT_TEMP` RENAME `ADDRESSES`";
        let msg = "table addresses_snapshot_temp is not found.";
        assert!(
            !is_snapshot_backup_rename_not_found(stmt, msg),
            "shadow->main rename must NOT be masked (no _DROP_ target)"
        );
    }

    #[test]
    fn test_backup_rename_not_found_rejects_rename_column() {
        // RENAME COLUMN path must never be silenced (DDL poller depends on
        // real errors propagating for column ops).
        let stmt = "ALTER TABLE `EXAMPLE_DB`.`USERS` RENAME COLUMN `OLD` TO `NEW`";
        let msg = "column old does not exist";
        assert!(
            !is_snapshot_backup_rename_not_found(stmt, msg),
            "RENAME COLUMN must NOT be masked"
        );
    }

    #[test]
    fn test_backup_rename_not_found_rejects_short_timestamp() {
        // Defensive: a 5-digit suffix is not a millisecond timestamp. Refuse
        // to mask — could be a legitimately-named table accidentally matching
        // the prefix pattern.
        let stmt = "ALTER TABLE `DB`.`FOO` RENAME `FOO_DROP_12345_1`";
        let msg = "table foo is not found.";
        assert!(
            !is_snapshot_backup_rename_not_found(stmt, msg),
            "Short suffix (<13 digits) must NOT match (avoids false positives on legitimate _drop_ names)"
        );
    }

    #[test]
    fn test_backup_rename_not_found_rejects_unrelated_error() {
        // Same SQL shape, but a different error message (e.g. syntax error).
        // Must NOT be masked — it's a real failure.
        let stmt = "ALTER TABLE `DB`.`ADDR` RENAME `ADDR_DROP_1782081437313_2`";
        let msg = "getting syntax error. line 1, column 42";
        assert!(
            !is_snapshot_backup_rename_not_found(stmt, msg),
            "Non-not-found errors must NOT be masked"
        );
    }

    fn make_test_config(urls: &str) -> SrConfig {
        SrConfig {
            node_urls: urls.to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_single_host() {
        let config = make_test_config("sr-node1:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 1);
        assert!(manager.active_node_index().is_none());
    }

    #[test]
    fn test_bare_host_uses_default_ports() {
        let config = make_test_config("my-fe");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 1);
        assert_eq!(manager.nodes[0].host, "my-fe");
        assert_eq!(manager.nodes[0].fe_http_port, 8030);
        assert_eq!(manager.nodes[0].fe_jdbc_port, 9030);
    }

    #[test]
    fn test_mixed_formats() {
        // bare host, host:http, and host:http:jdbc all in one string
        let config = make_test_config("fe1;fe2:8031;fe3:8032:9032");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 3);
        // fe1: defaults
        assert_eq!(manager.nodes[0].host, "fe1");
        assert_eq!(manager.nodes[0].fe_http_port, 8030);
        assert_eq!(manager.nodes[0].fe_jdbc_port, 9030);
        // fe2: http=8031, jdbc=default
        assert_eq!(manager.nodes[1].host, "fe2");
        assert_eq!(manager.nodes[1].fe_http_port, 8031);
        assert_eq!(manager.nodes[1].fe_jdbc_port, 9030);
        // fe3: both explicit
        assert_eq!(manager.nodes[2].host, "fe3");
        assert_eq!(manager.nodes[2].fe_http_port, 8032);
        assert_eq!(manager.nodes[2].fe_jdbc_port, 9032);
    }

    #[test]
    fn test_multi_host_semicolon() {
        let config = make_test_config("sr-node1:8030;sr-node2:8030;sr-node3:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 3);
        assert!(manager.active_node_index().is_none());
    }

    #[test]
    fn test_host_with_port_override() {
        let config = make_test_config("node1:8031:9031;node2:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 2);
        assert_eq!(manager.nodes[0].host, "node1");
        assert_eq!(manager.nodes[0].fe_http_port, 8031);
        assert_eq!(manager.nodes[0].fe_jdbc_port, 9031);
        assert_eq!(manager.nodes[1].host, "node2");
        assert_eq!(manager.nodes[1].fe_http_port, 8030); // default
        assert_eq!(manager.nodes[1].fe_jdbc_port, 9030); // default
    }

    #[test]
    fn test_host_with_only_http_override() {
        let config = make_test_config("node1:8040");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(manager.node_count(), 1);
        assert_eq!(manager.nodes[0].host, "node1");
        assert_eq!(manager.nodes[0].fe_http_port, 8040);
        assert_eq!(manager.nodes[0].fe_jdbc_port, 9030); // default jdbc, not http+1000
    }

    #[test]
    fn test_empty_host_fallback() {
        // Empty node_urls should produce an error, not a silent fallback
        let config = make_test_config("");
        assert!(SrConnectionManager::new(&config, 2, 10000, 10000).is_err());
    }

    #[test]
    fn test_set_and_clear_active() {
        let config = make_test_config("node1:8030;node2:8030;node3:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        assert!(manager.active_node_index().is_none());
        manager.set_active(1);
        assert_eq!(manager.active_node_index(), Some(1));
        assert_eq!(manager.active_host(), Some("node2".to_string()));

        manager.clear_active();
        assert!(manager.active_node_index().is_none());
    }

    #[test]
    fn test_node_order_with_active() {
        let config = make_test_config("n1:8030;n2:8030;n3:8030;n4:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // No active → returns [0, 1, 2, 3]
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1, 2, 3]);

        // Active = 2 → returns [2, 0, 1, 3]
        manager.set_active(2);
        let order = manager.node_order();
        assert_eq!(order, vec![2, 0, 1, 3]);
    }

    #[test]
    fn test_clear_active_if_match() {
        let config = make_test_config("n1:8030;n2:8030;n3:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        manager.set_active(1);
        assert_eq!(manager.active_node_index(), Some(1));

        // Clear with non-matching index — no change
        manager.clear_active_if_match(0);
        assert_eq!(manager.active_node_index(), Some(1));

        // Clear with matching index — cleared
        manager.clear_active_if_match(1);
        assert!(manager.active_node_index().is_none());
    }

    #[test]
    fn test_stream_load_url() {
        let config = make_test_config("my-fe:8030:9030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        let url = manager.stream_load_url("mydb", "mytbl", false, 0);
        assert_eq!(url, "http://my-fe:8030/api/mydb/mytbl/_stream_load");

        let url_tls = manager.stream_load_url("mydb", "mytbl", true, 0);
        assert_eq!(url_tls, "https://my-fe:8030/api/mydb/mytbl/_stream_load");
    }

    #[test]
    fn test_stream_load_url_oob_fallback() {
        let config = make_test_config("fe1:8030:9030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Single node (index 0) — requesting index 1 should fall back to node 0
        let url = manager.stream_load_url("db", "t", false, 1);
        assert_eq!(url, "http://fe1:8030/api/db/t/_stream_load");

        // Index 5 should also fall back
        let url = manager.stream_load_url("db", "t", false, 5);
        assert_eq!(url, "http://fe1:8030/api/db/t/_stream_load");
    }

    #[test]
    fn test_stream_load_url_active() {
        let config = make_test_config("n1:8031:9031;n2:8031:9031");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // No active → uses node 0
        let (url, idx) = manager.stream_load_url_active("db", "t", false);
        assert_eq!(idx, 0);
        assert_eq!(url, "http://n1:8031/api/db/t/_stream_load");

        // Set active = 1 → uses node 1
        manager.set_active(1);
        let (url, idx) = manager.stream_load_url_active("db", "t", false);
        assert_eq!(idx, 1);
        assert_eq!(url, "http://n2:8031/api/db/t/_stream_load");
    }

    #[test]
    fn test_node_order_handles_out_of_bounds_active() {
        let config = make_test_config("n1:8030;n2:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Set active to an out-of-bounds index (shouldn't happen in practice,
        // but the manager should handle it gracefully).
        *manager.active_node.lock().unwrap() = Some(999);
        // node_order should ignore invalid active index → all nodes in order
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1]);
    }

    #[test]
    fn test_failover_node_order() {
        // Simulate the failover behavior: start with no active node,
        // then set node 1 as active after it succeeds, then verify
        // node_order puts node 1 first.
        let config = make_test_config("invalid:9999;good:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Initially no active node → try all in order
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1], "no active → original order");

        // Mark node 1 as the one that succeeded (failover)
        manager.set_active(1);
        assert_eq!(manager.active_node_index(), Some(1));
        assert_eq!(manager.active_host(), Some("good".to_string()));

        // Node order should now put node 1 first
        let order = manager.node_order();
        assert_eq!(order, vec![1, 0], "active node 1 → [1, 0]");

        // Mark node 1 as failing → clear active
        manager.clear_active_if_match(1);
        assert!(manager.active_node_index().is_none());

        // Back to original order
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1], "after clear → original order");

        // Set active to node 0 → order [0, 1]
        manager.set_active(0);
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1], "active node 0 → [0, 1]");

        // Clear all → no active → original order
        manager.clear_active();
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1], "clear all → original order");
    }

    // ── URL encoding ────────────────────────────────────────────────────

    #[test]
    fn test_urlencode_plain_text() {
        assert_eq!(urlencode("hello"), "hello");
    }

    #[test]
    fn test_urlencode_spaces() {
        assert_eq!(urlencode("my table"), "my%20table");
    }

    #[test]
    fn test_urlencode_special_chars() {
        assert_eq!(urlencode("test_db"), "test_db");
        assert_eq!(urlencode("a/b"), "a%2Fb");
    }

    #[test]
    fn test_urlencode_unicode() {
        assert_eq!(urlencode("schema.表"), "schema.%E8%A1%A8");
    }

    #[test]
    fn test_stream_load_url_encodes_db_and_table() {
        let config = make_test_config("n1:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        let url = manager.stream_load_url("my db", "my table", false, 0);
        assert!(
            url.contains("/api/my%20db/my%20table/_stream_load"),
            "URL should encode db/table names: {url}"
        );
    }

    // ── IPv6 host bracketing in stream_load_url ──

    #[test]
    fn test_stream_load_url_ipv6_host_bracketed() {
        // An IPv6 literal like ::1 must be bracketed in the URL:
        // http://[::1]:8030/... not http://::1:8030/... (malformed).
        // Tested via the bracket_ipv6_host helper (SrConfig.parse_hosts can't
        // parse colon-delimited IPv6 in node_urls, so we test the logic directly).
        assert_eq!(bracket_ipv6_host("::1"), "[::1]");
        assert_eq!(bracket_ipv6_host("fd00::1"), "[fd00::1]");
        assert_eq!(bracket_ipv6_host("fe80::1234:5678"), "[fe80::1234:5678]");
    }

    #[test]
    fn test_stream_load_url_ipv4_host_not_bracketed() {
        // IPv4 and hostnames should not be bracketed.
        assert_eq!(bracket_ipv6_host("192.0.2.1"), "192.0.2.1");
        assert_eq!(bracket_ipv6_host("my-fe.svc.local"), "my-fe.svc.local");
        assert_eq!(bracket_ipv6_host("fe1"), "fe1");
    }

    // ── Percent-encoding (space → %20) in path segments ──

    #[test]
    fn test_stream_load_url_percent_encodes_space_not_plus() {
        // form_urlencoded encoded space as '+', which in a URL path
        // segment is a literal plus. percent_encoding encodes space as %20.
        let config = make_test_config("n1:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        let url = manager.stream_load_url("my db", "order items", false, 0);
        assert!(
            url.contains("my%20db"),
            "space in db name must be %20: {url}"
        );
        assert!(
            url.contains("order%20items"),
            "space in table name must be %20: {url}"
        );
        assert!(
            !url.contains("+"),
            "URL must not contain '+' (old form_urlencoded behavior): {url}"
        );
    }

    // ── SHOW LOAD State column resolved by name ──

    #[test]
    fn test_find_col_index_resolves_by_name() {
        // The State column should be found by NAME, not by a hardcoded
        // index (2). Simulate SR's standard column ordering: JobId, Label, State.
        let cols = ["JobId", "Label", "State"];
        let idx = find_col_index(&cols, "State").unwrap();
        assert_eq!(
            idx, 2,
            "State should be found at index 2 in standard SR SHOW LOAD output"
        );
    }

    #[test]
    fn test_find_col_index_handles_reordered_columns() {
        // If a future SR version reorders columns, we should still find
        // State by name — not read the wrong column at the old index 2.
        let cols = ["State", "Label", "JobId"];
        let idx = find_col_index(&cols, "State").unwrap();
        assert_eq!(
            idx, 0,
            "State should be found at index 0 when reordered, not hardcoded 2"
        );
    }

    #[test]
    fn test_find_col_index_case_insensitive() {
        let cols = ["jobid", "label", "state"];
        assert!(
            find_col_index(&cols, "State").is_ok(),
            "lookup must be case-insensitive (SR may lowercase column names)"
        );
        assert!(
            find_col_index(&cols, "STATE").is_ok(),
            "lookup must be case-insensitive"
        );
    }

    #[test]
    fn test_find_col_index_missing_returns_error() {
        let cols = ["JobId", "Label", "Progress"];
        let result = find_col_index(&cols, "State");
        assert!(
            result.is_err(),
            "missing State column must return Err, not read wrong column"
        );
    }

    #[test]
    fn test_mysql_pool_per_node() {
        let config = make_test_config("fe1;fe2:8031;fe3:8032:9032");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        assert_eq!(
            manager.pools.len(),
            3,
            "Should have one Pool per node, got {} pools for {} nodes",
            manager.pools.len(),
            manager.nodes.len(),
        );
    }

    #[test]
    fn test_build_mysql_opts_inactive_connection_ttl() {
        let config = make_test_config("n1:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();
        let node = &manager.nodes[0];
        let opts = manager
            .build_mysql_opts_with_db(node, Some("testdb"))
            .expect("build_mysql_opts_with_db should succeed");
        let pool_opts = opts.pool_opts();
        assert!(
            pool_opts.inactive_connection_ttl() > Duration::ZERO,
            "inactive_connection_ttl should be set to non-zero: {:?}",
            pool_opts.inactive_connection_ttl(),
        );
        assert_eq!(
            pool_opts.inactive_connection_ttl(),
            Duration::from_secs(300),
            "inactive_connection_ttl should be 300s",
        );
    }

    // ── Regression tests: circuit breaker ────────────

    #[test]
    fn test_circuit_breaker_pushes_failed_node_to_back() {
        // When a node fails, clear_active_if_match records the failure
        // timestamp. node_order() should push failed nodes to the back
        // within the cooldown period, so healthy nodes are tried first.
        let config = make_test_config("fe1:8030;fe2:8030;fe3:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Initially all nodes are in config order: [0, 1, 2]
        let order = manager.node_order();
        assert_eq!(order, vec![0, 1, 2],
            "Initial node order should be config order");

        // Mark node 0 as failed (simulates clear_active_if_match)
        manager.clear_active_if_match(0);

        // Now node 0 should be pushed to back (it's in cooldown)
        let order_after = manager.node_order();
        assert_eq!(order_after[0], 1,
            "Healthy node 1 should be first after node 0 failure");
        assert_eq!(order_after[2], 0,
            "Failed node 0 should be last (circuit breaker cooldown)");
    }

    #[test]
    fn test_circuit_breaker_clears_on_success() {
        // set_active clears the failure timestamp — a recovered node
        // is no longer deprioritized.
        let config = make_test_config("fe1:8030;fe2:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Fail node 0
        manager.clear_active_if_match(0);
        let order = manager.node_order();
        assert_eq!(order[0], 1, "Node 1 should be first after node 0 fails");

        // Node 0 recovers (set_active clears failure)
        manager.set_active(0);
        let order_after = manager.node_order();
        assert_eq!(order_after[0], 0,
            "Recovered node 0 should be first (active + no failure)");
    }

    #[test]
    fn test_circuit_breaker_multiple_failures() {
        let config = make_test_config("fe1:8030;fe2:8030;fe3:8030");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Fail nodes 0 and 2
        manager.clear_active_if_match(0);
        manager.clear_active_if_match(2);

        let order = manager.node_order();
        // Only node 1 is healthy → first; then 0 and 2 (ailing, in config order)
        assert_eq!(order[0], 1, "Only healthy node 1 should be first");
        // Failed nodes last (order within the failed set depends on
        // iteration order — both 0 and 2 should appear after 1)
        assert!(order.iter().position(|&x| x == 0).unwrap() > 0, "Node 0 should be after healthy nodes");
        assert!(order.iter().position(|&x| x == 2).unwrap() > 0, "Node 2 should be after healthy nodes");
    }

    #[test]
    fn test_execute_sql_records_failure_on_connect_error() {
        // Point to a non-existent port — connect will fail immediately.
        let config = make_test_config("localhost:19999:29999");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        // Attempt SQL — should fail with a connect error
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(manager.execute_sql("SELECT 1"));

        // Verify failure timestamp was recorded — this is what the circuit
        // breaker uses to deprioritize the node for 30s on subsequent calls.
        // Before the fix, execute_sql_with_db skipped clear_active_if_match
        // on per-node failures, so the dead node stayed at the front of
        // node_order() and every subsequent query paid the full
        // connect_timeout before failing over.
        let failures = manager.last_failure_at.lock().unwrap();
        assert!(
            failures[0].is_some(),
            "Failure should be recorded for connect error (circuit breaker cooldown)"
        );
    }

    #[test]
    fn test_query_first_col_records_failure_on_connect_error() {
        let config = make_test_config("localhost:19999:29999");
        let manager = SrConnectionManager::new(&config, 2, 10000, 10000).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(manager.query_first_col("SHOW TABLES"));

        let failures = manager.last_failure_at.lock().unwrap();
        assert!(
            failures[0].is_some(),
            "Failure should be recorded for query connect error"
        );
    }
}
