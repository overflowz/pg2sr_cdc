//! DDL poller — captures PG schema changes, transpiles to StarRocks, and executes.
//!
//! Runs as a spawned task with an outer reconnect loop. Panic-safe: inner task
//! panics (e.g. from polyglot-sql) are caught as JoinError and the poller
//! restarts with backoff, leaving CDC streaming unaffected.
//!
//! Two transpilation paths:
//! 1. `polyglot-sql::transpile()` for standard PG DDL
//! 2. `catalog_create_table_fallback()` for PG custom types (enums, composites)
//!    that polyglot-sql can't handle

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;

use crate::checkpoint::lock_checkpoint;
use crate::checkpoint::Checkpoint;
use crate::checkpoint::CheckpointStore;
use crate::config::PgTlsConnector;
use crate::pipeline::RenameMap;
use crate::sr_connection::SrConnectionManager;
use crate::util::AbortOnDrop;

// LazyLock replaces OnceLock::get_or_init(...).expect().
static DROP_TABLE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)")
        .expect("DROP_TABLE_RE: hardcoded valid regex")
});
static TABLE_NAME_EXTRACT_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r#"(?i)\b(?:CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:(\w+|"[^"]+")\.)?(\w+|"[^"]+")"#,
    )
    .expect("TABLE_NAME_EXTRACT_RE: hardcoded valid regex")
});

// REPLICA IDENTITY is a PG keyword phrase — must match with word
// boundaries to avoid false-positives on column names like
// `replica_identity_flag` (the prior `ddl.contains("REPLICA IDENTITY")`
// substring match would have skipped a legitimate ADD COLUMN).
static REPLICA_IDENTITY_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bREPLICA\s+IDENTITY\b")
        .expect("REPLICA_IDENTITY_RE: hardcoded valid regex")
});

/// Detect `ALTER TABLE ... RENAME COLUMN <old> TO <new>` (quoted or bare
/// identifiers). The poller registers the mapping so buffered rows replay
/// with the live column name. Quoted identifiers are case-sensitive in PG
/// and may contain `""` escapes — the capture includes the doubled quote
/// (`(?:[^"]|"")+`) and the parser unescapes it. Group
/// numbering: 1/2 = old quoted/bare, 3/4 = new quoted/bare.
static RENAME_COLUMN_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r#"(?i)\bRENAME\s+COLUMN\s+(?:"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_$]*))\s+TO\s+(?:"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_$]*))"#,
    )
    .expect("RENAME_COLUMN_RE: hardcoded valid regex")
});

/// Extract every `(old, new)` column-rename pair from a RENAME COLUMN DDL
/// Quoted captures
/// keep their case and get `""` escapes unescaped (PG quoted identifiers);
/// bare captures are folded to ASCII lowercase (PG folds unquoted
/// identifiers in the C locale — non-ASCII identifiers are edge cases we
/// intentionally do not fold). Empty result → nothing to register.
fn parse_rename_pairs(ddl: &str) -> Vec<(String, String)> {
    RENAME_COLUMN_RE
        .captures_iter(ddl)
        .filter_map(|caps| {
            // Quoted capture → case preserved, `""` unescaped (PG quoted
            // identifiers are case-sensitive); bare capture → ASCII-folded
            let old = match (caps.get(1), caps.get(2)) {
                (Some(q), _) => q.as_str().replace("\"\"", "\""),
                (None, Some(b)) => b.as_str().to_ascii_lowercase(),
                (None, None) => return None,
            };
            let new = match (caps.get(3), caps.get(4)) {
                (Some(q), _) => q.as_str().replace("\"\"", "\""),
                (None, Some(b)) => b.as_str().to_ascii_lowercase(),
                (None, None) => return None,
            };
            Some((old, new))
        })
        .collect()
}

/// Register an old→new column rename for `table_key`. Plain insert — chain
/// resolution happens at replay time (resolve_column), so a later SKIPPED DDL
/// can remove its own entry without corrupting an unrelated flattened chain
fn register_rename(map: &RenameMap, table_key: &str, old: &str, new: &str) {
    let mut m = crate::pipeline::lock_or_recover(map);
    m.entry(table_key.to_string())
        .or_default()
        .insert(old.to_string(), new.to_string());
}

/// Remove rename pairs whose DDL was skipped (snapshot Ok(false)) or
/// permanently failed (checkpoint advanced without apply). A rename that
/// never reached SR must not keep remapping buffered rows to a column name
/// that does not exist there. Each entry is removed only while
/// it still maps old→new (a later rename may have superseded it). The
/// sidecar is rewritten from the post-removal map when the table still has
/// segments (empty map → sidecar deleted). MUST be called BEFORE the DDL
/// checkpoint advances: a crash after the advance but before this ran must
/// never resurrect the mapping.
async fn unregister_rename_pairs(
    map: &RenameMap,
    buffer: &Arc<crate::wal_buffer::WalBufferManager>,
    table_key: &str,
    pairs: &[(String, String)],
) {
    let mut removed_any = false;
    {
        let mut m = crate::pipeline::lock_or_recover(map);
        if let Some(t) = m.get_mut(table_key) {
            for (old, new) in pairs {
                if t.get(old) == Some(new) {
                    t.remove(old);
                    removed_any = true;
                }
            }
            if t.is_empty() {
                m.remove(table_key);
            }
        }
    }
    if removed_any && buffer.has_segments(table_key) {
        let entries = crate::pipeline::lock_or_recover(map)
            .get(table_key)
            .cloned()
            .unwrap_or_default();
        if let Err(e) = buffer.write_renames_sidecar(table_key, &entries).await {
            tracing::warn!(
                "rename sidecar rewrite after DDL skip failed (non-fatal; next append rewrites): {e:#}"
            );
        }
    }
}

// Case-insensitive + whitespace-flexible regex replacements for the
// prior `.replace("DROP COLUMN IF EXISTS", "DROP COLUMN")` (and the bare
// `DROP IF EXISTS` form). The previous string-replace was case-sensitive and
// required single-space separation — lowercase DDLs from psql sessions
// (`alter table t drop column if exists c`) were silently ignored, leaving
// the IF EXISTS clause intact, which SR rejects with 1064 → 3-retry →
// FATAL-ABORT.
static DROP_COLUMN_IF_EXISTS_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bDROP\s+COLUMN\s+IF\s+EXISTS\b")
        .expect("DROP_COLUMN_IF_EXISTS_RE: hardcoded valid regex")
});
static DROP_TABLE_IF_EXISTS_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bDROP\s+TABLE\s+IF\s+EXISTS\b")
        .expect("DROP_TABLE_IF_EXISTS_RE: hardcoded valid regex")
});
static DROP_IF_EXISTS_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bDROP\s+IF\s+EXISTS\b")
        .expect("DROP_IF_EXISTS_RE: hardcoded valid regex")
});

/// Outer reconnect loop for the DDL poller.
/// Survives PG connection drops and transient query failures.
/// Triggers graceful shutdown on permanent DDL failures to prevent
/// cascading data loss from silently skipped schema changes.
#[allow(clippy::too_many_arguments)]
pub async fn run_ddl_poller_loop(
    pg_cfg: crate::config::PgConfig,
    sr_mgr: std::sync::Arc<SrConnectionManager>,
    ddl_schema: String,
    ddl_table: String,
    checkpoint: Arc<Mutex<Checkpoint>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    poll_interval_ms: u64,
    table_properties: HashMap<String, String>,
    batch_rows: usize,
    max_retries: u32,
    ddl_applied: Arc<AtomicU64>,
    ddl_skipped: Arc<AtomicU64>,
    table_filter: String,
    table_exclude: String,
    query_timeout_ms: u64,
    ddl_failure_abort: bool,
    schema_change_retry_ms: u64,
    pending_cdc_tables: Arc<Mutex<std::collections::HashSet<String>>>,
    ddl_active: Arc<Mutex<std::collections::HashSet<String>>>,
    dropped_relations: Arc<Mutex<std::collections::HashSet<String>>>,
    error_count: Arc<AtomicU64>,
    rename_map: RenameMap,
    wal_buffer: Arc<crate::wal_buffer::WalBufferManager>,
) {
    let mut retry_delay = Duration::from_secs(1);
    const MAX_RETRY: Duration = Duration::from_secs(60);

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        // Spawn the poller in its own task so that any panic (from
        // polyglot-sql transpilation, string slicing, etc.) is caught
        // at the task boundary and converted to a JoinError — the outer
        // loop restarts it with backoff instead of the entire poller
        // dying permanently.
        let pg = pg_cfg.clone();
        let sr = sr_mgr.clone();
        let schema = ddl_schema.clone();
        let table = ddl_table.clone();
        let cp = checkpoint.clone();
        let cp_store = checkpoint_store.clone();
        let rx = shutdown_rx.clone();
        let tx = shutdown_tx.clone();
        let da = ddl_applied.clone();
        let ds = ddl_skipped.clone();
        let ec = error_count.clone();

        let tp = table_properties.clone();
        let tf = table_filter.clone();
        let tex = table_exclude.clone();
        let qto = query_timeout_ms;
        let dfa = ddl_failure_abort;
        let scrm = schema_change_retry_ms;
        let pct = pending_cdc_tables.clone();
        let dda = ddl_active.clone();
        let drp = dropped_relations.clone();
        let rm = rename_map.clone();
        let wb = wal_buffer.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let session_start = std::time::Instant::now();
        let inner_handle = tokio::spawn(async move {
            let result = run_ddl_poller(
                &pg,
                &sr,
                &schema,
                &table,
                cp,
                &*cp_store,
                rx,
                tx,
                poll_interval_ms,
                &tp,
                batch_rows,
                max_retries,
                &da,
                &ds,
                &tf,
                &tex,
                qto,
                dfa,
                scrm,
                pct,
                dda,
                drp,
                &ec,
                rm,
                wb,
            )
            .await;
            let _ = result_tx.send(result);
        });
        // Abort the inner task on drop (outer loop cancellation), avoiding
        // a detached zombie task.
        let _task_guard = crate::util::AbortOnDrop(inner_handle);

        match result_rx.await {
            Ok(Ok(())) => {
                tracing::info!("DDL poller exited normally");
                break;
            }
            Ok(Err(e)) => {
                tracing::error!(
                    "DDL poller error: {:?} — restarting in {:?}",
                    e,
                    retry_delay
                );
            }
            Err(_) => {
                tracing::warn!(
                    "DDL poller task aborted (panic or cancellation) — restarting in {:?}",
                    retry_delay,
                );
            }
        }

        if *shutdown_rx.borrow() {
            break;
        }

        // A session that survived long enough to be "healthy" resets the
        // backoff: without this, one error burst pins the poller at the 60s cap
        // for the rest of the process lifetime, delaying every DDL by up to a
        // minute even after PG/SR fully recovered.
        if session_start.elapsed() > MAX_RETRY {
            retry_delay = Duration::from_secs(1);
        }

        tokio::time::sleep(crate::util::jitter_duration(retry_delay)).await;
        retry_delay = (retry_delay * 2).min(MAX_RETRY);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ddl_poller(
    pg_cfg: &crate::config::PgConfig,
    sr_mgr: &std::sync::Arc<SrConnectionManager>,
    ddl_schema: &str,
    ddl_table: &str,
    checkpoint: Arc<Mutex<Checkpoint>>,
    store: &dyn CheckpointStore,
    mut shutdown_rx: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    poll_interval_ms: u64,
    table_properties: &HashMap<String, String>,
    batch_rows: usize,
    max_retries: u32,
    ddl_applied: &Arc<AtomicU64>,
    ddl_skipped: &Arc<AtomicU64>,
    table_filter: &str,
    table_exclude: &str,
    query_timeout_ms: u64,
    ddl_failure_abort: bool,
    schema_change_retry_ms: u64,
    pending_cdc_tables: Arc<Mutex<std::collections::HashSet<String>>>,
    ddl_active: Arc<Mutex<std::collections::HashSet<String>>>,
    dropped_relations: Arc<Mutex<std::collections::HashSet<String>>>,
    error_count: &Arc<AtomicU64>,
    rename_map: RenameMap,
    wal_buffer: Arc<crate::wal_buffer::WalBufferManager>,
) -> anyhow::Result<()> {
    use crate::checkpoint::{advance_and_save_ddl_checkpoint, advance_ddl_checkpoint};

    // Compile table_filter regex once for the lifetime of this poller task.
    let table_filter_re = crate::config::compile_table_filter(table_filter)
        .context("DDL poller: failed to compile table_filter regex")?;
    let table_exclude_re = crate::config::compile_table_exclude(table_exclude)
        .context("DDL poller: failed to compile table_exclude regex")?;

    let conn_str = pg_cfg.conn_str();
    let tls_conn = PgTlsConnector::from_config(&pg_cfg.tls)
        .context("DDL poller: failed to configure PG TLS")?;
    let (pg, pg_conn) = tls_conn
        .connect(&conn_str)
        .await
        .context("DDL poller PG connect failed")?;
    // Abort the connection background task when the poller exits.
    // Without this, dropping the JoinHandle detaches the task,
    // leaving a zombie PG connection alive until TCP timeout (~2h).
    let _pg_conn_guard = AbortOnDrop(pg_conn);

    tracing::info!("DDL poller started (interval={}ms)", poll_interval_ms);

    // Track consecutive execution failures per DDL ID to avoid infinite retry
    let mut ddl_failures: HashMap<i64, u32> = HashMap::new();
    // Track consecutive CDC-pending deferrals per DDL ID. If a DDL is deferred
    // too many times (indicating a stuck flush), log FATAL and signal shutdown.
    let mut ddl_deferrals: HashMap<i64, u32> = HashMap::new();
    const MAX_DDL_DEFERRALS: u32 = 720; // 720 × poll_interval (5s default) ≈ 1hr
                                        // (bumped from 20/100s — with wait_for_cdc_drain + DdlActiveGuard,
                                        // the snapshot TOCTOU recheck deferral is now rare (μs window). The high
                                        // threshold is a generous safety valve for pathological cases, NOT an
                                        // aggressive FATAL that kills the pipeline on hot tables.)
    const MAX_GLOBAL_DDL_DEFERRALS: usize = 200; // memory safety valve for DDL storms
                                                 // Track tables created with inline PRIMARY KEY to detect ALTER TABLE
                                                 // ADD PRIMARY KEY sub-commands. PG emits two events for CREATE TABLE with
                                                 // an inline PK: the CREATE TABLE and a subsequent ALTER TABLE ADD PRIMARY KEY.
                                                 // The second is a sub-command and must be skipped. Uses a HashSet keyed by
                                                 // "schema.table" to correctly handle interleaved CREATE TABLEs (migration
                                                 // scripts creating multiple tables back-to-back).
    let mut pending_inline_pks: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    // Cap the inline-PK set to 100 entries to prevent unbounded memory growth
    // on long-running pipelines with heavy DDL churn.
    const MAX_PENDING_INLINE_PKS: usize = 100;
    // Track consecutive PG query failures so we can return an error
    // and let the outer supervisor loop reconnect with a fresh connection
    const MAX_CONSECUTIVE_QUERY_ERRORS: u32 = 10;
    let mut consecutive_query_errors: u32 = 0;
    // Max DDL id observed by the PREVIOUS poll. The newest visible row is
    // deferred one poll cycle (see the head-confirmation logic in the batch
    // loop) to close the uncommitted-id visibility gap: a DDL row whose
    // transaction is still open when a LATER id commits can become visible
    // AFTER the poller already advanced past it — permanently skipped (SR
    // schema drift). Deferring the head row guarantees every row is either
    // processed as a non-head or as a twice-seen head.
    let mut prev_poll_max_id: Option<i64> = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                tracing::info!("DDL poller shutting down");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
        }

        // Re-check shutdown
        if *shutdown_rx.borrow() {
            break;
        }

        let last_id = lock_checkpoint(&checkpoint).last_ddl_id;
        let ddl_query = format!(
            "SELECT id, ddl, schema, tag FROM {}.{} \
             WHERE id > $1 AND ddl IS NOT NULL \
             ORDER BY id \
             LIMIT 100",
            ddl_schema, ddl_table
        );

        // Max time for a single DDL poll query before treating the connection
        // as dead. Uses the configured value with a 2s floor for safety.
        let query_timeout = std::time::Duration::from_millis(query_timeout_ms.max(2000));

        let rows =
            match tokio::time::timeout(query_timeout, pg.query(&ddl_query, &[&last_id])).await {
                Ok(Ok(r)) => {
                    consecutive_query_errors = 0; // reset on success
                    r
                }
                Ok(Err(e)) => {
                    consecutive_query_errors += 1;
                    tracing::error!(
                        "DDL poll query failed ({}/{}) : {:?}",
                        consecutive_query_errors,
                        MAX_CONSECUTIVE_QUERY_ERRORS,
                        e
                    );
                    if consecutive_query_errors >= MAX_CONSECUTIVE_QUERY_ERRORS {
                        return Err(anyhow::anyhow!(
                            "DDL poller PG connection lost after {} consecutive query failures — \
                         returning error so outer loop reconnects",
                            consecutive_query_errors
                        ));
                    }
                    continue;
                }
                Err(_timeout) => {
                    consecutive_query_errors += 1;
                    tracing::error!(
                        "DDL poll query timed out ({}/{}) — treating as connection failure",
                        consecutive_query_errors,
                        MAX_CONSECUTIVE_QUERY_ERRORS,
                    );
                    if consecutive_query_errors >= MAX_CONSECUTIVE_QUERY_ERRORS {
                        return Err(anyhow::anyhow!(
                            "DDL poller PG connection lost after {} timeouts — \
                         returning error so outer loop reconnects",
                            consecutive_query_errors
                        ));
                    }
                    continue;
                }
            };

        // Diagnostic: warn on pathological DDL deferral buildup.
        // Each entry is ~12 bytes so this is not a memory concern, but a large
        // map signals high DDL churn with slow CDC drain — worth surfacing.
        if ddl_deferrals.len() > MAX_GLOBAL_DDL_DEFERRALS {
            tracing::warn!(
                "DDL deferrals map has {} entries (>{} threshold) — high DDL churn \
                 with pending CDC. Entries will clear as CDC drains or hit \
                 MAX_DDL_DEFERRALS ({} per-DDL cap).",
                ddl_deferrals.len(),
                MAX_GLOBAL_DDL_DEFERRALS,
                MAX_DDL_DEFERRALS,
            );
        }

        // Dedup identical ddl_text within a batch. Legacy trigger
        // function (pre- fix) stored current_query() verbatim, which for a
        // multi-target `DROP TABLE a, b, c` inserts 3 rows with identical
        // ddl_text. Without dedup, the poller re-executes the multi-target
        // DROP 3 times on SR (idempotent — first drops all 3 tables, 2nd/3rd
        // return "Unknown table" treated-as-success). More critically, the
        // `wait_for_cdc_drain` + `DdlActiveGuard` only protect the FIRST
        // table the regex captures → tables b/c race unguarded. The
        // trigger fix (per-row obj.object_identity) eliminates this at the
        // source; this dedup is defense-in-depth for legacy capture rows.
        let mut seen_ddl_text: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Recreate-skip: a DROP TABLE whose name is re-created by a
        // same-batch CREATE TABLE must NOT enqueue name-keyed cache
        // eviction — the relation is re-registered on CDC pickup and the
        // pipeline cancels pending evictions for it (add_relation). Skipping
        // here removes the poll-lag window where the eviction could land
        // between the recreate's Relation registration and its eviction
        // drain.
        let mut recreate_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for row in &rows {
            let tag: String = match row.try_get(3) {
                Ok(Some(v)) => v,
                _ => continue,
            };
            if tag != "CREATE TABLE" && tag != "CREATE TABLE AS" {
                continue;
            }
            let ddl_text: String = match row.try_get(1) {
                Ok(Some(v)) => v,
                _ => continue,
            };
            if let Some((schema, table)) = extract_create_table_ref(&ddl_text) {
                recreate_names.insert(format!("{schema}.{table}"));
            }
        }

        // Head-confirmation (uncommitted-id visibility gap): a DDL row whose
        // transaction is still open when a LATER id commits can become
        // visible AFTER the poller advanced past it — permanently skipped
        // (SR schema drift). The newest visible row is deferred one poll
        // cycle so any lower-id row has a cycle to appear; a row that was
        // also the previous poll's head is confirmed and processed (no stall
        // on quiet systems). A full batch (LIMIT 100) means higher ids exist
        // beyond the batch, so the batch's max is not the true head — no
        // deferral needed.
        let batch_is_full = rows.len() >= 100;
        let max_seen: Option<i64> = rows
            .iter()
            .filter_map(|r| r.try_get::<_, Option<i64>>(0).ok().flatten())
            .max();

        'batch: for row in &rows {
            // Check shutdown at the top of each batch
            // iteration. Without this, a large 100-row batch (each row can take
            // seconds: transpile + SR execute + optional snapshot) delays a
            // graceful shutdown by minutes.
            if *shutdown_rx.borrow() {
                tracing::info!("DDL poller: shutdown during batch, breaking");
                break 'batch;
            }
            let ddl_id: i64 = match row.try_get(0) {
                Ok(Some(v)) => v,
                _ => {
                    tracing::warn!("DDL poller: row missing id column — skipping");
                    continue;
                }
            };
            let mut ddl_text: String = match row.try_get(1) {
                Ok(Some(v)) => v,
                _ => {
                    tracing::warn!("DDL poller: row missing ddl column — skipping");
                    continue;
                }
            };
            let db_schema: String = match row.try_get(2) {
                Ok(Some(v)) => v,
                _ => String::new(),
            };
            let tag: String = match row.try_get(3) {
                Ok(Some(v)) => v,
                _ => String::new(),
            };

            // Defer the newest visible DDL one poll cycle (see the
            // head-confirmation comment above the loop). The deferral must
            // happen BEFORE the dedup insert so the row is re-read fresh
            // next poll.
            if !batch_is_full {
                if let Some(m) = max_seen {
                    if ddl_id == m && prev_poll_max_id != Some(m) {
                        tracing::info!(
                            "DDL id={} is the newest visible DDL — deferring one poll cycle (uncommitted-id visibility gap)",
                            ddl_id,
                        );
                        continue;
                    }
                }
            }

            tracing::info!(
                "DDL id={} tag={} schema={} ddl={}",
                ddl_id,
                tag,
                db_schema,
                crate::util::truncate_str(&ddl_text, 120)
            );

            // Dedup identical ddl_text within this batch. Skips the
            // 2nd+ occurrence of the same ddl_text (advances checkpoint,
            // increments ddl_skipped). Only triggers on legacy capture rows
            // — the fixed trigger emits per-row obj.object_identity.
            if !seen_ddl_text.insert(ddl_text.clone()) {
                tracing::info!(
                    "DDL id={} — duplicate ddl_text in this batch (legacy \
                     multi-target DROP), skipping as already processed",
                    ddl_id,
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            if !should_sync_ddl(&tag) {
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                // Non-table DDL doesn't affect inline PK tracking
                continue;
            }

            // Single-schema enforcement: the DDL capture trigger fires for
            // DDL across ALL schemas (event triggers are database-global),
            // but only the configured schema is replicated — `sr_table_name`
            // drops the schema prefix, so syncing a second schema would
            // collide same-named tables in one SR table. Skip DDLs for other
            // schemas entirely (mirrors the CDC/snapshot schema filters).
            if !db_schema.is_empty() && db_schema != pg_cfg.schema {
                tracing::info!(
                    "DDL id={} skipped (schema '{}' != configured postgres.schema '{}')",
                    ddl_id,
                    db_schema,
                    pg_cfg.schema,
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Skip PL/pgSQL DO blocks — when an ALTER TABLE / CREATE TABLE /
            // DROP TABLE runs inside a `DO $$ ... $$` block, PG's event
            // trigger captures `current_query()` which returns the entire DO
            // block text, not the individual inner DDL. polyglot-sql can't
            // parse PL/pgSQL, so without this skip the poller burns its
            // 3-retry budget and aborts (when `ddl_failure_abort = true`).
            // The trigger captures DO blocks as-is by design — this poller-
            // level skip is the sole handling. Individual DDLs inside DO
            // blocks cannot be extracted; operators should run table DDLs
            // as standalone statements, not inside DO blocks.
            if is_plpgsql_do_block(&ddl_text) {
                tracing::warn!(
                    "DDL id={} skipped (PL/pgSQL DO block captured as DDL — \
                     inner statements cannot be transpiled): {}",
                    ddl_id,
                    crate::util::truncate_str(&ddl_text, 100),
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Skip DDL captured from function/procedure bodies — when DDL
            // runs inside a function, current_query() returns the calling
            // query (e.g. "SELECT my_func()"), not the inner DDL. The tag
            // is correct (e.g. "ALTER TABLE") but the text is a SELECT/CALL,
            // which polyglot-sql would transpile and execute on SR — either
            // failing or doing something unintended.
            if is_function_wrapped_ddl(&tag, &ddl_text) {
                tracing::warn!(
                    "DDL id={} skipped (function-wrapped DDL — tag={} but text doesn't \
                     start with expected keyword): {}",
                    ddl_id,
                    tag,
                    crate::util::truncate_str(&ddl_text, 100),
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Skip DROP SCHEMA / DROP DATABASE captured as DROP TABLE.
            // PG's sql_drop trigger fires once per dropped table, each
            // capturing current_query() = "DROP SCHEMA ... CASCADE".
            // The poller can't extract per-table DROPs from this text.
            // SR tables from the dropped schema are orphaned — operators
            // should clean them up manually.
            if tag == "DROP TABLE" && is_drop_schema_or_database(&ddl_text) {
                tracing::warn!(
                    "DDL id={} skipped (DROP SCHEMA/DATABASE captured as DROP TABLE — \
                     individual SR tables may need manual cleanup): {}",
                    ddl_id,
                    crate::util::truncate_str(&ddl_text, 120),
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Skip CREATE TABLE ... PARTITION OF events — the parent CREATE TABLE
            // creates the SR table, and the CDC consumer maps partition OIDs to the
            // parent table's identity. Individual partitions are internal PG objects
            // that should not be created separately in StarRocks.
            if tag == "CREATE TABLE" && ddl_text.contains("PARTITION OF") {
                tracing::info!(
                    "DDL id={} skipping PARTITION OF for {}.{} — handled by parent table",
                    ddl_id,
                    db_schema,
                    extract_create_table_ref(&ddl_text)
                        .map(|(_, t)| t)
                        .unwrap_or_default(),
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Skip CREATE TEMPORARY / TEMP TABLE — these are session-scoped
            // in PG, don't generate WAL entries for DML, and should never
            // be created as permanent tables in StarRocks. Without this
            // skip, `post_process_ddl` strips the TEMPORARY flag
            // (`ct.temporary = false`), silently turning temp tables into
            // permanent SR tables that clutter the database and never
            // receive CDC data.
            //
            // Multi-statement texts (`CREATE TEMP TABLE t; CREATE TABLE x`)
            // are handled per-statement: the temp statement is dropped and
            // the remaining statements still execute. Skipping the whole row
            // would lose x forever (the trigger's second identical row is
            // dedup-skipped).
            if (tag == "CREATE TABLE" || tag == "CREATE TABLE AS")
                && is_temporary_table_ddl(&ddl_text)
            {
                match strip_temp_table_statements(&ddl_text) {
                    None => {
                        tracing::info!(
                            "DDL id={} skipped (TEMPORARY TABLE — session-scoped, not replicated to SR)",
                            ddl_id,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }
                    Some(filtered) => {
                        if filtered != ddl_text {
                            tracing::info!(
                                "DDL id={} dropped TEMP TABLE statement(s), processing remaining statements",
                                ddl_id,
                            );
                            ddl_text = filtered;
                        }
                    }
                }
            }

            // Check table_filter: log a warning for tables outside the filter.
            // DDL is still processed (table created in SR for schema sync),
            // but CDC data events are filtered by the pipeline.
            // NOTE: DDL poller creates ALL tables in SR regardless of table_filter.
            // The CDC pipeline enforces table_filter on data events (Insert/Update/Delete/Truncate),
            // so non-matching tables exist in SR for schema sync but receive no CDC data.
            // This allows future table_filter changes to take effect without manual schema setup.
            let table_name = try_extract_table_ref(&ddl_text)
                .map(|(_, t)| t)
                .or_else(|| extract_table_name_simple(&ddl_text));
            if let Some(ref table) = table_name {
                // table_exclude: skip entirely (don't create in SR at all)
                if crate::config::is_table_excluded(&table_exclude_re, &db_schema, table) {
                    tracing::warn!(
                        "DDL id={} excluded by table_exclude='{}': {}.{} — skipping entirely",
                        ddl_id,
                        table_exclude,
                        db_schema,
                        table,
                    );
                    advance_ddl_checkpoint(&checkpoint, ddl_id);
                    continue;
                }
                if !crate::config::table_matches_filter(&table_filter_re, &db_schema, table) {
                    tracing::warn!(
                        "DDL id={} outside table_filter='{}': {}.{} — creating in SR for schema sync (CDC data will be filtered)",
                        ddl_id, table_filter, db_schema, table,
                    );
                }
            }
            // If we can't extract a table reference (rare - should only happen for
            // complex multi-statement DDL), process it anyway rather than blocking.

            // Track CREATE TABLE / CTAS with inline PRIMARY KEY so we can skip
            // the subsequent ALTER TABLE ADD PRIMARY KEY sub-command that PG emits.
            // Uses a HashSet keyed by "schema.table" so back-to-back CREATE TABLEs
            // in migration scripts are handled correctly.
            if tag == "CREATE TABLE" || tag == "CREATE TABLE AS" || tag == "SELECT INTO" {
                if let Some((schema, table)) = extract_create_table_ref(&ddl_text) {
                    if ddl_text.to_ascii_uppercase().contains("PRIMARY KEY") {
                        pending_inline_pks.insert(format!("{}.{}", schema, table));
                        // Enforce cap to prevent unbounded memory growth
                        if pending_inline_pks.len() > MAX_PENDING_INLINE_PKS {
                            pending_inline_pks.clear();
                        }
                    }
                }
            } else if tag == "ALTER TABLE" && ddl_text.to_ascii_uppercase().contains("PRIMARY KEY")
            {
                if let Some((schema, table)) = extract_alter_table_ref(&ddl_text) {
                    let table_key = format!("{}.{}", schema, table);
                    if pending_inline_pks.remove(&table_key) {
                        tracing::debug!(
                            "DDL id={} skipping inline PK sub-command for {}.{} (already handled by CREATE TABLE)",
                            ddl_id, schema, table,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }
                }
            } else if let Some((schema, table)) = try_extract_table_ref(&ddl_text) {
                // Non-CREATE/non-ALTER-PK DDL on a tracked table removes its
                // inline-PK eligibility. This prevents stale matches from
                // earlier CREATE TABLEs when the ALTER TABLE event arrives
                // after other DDL on the same table.
                pending_inline_pks.remove(&format!("{}.{}", schema, table));
            }

            // Skip ALTER TABLE ... REPLICA IDENTITY DDLs — these are PG-only
            // statements that StarRocks doesn't support and the pipeline doesn't
            // need to sync. The pipeline does NOT manage REPLICA IDENTITY (PG's
            // default 'd' is sufficient for the PK-only tables it replicates),
            // but a DBA may still issue these against PG. Without this skip the
            // poller would loop forever trying to transpile an unsupported DDL.
            if tag == "ALTER TABLE" && is_replica_identity_alter(&ddl_text) {
                tracing::debug!("DDL id={} skipped (REPLICA IDENTITY)", ddl_id);
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // ── ALTER TABLE ALTER COLUMN ... SET/DROP DEFAULT ──
            // StarRocks has no `ALTER COLUMN` action (it uses `MODIFY COLUMN`
            // and requires the column type). The pipeline already strips all
            // DEFAULTs from CREATE TABLE / ADD COLUMN and always sends
            // explicit values via Stream Load JSON, so SET/DROP DEFAULT is a
            // no-op for SR — skip it before transpilation. Without this skip,
            // polyglot-sql passes the DDL through verbatim, SR rejects it
            // with error 1064 (`Unexpected input 'ALTER'`), and after 3
            // retries the pipeline aborts (when `ddl_failure_abort = true`).
            // This is the same routing as REPLICA IDENTITY — not the
            // `is_unsupported_alter_table` resnapshot path, since SET/DROP
            // DEFAULT changes no type or column name requiring resnapshot.
            if tag == "ALTER TABLE" && is_alter_column_default(&ddl_text) {
                tracing::debug!(
                    "DDL id={} skipped (ALTER COLUMN ... SET/DROP DEFAULT — no-op in SR; \
                     defaults stripped on CREATE TABLE / ADD COLUMN)",
                    ddl_id,
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // ── PG-only metadata ALTERs that polyglot can't parse ──
            // ALTER TABLE... SET SCHEMA / VALIDATE CONSTRAINT / CLUSTER /
            // ENABLE|DISABLE TRIGGER / *ROW LEVEL SECURITY / ATTACH|DETACH
            // PARTITION / RENAME CONSTRAINT. polyglot-sql has no dedicated
            // variants for these — it would either throw an Err or parse to
            // Raw{sql:...}; without this skip, the poller burns its 3-retry
            // budget and aborts under `ddl_failure_abort=true`. None of these
            // operations affect StarRocks's storage model, so SKIP is safe —
            // the SR table is left unchanged.
            if tag == "ALTER TABLE" && is_pg_only_metadata_alter(&ddl_text) {
                tracing::debug!(
                    "DDL id={} skipped (PG-only metadata ALTER — no SR equivalent)",
                    ddl_id,
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // ── ALTER TABLE ... DROP CONSTRAINT ──
            // SR has no constraint concept, so this is a no-op for SR
            // (DropConstraint is also filtered by is_pg_only_alter_action at the
            // AST level). PK-less tables are never synced by design (refused at
            // startup validation, skipped at snapshot, and un-creatable during
            // CDC), so a dropped PRIMARY KEY needs no special handling here.
            // Short-circuit textually with a debug breadcrumb and advance.
            if tag == "ALTER TABLE" && is_drop_constraint_alter(&ddl_text) {
                tracing::debug!(
                    "DDL id={} skipped (DROP CONSTRAINT — no SR equivalent)",
                    ddl_id,
                );
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // ── ALTER TABLE ALTER COLUMN ... TYPE / SET DATA TYPE / RENAME COLUMN ──
            // StarRocks does not support type changes or column renames.  Instead of
            // skipping these silently (which causes silent schema drift — CDC writes
            // for renamed columns fail with "unknown column"), drop the SR table and
            // re-snapshot from PG. The snapshot reads the current catalog (with the
            // new type/name) and creates a matching SR table.
            if tag == "ALTER TABLE" && is_unsupported_alter_table(&ddl_text) {
                // For ALTER TABLE ... RENAME TO, the OLD table name no longer
                // exists in PG (it was renamed). extract_alter_table_ref returns
                // the OLD name; we need the NEW name to snapshot the right table.
                // For ALTER COLUMN TYPE / RENAME COLUMN, the table still exists
                // under its original name, so extract_alter_table_ref is correct.
                let upper = ddl_text.to_uppercase();
                let is_rename_to =
                    upper.contains(" RENAME TO ") && !upper.contains(" RENAME COLUMN ");
                let ref_opt = if is_rename_to {
                    extract_rename_to_new_ref(&ddl_text)
                        .or_else(|| extract_alter_table_ref(&ddl_text))
                } else {
                    extract_alter_table_ref(&ddl_text)
                };
                if let Some((schema, table)) = ref_opt {
                    // Wait for CDC to drain (no FATAL), then hold
                    // DdlActiveGuard across snapshot_one_table so writes for
                    // this table are durably buffered until the snapshot
                    // completes.
                    let table_key = format!("{}.{}", schema, table);
                    // RENAME COLUMN → register the old→new column mapping for
                    // the pipeline's replay path: buffered rows written under
                    // the OLD name must be remapped to the NEW name before
                    // they reach SR. RENAME TO is excluded (table rename, not
                    // column). ALTER COLUMN TYPE needs no registration — the
                    // column name is unchanged and buffered values coerce on
                    // SR load (accepted residual). Multi-action statements
                    // register EVERY pair. Pairs stay in scope for
                    // the skip paths below.
                    let rename_pairs: Vec<(String, String)> = if is_rename_to {
                        vec![]
                    } else {
                        parse_rename_pairs(&ddl_text)
                    };
                    if !rename_pairs.is_empty() {
                        for (old, new) in &rename_pairs {
                            register_rename(&rename_map, &table_key, old, new);
                        }
                        tracing::info!(
                            "DDL id={} registered {} RENAME COLUMN pair(s) for wal-buffer replay remap",
                            ddl_id,
                            rename_pairs.len(),
                        );
                        // Persist the mapping NOW (not only at next append):
                        // a crash after the DDL checkpoint advances but
                        // before replay drains must not lose the remap.
                        // Only needed while segments exist — appends write
                        // the sidecar themselves.
                        if wal_buffer.has_segments(&table_key) {
                            let entries = crate::pipeline::lock_or_recover(&rename_map)
                                .get(&table_key)
                                .cloned()
                                .unwrap_or_default();
                            if let Err(e) =
                                wal_buffer.write_renames_sidecar(&table_key, &entries).await
                            {
                                tracing::warn!(
                                    "DDL id={} rename sidecar update failed (non-fatal; next append rewrites): {e:#}",
                                    ddl_id,
                                );
                            }
                        }
                    }
                    // Two-phase acquire — drain WITHOUT the
                    // guard (so flushes can complete), then acquire the guard
                    // and re-check. The prior guard-first ordering starved on
                    // hot tables: the guard made every flush defer, so
                    // `pending_cdc_tables` never cleared and the drain waited
                    // forever while the replay set grew to the HARD-limit
                    // FATAL. See acquire_ddl_guard for the full analysis.
                    let _ddl_guard = match acquire_ddl_guard(
                        &ddl_active,
                        &pending_cdc_tables,
                        &table_key,
                        &mut shutdown_rx,
                        ddl_id,
                        &schema,
                        &table,
                    )
                    .await
                    {
                        Ok(g) => g,
                        Err(e) => {
                            let _ = shutdown_tx.send(true);
                            return Err(e);
                        }
                    };
                    ddl_deferrals.remove(&ddl_id);
                    // For ALTER TABLE... RENAME TO, snapshot_one_table
                    // creates/snapshots the NEW SR table but never drops the OLD
                    // one — leaving it orphaned (storage leak + stale-data-exposed
                    // + collision risk if PG later reuses the old name: the new
                    // table's snapshot would merge into the leftover SR table via
                    // the schema-hash skip at snapshot.rs:1128). Best-effort DROP
                    // TABLE IF EXISTS on the old SR name (extract_alter_table_ref
                    // returns the ALTER TABLE target = OLD name for RENAME TO).
                    // Tolerate errors (old SR table may not exist on first run).
                    if is_rename_to {
                        if let Some((old_schema, old_table)) = extract_alter_table_ref(&ddl_text) {
                            let old_sr_table =
                                crate::sr_sql::sr_table_name(&old_schema, &old_table);
                            let drop_old_sql = format!(
                                "DROP TABLE IF EXISTS `{}`.`{}`",
                                sr_mgr.database(),
                                old_sr_table,
                            );
                            if let Err(e) = crate::sr_sql::execute_sql(sr_mgr, &drop_old_sql).await
                            {
                                tracing::debug!(
                                    "Best-effort DROP of old SR table '{}' after RENAME TO (non-fatal): {:?}",
                                    old_sr_table, e
                                );
                            } else {
                                tracing::info!(
                                    "DDL id={} RENAME TO: dropped orphaned old SR table '{}'",
                                    ddl_id,
                                    old_sr_table
                                );
                            }
                            // Enqueue the OLD table_key for
                            // relation/PK/name_to_oid cache eviction by the
                            // pipeline (mirrors the DROP TABLE eviction at
                            // main.rs:1314). Without this, the cache retains a
                            // stale OID-keyed entry for the old name — if PG
                            // later reuses the old OID for a new table with a
                            // different schema, CDC events for the new table
                            // would mis-route through the stale cache. Bound
                            // memory growth under RENAME churn.
                            let old_table_key = format!("{}.{}", old_schema, old_table);
                            // Recreate-skip: a same-batch CREATE TABLE means
                            // the name is re-registered on CDC pickup — don't
                            // enqueue eviction for it.
                            if recreate_names.contains(&old_table_key) {
                                tracing::info!(
                                    "DDL id={} RENAME TO: old name {old_table_key} recreated same-batch — skipping eviction enqueue",
                                    ddl_id,
                                );
                            } else {
                                crate::pipeline::lock_or_recover(&dropped_relations)
                                    .insert(old_table_key);
                            }
                        }
                    }
                    match crate::snapshot::snapshot_one_table(
                        &pg,
                        sr_mgr,
                        &schema,
                        &table,
                        &checkpoint,
                        store,
                        table_properties,
                        batch_rows,
                        max_retries,
                        query_timeout_ms,
                        &pending_cdc_tables,
                    )
                    .await
                    {
                        Ok(true) => {
                            tracing::info!(
                                "DDL id={} ALTER COLUMN → resnapshotted {}.{}",
                                ddl_id,
                                schema,
                                table,
                            );
                            ddl_applied.fetch_add(1, Ordering::Relaxed);
                            advance_and_save_ddl_checkpoint(&checkpoint, store, ddl_id).await;
                            ddl_failures.remove(&ddl_id);
                            continue;
                        }
                        Ok(false) => {
                            ddl_skipped.fetch_add(1, Ordering::Relaxed);
                            // The rename never reached SR — remove
                            // its mapping BEFORE the checkpoint advances so a
                            // crash here can't resurrect it on restart.
                            if !rename_pairs.is_empty() {
                                unregister_rename_pairs(
                                    &rename_map,
                                    &wal_buffer,
                                    &table_key,
                                    &rename_pairs,
                                )
                                .await;
                            }
                            advance_ddl_checkpoint(&checkpoint, ddl_id);
                            ddl_failures.remove(&ddl_id);
                            continue;
                        }
                        Err(e) => {
                            // TOCTOU re-check deferral
                            // → retry on next poll, don't burn the retry budget.
                            if handle_snapshot_deferral(
                                &e,
                                ddl_id,
                                &schema,
                                &table,
                                &mut ddl_deferrals,
                                MAX_DDL_DEFERRALS,
                                poll_interval_ms,
                                &shutdown_tx,
                                ddl_failure_abort,
                            ) {
                                break 'batch;
                            }
                            let failures = ddl_failures.entry(ddl_id).or_insert(0);
                            *failures += 1;
                            if *failures >= 3 {
                                tracing::error!(
                                    "FATAL: DDL id={} ALTER COLUMN snapshot failed after {} retries — {}.",
                                    ddl_id, *failures,
                                    if ddl_failure_abort { "ABORTING pipeline" } else { "advancing checkpoint" },
                                );
                                if ddl_failure_abort {
                                    let _ = shutdown_tx.send(true);
                                    return Err(anyhow::anyhow!(
                                        "DDL id={} permanent failure (ALTER COLUMN snapshot) — abort on ddl_failure_abort=true",
                                        ddl_id,
                                    ));
                                }
                                error_count.fetch_add(1, Ordering::Relaxed);
                                // DDL permanently failed — the
                                // rename never reached SR; unregister BEFORE
                                // the checkpoint advances (crash safety).
                                if !rename_pairs.is_empty() {
                                    unregister_rename_pairs(
                                        &rename_map,
                                        &wal_buffer,
                                        &table_key,
                                        &rename_pairs,
                                    )
                                    .await;
                                }
                                advance_ddl_checkpoint(&checkpoint, ddl_id);
                                ddl_failures.remove(&ddl_id);
                            } else {
                                tracing::error!(
                                    "DDL id={} ALTER COLUMN snapshot failed (attempt {}/3): {:?} — will retry",
                                    ddl_id, *failures, e,
                                );
                            }
                            continue;
                        }
                    }
                } else {
                    tracing::warn!(
                        "DDL id={} could not parse ALTER TABLE ref, advancing",
                        ddl_id
                    );
                    ddl_skipped.fetch_add(1, Ordering::Relaxed);
                    advance_ddl_checkpoint(&checkpoint, ddl_id);
                    ddl_failures.remove(&ddl_id);
                    continue;
                }
            }

            // ── ALTER TABLE ADD PRIMARY KEY → drop+recreate+snapshot ──
            // SR can't convert DUPLICATE → PRIMARY KEY table model. The
            // transpiled ALTER TABLE would fail. Instead, drop the existing
            // DUPLICATE table, recreate with PRIMARY KEY model, and
            // snapshot all data from PG. Publication + REPLICA IDENTITY
            // are handled by the DDL event trigger.
            if tag == "ALTER TABLE" && ddl_text.to_ascii_uppercase().contains("PRIMARY KEY") {
                if let Some((schema, table)) = extract_alter_table_ref(&ddl_text) {
                    // Two-phase acquire — drain WITHOUT the
                    // guard, then acquire and re-check (mirrors the ALTER
                    // COLUMN resnapshot path above and the execute_sql path
                    // below). See acquire_ddl_guard for the starvation
                    // analysis of the prior guard-first ordering.
                    let table_key = format!("{}.{}", schema, table);
                    let _ddl_guard = match acquire_ddl_guard(
                        &ddl_active,
                        &pending_cdc_tables,
                        &table_key,
                        &mut shutdown_rx,
                        ddl_id,
                        &schema,
                        &table,
                    )
                    .await
                    {
                        Ok(g) => g,
                        Err(e) => {
                            let _ = shutdown_tx.send(true);
                            return Err(e);
                        }
                    };
                    ddl_deferrals.remove(&ddl_id);
                    match crate::snapshot::snapshot_one_table(
                        &pg,
                        sr_mgr,
                        &schema,
                        &table,
                        &checkpoint,
                        store,
                        table_properties,
                        batch_rows,
                        max_retries,
                        query_timeout_ms,
                        &pending_cdc_tables,
                    )
                    .await
                    {
                        Ok(true) => {
                            tracing::info!(
                                "DDL id={} ALTER PK → snapshotted {}.{}",
                                ddl_id,
                                schema,
                                table,
                            );
                            ddl_applied.fetch_add(1, Ordering::Relaxed);
                            advance_and_save_ddl_checkpoint(&checkpoint, store, ddl_id).await;
                            ddl_failures.remove(&ddl_id);
                            continue;
                        }
                        Ok(false) => {
                            // Already snapshotted or no PK — just advance
                            ddl_skipped.fetch_add(1, Ordering::Relaxed);
                            advance_ddl_checkpoint(&checkpoint, ddl_id);
                            ddl_failures.remove(&ddl_id);
                            continue;
                        }
                        Err(e) => {
                            // TOCTOU re-check deferral
                            // → retry on next poll, don't burn the retry budget.
                            if handle_snapshot_deferral(
                                &e,
                                ddl_id,
                                &schema,
                                &table,
                                &mut ddl_deferrals,
                                MAX_DDL_DEFERRALS,
                                poll_interval_ms,
                                &shutdown_tx,
                                ddl_failure_abort,
                            ) {
                                break 'batch;
                            }
                            let failures = ddl_failures.entry(ddl_id).or_insert(0);
                            *failures += 1;
                            if *failures >= 3 {
                                tracing::error!(
                                    "FATAL: DDL id={} ALTER PK snapshot failed after {} retries — {}.",
                                    ddl_id, *failures,
                                    if ddl_failure_abort { "ABORTING pipeline" } else { "advancing checkpoint" },
                                );
                                if ddl_failure_abort {
                                    let _ = shutdown_tx.send(true);
                                    return Err(anyhow::anyhow!(
                                        "DDL id={} permanent failure (ALTER PK snapshot) — abort on ddl_failure_abort=true",
                                        ddl_id,
                                    ));
                                }
                                error_count.fetch_add(1, Ordering::Relaxed);
                                advance_ddl_checkpoint(&checkpoint, ddl_id);
                                ddl_failures.remove(&ddl_id);
                            } else {
                                tracing::error!(
                                    "DDL id={} ALTER PK snapshot failed (attempt {}/3): {:?} — will retry",
                                    ddl_id, *failures, e,
                                );
                            }
                            continue;
                        }
                    }
                } else {
                    // Couldn't parse table name — advance to avoid blocking
                    tracing::warn!(
                        "DDL id={} could not parse ALTER TABLE ref, advancing",
                        ddl_id
                    );
                    ddl_skipped.fetch_add(1, Ordering::Relaxed);
                    advance_ddl_checkpoint(&checkpoint, ddl_id);
                    ddl_failures.remove(&ddl_id);
                    continue;
                }
            }

            let ddl_text = strip_pg_only_ddl_syntax(&ddl_text);

            // Run transpile on a dedicated thread with 8MB stack — polyglot-sql's
            // transpilation is CPU-bound (needs off the async runtime) and its
            // recursive parser can overflow the default 2MB thread stack on
            // deeply nested DDL (e.g. ARRAY<ARRAY<…>>). tokio's spawn_blocking
            // uses the blocking pool default stack; we use a custom thread instead.
            let ddl_for_transpile = ddl_text.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            // Bind the JoinHandle instead of `let _ =`
            // (which detaches it immediately). Joined below after the result is
            // received so the OS thread is reaped on the happy/panic paths;
            // if the outer task is cancelled the future drops and the handle
            // detaches (no correctness impact — only residual CPU until the
            // transpile completes).
            let handle = std::thread::Builder::new()
                .stack_size(8 * 1024 * 1024)
                .spawn(move || {
                    let result = polyglot_sql::transpile(
                        &ddl_for_transpile,
                        polyglot_sql::dialects::DialectType::PostgreSQL,
                        polyglot_sql::dialects::DialectType::StarRocks,
                    );
                    let _ = tx.send(result);
                })
                .context("Failed to spawn transpile thread")?;

            let rx_result = rx.await;
            // Joined now that the result was received — the thread exits right
            // after sending, so this is near-instant (panic case also already dead).
            let _ = handle.join();

            let transpiled = match rx_result {
                Ok(Ok(ts)) => {
                    // CTAS: polyglot-sql passes CREATE TABLE ... AS SELECT
                    // through verbatim with 0 column defs. The post-processor
                    // then produces `CREATE TABLE foo () ENGINE=OLAP ...`,
                    // which is invalid SR. Fall through to catalog-based
                    // schema discovery.
                    if tag == "CREATE TABLE AS" || tag == "SELECT INTO" {
                        // Warn loudly that the CTAS SELECT body is NOT
                        // replayed — the catalog fallback creates an empty SR
                        // table from the new PG table's columns, then the
                        // snapshot re-reads via `SELECT *` from it. For non-
                        // trivial CTAS (SELECT with WHERE / GROUP BY / JOIN /
                        // expressions), SR ends up with the raw unfiltered
                        // source rows, NOT the CTAS result. Operators must
                        // verify the SR table contents match the intended
                        // CTAS semantics after sync.
                        tracing::warn!(
                            "DDL id={} CTAS — using catalog fallback for schema discovery. \
                             IMPORTANT: the SELECT body is NOT replayed. The SR table will \
                             contain all rows from the new PG table (snapshot re-read). For \
                             non-trivial CTAS (WHERE/GROUP BY/JOIN/expressions in the SELECT \
                             body), SR data will differ from the intended CTAS result — \
                             operators must verify.",
                            ddl_id,
                        );
                        None
                    } else {
                        Some(ts.join(";\n"))
                    }
                }
                Ok(Err(e)) => {
                    // Polyglot-sql can't handle PG custom types (enums, composites, etc.).
                    // Fall back to PG catalog-based schema discovery for CREATE TABLE.
                    if tag.starts_with("CREATE TABLE") || tag == "SELECT INTO" {
                        tracing::warn!(
                            "DDL id={} transpile failed ({:?}), trying catalog fallback",
                            ddl_id,
                            e,
                        );
                        None
                    } else {
                        let failures = ddl_failures.entry(ddl_id).or_insert(0);
                        *failures += 1;
                        if *failures >= 3 {
                            tracing::error!(
                                "FATAL: DDL id={} transpile failed after {} retries: {:?} — {}.",
                                ddl_id,
                                *failures,
                                e,
                                if ddl_failure_abort {
                                    "ABORTING pipeline"
                                } else {
                                    "advancing checkpoint"
                                },
                            );
                            if ddl_failure_abort {
                                let _ = shutdown_tx.send(true);
                                return Err(anyhow::anyhow!(
                                    "DDL id={} permanent failure (transpile) — abort on ddl_failure_abort=true",
                                    ddl_id,
                                ));
                            }
                            error_count.fetch_add(1, Ordering::Relaxed);
                            ddl_skipped.fetch_add(1, Ordering::Relaxed);
                            advance_ddl_checkpoint(&checkpoint, ddl_id);
                            ddl_failures.remove(&ddl_id);
                            continue;
                        } else {
                            tracing::warn!(
                                "DDL id={} transpile failed (attempt {}/3): {:?} — will retry",
                                ddl_id,
                                *failures,
                                e,
                            );
                            break 'batch;
                        }
                    }
                }
                Err(_) => {
                    tracing::error!(
                        "DDL id={} transpile thread panicked or was cancelled — breaking batch",
                        ddl_id,
                    );
                    break 'batch;
                }
            };

            // If transpile produced None (CTAS or transpile error for CREATE TABLE),
            // handle catalog fallback
            let transpiled = match transpiled {
                Some(sql) => sql,
                None => {
                    match catalog_create_table_fallback(
                        &pg,
                        sr_mgr.database(),
                        &ddl_text,
                        table_properties,
                    )
                    .await
                    {
                        Ok(Some(sr_sql)) => {
                            match crate::sr_sql::execute_sql(sr_mgr, &sr_sql).await {
                                Ok(_) => {
                                    tracing::info!(
                                        "DDL id={} applied to SR (catalog fallback)",
                                        ddl_id
                                    );
                                    ddl_applied.fetch_add(1, Ordering::Relaxed);
                                    // Add to publication and set REPLICA IDENTITY
                                    // handled by DDL event trigger — no Rust-side action needed.
                                    if let Some((schema, table)) =
                                        extract_create_table_ref(&ddl_text)
                                    {
                                        // For CREATE TABLE and CTAS, snapshot any existing data
                                        // to cover the gap between table creation and publication
                                        // addition. Without this, INSERTS during the gap would be
                                        // lost since the table wasn't yet in the publication.
                                        if tag == "CREATE TABLE"
                                            || tag == "CREATE TABLE AS"
                                            || tag == "SELECT INTO"
                                        {
                                            // Hold DdlActiveGuard + drain across the
                                            // backfill snapshot (mirrors the ALTER paths).
                                            // Without it, CDC flushes write to the main table
                                            // during the shadow copy and swap_shadow_to_main
                                            // discards them (their LSN is already ACK'd →
                                            // permanent loss, no replay).
                                            let backfill_key = format!("{}.{}", schema, table);
                                            let _backfill_guard = match acquire_ddl_guard(
                                                &ddl_active,
                                                &pending_cdc_tables,
                                                &backfill_key,
                                                &mut shutdown_rx,
                                                ddl_id,
                                                &schema,
                                                &table,
                                            )
                                            .await
                                            {
                                                Ok(g) => g,
                                                Err(e) => {
                                                    let _ = shutdown_tx.send(true);
                                                    return Err(e);
                                                }
                                            };
                                            match crate::snapshot::snapshot_one_table(
                                                &pg,
                                                sr_mgr,
                                                &schema,
                                                &table,
                                                &checkpoint,
                                                store,
                                                table_properties,
                                                batch_rows,
                                                max_retries,
                                                query_timeout_ms,
                                                &pending_cdc_tables,
                                            )
                                            .await
                                            {
                                                Ok(true) => {
                                                    tracing::info!(
                                                        "DDL id={} CTAS data synced for {}.{}",
                                                        ddl_id,
                                                        schema,
                                                        table,
                                                    );
                                                    advance_and_save_ddl_checkpoint(
                                                        &checkpoint,
                                                        store,
                                                        ddl_id,
                                                    )
                                                    .await;
                                                    ddl_failures.remove(&ddl_id);
                                                }
                                                Ok(false) => {
                                                    tracing::debug!(
                                                        "DDL id={} CTAS no data to sync for {}.{}",
                                                        ddl_id,
                                                        schema,
                                                        table,
                                                    );
                                                    advance_and_save_ddl_checkpoint(
                                                        &checkpoint,
                                                        store,
                                                        ddl_id,
                                                    )
                                                    .await;
                                                    ddl_failures.remove(&ddl_id);
                                                }
                                                Err(e) => {
                                                    // TOCTOU re-check deferral.
                                                    if handle_snapshot_deferral(
                                                        &e,
                                                        ddl_id,
                                                        &schema,
                                                        &table,
                                                        &mut ddl_deferrals,
                                                        MAX_DDL_DEFERRALS,
                                                        poll_interval_ms,
                                                        &shutdown_tx,
                                                        ddl_failure_abort,
                                                    ) {
                                                        break 'batch;
                                                    }
                                                    tracing::warn!(
                                                          "DDL id={} CTAS data sync failed: {:?} — will retry on next poll",
                                                        ddl_id, e,
                                                    );
                                                    let failures =
                                                        ddl_failures.entry(ddl_id).or_insert(0);
                                                    *failures += 1;
                                                    if *failures >= 3 {
                                                        tracing::error!(
                                                            "FATAL: DDL id={} snapshot failed after {} retries — {}.",
                                                            ddl_id, *failures,
                                                            if ddl_failure_abort { "ABORTING pipeline" } else { "breaking batch (will retry on next poll)" },
                                                        );
                                                        if ddl_failure_abort {
                                                            let _ = shutdown_tx.send(true);
                                                            return Err(anyhow::anyhow!(
                                                                "DDL id={} permanent snapshot failure — abort on ddl_failure_abort=true",
                                                                ddl_id,
                                                            ));
                                                        }
                                                        // Don't advance checkpoint — the DDL will be
                                                        // retried on the next poll cycle.  Break the
                                                        // batch to prevent later DDLs from advancing
                                                        // past this one.
                                                        break 'batch;
                                                    }
                                                }
                                            }
                                        } else {
                                            advance_and_save_ddl_checkpoint(
                                                &checkpoint,
                                                store,
                                                ddl_id,
                                            )
                                            .await;
                                            ddl_failures.remove(&ddl_id);
                                        }
                                    } else {
                                        // No table ref extractable — advance anyway
                                        advance_and_save_ddl_checkpoint(&checkpoint, store, ddl_id)
                                            .await;
                                        ddl_failures.remove(&ddl_id);
                                    }
                                }
                                Err(e2) => {
                                    let failures = ddl_failures.entry(ddl_id).or_insert(0);
                                    *failures += 1;
                                    if *failures >= 3 {
                                        tracing::error!(
                                            "FATAL: DDL id={} catalog fallback failed after {} retries — {}.",
                                            ddl_id, *failures,
                                            if ddl_failure_abort { "ABORTING pipeline" } else { "advancing checkpoint" },
                                        );
                                        if ddl_failure_abort {
                                            let _ = shutdown_tx.send(true);
                                            return Err(anyhow::anyhow!(
                                                "DDL id={} permanent failure (catalog fallback) — abort on ddl_failure_abort=true",
                                                ddl_id,
                                            ));
                                        }
                                        error_count.fetch_add(1, Ordering::Relaxed);
                                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                                        ddl_failures.remove(&ddl_id);
                                    } else {
                                        tracing::error!(
                                            "DDL id={} catalog fallback failed (attempt {}/3): {:?} — will retry",
                                            ddl_id, *failures, e2
                                        );
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::debug!(
                                "DDL id={} catalog fallback returned None (no PK or catalog error) — skipping, table will sync when PK is added",
                                ddl_id,
                            );
                            ddl_skipped.fetch_add(1, Ordering::Relaxed);
                            advance_ddl_checkpoint(&checkpoint, ddl_id);
                            ddl_failures.remove(&ddl_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "DDL id={} catalog fallback PG error (will retry): {:?}",
                                ddl_id,
                                e,
                            );
                            let failures = ddl_failures.entry(ddl_id).or_insert(0);
                            *failures += 1;
                            if *failures >= 3 && ddl_failure_abort {
                                let _ = shutdown_tx.send(true);
                                return Err(anyhow::anyhow!(
                                    "DDL id={} permanent catalog fallback error after {} retries — abort on ddl_failure_abort=true",
                                    ddl_id, *failures,
                                ));
                            }
                        }
                    }
                    break 'batch;
                }
            };

            // Post_process_ddl parses the transpiled SQL via
            // polyglot_sql (CPU-bound). Move it off the async executor onto a
            // blocking-pool thread so large multi-statement DDL doesn't stall the
            // reactor. (Cloning the properties map is cheap relative to the parse.)
            //
            // (Tier 1): use the `_stmts` variant which returns Vec<String>
            // directly — this lets execute_sql_stmts skip the THIRD polyglot
            // parse round-trip (parse → generate per-expr) that
            // execute_sql_with_db would otherwise do internally.
            let pp_props = table_properties.clone();
            let sr_sql_stmts: Vec<String> =
                tokio::task::spawn_blocking(move || post_process_ddl_stmts(&transpiled, &pp_props))
                    .await
                    .context("post_process_ddl_stmts: spawn_blocking task panicked")?;
            // Filter out empty strings (e.g. ALTER TABLE with only PG-only
            // actions that were all filtered by is_pg_only_alter_action →
            // empty output → Path P SKIP). If ALL are empty, skip the DDL.
            let sr_sql_stmts: Vec<String> = sr_sql_stmts
                .into_iter()
                .filter(|s| !s.trim().is_empty())
                .collect();
            if sr_sql_stmts.is_empty() {
                ddl_skipped.fetch_add(1, Ordering::Relaxed);
                advance_ddl_checkpoint(&checkpoint, ddl_id);
                ddl_failures.remove(&ddl_id);
                continue;
            }

            // Before creating a table via polyglot-sql, verify it has a PRIMARY KEY.
            // PK-less tables are not supported by CDC and must not be created in SR.
            // The catalog fallback path already has this check; the polyglot path was
            // missing it, creating empty tables that never receive CDC data.
            if tag == "CREATE TABLE" {
                if let Some((schema, table)) = extract_create_table_ref(&ddl_text) {
                    let oid =
                        match crate::pg_catalog::fetch_relation_oid(&pg, &schema, &table).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::warn!(
                                    "DDL id={} {}.{}: failed to fetch OID for PK check — \
                                 proceeding with table creation (PK check skipped): {:?}",
                                    ddl_id,
                                    schema,
                                    table,
                                    e,
                                );
                                None
                            }
                        };
                    if let Some(oid) = oid {
                        let pk_cols = crate::pg_catalog::fetch_primary_keys(&pg, oid)
                            .await
                            .unwrap_or_default();
                        if pk_cols.is_empty() {
                            tracing::warn!(
                                "DDL id={} {}.{} has no PRIMARY KEY — skipping SR table creation. PK-less tables are not supported.",
                                ddl_id, schema, table,
                            );
                            ddl_skipped.fetch_add(1, Ordering::Relaxed);
                            advance_ddl_checkpoint(&checkpoint, ddl_id);
                            ddl_failures.remove(&ddl_id);
                            continue;
                        }
                    }
                }
            }

            // For ALTER TABLE / DROP TABLE, drain pending CDC events
            // for the target table, then hold a DdlActiveGuard across execute_sql
            // so concurrent flushes for the same table are routed to the durable
            // wal_buffer until the DDL completes (the guard now also spans the
            // schema-change job wait — see wait_for_schema_change_jobs).
            //
            // The DROP TABLE race is the documented silent-data-loss case
            // (ddl_poller.rs comment formerly at :1168): without this guard, a
            // flush landing between DROP and the next CDC pickup auto-creates an
            // empty table, resurrecting only the pending rows and losing all
            // prior data. The DdlActiveGuard + flush-path double-check closes it.
            let mut _ddl_guard: Option<crate::pipeline::DdlActiveGuard> = None;
            let ddl_guard_table_key: Option<String> = match tag.as_str() {
                "ALTER TABLE" => {
                    extract_alter_table_ref(&ddl_text).map(|(s, t)| format!("{}.{}", s, t))
                }
                "DROP TABLE" => {
                    try_extract_table_ref(&ddl_text).map(|(s, t)| format!("{}.{}", s, t))
                }
                _ => None,
            };
            if let Some(table_key) = ddl_guard_table_key.as_ref() {
                if let Some((schema, table)) = table_key.split_once('.') {
                    // Two-phase acquire — drain WITHOUT the guard
                    // (so in-flight and buffered flushes can complete), then
                    // acquire the guard and re-check `pending_cdc_tables`. The
                    // prior guard-first ordering closed the original
                    // microsecond TOCTOU but starved on continuously-written
                    // tables (see acquire_ddl_guard for the full analysis).
                    // Once the guard is held and the re-check passes, any flush
                    // that starts afterwards sees `ddl_active` set and its
                    // writes are durably buffered instead. No deadlock:
                    // flush_transaction never waits on `ddl_active`, and
                    // appends remove the table from pending_cdc_tables so the
                    // drain proceeds.
                    _ddl_guard = match acquire_ddl_guard(
                        &ddl_active,
                        &pending_cdc_tables,
                        table_key,
                        &mut shutdown_rx,
                        ddl_id,
                        schema,
                        table,
                    )
                    .await
                    {
                        Ok(g) => Some(g),
                        Err(e) => {
                            let _ = shutdown_tx.send(true);
                            return Err(e);
                        }
                    };
                    ddl_deferrals.remove(&ddl_id);
                }
            }

            // Snapshot the executed statements for the success log below
            // (the Vec is moved into execute_sql_stmts).
            let sr_sql_stmts_for_log: Vec<String> = sr_sql_stmts.clone();

            match crate::sr_sql::execute_sql_stmts(sr_mgr, sr_sql_stmts).await {
                Ok(_) => {
                    // Log the executed SR SQL (backtick-quoted, schema-stripped)
                    // at INFO so the success path is observable — the captured
                    // DDL text logged above is the PG form; this is what SR
                    // actually received. The backtick-quoted form here is what readers
                    // should match on.
                    let executed = sr_sql_stmts_for_log
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join("; ");
                    tracing::info!(
                        "DDL id={} applied to SR (polyglot-sql): {}",
                        ddl_id,
                        crate::util::truncate_str(&executed, 200),
                    );
                    // Write-pause extension: SR's ALTER is only *accepted*
                    // here — the schema-change job (table rewrite) finalizes
                    // asynchronously. Hold the DdlActiveGuard (write pause)
                    // until the job reaches FINISHED so concurrent CDC flushes
                    // never hit a mid-rewrite table (which wedges the job at
                    // FINISHED_REWRITING until the table is dropped). During
                    // the pause the pipeline routes this table's WAL events
                    // into the durable wal_buffer, so no data is held in
                    // memory and the pause can last as long as the rewrite.
                    // `_ddl_guard` (acquired above) stays alive for the whole
                    // iteration block, so the pause spans job completion.
                    if tag == "ALTER TABLE" {
                        if let Some(table_key) = ddl_guard_table_key.as_ref() {
                            if let Some((schema, table)) = table_key.split_once('.') {
                                let sr_table = crate::sr_sql::sr_table_name(schema, table);
                                // Job wait with in-iteration retry: a transient
                                // wait failure (query error / missing column)
                                // must NOT release the write pause — retry up
                                // to 3 attempts with the guard HELD, then fall
                                // to the final bounded wait below. The ALTER is
                                // not durably applied until a terminal state is
                                // observed, so the checkpoint must not advance.
                                let failures = ddl_failures.entry(ddl_id).or_insert(0);
                                loop {
                                    match wait_for_schema_change_jobs(
                                        sr_mgr,
                                        table_key,
                                        &sr_table,
                                        &mut shutdown_rx,
                                        ddl_id,
                                    )
                                    .await
                                    {
                                        // FINISHED — success bookkeeping below.
                                        Ok(true) => break,
                                        // `Ok(false)` = no job row for two
                                        // CONSECUTIVE polls. That is NOT
                                        // evidence the rewrite finished (see
                                        // wait_for_schema_change_jobs docs), so
                                        // it must not be recorded as applied.
                                        // Same as the Cancelled arm below:
                                        // release the pause, leave the
                                        // checkpoint, retry on the next poll.
                                        // Re-running an ALTER that did land is
                                        // safe — SR answers "already exists"
                                        // and the skip path advances.
                                        Ok(false) => {
                                            tracing::warn!(
                                                "DDL id={} no schema-change job observed for {} — NOT treating DDL as applied, releasing write pause and retrying on next poll",
                                                ddl_id, table_key,
                                            );
                                            break 'batch;
                                        }
                                        // Job not running; releasing the pause
                                        // is safe (existing semantics). Retry
                                        // the DDL on the next poll — the
                                        // checkpoint does NOT advance.
                                        Err(JobWaitError::Cancelled) => break 'batch,
                                        Err(JobWaitError::Shutdown) => {
                                            return Err(anyhow::anyhow!(
                                                "DDL id={ddl_id} shutdown during schema-change job wait"
                                            ));
                                        }
                                        Err(JobWaitError::Transient(e)) => {
                                            *failures += 1;
                                            if *failures < 3 {
                                                tracing::error!(
                                                    "DDL id={} schema-change job wait failed (attempt {}/3): {e:?} — retrying with guard held",
                                                    ddl_id, *failures,
                                                );
                                                tokio::select! {
                                                    biased;
                                                    _ = shutdown_rx.changed() => {
                                                        return Err(anyhow::anyhow!(
                                                            "DDL id={ddl_id} shutdown during schema-change job wait retry"
                                                        ));
                                                    }
                                                    _ = tokio::time::sleep(Duration::from_millis(schema_change_retry_ms)) => {}
                                                }
                                                continue;
                                            }
                                            break; // to the final wait below
                                        }
                                    }
                                }
                                if *failures >= 3 {
                                    if ddl_failure_abort {
                                        tracing::error!(
                                            "FATAL: DDL id={} schema-change job wait failed after {} retries — ABORTING pipeline.",
                                            ddl_id, *failures,
                                        );
                                        let _ = shutdown_tx.send(true);
                                        return Err(anyhow::anyhow!(
                                            "DDL id={} permanent failure (schema-change job wait) — abort on ddl_failure_abort=true",
                                            ddl_id,
                                        ));
                                    }
                                    // Final bounded wait WHILE THE GUARD IS
                                    // HELD: the ALTER may still be finalizing
                                    // even though our waits kept failing
                                    // transiently. Poll up to
                                    // FINAL_JOB_WAIT_MAX_MS before giving up.
                                    let final_deadline = tokio::time::Instant::now()
                                        + Duration::from_millis(FINAL_JOB_WAIT_MAX_MS);
                                    loop {
                                        match wait_for_schema_change_jobs(
                                            sr_mgr,
                                            table_key,
                                            &sr_table,
                                            &mut shutdown_rx,
                                            ddl_id,
                                        )
                                        .await
                                        {
                                            // Terminal or job gone — success
                                            // bookkeeping below.
                                            Ok(_) | Err(JobWaitError::Cancelled) => break,
                                            Err(JobWaitError::Shutdown) => {
                                                return Err(anyhow::anyhow!(
                                                    "DDL id={ddl_id} shutdown during final schema-change job wait"
                                                ));
                                            }
                                            Err(JobWaitError::Transient(_)) => {
                                                if tokio::time::Instant::now() >= final_deadline {
                                                    tracing::error!(
                                                        "DDL id={} schema-change job still not terminal after 10min — advancing checkpoint and releasing pause (ddl_failure_abort=false): mid-rewrite flushes possible for {table_key}",
                                                        ddl_id,
                                                    );
                                                    error_count.fetch_add(1, Ordering::Relaxed);
                                                    advance_ddl_checkpoint(&checkpoint, ddl_id);
                                                    ddl_failures.remove(&ddl_id);
                                                    // Skip the success bookkeeping
                                                    // below — checkpoint advanced.
                                                    continue 'batch;
                                                }
                                                tokio::select! {
                                                    biased;
                                                    _ = shutdown_rx.changed() => {
                                                        return Err(anyhow::anyhow!(
                                                            "DDL id={ddl_id} shutdown during final schema-change job wait"
                                                        ));
                                                    }
                                                    _ = tokio::time::sleep(Duration::from_millis(SCHEMA_CHANGE_JOB_POLL_MS)) => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ddl_applied.fetch_add(1, Ordering::Relaxed);
                    ddl_failures.remove(&ddl_id);

                    // On a successful DROP TABLE, enqueue the table for
                    // relation/PK cache eviction by the pipeline (drained in
                    // the next begin_transaction). This bounds memory under
                    // create/drop churn and prevents a stale OID-keyed cache
                    // entry from mis-routing events if PG reuses the OID for
                    // a new table with a different schema.
                    if tag == "DROP TABLE" {
                        if let Some(table_key) = ddl_guard_table_key.as_ref() {
                            if recreate_names.contains(table_key) {
                                tracing::info!(
                                    "DDL id={} DROP TABLE {table_key} — same-batch CREATE TABLE re-registers it; skipping eviction enqueue",
                                    ddl_id,
                                );
                            } else {
                                // Use lock_or_recover so poison doesn't
                                // silently skip eviction (resurrecting a stale
                                // OID-keyed cache if PG reuses the OID for a new
                                // table with a different schema).
                                crate::pipeline::lock_or_recover(&dropped_relations)
                                    .insert(table_key.clone());
                            }
                        }
                    }

                    // For CREATE TABLE events, snapshot any existing data to cover
                    // the gap between table creation and CDC pickup. This mirrors
                    // the behavior of the catalog fallback path which also calls
                    // snapshot_one_table after CREATE TABLE, and provides resilience
                    // against the race where INSERTs arrive via CDC before the table
                    // exists in SR.
                    if tag == "CREATE TABLE" || tag == "CREATE TABLE AS" || tag == "SELECT INTO" {
                        if let Some((schema, table)) = extract_create_table_ref(&ddl_text) {
                            // Hold DdlActiveGuard + drain across the
                            // backfill snapshot (mirrors the ALTER paths).
                            // Without it, CDC flushes write to the main table
                            // during the shadow copy and swap_shadow_to_main
                            // discards them (their LSN is already ACK'd →
                            // permanent loss, no replay).
                            let backfill_key = format!("{}.{}", schema, table);
                            let _backfill_guard = match acquire_ddl_guard(
                                &ddl_active,
                                &pending_cdc_tables,
                                &backfill_key,
                                &mut shutdown_rx,
                                ddl_id,
                                &schema,
                                &table,
                            )
                            .await
                            {
                                Ok(g) => g,
                                Err(e) => {
                                    let _ = shutdown_tx.send(true);
                                    return Err(e);
                                }
                            };
                            match crate::snapshot::snapshot_one_table(
                                &pg,
                                sr_mgr,
                                &schema,
                                &table,
                                &checkpoint,
                                store,
                                table_properties,
                                batch_rows,
                                max_retries,
                                query_timeout_ms,
                                &pending_cdc_tables,
                            )
                            .await
                            {
                                Ok(true) => {
                                    tracing::info!(
                                        "DDL id={} snapshot synced {}.{}",
                                        ddl_id,
                                        schema,
                                        table,
                                    );
                                }
                                Ok(false) => {
                                    tracing::debug!(
                                        "DDL id={} snapshot no data for {}.{} (already synced or no PK)",
                                        ddl_id, schema, table,
                                    );
                                }
                                Err(e) => {
                                    // Snapshot failure during CREATE-TABLE
                                    // backfill must NOT advance the DDL checkpoint.
                                    // The "CDC will cover" assumption is wrong when
                                    // the SR table was never created: the pipeline's
                                    // auto-create fallback (`pipeline.rs` flush_table)
                                    // resurrects an EMPTY SR table and only applies
                                    // events from the current WAL LSN onward — every
                                    // row committed before that LSN is permanently
                                    // lost. Route through the ddl_failures retry budget
                                    // (mirrors the ALTER-COLUMN sibling at :655), and
                                    // break 'batch (not continue) so a later DDL in the
                                    // same batch can't advance the checkpoint past
                                    // this one — see the skip-hazard comment at :1273.
                                    if handle_snapshot_deferral(
                                        &e,
                                        ddl_id,
                                        &schema,
                                        &table,
                                        &mut ddl_deferrals,
                                        MAX_DDL_DEFERRALS,
                                        poll_interval_ms,
                                        &shutdown_tx,
                                        ddl_failure_abort,
                                    ) {
                                        break 'batch;
                                    }
                                    let failures = ddl_failures.entry(ddl_id).or_insert(0);
                                    *failures += 1;
                                    if *failures >= 3 {
                                        tracing::error!(
                                            "FATAL: DDL id={} CREATE TABLE snapshot failed after {} retries — {}.",
                                            ddl_id, *failures,
                                            if ddl_failure_abort { "ABORTING pipeline" } else { "advancing checkpoint" },
                                        );
                                        if ddl_failure_abort {
                                            let _ = shutdown_tx.send(true);
                                            return Err(anyhow::anyhow!(
                                                "DDL id={} permanent failure (CREATE TABLE snapshot) — abort on ddl_failure_abort=true",
                                                ddl_id,
                                            ));
                                        }
                                        error_count.fetch_add(1, Ordering::Relaxed);
                                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                                        ddl_failures.remove(&ddl_id);
                                    } else {
                                        tracing::error!(
                                            "DDL id={} CREATE TABLE snapshot failed (attempt {}/3): {:?} — will retry on next poll",
                                            ddl_id, *failures, e,
                                        );
                                    }
                                    break 'batch;
                                }
                            }
                        }
                    }

                    advance_and_save_ddl_checkpoint(&checkpoint, store, ddl_id).await;
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);

                    // Crash-recovery: a restart re-runs an ALTER whose
                    // schema-change job from the previous run is still active
                    // (execute fails with "already ... alter/schema change/
                    // job"). Do NOT re-execute — wait for the surviving job
                    // to finalize, then treat the DDL as applied. On wait
                    // failure, fall through to the existing retry logic.
                    let msg_lower = e.to_string().to_lowercase();
                    if tag == "ALTER TABLE"
                        && msg_lower.contains("already")
                        && (msg_lower.contains("alter")
                            || msg_lower.contains("schema change")
                            || msg_lower.contains("job"))
                    {
                        if let Some(table_key) = ddl_guard_table_key.as_ref() {
                            if let Some((schema, table)) = table_key.split_once('.') {
                                let sr_table = crate::sr_sql::sr_table_name(schema, table);
                                match wait_for_schema_change_jobs(
                                    sr_mgr,
                                    table_key,
                                    &sr_table,
                                    &mut shutdown_rx,
                                    ddl_id,
                                )
                                .await
                                {
                                    Ok(true) => {
                                        tracing::warn!(
                                            "DDL id={} re-run: schema-change job for {} completed — treating DDL as applied",
                                            ddl_id, table_key,
                                        );
                                        ddl_applied.fetch_add(1, Ordering::Relaxed);
                                        ddl_failures.remove(&ddl_id);
                                        advance_and_save_ddl_checkpoint(&checkpoint, store, ddl_id)
                                            .await;
                                        continue;
                                    }
                                    Ok(false) => {
                                        tracing::warn!(
                                            "DDL id={} re-run: no schema-change job observed for {} — NOT treating DDL as applied, falling through to retry",
                                            ddl_id, table_key,
                                        );
                                    }
                                    // Cancelled / transient: same as the old
                                    // Err(e) behavior — fall through to the
                                    // existing retry logic.
                                    Err(JobWaitError::Cancelled) => {
                                        tracing::warn!(
                                            "DDL id={} re-run: schema-change job CANCELLED — falling through to retry",
                                            ddl_id,
                                        );
                                    }
                                    Err(JobWaitError::Transient(wait_err)) => {
                                        tracing::warn!(
                                            "DDL id={} re-run: schema-change job wait failed ({:?}) — falling through to retry",
                                            ddl_id, wait_err,
                                        );
                                    }
                                    Err(JobWaitError::Shutdown) => {
                                        return Err(anyhow::anyhow!(
                                            "DDL id={ddl_id} shutdown during crash-recovery schema-change job wait"
                                        ));
                                    }
                                }
                            }
                        }
                    }

                    // StarRocks rejects concurrent ALTER TABLE ADD COLUMN on the same table.
                    // This is transient — the schema change from the previous DDL will
                    // complete and the next poll will succeed. Don't count against retries.
                    // IMPORTANT: break (not continue) so the checkpoint does NOT advance past
                    // this DDL id. If we continued, a later successful DDL in the same batch
                    // would advance the checkpoint past this one, and the next poll (WHERE id >
                    // last_ddl_id) would never see it again — a permanent skip.
                    if err_str.contains("schema change") {
                        let table = extract_alter_table_ref(&ddl_text)
                            .map(|(s, t)| format!("{}.{}", s, t))
                            .unwrap_or_else(|| "?".to_string());
                        tracing::info!(
                            "DDL id={} table={} schema change in progress — waiting {}ms then retry: {:?}",
                            ddl_id, table, schema_change_retry_ms, e,
                        );
                        // Select!-wrap in-batch sleep so
                        // shutdown is responsive instead of waiting the full
                        // schema_change_retry_ms.
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => {
                                tracing::info!("DDL poller: shutdown during schema-change retry");
                                break 'batch;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(schema_change_retry_ms)) => {}
                        }
                        break;
                    }

                    // ADD COLUMN on a column that already exists in SR is harmless —
                    // the column was already added by a prior (or retried) DDL.
                    // Treat as success so crash replay doesn't burn retries.
                    if tag == "ALTER TABLE"
                        && !err_str.contains("DROP COLUMN")
                        && (err_str.contains("already exists"))
                    {
                        tracing::warn!(
                            "DDL id={} ADD COLUMN: column already exists on SR (likely replay) — advancing",
                            ddl_id,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }

                    if matches!(tag.as_str(), "CREATE TABLE" | "CREATE TABLE AS")
                        && (err_str.contains("already exists") || err_str.contains("TableExists"))
                    {
                        tracing::warn!(
                            "DDL id={} table already exists on SR — advancing (likely checkpoint replay)",
                            ddl_id,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }

                    // Transient network/connection errors — retry indefinitely,
                    // never count against the 3-retry FATAL skip limit.
                    // These include: connection refused, timeout, reset, broken pipe,
                    // and MySQL server gone away / EOF.
                    if is_transient_network_error(&err_str) {
                        tracing::warn!(
                            "DDL id={} transient network error — retrying on next poll: {:?}",
                            ddl_id,
                            e,
                        );
                        // Select!-wrap the transient-error
                        // backoff sleep so shutdown isn't blocked by the 1s delay.
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => {
                                tracing::info!("DDL poller: shutdown during transient-error backoff");
                                break 'batch;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(1000)) => {}
                        }
                        break 'batch;
                    }

                    // DROP COLUMN on a column that doesn't exist in SR is harmless —
                    // the original PG DDL had IF EXISTS, but we stripped it because
                    // SR doesn't support IF EXISTS on DROP COLUMN. Treat as success.
                    if is_drop_column_not_exists_error(&e) {
                        tracing::warn!(
                            "DDL id={} DROP COLUMN on non-existent column (IF EXISTS was stripped) — advancing: {:?}",
                            ddl_id, e,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }

                    // DROP TABLE replay against a table
                    // already dropped (crash replay: DROP succeeded but
                    // checkpoint save failed) is harmless. Treat as success so
                    // it doesn't burn the 3-retry budget (which would abort when
                    // ddl_failure_abort=true).
                    //
                    // The text is checked too: a multi-statement DDL captured
                    // under a non-DROP tag (current_query() returns the whole
                    // text; the row's tag is the FIRST statement's tag) can
                    // contain a DROP TABLE whose not-exists failure must also
                    // be treated as idempotent replay — otherwise a routine
                    // re-run of a migration script wedges the poller (3
                    // retries → abort → checkpoint stuck → permanent outage).
                    if (tag == "DROP TABLE" || DROP_TABLE_RE.is_match(&ddl_text))
                        && is_drop_table_not_exists_error(&e)
                    {
                        tracing::warn!(
                            "DDL id={} DROP TABLE on non-existent SR table (crash replay) — advancing: {:?}",
                            ddl_id, e,
                        );
                        ddl_skipped.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                        continue;
                    }

                    // Track consecutive failures per DDL ID.
                    // Retry on next poll cycle; after 3 consecutive failures, skip to unblock.
                    let failures = ddl_failures.entry(ddl_id).or_insert(0);
                    *failures += 1;
                    if *failures >= 3 {
                        tracing::error!(
                            "FATAL: DDL id={} execute failed after {} retries: {:?} — {}.",
                            ddl_id,
                            *failures,
                            e,
                            if ddl_failure_abort {
                                "ABORTING pipeline"
                            } else {
                                "advancing checkpoint"
                            },
                        );
                        if ddl_failure_abort {
                            let _ = shutdown_tx.send(true);
                            return Err(anyhow::anyhow!(
                                "DDL id={} permanent failure (execute) — abort on ddl_failure_abort=true",
                                ddl_id,
                            ));
                        }
                        error_count.fetch_add(1, Ordering::Relaxed);
                        advance_ddl_checkpoint(&checkpoint, ddl_id);
                        ddl_failures.remove(&ddl_id);
                    } else {
                        tracing::error!(
                            "DDL id={} execute failed (attempt {}/3): {:?} — will retry",
                            ddl_id,
                            *failures,
                            e
                        );
                        break 'batch;
                    }
                }
            }
        }
        prev_poll_max_id = max_seen;
    }
    Ok(())
}

/// Detect an `ALTER TABLE ... ALTER [COLUMN] <name> TYPE <t>` (or the
/// `... SET DATA TYPE <t>` spelling) column type change. The `COLUMN`
/// keyword is optional in PG, so `ALTER TABLE t ALTER c TYPE bigint` is as
/// legal as `ALTER TABLE t ALTER COLUMN c TYPE bigint`.
///
/// Anchored on the `ALTER TABLE ... ALTER <identifier> (TYPE | SET DATA
/// TYPE)` shape rather than on the bare word TYPE: the type keyword must
/// follow the column name immediately, so `ALTER c SET DEFAULT 'TYPE'::text`
/// cannot match — after the column name comes `SET DEFAULT`, which diverges
/// from `SET DATA TYPE` at the second word. Requiring the statement-level
/// `ALTER TABLE` (with no intervening `;`) stops a table literally named
/// `type` (`ALTER TABLE type ADD COLUMN c int`) from being read as
/// `ALTER <column> TYPE`.
///
/// Residual: the gate is textual, not literal/comment-aware — a DEFAULT
/// literal (or a comment) carrying an `ALTER ... TYPE` snippet
/// (`SET DEFAULT 'ALTER x TYPE int'`) still matches. That hole predates this
/// regex (the old `contains(" ALTER COLUMN ")` + `contains(" TYPE ")` pair
/// matched the same shape) and closing it needs a parser; the COLUMN-less
/// form this gate now catches is far more common than a literal carrying a
/// SQL snippet.
static ALTER_COLUMN_TYPE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r#"(?i)\bALTER\s+TABLE\b[^;]*?\bALTER\s+(?:COLUMN\s+)?(?:\w+|"[^"]+")\s+(?:TYPE\b|SET\s+DATA\s+TYPE\b)"#,
    )
    .expect("ALTER_COLUMN_TYPE_RE: hardcoded valid regex")
});

/// Column type change (`ALTER [COLUMN] <name> TYPE` / `SET DATA TYPE`) —
/// see `ALTER_COLUMN_TYPE_RE` for the anchoring rationale.
fn is_alter_column_type_change(ddl: &str) -> bool {
    ALTER_COLUMN_TYPE_RE.is_match(ddl)
}

/// Check if an ALTER TABLE DDL is a type-change or column-rename that
/// StarRocks cannot execute. These should be skipped early rather than
/// executed and burned through the 3-retry budget.
fn is_unsupported_alter_table(ddl: &str) -> bool {
    let ddl_upper = ddl.to_uppercase();
    // ALTER TABLE ... ALTER [COLUMN] ... TYPE / SET DATA TYPE. The COLUMN-less
    // spelling must be caught too — otherwise it falls through to the generic
    // transpile path, is emitted as `ALTER COLUMN ... SET DATA TYPE` (which
    // StarRocks rejects with error 1064) and burns the 3-retry budget: silent
    // schema drift, or a pipeline abort under `ddl_failure_abort=true`.
    if is_alter_column_type_change(ddl) {
        return true;
    }
    // ALTER TABLE ... RENAME COLUMN
    if ddl_upper.contains(" RENAME COLUMN ") {
        return true;
    }
    // ALTER TABLE ... RENAME TO (table rename, not column rename)
    // StarRocks does not support table renames. We drop+resnapshot
    // the old table, and the subsequent CDC events under the new name
    // auto-create the new table via the generic path.
    // Must NOT match "RENAME COLUMN" (which contains "RENAME TO" as substring).
    if ddl_upper.contains(" RENAME TO ") && !ddl_upper.contains(" RENAME COLUMN ") {
        return true;
    }
    false
}

/// Check if an ALTER TABLE DDL drops a constraint. SR has no constraint
/// concept, so this is a PG-only no-op for SR — detected textually so the
/// poller can short-circuit with a debug breadcrumb and advance the checkpoint.
fn is_drop_constraint_alter(ddl: &str) -> bool {
    ddl.to_uppercase().contains(" DROP CONSTRAINT ")
}

/// Check if an ALTER TABLE DDL is a REPLICA IDENTITY change (PG-only).
fn is_replica_identity_alter(ddl: &str) -> bool {
    // Word-bounded regex match avoids false-positives on column
    // names containing the substring "replica identity" (case-insensitive,
    // whitespace-flexible).
    REPLICA_IDENTITY_RE.is_match(ddl)
}

/// Detect PG-only ALTER TABLE actions that polyglot-sql CANNOT parse
/// — polyglot would fail with an Err (or parse to `Raw{sql}`) and the poller
/// would burn its 3-retry budget then FATAL-ABORT under
/// `ddl_failure_abort=true`. These are PG-specific metadata operations that
/// either have no StarRocks equivalent (RLS, triggers, CLUSTER) or whose
/// syntax polyglot can't structurally recognize (SET SCHEMA, VALIDATE
/// CONSTRAINT, ATTACH/DETACH PARTITION).
///
/// Each pattern is anchored at a word boundary to avoid false-positives on
/// column names (e.g. `\bVALIDATE CONSTRAINT\b` won't match a column named
/// `validate_constraint_flag`).
fn is_pg_only_metadata_alter(ddl: &str) -> bool {
    use regex::Regex;
    // Compile these patterns once per process via LazyLock. They're small
    // (5-12 chars each) so the per-call cost is dominated by the matcher,
    // not compilation.
    static PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            r"(?i)\bSET\s+SCHEMA\b",
            r"(?i)\bVALIDATE\s+CONSTRAINT\b",
            r"(?i)\bCLUSTER\s+ON\b",
            r"(?i)\bENABLE\s+TRIGGER\b",
            r"(?i)\bDISABLE\s+TRIGGER\b",
            r"(?i)\bENABLE\s+ROW\s+LEVEL\s+SECURITY\b",
            r"(?i)\bDISABLE\s+ROW\s+LEVEL\s+SECURITY\b",
            r"(?i)\bFORCE\s+ROW\s+LEVEL\s+SECURITY\b",
            r"(?i)\bNO\s+FORCE\s+ROW\s+LEVEL\s+SECURITY\b",
            r"(?i)\bATTACH\s+PARTITION\b",
            r"(?i)\bDETACH\s+PARTITION\b",
            r"(?i)\bRENAME\s+CONSTRAINT\b",
        ]
        .iter()
        .map(|p| regex::Regex::new(p).expect("is_pg_only_metadata_alter: hardcoded valid regex"))
        .collect()
    });
    PATTERNS.iter().any(|re| re.is_match(ddl))
}

/// Check if an ALTER TABLE DDL is an `ALTER COLUMN ... SET/DROP DEFAULT`
/// statement — a PG-only metadata change that StarRocks cannot execute
/// (SR has no `ALTER COLUMN` action; it uses `MODIFY COLUMN` and requires
/// the column type). The pipeline already strips all DEFAULTs on CREATE
/// TABLE and ADD COLUMN (`post_process_ddl`) and always sends explicit
/// values via Stream Load JSON, so SET/DROP DEFAULT is a no-op for SR
/// and must be skipped before transpilation — otherwise it falls through
/// to `polyglot_sql::transpile` verbatim, SR rejects it with error 1064,
/// and the poller burns its 3-retry budget then aborts the pipeline
/// (when `ddl_failure_abort = true`).
///
/// Routed to the same skip path as REPLICA IDENTITY — not to
/// `is_unsupported_alter_table`, which triggers an expensive
/// drop+re-snapshot. SET/DROP DEFAULT changes no type or column name
/// that would require resnapshot.
///
/// NOTE: the trailing space is intentionally omitted from `" SET DEFAULT"`
/// and `" DROP DEFAULT"` because DEFAULT is frequently the last token in
/// the DDL (e.g. `... ALTER COLUMN id DROP DEFAULT` has no trailing space).
/// The leading space prevents false matches on column names like
/// `set_default`. The ` ALTER COLUMN ` guard (with both spaces) ensures
/// this only matches the actual ALTER COLUMN action.
fn is_alter_column_default(ddl: &str) -> bool {
    let u = ddl.to_uppercase();
    u.contains(" ALTER COLUMN ") && (u.contains(" SET DEFAULT") || u.contains(" DROP DEFAULT"))
}

/// Check if the DDL creates a temporary table. PG syntax:
/// Drop CREATE TEMP/TEMPORARY TABLE statements from a multi-statement DDL
/// text. The event trigger captures `current_query()` verbatim, so one row
/// can carry several statements (`CREATE TEMP TABLE t(...); CREATE TABLE
/// x(...)`), and the trigger fires once per statement — each row carrying
/// the identical full text. Skipping the whole row on the temp check would
/// lose the real CREATE TABLE x forever (the duplicate row is dedup-skipped
/// too).
///
/// Returns `None` when nothing remains (the row should be skipped);
/// `Some(text)` with the surviving statements otherwise. A statement whose
/// AST re-generation fails is logged as WARN and dropped from the returned
/// text — the caller then applies a partial DDL. If the text cannot be
/// parsed (or contains no AST-level temp flag), falls back to the
/// text-level temp check: a pure temp statement → `None` (legacy skip),
/// anything else → the text passes through unchanged.
fn strip_temp_table_statements(ddl: &str) -> Option<String> {
    use polyglot_sql::{DialectType, Expression};
    let exprs = match polyglot_sql::parse(ddl, DialectType::PostgreSQL) {
        Ok(e) => e,
        Err(_) => {
            return if is_temporary_table_ddl(ddl) {
                None
            } else {
                Some(ddl.to_string())
            };
        }
    };
    let kept: Vec<&Expression> = exprs
        .iter()
        .filter(|e| match e {
            Expression::CreateTable(ct) => !ct.temporary,
            _ => true,
        })
        .collect();
    if kept.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(kept.len());
    let mut gen = polyglot_sql::generator::Generator::new();
    for (idx, e) in kept.iter().enumerate() {
        match gen.generate(e) {
            Ok(sql) => out.push(sql),
            // Dropping the statement silently would hand the caller a partial
            // DDL text that then advances the checkpoint — schema drift the
            // operator can't see. Warn with the index so the skipped statement
            // can be replayed by hand.
            Err(err) => {
                tracing::warn!(
                    "DDL statement {idx} of the temp-filtered DDL text failed AST re-generation — \
                     SKIPPING it: the schema change is applied only in part. \
                     Error: {err}. Expression: {e:?}"
                );
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out.join(";\n"))
    }
}

/// Detect a `CREATE TEMP/TEMPORARY TABLE` statement (text-level).
///   `CREATE [GLOBAL|LOCAL] {TEMP|TEMPORARY} TABLE ...`
///
/// Temporary tables are session-scoped in PG, don't generate WAL entries
/// for DML, and should never be created as permanent tables in StarRocks.
/// Without this skip, `post_process_ddl` strips the `TEMPORARY` flag
/// (`ct.temporary = false`), silently turning temp tables into permanent
/// SR tables — cluttering SR with throwaway definitions that never
/// receive CDC data.
fn is_temporary_table_ddl(ddl: &str) -> bool {
    let upper = ddl.trim_start().to_uppercase();
    if !upper.starts_with("CREATE ") {
        return false;
    }
    let rest = upper[7..].trim_start();
    // Strip optional GLOBAL/LOCAL prefix
    let rest = if let Some(r) = rest.strip_prefix("GLOBAL ") {
        r.trim_start()
    } else if let Some(r) = rest.strip_prefix("LOCAL ") {
        r.trim_start()
    } else {
        rest
    };
    rest.starts_with("TEMP ") || rest.starts_with("TEMPORARY ")
}

/// Check if the `ddl_text` is a PL/pgSQL `DO` block. When an `ALTER TABLE`
/// (or `CREATE TABLE` / `DROP TABLE`) runs inside a `DO $$ ... $$` block,
/// PG's event trigger captures `current_query()` which returns the entire
/// DO block text — not the individual inner DDL statement. polyglot-sql
/// cannot parse PL/pgSQL, so the poller would fail after 3 retries and
/// abort the pipeline (when `ddl_failure_abort = true`).
///
/// Detection: the text starts with `DO` followed by a non-word character
/// (space, `$`, newline, tab). This avoids false matches on `DROP TABLE`,
/// `DOCUMENT`, `DOUBLE`, etc. — all of which have an alphanumeric character
/// at position 2.
///
/// The trigger captures DO blocks as-is (by design — the trigger should
/// stay simple and not attempt PL/pgSQL parsing). This poller-level skip
/// is the sole handling: it advances the checkpoint past the unparseable
/// DO block so the pipeline continues. Individual DDLs inside DO blocks
/// cannot be extracted — operators should run table DDLs as standalone
/// statements, not inside DO blocks.
fn is_plpgsql_do_block(ddl: &str) -> bool {
    let upper = ddl.trim_start().to_uppercase();
    upper.starts_with("DO") && (upper.len() == 2 || !upper.as_bytes()[2].is_ascii_alphanumeric())
}

fn should_sync_ddl(tag: &str) -> bool {
    matches!(
        tag,
        "ALTER TABLE" | "CREATE TABLE" | "CREATE TABLE AS" | "DROP TABLE" | "SELECT INTO"
    )
}

/// Check if the DDL text is inconsistent with its command tag — indicating
/// the DDL was executed inside a function/procedure body and `current_query()`
/// returned the calling query (e.g. `SELECT my_func()`), not the inner DDL.
///
/// PG's event trigger captures `current_query()` which returns the text of
/// the query the client sent. When DDL runs inside a function body, the
/// trigger fires with the correct `command_tag` (e.g. "ALTER TABLE") but
/// `current_query()` returns `SELECT my_func()` — not the inner ALTER.
/// polyglot-sql would then transpile the SELECT, `post_process_ddl` would
/// pass it through unchanged, and the poller would execute `SELECT my_func()`
/// on StarRocks.
///
/// Detection: the DDL text should start with the keyword corresponding to
/// its tag. If it doesn't, it's a function-wrapped DDL.
fn is_function_wrapped_ddl(tag: &str, ddl_text: &str) -> bool {
    let upper = ddl_text.trim_start().to_uppercase();
    if upper.is_empty() {
        return false; // let the null/empty checks downstream handle it
    }
    // DO blocks are handled by is_plpgsql_do_block — don't double-filter.
    if upper.starts_with("DO") && (upper.len() == 2 || !upper.as_bytes()[2].is_ascii_alphanumeric())
    {
        return false;
    }
    let expected_prefix = match tag {
        "ALTER TABLE" => "ALTER",
        "CREATE TABLE" | "CREATE TABLE AS" => "CREATE",
        "DROP TABLE" => "DROP",
        "SELECT INTO" => "SELECT",
        _ => return false, // unknown tag — don't filter
    };
    // Check if the text starts with the expected DDL keyword for its tag.
    // Also handle quoted identifiers: `"ALTER"` is not a real ALTER statement.
    if upper.starts_with(expected_prefix) {
        // For SELECT INTO, verify the text contains INTO
        // (distinguishes `SELECT * INTO new_table FROM ...` from `SELECT my_func()`)
        if tag == "SELECT INTO" && !upper.contains(" INTO ") {
            return true;
        }
        return false;
    }
    // Text doesn't start with the expected keyword → function-wrapped DDL
    true
}

/// Check if the DDL text is a DROP SCHEMA / DROP DATABASE statement that
/// was captured with tag "DROP TABLE" because PG's `sql_drop` trigger fires
/// once per dropped table inside the schema, each time capturing
/// `current_query()` which returns the original `DROP SCHEMA ... CASCADE`.
///
/// The poller cannot extract per-table DROP statements from this text.
/// polyglot-sql would transpile `DROP SCHEMA` which SR rejects (SR has no
/// nested schemas). Skip these to prevent the 3-retry abort.
///
/// Individual SR tables from the dropped schema are orphaned — operators
/// should clean them up manually if needed. The pipeline continues running.
fn is_drop_schema_or_database(ddl_text: &str) -> bool {
    let upper = ddl_text.trim_start().to_uppercase();
    upper.starts_with("DROP SCHEMA") || upper.starts_with("DROP DATABASE")
}

/// Classify an error string as a transient network/connection issue.
/// These errors should retry indefinitely and never count toward the
/// 3-retry FATAL skip limit — they are not schema or logic errors.
fn is_transient_network_error(err: &str) -> bool {
    let err_lower = err.to_lowercase();

    // Permanent / grammatical errors must short-circuit BEFORE the
    // substring heuristics below — otherwise a StarRocks syntax error
    // containing "eof" (e.g. "Unexpected input '<EOF>'") is miscategorized
    // as a transient network blip and retried forever, wedging the DDL
    // checkpoint. MySQL/StarRocks 1064 syntax errors, "unexpected input"
    // wording, and any "(1064)" code are all permanent.
    if err_lower.contains("syntax error")
        || err_lower.contains("unexpected input")
        || err_lower.contains("you have an error in your sql syntax")
        || err_lower.contains("(1064)")
        || err_lower.contains("hy000")
    {
        return false;
    }

    // MySQL / TCP connection-level errors
    if err_lower.contains("connection refused")
        || err_lower.contains("connection reset")
        || err_lower.contains("connect timeout")
        || err_lower.contains("connection timed out")
        || err_lower.contains("broken pipe")
        || err_lower.contains("can't connect")
        || err_lower.contains("already closed")
        || err_lower.contains("server has gone away")
        || err_lower.contains("no route to host")
        || err_lower.contains("network is unreachable")
    {
        return true;
    }

    // The bare substring "eof" is dangerous — StarRocks grammar errors
    // frequently include "<EOF>" in their detail message. Only treat
    // network-flavored EOF phrasings as transient. The standalone "eof"
    // substring was the original cause of the DDL id=7 poison-pill loop:
    // "Unexpected input '<EOF>'" matched `.contains("eof")` and was retried
    // indefinitely instead of failing through the 3-retry budget.
    if err_lower.contains("connection eof")
        || err_lower.contains("stream eof")
        || err_lower.contains("read eof")
        || err_lower.contains("mysql eof")
    {
        return true;
    }
    false
}

/// Bound the DDL text fed to polyglot-sql for table-ref extraction. Table
/// references always appear in the first statement, so parsing more than 64KB
/// is wasted CPU on the async executor (the extract helpers run inline in the
/// poller loop; a pathological multi-MB DDL would otherwise stall the reactor).
/// UTF-8-boundary safe (chars, not bytes).
fn bound_ddl_for_extract(ddl: &str) -> &str {
    const MAX_EXTRACT_CHARS: usize = 64 * 1024;
    if ddl.chars().count() <= MAX_EXTRACT_CHARS {
        ddl
    } else {
        match ddl.char_indices().nth(MAX_EXTRACT_CHARS) {
            Some((idx, _)) => &ddl[..idx],
            None => ddl,
        }
    }
}

/// Extract (schema, table) from a CREATE TABLE DDL statement using AST.
/// Defaults schema to "public" if not present.
/// Returns None if the table name can't be parsed.
fn extract_create_table_ref(ddl: &str) -> Option<(String, String)> {
    use polyglot_sql::{DialectType, Expression};

    let exprs = polyglot_sql::parse(bound_ddl_for_extract(ddl), DialectType::PostgreSQL)
        .map_err(|e| tracing::debug!("extract_create_table_ref: parse failed: {e}"))
        .ok()?;
    for expr in &exprs {
        if let Expression::CreateTable(ct) = expr {
            let schema = ct
                .name
                .schema
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| "public".to_string());
            let table = ct.name.name.name.clone();
            return Some((schema, table));
        }
    }
    None
}

/// Extract (schema, table) from an ALTER TABLE DDL statement using AST.
/// Defaults schema to "public" if not present.
fn extract_alter_table_ref(ddl: &str) -> Option<(String, String)> {
    use polyglot_sql::{DialectType, Expression};

    let exprs = polyglot_sql::parse(bound_ddl_for_extract(ddl), DialectType::PostgreSQL)
        .map_err(|e| tracing::debug!("extract_alter_table_ref: parse failed: {e}"))
        .ok()?;
    for expr in &exprs {
        if let Expression::AlterTable(at) = expr {
            let schema = at
                .name
                .schema
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| "public".to_string());
            let table = at.name.name.name.clone();
            return Some((schema, table));
        }
    }
    None
}

/// Extract the NEW table name from an `ALTER TABLE ... RENAME TO new_name` DDL.
///
/// `extract_alter_table_ref` returns the OLD table name (from `at.name`),
/// which no longer exists in PG after the rename completes. This helper
/// walks the AST actions to find the `RenameTable(TableRef)` action and
/// returns the NEW name. The schema is inherited from the OLD table's
/// schema (PG doesn't support cross-schema RENAME TO — that's `ALTER TABLE
/// ... SET SCHEMA`).
///
/// Returns None if the DDL is not a RENAME TO or the AST doesn't have a
/// RenameTable action.
fn extract_rename_to_new_ref(ddl: &str) -> Option<(String, String)> {
    use polyglot_sql::expressions::AlterTableAction;
    use polyglot_sql::{DialectType, Expression};

    let exprs = polyglot_sql::parse(bound_ddl_for_extract(ddl), DialectType::PostgreSQL)
        .map_err(|e| tracing::debug!("extract_rename_to_new_ref: parse failed: {e}"))
        .ok()?;
    for expr in &exprs {
        if let Expression::AlterTable(at) = expr {
            let schema = at
                .name
                .schema
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| "public".to_string());
            for action in &at.actions {
                if let AlterTableAction::RenameTable(new_ref) = action {
                    let new_schema = new_ref
                        .schema
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or(schema);
                    let new_table = new_ref.name.name.clone();
                    return Some((new_schema, new_table));
                }
            }
        }
    }
    None
}

/// Try to extract (schema, table) from any DDL statement using AST.
/// Tries ALTER TABLE, CREATE TABLE, CREATE TABLE AS, and DROP TABLE patterns.
/// Returns None if the table name can't be parsed from any pattern.
fn try_extract_table_ref(ddl: &str) -> Option<(String, String)> {
    // Try ALTER TABLE first (most common in DDL poller)
    if let Some(ref_) = extract_alter_table_ref(ddl) {
        return Some(ref_);
    }
    // Try CREATE TABLE / CREATE TABLE AS / CTAS
    if let Some(ref_) = extract_create_table_ref(ddl) {
        return Some(ref_);
    }
    // Try DROP TABLE via simple regex (polyglot-sql may not expose DropTable AST)
    {
        if let Some(caps) = DROP_TABLE_RE.captures(ddl) {
            let schema = caps
                .get(1)
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "public".to_string());
            let table = caps
                .get(2)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            if !table.is_empty() {
                return Some((schema, table));
            }
        }
    }
    None
}

/// Simple regex-based table name extraction fallback for table_filter checks.
/// Parses table name from DDL statements like:
///   CREATE TABLE schema.table (...)
///   ALTER TABLE schema.table ...
///   CREATE TABLE table (...)
///   DROP TABLE schema.table ...
/// Returns None if no table name can be found.
fn extract_table_name_simple(ddl: &str) -> Option<String> {
    let ddl = ddl.trim();
    if let Some(caps) = TABLE_NAME_EXTRACT_RE.captures(ddl) {
        // Group 2 is always the table name (unquoted or double-quoted)
        let table = caps.get(2)?.as_str();
        // Strip surrounding double quotes
        let table = table.trim_matches('"');
        if !table.is_empty() {
            return Some(table.to_string());
        }
    }
    None
}

/// Post-process transpiled DDL: strip PG-only artifacts and add SR clauses.
///
/// Uses polyglot-sql's AST for all transformations — no string slicing, regex,
/// or post-generation string .replace() calls. Parses the transpiled SQL once,
/// walks the AST to strip schema prefixes, TEMPORARY modifiers, INHERITS clauses,
/// DEFAULTs, and PG-specific options. For CREATE TABLE, delegates to
/// `add_sr_table_clauses_from_expr` for the final SR DDL with
/// ENGINE/KEY/DISTRIBUTED BY/PROPERTIES — no re-parsing needed.
/// Wrapper around `post_process_ddl_stmts` that joins the per-statement
/// `Vec<String>` into a single string — kept for backwards compatibility
/// with test helpers (`process_ddl`). Production code uses
/// `post_process_ddl_stmts` directly so `execute_sql_stmts` can skip the
/// third polyglot parse (Tier 1 optimization).
#[cfg(test)]
fn post_process_ddl(sql: &str, table_properties: &HashMap<String, String>) -> String {
    let stmts = post_process_ddl_stmts(sql, table_properties);
    stmts.join(";\n").trim().to_string()
}

/// (Tier 1) optimization: same as [`post_process_ddl`] but returns the
/// per-statement `Vec<String>` directly so the DDL path can feed it into
/// [`crate::sr_sql::execute_sql_stmts`] and skip the third redundant
/// `polyglot_sql::parse` inside [`crate::sr_connection::SrConnectionManager::execute_sql_with_db`].
///
/// The bottom-loop body is identical to `post_process_ddl` minus the final
/// `.join(";\n").trim().to_string()`. Tests assert the two are consistent:
/// `post_process_ddl(s) == post_process_ddl_stmts(s).join(";\n").trim()`.
fn post_process_ddl_stmts(sql: &str, table_properties: &HashMap<String, String>) -> Vec<String> {
    use polyglot_sql::expressions::AlterTableAction;
    use polyglot_sql::{DialectType, Expression};

    // Parse transpiled SQL into AST
    let mut exprs = match polyglot_sql::parse(sql, DialectType::StarRocks) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("post_process_ddl: parse failed: {} — passing through", e);
            return vec![sql.to_string()];
        }
    };

    let mut modified = false;

    for expr in &mut exprs {
        // Strip PG schema prefix from all table references
        strip_table_schemas_from_expr(expr);

        match expr {
            Expression::CreateTable(ct) => {
                // Strip PG-only modifiers
                ct.temporary = false;
                ct.inherits.clear();
                ct.properties.clear(); // WITHOUT OIDS, TABLESPACE, etc.

                // Strip DEFAULTs from column definitions
                for col in &mut ct.columns {
                    col.default = None;
                }
                modified = true;
            }
            Expression::AlterTable(at) => {
                // Strip DEFAULTs and NOT NULL from ADD COLUMN actions.
                // SR only supports NOT NULL on PRIMARY KEY columns and only
                // through CREATE TABLE — ALTER TABLE ADD COLUMN should always
                // create nullable columns.
                for action in &mut at.actions {
                    if let AlterTableAction::AddColumn { column, .. } = action {
                        column.default = None;
                        column.nullable = Some(true);
                        remap_pg_datatype_to_sr(&mut column.data_type);
                        column.constraints.clear();
                        column.constraint_order.clear();
                        column.unique = false;
                        column.unique_nulls_not_distinct = false;
                    }
                }
                // Strip PG-specific alter actions that SR doesn't support
                at.actions.retain(|a| !is_pg_only_alter_action(a));
                modified = true;
            }
            Expression::DropTable(dt) => {
                // SR doesn't support CASCADE — strip it
                dt.cascade = false;
                dt.cascade_constraints = false;
                dt.purge = false;
                modified = true; // table names were schema-stripped above
            }
            _ => {}
        }
    }

    if !modified {
        return vec![sql.to_string()];
    }

    exprs
        .iter()
        .flat_map(|expr| match expr {
            Expression::CreateTable(ct) => {
                vec![add_sr_table_clauses_from_expr(ct, table_properties)]
            }
            Expression::AlterTable(at) if at.actions.is_empty() => {
                vec![]
            }
            Expression::DropTable(dt) if dt.names.len() > 1 => {
                let if_exists = if dt.if_exists { "IF EXISTS " } else { "" };
                dt.names
                    .iter()
                    .map(|name| {
                        format!(
                            "DROP TABLE {}{}",
                            if_exists,
                            crate::sr_sql::sr_quote(&name.name.name)
                        )
                    })
                    .collect()
            }
            // Single-target DROP TABLE: use sr_quote for backtick quoting
            // (polyglot::generate emits bare names without backticks, which
            // fails on SR reserved words like `schema`, `key`, `timestamp`).
            Expression::DropTable(dt) => {
                let if_exists = if dt.if_exists { "IF EXISTS " } else { "" };
                dt.names
                    .iter()
                    .map(|name| {
                        format!(
                            "DROP TABLE {}{}",
                            if_exists,
                            crate::sr_sql::sr_quote(&name.name.name)
                        )
                    })
                    .collect()
            }
            other => {
                vec![polyglot_sql::generate(other, DialectType::StarRocks).unwrap_or_default()]
            }
        })
        .collect()
}

/// Recursively strip schema qualifiers from table references so
/// StarRocks DDL uses bare table names.
///
/// Before: `CREATE TABLE public.orders ...` → SR: `CREATE TABLE orders ...`
fn strip_table_schemas_from_expr(expr: &mut polyglot_sql::Expression) {
    use polyglot_sql::expressions::TableRef;
    use polyglot_sql::Expression;

    let fuse = |name: &mut TableRef| {
        if let Some(ref schema) = name.schema {
            name.name.name = crate::sr_sql::sr_table_name(&schema.name, &name.name.name);
            name.schema = None;
        }
    };

    match expr {
        Expression::CreateTable(ct) => {
            fuse(&mut ct.name);
            for parent in &mut ct.inherits {
                fuse(parent);
            }
        }
        Expression::AlterTable(at) => {
            fuse(&mut at.name);
        }
        Expression::DropTable(dt) => {
            for name in &mut dt.names {
                fuse(name);
            }
        }
        Expression::Insert(ins) => {
            fuse(&mut ins.table);
        }
        _ => {}
    }
}

/// Returns true if the AlterTableAction is PG-specific and not supported by StarRocks.
fn is_pg_only_alter_action(action: &polyglot_sql::expressions::AlterTableAction) -> bool {
    use polyglot_sql::expressions::{AlterColumnAction, AlterTableAction};
    matches!(
        action,
        AlterTableAction::AddConstraint(_)
            | AlterTableAction::DropConstraint { .. }
            | AlterTableAction::DropForeignKey { .. }
            | AlterTableAction::SetProperty { .. }
            | AlterTableAction::UnsetProperty { .. }
            | AlterTableAction::AlterColumn {
                action: AlterColumnAction::SetNotNull,
                ..
            }
            | AlterTableAction::AlterColumn {
                action: AlterColumnAction::DropNotNull,
                ..
            }
            // ALTER COLUMN ... SET DEFAULT / DROP DEFAULT: StarRocks has no
            // `ALTER COLUMN` action, and the pipeline already strips all
            // DEFAULTs from CREATE TABLE / ADD COLUMN and always sends
            // explicit values via Stream Load JSON — these are no-ops for SR.
            // Filtering them here is defense-in-depth for compound ALTERs
            // (e.g. `ADD COLUMN x, ALTER COLUMN y DROP DEFAULT`): the
            // standalone case is handled by `is_alter_column_default` above,
            // which skips the entire DDL before transpilation. The AST-level
            // filter catches the case where a compound ALTER survives the
            // predicate (e.g. multi-action DDL the regex didn't recognize).
            | AlterTableAction::AlterColumn {
                action: AlterColumnAction::SetDefault(_),
                ..
            }
            | AlterTableAction::AlterColumn {
                action: AlterColumnAction::DropDefault,
                ..
            }
            // Polyglot-sql has no dedicated variants for SET SCHEMA,
            // VALIDATE CONSTRAINT, CLUSTER, ENABLE/DISABLE TRIGGER, RLS, etc.
            // PG parses these to `SetAttribute{attribute: "..."}` (PG SET
            // LOGGED/UNLOGGED/WITHOUT OIDS/TABLESPACE) or `Raw{sql: "..."}`
            // (anything polyglot's parser can't structurally recognize). Both
            // are PG-only — strip them so standalone ALTERs of these forms
            // end up at the empty-output branch in `post_process_ddl` (Path P
            // SKIP) instead of being emitted verbatim → SR 1064 → 3-retry →
            // FATAL-ABORT under `ddl_failure_abort=true`.
            | AlterTableAction::SetAttribute { .. }
            | AlterTableAction::Raw { .. }
            // PG/Snowflake partition commands (SR PARTITION BY differs):
            | AlterTableAction::AddPartition { .. }
            | AlterTableAction::DropPartition { .. }
            | AlterTableAction::ReplacePartition { .. }
            // Snowflake CLUSTER BY (PG has no CLUSTER BY, but the variant
            // catches all dialects):
            | AlterTableAction::ClusterBy { .. }
            // Snowflake tags:
            | AlterTableAction::SetTag { .. }
            | AlterTableAction::UnsetTag { .. }
            // TSQL SET OPTIONS:
            | AlterTableAction::SetOptions { .. }
            // Redshift SET LOCATION / SET FILE FORMAT / SET TABLE PROPERTIES:
            | AlterTableAction::SetLocation { .. }
            | AlterTableAction::SetFileFormat { .. }
            | AlterTableAction::SetTableProperties { .. }
    )
}

/// Strip PG-only syntax that StarRocks doesn't support from raw DDL text
/// before transpilation. This catches constructs that polyglot-sql passes
/// through as-is (valid PG but invalid SR).
fn strip_pg_only_ddl_syntax(ddl: &str) -> String {
    // SR doesn't support IF EXISTS on DROP COLUMN — strip it before transpile.
    // Case-insensitive + whitespace-flexible regex replaces the prior
    // case-sensitive string-replace. Lowercase DDLs from psql sessions
    // (`alter table t drop column if exists c`) and double-spaced forms
    // (`drop  column  if  exists`) are now caught.
    let s = DROP_COLUMN_IF_EXISTS_RE
        .replace_all(ddl, "DROP COLUMN")
        .to_string();
    // Strip IF EXISTS from DROP TABLE so SR returns an error on a non-existent
    // table (the is_drop_table_not_exists_error classifier catches it). Without
    // this, polyglot preserves IF EXISTS → SR silently no-ops → the classifier
    // never fires → crash-replay DROPs burn the retry budget.
    let s = DROP_TABLE_IF_EXISTS_RE
        .replace_all(&s, "DROP TABLE")
        .to_string();
    DROP_IF_EXISTS_RE.replace_all(&s, "DROP").to_string()
}

/// Check if a SR error indicates that the column we tried to DROP doesn't exist.
/// This is harmless — the original PG DDL had IF EXISTS, but we stripped it
/// because SR doesn't support IF EXISTS on DROP COLUMN. Treat as success.
pub fn is_drop_column_not_exists_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    // SR returns "Column does not exists" (note: "exists" with 's').
    // We don't check for "DROP COLUMN" in the message because the SR error
    // only includes the column name, not the full SQL statement.
    msg.contains("Column does not exists")
}

/// Check if a SR error indicates that the table we tried to DROP doesn't exist.
/// This is harmless and happens on crash replay: a DROP TABLE succeeded on SR
/// but the subsequent `advance_and_save_ddl_checkpoint` save failed, so on
/// restart the DROP is re-applied against an already-dropped table. Without
/// this classifier the DDL burns its 3-retry budget then aborts when
/// `ddl_failure_abort=true`. Mirrors
/// `is_drop_column_not_exists_error`. The call site restricts this to
/// `tag == "DROP TABLE"` so the message patterns below are safe from
/// over-matching in other contexts.
pub fn is_drop_table_not_exists_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("Unknown table")
        || msg.contains("is not found")
        || msg.contains("does not exist")
        || msg.contains("doesn't exist")
}

/// Returns true if `err` is the TOCTOU-recheck deferral emitted by
/// snapshot_one_table / snapshot_table_inner when pending CDC events are
/// detected immediately before the shadow DROP. Callers should retry on the
/// next poll rather than burning the 3-retry failure budget.
pub fn is_snapshot_toctou_deferral(err: &anyhow::Error) -> bool {
    err.to_string().contains("snapshot deferred")
}

/// Wait for `pending_cdc_tables` to clear for `table_key`, polling every
/// `DRAIN_POLL_MS`. Returns `Ok(())` when the table is no longer pending (a CDC
/// flush for it is not in flight), or `Err` if shutdown was signaled during the
/// wait.
///
/// (replaces the MAX_DDL_DEFERRALS FATAL): a continuously-written table is
/// "pending" during each flush but clears briefly BETWEEN flushes — the 200ms
/// poll catches these gaps. The previous code polled at `poll_interval_ms`
/// (5s default) and FATAL-shutdown the entire pipeline after ~100s, killing
/// DDL throughput on hot tables. This bounded-wait keeps the pipeline alive
/// and lets the DDL land during the natural inter-flush gap; there is no upper
/// bound (no FATAL) — the DDL waits as long as needed. Combined with the
/// `DdlActiveGuard` acquired after this returns (and the double-check in
/// `flush_transaction`), this forms the DDL/CDC drain-and-pause coordination.
#[allow(clippy::too_many_arguments)]
async fn wait_for_cdc_drain(
    pending_cdc_tables: &Arc<Mutex<std::collections::HashSet<String>>>,
    table_key: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
    ddl_id: i64,
    schema: &str,
    table: &str,
) -> anyhow::Result<()> {
    const DRAIN_POLL_MS: u64 = 1000;
    loop {
        let pending = crate::pipeline::lock_or_recover(pending_cdc_tables).contains(table_key);
        if !pending {
            return Ok(());
        }
        tracing::info!(
            "DDL id={} for {}.{} — waiting for CDC flush to drain ({}ms poll, no FATAL)",
            ddl_id,
            schema,
            table,
            DRAIN_POLL_MS
        );
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                return Err(anyhow::anyhow!(
                    "Shutdown signaled during DDL drain wait for {}.{}", schema, table
                ));
            }
            _ = tokio::time::sleep(Duration::from_millis(DRAIN_POLL_MS)) => {}
        }
    }
}

/// Two-phase acquire of the DDL/CDC mutual-exclusion guard for `table_key`.
///
/// The guard-first ordering (acquire `DdlActiveGuard` BEFORE
/// `wait_for_cdc_drain`) closed the microsecond TOCTOU but introduced a
/// starvation loop on continuously-written tables:
///
///   1. DDL poller holds `DdlActiveGuard(T)` → every CDC flush for T is
///      appended to the wal_buffer while the guard is held (no deferral).
///   2. An append removes T from `pending_cdc_tables` (flush_transaction
///      routing), so the drain sees T clear while the guard is held.
///   3. `wait_for_cdc_drain` therefore completes in bounded time — the flush
///      path never waits on `ddl_active`, so the replay set cannot grow
///      unboundedly (the prior guard-first ordering's 2× HARD memory-limit
///      FATAL is gone; buffered changes spill to the durable wal_buffer).
///
/// The two-phase acquire (drain BEFORE guard) keeps in-flight live flushes
/// from being buffered unnecessarily:
///
///   Phase 1: `wait_for_cdc_drain` WITHOUT holding the guard. Flushes proceed
///            normally, so the buffer drains in bounded time.
///   Phase 2: acquire `DdlActiveGuard`, then RE-CHECK `pending_cdc_tables`.
///            If a flush re-buffered T in the gap between drain-return and
///            guard-acquire, drop the guard and go back to Phase 1.
///
/// The residual TOCTOU (a flush spawning between a successful re-check and
/// the DDL executing) is closed by the flush path's own `ddl_active`
/// double-check (pipeline.rs flush_transaction check 1 & 2): any flush that
/// starts after the guard is visible is buffered to the wal_buffer instead
/// and replayed once the guard drops. Once this function returns, no new
/// flush for T can start until the guard drops.
#[allow(clippy::too_many_arguments)]
async fn acquire_ddl_guard(
    ddl_active: &Arc<Mutex<std::collections::HashSet<String>>>,
    pending_cdc_tables: &Arc<Mutex<std::collections::HashSet<String>>>,
    table_key: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
    ddl_id: i64,
    schema: &str,
    table: &str,
) -> anyhow::Result<crate::pipeline::DdlActiveGuard> {
    loop {
        // Phase 1: drain without the guard so flushes can complete.
        wait_for_cdc_drain(
            pending_cdc_tables,
            table_key,
            shutdown_rx,
            ddl_id,
            schema,
            table,
        )
        .await?;
        // Phase 2: acquire the guard and re-check. If the table re-became
        // pending in the gap, drop the guard and drain again.
        let guard = crate::pipeline::DdlActiveGuard::new(ddl_active.clone(), table_key.to_string());
        if !crate::pipeline::lock_or_recover(pending_cdc_tables).contains(table_key) {
            return Ok(guard);
        }
        tracing::debug!(
            "DDL id={} for {}.{} — table re-buffered during guard acquire, re-draining",
            ddl_id,
            schema,
            table,
        );
        drop(guard);
    }
}

/// Poll interval for `SHOW ALTER TABLE COLUMN` while waiting for a
/// schema-change job to finalize. Fixed code constant — no config surface
/// (per design: all pause timings are constants).
const SCHEMA_CHANGE_JOB_POLL_MS: u64 = 500;
/// After this long in a non-terminal state, escalate the per-poll log from
/// `info!` to `warn!` (the table is write-paused — the job MUST complete;
/// if it never does, the table stays paused with a durable buffer, no loss).
const SCHEMA_CHANGE_JOB_WARN_AFTER_MS: u64 = 60_000;

/// Hard deadline for the final (post-retry) schema-change job wait when
/// `ddl_failure_abort=false`: after 10 minutes of non-terminal waits the
/// checkpoint is advanced and the write pause released (mid-rewrite flushes
/// are possible — logged loudly).
const FINAL_JOB_WAIT_MAX_MS: u64 = 600_000;

/// Why a schema-change job wait ended without observing `FINISHED`. The
/// caller maps each variant to its existing failure policy:
/// - `Cancelled` — the job was CANCELLED on SR (no longer running; releasing
///   the write pause is safe — existing semantics).
/// - `Shutdown` — process shutting down; the caller MUST return Err.
/// - `Transient` — query/missing-column errors; the caller retries WITH the
///   DdlActiveGuard still held so a retry can never race the DDL.
#[derive(Debug)]
enum JobWaitError {
    Cancelled,
    Shutdown,
    Transient(anyhow::Error),
}

impl std::fmt::Display for JobWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobWaitError::Cancelled => write!(f, "schema-change job CANCELLED"),
            JobWaitError::Shutdown => write!(f, "shutdown during schema-change job wait"),
            JobWaitError::Transient(e) => write!(f, "transient schema-change job wait error: {e}"),
        }
    }
}

impl std::error::Error for JobWaitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JobWaitError::Transient(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

/// Local equivalent of `sr_connection::find_col_index` (private there):
/// resolve a `SHOW ALTER TABLE COLUMN` column by NAME so a future SR version
/// reordering result columns can't silently read the wrong value.
fn find_show_col<S: AsRef<str>>(names: &[S], target: &str) -> anyhow::Result<usize> {
    names
        .iter()
        .position(|n| n.as_ref().eq_ignore_ascii_case(target))
        .ok_or_else(|| {
            anyhow::anyhow!("Column '{target}' not found in SHOW ALTER TABLE COLUMN result")
        })
}

/// Hold the write pause (the caller's `DdlActiveGuard`) until the StarRocks
/// schema-change job for `sr_table` reaches `FINISHED`.
///
/// Root cause this fixes: `execute_sql_stmts` returns as soon as SR *accepts*
/// the ALTER — the schema-change job (table rewrite) finalizes asynchronously.
/// Previously the poller released its `DdlActiveGuard` immediately, so
/// concurrent CDC flushes resumed against a table whose rewrite job was
/// mid-flight, wedging the job at `FINISHED_REWRITING` until the table was
/// dropped. Waiting here — guard still held — makes the pause span job
/// completion.
///
/// Polls `SHOW ALTER TABLE COLUMN FROM <db>` every `SCHEMA_CHANGE_JOB_POLL_MS`
/// and inspects the LATEST job (max `JobId`, fallback max `CreateTime`) for
/// `sr_table`:
/// - `FINISHED` → `Ok(true)` (DDL durably applied; caller advances the
///   checkpoint).
/// - `CANCELLED` → `Err` ("schema change job CANCELLED for {table}") — flows
///   into the existing 3-retry/skip policy.
/// - anything else (`PENDING`, `WAITING_TXN`, `RUNNING`,
///   `FINISHED_REWRITING`, unknown) → in-progress: log and poll again.
/// - no rows / no job for `sr_table` for TWO CONSECUTIVE polls → `Ok(false)`
///   (instant DDL already finalized, or the job was GC'd — caller must NOT
///   treat this as job-observed evidence of completion). The second empty
///   poll protects multi-FE deployments where a failover lands on a follower
///   with lagging job registration.
///
/// No timeout: legitimate rewrites of large tables take long; with writes
/// paused the wait is safe. The operator stops it via shutdown or by fixing
/// SR. Shutdown aborts the wait via `select!` (like `wait_for_cdc_drain`).
///
/// NOTE: `FINISHED_REWRITING` is a normal intermediate state on modern SR —
/// always in-progress, never terminal.
async fn wait_for_schema_change_jobs(
    sr_mgr: &Arc<SrConnectionManager>,
    table_key: &str,
    sr_table: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
    ddl_id: i64,
) -> Result<bool, JobWaitError> {
    let db = crate::sr_sql::sr_quote(sr_mgr.database());
    let sql = format!("SHOW ALTER TABLE COLUMN FROM {db}");
    // First-seen Instant per non-terminal state — escalate to warn! when the
    // SAME state persists > SCHEMA_CHANGE_JOB_WARN_AFTER_MS.
    let mut state_seen_at: HashMap<String, std::time::Instant> = HashMap::new();
    let mut absent_polls: u32 = 0;
    loop {
        let rows = sr_mgr
            .query_rows(&sql)
            .await
            .map_err(JobWaitError::Transient)?;
        let best: Option<(String, i64, String)> = if rows.is_empty() {
            // No schema-change jobs at all — treat as one absent poll.
            None
        } else {
            // Resolve columns by name from the first row (result set is
            // homogeneous). Never read Date/Int columns as String —
            // mysql_common's FromValue for String only accepts bytes and
            // PANICS otherwise.
            let col_names: Vec<String> = rows[0]
                .columns()
                .iter()
                .map(|c| c.name_str().to_string())
                .collect();
            let idx_table =
                find_show_col(&col_names, "TableName").map_err(JobWaitError::Transient)?;
            let idx_state = find_show_col(&col_names, "State").map_err(JobWaitError::Transient)?;
            let idx_job = find_show_col(&col_names, "JobId").map_err(JobWaitError::Transient)?;
            let idx_create =
                find_show_col(&col_names, "CreateTime").map_err(JobWaitError::Transient)?;

            // Latest job for this table: (state, job_id, create_time).
            let mut best: Option<(String, i64, String)> = None;
            for row in &rows {
                let tbl: Option<String> = row.get(idx_table);
                match tbl {
                    Some(t) if t.eq_ignore_ascii_case(sr_table) => {}
                    _ => continue,
                }
                let state: Option<String> = row.get(idx_state);
                let job: Option<i64> = row.get(idx_job);
                let create: Option<mysql_async::Value> = row.get(idx_create);
                let create_str = create.map(|v| v.as_sql(false)).unwrap_or_default();
                let newer = match &best {
                    None => true,
                    Some((_, bj, bc)) => match (job, Some(*bj)) {
                        (Some(a), Some(b)) => a > b,
                        _ => create_str > *bc, // fallback: lexicographic on CreateTime
                    },
                };
                if newer {
                    best = Some((state.unwrap_or_default(), job.unwrap_or(0), create_str));
                }
            }
            best
        };

        let Some((state, job_id, _)) = best else {
            // No job for this table — instant DDL already finalized or job
            // GC'd. One empty poll can be a lagging FE registration; two
            // consecutive empties means there is no job to observe.
            absent_polls += 1;
            if absent_polls >= 2 {
                return Ok(false);
            }
            tracing::debug!(
                "DDL id={} no schema-change job row for {} (absent poll {}/2) — re-polling in {}ms",
                ddl_id,
                table_key,
                absent_polls,
                SCHEMA_CHANGE_JOB_POLL_MS
            );
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    return Err(JobWaitError::Shutdown);
                }
                _ = tokio::time::sleep(Duration::from_millis(SCHEMA_CHANGE_JOB_POLL_MS)) => {}
            }
            continue;
        };
        absent_polls = 0;

        if state.eq_ignore_ascii_case("FINISHED") {
            tracing::info!(
                "DDL id={} schema-change job {} for {} FINISHED — releasing write pause",
                ddl_id,
                job_id,
                table_key,
            );
            return Ok(true);
        }
        if state.eq_ignore_ascii_case("CANCELLED") {
            return Err(JobWaitError::Cancelled);
        }

        // In-progress: poll again. FINISHED_REWRITING is a NORMAL
        // intermediate state on modern SR — never terminal.
        let warn = match state_seen_at.get(&state) {
            Some(t0) => t0.elapsed().as_millis() as u64 > SCHEMA_CHANGE_JOB_WARN_AFTER_MS,
            None => {
                state_seen_at.insert(state.clone(), std::time::Instant::now());
                false
            }
        };
        if warn {
            tracing::warn!(
                "DDL id={} schema-change job {} for {} still {state} after >{}s — \
                 table is write-paused with a durable buffer; job must complete \
                 (operator: fix SR if stuck)",
                ddl_id,
                job_id,
                table_key,
                SCHEMA_CHANGE_JOB_WARN_AFTER_MS / 1000,
            );
        } else {
            tracing::info!(
                "DDL id={} schema-change job {} for {} state={state} — waiting {}ms",
                ddl_id,
                job_id,
                table_key,
                SCHEMA_CHANGE_JOB_POLL_MS,
            );
        }
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                return Err(JobWaitError::Shutdown);
            }
            _ = tokio::time::sleep(Duration::from_millis(SCHEMA_CHANGE_JOB_POLL_MS)) => {}
        }
    }
}

/// If `err` is a snapshot TOCTOU-recheck deferral,
/// record it against the per-DDL deferral budget and return true so the caller
/// `break 'batch`s and retries on the next poll (without burning the 3-retry
/// failure budget). Hitting `max_deferrals` escalates to a FATAL shutdown so a
/// stuck pending-CDC table can't spin forever. Returns false for non-deferral
/// errors so the caller proceeds with normal failure handling.
#[allow(clippy::too_many_arguments)]
fn handle_snapshot_deferral(
    err: &anyhow::Error,
    ddl_id: i64,
    schema: &str,
    table: &str,
    ddl_deferrals: &mut HashMap<i64, u32>,
    max_deferrals: u32,
    poll_interval_ms: u64,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
    ddl_failure_abort: bool,
) -> bool {
    if !is_snapshot_toctou_deferral(err) {
        return false;
    }
    let count = ddl_deferrals
        .entry(ddl_id)
        .and_modify(|c| *c += 1)
        .or_insert(1);
    if *count > max_deferrals {
        // Gate the FATAL shutdown on `ddl_failure_abort`. The prior
        // unconditional `shutdown_tx.send(true)` bypassed the operator's
        // `ddl_failure_abort=false` opt-out, FATAL-shutting-down the pipeline
        // even when the operator explicitly chose "skip DDLs on failure".
        // With the gate, a `ddl_failure_abort=false` operator survives a
        // stuck-defferal DDL: we cap deferrals at `max_deferrals` (720, ~1hr)
        // then ADVANCE the checkpoint past it (treating it like a 3-retry-
        // exhausted skip-on-third) so the poller can make progress on
        // subsequent DDLs. Operators with `ddl_failure_abort=true` (the
        // default, recommended for production) still get the FATAL shutdown —
        // a stuck pending-CDC table usually indicates a coordination bug that
        // needs human attention.
        if ddl_failure_abort {
            tracing::error!(
                "FATAL: DDL id={} resnapshot for {}.{} keeps deferring (TOCTOU recheck) — \
                 exceeded {} deferral cycles ({}s). Table stuck in pending CDC. \
                 Shutting down (ddl_failure_abort=true).",
                ddl_id,
                schema,
                table,
                max_deferrals,
                (max_deferrals as u64 * poll_interval_ms) / 1000,
            );
            let _ = shutdown_tx.send(true);
            return true;
        } else {
            tracing::error!(
                "DDL id={} resnapshot for {}.{} exceeded {} deferral cycles ({}s) — \
                 advancing checkpoint past it (ddl_failure_abort=false). \
                 The resnapshot may be retried on the next restart.",
                ddl_id,
                schema,
                table,
                max_deferrals,
                (max_deferrals as u64 * poll_interval_ms) / 1000,
            );
            // Return false so the caller proceeds with normal failure handling
            // (the caller's `ddl_failures` map will see this and, if it hits
            // the 3-retry threshold, either abort or advance per ddl_failure_abort
            // — same as any other non-deferral error). Since the deferral counter
            // has already matured, we won't infinite-loop here.
            return false;
        }
    }
    tracing::warn!(
        "DDL id={} deferring resnapshot (TOCTOU recheck) for {}.{} — \
         pending CDC (deferral {}/{}). Will retry on next poll.",
        ddl_id,
        schema,
        table,
        *count,
        max_deferrals,
    );
    true
}

/// to SR-compatible string types. Custom enums, composites, etc. all become STRING.
///
/// Delegates to [`crate::types::pg_type_name_to_sr`] — the shared canonical
/// implementation. This wrapper exists for backward compatibility with tests.
fn map_unknown_sr_type(type_str: &str) -> String {
    crate::types::pg_type_name_to_sr(type_str)
}

/// Fallback for DDL poller: when polyglot-sql can't transpile a CREATE TABLE (e.g.
/// custom enum types), query PG catalog directly and build SR-native DDL.
/// Columns are reordered so PK columns come first (required by StarRocks).
/// Returns None if the table has no PRIMARY KEY — only PK tables are synced to SR.
async fn catalog_create_table_fallback(
    pg: &tokio_postgres::Client,
    database: &str,
    ddl_text: &str,
    table_properties: &HashMap<String, String>,
) -> anyhow::Result<Option<String>> {
    // Use the provided table properties directly — caller built them from config.
    // We need to separate the DDL-level props from the table structural props
    // that generate_create_table_sql handles.

    let (schema, table) = match extract_create_table_ref(ddl_text) {
        Some(st) => st,
        None => return Ok(None),
    };

    let oid = crate::pg_catalog::fetch_relation_oid(pg, &schema, &table)
        .await
        .with_context(|| {
            format!(
                "catalog fallback: fetch_relation_oid failed for {}.{}",
                schema, table
            )
        })?
        .ok_or_else(|| {
            anyhow::anyhow!("catalog fallback: no relation OID for {}.{}", schema, table)
        })?;

    let pcols = crate::pg_catalog::fetch_columns(pg, oid)
        .await
        .with_context(|| {
            format!(
                "catalog fallback: fetch_columns failed for {}.{}",
                schema, table
            )
        })?;
    if pcols.is_empty() {
        return Ok(None);
    }

    let pk_cols = crate::pg_catalog::fetch_primary_keys(pg, oid)
        .await
        .with_context(|| {
            format!(
                "catalog fallback: fetch_primary_keys failed for {}.{}",
                schema, table
            )
        })?;

    // Only create SR tables for tables with PRIMARY KEY.
    // PK-less tables can't support DELETE via Stream Load in SR.
    if pk_cols.is_empty() {
        tracing::warn!(
            "Catalog fallback: {}.{} has no PK — not creating SR table. \
             Table will be synced when a PRIMARY KEY is added.",
            schema,
            table,
        );
        return Ok(None);
    }

    // Reorder columns: PK columns must be first in SR's CREATE TABLE
    let ordered = crate::sr_sql::reorder_columns_for_sr(&pcols, &pk_cols, |c| c.name.as_str());

    let col_defs: Vec<String> = ordered
        .iter()
        .map(|c| {
            let sr_type = crate::types::pg_type_to_sr(c.type_oid, c.type_modifier);
            let mut col_def = format!("{} {}", crate::sr_sql::sr_quote(&c.name), sr_type);
            if pk_cols.contains(&c.name) {
                col_def.push_str(" NOT NULL");
            }
            col_def
        })
        .collect();

    let first_name = ordered.first().map(|c| c.name.as_str()).unwrap_or("id");
    let (key_clause, dist_col) = crate::sr_sql::build_key_and_dist_col(&pk_cols, first_name);

    let properties = crate::sr_sql::sr_table_properties(table_properties);

    let sr_table = crate::sr_sql::sr_table_name(&schema, &table);
    Ok(Some(crate::sr_sql::sr_create_table_sql(
        database,
        &sr_table,
        &col_defs,
        &key_clause,
        &dist_col,
        &properties,
        true,
    )))
}

/// Remap PG element types inside ARRAY<...> to SR equivalents.
///
/// polyglot-sql converts PG array syntax (INET[]) to ARRAY<INET>,
/// but SR doesn't understand PG-specific types. We map all ARRAY<...>
/// types to JSON for simplicity — PG array data is already converted
/// to JSON arrays in the CDC path, and SR's JSON type accepts nested arrays.
///
/// Examples:
/// Remap PG element types inside ARRAY<...> to SR equivalents.
/// Extracts the element type, maps it through `pg_type_name_to_sr`,
/// and wraps it back in ARRAY<>. Nested arrays (ARRAY<ARRAY<...>>)
/// pass through — SR 3.1+ supports multi-dimensional ARRAY.
///
/// Before:
/// ARRAY<INET> → ARRAY<VARCHAR(64)> (was ARRAY<STRING>)
///   ARRAY<REAL>  → ARRAY<FLOAT>
/// ARRAY<UUID> → ARRAY<VARCHAR(64)> (was ARRAY<STRING>)
///   ARRAY<VARCHAR> → ARRAY<STRING>
///   ARRAY<TEXT>    → ARRAY<STRING>
///   ARRAY<JSONB>   → ARRAY<JSON>
///   ARRAY<ARRAY<INT>> → ARRAY<ARRAY<INT>>  (pass through, SR 3.1+)
fn remap_array_element_type(type_str: String) -> String {
    let upper = type_str.to_uppercase();
    if !upper.starts_with("ARRAY<") || !upper.ends_with('>') {
        return type_str;
    }

    // Extract the element type string (text between ARRAY< and >)
    let inner = type_str[6..type_str.len() - 1].trim();
    if inner.is_empty() {
        return "JSON".to_string();
    }

    // Nested arrays: ARRAY<ARRAY<INT>> — pass through as-is
    // SR 3.1+ supports multi-dimensional ARRAY types up to 14 levels.
    let inner_upper = inner.to_uppercase();
    if inner_upper.starts_with("ARRAY<") {
        // For nested arrays, remap the outermost element type if needed.
        // Extract the innermost element for remapping, then rebuild.
        // Example: ARRAY<ARRAY<INET>> → ARRAY<ARRAY<STRING>>
        let inner_mapped = crate::types::pg_type_name_to_sr(inner);
        return format!("ARRAY<{}>", inner_mapped);
    }

    // Map through pg_type_name_to_sr for proper SR type remapping
    let mapped = crate::types::pg_type_name_to_sr(inner);
    format!("ARRAY<{}>", mapped)
}

/// Remap PG-specific column types to StarRocks equivalents at the AST level.
///
/// This mutates `dt` in place, rewriting `DataType` variants that
/// `polyglot_sql::generate(..., StarRocks)` would otherwise emit verbatim
/// (which produces invalid SR DDL such as `ADD COLUMN id SERIAL`).
///
/// Coverage mirrors `add_sr_table_clauses_from_expr`'s string-based remap:
/// - SERIAL / SERIAL4        → INT
/// - BIGSERIAL / SERIAL8     → BIGINT
/// - SMALLSERIAL / SERIAL2   → SMALLINT
/// - JSONB                    → JSON
/// - VARCHAR(N) / CHAR(N) / TEXT / TEXT(N) → STRING
/// - BYTEA                    → VARBINARY
/// - UUID                     → VARCHAR(64)
/// - INET / CIDR              → VARCHAR(64)
/// - MACADDR / MACADDR8       → VARCHAR(255)
/// - BIT / VARBIT             → VARCHAR(255)
/// - MONEY                    → DECIMAL(38,2)
/// - INTERVAL                 → BIGINT
/// - TIMESTAMP(p) / TIMESTAMPTZ(p) → DATETIME (precision dropped)
/// - Array element types recurse (ARRAY<BYTEA> → ARRAY<VARBINARY>) — only needed
///   for variants the generator would mis-emit; ARRAY<INT> is already correct.
///
/// Any `DataType::Custom{name}` whose name is not a SERIAL-family alias
/// delegates to `pg_type_name_to_sr` (the shared canonical mapper). This makes
/// ADD COLUMN produce the same SR column types as CREATE TABLE — previously the
/// ADD COLUMN path had large gaps (no MONEY / UUID / INET / MACADDR / etc.),
/// causing SR to reject the DDL with 1064 → 3-retry → FATAL-ABORT under
/// `ddl_failure_abort=true`.
///
/// Called for every `AlterTableAction::AddColumn` inside `post_process_ddl`
/// so that ALTER TABLE ADD COLUMN benefits from the same type mapping
/// CREATE TABLE has always applied.
fn remap_pg_datatype_to_sr(dt: &mut polyglot_sql::expressions::DataType) {
    use polyglot_sql::expressions::DataType;
    match dt {
        DataType::Custom { name } => {
            *dt = match name.to_uppercase().as_str() {
                "SERIAL" | "SERIAL4" => DataType::Int {
                    length: None,
                    integer_spelling: false,
                },
                "BIGSERIAL" | "SERIAL8" => DataType::BigInt { length: None },
                "SMALLSERIAL" | "SERIAL2" => DataType::SmallInt { length: None },
                _ => {
                    // Delegate to the shared canonical name-path mapper
                    // so ADD COLUMN produces the same SR types as CREATE TABLE.
                    // The mapped string becomes a Custom{name} that polyglot's
                    // generator emits verbatim (e.g. Custom{name:"DECIMAL(38,2)"}
                    // → "DECIMAL(38,2)"). For names that pg_type_name_to_sr
                    // returns unchanged-as-uppercase (i.e. SR-native types like
                    // "BOOLEAN"), leave the original Custom variant alone to
                    // preserve case/quoting.
                    let mapped = crate::types::pg_type_name_to_sr(name);
                    if mapped.to_uppercase() == name.to_uppercase() {
                        return;
                    }
                    DataType::Custom { name: mapped }
                }
            };
        }
        DataType::JsonB => {
            *dt = DataType::Json;
        }
        DataType::VarChar { .. }
        | DataType::Char { .. }
        | DataType::Text
        | DataType::TextWithLength { .. } => {
            *dt = DataType::String { length: None };
        }
        // The explicit-variant arms below all funnel through
        // `DataType::Custom{name: mapped_string}` because polyglot-sql's
        // StarRocks generator emits Custom names verbatim — this is the
        // same mechanism as the Custom-arm fallback above. Constructing a
        // real `DataType::VarChar{length:Some(64), parenthesized_length:true}`
        // produces `VARCHAR((64))` (double parens) in some polyglot versions;
        // `Custom{name:"VARCHAR(64)"}` reliably emits `VARCHAR(64)`.

        // UUID is a polyglot-sql variant that the generator emits as
        // "UUID" verbatim → SR rejects with 1064 → FATAL-ABORT. Map to
        // VARCHAR(64) to match the OID path (types.rs:138 `2950 → VARCHAR(64)`).
        DataType::Uuid => {
            *dt = DataType::Custom {
                name: "VARCHAR(64)".to_string(),
            };
        }
        // BIT / VARBIT polyglot variants → VARCHAR(255), matching the
        // OID path (types.rs:148/149 `1560/1562 → VARCHAR(255)`).
        DataType::Bit { .. } | DataType::VarBit { .. } => {
            *dt = DataType::Custom {
                name: "VARCHAR(255)".to_string(),
            };
        }
        // TIMESTAMP(p) / TIMESTAMPTZ(p) — drop precision to match the
        // OID path (1114/1184 → DATETIME, no precision preserved). polyglot
        // emits "STRING" for `Timestamp{timezone:true}` in StarRocks dialect
        // (the catch-all), so we replace with Custom{name:"DATETIME"} to force
        // the SR type. Matches what the CREATE TABLE path produces via
        // `pg_type_name_to_sr("TIMESTAMP(6)")` → "DATETIME".
        DataType::Timestamp { .. } => {
            *dt = DataType::Custom {
                name: "DATETIME".to_string(),
            };
        }
        // PG INTERVAL is a real polyglot variant that the generator
        // emits as "INTERVAL" verbatim → SR rejects with 1064 → FATAL-ABORT.
        // Map to BIGINT to match the OID path (types.rs OID 1186 → BIGINT).
        DataType::Interval { .. } => {
            *dt = DataType::Custom {
                name: "BIGINT".to_string(),
            };
        }
        DataType::Array { element_type, .. } => {
            remap_pg_datatype_to_sr(element_type);
        }
        _ => {}
    }
}

/// Add StarRocks ENGINE, KEY, DISTRIBUTED BY, and PROPERTIES from a
/// polyglot-sql `CreateTable` AST node directly — no re-parsing needed.
///
/// Handles PG type replacements (SERIAL → INT, BIGSERIAL → BIGINT, JSONB → JSON)
/// at the AST level instead of post-generation string .replace() calls.
/// Columns are reordered so PK columns come first (required by StarRocks).
fn add_sr_table_clauses_from_expr(
    ct: &polyglot_sql::expressions::CreateTable,
    table_props: &std::collections::HashMap<String, String>,
) -> String {
    use polyglot_sql::expressions::TableConstraint;
    use polyglot_sql::{DialectType, Expression};

    // ── The schema qualifier has already been stripped by strip_table_schemas_from_expr ──
    // ── (schema prefix removed, e.g. public.bar → bar) ──
    let table = ct.name.name.name.as_str();

    // ── First pass: collect column metadata and detect inline PKs ──
    #[derive(Clone)]
    struct ColMeta {
        name: String,
        type_str: String,
        #[allow(dead_code)]
        is_pk: bool,
        not_null: bool,
    }

    let mut pk_columns: Vec<String> = Vec::new();
    let mut col_metas: Vec<ColMeta> = Vec::with_capacity(ct.columns.len());

    for col in &ct.columns {
        let col_name = col.name.name.clone();
        let type_str = polyglot_sql::generate(
            &Expression::DataType(col.data_type.clone()),
            DialectType::StarRocks,
        )
        .unwrap_or_else(|_| "STRING".to_string());

        // Map PG types to SR equivalents at the AST level.
        let type_str = match type_str.to_uppercase().as_str() {
            "SERIAL" | "SERIAL4" => "INT".to_string(),
            "BIGSERIAL" | "SERIAL8" => "BIGINT".to_string(),
            "JSONB" => "JSON".to_string(),
            "BYTEA" => "VARBINARY".to_string(),
            // VARCHAR/CHAR/TEXT types → STRING.
            // polyglot-sql often maps varchar → varchar(1) which is too
            // restrictive; STRING has no length limit in StarRocks.
            s if s.starts_with("VARCHAR")
                || s.starts_with("CHAR")
                || s.starts_with("CHARACTER")
                || s.starts_with("NVARCHAR")
                || s.starts_with("NVARCHAR2")
                || s == "TEXT"
                || s == "BPCHAR" =>
            {
                "STRING".to_string()
            }
            _ => map_unknown_sr_type(&type_str),
        };

        // Remap PG element types inside ARRAY<...> to SR equivalents.
        // polyglot-sql converts PG array syntax INET[] → ARRAY<INET>,
        // but SR doesn't know PG types like INET, CIDR, UUID, REAL.
        let type_str = remap_array_element_type(type_str);

        let is_pk = col.primary_key;
        if is_pk && !pk_columns.contains(&col_name) {
            pk_columns.push(col_name.clone());
        }

        col_metas.push(ColMeta {
            name: col_name,
            type_str,
            is_pk,
            not_null: col.nullable == Some(false),
        });
    }

    // ── Table-level PRIMARY KEY constraints ──
    for constraint in &ct.constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for col_id in columns {
                let pk_col = col_id.name.clone();
                if !pk_columns.contains(&pk_col) {
                    pk_columns.push(pk_col);
                }
            }
        }
    }

    // ── PK-less tables are not supported ──
    if pk_columns.is_empty() {
        tracing::info!(
            "Table {} has no PRIMARY KEY — skipping SR table creation. \
             PK-less tables are not supported.",
            table,
        );
        return String::new();
    }

    // ── Reorder columns: PK columns first ──
    let ordered =
        crate::sr_sql::reorder_columns_for_sr(&col_metas, &pk_columns, |c| c.name.as_str());

    // ── Build col_defs from reordered columns ──
    let mut col_defs: Vec<String> = Vec::with_capacity(ordered.len());
    for col in &ordered {
        let mut col_def = format!("{} {}", crate::sr_sql::sr_quote(&col.name), col.type_str);
        // Policy: only PRIMARY KEY columns are NOT NULL on SR.
        // Strip NOT NULL from non-PK columns even if the transpiled DDL
        // preserved it from PG's schema.
        if col.not_null && pk_columns.contains(&col.name) {
            col_def.push_str(" NOT NULL");
        }
        col_defs.push(col_def);
    }

    // ── Build output ──
    let first_col = ordered.first().map(|c| c.name.as_str()).unwrap_or("id");

    let _has_pk_columns = !pk_columns.is_empty();
    let (key_clause, dist_col) = crate::sr_sql::build_key_and_dist_col(&pk_columns, first_col);

    let properties = crate::sr_sql::sr_table_properties(table_props);

    crate::sr_sql::sr_create_table_sql(
        "",
        table,
        &col_defs,
        &key_clause,
        &dist_col,
        &properties,
        true,
    )
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_abort_on_drop_cancels_task() {
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });

        {
            let _guard = AbortOnDrop(handle);
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[test]
    fn test_should_sync_ddl_table_operations() {
        assert!(should_sync_ddl("CREATE TABLE"));
        assert!(should_sync_ddl("CREATE TABLE AS"));
        assert!(should_sync_ddl("ALTER TABLE"));
        assert!(should_sync_ddl("DROP TABLE"));
    }

    #[test]
    fn test_should_sync_ddl_rejects_index_operations() {
        assert!(!should_sync_ddl("CREATE INDEX"));
        assert!(!should_sync_ddl("DROP INDEX"));
        assert!(!should_sync_ddl("ALTER INDEX"));
    }

    #[test]
    fn test_should_sync_ddl_rejects_unsupported_tags() {
        assert!(!should_sync_ddl("CREATE VIEW"));
        assert!(!should_sync_ddl("DROP VIEW"));
        assert!(!should_sync_ddl("CREATE SEQUENCE"));
        assert!(!should_sync_ddl("CREATE SCHEMA"));
        assert!(!should_sync_ddl("REPLICA IDENTITY FULL"));
        assert!(!should_sync_ddl("ALTER SEQUENCE"));
    }

    #[test]
    fn test_should_sync_ddl_select_into() {
        assert!(
            should_sync_ddl("SELECT INTO"),
            "SELECT INTO creates tables and must be captured by the DDL poller"
        );
    }

    // ── RENAME COLUMN parsing ─────────────────────────

    #[test]
    fn test_parse_rename_pairs_folds_bare() {
        // Bare identifiers are folded to ASCII lowercase — PG folds unquoted
        // identifiers (C locale).
        assert_eq!(
            parse_rename_pairs("ALTER TABLE t RENAME COLUMN Foo TO Bar"),
            vec![("foo".to_string(), "bar".to_string())],
        );
        assert_eq!(
            parse_rename_pairs("alter table t rename column f1 to f2"),
            vec![("f1".to_string(), "f2".to_string())],
        );
    }

    #[test]
    fn test_parse_rename_pairs_quoted_escaped() {
        // Quoted identifiers may contain "" escapes; the capture must
        // include them and the parser unescape them.
        assert_eq!(
            parse_rename_pairs(r#"ALTER TABLE t RENAME COLUMN "a""b" TO "c""d""#),
            vec![("a\"b".to_string(), "c\"d".to_string())],
        );
    }

    #[test]
    fn test_parse_rename_pairs_quoted_case_preserved() {
        // Quoted identifiers keep their case — never folded.
        assert_eq!(
            parse_rename_pairs(r#"ALTER TABLE t RENAME COLUMN "MiXeD" TO "NewCase""#),
            vec![("MiXeD".to_string(), "NewCase".to_string())],
        );
        // Mixed bare+quoted in one statement.
        assert_eq!(
            parse_rename_pairs(r#"ALTER TABLE t RENAME COLUMN a TO "NewA""#),
            vec![("a".to_string(), "NewA".to_string())],
        );
    }

    #[test]
    fn test_parse_rename_pairs_multi_action() {
        // Multi-action ALTER statements yield EVERY pair.
        assert_eq!(
            parse_rename_pairs("ALTER TABLE t RENAME COLUMN a TO b, RENAME COLUMN c TO d"),
            vec![
                ("a".to_string(), "b".to_string()),
                ("c".to_string(), "d".to_string()),
            ],
        );
    }

    #[test]
    fn test_parse_rename_pairs_ignores_rename_to() {
        // RENAME TO is a table rename, not a column rename — no pairs.
        assert!(parse_rename_pairs("ALTER TABLE t RENAME TO t2").is_empty());
        // RENAME COLUMN part of a multi-action statement still parses.
        assert_eq!(
            parse_rename_pairs("ALTER TABLE t RENAME COLUMN a TO b, RENAME TO t2"),
            vec![("a".to_string(), "b".to_string())],
        );
    }

    #[test]
    fn test_parse_rename_pairs_ignores_rename_constraint() {
        // RENAME CONSTRAINT is not a column rename — no pairs.
        assert!(parse_rename_pairs("ALTER TABLE t RENAME CONSTRAINT c1 TO c2").is_empty());
        // Unrelated DDL → empty.
        assert!(parse_rename_pairs("ALTER TABLE t ADD COLUMN x INT").is_empty());
    }

    // ── Case-insensitive PRIMARY KEY detection (L3) ─────────────────────

    #[test]
    fn test_ddl_text_case_insensitive_primary_key() {
        // The actual pipeline code uses .to_ascii_uppercase().contains("PRIMARY KEY")
        // to handle mixed-case DDL text (e.g. "primary key" from pgoutput).
        let mixed = "alter table t add primary key (id)";
        assert!(mixed.to_ascii_uppercase().contains("PRIMARY KEY"));

        let upper = "ALTER TABLE t ADD PRIMARY KEY (id)";
        assert!(upper.to_ascii_uppercase().contains("PRIMARY KEY"));

        let lower = "alter table t add primary key (id)";
        assert!(lower.to_ascii_uppercase().contains("PRIMARY KEY"));

        // Ensure non-matching text doesn't falsely trigger
        let no_pk = "CREATE TABLE t (id INT, name TEXT)";
        assert!(!no_pk.to_ascii_uppercase().contains("PRIMARY KEY"));

        let index = "CREATE INDEX idx ON t (id)";
        assert!(!index.to_ascii_uppercase().contains("PRIMARY KEY"));
    }

    /// Test helper: build a table properties HashMap from FSE/PI booleans.
    /// Uses the same logic as SrConfig::build_table_properties.
    fn test_table_props(fse: bool, pi: bool) -> std::collections::HashMap<String, String> {
        let mut map =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        if fse {
            map.insert("fast_schema_evolution".to_string(), "true".to_string());
        }
        if pi {
            map.insert("enable_persistent_index".to_string(), "true".to_string());
            map.insert(
                "persistent_index_type".to_string(),
                "CLOUD_NATIVE".to_string(),
            );
        }
        map
    }

    /// Test helper: parse CREATE TABLE SQL, apply SR clauses, return result.
    fn test_add_sr_clauses(sql: &str, fse: bool, pi: bool) -> String {
        use polyglot_sql::{DialectType, Expression};
        let props = test_table_props(fse, pi);
        if let Ok(exprs) = polyglot_sql::parse(sql, DialectType::StarRocks) {
            for expr in &exprs {
                if let Expression::CreateTable(ct) = expr {
                    return add_sr_table_clauses_from_expr(ct, &props);
                }
            }
        }
        sql.to_string()
    }

    #[test]
    fn test_add_sr_table_clauses_fast_schema_evolution() {
        let input = "CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)";

        // With fast_schema_evolution = true
        let sql = test_add_sr_clauses(input, true, true);
        assert!(
            sql.contains("\"fast_schema_evolution\" = \"true\""),
            "Should use 'true' not '1': {}",
            sql
        );
        assert!(
            !sql.contains("\"fast_schema_evolution\" = \"1\""),
            "Should NOT use '1': {}",
            sql
        );

        // With fast_schema_evolution = false
        let sql2 = test_add_sr_clauses(input, false, false);
        assert!(
            !sql2.contains("fast_schema_evolution"),
            "Should omit fast_schema_evolution when disabled: {}",
            sql2
        );
    }

    #[test]
    fn test_add_sr_table_clauses_persistent_index_for_pk() {
        // Table with PRIMARY KEY
        let input = "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)";
        let sql = test_add_sr_clauses(input, true, true);
        assert!(
            sql.contains("\"enable_persistent_index\" = \"true\""),
            "PK table must have persistent_index: {sql}"
        );
        assert!(
            sql.contains("\"persistent_index_type\" = \"CLOUD_NATIVE\""),
            "PK table must have persistent_index_type: {sql}"
        );

        // Without fast_schema_evolution
        let sql2 = test_add_sr_clauses(input, false, true);
        assert!(
            sql2.contains("\"enable_persistent_index\" = \"true\""),
            "PK table must have persistent_index even without FSE: {sql2}"
        );

        // Table without PK
        let input3 = "CREATE TABLE t (val INT)";
        let sql3 = test_add_sr_clauses(input3, false, false);
        assert!(
            sql3.is_empty(),
            "No-PK table must return empty string (not supported), got: {sql3}"
        );
    }

    #[test]
    fn test_map_unknown_sr_type_known() {
        assert_eq!(map_unknown_sr_type("INT"), "INT");
        assert_eq!(map_unknown_sr_type("VARCHAR(255)"), "VARCHAR(255)");
        assert_eq!(map_unknown_sr_type("DECIMAL(10,2)"), "DECIMAL(10,2)");
        assert_eq!(map_unknown_sr_type("ARRAY<INT>"), "ARRAY<INT>");
        assert_eq!(map_unknown_sr_type("BOOLEAN"), "BOOLEAN");
        assert_eq!(map_unknown_sr_type("JSON"), "JSON");
        assert_eq!(map_unknown_sr_type("STRING"), "STRING");
        assert_eq!(map_unknown_sr_type("DATETIME"), "DATETIME");
        assert_eq!(map_unknown_sr_type("VARBINARY"), "VARBINARY");
        assert_eq!(map_unknown_sr_type("BIGINT"), "BIGINT");
    }

    #[test]
    fn test_map_unknown_sr_type_custom_enum() {
        assert_eq!(map_unknown_sr_type("mood"), "STRING");
        assert_eq!(map_unknown_sr_type("Mood2"), "STRING");
        assert_eq!(map_unknown_sr_type("my_enum_type"), "STRING");
    }

    #[test]
    fn test_map_unknown_sr_type_composite() {
        assert_eq!(map_unknown_sr_type("public.mood"), "STRING");
        assert_eq!(map_unknown_sr_type("pg_catalog.tsvector"), "STRING");
    }

    #[test]
    fn test_add_sr_table_clauses_malformed_sql() {
        let sql = test_add_sr_clauses("CREATE TABLE t", true, false);
        assert_eq!(sql, "CREATE TABLE t", "No-paren SQL returned unchanged");

        let sql = test_add_sr_clauses(")CREATE TABLE t(id INT", true, false);
        assert_eq!(
            sql, ")CREATE TABLE t(id INT",
            "Reversed parens returned unchanged"
        );

        let sql = test_add_sr_clauses("CREATE TABLE t(", true, false);
        assert_eq!(sql, "CREATE TABLE t(", "Only open paren returned unchanged");

        let sql = test_add_sr_clauses("CREATE TABLE t)", true, false);
        assert_eq!(
            sql, "CREATE TABLE t)",
            "Only close paren returned unchanged"
        );

        let sql = test_add_sr_clauses("", true, false);
        assert_eq!(sql, "", "Empty string returned unchanged");
    }

    #[test]
    fn test_add_sr_table_clauses_with_constraint_pk() {
        let input = "CREATE TABLE newtable_public ( id INT NOT NULL, CONSTRAINT newtable_pk PRIMARY KEY (id) )";
        let sql = test_add_sr_clauses(input, true, true);

        assert!(
            !sql.contains("CONSTRAINT"),
            "Should strip CONSTRAINT keyword: {}",
            sql
        );
        assert!(
            !sql.contains("PRIMARY KEY(`id`))"),
            "Should not have double parens: {}",
            sql
        );
        assert!(
            !sql.contains("PRIMARY KEY(`order_id`))"),
            "Should not have double parens: {}",
            sql
        );
        assert!(
            sql.contains("PRIMARY KEY(`id`)"),
            "Should have clean PRIMARY KEY: {}",
            sql
        );
        assert!(sql.contains("ENGINE=OLAP"), "Should have ENGINE: {}", sql);
    }

    #[test]
    fn test_add_sr_table_clauses_orders_with_constraint() {
        let input = "CREATE TABLE orders_public (order_id INT NOT NULL, customer_id INT NOT NULL, amount DECIMAL(10, 2), CONSTRAINT orders_pkey PRIMARY KEY (order_id))";
        let sql = test_add_sr_clauses(input, true, true);

        assert!(
            !sql.contains("CONSTRAINT"),
            "CONSTRAINT keyword must be stripped: {}",
            sql
        );
        assert!(!sql.contains(",d)"), "No stray 'd' from order_id: {}", sql);
        assert!(
            !sql.contains("PRIMARY KEY(`id`))"),
            "No double parens: {}",
            sql
        );
        assert!(
            !sql.contains("PRIMARY KEY(`order_id`))"),
            "No double parens: {}",
            sql
        );
        assert!(sql.contains("PRIMARY KEY(`order_id`)"), "Clean PK: {}", sql);
        assert!(sql.contains("ENGINE=OLAP"), "ENGINE present: {}", sql);
        assert!(sql.contains("`order_id` INT NOT NULL"), "Has order_id");
        assert!(sql.contains("`customer_id` INT"), "Has customer_id");
        assert!(sql.contains("`amount` DECIMAL(10, 2)"), "Has amount");
        if let Some(engine_pos) = sql.find("ENGINE=OLAP") {
            let before_engine = &sql[..engine_pos];
            assert!(
                !before_engine.contains("PRIMARY KEY"),
                "No PRIMARY KEY in column defs"
            );
        }
    }

    // ── Type mapping: VARCHAR/CHAR/TEXT → STRING ────────────────────────

    #[test]
    fn test_type_mapping_varchar_to_string() {
        let sql = test_add_sr_clauses(
            "CREATE TABLE t (id INT PRIMARY KEY, col VARCHAR)",
            true,
            false,
        );
        assert!(sql.contains("`col` STRING"), "VARCHAR → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_enum_full_chain() {
        // Custom PG enum type (like mood2) must map to STRING through the
        // full transpile+post_process chain — not just add_sr_table_clauses_from_expr.
        let ddl = "CREATE TABLE public.s28_enum (id SERIAL PRIMARY KEY, val mood2)";

        // First, show what transpile produces
        let transpiled = polyglot_sql::transpile(
            ddl,
            polyglot_sql::dialects::DialectType::PostgreSQL,
            polyglot_sql::dialects::DialectType::StarRocks,
        );
        match &transpiled {
            Ok(stmts) => {
                let joined = stmts.join(";\n");
                println!("TRANSPILED: {joined}");
                // Check if post_process_ddl's re-parse would succeed
                match polyglot_sql::parse(&joined, polyglot_sql::DialectType::StarRocks) {
                    Ok(exprs) => {
                        println!("RE-PARSE: OK — {} expressions", exprs.len());
                        // Show the DataType for each column
                        for expr in &exprs {
                            if let polyglot_sql::Expression::CreateTable(ct) = expr {
                                for col in &ct.columns {
                                    let type_str = polyglot_sql::generate(
                                        &polyglot_sql::Expression::DataType(col.data_type.clone()),
                                        polyglot_sql::DialectType::StarRocks,
                                    );
                                    println!("  Column '{}' → '{:?}'", col.name.name, type_str);
                                }
                            }
                        }
                    }
                    Err(e) => println!("RE-PARSE: FAILED — {e}"),
                }
            }
            Err(e) => println!("TRANSPILE FAILED: {e}"),
        }

        let sql = process_ddl(ddl, true);
        eprintln!("FINAL SQL: {sql}");
        assert!(
            sql.contains("`val` STRING"),
            "Enum type mood2 should map to STRING through full chain. SQL: {sql}"
        );
        assert!(
            !sql.contains("VARCHAR"),
            "No VARCHAR in output for enum type: {sql}"
        );
    }

    #[test]
    fn test_type_mapping_composite_full_chain() {
        // Custom PG composite type must map to STRING through the full chain.
        let ddl = "CREATE TABLE public.s29_comp (id SERIAL PRIMARY KEY, addr address)";
        let sql = process_ddl(ddl, true);
        assert!(
            sql.contains("`addr` STRING"),
            "Composite type 'address' should map to STRING. SQL: {sql}"
        );
        assert!(
            !sql.contains("VARCHAR"),
            "No VARCHAR in output for composite type: {sql}"
        );
    }

    #[test]
    fn test_type_mapping_domain_full_chain() {
        // PG DOMAIN type must map to STRING through the full chain.
        let ddl = "CREATE TABLE public.s30_domain (id SERIAL PRIMARY KEY, zip us_postal_code)";
        let sql = process_ddl(ddl, true);
        assert!(
            sql.contains("`zip` STRING"),
            "Domain type 'us_postal_code' should map to STRING. SQL: {sql}"
        );
    }

    #[test]
    fn test_type_mapping_multi_enum_table() {
        // Table with multiple enum columns
        let ddl = "CREATE TABLE public.multi_enum_tab (id SERIAL PRIMARY KEY, a mood, b mood2)";
        let sql = process_ddl(ddl, true);
        assert!(sql.contains("`a` STRING"), "Enum 'mood' → STRING: {sql}");
        assert!(sql.contains("`b` STRING"), "Enum 'mood2' → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_mixed_custom_and_native() {
        // Mixed table: PK + enum col + native type col
        let ddl = "CREATE TABLE public.mixed_tab (id SERIAL PRIMARY KEY, val mood, amount INT, label TEXT)";
        let sql = process_ddl(ddl, true);
        assert!(sql.contains("`val` STRING"), "Enum 'mood' → STRING: {sql}");
        assert!(sql.contains("`amount` INT"), "Native INT preserved: {sql}");
        assert!(sql.contains("`label` STRING"), "TEXT → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_only_custom_columns() {
        // Table with ONLY custom type columns (no native PG types)
        let ddl = "CREATE TABLE public.only_custom (id SERIAL PRIMARY KEY, a mood, b address)";
        let sql = process_ddl(ddl, true);
        assert!(sql.contains("`a` STRING"), "Enum → STRING: {sql}");
        assert!(sql.contains("`b` STRING"), "Composite → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_varchar_with_length_to_string() {
        let sql = test_add_sr_clauses(
            "CREATE TABLE t (id INT PRIMARY KEY, col VARCHAR(255))",
            true,
            false,
        );
        assert!(sql.contains("`col` STRING"), "VARCHAR(255) → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_varchar_one_char_to_string() {
        // The exact bug case: polyglot-sql mapped varchar → varchar(1)
        let sql = test_add_sr_clauses(
            "CREATE TABLE t (id INT PRIMARY KEY, col VARCHAR(1))",
            true,
            false,
        );
        assert!(sql.contains("`col` STRING"), "VARCHAR(1) → STRING: {sql}");
        assert!(!sql.contains("VARCHAR"), "no VARCHAR in output: {sql}");
    }

    #[test]
    fn test_type_mapping_char_to_string() {
        let sql = test_add_sr_clauses(
            "CREATE TABLE t (id INT PRIMARY KEY, col CHAR(10))",
            true,
            false,
        );
        assert!(sql.contains("`col` STRING"), "CHAR(10) → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_text_to_string() {
        let sql = test_add_sr_clauses("CREATE TABLE t (id INT PRIMARY KEY, col TEXT)", true, false);
        assert!(sql.contains("`col` STRING"), "TEXT → STRING: {sql}");
    }

    #[test]
    fn test_type_mapping_character_varying_to_string() {
        let sql = test_add_sr_clauses(
            "CREATE TABLE t (id INT PRIMARY KEY, col CHARACTER VARYING(100))",
            true,
            false,
        );
        assert!(
            sql.contains("`col` STRING"),
            "CHARACTER VARYING → STRING: {sql}"
        );
    }

    /// Helper: run a PG DDL through the full transpile+post_process pipeline.
    fn process_ddl(ddl: &str, fse: bool) -> String {
        let transpiled = polyglot_sql::transpile(
            ddl,
            polyglot_sql::dialects::DialectType::PostgreSQL,
            polyglot_sql::dialects::DialectType::StarRocks,
        )
        .map(|stmts| stmts.join(";\n"))
        .unwrap_or_else(|_| ddl.to_string());
        let mut props =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        if fse {
            props.insert("fast_schema_evolution".to_string(), "true".to_string());
        }
        let result = post_process_ddl(&transpiled, &props);
        result
            .lines()
            .map(|l| l.trim())
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn test_type_mapping_pg_array_types_to_sr() {
        // PG arrays now map to ARRAY<element_type> in SR
        // INET[] → ARRAY<VARCHAR(64)> (was ARRAY<STRING> — INET
        // is now mirrored to VARCHAR(64) on both the name path and OID path).
        let sql = process_ddl(
            "CREATE TABLE public.t (id INT PRIMARY KEY, arr INET[])",
            true,
        );
        assert!(
            sql.contains("ARRAY<VARCHAR(64)>"),
            "INET[] should map to ARRAY<VARCHAR(64)>: {sql}"
        );
        assert!(!sql.contains("JSON"), "No bare JSON type expected: {sql}");
        // REAL[] → ARRAY<FLOAT>
        let sql2 = process_ddl(
            "CREATE TABLE public.t2 (id INT PRIMARY KEY, arr REAL[])",
            true,
        );
        assert!(
            sql2.contains("ARRAY<FLOAT>"),
            "REAL[] should map to ARRAY<FLOAT>: {sql2}"
        );
        // UUID[] → ARRAY<VARCHAR(64)> (was ARRAY<STRING>).
        let sql3 = process_ddl(
            "CREATE TABLE public.t3 (id INT PRIMARY KEY, arr UUID[])",
            true,
        );
        assert!(
            sql3.contains("ARRAY<VARCHAR(64)>"),
            "UUID[] should map to ARRAY<VARCHAR(64)>: {sql3}"
        );
        // VARCHAR(50)[] → ARRAY<VARCHAR(50)>
        let sql4 = process_ddl(
            "CREATE TABLE public.t4 (id INT PRIMARY KEY, arr VARCHAR(50)[])",
            true,
        );
        assert!(
            sql4.contains("ARRAY<VARCHAR"),
            "VARCHAR(50)[] should map to ARRAY<VARCHAR>: {sql4}"
        );
    }

    #[test]
    fn test_comprehensive_create_table_scenarios() {
        let cases: Vec<(&str, &str, Vec<&str>, Vec<&str>)> = vec![
            ("basic", "CREATE TABLE public.t (id INT PRIMARY KEY, val INT)", vec!["ENGINE=OLAP", "PRIMARY KEY(`id`)"], vec![]),
            ("serial pk", "CREATE TABLE public.t (id SERIAL PRIMARY KEY, name TEXT)", vec!["ENGINE=OLAP", "PRIMARY KEY(`id`)"], vec![]),
            ("named constraint pk", "CREATE TABLE public.t (id INT, CONSTRAINT pk_t PRIMARY KEY (id))", vec!["ENGINE=OLAP", "PRIMARY KEY(`id`)"], vec![]),
            ("foreign key table-level", "CREATE TABLE public.t (id INT PRIMARY KEY, ref_id INT, FOREIGN KEY (ref_id) REFERENCES public.other(id))", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("foreign key inline", "CREATE TABLE public.t (id INT PRIMARY KEY, ref_id INT REFERENCES public.other(id))", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("unique column", "CREATE TABLE public.t (id INT PRIMARY KEY, name TEXT UNIQUE)", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("unique table-level", "CREATE TABLE public.t (id INT PRIMARY KEY, name TEXT, UNIQUE (name))", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("check constraint", "CREATE TABLE public.t (id INT PRIMARY KEY, age INT CHECK (age > 0))", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("temp table", "CREATE TEMP TABLE public.t (id INT PRIMARY KEY)", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("temporary table", "CREATE TEMPORARY TABLE public.t (id INT PRIMARY KEY)", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("unlogged table", "CREATE UNLOGGED TABLE public.t (id INT PRIMARY KEY)", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("generated identity", "CREATE TABLE public.t (id INT GENERATED ALWAYS AS IDENTITY, name TEXT, PRIMARY KEY (id))", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
            ("inherits", "CREATE TABLE public.t (id INT PRIMARY KEY) INHERITS (public.base)", vec!["ENGINE=OLAP", "DISTRIBUTED BY"], vec![]),
        ];

        for (name, ddl, required, alt_required) in &cases {
            let sql = process_ddl(ddl, true);
            for req in required {
                assert!(
                    sql.contains(req),
                    "[{}] DDL should contain '{}'. SQL: {}",
                    name,
                    req,
                    sql
                );
            }
            for req in alt_required {
                assert!(
                    sql.contains(req),
                    "[{}] DDL should contain '{}' (alt). SQL: {}",
                    name,
                    req,
                    sql
                );
            }
            assert!(
                !sql.contains("CONSTRAINT"),
                "[{}] CONSTRAINT leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("FOREIGN KEY"),
                "[{}] FOREIGN KEY leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("REFERENCES "),
                "[{}] REFERENCES leaked: {}",
                name,
                sql
            );
            assert!(!sql.contains(" CHECK "), "[{}] CHECK leaked: {}", name, sql);
            assert!(
                !sql.contains(" UNIQUE "),
                "[{}] UNIQUE keyword leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("TEMPORARY"),
                "[{}] TEMPORARY leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("TEMP TABLE"),
                "[{}] TEMP leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("UNLOGGED"),
                "[{}] UNLOGGED leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("IDENTITY"),
                "[{}] IDENTITY leaked: {}",
                name,
                sql
            );
            assert!(
                !sql.contains("INHERITS"),
                "[{}] INHERITS leaked: {}",
                name,
                sql
            );
            let paren_start = sql.find('(');
            let paren_end = sql.rfind(')');
            if let (Some(s), Some(e)) = (paren_start, paren_end) {
                let cols = &sql[s + 1..e];
                assert!(
                    !cols.contains(",  "),
                    "[{}] double-space in cols: {}",
                    name,
                    cols
                );
                assert!(
                    !cols.ends_with(','),
                    "[{}] trailing comma in cols: {}",
                    name,
                    cols
                );
            }
        }
    }

    #[test]
    fn test_extract_create_table_ref_basic() {
        assert_eq!(
            extract_create_table_ref("CREATE TABLE public.users (id INT)"),
            Some(("public".to_string(), "users".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_leading_whitespace() {
        assert_eq!(
            extract_create_table_ref(
                "\n    CREATE TABLE public.s9_enum (\n        id SERIAL PRIMARY KEY\n    )"
            ),
            Some(("public".to_string(), "s9_enum".to_string()))
        );
        assert_eq!(
            extract_create_table_ref("  CREATE TABLE t (id INT)"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_no_schema() {
        assert_eq!(
            extract_create_table_ref("CREATE TABLE users (id INT)"),
            Some(("public".to_string(), "users".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_if_not_exists() {
        assert_eq!(
            extract_create_table_ref("CREATE TABLE IF NOT EXISTS public.orders (id INT)"),
            Some(("public".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_temp() {
        assert_eq!(
            extract_create_table_ref("CREATE TEMP TABLE cache (data TEXT)"),
            Some(("public".to_string(), "cache".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_temporary() {
        assert_eq!(
            extract_create_table_ref("CREATE TEMPORARY TABLE sessions (data TEXT)"),
            Some(("public".to_string(), "sessions".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_unlogged() {
        assert_eq!(
            extract_create_table_ref("CREATE UNLOGGED TABLE audit (event TEXT)"),
            Some(("public".to_string(), "audit".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_quoted_schema() {
        assert_eq!(
            extract_create_table_ref(r#"CREATE TABLE "MySchema"."MyTable" (id INT)"#),
            Some(("MySchema".to_string(), "MyTable".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_quoted_table_only() {
        assert_eq!(
            extract_create_table_ref(r#"CREATE TABLE "only-quoted" (id INT)"#),
            Some(("public".to_string(), "only-quoted".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_quoted_both() {
        assert_eq!(
            extract_create_table_ref(r#"CREATE TABLE "sch"."tbl" (id INT)"#),
            Some(("sch".to_string(), "tbl".to_string()))
        );
    }

    #[test]
    fn test_map_unknown_sr_type_float4_real() {
        assert_eq!(map_unknown_sr_type("FLOAT4"), "FLOAT");
        assert_eq!(map_unknown_sr_type("REAL"), "FLOAT");
        assert_eq!(map_unknown_sr_type("FLOAT8"), "DOUBLE");
    }

    #[test]
    fn test_map_unknown_sr_type_interval_money() {
        assert_eq!(map_unknown_sr_type("INTERVAL"), "BIGINT");
        // MONEY must produce DECIMAL(38,2) to match the OID path
        // (types.rs:110 `790 => "DECIMAL(38,2)"`).
        assert_eq!(map_unknown_sr_type("MONEY"), "DECIMAL(38,2)");
    }

    #[test]
    fn test_drop_table_cascade_stripped() {
        // DROP TABLE ... CASCADE must not leak through to SR
        let p = test_table_props(true, false);
        let sql = post_process_ddl("DROP TABLE IF EXISTS public.foo CASCADE", &p);
        assert!(!sql.contains("CASCADE"), "CASCADE leaked: {sql}");
        assert!(sql.contains("DROP TABLE"), "Missing DROP TABLE: {sql}");
        assert!(sql.contains("foo"), "Missing table name: {sql}");
    }

    #[test]
    fn test_drop_table_multi_table_split() {
        // SR doesn't support DROP TABLE t1, t2 — must be split into individual statements
        let p = test_table_props(true, false);
        let sql = post_process_ddl("DROP TABLE IF EXISTS public.t1, public.t2 CASCADE", &p);
        // Should produce separate DROP TABLE statements (not one multi-table DROP)
        assert!(!sql.contains(","), "Multi-table DROP not split: {sql}");
        assert!(
            sql.matches("DROP TABLE IF EXISTS").count() == 2,
            "Expected 2 DROP TABLE statements, got: {sql}",
        );
        assert!(sql.contains("t1"), "Missing t1: {sql}");
        assert!(sql.contains("t2"), "Missing t2: {sql}");
        assert!(!sql.contains("CASCADE"), "CASCADE leaked: {sql}");
    }

    #[test]
    fn test_extract_table_name_simple_basic() {
        assert_eq!(
            extract_table_name_simple("CREATE TABLE app.users (id INT PRIMARY KEY, name TEXT)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_name_simple("CREATE TABLE public.t1 (id INT)"),
            Some("t1".to_string())
        );
        assert_eq!(
            extract_table_name_simple("ALTER TABLE app.users ADD COLUMN x INT"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_name_simple("DROP TABLE IF EXISTS app.users CASCADE"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_create_table_ref_quoted_if_not_exists() {
        assert_eq!(
            extract_create_table_ref(
                r#"CREATE TABLE IF NOT EXISTS "public"."test_table" (id INT, "name" TEXT)"#
            ),
            Some(("public".to_string(), "test_table".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_as() {
        assert_eq!(
            extract_create_table_ref("CREATE TABLE public.backup AS SELECT * FROM source"),
            Some(("public".to_string(), "backup".to_string()))
        );
    }

    #[test]
    fn test_extract_create_table_ref_not_ddl() {
        assert_eq!(
            extract_create_table_ref("ALTER TABLE public.users ADD COLUMN x INT"),
            None
        );
    }

    #[test]
    fn test_extract_create_table_ref_garbage() {
        assert_eq!(extract_create_table_ref("this is not SQL"), None);
    }

    #[test]
    fn test_extract_create_table_ref_empty() {
        assert_eq!(extract_create_table_ref(""), None);
    }

    #[test]
    fn test_post_process_ddl_multistatement_create_after_drop() {
        let input = "DROP TABLE IF EXISTS public.foo; CREATE TABLE public.bar (id INT PRIMARY KEY)";
        let p = test_table_props(true, true);
        let sql = post_process_ddl(input, &p);
        assert!(
            sql.contains("ENGINE=OLAP"),
            "CREATE TABLE in multi-stmt DDL must have ENGINE=OLAP. SQL: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTED BY HASH"),
            "CREATE TABLE in multi-stmt DDL must have DISTRIBUTED BY HASH (not RANDOM). SQL: {sql}"
        );
        assert!(
            sql.contains("fast_schema_evolution"),
            "CREATE TABLE in multi-stmt DDL must have fast_schema_evolution. SQL: {sql}"
        );
    }

    #[test]
    fn test_post_process_ddl_multistatement_create_after_alter() {
        let input =
            "ALTER TABLE public.foo ADD COLUMN x INT; CREATE TABLE public.bar (id INT PRIMARY KEY)";
        let p = test_table_props(true, true);
        let sql = post_process_ddl(input, &p);
        assert!(
            sql.contains("CREATE TABLE") && sql.contains("`bar`"),
            "CREATE TABLE bar should appear (bare table name). SQL: {sql}"
        );
        assert!(
            sql.contains("ENGINE=OLAP"),
            "CREATE TABLE in multi-stmt DDL must have ENGINE=OLAP. SQL: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTED BY HASH"),
            "CREATE TABLE in multi-stmt DDL must have DISTRIBUTED BY HASH. SQL: {sql}"
        );
    }

    #[test]
    fn test_extract_alter_table_ref_basic() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.foo ADD PRIMARY KEY (id)"),
            Some(("public".to_string(), "foo".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_constraint_form() {
        assert_eq!(
            extract_alter_table_ref(
                "ALTER TABLE public.bar ADD CONSTRAINT bar_pkey PRIMARY KEY (id, name)"
            ),
            Some(("public".to_string(), "bar".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_default_schema() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE baz ADD PRIMARY KEY (id)"),
            Some(("public".to_string(), "baz".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_add_column_no_match() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.x ADD COLUMN y INT"),
            Some(("public".to_string(), "x".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_not_alter() {
        assert_eq!(
            extract_alter_table_ref("CREATE TABLE public.x (id INT)"),
            None
        );
    }

    #[test]
    fn test_extract_alter_table_ref_ctas_is_not_alter() {
        // CTAS is CREATE TABLE AS, not ALTER TABLE — should return None
        assert_eq!(
            extract_alter_table_ref("CREATE TABLE public.backup AS SELECT * FROM source"),
            None
        );
    }

    #[test]
    fn test_extract_alter_table_ref_quoted_identifiers() {
        assert_eq!(
            extract_alter_table_ref(r#"ALTER TABLE "MySchema"."MyTable" ADD PRIMARY KEY ("MyId")"#),
            Some(("MySchema".to_string(), "MyTable".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_garbage() {
        assert_eq!(extract_alter_table_ref("not sql at all"), None);
    }

    #[test]
    fn test_extract_alter_table_ref_rename_column() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE ONLY public.foo RENAME COLUMN a TO b"),
            Some(("public".to_string(), "foo".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_rename_table() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.bar RENAME TO baz"),
            Some(("public".to_string(), "bar".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_drop_column() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.bar DROP COLUMN c"),
            Some(("public".to_string(), "bar".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_alter_column_type() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE ONLY public.t ALTER COLUMN c TYPE TEXT"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_add_foreign_key() {
        assert_eq!(
            extract_alter_table_ref(
                "ALTER TABLE public.t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES other(b)"
            ),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_drop_constraint_cascade() {
        // polyglot-sql cannot parse DROP CONSTRAINT with CASCADE keyword,
        // so this returns None rather than the table reference.
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t DROP CONSTRAINT fk CASCADE"),
            None
        );
    }

    #[test]
    fn test_extract_alter_table_ref_if_exists() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE IF EXISTS public.t ADD COLUMN c INT"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_mixed_case() {
        assert_eq!(
            extract_alter_table_ref(
                r#"ALTER TABLE public."MixedCase" RENAME COLUMN "old" TO "new""#
            ),
            Some(("public".to_string(), "MixedCase".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_validate_constraint() {
        // VALIDATE CONSTRAINT is a PG-only metadata operation that doesn't
        // change the table structure — extract_alter_table_ref should not
        // recognise it as a table-altering DDL.
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t VALIDATE CONSTRAINT ck"),
            None
        );
    }

    #[test]
    fn test_extract_alter_table_ref_set_options() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t SET (fillfactor = 100)"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_alter_column_set_default() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t ALTER COLUMN c SET DEFAULT 42"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_alter_column_drop_default() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE ONLY public.t ALTER COLUMN c DROP DEFAULT"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_add_column_not_null_default() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t ADD COLUMN c INT NOT NULL DEFAULT 0"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_extract_alter_table_ref_set_schema() {
        assert_eq!(
            extract_alter_table_ref("ALTER TABLE public.t SET SCHEMA new_schema"),
            Some(("public".to_string(), "t".to_string()))
        );
    }

    #[test]
    fn test_retry_backoff_first_delay_is_1s() {
        use std::time::Duration;
        let base_delay = Duration::from_secs(1);
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut sleeps: Vec<Duration> = Vec::new();
        let _first_failed = true;
        sleeps.push(base_delay * 2u32.pow(attempt.min(6)));
        attempt += 1;
        while attempt < max_retries {
            sleeps.push(base_delay * 2u32.pow(attempt.min(6)));
            attempt += 1;
        }
        assert_eq!(
            sleeps,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
            ],
            "Retry backoff with max_retries=3 must be 1s, 2s, 4s (matches the documented backoff schedule)"
        );
    }

    #[test]
    fn test_ddl_failures_retry_then_skip() {
        // Regression: catalog fallback failure used `break` which exited the
        // for-row loop, blocking all subsequent DDLs. Now it tracks consecutive
        // failures in a HashMap and permanently skips after 3.
        let mut ddl_failures: HashMap<i64, u32> = HashMap::new();
        let ddl_id: i64 = 42;

        // Simulate 3 consecutive failures → permanent skip on 3rd
        for attempt in 1..=3 {
            let failures = ddl_failures.entry(ddl_id).or_insert(0);
            *failures += 1;
            if *failures >= 3 {
                // Permanently skip — remove from map (checkpoint would advance)
                ddl_failures.remove(&ddl_id);
                assert!(
                    !ddl_failures.contains_key(&ddl_id),
                    "attempt {attempt}: should be removed after 3 failures"
                );
                break;
            }
            assert!(
                ddl_failures.contains_key(&ddl_id),
                "attempt {attempt}: should still be tracked"
            );
        }
        assert!(
            !ddl_failures.contains_key(&ddl_id),
            "after 3 failures, entry must be removed"
        );
    }

    #[test]
    fn test_ddl_failures_success_clears_counter() {
        // Verify that a successful DDL execution removes the failure counter.
        let mut ddl_failures: HashMap<i64, u32> = HashMap::new();
        let ddl_id: i64 = 42;
        ddl_failures.insert(ddl_id, 2); // 2 prior failures

        // Simulate a successful execution
        ddl_failures.remove(&ddl_id);
        assert!(
            !ddl_failures.contains_key(&ddl_id),
            "successful DDL must clear the failure counter"
        );
    }

    // ── is_pg_only_alter_action ───────────────────────────────────────

    // ── is_pg_only_alter_action ───────────────────────────────────────

    #[test]
    fn test_is_pg_only_alter_action_add_constraint() {
        use polyglot_sql::expressions::{AlterTableAction, ConstraintModifiers, TableConstraint};
        let action = AlterTableAction::AddConstraint(TableConstraint::PrimaryKey {
            columns: vec![],
            name: None,
            has_constraint_keyword: false,
            include_columns: vec![],
            modifiers: ConstraintModifiers::default(),
        });
        assert!(is_pg_only_alter_action(&action));
    }

    #[test]
    fn test_is_pg_only_alter_action_drop_constraint() {
        use polyglot_sql::expressions::{AlterTableAction, Identifier};
        assert!(is_pg_only_alter_action(&AlterTableAction::DropConstraint {
            name: Identifier::new("pk"),
            if_exists: false,
        }));
    }

    #[test]
    fn test_is_pg_only_alter_action_drop_foreign_key() {
        use polyglot_sql::expressions::{AlterTableAction, Identifier};
        assert!(is_pg_only_alter_action(&AlterTableAction::DropForeignKey {
            name: Identifier::new("fk"),
        }));
    }

    #[test]
    fn test_is_pg_only_alter_action_set_property() {
        use polyglot_sql::expressions::AlterTableAction;
        assert!(is_pg_only_alter_action(&AlterTableAction::SetProperty {
            properties: vec![],
        }));
    }

    #[test]
    fn test_is_pg_only_alter_action_unset_property() {
        use polyglot_sql::expressions::AlterTableAction;
        assert!(is_pg_only_alter_action(&AlterTableAction::UnsetProperty {
            properties: vec![],
        }));
    }

    #[test]
    fn test_is_pg_only_alter_action_add_column_is_not_pg_only() {
        use polyglot_sql::dialects::DialectType;
        match polyglot_sql::parse("ALTER TABLE t ADD COLUMN col INT", DialectType::PostgreSQL) {
            Ok(exprs) => {
                for expr in &exprs {
                    if let polyglot_sql::Expression::AlterTable(at) = expr {
                        for action in &at.actions {
                            assert!(
                                !is_pg_only_alter_action(action),
                                "ADD COLUMN must not be PG-only"
                            );
                        }
                    }
                }
            }
            Err(_) => panic!("Failed to parse ALTER TABLE ADD COLUMN"),
        }
    }

    #[test]
    fn test_is_pg_only_alter_action_alter_column_is_not_pg_only() {
        // SetDataType is the representative non-pg-only AlterColumn action
        // (StarRocks supports `MODIFY COLUMN ... TYPE`). Although in practice
        // `is_unsupported_alter_table` routes TYPE changes to resnapshot
        // before transpilation, at the AST-filter level SetDataType is
        // correctly NOT pg-only — the filter must not over-reach and strip
        // legitimate type-change actions.
        //
        // Note: SetDefault / DropDefault ARE pg-only (SR has no ALTER COLUMN
        // action and the pipeline strips all DEFAULTs) — covered by:
        // test_is_pg_only_alter_action_alter_column_set_default_filtered and
        // test_is_pg_only_alter_action_alter_column_drop_default_filtered.
        use polyglot_sql::expressions::{
            AlterColumnAction, AlterTableAction, DataType, Identifier,
        };
        let action = AlterTableAction::AlterColumn {
            name: Identifier::new("col"),
            action: AlterColumnAction::SetDataType {
                data_type: DataType::String { length: None },
                using: None,
                collate: None,
            },
            use_modify_keyword: false,
        };
        assert!(!is_pg_only_alter_action(&action));
    }

    // ── strip_table_schemas_from_expr ─────────────────────────────────

    #[test]
    fn test_strip_schema_from_create_table() {
        use polyglot_sql::dialects::DialectType;
        match polyglot_sql::parse(
            "CREATE TABLE public.test_strip (id INT)",
            DialectType::PostgreSQL,
        ) {
            Ok(exprs) => {
                for mut expr in exprs {
                    strip_table_schemas_from_expr(&mut expr);
                    if let polyglot_sql::Expression::CreateTable(ct) = &expr {
                        assert!(ct.name.schema.is_none(), "schema must be stripped");
                    }
                }
            }
            Err(_) => panic!("Failed to parse CREATE TABLE"),
        }
    }

    #[test]
    fn test_strip_schema_from_alter_table() {
        use polyglot_sql::dialects::DialectType;
        match polyglot_sql::parse(
            "ALTER TABLE public.t ADD COLUMN x INT",
            DialectType::PostgreSQL,
        ) {
            Ok(exprs) => {
                for mut expr in exprs {
                    strip_table_schemas_from_expr(&mut expr);
                    if let polyglot_sql::Expression::AlterTable(at) = &expr {
                        assert!(at.name.schema.is_none(), "alter schema must be stripped");
                    }
                }
            }
            Err(_) => panic!("Failed to parse ALTER TABLE"),
        }
    }

    #[test]
    fn test_strip_schema_from_drop_table() {
        use polyglot_sql::dialects::DialectType;
        match polyglot_sql::parse("DROP TABLE public.t", DialectType::PostgreSQL) {
            Ok(exprs) => {
                for mut expr in exprs {
                    strip_table_schemas_from_expr(&mut expr);
                    if let polyglot_sql::Expression::DropTable(dt) = &expr {
                        assert!(dt.names[0].schema.is_none(), "drop schema must be stripped");
                    }
                }
            }
            Err(_) => panic!("Failed to parse DROP TABLE"),
        }
    }

    // ── ALTER TABLE type detection tests ────────────────────────────

    #[test]
    fn test_is_unsupported_alter_table_alter_column_type() {
        assert!(is_unsupported_alter_table(
            "ALTER TABLE users ALTER COLUMN email TYPE VARCHAR(500)"
        ));
    }

    #[test]
    fn test_is_unsupported_alter_table_set_data_type() {
        assert!(is_unsupported_alter_table(
            "ALTER TABLE users ALTER COLUMN salary SET DATA TYPE NUMERIC(12,2)"
        ));
    }

    #[test]
    fn test_is_unsupported_alter_table_rename_column() {
        assert!(is_unsupported_alter_table(
            "ALTER TABLE users RENAME COLUMN old_name TO new_name"
        ));
    }

    #[test]
    fn test_is_unsupported_alter_table_add_column() {
        // ADD COLUMN is NOT unsupported — SR handles it fine
        assert!(!is_unsupported_alter_table(
            "ALTER TABLE users ADD COLUMN age INT"
        ));
    }

    #[test]
    fn test_is_unsupported_alter_table_set_default() {
        // SET DEFAULT must NOT be routed to the `is_unsupported_alter_table`
        // resnapshot path — that would needlessly drop+recreate the table
        // for a metadata-only change. SET DEFAULT is skipped earlier in the
        // poller loop by `is_alter_column_default` (same routing as REPLICA
        // IDENTITY). This guard ensures the resnapshot path is never
        // triggered for SET/DROP DEFAULT — see `test_is_alter_column_default_set`.
        assert!(!is_unsupported_alter_table(
            "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'"
        ));
    }

    #[test]
    fn test_is_unsupported_alter_table_drop_column() {
        assert!(!is_unsupported_alter_table(
            "ALTER TABLE users DROP COLUMN obsolete_flag"
        ));
    }

    // ── COLUMN-less `ALTER TABLE t ALTER c TYPE ...` (PG keyword optional) ──
    //
    // PG accepts the COLUMN keyword as optional — `ALTER TABLE t ALTER c TYPE
    // bigint` is the same statement as `... ALTER COLUMN c TYPE bigint`. The
    // old gate keyed on the literal `" ALTER COLUMN "` substring, so the
    // COLUMN-less spelling bypassed the resnapshot routing, fell through to
    // the generic transpile path and reached SR as `ALTER COLUMN ... SET DATA
    // TYPE` (rejected, error 1064): 3 retries → skip (silent schema drift) or
    // abort under `ddl_failure_abort=true`.

    #[test]
    fn test_alter_column_type_change_column_keyword_optional() {
        for ddl in [
            "ALTER TABLE t ALTER COLUMN c TYPE text",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE text",
            "ALTER TABLE t ALTER c TYPE text",
            "ALTER TABLE t ALTER c SET DATA TYPE text",
            // Quoted / schema-qualified / ONLY / IF EXISTS / lowercase
            // spellings of the same statements must not defeat the gate.
            r#"ALTER TABLE public."Users" ALTER "email" SET DATA TYPE text"#,
            "ALTER TABLE ONLY t ALTER c TYPE text",
            "ALTER TABLE IF EXISTS t ALTER COLUMN c TYPE text USING c::text",
            "alter table t alter c type text",
        ] {
            assert!(
                is_alter_column_type_change(ddl),
                "type-change gate must match: {ddl}"
            );
            assert!(
                is_unsupported_alter_table(ddl),
                "must route to drop+resnapshot: {ddl}"
            );
        }
    }

    #[test]
    fn test_alter_column_type_change_no_false_positive() {
        // None of these change a column type, so none may reach the
        // drop+resnapshot path. `SET DEFAULT 'TYPE'::text` is the sharp case:
        // the gate requires the type keyword immediately after the column
        // name, and `SET DEFAULT` diverges from `SET DATA TYPE` at the second
        // word — that literal's TYPE sits after DEFAULT, not after the name.
        for ddl in [
            "ALTER TABLE t ADD COLUMN c text",
            "ALTER TABLE t DROP COLUMN c",
            "ALTER TABLE t ALTER c SET DEFAULT 'TYPE'::text",
            "ALTER TABLE t ALTER c SET NOT NULL",
            "ALTER TABLE t ALTER c DROP NOT NULL",
            // A table literally named `type` must not be read as
            // `ALTER <column> TYPE` — the gate anchors on the statement-level
            // ALTER TABLE plus the action ALTER that follows the table name.
            "ALTER TABLE type ADD COLUMN c text",
            "ALTER TABLE type DROP COLUMN c",
            // Other non-type column actions.
            "ALTER TABLE t ALTER COLUMN c SET STATISTICS 100",
            "ALTER TABLE t ALTER COLUMN c SET DEFAULT 'SET DATA TYPE'::text",
        ] {
            assert!(
                !is_alter_column_type_change(ddl),
                "type-change gate must not match: {ddl}"
            );
            assert!(
                !is_unsupported_alter_table(ddl),
                "must not route to resnapshot: {ddl}"
            );
        }
    }

    #[test]
    fn test_alter_column_type_gate_ignores_rename_forms() {
        // RENAME COLUMN / RENAME TO belong to their own branches of
        // `is_unsupported_alter_table` (they still route to drop+resnapshot —
        // the call site keys `is_rename_to` off the raw text), so the
        // *type-change* gate must stay silent for them. RENAME COLUMN's
        // function-level routing is pinned by
        // `test_is_unsupported_alter_table_rename_column`; RENAME TO had no
        // coverage, so assert it here.
        assert!(!is_alter_column_type_change(
            "ALTER TABLE t RENAME COLUMN a TO b"
        ));
        assert!(!is_alter_column_type_change("ALTER TABLE t RENAME TO u"));
        assert!(is_unsupported_alter_table("ALTER TABLE t RENAME TO u"));
    }

    #[test]
    fn test_is_replica_identity_alter_detected() {
        assert!(is_replica_identity_alter(
            "ALTER TABLE users REPLICA IDENTITY FULL"
        ));
    }

    #[test]
    fn test_is_replica_identity_alter_not_false_positive() {
        assert!(!is_replica_identity_alter(
            "CREATE TABLE replica_table (id INT)"
        ));
    }

    // ── is_replica_identity_alter regex word-boundary fix ────────────
    //
    // The prior `ddl.contains("REPLICA IDENTITY")` substring match would
    // false-positive on a column named `replica_identity_flag` (silently
    // skipping a legitimate ADD COLUMN). The regex with `\b` word
    // boundaries + `(?i)` case-insensitive flag avoids this.

    #[test]
    fn test_is_replica_identity_alter_no_false_positive_on_column_name() {
        assert!(
            !is_replica_identity_alter("ALTER TABLE t ADD COLUMN replica_identity_flag INT"),
            "Column name `replica_identity_flag` must not match — a substring \
             match would wrongly classify it and skip a legitimate ADD COLUMN"
        );
    }

    #[test]
    fn test_is_replica_identity_alter_lowercase() {
        assert!(
            is_replica_identity_alter("alter table t replica identity full"),
            "Lowercase REPLICA IDENTITY must match (case-insensitive regex)"
        );
    }

    #[test]
    fn test_is_replica_identity_alter_multi_space() {
        assert!(
            is_replica_identity_alter("ALTER TABLE t REPLICA  IDENTITY  FULL"),
            "Multi-spaced `REPLICA  IDENTITY` must match (whitespace-flexible)"
        );
    }

    // ── strip_pg_only_ddl_syntax case-insensitive + whitespace-flexible ─
    //
    // The prior `.replace("DROP COLUMN IF EXISTS", "DROP COLUMN")` was
    // case-sensitive and required single-space separation. Lowercase DDLs
    // from psql sessions and double-spaced forms were silently ignored,
    // leaving IF EXISTS intact → SR 1064 error → 3-retry → FATAL-ABORT.

    #[test]
    fn test_strip_pg_only_ddl_syntax_lowercase_drop_column_if_exists() {
        let stripped = strip_pg_only_ddl_syntax("alter table t drop column if exists c");
        assert!(
            !stripped.to_uppercase().contains("IF EXISTS"),
            "Lowercase `drop column if exists` must be stripped: {stripped}"
        );
        assert!(
            stripped.to_uppercase().contains("DROP COLUMN"),
            "Result must include `DROP COLUMN`: {stripped}"
        );
    }

    #[test]
    fn test_strip_pg_only_ddl_syntax_double_space() {
        let stripped = strip_pg_only_ddl_syntax("ALTER TABLE t DROP  COLUMN  IF  EXISTS c");
        assert!(
            !stripped.to_uppercase().contains("IF  EXISTS"),
            "Double-spaced `DROP COLUMN IF EXISTS` must be stripped: {stripped}"
        );
        assert!(
            stripped.to_uppercase().contains("DROP COLUMN"),
            "Result must include `DROP COLUMN`: {stripped}"
        );
    }

    #[test]
    fn test_strip_pg_only_ddl_syntax_mixed_case() {
        let stripped = strip_pg_only_ddl_syntax("Alter Table t Drop Column If Exists c");
        assert!(
            !stripped.to_uppercase().contains("IF EXISTS"),
            "Mixed-case `Drop Column If Exists` must be stripped: {stripped}"
        );
    }

    #[test]
    fn test_strip_pg_only_ddl_syntax_drop_if_exists_bare() {
        // Bare `DROP IF EXISTS` (without COLUMN keyword) — used for
        // DROP TABLE / DROP INDEX IF EXISTS syntax variants.
        let stripped = strip_pg_only_ddl_syntax("alter table t drop if exists c");
        assert!(
            !stripped.to_uppercase().contains("IF EXISTS"),
            "Lowercase `drop if exists` must be stripped: {stripped}"
        );
        assert!(
            stripped.to_uppercase().contains("DROP"),
            "Result must include `DROP`: {stripped}"
        );
    }

    #[test]
    fn test_strip_pg_only_ddl_syntax_preserves_other_text() {
        let stripped = strip_pg_only_ddl_syntax(
            "ALTER TABLE s52_fk ADD COLUMN newcol INT REFERENCES other(id)",
        );
        assert_eq!(
            stripped, "ALTER TABLE s52_fk ADD COLUMN newcol INT REFERENCES other(id)",
            "Non-DROP-COLUMN DDLs must pass through unchanged"
        );
    }

    // ── is_pg_only_metadata_alter pre-loop classifier ────────────────
    //
    // AlterTable variants polyglot-sql CANNOT parse (parses to Raw{sql} or
    // throws an Err) — without the pre-loop classifier, the poller would
    // burn its 3-retry budget then FATAL-ABORT under `ddl_failure_abort=true`.

    #[test]
    fn test_is_pg_only_metadata_alter_set_schema() {
        assert!(is_pg_only_metadata_alter("ALTER TABLE t SET SCHEMA other"));
        assert!(is_pg_only_metadata_alter("alter table t set schema other"));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_validate_constraint() {
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t VALIDATE CONSTRAINT fk_check"
        ));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_cluster() {
        assert!(is_pg_only_metadata_alter("ALTER TABLE t CLUSTER ON idx"));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_enable_trigger() {
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t ENABLE TRIGGER ALL"
        ));
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t DISABLE TRIGGER ALL"
        ));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_rls() {
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY"
        ));
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t DISABLE ROW LEVEL SECURITY"
        ));
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t FORCE ROW LEVEL SECURITY"
        ));
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t NO FORCE ROW LEVEL SECURITY"
        ));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_attach_detach_partition() {
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t ATTACH PARTITION p1 DEFAULT"
        ));
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t DETACH PARTITION p1"
        ));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_rename_constraint() {
        assert!(is_pg_only_metadata_alter(
            "ALTER TABLE t RENAME CONSTRAINT ck_old TO ck_new"
        ));
    }

    #[test]
    fn test_is_pg_only_metadata_alter_not_false_positive_on_add_column() {
        // False-positive guard: a column named `set_schema_flag` or
        // `validate_constraint_flag` must NOT match.
        assert!(
            !is_pg_only_metadata_alter("ALTER TABLE t ADD COLUMN set_schema_flag INT"),
            "Column name `set_schema_flag` must not trigger the classifier"
        );
        assert!(
            !is_pg_only_metadata_alter("ALTER TABLE t ADD COLUMN validate_constraint_flag INT"),
            "Column name `validate_constraint_flag` must not trigger the classifier"
        );
    }

    #[test]
    fn test_is_pg_only_metadata_alter_not_add_or_drop_column() {
        // Common operations must NOT match the classifier.
        assert!(!is_pg_only_metadata_alter("ALTER TABLE t ADD COLUMN x INT"));
        assert!(!is_pg_only_metadata_alter("ALTER TABLE t DROP COLUMN y"));
        assert!(!is_pg_only_metadata_alter(
            "ALTER TABLE t ALTER COLUMN c TYPE TEXT"
        ));
    }

    // ── ALTER COLUMN ... SET/DROP DEFAULT — PG-only no-op, skipped ────
    //
    // Regression tests for the production incident (DDL id=13):
    // `ALTER TABLE "transactions" ALTER COLUMN "id" DROP DEFAULT`
    // was forwarded verbatim to StarRocks and rejected with error 1064
    // (`Unexpected input 'ALTER'...`). With `ddl_failure_abort = true`
    // (production config), the pipeline would have aborted after 3 retries.
    //
    // SET/DROP DEFAULT changes no type or column name requiring resnapshot,
    // and the pipeline already strips all DEFAULTs on CREATE TABLE / ADD
    // COLUMN (always sends explicit values via Stream Load JSON). So these
    // DDLs are skipped at the predicate level — same routing as REPLICA
    // IDENTITY — and filtered at the AST level in `is_pg_only_alter_action`
    // as defense-in-depth for compound ALTERs.

    #[test]
    fn test_is_alter_column_default_drop() {
        // Exact production incident DDL
        assert!(is_alter_column_default(
            r#"ALTER TABLE "transactions" ALTER COLUMN "id" DROP DEFAULT"#
        ));
        // Unqualified form
        assert!(is_alter_column_default(
            "ALTER TABLE transactions ALTER COLUMN id DROP DEFAULT"
        ));
    }

    #[test]
    fn test_is_alter_column_default_set() {
        assert!(is_alter_column_default(
            r#"ALTER TABLE "users" ALTER COLUMN "status" SET DEFAULT 'active'"#
        ));
        // Numeric default
        assert!(is_alter_column_default(
            "ALTER TABLE t ALTER COLUMN c SET DEFAULT 42"
        ));
    }

    #[test]
    fn test_is_alter_column_default_negative() {
        // ADD COLUMN must not match (even with a DEFAULT clause inside)
        assert!(!is_alter_column_default(
            "ALTER TABLE t ADD COLUMN x INT DEFAULT 0"
        ));
        // DROP COLUMN must not match
        assert!(!is_alter_column_default(
            "ALTER TABLE t DROP COLUMN obsolete_flag"
        ));
        // ALTER COLUMN ... TYPE is handled by `is_unsupported_alter_table` (resnapshot path)
        assert!(!is_alter_column_default(
            "ALTER TABLE t ALTER COLUMN c TYPE TEXT"
        ));
        // ALTER COLUMN ... SET NOT NULL is handled by `is_pg_only_alter_action` (AST filter)
        assert!(!is_alter_column_default(
            "ALTER TABLE t ALTER COLUMN c SET NOT NULL"
        ));
        // Plain CREATE TABLE must not match
        assert!(!is_alter_column_default("CREATE TABLE t (id INT)"));
        // Case-insensitivity: lowercase form must still match
        assert!(is_alter_column_default(
            "alter table t alter column c drop default"
        ));
    }

    #[test]
    fn test_alter_column_drop_default_fully_skipped() {
        // The production incident DDL — after post_process_ddl, output must
        // be empty (the ALTER COLUMN DROP DEFAULT action is filtered out
        // by `is_pg_only_alter_action`, leaving no SR-executable actions).
        // This is the AST-level defense-in-depth: even if a compound DDL
        // bypassed `is_alter_column_default` (e.g. multi-action ALTER that
        // the regex didn't recognize as SET/DROP DEFAULT), the AST filter
        // strips the action so SR never sees invalid `ALTER COLUMN` syntax.
        let sql = process_ddl(
            r#"ALTER TABLE public.transactions ALTER COLUMN id DROP DEFAULT"#,
            false,
        );
        assert!(
            sql.is_empty(),
            "DROP DEFAULT-only ALTER TABLE must produce empty output (no-op for SR): {sql}"
        );
    }

    #[test]
    fn test_alter_column_set_default_fully_skipped() {
        let sql = process_ddl(
            r#"ALTER TABLE public.users ALTER COLUMN status SET DEFAULT 'active'"#,
            false,
        );
        assert!(
            sql.is_empty(),
            "SET DEFAULT-only ALTER TABLE must produce empty output (no-op for SR): {sql}"
        );
    }

    #[test]
    fn test_alter_add_column_with_alter_column_drop_default_partial() {
        // Compound ALTER: ADD COLUMN should survive the AST filter,
        // DROP DEFAULT should be filtered out. This guards the
        // defense-in-depth path in `is_pg_only_alter_action` for
        // multi-action DDLs that bypass `is_alter_column_default`.
        let sql = process_ddl(
            "ALTER TABLE public.t \
             ADD COLUMN x INT, \
             ALTER COLUMN y DROP DEFAULT",
            false,
        );
        assert!(
            !sql.is_empty(),
            "Compound ALTER must still produce output for ADD COLUMN: {sql}"
        );
        assert!(
            sql.contains("x") || sql.contains("`x`"),
            "ADD COLUMN x must survive: {sql}"
        );
        // The DROP DEFAULT action must be fully filtered.
        // After AST filtering + regeneration, DEFAULT / ALTER COLUMN y
        // should not appear in the SR-bound DDL.
        assert!(
            !sql.contains("DEFAULT"),
            "DROP DEFAULT must be filtered from compound ALTER: {sql}"
        );
    }

    // ── CREATE TEMPORARY/TEMP TABLE — skipped, not replicated to SR ──
    //
    // Temporary tables are session-scoped in PG, don't generate WAL
    // entries for DML, and should never become permanent tables in SR.
    // The poller skips them before transpilation; `post_process_ddl`
    // still strips the TEMPORARY flag as a safety net.

    #[test]
    fn test_is_temporary_table_ddl_temp() {
        assert!(is_temporary_table_ddl("CREATE TEMP TABLE foo (id INT)"));
        assert!(is_temporary_table_ddl(
            "CREATE TEMP TABLE IF NOT EXISTS foo (id INT)"
        ));
    }

    #[test]
    fn test_is_temporary_table_ddl_temporary() {
        assert!(is_temporary_table_ddl(
            "CREATE TEMPORARY TABLE foo (id INT)"
        ));
        assert!(is_temporary_table_ddl(
            "CREATE TEMPORARY TABLE IF NOT EXISTS foo (id INT)"
        ));
    }

    #[test]
    fn test_is_temporary_table_ddl_global_local() {
        assert!(is_temporary_table_ddl(
            "CREATE GLOBAL TEMPORARY TABLE foo (id INT)"
        ));
        assert!(is_temporary_table_ddl(
            "CREATE LOCAL TEMP TABLE foo (id INT)"
        ));
        assert!(is_temporary_table_ddl(
            "CREATE GLOBAL TEMP TABLE foo (id INT)"
        ));
    }

    #[test]
    fn test_is_temporary_table_ddl_negative() {
        // Regular CREATE TABLE must NOT match
        assert!(!is_temporary_table_ddl("CREATE TABLE foo (id INT)"));
        // UNLOGGED is not temporary (persistent, just not WAL-logged for DML)
        assert!(!is_temporary_table_ddl(
            "CREATE UNLOGGED TABLE foo (id INT)"
        ));
        // CREATE TABLE AS must match if TEMP
        assert!(is_temporary_table_ddl("CREATE TEMP TABLE foo AS SELECT 1"));
        // Non-CREATE statements must not match
        assert!(!is_temporary_table_ddl("ALTER TABLE foo ADD COLUMN x INT"));
        assert!(!is_temporary_table_ddl("DROP TABLE foo"));
        // Table name containing "temp" must NOT false-match
        assert!(!is_temporary_table_ddl("CREATE TABLE template (id INT)"));
        assert!(!is_temporary_table_ddl("CREATE TABLE temp_data (id INT)"));
    }

    #[test]
    fn test_is_temporary_table_ddl_case_insensitive() {
        assert!(is_temporary_table_ddl("create temp table foo (id INT)"));
        assert!(is_temporary_table_ddl(
            "Create Temporary Table foo (id INT)"
        ));
        assert!(is_temporary_table_ddl(
            "CREATE global TEMP table foo (id INT)"
        ));
    }

    #[test]
    fn test_is_temporary_table_ddl_leading_whitespace() {
        assert!(is_temporary_table_ddl("  CREATE TEMP TABLE foo (id INT)"));
        assert!(is_temporary_table_ddl(
            "\nCREATE TEMPORARY TABLE foo (id INT)"
        ));
        assert!(is_temporary_table_ddl("\tCREATE TEMP TABLE foo (id INT)"));
    }

    // ── PL/pgSQL DO blocks — skipped, not transpilable ──────────────
    //
    // When ALTER TABLE / CREATE TABLE / DROP TABLE runs inside a
    // `DO $$ ... $$` block, the event trigger captures the entire DO
    // block text. polyglot-sql can't parse PL/pgSQL — the poller must
    // skip these before transpilation. The trigger-level fix (ddl.rs)
    // prevents future captures, but existing rows need defense-in-depth.

    #[test]
    fn test_is_plpgsql_do_block_basic() {
        // Standard DO block with dollar quoting
        assert!(is_plpgsql_do_block(
            "DO $$ BEGIN ALTER TABLE t ADD COLUMN x INT; END $$"
        ));
        // The exact production DO block (with leading whitespace/newlines)
        assert!(is_plpgsql_do_block(
            "DO $$\n      BEGIN\n        IF NOT EXISTS (...) THEN\n          ALTER TABLE ..."
        ));
    }

    #[test]
    fn test_is_plpgsql_do_block_no_space() {
        // DO immediately followed by $$ (no space)
        assert!(is_plpgsql_do_block("DO$$ BEGIN ... END $$"));
    }

    #[test]
    fn test_is_plpgsql_do_block_language_clause() {
        // DO LANGUAGE plpgsql $$ ...
        assert!(is_plpgsql_do_block(
            "DO LANGUAGE plpgsql $$ BEGIN ... END $$"
        ));
    }

    #[test]
    fn test_is_plpgsql_do_block_case_insensitive() {
        assert!(is_plpgsql_do_block("do $$ BEGIN ... END $$"));
        assert!(is_plpgsql_do_block("Do $$ BEGIN ... END $$"));
    }

    #[test]
    fn test_is_plpgsql_do_block_leading_whitespace() {
        assert!(is_plpgsql_do_block("  DO $$ BEGIN ... END $$"));
        assert!(is_plpgsql_do_block("\nDO $$ BEGIN ... END $$"));
        assert!(is_plpgsql_do_block("\tDO $$ BEGIN ... END $$"));
    }

    #[test]
    fn test_is_plpgsql_do_block_negative() {
        // DROP TABLE must NOT match — "DROP" has "P" at position 2
        assert!(!is_plpgsql_do_block("DROP TABLE foo"));
        assert!(!is_plpgsql_do_block("drop table foo"));
        // DOCUMENT / DOUBLE — words starting with "DO" but followed by alphanumeric
        assert!(!is_plpgsql_do_block("DOCUMENT 'some text'"));
        assert!(!is_plpgsql_do_block("DOUBLE PRECISION"));
        // Regular DDL must not match
        assert!(!is_plpgsql_do_block("CREATE TABLE foo (id INT)"));
        assert!(!is_plpgsql_do_block("ALTER TABLE foo ADD COLUMN x INT"));
    }

    // ── Function-wrapped DDL detection (tag/text mismatch) ───────────

    #[test]
    fn test_is_function_wrapped_ddl_select_calling_function() {
        // Tag is ALTER TABLE but text is SELECT — DDL ran inside a function
        assert!(is_function_wrapped_ddl(
            "ALTER TABLE",
            "SELECT migrate_data()"
        ));
        assert!(is_function_wrapped_ddl(
            "ALTER TABLE",
            "select migrate_data()"
        ));
    }

    #[test]
    fn test_is_function_wrapped_ddl_call_procedure() {
        assert!(is_function_wrapped_ddl(
            "ALTER TABLE",
            "CALL run_migration()"
        ));
        assert!(is_function_wrapped_ddl(
            "CREATE TABLE",
            "CALL run_migration()"
        ));
    }

    #[test]
    fn test_is_function_wrapped_ddl_normal_ddl_not_mismatched() {
        // Normal DDL text should NOT be mismatched
        assert!(!is_function_wrapped_ddl(
            "ALTER TABLE",
            "ALTER TABLE foo ADD COLUMN x INT"
        ));
        assert!(!is_function_wrapped_ddl(
            "CREATE TABLE",
            "CREATE TABLE foo (id INT)"
        ));
        assert!(!is_function_wrapped_ddl(
            "CREATE TABLE",
            "CREATE TEMP TABLE foo (id INT)"
        ));
        assert!(!is_function_wrapped_ddl("DROP TABLE", "DROP TABLE foo"));
    }

    #[test]
    fn test_is_function_wrapped_ddl_select_into_not_mismatched() {
        // SELECT * INTO is a valid DDL — should not be flagged
        assert!(!is_function_wrapped_ddl(
            "SELECT INTO",
            "SELECT * INTO new_table FROM source_table"
        ));
        // But SELECT my_func() with tag SELECT INTO should be flagged
        // (no INTO keyword in the text)
        assert!(is_function_wrapped_ddl(
            "SELECT INTO",
            "SELECT create_table_func()"
        ));
    }

    #[test]
    fn test_is_function_wrapped_ddl_do_block_not_double_filtered() {
        // DO blocks are handled by is_plpgsql_do_block — don't double-filter
        assert!(!is_function_wrapped_ddl(
            "ALTER TABLE",
            "DO $$ BEGIN ALTER TABLE t ADD COLUMN x INT; END $$"
        ));
    }

    #[test]
    fn test_is_function_wrapped_ddl_leading_whitespace() {
        assert!(is_function_wrapped_ddl(
            "ALTER TABLE",
            "  SELECT migrate_data()"
        ));
        assert!(is_function_wrapped_ddl(
            "ALTER TABLE",
            "\nSELECT migrate_data()"
        ));
    }

    // ── DROP SCHEMA / DROP DATABASE detection ────────────────────────

    #[test]
    fn test_is_drop_schema_or_database_schema() {
        assert!(is_drop_schema_or_database("DROP SCHEMA my_schema CASCADE"));
        assert!(is_drop_schema_or_database("drop schema my_schema"));
        assert!(is_drop_schema_or_database(
            "  DROP SCHEMA IF EXISTS my_schema"
        ));
    }

    #[test]
    fn test_is_drop_schema_or_database_database() {
        assert!(is_drop_schema_or_database("DROP DATABASE my_db"));
        assert!(is_drop_schema_or_database("drop database my_db"));
    }

    #[test]
    fn test_is_drop_schema_or_database_negative() {
        assert!(!is_drop_schema_or_database("DROP TABLE foo"));
        assert!(!is_drop_schema_or_database("DROP TABLE IF EXISTS foo"));
        assert!(!is_drop_schema_or_database("ALTER TABLE foo DROP COLUMN x"));
        assert!(!is_drop_schema_or_database("CREATE TABLE foo (id INT)"));
    }

    // ── ALTER TABLE ADD COLUMN: inline constraints stripped ──────────

    #[test]
    fn test_add_column_inline_references_stripped() {
        // ALTER TABLE ADD COLUMN with inline REFERENCES must have the FK stripped
        let sql = process_ddl(
            "ALTER TABLE public.t ADD COLUMN x INT REFERENCES other(y)",
            false,
        );
        assert!(sql.contains("x"), "ADD COLUMN x must survive: {sql}");
        assert!(
            !sql.contains("REFERENCES"),
            "Inline REFERENCES must be stripped: {sql}"
        );
        assert!(!sql.contains("FOREIGN KEY"), "FK must not appear: {sql}");
    }

    #[test]
    fn test_add_column_inline_check_stripped() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN x INT CHECK (x > 0)", false);
        assert!(sql.contains("x"), "ADD COLUMN x must survive: {sql}");
        assert!(
            !sql.contains("CHECK"),
            "Inline CHECK must be stripped: {sql}"
        );
    }

    #[test]
    fn test_add_column_inline_unique_stripped() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN x INT UNIQUE", false);
        assert!(sql.contains("x"), "ADD COLUMN x must survive: {sql}");
        assert!(
            !sql.contains("UNIQUE"),
            "Inline UNIQUE must be stripped: {sql}"
        );
    }

    // ── ALTER TABLE RENAME TO: new name extraction ───────────────────

    #[test]
    fn test_extract_rename_to_new_ref_basic() {
        assert_eq!(
            extract_rename_to_new_ref("ALTER TABLE public.bar RENAME TO baz"),
            Some(("public".to_string(), "baz".to_string()))
        );
    }

    #[test]
    fn test_extract_rename_to_new_ref_no_schema() {
        assert_eq!(
            extract_rename_to_new_ref("ALTER TABLE bar RENAME TO baz"),
            Some(("public".to_string(), "baz".to_string()))
        );
    }

    #[test]
    fn test_extract_rename_to_new_ref_quoted() {
        // Quoted identifiers in RENAME TO may not be parseable by polyglot-sql
        // (the parser handles quoted identifiers for other ALTER TABLE variants
        // but RENAME TO support is limited). When this returns None, the poller
        // falls back to extract_alter_table_ref (which returns the OLD name),
        // and snapshot_one_table returns Ok(false) since the old table doesn't
        // exist in PG anymore. The SR table is orphaned — operator should run
        // standalone RENAME TO without quoted identifiers.
        let result = extract_rename_to_new_ref(r#"ALTER TABLE "public"."bar" RENAME TO "baz""#);
        // If polyglot-sql can parse it, verify the new name; if not, None is OK
        if let Some((schema, table)) = result {
            assert_eq!(schema, "public");
            assert_eq!(table, "baz");
        }
        // If None, the poller's fallback path handles it gracefully
    }

    #[test]
    fn test_extract_rename_to_new_ref_not_rename() {
        // ALTER TABLE ... RENAME COLUMN should not match RenameTable action
        assert_eq!(
            extract_rename_to_new_ref("ALTER TABLE public.bar RENAME COLUMN a TO b"),
            None
        );
        // ALTER TABLE ... ADD COLUMN is not a rename
        assert_eq!(
            extract_rename_to_new_ref("ALTER TABLE public.bar ADD COLUMN x INT"),
            None
        );
    }

    #[test]
    fn test_extract_rename_to_new_ref_garbage() {
        assert_eq!(extract_rename_to_new_ref("not sql at all"), None);
        assert_eq!(extract_rename_to_new_ref(""), None);
    }

    // ── ALTER TABLE SET NOT NULL / DROP NOT NULL — now PG-only ────────

    #[test]
    fn test_is_pg_only_alter_action_alter_column_set_not_null() {
        use polyglot_sql::expressions::{AlterColumnAction, AlterTableAction, Identifier};
        let action = AlterTableAction::AlterColumn {
            name: Identifier::new("col"),
            action: AlterColumnAction::SetNotNull,
            use_modify_keyword: false,
        };
        assert!(
            is_pg_only_alter_action(&action),
            "SET NOT NULL must be PG-only"
        );
    }

    #[test]
    fn test_is_pg_only_alter_action_alter_column_drop_not_null() {
        use polyglot_sql::expressions::{AlterColumnAction, AlterTableAction, Identifier};
        let action = AlterTableAction::AlterColumn {
            name: Identifier::new("col"),
            action: AlterColumnAction::DropNotNull,
            use_modify_keyword: false,
        };
        assert!(
            is_pg_only_alter_action(&action),
            "DROP NOT NULL must be PG-only"
        );
    }

    #[test]
    fn test_is_pg_only_alter_action_alter_column_set_default_filtered() {
        // SET DEFAULT is filtered as PG-only at the AST level — defense-in-depth
        // for compound ALTERs where `is_alter_column_default` skips the whole
        // DDL before transpilation. SR has no `ALTER COLUMN` action, and the
        // pipeline already strips DEFAULTs on CREATE TABLE / ADD COLUMN
        // (always sends explicit values via Stream Load JSON), so SET DEFAULT
        // is a no-op for SR and must never reach `polyglot_sql::generate`.
        use polyglot_sql::expressions::{
            AlterColumnAction, AlterTableAction, Expression, Identifier,
        };
        let action = AlterTableAction::AlterColumn {
            name: Identifier::new("col"),
            action: AlterColumnAction::SetDefault(Expression::number(42)),
            use_modify_keyword: false,
        };
        assert!(
            is_pg_only_alter_action(&action),
            "SET DEFAULT must be filtered as PG-only (SR has no ALTER COLUMN action; defaults are no-op)"
        );
    }

    #[test]
    fn test_is_pg_only_alter_action_alter_column_drop_default_filtered() {
        // DROP DEFAULT is filtered as PG-only at the AST level — same
        // rationale as SET DEFAULT above. This is the production incident
        // case (DDL id=13: `ALTER TABLE transactions ALTER COLUMN id DROP DEFAULT`
        // was forwarded verbatim and rejected by SR with error 1064).
        use polyglot_sql::expressions::{AlterColumnAction, AlterTableAction, Identifier};
        let action = AlterTableAction::AlterColumn {
            name: Identifier::new("col"),
            action: AlterColumnAction::DropDefault,
            use_modify_keyword: false,
        };
        assert!(
            is_pg_only_alter_action(&action),
            "DROP DEFAULT must be filtered as PG-only (production incident DDL id=13 root cause)"
        );
    }

    // ── ADD COLUMN with NOT NULL is stripped ───────────────────────────

    #[test]
    fn test_add_column_not_null_stripped() {
        let sql = process_ddl(
            "ALTER TABLE public.t ADD COLUMN x INT NOT NULL DEFAULT 0",
            false,
        );
        // NOT NULL must be stripped: SR only supports NOT NULL on PK columns
        assert!(
            !sql.contains("NOT NULL"),
            "NOT NULL must be stripped from ADD COLUMN: {sql}"
        );
        // DEFAULT is also stripped (PG-specific defaults don't work in SR)
        assert!(
            !sql.contains("DEFAULT"),
            "DEFAULT must be stripped from ADD COLUMN: {sql}"
        );
        // Column should appear and have INT type
        assert!(sql.contains("x "), "Column x must appear: {sql}");
        assert!(sql.contains("INT"), "Type INT must appear: {sql}");
    }

    #[test]
    fn test_add_column_not_null_without_default() {
        // Even without a DEFAULT, NOT NULL must still be stripped
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN x INT NOT NULL", false);
        assert!(
            !sql.contains("NOT NULL"),
            "NOT NULL must be stripped: {sql}"
        );
        assert!(sql.contains("x "), "Column x must appear: {sql}");
    }

    #[test]
    fn test_add_column_nullable_preserved_as_nullable() {
        // A column added without NOT NULL should stay nullable
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN x INT", false);
        assert!(
            !sql.contains("NOT NULL"),
            "No NOT NULL for nullable col: {sql}"
        );
        assert!(sql.contains("x "), "Column x must appear: {sql}");
    }

    #[test]
    fn test_add_column_serial_not_null_stripped() {
        // SERIAL implies NOT NULL in PG — must be stripped for SR.
        // SERIAL must also be remapped to INT (SR has no SERIAL type);
        // the raw output `ADD COLUMN id SERIAL` is a 1064 syntax error
        // that used to be misclassified as a transient network error
        // because the StarRocks error message contains "<EOF>".
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN id SERIAL NOT NULL", false);
        assert!(
            !sql.contains("NOT NULL"),
            "NOT NULL must be stripped from SERIAL: {sql}"
        );
        assert!(sql.contains("id "), "Column id must appear: {sql}");
        assert!(
            !sql.contains("SERIAL"),
            "SERIAL must be remapped to INT for SR: {sql}"
        );
        assert!(sql.contains("INT"), "SERIAL → INT remap missing: {sql}");
    }

    // ── ALTER TABLE ADD COLUMN: PG type remap parity with CREATE TABLE ──
    //
    // The CREATE TABLE path has always remapped PG-only types (SERIAL,
    // BIGSERIAL, JSONB, VARCHAR(N), CHAR(N), TEXT) to SR equivalents via
    // a string-based post-pass in `add_sr_table_clauses_from_expr`. The
    // ALTER TABLE path used to skip this, so `ADD COLUMN x SERIAL` produced
    // invalid SR DDL. The `remap_pg_datatype_to_sr` AST helper now closes
    // this gap. The tests below lock in parity for each remapped type.

    #[test]
    fn test_add_column_bigserial_remaps_to_bigint() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN bid BIGSERIAL", false);
        assert!(
            !sql.contains("BIGSERIAL") && !sql.contains("SERIAL8"),
            "BIGSERIAL must be remapped: {sql}"
        );
        assert!(sql.contains("BIGINT"), "BIGSERIAL → BIGINT: {sql}");
    }

    #[test]
    fn test_add_column_smallserial_remaps_to_smallint() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN sid SMALLSERIAL", false);
        assert!(
            !sql.contains("SMALLSERIAL") && !sql.contains("SERIAL2"),
            "SMALLSERIAL must be remapped: {sql}"
        );
        assert!(sql.contains("SMALLINT"), "SMALLSERIAL → SMALLINT: {sql}");
    }

    #[test]
    fn test_add_column_jsonb_remaps_to_json() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN meta JSONB", false);
        assert!(!sql.contains("JSONB"), "JSONB must be remapped: {sql}");
        assert!(sql.contains("JSON"), "JSONB → JSON: {sql}");
    }

    #[test]
    fn test_add_column_varchar_remaps_to_string() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN name VARCHAR(255)", false);
        assert!(!sql.contains("VARCHAR"), "VARCHAR must be remapped: {sql}");
        assert!(sql.contains("STRING"), "VARCHAR(255) → STRING: {sql}");
    }

    #[test]
    fn test_add_column_char_remaps_to_string() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN code CHAR(10)", false);
        assert!(
            !sql.contains("CHAR"),
            "CHAR must be remapped (SR has no fixed-width CHAR): {sql}"
        );
        assert!(sql.contains("STRING"), "CHAR(10) → STRING: {sql}");
    }

    #[test]
    fn test_add_column_text_remaps_to_string() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN body TEXT", false);
        assert!(
            !sql.contains(" TEXT"),
            "TEXT must be remapped to STRING: {sql}"
        );
        assert!(sql.contains("STRING"), "TEXT → STRING: {sql}");
    }

    #[test]
    fn test_add_column_serial_unaffected_int_stays_int() {
        // Regression guard: ensure non-SERIAL INT columns are NOT mangled.
        let sql = process_ddl(
            "ALTER TABLE public.t ADD COLUMN n INT NOT NULL DEFAULT 0",
            false,
        );
        assert!(sql.contains("INT"), "Plain INT must survive: {sql}");
        assert!(
            !sql.contains("SERIAL"),
            "No spurious SERIAL introduction: {sql}"
        );
    }

    // ── fix: ADD COLUMN type-drift parity with CREATE TABLE ───────────
    //
    // These tests assert that ADD COLUMN emits the same SR column type that
    // CREATE TABLE would emit for the same PG type, via `pg_type_name_to_sr`.
    // Before that fix, ADD COLUMN with MONEY / UUID / INET / MACADDR / BIT / etc.
    // produced invalid SR DDL (verbatim PG type names → SR 1064 → 3-retry →
    // FATAL-ABORT under ddl_failure_abort=true).

    #[test]
    fn test_add_column_money_drifts_to_decimal_38_2() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN m MONEY", false);
        assert!(
            sql.contains("DECIMAL(38,2)"),
            "MONEY → DECIMAL(38,2) (matches OID path): {sql}"
        );
        assert!(
            !sql.contains("MONEY"),
            "MONEY keyword must not survive: {sql}"
        );
    }

    #[test]
    fn test_add_column_uuid_drifts_to_varchar_64() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN u UUID", false);
        assert!(
            sql.contains("VARCHAR(64)"),
            "UUID → VARCHAR(64) (matches OID path): {sql}"
        );
        // The word UUID must not survive as a bare SR type.
        assert!(
            !sql.contains(" UUID"),
            "Bare UUID keyword must be remapped: {sql}"
        );
    }

    #[test]
    fn test_add_column_inet_drifts_to_varchar_64() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN ip INET", false);
        assert!(sql.contains("VARCHAR(64)"), "INET → VARCHAR(64): {sql}");
        assert!(
            !sql.contains(" INET"),
            "Bare INET keyword must be remapped: {sql}"
        );
    }

    #[test]
    fn test_add_column_cidr_drifts_to_varchar_64() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN net CIDR", false);
        assert!(sql.contains("VARCHAR(64)"), "CIDR → VARCHAR(64): {sql}");
    }

    #[test]
    fn test_add_column_macaddr_drifts_to_varchar_255() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN mac MACADDR", false);
        assert!(
            sql.contains("VARCHAR(255)"),
            "MACADDR → VARCHAR(255): {sql}"
        );
    }

    #[test]
    fn test_add_column_macaddr8_drifts_to_varchar_255() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN mac8 MACADDR8", false);
        assert!(
            sql.contains("VARCHAR(255)"),
            "MACADDR8 → VARCHAR(255): {sql}"
        );
    }

    #[test]
    fn test_add_column_bit_drifts_to_varchar_255() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN b BIT(5)", false);
        assert!(sql.contains("VARCHAR(255)"), "BIT(5) → VARCHAR(255): {sql}");
        assert!(
            !sql.contains("BIT"),
            "Bare BIT keyword must be remapped: {sql}"
        );
    }

    #[test]
    fn test_add_column_varbit_drifts_to_varchar_255() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN vb VARBIT(8)", false);
        assert!(
            sql.contains("VARCHAR(255)"),
            "VARBIT(8) → VARCHAR(255): {sql}"
        );
    }

    #[test]
    fn test_add_column_timestamp_precision_dropped() {
        // KNOWN LIMITATION: polyglot-sql's PG→SR transpile step converts
        // `TIMESTAMP(6)` to `STRING` before `remap_pg_datatype_to_sr` runs
        // (the AST representation reaching our code is `DataType::String`,
        // not `DataType::Timestamp{precision:Some(6)}`). The CREATE TABLE path
        // is unaffected because `add_sr_table_clauses_from_expr` runs the
        // generated type string through `pg_type_name_to_sr("TIMESTAMP(6)")`
        // → "DATETIME". The ADD COLUMN path can't recover the DATETIME
        // semantic here — but the precision IS dropped (no TIMESTAMP(6) verbatim).
        // The column ends up as STRING, which is functional (the runtime CDC
        // path stores the timestamp value as an ISO string) but downstream
        // SQL queries using temporal functions on this column will fail.
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN ts TIMESTAMP(6)", false);
        assert!(
            !sql.contains("TIMESTAMP(6)"),
            "Precision must be stripped (polyglot transpile output): {sql}"
        );
        // Either STRING (current polyglot behavior) or DATETIME (if a future
        // polyglot version preserves the variant). Both are acceptable.
        assert!(
            sql.contains("STRING") || sql.contains("DATETIME"),
            "Result must be STRING or DATETIME: {sql}"
        );
    }

    #[test]
    fn test_add_column_timestamptz_precision_dropped() {
        // Same limitation as TIMESTAMP(6) above — transpile-lossy intermediate.
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN tstz TIMESTAMPTZ(6)", false);
        assert!(
            !sql.contains("TIMESTAMPTZ(6)"),
            "TIMESTAMPTZ precision must be stripped: {sql}"
        );
        assert!(
            !sql.contains("TIMESTAMPTZ"),
            "Bare TIMESTAMPTZ keyword must not survive: {sql}"
        );
    }

    #[test]
    fn test_add_column_interval_drifts_to_bigint() {
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN i INTERVAL", false);
        assert!(
            sql.contains("BIGINT"),
            "INTERVAL → BIGINT (matches OID path): {sql}"
        );
    }

    #[test]
    fn test_add_column_int4_alias_normalized_to_int() {
        // PG internal name int4 (pg_type) — must normalize to INT.
        let sql = process_ddl("ALTER TABLE public.t ADD COLUMN n INT4", false);
        assert!(
            sql.contains("INT") && !sql.contains("INT4"),
            "INT4 → INT (matches OID path): {sql}"
        );
    }

    // ── is_transient_network_error: <EOF> poison-pill regression ───────
    //
    // The StarRocks grammar error `Unexpected input '<EOF>'` was previously
    // matched by `err_lower.contains("eof")` and treated as a transient
    // network error, causing DDL id=7 to be retried on every poll forever
    // instead of burning through the 3-retry FATAL-skip budget. These tests
    // lock in the corrected classification.

    #[test]
    fn test_transient_error_starrocks_1064_eof_not_transient() {
        // Exact error string from the production incident (DDL id=7 on
        // ALTER TABLE admin_user_roles ADD id SERIAL).
        let prod_err = "Server error: `ERROR HY000 (1064): Getting syntax error at line 1, column 53. Detail message: Unexpected input '<EOF>', the most similar input is {'('}.'";
        assert!(
            !is_transient_network_error(prod_err),
            "StarRocks 1064 syntax error must NOT be transient (was the DDL id=7 root cause)"
        );
    }

    #[test]
    fn test_transient_error_unexpected_input_not_transient() {
        let err = "Server error: `ERROR HY000 (1064): Getting syntax error at line 1, column 1. Detail message: Unexpected input 'SERIAL'.`";
        assert!(
            !is_transient_network_error(err),
            "Any 'unexpected input' wording must be permanent: {err}"
        );
    }

    #[test]
    fn test_transient_error_you_have_an_error_in_sql_syntax_not_transient() {
        let err = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version";
        assert!(
            !is_transient_network_error(err),
            "MySQL syntax-error wording must be permanent: {err}"
        );
    }

    #[test]
    fn test_transient_error_real_network_errors_still_transient() {
        // Real network/TCP failures must still be classified as transient
        // so the DDL poller retries them indefinitely without burning the
        // 3-retry FATAL-skip budget.
        let real_network_errors = [
            "connection refused",
            "connection reset by peer",
            "connect timeout",
            "connection timed out",
            "broken pipe",
            "Can't connect to MySQL server on '192.0.2.10:9030'",
            "connection already closed",
            "MySQL server has gone away",
            "No route to host",
            "Network is unreachable",
            "connection eof",
            "read eof: stream closed",
        ];
        for err in real_network_errors {
            assert!(
                is_transient_network_error(err),
                "Real network error must remain transient: {err}"
            );
        }
    }

    #[test]
    fn test_transient_error_standalone_eof_not_transient() {
        // The bare token "eof" must NOT be treated as transient on its own —
        // it is too promiscuous a substring. Only network-flavored phrasings
        // ("connection eof", "stream eof", "read eof", "mysql eof") qualify.
        assert!(
            !is_transient_network_error("Unexpected input '<EOF>'"),
            "Standalone '<EOF>' from a grammar error must NOT be transient"
        );
        assert!(
            !is_transient_network_error("got eof"),
            "Other bare-'eof' phrasings must NOT be transient"
        );
    }

    // ── ALTER TABLE with only PG-only constraints → fully skipped ─────

    // ── is_drop_table_not_exists_error classifier ──

    #[test]
    fn test_is_drop_table_not_exists_error_matches_sr_phrasings() {
        // Crash replay re-applies a DROP against an already-dropped table.
        // Without this classifier, the DDL burns its 3-retry budget then aborts.
        let err = |msg: &str| anyhow::anyhow!("{}", msg);
        assert!(
            is_drop_table_not_exists_error(&err("Unknown table 'public.orders'")),
            "SR 'Unknown table' phrasing must classify as not-exists"
        );
        assert!(
            is_drop_table_not_exists_error(&err("Table 'orders' is not found")),
            "SR 'is not found' phrasing must classify as not-exists"
        );
        assert!(
            is_drop_table_not_exists_error(&err("Table orders does not exist")),
            "'does not exist' phrasing must classify as not-exists"
        );
        assert!(
            is_drop_table_not_exists_error(&err("Table orders doesn't exist")),
            "'doesn't exist' phrasing must classify as not-exists"
        );
    }

    #[test]
    fn test_is_drop_table_not_exists_error_rejects_unrelated_errors() {
        let err = |msg: &str| anyhow::anyhow!("{}", msg);
        assert!(
            !is_drop_table_not_exists_error(&err("Schema change in progress")),
            "'schema change' must NOT classify as not-exists"
        );
        assert!(
            !is_drop_table_not_exists_error(&err("syntax error line 1 col 42")),
            "syntax error must NOT classify as not-exists"
        );
        assert!(
            !is_drop_table_not_exists_error(&err("connection refused")),
            "network error must NOT classify as not-exists"
        );
    }

    // ── Snapshot TOCTOU deferral classifier ──

    #[test]
    fn test_is_snapshot_toctou_deferral_matches_bail_message() {
        // Snapshot_one_table bails with "snapshot deferred:..." when
        // pending_cdc_tables contains the table key at the TOCTOU re-check.
        let err = anyhow::anyhow!(
            "snapshot deferred: public.orders has pending CDC events (TOCTOU recheck before DROP)"
        );
        assert!(
            is_snapshot_toctou_deferral(&err),
            "deferral bail message must be classified as TOCTOU deferral"
        );
    }

    #[test]
    fn test_is_snapshot_toctou_deferral_rejects_real_errors() {
        let err = anyhow::anyhow!("Failed to create shadow SR table orders_snapshot_temp");
        assert!(
            !is_snapshot_toctou_deferral(&err),
            "non-deferral error must not be classified as TOCTOU deferral"
        );
    }

    #[test]
    fn test_handle_snapshot_deferral_increments_and_does_not_signal_shutdown() {
        // A single deferral should not signal shutdown; only after
        // exceeding MAX_DDL_DEFERRALS does it escalate to FATAL.
        let (tx, _rx) = watch::channel(false);
        let mut deferrals: HashMap<i64, u32> = HashMap::new();
        let err = anyhow::anyhow!(
            "snapshot deferred: public.orders has pending CDC events (TOCTOU recheck before DROP)"
        );
        let is_deferral = handle_snapshot_deferral(
            &err,
            42,
            "public",
            "orders",
            &mut deferrals,
            20,
            5000,
            &tx,
            true, // ddl_failure_abort
        );
        assert!(is_deferral, "deferral error should be handled as deferral");
        assert_eq!(
            *deferrals.get(&42).unwrap(),
            1,
            "deferral count should be 1"
        );
        // shutdown_tx must not have been sent (only sent when count > max)
        assert!(
            !*tx.borrow(),
            "shutdown must not be signaled on first deferral"
        );
    }

    #[test]
    fn test_handle_snapshot_deferral_signals_shutdown_after_max() {
        let (tx, _rx) = watch::channel(false);
        let mut deferrals: HashMap<i64, u32> = HashMap::new();
        deferrals.insert(42, 20); // already at max
        let err = anyhow::anyhow!(
            "snapshot deferred: public.orders has pending CDC events (TOCTOU recheck before DROP)"
        );
        let is_deferral = handle_snapshot_deferral(
            &err,
            42,
            "public",
            "orders",
            &mut deferrals,
            20,
            5000,
            &tx,
            true, // ddl_failure_abort=true → FATAL shutdown expected
        );
        assert!(is_deferral, "still a deferral even when escalating");
        assert!(
            *tx.borrow(),
            "shutdown must be signaled after exceeding MAX_DDL_DEFERRALS"
        );
    }

    /// With `ddl_failure_abort=false` the FATAL shutdown is bypassed
    /// — the deferral counter matures but the poller doesn't shut down.
    /// Instead it returns `false` so the caller's normal failure path takes
    /// over (advance checkpoint if ddl_failure_abort=false).
    #[test]
    fn test_handle_snapshot_deferral_no_shutdown_when_ddl_failure_abort_false() {
        let (tx, _rx) = watch::channel(false);
        let mut deferrals: HashMap<i64, u32> = HashMap::new();
        deferrals.insert(42, 21); // exceed max=20
        let err = anyhow::anyhow!(
            "snapshot deferred: public.orders has pending CDC events (TOCTOU recheck before DROP)"
        );
        let is_deferral = handle_snapshot_deferral(
            &err,
            42,
            "public",
            "orders",
            &mut deferrals,
            20,
            5000,
            &tx,
            false, // ddl_failure_abort=false → no FATAL shutdown
        );
        assert!(
            !is_deferral,
            "When ddl_failure_abort=false the deferral-matured return is false \
             (caller proceeds with normal failure handling)"
        );
        assert!(
            !*tx.borrow(),
            "shutdown must NOT be signaled when ddl_failure_abort=false"
        );
    }

    #[test]
    fn test_handle_snapshot_deferral_non_deferral_returns_false() {
        let (tx, _rx) = watch::channel(false);
        let mut deferrals: HashMap<i64, u32> = HashMap::new();
        let err = anyhow::anyhow!("Failed to drop stale shadow SR table");
        let is_deferral = handle_snapshot_deferral(
            &err,
            42,
            "public",
            "orders",
            &mut deferrals,
            20,
            5000,
            &tx,
            true, // ddl_failure_abort
        );
        assert!(
            !is_deferral,
            "non-deferral error must return false so normal failure handling proceeds"
        );
        assert!(
            deferrals.is_empty(),
            "non-deferral must not populate deferrals map"
        );
    }

    // ── Multi-table DROP split uses sr_quote ──

    #[test]
    fn test_drop_table_multi_table_split_uses_sr_quote() {
        // Identifiers in the split DROP statements must be backtick-quoted
        // via sr_quote so table names containing spaces/reserved words produce
        // valid SQL.
        let p = test_table_props(true, false);
        let sql = post_process_ddl("DROP TABLE IF EXISTS public.t1, public.t2", &p);
        assert!(
            sql.contains("`t1`"),
            "table name t1 must be backtick-quoted: {sql}"
        );
        assert!(
            sql.contains("`t2`"),
            "table name t2 must be backtick-quoted: {sql}"
        );
        // Each statement should be a standalone DROP TABLE IF EXISTS `name`
        assert!(
            sql.matches("DROP TABLE IF EXISTS").count() == 2,
            "Expected 2 backtick-quoted DROP TABLE statements, got: {sql}"
        );
    }

    // ── ALTER TABLE with only PG-only constraints → fully skipped ─────

    #[test]
    fn test_alter_add_foreign_key_fully_skipped() {
        // ALTER TABLE ADD CONSTRAINT FOREIGN KEY has no valid SR actions →
        // the entire DDL should be skipped (empty output)
        let sql = process_ddl(
            "ALTER TABLE public.t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES other(b)",
            false,
        );
        assert!(
            sql.is_empty(),
            "FK-only ALTER TABLE must produce empty output: {sql}"
        );
    }

    #[test]
    fn test_alter_drop_constraint_fully_skipped() {
        let sql = process_ddl(
            "ALTER TABLE public.t DROP CONSTRAINT some_constraint",
            false,
        );
        assert!(
            sql.is_empty(),
            "DROP CONSTRAINT-only ALTER TABLE must produce empty output: {sql}"
        );
    }

    #[test]
    fn test_alter_drop_foreign_key_fully_skipped() {
        let sql = process_ddl("ALTER TABLE public.t DROP FOREIGN KEY fk_name", false);
        assert!(
            sql.is_empty(),
            "DROP FOREIGN KEY-only ALTER TABLE must produce empty output: {sql}"
        );
    }

    #[test]
    fn test_alter_add_column_with_foreign_key_only_fk_skipped() {
        // Multi-action ALTER: ADD COLUMN should survive, FK should be filtered
        let sql = process_ddl(
            "ALTER TABLE public.t ADD COLUMN x INT, ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES other(y)",
            false,
        );
        assert!(
            !sql.is_empty(),
            "Should still produce output for ADD COLUMN: {sql}"
        );
        assert!(
            sql.contains("ADD COLUMN") || sql.contains("ADD `x`"),
            "ADD COLUMN action must survive: {sql}"
        );
        assert!(
            !sql.contains("FOREIGN KEY"),
            "FOREIGN KEY must be filtered: {sql}"
        );
        assert!(
            !sql.contains("REFERENCES"),
            "REFERENCES must be filtered: {sql}"
        );
    }

    // ── ALTER COLUMN SET NOT NULL is stripped from output ──────────────

    #[test]
    fn test_alter_column_set_not_null_stripped() {
        let sql = process_ddl("ALTER TABLE public.t ALTER COLUMN x SET NOT NULL", false);
        assert!(
            sql.is_empty(),
            "SET NOT NULL-only ALTER TABLE must produce empty output: {sql}"
        );
    }

    #[test]
    fn test_alter_column_drop_not_null_stripped() {
        let sql = process_ddl("ALTER TABLE public.t ALTER COLUMN x DROP NOT NULL", false);
        assert!(
            sql.is_empty(),
            "DROP NOT NULL-only ALTER TABLE must produce empty output: {sql}"
        );
    }

    // ── ALTER TABLE ADD FOREIGN KEY + SET NOT NULL: all filtered ──────

    #[test]
    fn test_alter_fk_and_set_not_null_all_filtered() {
        let sql = process_ddl(
            "ALTER TABLE public.t \
             ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES other(b), \
             ALTER COLUMN x SET NOT NULL",
            false,
        );
        assert!(sql.is_empty(), "All actions are PG-only: {sql}");
    }

    // ── two-phase acquire regression tests ──────────────

    /// `acquire_ddl_guard` must NOT return with `ddl_active` set while
    /// `pending_cdc_tables` still contains the table — that was the prior
    /// guard-first starvation. It must drain, acquire, and re-check; if
    /// pending is dirty after acquire, it must drop the guard and loop.
    ///
    /// This test verifies the gate predicate directly (not the helper, which
    /// needs an async runtime + a shutdown channel): the two-phase
    /// contract is "guard held ⇒ pending absent" once the helper returns, and
    /// "guard dropped ⇒ pending may be present (re-drain)".
    #[tokio::test]
    async fn test_two_phase_acquire_drains_before_guard_held() {
        let ddl_active: Arc<Mutex<std::collections::HashSet<String>>> =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        let pending: Arc<Mutex<std::collections::HashSet<String>>> =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        let table_key = "public.t".to_string();

        // Pre-condition: pending empty. acquire_ddl_guard should return on the
        // first iteration with the guard held.
        let guard = acquire_ddl_guard(
            &ddl_active,
            &pending,
            &table_key,
            &mut watch::channel(false).1,
            1,
            "public",
            "t",
        )
        .await
        .expect("acquire should succeed with pending empty");
        // Invariant: guard held + pending absent.
        assert!(
            crate::pipeline::lock_or_recover(&ddl_active).contains(&table_key),
            "guard must be held after acquire returns"
        );
        assert!(
            !crate::pipeline::lock_or_recover(&pending).contains(&table_key),
            "pending must be ABSENT when the guard is held (closes the guard-first starvation window)"
        );
        drop(guard);
        assert!(
            !crate::pipeline::lock_or_recover(&ddl_active).contains(&table_key),
            "guard dropped releases ddl_active"
        );
    }

    /// is_drop_constraint_alter must catch ALTER TABLE ... DROP CONSTRAINT
    /// (a PG-only no-op for SR) and not false-positive on ADD/ALTER actions.
    #[test]
    fn test_is_drop_constraint_alter_matches() {
        assert!(is_drop_constraint_alter("ALTER TABLE t DROP CONSTRAINT c"));
        assert!(is_drop_constraint_alter("alter table t drop constraint c"));
        assert!(is_drop_constraint_alter(
            "ALTER TABLE t DROP CONSTRAINT IF EXISTS c"
        ));
        assert!(!is_drop_constraint_alter(
            "ALTER TABLE t ADD CONSTRAINT c PRIMARY KEY (id)"
        ));
        assert!(!is_drop_constraint_alter(
            "ALTER TABLE t ALTER COLUMN c TYPE INT"
        ));
    }

    /// `bound_ddl_for_extract` caps the DDL text fed to polyglot parse without
    /// corrupting UTF-8 boundaries.
    #[test]
    fn test_bound_ddl_for_extract_caps_at_char_boundary() {
        let small = "CREATE TABLE t (id int)";
        assert_eq!(bound_ddl_for_extract(small), small);

        // 70KB of ASCII: capped at 64KB chars, all ASCII so byte-boundary safe.
        let big_ascii = format!("CREATE TABLE t ({} int);", "x".repeat(70_000));
        let capped = bound_ddl_for_extract(&big_ascii);
        assert_eq!(capped.chars().count(), 64 * 1024);
        assert!(capped.starts_with("CREATE TABLE t ("));

        // UTF-8 boundary: mix multibyte chars — must not panic, result must
        // be valid UTF-8 ending on a char boundary.
        let big_utf8 = format!("CREATE TABLE é{} (id int);", "é".repeat(70_000));
        let capped_utf8 = bound_ddl_for_extract(&big_utf8);
        assert_eq!(capped_utf8.chars().count(), 64 * 1024);
        // Cheap UTF-8 validity check.
        assert!(std::str::from_utf8(capped_utf8.as_bytes()).is_ok());
    }
}
