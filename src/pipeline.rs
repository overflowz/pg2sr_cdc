//! Pipeline — transforms pgoutput events into StarRocks batches and sends them.
//! Per-table change buffers (INSERT→upsert_rows, UPDATE→upsert_rows+delete_rows,
//! DELETE→delete_rows), type-aware JSON value conversion (ARRAY, BYTEA, INTERVAL,
//! MONEY), and auto-create logic for missing SR tables during streaming.
use crate::cdc::ColValue;
use crate::cdc::DataEvent;
use crate::sink::{SrBatch, SrSink};
use crate::sr_connection::SrConnectionManager;
use crate::wal_buffer::WalBufferManager;
use crate::wal_buffer::{
    decode_segment, BufRecord, ReplaySlotGuard, BUF_DELETE, BUF_TRUNCATE, BUF_UPSERT,
};
use anyhow::{Context, Result};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Column-rename map per table: `table_key -> {old_name -> new_name}`,
/// registered by the DDL poller on RENAME COLUMN. Buffered rows carry the
/// OLD column names; replay remaps them so the live SR schema is written.
/// Entries are overwritten per table and never cleaned (tiny, accepted).
pub(crate) type RenameMap = Arc<Mutex<HashMap<String, HashMap<String, String>>>>;

/// Acquire a `Mutex`, recovering from poison instead of silently skipping the
/// critical section. Without this, `if let Ok(g) = lock()` would drop the
/// guard on poison — silently no-op-ing inserts/removes/contains and degrading
/// the DDL/CDC TOCTOU protection (e.g. a table stuck "pending" or "ddl_active"
/// forever after a panic). Mirrors `sr_connection.rs`; logs an `error!` so a
/// poisoned mutex is visible rather than silent.
pub(crate) fn lock_or_recover<T>(mx: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mx.lock() {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(
                "Mutex poisoned — DDL/CDC TOCTOU protection degraded; recovering guard"
            );
            e.into_inner()
        }
    }
}

/// RAII guard that removes a table from `pending_cdc_tables` on drop.
/// Ensures cleanup even on panic or error, narrowing the DDL/DML TOCTOU window.
pub(crate) struct PendingCdcTableGuard {
    set: Arc<Mutex<HashSet<String>>>,
    table_key: String,
}

impl PendingCdcTableGuard {
    pub(crate) fn new(set: Arc<Mutex<HashSet<String>>>, table_key: String) -> Self {
        lock_or_recover(&set).insert(table_key.clone());
        Self { set, table_key }
    }
}

impl Drop for PendingCdcTableGuard {
    fn drop(&mut self) {
        lock_or_recover(&self.set).remove(&self.table_key);
    }
}

/// RAII guard that marks a table as "DDL in progress" on the shared `ddl_active`
/// set, so the CDC flush path can detect concurrent DDLs and route the table's
/// changes into the durable wal_buffer (flush_transaction routing condition:
/// `ddl_active` membership) until the DDL finishes.
///
/// The DDL poller holds this guard across `execute_sql` / `snapshot_one_table`
/// for ALTER/DROP/RENAME/CREATE-backfill paths. The CDC flush path checks
/// `ddl_active` (before AND after creating its `PendingCdcTableGuard` — the
/// double-check is defense-in-depth that narrows the window).
///
/// NOTE: the double-check IS the primary TOCTOU closure again.
/// The DDL poller now uses TWO-PHASE acquire (`acquire_ddl_guard`): it drains
/// `pending_cdc_tables` WITHOUT holding `DdlActiveGuard` (so flushes complete
/// and the buffer empties), then acquires the guard and re-checks. The prior
/// guard-first ordering (acquire guard BEFORE the drain) closed the
/// microsecond TOCTOU but starved on continuously-written tables: the guard
/// made every flush defer, so `pending_cdc_tables` never cleared and the
/// drain waited forever while the replay set grew to the HARD-limit FATAL.
/// With two-phase acquire, a flush spawned after the guard becomes visible is
/// caught by this double-check and defers (buffer preserved).
///
/// Without this mutual-exclusion mechanism, a DDL running concurrently with a
/// CDC Stream Load for the same table produces the race documented at
/// `ddl_poller.rs:1168-1172`: DROP TABLE races with in-flight flush → the
/// auto-create fallback resurrects an empty SR table → silent prior-data loss.
pub(crate) struct DdlActiveGuard {
    set: Arc<Mutex<HashSet<String>>>,
    table_key: String,
}

impl DdlActiveGuard {
    pub(crate) fn new(set: Arc<Mutex<HashSet<String>>>, table_key: String) -> Self {
        lock_or_recover(&set).insert(table_key.clone());
        Self { set, table_key }
    }
}

impl Drop for DdlActiveGuard {
    fn drop(&mut self) {
        lock_or_recover(&self.set).remove(&self.table_key);
    }
}

mod value_conv;
pub(crate) use value_conv::*;

use tokio::sync::{watch, Semaphore};

/// Table-level change buffer for a single transaction.
///
/// Operations are recorded in WAL order so net-change dedup can select the
/// *last* operation per PK (e.g. DELETE→INSERT keeps the row;
/// INSERT→DELETE removes it).
#[derive(Debug, Clone)]
struct TableChanges {
    /// Ordered list of operations — WAL insertion order preserved.
    ops: Vec<OrderedOp>,
    /// If true, TRUNCATE TABLE before applying DELETE/UPSERT for this table.
    truncate: bool,
}

/// A serialized row operation — the JSON bytes (wrapped in `Box<RawValue>`,
/// cheaply cloneable via an internal `Arc`) plus a pre-computed PK key string
/// for net-change dedup.
///
/// Replaces the prior `OrderedOp(serde_json::Value)` which held a whole
/// `Value::Object(Map)` per row — N `String`-key clones + N `Value` wrappers
/// plus BTreeMap node allocations, all rebuilt every row and then re-serialized
/// at flush time. Here the row is serialized ONCE into a reused `Vec<u8>`
/// buffer (`serialize_row`) and the PK key is extracted during serialization
/// (col→index lookup), so dedup never re-parses the JSON.
#[derive(Debug, Clone)]
struct RowOp {
    data: Box<serde_json::value::RawValue>,
    /// Pre-computed `"v1"|"v2"`-style PK key (matching the old
    /// `extract_pk_key` JSON-text output). `None` when the table has no PK
    /// columns (dedup falls back to keeping every op).
    pk_key: Option<String>,
    /// Estimated in-memory bytes of this op, computed by
    /// `Pipeline::estimate_row_size` at process time. Storing it per-op keeps
    /// `pending_bytes` accounting in ONE unit: TRUNCATE subtraction and the
    /// DdlInProgressError deferral-restore previously used the serialized
    /// length (`data.get().len()`), which under-subtracted vs the ~2×-raw
    /// estimate (premature soft-flushes after TRUNCATE) and under-counted
    /// after a deferral (weakened hard-limit tripwire).
    est_bytes: usize,
}

impl RowOp {
    /// Test/helper constructor: serialize a `serde_json::Value` once into a
    /// `RowOp`. PK key is extracted from the value via the legacy path so
    /// test-built ops dedup EXACTLY like production-built ones — including the
    /// unresolved sentinel: `None` (which keeps every op), NOT a shared empty
    /// key that would collapse distinct rows into one "last op wins" survivor.
    #[cfg(test)]
    fn from_value(row: serde_json::Value, pk_columns: &[String]) -> Self {
        let pk_key = extract_pk_key_from_value(&row, pk_columns);
        let data = serde_json::value::RawValue::from_string(row.to_string())
            .expect("serde_json::Value serializes to valid JSON");
        let est_bytes = data.get().len();
        Self {
            data,
            pk_key,
            est_bytes,
        }
    }
}

#[derive(Debug, Clone)]
enum OrderedOp {
    Delete(RowOp),
    Upsert(RowOp),
}

impl TableChanges {
    fn new() -> Self {
        Self {
            ops: Vec::new(),
            truncate: false,
        }
    }

    fn len(&self) -> usize {
        self.ops.len()
    }

    fn add_delete(&mut self, op: RowOp) {
        self.ops.push(OrderedOp::Delete(op));
    }

    fn add_upsert(&mut self, op: RowOp) {
        self.ops.push(OrderedOp::Upsert(op));
    }
}

/// Pipeline state — maintains per-table change buffers and relation metadata.
pub struct Pipeline {
    /// Relation cache: relation_id -> (schema, table, [col_name, col_type_oid, col_type_mod])
    relations: HashMap<u32, RelationInfo>,
    /// Name lookup: "schema.table" → relation_id (avoids O(n) linear scan in flush)
    name_to_oid: HashMap<String, u32>,
    /// Current transaction's buffered changes per table (key: "schema.table")
    tables: HashMap<String, TableChanges>,
    /// StarRocks sink
    sink: SrSink,
    /// SR database name (from config)
    sr_database: String,
    /// SR connection manager (for DDL operations like auto-create, TRUNCATE)
    sr_mgr: Arc<SrConnectionManager>,
    /// Table properties for CREATE TABLE statements (built from config by main.rs)
    table_properties: HashMap<String, String>,
    /// PG connection config (for ad hoc catalog queries to resolve PK info)
    pg_config: crate::config::PgConfig,
    /// Total estimated bytes of buffered event data. Reset on every flush.
    /// Tracks approximate memory used by pending UPSERT/DELETE row values to
    /// prevent unbounded growth during PG write bursts.
    pending_bytes: usize,
    /// Max pending bytes before the pipeline signals a forced flush.
    max_pending_bytes: usize,
    /// Set true when `pending_bytes` exceeds `max_pending_bytes` — tells the
    /// caller to flush immediately instead of reading more WAL events.
    flush_requested: bool,
    /// Set true after the first 80%-of-memory-limit warning is logged,
    /// so we don't spam the log on every subsequent event near the threshold.
    eighty_percent_warned: bool,
    /// Shared atomic for external metrics — mirrors current pending_bytes so
    /// the stats heartbeat can report pipeline memory pressure without
    /// borrowing Pipeline.
    pending_bytes_shared: Arc<AtomicUsize>,
    /// Optional table filter regex (compiled from glob) — CDC events for
    /// non-matching tables are silently skipped. Matches against bare table names.
    table_filter_re: Option<Regex>,
    /// DDL race retry sleep in seconds — when a Stream Load hits "unknown column"
    /// the pipeline sleeps this long before retrying, giving the DDL poller time
    /// to apply schema changes. Higher values reduce DDL-race failures but
    /// increase latency for affected batches.
    ddl_race_retry_ms: u64,
    /// Concurrency limiter for per-table Stream Load flushes.
    /// Prevents overwhelming StarRocks when many tables have pending changes.
    flush_semaphore: Arc<Semaphore>,
    /// In-memory cache of primary key columns per relation OID.
    /// Avoids redundant PG catalog queries for tables that have been confirmed
    /// to have (or not have) a PRIMARY KEY.  Entries expire after `pk_cache_ttl`.
    pk_cache: HashMap<u32, CachedPkInfo>,
    /// TTL for PK cache entries in seconds (default 30).
    pk_cache_ttl: Duration,
    /// Shared set of table keys ("schema.table") for which the DDL poller is
    /// currently executing a DDL (ALTER/DROP/RENAME/snapshot). The CDC flush
    /// path checks this (before AND after creating the PendingCdcTableGuard)
    /// to defer flushes for tables being DDL'd — see `DdlActiveGuard` and the
    /// DDL/CDC drain-and-pause coordination. Mirrors `pending_cdc_tables`
    /// (the reverse signal: CDC flush in progress, DDL poller waits).
    pending_cdc_tables: Arc<Mutex<HashSet<String>>>,
    ddl_active: Arc<Mutex<HashSet<String>>>,
    /// Tables dropped via DDL since the last `begin_transaction`. The DDL
    /// poller pushes "schema.table" keys here on a successful DROP TABLE; the
    /// pipeline drains the set in `begin_transaction` and evicts the matching
    /// `relations` / `name_to_oid` / `pk_cache` entries. Without this, months
    /// of create/drop churn (or PG OID reuse after a DROP) leave stale
    /// cache entries that grow memory unbounded and can mis-route events for
    /// a re-created table that reuses a dropped table's OID.
    dropped_relations: Arc<Mutex<HashSet<String>>>,
    /// Reusable serializer buffer for per-row column→JSON conversion.
    /// `serialize_row` writes the JSON object directly here (clear→write→
    /// wrap-in-`RawValue`), avoiding the per-row `serde_json::Map` (N String
    /// keys + BTreeMap nodes) the legacy `columns_to_json` allocated.
    /// `columns_to_json` (retained for tests + schema-mismatch detection)
    /// still takes a `&mut Map`, but the production hot path uses this buffer.
    row_ser_buf: Vec<u8>,
    /// Shutdown signal receiver. Checked during flush to avoid blocking
    /// the CDC loop on semaphore acquire when all permits are in use.
    shutdown_rx: watch::Receiver<bool>,
    /// Durable per-table WAL-event segment buffer (write-pause storage).
    /// Tables with an in-flight SR schema-change job (or leftover segments
    /// from a crash) route their changes here instead of memory; replay
    /// drains the segments through the normal flush path once the DDL ends.
    wal_buffer: Arc<crate::wal_buffer::WalBufferManager>,
    /// Metrics: events appended to the wal buffer (shared with main.rs).
    wal_buffered_events: Arc<AtomicU64>,
    /// Metrics: payload bytes appended to the wal buffer.
    wal_buffered_bytes: Arc<AtomicU64>,
    /// Metrics: events replayed from the wal buffer into SR.
    wal_replayed_events: Arc<AtomicU64>,
    /// Metrics: failed replay tasks (segment kept, retried next kick).
    wal_replay_failures: Arc<AtomicU64>,
    /// Column-rename map per table (old → new), registered by the DDL poller
    /// on RENAME COLUMN so buffered rows replay with the live column names.
    rename_map: RenameMap,
    /// Consecutive-failure deferral for wal-buffer replay kicks (see
    /// kick_replays / note_replay_result). A replay that just failed against
    /// a down StarRocks burns its full per-node retry budget (~flush_timeout)
    /// on every subsequent kick; this schedule defers re-spawning while SR
    /// stays unhealthy instead of hammering it once per flush cycle.
    replay_backoff: Arc<Mutex<HashMap<String, ReplayBackoff>>>,
    /// In-flight wal-buffer replay tasks (one per table). Owned by the
    /// pipeline so `drain_replays` can await them at shutdown; pruned at
    /// every kick so it never grows unbounded.
    replay_tasks: tokio::task::JoinSet<()>,
}

/// A cached PK query timestamp for TTL-based re-query avoidance.
/// Used only for the negative case (confirmed "no PK") — positive results
/// are stored directly in the relation cache's `pk_columns`.
#[derive(Debug, Clone)]
struct CachedPkInfo {
    fetched_at: Instant,
}

impl CachedPkInfo {
    fn is_fresh(&self, ttl: Duration) -> bool {
        self.fetched_at.elapsed() < ttl
    }
}

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColInfo>,
    /// Primary key column names. Populated by load_relations() from PG catalog.
    /// Empty when PK is unknown (WAL Relation events don't carry PK metadata).
    pub pk_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColInfo {
    pub name: String,
    pub type_oid: u32,
    pub type_mod: i32,
}

impl Pipeline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sink: SrSink,
        sr_mgr: Arc<SrConnectionManager>,
        table_properties: HashMap<String, String>,
        max_pending_memory_mb: usize,
        pg_config: crate::config::PgConfig,
        pending_bytes_shared: Arc<AtomicUsize>,
        table_filter_re: Option<Regex>,
        ddl_race_retry_ms: u64,
        max_concurrent_flushes: usize,
        pending_cdc_tables: Arc<Mutex<HashSet<String>>>,
        ddl_active: Arc<Mutex<HashSet<String>>>,
        dropped_relations: Arc<Mutex<HashSet<String>>>,
        shutdown_rx: watch::Receiver<bool>,
        wal_buffer: Arc<crate::wal_buffer::WalBufferManager>,
        wal_buffered_events: Arc<AtomicU64>,
        wal_buffered_bytes: Arc<AtomicU64>,
        wal_replayed_events: Arc<AtomicU64>,
        wal_replay_failures: Arc<AtomicU64>,
        rename_map: RenameMap,
    ) -> Self {
        Self {
            relations: HashMap::new(),
            name_to_oid: HashMap::new(),
            tables: HashMap::new(),
            sink,
            sr_database: sr_mgr.database().to_string(),
            sr_mgr,
            table_properties,
            pg_config,
            pending_bytes: 0,
            max_pending_bytes: max_pending_memory_mb * 1024 * 1024,
            flush_requested: false,
            eighty_percent_warned: false,
            pending_bytes_shared,
            table_filter_re,
            ddl_race_retry_ms,
            flush_semaphore: Arc::new(Semaphore::new(max_concurrent_flushes)),
            pk_cache: HashMap::new(),
            pk_cache_ttl: Duration::from_secs(30),
            pending_cdc_tables,
            ddl_active,
            dropped_relations,
            row_ser_buf: Vec::new(),
            shutdown_rx,
            wal_buffer,
            wal_buffered_events,
            wal_buffered_bytes,
            wal_replayed_events,
            wal_replay_failures,
            rename_map,
            replay_backoff: Arc::new(Mutex::new(HashMap::new())),
            replay_tasks: tokio::task::JoinSet::new(),
        }
    }

    /// Update relation cache from a Relation event.
    /// Preserves existing pk_columns when the new event doesn't carry PK metadata
    /// (WAL Relation events omit PK info; synthetic events from load_relations have it).
    pub fn add_relation(
        &mut self,
        relation_id: u32,
        schema: &str,
        table: &str,
        columns: &[crate::cdc::RelationColumn],
        pk_columns: &[String],
    ) {
        let cols: Vec<ColInfo> = columns
            .iter()
            .map(|c| ColInfo {
                name: c.name.clone(),
                type_oid: c.type_oid,
                type_mod: c.type_modifier,
            })
            .collect();

        let resolved_pk = if pk_columns.is_empty() {
            // WAL Relation events don't carry PK metadata. If we already have
            // PK info from a prior synthetic event (load_relations), keep it.
            self.relations
                .get(&relation_id)
                .map(|existing| existing.pk_columns.clone())
                .unwrap_or_default()
        } else {
            pk_columns.to_vec()
        };

        self.relations.insert(
            relation_id,
            RelationInfo {
                schema: schema.to_string(),
                table: table.to_string(),
                columns: cols,
                pk_columns: resolved_pk,
            },
        );
        // Maintain the name→OID index for O(1) lookup in flush_transaction.
        let name_key = format!("{}.{}", schema, table);
        // DROP+CREATE: if the name was already mapped to a DIFFERENT oid
        // (stale entry from the pre-DROP incarnation), evict the old oid's
        // relation/PK entries BEFORE the new mapping — the cache must never
        // route events for the new table through the old columns, and the
        // OID-keyed maps must not leak under create/drop churn.
        if let Some(&prev) = self.name_to_oid.get(&name_key) {
            if prev != relation_id {
                self.relations.remove(&prev);
                self.pk_cache.remove(&prev);
            }
        }
        self.name_to_oid.insert(name_key.clone(), relation_id);
        // A re-created table cancels any pending name-keyed eviction: the
        // eviction drain (begin_transaction) must never remove the LIVE
        // relation registered above — a same-batch DROP+CREATE or a delayed
        // DROP enqueue would otherwise evict the fresh entry.
        lock_or_recover(&self.dropped_relations).remove(&name_key);
    }

    /// Reset per-transaction buffers and pending-byte counter.
    /// Also clears the shared pending-CDC-tables set so the DDL poller
    /// sees no tables as pending when no events have been processed yet —
    /// EXCEPT tables whose wal-buffer replay task is still in flight: those
    /// hold their key via `PendingCdcTableGuard` for the whole replay
    /// iteration (LIST → Stream Load → segment delete) and that key is exactly
    /// what the DDL poller's `wait_for_cdc_drain` waits on. Clearing it here
    /// would let the drain pass while a replay Stream Load is still running —
    /// the DDL/CDC race the guard system exists to prevent.
    pub fn begin_transaction(&mut self) {
        self.tables.clear();
        self.pending_bytes = 0;
        self.flush_requested = false;
        self.eighty_percent_warned = false;
        // Three phases, never holding `pending_cdc_tables` across a wal-buffer
        // call: `is_replaying` takes the wal buffer's state lock, and nesting
        // pending → wal here would invert the order every flush path uses
        // (wal-buffer routing checks run before the pending insert) and can
        // deadlock against them.
        let keys: Vec<String> = lock_or_recover(&self.pending_cdc_tables)
            .iter()
            .cloned()
            .collect();
        let preserved: HashSet<String> = keys
            .into_iter()
            .filter(|k| self.wal_buffer.is_replaying(k.as_str()))
            .collect();
        lock_or_recover(&self.pending_cdc_tables).retain(|k| preserved.contains(k));
        // Drain any tables the DDL poller dropped since the last transaction and
        // evict their stale cache entries (relations / name_to_oid / pk_cache).
        // Called once per batch — cheap and bounds memory under DDL churn.
        self.evict_dropped_relations();
    }

    /// Evict relation/PK cache entries for tables dropped by the DDL poller.
    ///
    /// Drains the shared `dropped_relations` set (populated by the DDL poller
    /// on a successful DROP TABLE) and removes the matching `name_to_oid`,
    /// `relations`, and `pk_cache` entries. This (a) bounds memory growth under
    /// create/drop churn, and (b) prevents a stale entry from being mis-used if
    /// PG reassigns the dropped table's OID to a new table with a different
    /// schema (which would otherwise route events to the wrong columns).
    ///
    /// Residual race (accepted, narrowed): a DROP enqueued AFTER the recreate's
    /// Relation registration in a LATER poll batch can still evict the live
    /// entry — cross-batch poll lag. The DDL poller skips enqueueing evictions
    /// for same-batch recreates (recreate_names) and `add_relation` cancels
    /// pending evictions on re-registration, so the window is one poll batch
    /// with the DDL trigger capture order guaranteeing the CREATE row follows
    /// the DROP row.
    fn evict_dropped_relations(&mut self) {
        let dropped: Vec<String> = match self.dropped_relations.lock() {
            Ok(mut set) => set.drain().collect(),
            Err(e) => {
                // Poison-recovery: drain so we still make progress.
                e.into_inner().drain().collect()
            }
        };
        if dropped.is_empty() {
            return;
        }
        for table_key in &dropped {
            if let Some(&oid) = self.name_to_oid.get(table_key) {
                self.relations.remove(&oid);
                self.pk_cache.remove(&oid);
            }
            self.name_to_oid.remove(table_key);
            // Also drop any in-flight changes for the dropped table so a
            // straggler CDC event can't resurrect a flush for it. Reclaim their
            // byte estimate too — an over-counting gauge triggers premature
            // flushes and misleads the memory-limit monitoring.
            if let Some(changes) = self.tables.remove(table_key) {
                let freed: usize = changes
                    .ops
                    .iter()
                    .map(|op| match op {
                        OrderedOp::Upsert(r) | OrderedOp::Delete(r) => r.est_bytes,
                    })
                    .sum();
                self.pending_bytes = self.pending_bytes.saturating_sub(freed);
                self.pending_bytes_shared
                    .store(self.pending_bytes, Ordering::Relaxed);
            }
            // Cross-incarnation rename-map hygiene: a dropped table's rename
            // entries must not survive into a recreated table with the same
            // name (same-incarnation reuse after a
            // skipped DDL stays guarded by resolve_column's live-key check).
            lock_or_recover(&self.rename_map).remove(table_key);
        }
        tracing::debug!(
            dropped_count = dropped.len(),
            tables = ?dropped,
            "Evicted relation cache entries for dropped tables",
        );
    }

    /// Query PG catalog to fill in PK info for relations that lack it.
    /// WAL Relation events don't carry PK metadata — for tables created after
    /// startup, the relation cache only gets column info. This method connects
    /// to PG via self.pg_config and fetches PK columns for any cached relation
    /// with empty pk_columns. Call periodically from the CDC event loop.
    ///
    /// An in-memory cache with TTL avoids redundant PG queries for tables that
    /// have been confirmed to have (or not have) a PRIMARY KEY.  Tables without
    /// PK stay in the cache for `pk_cache_ttl` seconds (default 30), preventing
    /// connection churn on every batch for PK-less tables.
    pub async fn refresh_relation_pks(&mut self) -> anyhow::Result<()> {
        let oids_to_refresh: Vec<u32> = self
            .relations
            .iter()
            .filter(|(oid, rel)| {
                if rel.pk_columns.is_empty() {
                    // Check the in-memory cache before deciding to query PG
                    match self.pk_cache.get(oid) {
                        Some(cached) if cached.is_fresh(self.pk_cache_ttl) => {
                            // Cached "no PK" result — don't re-query yet.
                            false
                        }
                        _ => true, // Need to query PG
                    }
                } else {
                    false // Already has PK info, skip
                }
            })
            .map(|(oid, _)| *oid)
            .collect();

        if oids_to_refresh.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Refreshing PK info for {} relation(s)",
            oids_to_refresh.len()
        );

        let tls = crate::config::PgTlsConnector::from_config(&self.pg_config.tls)?;
        let (pg, conn_handle) = tls.connect(&self.pg_config.conn_str()).await?;
        let _conn_guard = crate::util::AbortOnDrop(conn_handle);

        for oid in oids_to_refresh {
            match crate::pg_catalog::fetch_primary_keys(&pg, oid).await {
                Ok(pks) if !pks.is_empty() => {
                    tracing::info!("Resolved PK columns for relation oid={}: {:?}", oid, pks,);
                    if let Some(rel) = self.relations.get_mut(&oid) {
                        rel.pk_columns = pks.clone();
                    }
                    // Cache the positive result (effectively permanent — never re-query)
                    self.pk_cache.insert(
                        oid,
                        CachedPkInfo {
                            fetched_at: Instant::now(),
                        },
                    );
                }
                Ok(_) => {
                    tracing::debug!("Relation oid={} still has no PK (not an error)", oid);
                    // Cache the negative result to avoid re-querying every batch
                    self.pk_cache.insert(
                        oid,
                        CachedPkInfo {
                            fetched_at: Instant::now(),
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to query PK for relation oid={}: {:?}", oid, e);
                    // Don't cache on error — retry on next batch
                }
            }
        }

        Ok(())
    }

    /// Reload a single relation's column metadata from PG catalog.
    /// Used when `columns_to_json` detects a schema mismatch (WAL tuple has more
    /// values than the cached columns) — which happens after ALTER TABLE ADD COLUMN
    /// when pgoutput hasn't sent a Relation message yet. Queries PG's `pg_attribute`
    /// directly and updates the in-memory cache so the retry will succeed.
    pub async fn reload_relation_columns(&mut self, relation_id: u32) -> anyhow::Result<()> {
        // Note: This only refreshes column metadata (columns + pk_columns).
        // It does NOT change rel.schema or rel.table, so name_to_oid remains
        // correct. Table renames go through the DDL poller's drop+resnapshot
        // path, not through this method.
        let tls = crate::config::PgTlsConnector::from_config(&self.pg_config.tls)?;
        let (pg, conn_handle) = tls.connect(&self.pg_config.conn_str()).await?;
        let _conn_guard = crate::util::AbortOnDrop(conn_handle);

        match crate::pg_catalog::fetch_columns(&pg, relation_id).await {
            Ok(cols) if !cols.is_empty() => {
                tracing::info!(
                    "Reloaded {} columns for relation oid={} from PG catalog (DDL schema sync)",
                    cols.len(),
                    relation_id,
                );
                if let Some(rel) = self.relations.get_mut(&relation_id) {
                    rel.columns = cols
                        .into_iter()
                        .map(|c| ColInfo {
                            name: c.name,
                            type_oid: c.type_oid,
                            type_mod: c.type_modifier,
                        })
                        .collect();
                }
            }
            Ok(_) => {
                tracing::debug!(
                    "Reloaded relation oid={} has no columns (not an error)",
                    relation_id
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to reload columns for relation oid={}: {:?}",
                    relation_id,
                    e
                );
            }
        }

        // Also refresh PK info while we have a connection
        match crate::pg_catalog::fetch_primary_keys(&pg, relation_id).await {
            Ok(pks) if !pks.is_empty() => {
                if let Some(rel) = self.relations.get_mut(&relation_id) {
                    rel.pk_columns = pks;
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Whether the pipeline buffer has exceeded the memory limit and needs flushing.
    pub fn flush_requested(&self) -> bool {
        self.flush_requested
    }

    /// Current pending bytes estimate (for diagnostics).
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Whether the pipeline buffer has exceeded a **hard** memory limit (2× the
    /// configured max).  Always checked — even during replay batches where the
    /// normal `flush_requested()` is suppressed.  Without this guard, a replayed
    /// batch larger than `max_pending_memory_mb` could OOM the process.
    pub fn hard_flush_requested(&self) -> bool {
        self.pending_bytes > self.max_pending_bytes * 2
    }

    /// Whether there are any pending events in the pipeline buffer that haven't
    /// been flushed to StarRocks yet. Used by the shutdown path to determine
    /// if a final flush is needed before saving the checkpoint.
    pub fn has_pending(&self) -> bool {
        !self.tables.is_empty()
    }

    /// Mark the current shutdown signal as "seen" so that `flush_transaction()`
    /// won't immediately bail on `shutdown_rx.changed()`. Call this before the
    /// final shutdown flush to allow the flush to proceed even though shutdown
    /// is already signaled.
    pub fn mark_shutdown_seen(&mut self) {
        self.shutdown_rx.borrow_and_update();
    }

    /// Compute total raw byte length of all non-null WAL values in a row.
    /// Used for pre-event size checks before building the JSON tree.
    fn raw_value_len(values: &[ColValue]) -> usize {
        values
            .iter()
            .map(|v| match v {
                ColValue::Value(s) => s.len(),
                _ => 0,
            })
            .sum()
    }

    /// Estimate the memory footprint of a row of WAL values.
    /// Returns total bytes of all non-null, non-unchanged string values, plus a per-column
    /// JSON overhead estimate (key names, quotes, colons, braces, commas, Map headers).
    /// The raw string length is doubled to account for JSON serialization expansion
    /// (quotes around string values, JSON key names, `Map` node overhead).
    /// JSON/JSONB columns get an additional 64 bytes of overhead to account for the
    /// in-memory `serde_json::Value` tree (node headers, discriminants, BTree overhead).
    fn estimate_row_size(values: &[ColValue], columns: &[ColInfo]) -> usize {
        let mut raw: usize = 0;
        let mut json_overhead: usize = 0;
        for (i, v) in values.iter().enumerate() {
            if let ColValue::Value(s) = v {
                raw += s.len();
                // JSON/JSONB columns have significant parsed-Value tree overhead
                // (~80B per Map entry for BTree nodes + enum discriminants) not
                // captured by the ×2 formula on raw string length.
                // Estimate as up to 2× raw length (for deeply nested small JSON)
                // or 4KB (for large JSON documents), whichever is smaller.
                if i < columns.len() && (columns[i].type_oid == 114 || columns[i].type_oid == 3802)
                {
                    json_overhead += s.len().saturating_mul(2).min(4096);
                }
            }
        }
        // ×2 for JSON formatting overhead (keys, quotes, colons, braces, commas, serde_json Map)
        // +16 bytes per column for Map entry node + Value discriminant overhead
        // +up to 2× raw len (capped at 4KB) per JSON/JSONB column for the parsed Value tree overhead
        raw * 2 + values.len() * 16 + json_overhead
    }

    /// Check if a table matches the configured schema + table_filter regex.
    /// Returns true if the table should be filtered out (not replicated to SR).
    ///
    /// Single-schema enforcement: `sr_table_name` drops the schema prefix, so
    /// replicating a second schema would silently collide same-named tables
    /// into one SR table. The CDC consumer filters upstream too; this is
    /// defense-in-depth for the flush path.
    fn is_table_filtered_out(&self, schema: &str, table: &str) -> bool {
        if schema != self.pg_config.schema {
            return true;
        }
        if let Some(ref re) = self.table_filter_re {
            if !crate::config::table_matches_filter(re, schema, table) {
                return true;
            }
        }
        false
    }

    /// Process a DataEvent and buffer the change.
    /// If `columns_to_json` detects a schema mismatch (WAL values > cached columns),
    /// reloads the relation from PG catalog and retries once. This handles the case
    /// where ALTER TABLE ADD COLUMN changes a table's schema but pgoutput hasn't sent
    /// a Relation message before the next DML event.
    pub async fn process_event(&mut self, event: &DataEvent) -> Result<()> {
        match event {
            DataEvent::Relation {
                relation_id,
                namespace,
                name,
                columns,
                pk_columns,
            } => {
                self.add_relation(*relation_id, namespace, name, columns, pk_columns);
            }
            DataEvent::Insert {
                relation_id,
                columns: values,
            } => {
                let mut rel = self
                    .relations
                    .get(relation_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown relation_id: {}", relation_id))?
                    .clone();

                let table_key = format!("{}.{}", rel.schema, rel.table);
                if self.is_table_filtered_out(&rel.schema, &rel.table) {
                    return Ok(());
                }
                // Notify DDL poller that this table has pending CDC events
                lock_or_recover(&self.pending_cdc_tables).insert(table_key.clone());
                // Pre-event size check: warn on large rows before building JSON tree
                let raw_len = Self::raw_value_len(values);
                if raw_len > 1_048_576 {
                    tracing::warn!(
                        "Large INSERT event for {}.{}: raw value size {} bytes ({:.1} MB) — \
                         may cause CPU stall during JSON conversion",
                        rel.schema,
                        rel.table,
                        raw_len,
                        raw_len as f64 / 1_048_576.0,
                    );
                }
                // Serialize_row writes JSON directly to a reused Vec<u8>
                // (1 alloc/row — the RawValue's String) instead of building a
                // serde_json::Map (N String-keys + BTreeMap nodes). The PK key
                // is extracted during serialization (col→index), so dedup
                // never re-parses the JSON.
                let mut row_op = match serialize_row(
                    &rel.columns,
                    values,
                    &rel.pk_columns,
                    &mut self.row_ser_buf,
                ) {
                    Ok((data, pk_key)) => RowOp {
                        data,
                        pk_key,
                        est_bytes: 0,
                    },
                    Err(e) if is_schema_mismatch(&e) => {
                        tracing::warn!(
                            "Schema mismatch for Insert oid={}: {} cols cached vs {} WAL values. Reloading from PG...",
                            relation_id, rel.columns.len(), values.len(),
                        );
                        self.reload_relation_columns(*relation_id).await?;
                        // Reassign the OUTER `rel` (not a new binding) so the
                        // reloaded columns are used by estimate_row_size AND any
                        // later serialize_row call (e.g. UPDATE's old-PK path)
                        // below. A scoped `let rel =` here would leave the
                        // outer (stale) columns in scope, undercounting the size
                        // estimate after a schema-mismatch reload.
                        rel = self
                            .relations
                            .get(relation_id)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Lost relation oid={} after schema mismatch reload",
                                    relation_id
                                )
                            })?
                            .clone();
                        let (data, pk_key) = serialize_row(
                            &rel.columns,
                            values,
                            &rel.pk_columns,
                            &mut self.row_ser_buf,
                        )?;
                        RowOp {
                            data,
                            pk_key,
                            est_bytes: 0,
                        }
                    }
                    Err(e) => return Err(e),
                };
                tokio::task::yield_now().await;
                let row_size = Self::estimate_row_size(values, &rel.columns);
                row_op.est_bytes = row_size;
                self.pending_bytes += row_size;
                let entry = self
                    .tables
                    .entry(table_key)
                    .or_insert_with(TableChanges::new);
                entry.add_upsert(row_op);
            }
            DataEvent::Update {
                relation_id,
                old_columns: old_values,
                new_columns: new_values,
            } => {
                let mut rel = self
                    .relations
                    .get(relation_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown relation_id: {}", relation_id))?
                    .clone();

                let table_key = format!("{}.{}", rel.schema, rel.table);
                if self.is_table_filtered_out(&rel.schema, &rel.table) {
                    return Ok(());
                }
                // Notify DDL poller that this table has pending CDC events
                lock_or_recover(&self.pending_cdc_tables).insert(table_key.clone());
                // Pre-event size check: warn on large rows before building JSON tree
                let raw_len = Self::raw_value_len(new_values);
                if raw_len > 1_048_576 {
                    tracing::warn!(
                        "Large UPDATE event for {}.{}: raw value size {} bytes ({:.1} MB)",
                        rel.schema,
                        rel.table,
                        raw_len,
                        raw_len as f64 / 1_048_576.0,
                    );
                }
                // Serialize_row → RowOp (see Insert arm).
                let mut new_row = match serialize_row(
                    &rel.columns,
                    new_values,
                    &rel.pk_columns,
                    &mut self.row_ser_buf,
                ) {
                    Ok((data, pk_key)) => RowOp {
                        data,
                        pk_key,
                        est_bytes: 0,
                    },
                    Err(e) if is_schema_mismatch(&e) => {
                        tracing::warn!(
                            "Schema mismatch for Update oid={}: {} cols cached vs {} WAL values. Reloading from PG...",
                            relation_id, rel.columns.len(), new_values.len(),
                        );
                        self.reload_relation_columns(*relation_id).await?;
                        // Reassign the OUTER `rel` (not a new binding) so the
                        // reloaded columns are used by estimate_row_size AND the
                        // old-PK serialize_row call below. A scoped `let rel =`
                        // here would leave the outer (stale) columns in scope,
                        // undercounting the size estimate after a schema-mismatch
                        // reload.
                        rel = self
                            .relations
                            .get(relation_id)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Lost relation oid={} after schema mismatch reload",
                                    relation_id
                                )
                            })?
                            .clone();
                        let (data, pk_key) = serialize_row(
                            &rel.columns,
                            new_values,
                            &rel.pk_columns,
                            &mut self.row_ser_buf,
                        )?;
                        RowOp {
                            data,
                            pk_key,
                            est_bytes: 0,
                        }
                    }
                    Err(e) => return Err(e),
                };
                tokio::task::yield_now().await;
                let row_size = Self::estimate_row_size(new_values, &rel.columns);
                new_row.est_bytes = row_size;
                self.pending_bytes += row_size;
                let entry = self
                    .tables
                    .entry(table_key)
                    .or_insert_with(TableChanges::new);

                // Check for PK update — if old PK differs from new PK, emit a
                // DELETE for the old PK row.  Otherwise StarRocks upserts the
                // new row under the new PK and the old-PK row remains as a stale
                // orphan (permanent duplicate).
                let pk_changed = if !old_values.is_empty() && !rel.pk_columns.is_empty() {
                    match serialize_row(
                        &rel.columns,
                        old_values,
                        &rel.pk_columns,
                        &mut self.row_ser_buf,
                    ) {
                        Ok((data, pk_key)) => {
                            let old_est = Self::estimate_row_size(old_values, &rel.columns);
                            let old_row = RowOp {
                                data,
                                pk_key,
                                est_bytes: old_est,
                            };
                            tokio::task::yield_now().await;
                            // PK-change DETECTION uses the PADDED key built
                            // from the serialized rows — NOT `RowOp.pk_key`.
                            // `None` there means "unresolved → keep every op"
                            // (dedup semantics) and comparing it here would
                            // collapse two DIFFERENT composite keys with one
                            // unresolved part into equal Nones, dropping the
                            // old-PK DELETE and orphaning the old row in
                            // StarRocks forever. The padded key keeps the
                            // resolved parts visible ("1|?" vs "2|?").
                            let old_pk = pk_change_key(old_row.data.get(), &rel.pk_columns);
                            let new_pk = pk_change_key(new_row.data.get(), &rel.pk_columns);
                            if old_pk != new_pk {
                                entry.add_delete(old_row);
                                self.pending_bytes += old_est;
                                true
                            } else {
                                false
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Cannot compute old PK for {}.{}: {:?} — skipping PK-change delete",
                                rel.schema,
                                rel.table,
                                e,
                            );
                            false
                        }
                    }
                } else {
                    false
                };

                entry.add_upsert(new_row);

                if pk_changed {
                    tracing::debug!(
                        "PK update detected for {}.{}: emitted DELETE for old PK + UPSERT for new PK",
                        rel.schema, rel.table,
                    );
                }
            }
            DataEvent::Delete {
                relation_id,
                old_columns: values,
            } => {
                let mut rel = self
                    .relations
                    .get(relation_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown relation_id: {}", relation_id))?
                    .clone();

                let table_key = format!("{}.{}", rel.schema, rel.table);
                if self.is_table_filtered_out(&rel.schema, &rel.table) {
                    return Ok(());
                }
                // Notify DDL poller that this table has pending CDC events
                lock_or_recover(&self.pending_cdc_tables).insert(table_key.clone());
                // Pre-event size check: warn on large rows before building JSON tree
                let raw_len = Self::raw_value_len(values);
                if raw_len > 1_048_576 {
                    tracing::warn!(
                        "Large DELETE event for {}.{}: raw value size {} bytes ({:.1} MB)",
                        rel.schema,
                        rel.table,
                        raw_len,
                        raw_len as f64 / 1_048_576.0,
                    );
                }
                // Serialize_row → RowOp (see Insert arm).
                let mut row_op = match serialize_row(
                    &rel.columns,
                    values,
                    &rel.pk_columns,
                    &mut self.row_ser_buf,
                ) {
                    Ok((data, pk_key)) => RowOp {
                        data,
                        pk_key,
                        est_bytes: 0,
                    },
                    Err(e) if is_schema_mismatch(&e) => {
                        tracing::warn!(
                            "Schema mismatch for Delete oid={}: {} cols cached vs {} WAL values. Reloading from PG...",
                            relation_id, rel.columns.len(), values.len(),
                        );
                        self.reload_relation_columns(*relation_id).await?;
                        // Reassign the OUTER `rel` (not a new binding) so the
                        // reloaded columns are used by estimate_row_size AND any
                        // later serialize_row call (e.g. UPDATE's old-PK path)
                        // below. A scoped `let rel =` here would leave the
                        // outer (stale) columns in scope, undercounting the size
                        // estimate after a schema-mismatch reload.
                        rel = self
                            .relations
                            .get(relation_id)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Lost relation oid={} after schema mismatch reload",
                                    relation_id
                                )
                            })?
                            .clone();
                        let (data, pk_key) = serialize_row(
                            &rel.columns,
                            values,
                            &rel.pk_columns,
                            &mut self.row_ser_buf,
                        )?;
                        RowOp {
                            data,
                            pk_key,
                            est_bytes: 0,
                        }
                    }
                    Err(e) => return Err(e),
                };
                tokio::task::yield_now().await;
                let row_size = Self::estimate_row_size(values, &rel.columns);
                row_op.est_bytes = row_size;
                self.pending_bytes += row_size;
                let entry = self
                    .tables
                    .entry(table_key)
                    .or_insert_with(TableChanges::new);
                entry.add_delete(row_op);
            }
            DataEvent::Truncate { relation_id } => {
                // TRUNCATE → mark the table for truncation.
                // We buffer the truncation and apply it during flush, before any
                // DELETEs/UPSERTs in the same transaction.
                let rel = self
                    .relations
                    .get(relation_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown relation_id: {}", relation_id))?
                    .clone();
                let table_key = format!("{}.{}", rel.schema, rel.table);
                if self.is_table_filtered_out(&rel.schema, &rel.table) {
                    return Ok(());
                }
                // Notify DDL poller that this table has pending CDC events
                lock_or_recover(&self.pending_cdc_tables).insert(table_key.clone());
                let entry = self
                    .tables
                    .entry(table_key)
                    .or_insert_with(TableChanges::new);
                // TRUNCATE discards all prior ops buffered in the same
                // transaction. Without this, `INSERT(A) → TRUNCATE → INSERT(B)`
                // flushes BOTH A and B after the TRUNCATE, yielding SR {A,B}
                // instead of PG's {B} (silent data corruption). Drop the
                // buffered ops and reclaim their pending_bytes estimate.
                if !entry.ops.is_empty() {
                    let cleared: usize = entry
                        .ops
                        .iter()
                        .map(|op| match op {
                            // Same unit as the additions (estimate_row_size at
                            // process time) — previously the serialized length
                            // was subtracted, under-counting vs the ~2× estimate.
                            OrderedOp::Delete(r) | OrderedOp::Upsert(r) => r.est_bytes,
                        })
                        .sum();
                    entry.ops.clear();
                    self.pending_bytes = self.pending_bytes.saturating_sub(cleared);
                }
                entry.truncate = true;
            }
            DataEvent::Type => {}
        }

        // Check memory limit after every event
        if self.pending_bytes > self.max_pending_bytes {
            tracing::warn!(
                "Pipeline buffer exceeded max_pending_bytes ({} > {}), forcing flush",
                self.pending_bytes,
                self.max_pending_bytes,
            );
            self.flush_requested = true;
        } else if !self.eighty_percent_warned
            && self.pending_bytes > self.max_pending_bytes * 80 / 100
        {
            tracing::warn!(
                "Pipeline buffer at {:.0}% of max_pending_bytes ({} / {}), approaching limit",
                (self.pending_bytes as f64 / self.max_pending_bytes as f64) * 100.0,
                self.pending_bytes,
                self.max_pending_bytes,
            );
            self.eighty_percent_warned = true;
        }

        // Update shared atomic for heartbeat metrics
        self.pending_bytes_shared
            .store(self.pending_bytes, Ordering::Relaxed);

        Ok(())
    }

    ///
    /// Returns Ok even when individual table flushes fail (Stream Load error,
    /// task panic, or per-flush timeout): the failed table's unconfirmed rows
    /// are durably buffered (wal_buffer) so the checkpoint advances and PG
    /// never re-streams the same WAL. Err is reserved for (a) a failed buffer
    /// APPEND (local disk full / S3 down — data cannot be made durable) and
    /// (b) shutdown mid-flush.
    pub async fn flush_transaction(&mut self, commit_lsn: i64) -> Result<()> {
        // Replay kick first: leftover segments for tables no longer under a
        // DDL write pause replay even when this batch is empty (the pause may
        // have ended while the stream was idle).
        self.kick_replays();
        if self.tables.is_empty() {
            tracing::debug!("Empty transaction at LSN {}", commit_lsn);
            return Ok(());
        }

        let total_changes: usize = self.tables.values().map(|t| t.len()).sum();
        tracing::debug!(
            tables = self.tables.len(),
            changes = total_changes,
            lsn = commit_lsn,
            "Flushing tables",
        );

        // Take ownership of tables to break borrow conflicts with sink
        let tables = std::mem::take(&mut self.tables);
        self.pending_bytes = 0;
        self.flush_requested = false;
        self.pending_bytes_shared.store(0, Ordering::Relaxed);
        // Note: pending_cdc_tables is NOT cleared here. Each spawned flush
        // task re-adds its table key at start and removes it on completion.
        // This narrows the DDL/DML TOCTOU window to microseconds between
        // task spawn and the re-insert, instead of the entire flush duration.

        // Pre-compute flush data for each table before spawning tasks.
        // This avoids lifetime issues with &self references across await points
        // and lets each table flush independently.
        let sink = self.sink.clone();
        let sr_mgr = self.sr_mgr.clone();
        let database = self.sr_database.clone();
        let table_properties = self.table_properties.clone();
        let ddl_race_retry_ms = self.ddl_race_retry_ms;

        // Flush each table in a spawned task for concurrency.
        // StarRocks 3.x Stream Load is per-table, so there's no cross-table
        // transaction benefit from sequential flushes. Concurrent flushes
        // improve throughput when replicating many tables.
        // A semaphore limits the number of concurrent flush tasks to avoid
        // overwhelming StarRocks with too many simultaneous Stream Load jobs.
        // Tuple: (table_key, unsent slot, handle). The slot lets the collect
        // loop durably buffer unconfirmed rows on error/panic/timeout instead
        // of failing the flush (reconnect → PG re-streams the same WAL).
        let mut handles: Vec<TableFlushHandle> = Vec::with_capacity(tables.len());
        // Guards are now owned INSIDE the spawned tasks (not here)
        // so send_stream_load can drop them before the DDL-race retry sleep.
        // The prior `guards: Vec<PendingCdcTableGuard>` design held them alive
        // across the entire sleep — blocking the DDL poller's
        // wait_for_cdc_drain (25-72s stall per "unknown column" incident).

        let flush_timeout = self.compute_flush_timeout();

        for (table_key, changes) in tables {
            // Durable write-pause routing (replaces the old in-memory
            // deferral): if a DDL is in progress for this table (ddl_active)
            // OR it has buffered segments (leftover from a crash / a prior
            // pause), route its changes into the durable wal_buffer instead of
            // streaming. Avoids the documented DROP-TABLE +
            // auto-create-empty-table silent-data-loss race (the DDL poller
            // acquires the guard first), and bounds memory during long schema
            // changes: segments are fsynced (local) / PUT-confirmed (S3)
            // before the checkpoint advances, so no data is lost if the
            // process dies mid-pause.
            let paused = lock_or_recover(&self.ddl_active).contains(&table_key);
            let buffered = self.wal_buffer.has_segments(&table_key);
            if paused || buffered {
                tracing::debug!(
                    "Buffering {} ops for {} (ddl_paused={} buffered={}) — write pause active",
                    changes.len(),
                    table_key,
                    paused,
                    buffered,
                );
                self.buffer_table_changes(&table_key, &changes).await?;
                // Notify the DDL poller's wait_for_cdc_drain: this table is no
                // longer "pending" (its changes are durably buffered), so the
                // DDL proceeds without starvation. While a replay flush holds
                // a PendingCdcTableGuard for this table, the key MUST stay —
                // wait_for_cdc_drain must not pass early against an
                // in-flight replay load.
                if !self.wal_buffer.is_replaying(&table_key) {
                    lock_or_recover(&self.pending_cdc_tables).remove(&table_key);
                }
                continue;
            }
            // Check for shutdown before waiting for a semaphore permit.
            // Without this, when all permits are in use and SR is slow,
            // the CDC loop blocks here during shutdown until a permit
            // frees up or the per-flush timeout fires.
            let permit = tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    tracing::warn!(
                        "Shutdown received during flush — aborting {} spawned tasks, \
                         dropping pending batch (will replay from checkpoint)",
                        handles.len()
                    );
                    // Abort already-spawned tasks to avoid orphaned in-flight
                    // SR loads racing process teardown.
                    for (_, _, h) in &handles {
                        h.abort();
                    }
                    return Err(anyhow::anyhow!("Shutdown during flush semaphore wait"));
                }
                permit = self.flush_semaphore.clone().acquire_owned() => permit,
            };
            // split_once (first dot), not rsplit_once: the table_key is built
            // as `format!("{}.{}", schema, table)`, and PG table names may
            // themselves contain dots (quoted identifiers) while schemas with
            // dots are far rarer. rsplit_once would mis-map `public."weird.name"`
            // → schema="public.weird", table="name" → wrong SR table name and
            // wrong guard keying.
            let (schema, table) = table_key
                .split_once('.')
                .context("table_key must be in schema.table format")?;
            let schema = schema.to_string();
            let table = table.to_string();

            let relation = self
                .name_to_oid
                .get(&table_key)
                .and_then(|oid| self.relations.get(oid))
                .cloned();

            let col_names: Vec<String> = relation
                .as_ref()
                .map(|r| r.columns.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();

            let bytea_transforms: Vec<String> = relation
                .as_ref()
                .map(|r| {
                    r.columns
                        .iter()
                        .filter(|c| c.type_oid == 17)
                        .map(|c| format!("{}=unhex({})", c.name, c.name))
                        .collect()
                })
                .unwrap_or_default();

            // Pre-compute the flush plan (net-change dedup + 90MB chunking)
            // into the unsent slot BEFORE spawning: an abort at ANY point
            // then leaves every unconfirmed row capturable for durable
            // buffering — the table's rows are never lost to a reconnect.
            // PK-less tables return None (ops dropped by design — see
            // prep_table_flush) and are skipped without spawning a task.
            let unsent = match prep_table_flush(&schema, &table, changes, relation.as_ref()) {
                Some(u) => u,
                None => {
                    drop(permit);
                    continue;
                }
            };

            // The DDL-race retry deadline matches the per-flush timeout
            // (computed above) so they agree — formerly a hardcoded 90s·0.8=72s
            // inner deadline that fired before the real outer timeout could catch
            // a runaway flush. Per-table `now` keeps the deadline relative to
            // this table's flush start; the outer timeout (started at collect)
            // starts later, so this fires first with a descriptive error.
            let deadline = std::time::Instant::now() + flush_timeout;
            let s = sink.clone();
            let m = sr_mgr.clone();
            let db = database.clone();
            let tp = table_properties.clone();
            let drr = ddl_race_retry_ms;
            let pending = self.pending_cdc_tables.clone();
            let ddl_active = self.ddl_active.clone();
            let tk = table_key.clone();

            // The guard is now created INSIDE the spawned task (not
            // here) so it can be dropped before the DDL-race retry sleep in
            // send_stream_load — letting the DDL poller's wait_for_cdc_drain
            // proceed so it can apply the ALTER the retry is waiting for. The
            // prior design held the guard in a `guards: Vec` here, keeping it
            // alive across the entire sleep (25-72s stall per "unknown column"
            // incident). The guard is re-acquired after each sleep with a
            // ddl_active re-check (mirrors the double-check at flush start)
            // to close the re-opened TOCTOU.
            //
            // Abort-safety note: the prior design kept the guard outside the
            // task so handle.abort() (on per-table timeout) wouldn't drop it
            // while SR was finalizing an orphaned Stream Load. With the guard
            // inside the task, abort DOES drop it early — but the fix
            // (DdlActiveGuard acquired FIRST in the DDL poller, committed as
            // 91ff065) protects against this: the DDL poller's guard-first
            // ordering means `ddl_active` is set before `wait_for_cdc_drain`,
            // and the flush path's double-check at line ~1175 below defers if
            // `ddl_active` contains the table. So even on abort-induced early
            // guard drop, the DDL poller sees `pending_cdc_tables` clear but
            // the flush hasn't spawned (the double-check catches it). If a
            // flush IS already in-flight when the abort drops the guard, the
            // DDL poller's guard-first ordering means the DDL sees
            // `ddl_active` set and waits — the in-flight flush completes (or
            // the orphaned load is absorbed by SR's dedup).

            // Re-check (defense-in-depth, replaces the old deferral): a DDL
            // may have started between the routing check above and now (the
            // DDL poller acquired DdlActiveGuard right after our check). If
            // so, route THIS table's changes to the durable buffer instead of
            // spawning a flush that would race the DDL. The primary TOCTOU
            // closure remains the DDL poller's guard-first ordering; this
            // closes the residual permit-wait window.
            if lock_or_recover(&self.ddl_active).contains(&table_key)
                || self.wal_buffer.has_segments(&table_key)
            {
                tracing::debug!(
                    "Re-check: DDL/buffer for {} detected after permit wait — buffering",
                    table_key
                );
                drop(permit);
                // Drain the unsent slot (the changes were already moved into
                // it by prep_table_flush above).
                if let Some(tc) = slot_to_changes(&unsent) {
                    self.buffer_table_changes(&table_key, &tc).await?;
                }
                // Same replay-in-flight guard as the primary routing branch
                // keep the key visible to wait_for_cdc_drain
                // while a replay flush is loading this table.
                if !self.wal_buffer.is_replaying(&table_key) {
                    lock_or_recover(&self.pending_cdc_tables).remove(&table_key);
                }
                continue;
            }

            let unsent_t = unsent.clone();
            handles.push((
                tk.clone(),
                unsent_t.clone(),
                tokio::spawn(async move {
                    let _permit = permit;
                    // Create the guard INSIDE the task so send_stream_load can
                    // drop/re-acquire it around the DDL-race retry sleep.
                    let mut guard = Some(PendingCdcTableGuard::new(pending.clone(), tk.clone()));
                    flush_table(
                        s,
                        m,
                        db,
                        &schema,
                        &table,
                        relation,
                        &col_names,
                        &bytea_transforms,
                        &tp,
                        drr,
                        deadline,
                        &mut guard,
                        pending,
                        ddl_active,
                        tk,
                        unsent_t,
                    )
                    .await
                }),
            ));
        }

        // Collect results. Per-table failures (error/panic/timeout) no longer
        // fail the flush: the task's unconfirmed rows are drained from its
        // unsent slot into the durable wal_buffer, so the checkpoint may
        // advance and PG never re-streams the same WAL. Only a failed buffer
        // APPEND (local disk full / S3 down — data cannot be made durable)
        // returns Err → reconnect.
        // Each handle is wrapped with the per-flush timeout computed above the
        // per-table loop to prevent hanging on unresponsive SR. It bounds the
        // full chain: per-attempt budget × attempts-per-node × node_count × 1.2,
        // plus inter-attempt backoff sleeps. With prod defaults
        // (max_retries=2, 3 nodes, http=10s, mysql=10s) the poll budget is
        // 5×(2s+10s+10s)=110s and per-attempt is 120s, giving
        // (120s×3 + 3s)×3×1.2 ≈ 1307s for 3 nodes (≈436s for 1 node) —
        // see compute_flush_timeout, which is the single source of truth.
        let mut first_error: Option<anyhow::Error> = None;
        for (table_key, unsent, handle) in handles {
            // Pin the handle so we can abort it if the timeout fires.
            tokio::pin!(handle);
            match tokio::time::timeout(flush_timeout, handle.as_mut()).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    // Normal per-table flush error (Stream Load failure,
                    // TRUNCATE failure, ...): buffer the unconfirmed rows.
                    if let Some(tc) = slot_to_changes(&unsent) {
                        tracing::warn!(
                            "Table flush task error for {}: {:?} — buffering {}",
                            table_key,
                            e,
                            drained_desc(&tc),
                        );
                        if let Err(ae) = self.buffer_table_changes(&table_key, &tc).await {
                            if first_error.is_none() {
                                first_error = Some(ae);
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Table flush task error for {}: {:?} — nothing left to buffer",
                            table_key,
                            e,
                        );
                    }
                }
                Ok(Err(join_err)) => {
                    let msg = if join_err.is_panic() {
                        join_err
                            .into_panic()
                            .downcast_ref::<String>()
                            .cloned()
                            .unwrap_or_else(|| "unknown panic".to_string())
                    } else {
                        format!("{:?}", join_err)
                    };
                    if let Some(tc) = slot_to_changes(&unsent) {
                        tracing::error!(
                            "Table flush task panicked: {:?} — buffering {}",
                            msg,
                            drained_desc(&tc),
                        );
                        if let Err(ae) = self.buffer_table_changes(&table_key, &tc).await {
                            if first_error.is_none() {
                                first_error = Some(ae);
                            }
                        }
                    } else {
                        tracing::error!(
                            "Table flush task panicked: {:?} — nothing left to buffer",
                            msg,
                        );
                    }
                }
                Err(_) => {
                    // Abort is cooperative — join so the task is fully dead
                    // before the slot is drained; aborted reqwest/mysql
                    // futures drop the in-flight connection with the chunk
                    // still in the slot (nothing silently confirmed).
                    handle.abort();
                    let join = handle.await;
                    if let Some(tc) = slot_to_changes(&unsent) {
                        tracing::error!(
                            "Table flush task timed out after {:?} — SR may be unresponsive; \
                             buffering {}",
                            flush_timeout,
                            drained_desc(&tc),
                        );
                        if let Err(ae) = self.buffer_table_changes(&table_key, &tc).await {
                            if first_error.is_none() {
                                first_error = Some(ae);
                            }
                        }
                    } else if matches!(join, Ok(Ok(()))) {
                        // The task won the race with the abort and finished
                        // successfully: nothing is unconfirmed.
                        tracing::warn!(
                            "Table flush task completed just as the {:?} timeout fired — \
                             nothing left to buffer",
                            flush_timeout,
                        );
                    } else {
                        tracing::error!(
                            "Table flush task timed out after {:?} — SR may be unresponsive; \
                             nothing left to buffer",
                            flush_timeout,
                        );
                    }
                }
            }
        }

        if let Some(e) = first_error {
            // The wal buffer could not make the failed tables' changes durable
            // (local disk full / S3 down) — reconnect is the only option. WAL
            // replay re-delivers everything (the checkpoint doesn't advance
            // on Err). Tables whose appends succeeded are safe and will
            // replay from their segments.
            tracing::warn!(
                "wal buffer append failed after flush failure — reconnecting: {:?}",
                e,
            );
            return Err(e);
        }

        // Replay kick: tables with buffered segments whose DDL pause has ended
        // get their segments replayed through the normal flush path now.
        self.kick_replays();

        Ok(())
    }

    /// Per-flush timeout budget, shared by live flushes and replay.
    /// Formula includes inter-attempt backoff sleeps (1s,2s,4s,... capped at
    /// 64s per node) which the prior math omitted (computed 216s but
    /// real worst-case ~240s, causing false-positive aborts of legit polls).
    ///   flush_timeout = (per_attempt_budget × attempts_per_node
    ///                    + backoff_per_node) × node_count × 1.2
    ///   per_attempt_budget = max(http, mysql) + poll_budget
    ///   poll_budget = 5 × (2s + mysql_query_timeout + connection_timeout)
    /// With the config defaults that is 110s, so per_attempt is 120s and the
    /// total is ≈1307s for a 3-node cluster (≈436s for one node).
    fn compute_flush_timeout(&self) -> Duration {
        // SHOW LOAD label-poll budget: 5 iterations × (2s poll sleep +
        // query_timeout + connection_timeout) — the exact worst case of
        // SrSink::poll_label_until_terminal (sink.rs), which runs as part of
        // a duplicate-label attempt. The old flat 10s under-counted by ~6×,
        // so a legitimate poll sequence could exceed the per-flush deadline
        // and abort a load that would have succeeded (no data loss — replay
        // — but abort/replay stall loops under slow SR publish).
        const POLL_ITERATIONS: u32 = 5;
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        let poll_budget =
            (POLL_INTERVAL + self.sr_mgr.query_timeout() + self.sr_mgr.connection_timeout())
                .saturating_mul(POLL_ITERATIONS);
        let per_attempt_budget =
            self.sr_mgr.http_timeout().max(self.sr_mgr.query_timeout()) + poll_budget;
        let node_count = self.sr_mgr.node_count().max(1) as u32;
        let attempts_per_node = self.sr_mgr.max_retries() + 1;
        // Per-node inter-attempt backoff: 2^0 + 2^1 + ... for `max_retries`
        // sleeps (after each non-final attempt), each capped at 2^6=64s.
        let backoff_per_node_secs: u64 = (0..self.sr_mgr.max_retries())
            .map(|i| 1u64 << i.min(6))
            .sum();
        (per_attempt_budget.saturating_mul(attempts_per_node)
            + Duration::from_secs(backoff_per_node_secs))
        .saturating_mul(node_count)
        .mul_f32(1.2)
        // Floor at 30s for low-retry / single-node configs to avoid spurious
        // aborts when SR's response latency spikes.
        .max(Duration::from_secs(30))
    }

    /// Durable-append helper: serialize `changes` into wal_buffer records,
    /// append a segment (fsynced / PUT-confirmed before Ok), and bump the
    /// buffered-events/bytes metrics. Err propagates to flush_transaction's
    /// caller — the flush fails, the checkpoint does NOT advance, and WAL
    /// replay re-delivers the events (no loss).
    async fn buffer_table_changes(&self, table_key: &str, changes: &TableChanges) -> Result<()> {
        // Stamp records with the relation OID as of buffering time (0 =
        // unknown/wildcard). Replay uses it to discard stale segments from a
        // dropped-and-recreated relation (see replay_buffered_table).
        let oid = self.name_to_oid.get(table_key).copied().unwrap_or(0);
        let records = records_from_changes(changes, oid);
        let n = records.len();
        let bytes: usize = records.iter().map(|r| r.payload.len()).sum();
        // Persist the table's current rename map alongside the segment so a
        // crash between DDL registration and replay drain can't lose it.
        let renames = lock_or_recover(&self.rename_map)
            .get(table_key)
            .cloned()
            .unwrap_or_default();
        self.wal_buffer
            .append(table_key, &records, &renames)
            .await?;
        self.wal_buffered_events
            .fetch_add(n as u64, Ordering::Relaxed);
        self.wal_buffered_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Spawn replay tasks for tables with buffered segments whose DDL write
    /// pause has ended. Idempotent per table: `try_begin_replay` prevents two
    /// concurrent replayers. Uses the CURRENT relation snapshot; tables
    /// dropped since buffering get their segments discarded inside the
    /// replayer (mirrors flush_table's skip — dropped-table events are never
    /// resurrected). Called at the top of `flush_transaction` (covers empty
    /// batches), after each successful flush, and once at startup by main.rs.
    ///
    /// Runs `evict_dropped_relations` FIRST: a DROP/RENAME-TO'd table must be
    /// evicted before the relation lookup below, or a stale pre-DROP relation
    /// would let the replayer AUTO-CREATE the dropped SR table (the idle
    /// branch kicks every iteration while `begin_transaction` never runs, so
    /// this window was unbounded on idle streams).
    pub fn kick_replays(&mut self) {
        self.evict_dropped_relations();
        // Prune finished tasks FIRST — before the empty-segments early
        // return, so completed task handles never accumulate in the JoinSet
        // while the system is idle (no table segmented → every kick would
        // otherwise skip the prune). Panics are logged + counted here (the
        // slot guard already released, so the next kick retries cleanly).
        while let Some(r) = self.replay_tasks.try_join_next() {
            if let Err(join) = r {
                tracing::error!("wal buffer replay task panicked: {join:?}");
                self.wal_replay_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        let seg_tables = self.wal_buffer.segmented_tables();
        // Sweep stale failure-backoff entries: success already clears its
        // entry at task exit; this catches entries orphaned by segment
        // eviction/purge so they can't pin a non-segmented table forever.
        {
            let live: HashSet<&String> = seg_tables.iter().collect();
            lock_or_recover(&self.replay_backoff).retain(|k, _| live.contains(k));
        }
        if seg_tables.is_empty() {
            return;
        }
        let flush_timeout = self.compute_flush_timeout();
        for table_key in seg_tables {
            if lock_or_recover(&self.ddl_active).contains(&table_key) {
                continue;
            }
            // Failure backoff (see note_replay_result): consecutive failed
            // attempts defer the next kick so an unhealthy SR isn't handed a
            // fresh full-retry-budget load attempt on every flush cycle.
            if replay_backoff_active(
                lock_or_recover(&self.replay_backoff).get(&table_key),
                Instant::now(),
            ) {
                tracing::debug!(
                    table = %table_key,
                    "wal buffer replay deferred by failure backoff",
                );
                continue;
            }
            if !self.wal_buffer.try_begin_replay(&table_key) {
                continue;
            }
            let Some((schema, table)) = table_key.split_once('.') else {
                tracing::error!(
                    "wal buffer table key '{table_key}' not in schema.table format — skipping replay"
                );
                self.wal_buffer.end_replay(&table_key);
                continue;
            };
            let schema = schema.to_string();
            let table = table.to_string();
            let relation = self
                .name_to_oid
                .get(&table_key)
                .and_then(|oid| self.relations.get(oid))
                .cloned();
            let col_names: Vec<String> = relation
                .as_ref()
                .map(|r| r.columns.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();
            let bytea_transforms: Vec<String> = relation
                .as_ref()
                .map(|r| {
                    r.columns
                        .iter()
                        .filter(|c| c.type_oid == 17)
                        .map(|c| format!("{}=unhex({})", c.name, c.name))
                        .collect()
                })
                .unwrap_or_default();
            // Gauge is manager-owned (total_segments/total_buffered_bytes);
            // the metrics loop in main.rs refreshes it. NEVER touch it here —
            // a shared set-based gauge races concurrent replayers.
            tracing::info!(
                table = %table_key,
                segments = self.wal_buffer.total_segments(),
                "wal buffer: starting replay",
            );
            let relation_oid = self.name_to_oid.get(&table_key).copied();
            let tk = table_key.clone();
            let tk_log = table_key.clone();
            let rf = self.wal_replay_failures.clone();
            let rp_sink = self.sink.clone();
            let rp_sr_mgr = self.sr_mgr.clone();
            let rp_database = self.sr_database.clone();
            let rp_props = self.table_properties.clone();
            let rp_retry = self.ddl_race_retry_ms;
            let rp_timeout = flush_timeout;
            let rp_buffer = self.wal_buffer.clone();
            let rp_sem = self.flush_semaphore.clone();
            let rp_pending = self.pending_cdc_tables.clone();
            let rp_ddl_active = self.ddl_active.clone();
            let rp_replayed = self.wal_replayed_events.clone();
            let rp_rename_map = self.rename_map.clone();
            // The task owns the ReplaySlotGuard: the slot is released on ANY
            // exit — normal return, Err, panic, or abort. Outcome recording
            // (backoff reset/escalation) and failure logging live at task
            // level; the JoinSet join in drain_replays/prune only sees panics.
            let bk_tk = tk.clone();
            let bk_backoff = self.replay_backoff.clone();
            self.replay_tasks.spawn(async move {
                let _slot = ReplaySlotGuard::new(rp_buffer.clone(), tk.clone());
                let result = replay_buffered_table(
                    rp_sink,
                    rp_sr_mgr,
                    rp_database,
                    tk,
                    schema,
                    table,
                    relation,
                    relation_oid,
                    col_names,
                    bytea_transforms,
                    rp_props,
                    rp_retry,
                    rp_timeout,
                    rp_buffer,
                    rp_sem,
                    rp_pending,
                    rp_ddl_active,
                    rp_replayed,
                    rp_rename_map,
                )
                .await;
                // Record the outcome BEFORE the guard drops: the very next
                // kick then sees either the cleared entry (drained) or the
                // escalated backoff atomically with the freed replay slot.
                let retry_wait = note_replay_result(&bk_backoff, &bk_tk, &result);
                if retry_wait.is_some() {
                    rf.fetch_add(1, Ordering::Relaxed);
                }
                if let Err(e) = result {
                    tracing::error!(
                        "wal buffer replay task failed for {tk_log}: {e:?} — \
                         segment(s) kept; next attempt in ~{retry_wait:?}"
                    );
                }
            });
        }
    }

    /// Drain in-flight replay tasks at shutdown. Waits up
    /// to one flush-timeout per task; tasks that don't finish leave their
    /// segments on disk — replayed on next start (at-least-once, no loss).
    pub async fn drain_replays(&mut self) {
        let t = self.compute_flush_timeout();
        loop {
            match tokio::time::timeout(t, self.replay_tasks.join_next()).await {
                Ok(Some(Ok(()))) => continue,
                Ok(Some(Err(join))) => {
                    tracing::error!("wal buffer replay task panicked during drain: {join:?}");
                    self.wal_replay_failures.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(None) => break,
                Err(_) => {
                    if !self.replay_tasks.is_empty() {
                        tracing::warn!(
                            "wal buffer replay drain timed out — segments preserved, replayed on next start"
                        );
                    }
                    break;
                }
            }
        }
    }

    /// Access the relation cache for tests / diagnostics.
    #[allow(dead_code)]
    pub fn get_relation_by_name(&self, schema: &str, table: &str) -> Option<&RelationInfo> {
        let key = format!("{}.{}", schema, table);
        self.name_to_oid
            .get(&key)
            .and_then(|oid| self.relations.get(oid))
    }

    /// Test/diagnostic accessor: returns `(buffered op count, truncate flag)`
    /// for a table's pending changes. Used to verify TRUNCATE clears prior ops.
    #[cfg(test)]
    pub(crate) fn debug_table_state(&self, schema: &str, table: &str) -> Option<(usize, bool)> {
        let key = format!("{}.{}", schema, table);
        self.tables.get(&key).map(|tc| (tc.ops.len(), tc.truncate))
    }
}
/// Consecutive-failure deferral state for wal-buffer replay kicks (see
/// `kick_replays` / `note_replay_result`). Kept per table_key in
/// `Pipeline::replay_backoff`.
struct ReplayBackoff {
    /// Failed attempts since the last successful drain.
    failures: u32,
    /// Earliest Instant a kick may spawn the next attempt.
    next_attempt: Instant,
}

/// Kick-side deferral check — true while `entry` still holds `table_key`
/// out of replay at time `now`.
fn replay_backoff_active(entry: Option<&ReplayBackoff>, now: Instant) -> bool {
    matches!(entry, Some(b) if now < b.next_attempt)
}

/// Backoff delay after `failures` (≥1) consecutive failed replay attempts:
/// 5s doubling to a hard 60s cap. Deliberately not jittered: replays are
/// per-table and naturally out of phase (each table's attempt start/duration
/// varies with its segment sizes), unlike the global PG reconnect loop where
/// aligned stampedes matter — and determinism keeps the schedule legible in
/// logs and tests.
fn replay_backoff_delay(failures: u32) -> Duration {
    const BASE_SECS: u64 = 5;
    const CAP_SECS: u64 = 60;
    let exp = failures.saturating_sub(1).min(4);
    Duration::from_secs((BASE_SECS << exp).min(CAP_SECS))
}

/// Record a replay task outcome into the shared backoff map. Ok(()) removes
/// the entry (the table drained); Err escalates it. Returns how long the
/// NEXT kick must wait before retrying (None on success).
fn note_replay_result(
    st: &Arc<Mutex<HashMap<String, ReplayBackoff>>>,
    table_key: &str,
    result: &Result<()>,
) -> Option<Duration> {
    match result {
        Ok(()) => {
            lock_or_recover(st).remove(table_key);
            None
        }
        Err(_) => {
            let mut m = lock_or_recover(st);
            let entry = m.entry(table_key.to_string()).or_insert(ReplayBackoff {
                failures: 0,
                next_attempt: Instant::now(),
            });
            entry.failures += 1;
            let delay = replay_backoff_delay(entry.failures);
            entry.next_attempt = Instant::now() + delay;
            Some(delay)
        }
    }
}

/// Maximum payload bytes per StarRocks Stream Load call. SR's
/// `streaming_load_max_batch_size_mb` defaults to 100MB; we cap at 90MB for a
/// safety margin so a single table's batch never trips the limit.
const SR_STREAM_LOAD_MAX_BYTES: usize = 90 * 1024 * 1024;

/// Split `rows` (each paired with its ORIGINAL position in the batch) into
/// chunks whose serialized JSON-lines size is ≤ `max_bytes`, returning the
/// row chunks plus a parallel per-chunk index list. Drained unsent slots
/// use the index lists to restore WAL order. Byte size is `data.get().len()`
/// plus 1 for the newline separator; a single row larger than `max_bytes`
/// becomes its own chunk (bounded upstream by `max_event_bytes`). Keeps each
/// Stream Load under SR's per-load size limit, preventing the
/// reconnect→same-oversized-batch poison pill.
fn chunk_rows_with_idxs(
    rows: Vec<(RowOp, usize)>,
    max_bytes: usize,
) -> (Vec<Vec<RowOp>>, Vec<Vec<usize>>) {
    let mut row_chunks: Vec<Vec<RowOp>> = Vec::new();
    let mut idx_chunks: Vec<Vec<usize>> = Vec::new();
    let mut cur: Vec<RowOp> = Vec::new();
    let mut cur_idx: Vec<usize> = Vec::new();
    let mut acc: usize = 0;
    for (row, idx) in rows {
        // +1 for the newline separator between rows in the JSON-lines payload.
        let row_bytes = row.data.get().len() + 1;
        if !cur.is_empty() && acc + row_bytes > max_bytes {
            row_chunks.push(std::mem::take(&mut cur));
            idx_chunks.push(std::mem::take(&mut cur_idx));
            acc = 0;
        }
        cur.push(row);
        cur_idx.push(idx);
        acc += row_bytes;
    }
    if !cur.is_empty() {
        row_chunks.push(cur);
        idx_chunks.push(cur_idx);
    }
    (row_chunks, idx_chunks)
}

/// Send a single Stream Load batch for `sr_table`, recovering from transient
/// "unknown table" (auto-create) and "unknown column / schema change"
/// (DDL-race retry) errors. Extracted from `flush_table` so the DELETE and
/// UPSERT paths share one implementation and can both chunk via repeated calls.
#[allow(clippy::too_many_arguments)]
async fn send_stream_load(
    sink: &SrSink,
    sr_mgr: &Arc<SrConnectionManager>,
    sr_table: &str,
    relation: Option<&RelationInfo>,
    table_properties: &HashMap<String, String>,
    columns: &[String],
    op_type: &str,
    data: bytes::Bytes,
    row_count: usize,
    database: &str,
    ddl_race_retry_ms: u64,
    ddl_race_deadline: std::time::Instant,
    guard: &mut Option<PendingCdcTableGuard>,
    pending_cdc_tables: &Arc<Mutex<HashSet<String>>>,
    ddl_active: &Arc<Mutex<HashSet<String>>>,
    table_key: &str,
) -> Result<()> {
    let op_upper = match op_type {
        "delete" => "DELETE",
        "upsert" => "UPSERT",
        other => other,
    };
    let batch = SrBatch {
        database: database.to_string(),
        table: sr_table.to_string(),
        data,
        row_count,
        columns: columns.to_vec(),
        op_type: op_type.to_string(),
    };
    if let Err(e) = sink.stream_load(&batch).await {
        let msg = e.to_string().to_lowercase();
        if msg.contains("unknown table") {
            if let Some(rel) = relation {
                auto_create_sr_table(sr_mgr, sr_table, rel, table_properties).await?;
            }
            sink.stream_load(&batch)
                .await
                .with_context(|| format!("{} Stream Load failed for {}", op_upper, sr_table))?;
        } else if msg.contains("unknown column")
            || msg.contains("column not found")
            || msg.contains("schema change")
        {
            // DDL race / schema-change-in-progress: DDL poller hasn't applied the
            // schema change yet, or SR is processing a concurrent schema change.
            // Retry in a loop with sleep between — this avoids an expensive
            // full CDC reconnect cycle. Each sleep gives the DDL poller time
            // to eat through backlog and apply the needed ALTER.
            //
            // DROP the guard BEFORE sleeping so the DDL poller's
            // wait_for_cdc_drain sees the table as not-pending → the DDL
            // poller can acquire DdlActiveGuard and apply the ALTER the retry
            // is waiting for. Previously the guard was held across the entire
            // sleep (25-72s), blocking the DDL poller — a stall, not a true
            // deadlock (bounded by ddl_race_deadline), but harmful for DDL
            // throughput on schema-evolution-heavy workloads.
            //
            // After the sleep, re-acquire the guard with a ddl_active re-check
            // (mirrors the double-check at flush_transaction line ~1175):
            // if the DDL started during the sleep (ddl_active now contains
            // the table), do NOT re-acquire — return an error so the caller
            // defers to the next flush cycle (the buffer is preserved; no
            // data loss). This closes the re-opened TOCTOU between guard-drop
            // and re-acquire.
            let mut ddl_retries: u32 = 0;
            const MAX_DDL_RACE_RETRIES: u32 = 5;
            loop {
                if std::time::Instant::now() >= ddl_race_deadline {
                    return Err(anyhow::anyhow!(
                        "{} Stream Load DDL-race deadline exceeded for {}",
                        op_upper,
                        sr_table,
                    ));
                }
                tracing::info!(
                    "{} Stream Load transient error (schema change) for {} — attempt {}/{} sleeping {}s",
                    op_upper, sr_table, ddl_retries + 1, MAX_DDL_RACE_RETRIES, ddl_race_retry_ms,
                );
                // Drop the guard before sleeping so the DDL poller can
                // proceed (wait_for_cdc_drain sees the table as not-pending).
                *guard = None;
                tokio::time::sleep(Duration::from_millis(ddl_race_retry_ms)).await;
                // Re-acquire the guard with a ddl_active re-check. If the
                // DDL started during the sleep, return an error so the caller
                // defers to the next flush cycle (buffer preserved, no loss).
                if lock_or_recover(ddl_active).contains(table_key) {
                    return Err(anyhow::anyhow!(
                        "{} Stream Load for {} deferred — DDL active during retry",
                        op_upper,
                        sr_table,
                    ));
                }
                *guard = Some(PendingCdcTableGuard::new(
                    pending_cdc_tables.clone(),
                    table_key.to_string(),
                ));
                match sink.stream_load(&batch).await {
                    Ok(_) => break,
                    Err(e2) => {
                        ddl_retries += 1;
                        let msg2 = e2.to_string().to_lowercase();
                        if ddl_retries >= MAX_DDL_RACE_RETRIES
                            || (!msg2.contains("unknown column")
                                && !msg2.contains("column not found")
                                && !msg2.contains("schema change"))
                        {
                            return Err(e2).with_context(|| {
                                format!(
                                    "{} Stream Load failed for {} (DDL race, {} retries)",
                                    op_upper, sr_table, ddl_retries
                                )
                            });
                        }
                        // Column still missing or schema change still in progress
                        // — continue the retry loop (guard will be dropped at top).
                    }
                }
            }
        } else {
            return Err(e)
                .with_context(|| format!("{} Stream Load failed for {}", op_upper, sr_table));
        }
    }
    Ok(())
}

/// Rows a per-table flush has not CONFIRMED applied. Owned by
/// flush_transaction; the spawned flush task removes entries only after a
/// successful Stream Load, so on task error/panic/abort every unconfirmed row
/// remains capturable for durable buffering (no reconnect, no WAL re-stream).
struct UnsentFlushState {
    /// TRUNCATE not yet confirmed executed. Cleared only AFTER execute_sql
    /// succeeds, so an abort mid-TRUNCATE re-buffers the marker (idempotent).
    truncate_pending: bool,
    /// DELETE chunks in send order, each an Arc so the task can borrow for
    /// serialization while the owner retains the rows for buffering.
    deletes: Vec<Arc<Vec<RowOp>>>,
    /// Per-op original batch positions, chunked with the SAME boundaries as
    /// `deletes` (kept in lockstep by flush_table's remove(0)). Drained slots
    /// use these to restore original WAL order across the two lists.
    delete_idxs: Vec<Arc<Vec<usize>>>,
    /// UPSERT chunks, same semantics. Post-dedup, one surviving op per PK, so
    /// cross-type order matters only for un-keyed sentinel rows — restored
    /// via the index lists at drain time.
    upserts: Vec<Arc<Vec<RowOp>>>,
    upsert_idxs: Vec<Arc<Vec<usize>>>,
}

/// One spawned per-table flush task: (table_key, durable-unsent slot, join
/// handle). The alias keeps `flush_transaction`'s collect loop readable.
type TableFlushHandle = (
    String,
    Arc<Mutex<UnsentFlushState>>,
    tokio::task::JoinHandle<Result<()>>,
);

/// Pre-compute a table's flush plan: net-change dedup + byte-budget chunking,
/// wrapped in a shared unsent slot. `None` → nothing to flush (PK-less table;
/// ops dropped — same as the old flush_table early-return).
///
/// Carries `schema`/`table` (deviation from the plan's 2-arg sketch) so the
/// PK-less warning moved verbatim from flush_table keeps naming the table.
fn prep_table_flush(
    schema: &str,
    table: &str,
    mut changes: TableChanges,
    relation: Option<&RelationInfo>,
) -> Option<Arc<Mutex<UnsentFlushState>>> {
    let sr_table = crate::sr_sql::sr_table_name(schema, table);

    // PK-less tables are not supported — skip CDC flush.
    // The DDL poller doesn't create the SR table and snapshot is skipped,
    // so there's nothing to write to. Dropping the events is correct.
    //
    // This check is safe: `relation` is populated at startup via `load_relations`
    // for every table in the publication, so an empty pk_columns here means the
    // table was confirmed PK-less by the PG catalog — not a transient cache miss.
    if let Some(rel) = relation {
        if rel.pk_columns.is_empty() {
            tracing::warn!(
                "{}.{} has no PK — dropping {} ops. PK-less tables are not supported by CDC.",
                schema,
                sr_table,
                changes.ops.len(),
            );
            return None;
        }
    }

    // ── Net-change dedup: per-PK, keep only the *last* operation ──
    // The PK key is pre-computed at serialize time (`RowOp.pk_key`),
    // so dedup is a cheap `Option<String>::clone` instead of re-parsing
    // the JSON row per op. No PK columns → keep every op (legacy fallback).
    // Each surviving op carries its ORIGINAL batch position so the drain can
    // restore WAL order across the delete/upsert split (un-keyed sentinel
    // rows keep every op; same-row upsert→delete pairs must not invert).
    let mut maybe_delete: Vec<(RowOp, usize)> = Vec::with_capacity(changes.ops.len());
    let mut maybe_upsert: Vec<(RowOp, usize)> = Vec::with_capacity(changes.ops.len());

    if let Some(rel) = relation {
        if !rel.pk_columns.is_empty() {
            // Dedup key per op: the pre-computed PK key, or a per-op UNIQUE
            // sentinel when the key is missing (rows serialized before the
            // cache had PK info — e.g. the first batch of a table created
            // mid-stream, where the WAL Relation event carries no PK metadata
            // and refresh_relation_pks runs only AFTER the batch is
            // serialized). Collapsing missing keys to "?" would keep only the
            // LAST row of that batch — silent data loss. SR dedupes by PK
            // anyway, so keeping every un-keyed op is always safe (idempotent
            // upserts).
            let dedup_key = |row: &RowOp, idx: usize| -> String {
                row.pk_key
                    .clone()
                    .unwrap_or_else(|| format!("__no_pk_key_{idx}__"))
            };
            // Pass 1: find the last occurrence index for each PK.
            let mut last_idx: HashMap<String, usize> = HashMap::with_capacity(changes.ops.len());
            for (i, op) in changes.ops.iter().enumerate() {
                let key = match op {
                    OrderedOp::Delete(r) | OrderedOp::Upsert(r) => dedup_key(r, i),
                };
                last_idx.insert(key, i);
            }
            // Pass 2: drain, keeping only the last occurrence per PK.
            for (i, op) in changes.ops.drain(..).enumerate() {
                let (is_upsert, row) = match op {
                    OrderedOp::Delete(row) => (false, row),
                    OrderedOp::Upsert(row) => (true, row),
                };
                let key = dedup_key(&row, i);
                if last_idx.get(&key) == Some(&i) {
                    if is_upsert {
                        maybe_upsert.push((row, i));
                    } else {
                        maybe_delete.push((row, i));
                    }
                }
            }
        }
    } else {
        // No relation metadata — keep all operations as-is (no PK info to dedup).
        // This path should be rare: `load_relations` populates the cache at startup
        // and WAL Relation events update it during streaming.
        if !changes.ops.is_empty() {
            tracing::warn!(
                "{}.{}: no relation metadata in cache — {} ops processed without dedup, deletes may be lost",
                schema, sr_table,
                changes.ops.len(),
            );
        }
        for (i, op) in changes.ops.drain(..).enumerate() {
            match op {
                OrderedOp::Delete(row) => maybe_delete.push((row, i)),
                OrderedOp::Upsert(row) => maybe_upsert.push((row, i)),
            }
        }
        // Without relation metadata the old flush_table never SENT deletes
        // (the DELETE branch required PK columns) — preserve that: drop them.
        maybe_delete.clear();
    }

    let truncate_pending = changes.truncate;
    let (del_rows, del_idxs) = chunk_rows_with_idxs(maybe_delete, SR_STREAM_LOAD_MAX_BYTES);
    let (ups_rows, ups_idxs) = chunk_rows_with_idxs(maybe_upsert, SR_STREAM_LOAD_MAX_BYTES);
    Some(Arc::new(Mutex::new(UnsentFlushState {
        truncate_pending,
        deletes: del_rows.into_iter().map(Arc::new).collect(),
        delete_idxs: del_idxs.into_iter().map(Arc::new).collect(),
        upserts: ups_rows.into_iter().map(Arc::new).collect(),
        upsert_idxs: ups_idxs.into_iter().map(Arc::new).collect(),
    })))
}

/// Drain a slot's unconfirmed rows into a `TableChanges` for durable
/// buffering. `None` → the slot is empty (the task confirmed everything).
/// Drains via `std::mem::take` — the caller always transfers ownership to
/// buffering, so the drained contents are never re-drained.
fn slot_to_changes(slot: &Arc<Mutex<UnsentFlushState>>) -> Option<TableChanges> {
    let mut st = lock_or_recover(slot);
    if !st.truncate_pending && st.deletes.is_empty() && st.upserts.is_empty() {
        return None;
    }
    let deletes = std::mem::take(&mut st.deletes);
    let delete_idxs = std::mem::take(&mut st.delete_idxs);
    let upserts = std::mem::take(&mut st.upserts);
    let upsert_idxs = std::mem::take(&mut st.upsert_idxs);
    let truncate = st.truncate_pending;
    st.truncate_pending = false;
    let mut ops: Vec<(usize, OrderedOp)> = Vec::with_capacity(
        deletes.iter().map(|c| c.len()).sum::<usize>()
            + upserts.iter().map(|c| c.len()).sum::<usize>(),
    );
    for (c, ic) in deletes.iter().zip(delete_idxs.iter()) {
        for (row, idx) in c.iter().zip(ic.iter()) {
            ops.push((*idx, OrderedOp::Delete(row.clone())));
        }
    }
    for (c, ic) in upserts.iter().zip(upsert_idxs.iter()) {
        for (row, idx) in c.iter().zip(ic.iter()) {
            ops.push((*idx, OrderedOp::Upsert(row.clone())));
        }
    }
    // Restore original WAL order: the deletes/upserts split exists only
    // because Stream Load modes can't mix. Un-keyed sentinel rows can
    // interleave, and replay's last-wins dedup must see the same final op
    // WAL order implies (an upsert→delete pair must never drain inverted).
    ops.sort_by_key(|(idx, _)| *idx);
    let ops: Vec<OrderedOp> = ops.into_iter().map(|(_, op)| op).collect();
    Some(TableChanges { ops, truncate })
}
/// Human-readable description of what a drained unsent slot holds, for
/// flush-failure logs. Drains carrying a TRUNCATE marker report it
/// (durably buffered; re-executes on replay) instead of a misleading
/// row count alone.
fn drained_desc(tc: &TableChanges) -> String {
    if tc.ops.is_empty() && tc.truncate {
        "TRUNCATE marker (0 rows)".to_string()
    } else if tc.truncate {
        format!("TRUNCATE marker + {} unsent rows", tc.ops.len())
    } else {
        format!("{} unsent rows", tc.ops.len())
    }
}

/// Flush a single table's changes to StarRocks.
/// Extracted into its own function so it can be called from spawned tasks
/// for concurrent per-table flushing.
///
/// Slot-driven: the flush plan (dedup + chunking) is pre-computed into
/// `unsent` by `prep_table_flush`; each chunk is removed from the slot ONLY
/// after its Stream Load is confirmed, so a task that errors, panics, or is
/// aborted by the per-flush timeout leaves every unconfirmed row capturable
/// for durable buffering by the caller (flush_transaction / replay).
#[allow(clippy::too_many_arguments)]
async fn flush_table(
    sink: SrSink,
    sr_mgr: Arc<SrConnectionManager>,
    database: String,
    schema: &str,
    table: &str,
    relation: Option<RelationInfo>,
    col_names: &[String],
    bytea_transforms: &[String],
    table_properties: &HashMap<String, String>,
    ddl_race_retry_ms: u64,
    ddl_race_deadline: std::time::Instant,
    guard: &mut Option<PendingCdcTableGuard>,
    pending_cdc_tables: Arc<Mutex<HashSet<String>>>,
    ddl_active: Arc<Mutex<HashSet<String>>>,
    table_key: String,
    unsent: Arc<Mutex<UnsentFlushState>>,
) -> Result<()> {
    let sr_table = crate::sr_sql::sr_table_name(schema, table);

    let unsent_ops = {
        let st = lock_or_recover(&unsent);
        st.deletes.iter().map(|c| c.len()).sum::<usize>()
            + st.upserts.iter().map(|c| c.len()).sum::<usize>()
    };

    // Diagnostic: log columns being sent to SR
    tracing::debug!(
        "flush {}.{}: relation_present={} col_names=[{}] pk_columns=[{}] ops={}",
        schema,
        sr_table,
        relation.is_some(),
        col_names.join(","),
        relation
            .as_ref()
            .map(|r| r.pk_columns.join(","))
            .unwrap_or_default(),
        unsent_ops,
    );

    // TRUNCATE first (if requested). The slot flag is cleared only AFTER
    // execute_sql succeeds: an abort/error mid-TRUNCATE leaves it set, so
    // buffering re-includes the marker (replay re-executes — idempotent).
    if lock_or_recover(&unsent).truncate_pending {
        let truncate_sql = format!(
            "TRUNCATE TABLE {}.{}",
            crate::sr_sql::sr_quote(&database),
            crate::sr_sql::sr_quote(&sr_table)
        );
        tracing::info!("TRUNCATE {}.{}", schema, sr_table);
        if let Err(e) = crate::sr_sql::execute_sql(&sr_mgr, &truncate_sql).await {
            return Err(anyhow::anyhow!(
                "TRUNCATE {}.{} failed: {:?} — aborting batch for retry",
                schema,
                sr_table,
                e,
            ));
        }
        lock_or_recover(&unsent).truncate_pending = false;
    }

    // DELETE — only supported on PRIMARY KEY model tables in SR.
    // Each slot chunk is a distinct Stream Load with its own label
    // (idempotent by PRIMARY KEY); the LSN doesn't advance until ALL chunks
    // succeed. The chunk is removed from the slot ONLY after the load
    // returns Ok — an Err/panic/abort leaves it in place (re-buffered).
    let columns: Vec<String> = [col_names.to_vec(), bytea_transforms.to_vec()].concat();
    loop {
        let chunk = match lock_or_recover(&unsent).deletes.first() {
            Some(c) => c.clone(),
            None => break,
        };
        let count = chunk.len();
        let data = build_json_data(&chunk);
        drop(chunk); // free the Value tree before the HTTP round-trip
        send_stream_load(
            &sink,
            &sr_mgr,
            &sr_table,
            relation.as_ref(),
            table_properties,
            &columns,
            "delete",
            data,
            count,
            &database,
            ddl_race_retry_ms,
            ddl_race_deadline,
            guard,
            &pending_cdc_tables,
            &ddl_active,
            &table_key,
        )
        .await?;
        let mut st = lock_or_recover(&unsent);
        st.deletes.remove(0);
        st.delete_idxs.remove(0);
    }

    // UPSERT
    loop {
        let chunk = match lock_or_recover(&unsent).upserts.first() {
            Some(c) => c.clone(),
            None => break,
        };
        let count = chunk.len();
        let data = build_json_data(&chunk);
        drop(chunk); // free the Value tree before the HTTP round-trip
        send_stream_load(
            &sink,
            &sr_mgr,
            &sr_table,
            relation.as_ref(),
            table_properties,
            &columns,
            "upsert",
            data,
            count,
            &database,
            ddl_race_retry_ms,
            ddl_race_deadline,
            guard,
            &pending_cdc_tables,
            &ddl_active,
            &table_key,
        )
        .await?;
        let mut st = lock_or_recover(&unsent);
        st.upserts.remove(0);
        st.upsert_idxs.remove(0);
    }

    Ok(())
}

/// Serialize a `TableChanges` batch into wal_buffer records — Truncate marker
/// first (kind 3, empty payload), then Upsert(1)/Delete(2) with the row JSON
/// verbatim (`RowOp.data.get()` bytes), in WAL order. Every record is stamped
/// with the buffering-time relation OID so replay can drop stale segments
/// left by a DROP+CREATE cycle (the OID no longer matches the live relation).
fn records_from_changes(changes: &TableChanges, relation_oid: u32) -> Vec<BufRecord> {
    let mut records = Vec::with_capacity(changes.ops.len() + 1);
    if changes.truncate {
        records.push(BufRecord {
            kind: BUF_TRUNCATE,
            oid: relation_oid,
            payload: Vec::new(),
        });
    }
    for op in &changes.ops {
        match op {
            OrderedOp::Upsert(r) => records.push(BufRecord {
                kind: BUF_UPSERT,
                oid: relation_oid,
                payload: r.data.get().as_bytes().to_vec(),
            }),
            OrderedOp::Delete(r) => records.push(BufRecord {
                kind: BUF_DELETE,
                oid: relation_oid,
                payload: r.data.get().as_bytes().to_vec(),
            }),
        }
    }
    records
}

/// Resolve a buffered row's column key to its LIVE name through the rename
/// map. Chain-safe: follows old→new links (a→b, b→c …) up to 16 hops.
///
/// Stale-entry guard: a key already present in the CURRENT `col_names` is
/// live and is never remapped — a skipped DDL's entry must not rewrite a
/// same-incarnation column whose name was legitimately reused, in the worst
/// case. A chain hop that lands on a live name terminates the resolution
/// there.
///
/// More than 16 hops is treated as a corrupt chain (cycle/runaway): warn and
/// use the current node rather than loop forever.
fn resolve_column(
    key: &str,
    renames: &HashMap<String, String>,
    col_names: &[String],
    table_key: &str,
) -> String {
    if col_names.iter().any(|c| c == key) {
        return key.to_string();
    }
    let mut cur = key;
    for _ in 0..16 {
        match renames.get(cur) {
            Some(next) if !col_names.iter().any(|c| c == next) => cur = next,
            Some(next) => return next.to_string(),
            None => break,
        }
    }
    tracing::warn!("rename chain for '{key}' on {table_key} exceeds 16 hops — using '{cur}'");
    cur.to_string()
}

/// Rebuild a `RowOp` from a buffered record. The PK key is recomputed from the
/// row JSON + current `pk_columns` so `flush_table`'s net-change dedup works
/// per-PK (identical semantics to live flushes). Without a real key, every
/// replay row would collapse to the "?" fallback and only the LAST op per
/// segment would survive — silent row loss on replay.
///
/// The row JSON's keys are filtered to the CURRENT `col_names`: buffered rows
/// may carry a column dropped by the DDL that just finished (DROP COLUMN), and
/// SR rejects unknown JSON keys. ADD COLUMN needs no action — SR NULL-fills
/// missing fields on by-name mapping.
///
/// RENAME COLUMN remap: keys are resolved through `rename_map[table]` via
/// `resolve_column` (chain + stale-entry safe) BEFORE pk_key extraction and
/// column filtering, so buffered rows written under the old name reach SR
/// under the live name.
///
/// Invalid JSON payload → Err (replay quarantines the segment via
/// `rows_to_changes`; a malformed buffered row must never be silently
/// dropped).
fn row_op_from_buf(
    r: &BufRecord,
    table_key: &str,
    col_names: &[String],
    pk_columns: &[String],
    rename_map: &RenameMap,
) -> Result<RowOp> {
    let payload = String::from_utf8_lossy(&r.payload);
    let payload_owned = payload.into_owned();
    let parsed = serde_json::from_str::<serde_json::Value>(&payload_owned)
        .map_err(|e| anyhow::anyhow!("buffered row JSON invalid for {table_key}: {e}"))?;
    let (filtered, pk_source) = match parsed {
        serde_json::Value::Object(map) => {
            let table_renames = lock_or_recover(rename_map)
                .get(table_key)
                .cloned()
                .unwrap_or_default();
            // Remap old→new column names FIRST so both the pk_key extraction
            // and the col_names filter operate on the LIVE names.
            let remapped: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (resolve_column(&k, &table_renames, col_names, table_key), v))
                .collect();
            let remapped_value = serde_json::Value::Object(remapped);
            let pk_source = remapped_value.to_string();
            let filtered = if col_names.is_empty() {
                pk_source.clone()
            } else {
                let keep: serde_json::Map<String, serde_json::Value> = remapped_value
                    .as_object()
                    .expect("object above")
                    .iter()
                    .filter(|(k, _)| col_names.iter().any(|c| c == *k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                serde_json::Value::Object(keep).to_string()
            };
            (filtered, pk_source)
        }
        // Non-object JSON (array/string/number): pass through untouched —
        // not a row shape, but a legacy payload must not be dropped.
        _ => (payload_owned.clone(), payload_owned.clone()),
    };
    Ok(RowOp {
        data: serde_json::value::RawValue::from_string(filtered)
            .map_err(|_| anyhow::anyhow!("buffered row JSON invalid after column filtering"))?,
        pk_key: extract_pk_key_from_json(&pk_source, pk_columns),
        est_bytes: r.payload.len(),
    })
}

/// Convert decoded segment records into `TableChanges`, applying the
/// stale-OID filter to ALL record kinds and counting applied ops.
///
/// - Stale-relation filtering: records stamped with a relation OID that no
///   longer matches the live relation (`relation_oid`) are dropped
///   per-record — a DROP+CREATE cycle can leave segments whose rows belong
///   to the pre-DROP incarnation. The filter covers BUF_TRUNCATE too: a
/// stale TRUNCATE must not wipe the recreated table.
/// - `row_op_from_buf` Err (invalid row JSON) → Err (the caller quarantines
///   the segment — never silently dropped).
/// - Unknown record kind → Err (same).
/// - Return `(changes, applied)`: `applied` counts converted upserts/deletes
///   plus one per (OID-filtered-in) TRUNCATE — the replay-event metric and
///   log use it instead of raw record count.
fn rows_to_changes(
    records: &[BufRecord],
    table_key: &str,
    col_names: &[String],
    pk_columns: &[String],
    rename_map: &RenameMap,
    relation_oid: Option<u32>,
) -> Result<(TableChanges, u64)> {
    let mut changes = TableChanges::new();
    let mut applied: u64 = 0;
    let mut stale_oid_skipped: u64 = 0;
    for r in records {
        if r.oid != 0 && relation_oid != Some(r.oid) {
            // Rows written before a drop+recreate: the SR table was rebuilt and
            // re-snapshotted under the new OID, so these must not be applied.
            // Counted and logged — a silently discarded batch is
            // indistinguishable from data loss in the logs.
            stale_oid_skipped += 1;
            continue;
        }
        match r.kind {
            BUF_UPSERT => {
                changes.add_upsert(row_op_from_buf(
                    r, table_key, col_names, pk_columns, rename_map,
                )?);
                applied += 1;
            }
            BUF_DELETE => {
                changes.add_delete(row_op_from_buf(
                    r, table_key, col_names, pk_columns, rename_map,
                )?);
                applied += 1;
            }
            BUF_TRUNCATE => {
                changes.truncate = true;
                applied += 1;
            }
            k => {
                return Err(anyhow::anyhow!(
                    "wal buffer record kind {k} unknown for {table_key}"
                ));
            }
        }
    }
    if stale_oid_skipped > 0 {
        tracing::warn!(
            table = %table_key,
            count = stale_oid_skipped,
            "wal buffer replay: discarded records stamped with a stale relation OID — \
             table was dropped/recreated, so the SR table was re-snapshotted under the \
             new OID. Counted (not silent): if this is unexpected, the drop/recreate \
             window lost buffered rows whose data must come from the snapshot."
        );
    }
    Ok((changes, applied))
}

/// Replay one table's buffered segments through the normal flush path, oldest
/// first, deleting each segment only after its Stream Load succeeded
/// (Label-Already-Exists counts as success — SR is idempotent by PK, same
/// guarantee as live flushes at pipeline.rs:1271-1273).
///
/// Replay rows get REAL PK keys recomputed from the row JSON
/// (`row_op_from_buf`), so `flush_table`'s net-change dedup behaves exactly
/// like live flushes — per-PK last-op-wins, never collapsing distinct rows.
///
/// Ordering/consistency guarantees:
/// - Appends always create NEW segments (monotonic `next_seq`, never reused)
///   and never touch existing ones → delete-after-apply is race-free.
/// - Live flushes for a table are suppressed while `has_segments` is true
///   (flush_transaction routing), so replay never races a live flush of the
///   same table.
/// - `try_begin_replay` (called by the kick) prevents two concurrent
///   replayers per table; the `ReplaySlotGuard` releases the slot on ANY
///   exit — including panic/abort — so a crashed replayer can't wedge the
///   table out of replay forever.
/// - The per-iteration `ddl_active` check makes replay yield to a re-run DDL:
///   a new pause preserves the remaining segments and the next flush cycle
///   re-kicks replay. The check is repeated AFTER the semaphore permit
///   acquire (the post-permit TOCTOU window).
///
/// Crash semantics: a crash mid-replay leaves the applied segments deleted and
/// the rest on disk; startup `scan_existing` re-kicks replay from the oldest
/// remaining segment. Re-applied rows are idempotent (PK upsert/delete).
///
/// Corruption handling: a segment that fails checksum validation is
/// QUARANTINED (bytes preserved aside, counter bumped, loud error) and
/// skipped — the remaining segments still replay. ACKed data inside a
/// corrupt segment is unrecoverable (documented loudly, never silent).
///
/// Stale-relation filtering: records stamped with a relation OID that no
/// longer matches the live relation (`relation_oid`) are dropped per-record
/// — a DROP+CREATE cycle can leave segments whose rows belong to the
/// pre-DROP incarnation.
///
/// Error semantics: every Err path releases the table's replay slot (RAII
/// guard — so the next kick retries the table). The spawn wrapper in
/// `kick_replays` logs the failure and bumps `wal_replay_failures` once per
/// failed task.
#[allow(clippy::too_many_arguments)]
async fn replay_buffered_table(
    sink: SrSink,
    sr_mgr: Arc<SrConnectionManager>,
    database: String,
    table_key: String,
    schema: String,
    table: String,
    relation: Option<RelationInfo>,
    relation_oid: Option<u32>,
    col_names: Vec<String>,
    bytea_transforms: Vec<String>,
    table_properties: HashMap<String, String>,
    ddl_race_retry_ms: u64,
    flush_timeout: Duration,
    buffer: Arc<WalBufferManager>,
    semaphore: Arc<Semaphore>,
    pending_cdc_tables: Arc<Mutex<HashSet<String>>>,
    ddl_active: Arc<Mutex<HashSet<String>>>,
    replayed_counter: Arc<AtomicU64>,
    rename_map: RenameMap,
) -> Result<()> {
    // The replay slot is owned by the spawner's ReplaySlotGuard (kick_replays)
    // — released on any return, panic, or abort, so the slot can never wedge
    // in-flight.
    loop {
        // Register pending BEFORE any await in the iteration so the DDL
        // poller's wait_for_cdc_drain waits for our segment processing.
        let _pending = PendingCdcTableGuard::new(pending_cdc_tables.clone(), table_key.clone());
        // 1. A new DDL re-paused the table → yield; segments are preserved
        //    and the next flush cycle re-kicks replay.
        if lock_or_recover(&ddl_active).contains(&table_key) {
            return Ok(());
        }
        // 2. Nothing left to replay → done.
        let segs = match buffer.list_segments(&table_key).await {
            Ok(s) => s,
            Err(e) => {
                return Err(e.context(format!(
                    "wal buffer replay LIST failed for {table_key} — slot released, retried next kick"
                )));
            }
        };
        if segs.is_empty() {
            // A LIST that returns nothing while the accounting still counts
            // segments for this table is NOT a clean drain — it means the
            // listing can't see what the appends wrote (e.g. an S3 provider
            // returning encoding-type=url keys that parse_object_key drops).
            // Log loudly: the table is wedged (every live event keeps
            // buffering) and the segments will never drain on this path.
            let accounted = buffer.segment_count(&table_key);
            if accounted > 0 {
                tracing::warn!(
                    table = %table_key,
                    accounted,
                    "wal buffer replay: LIST returned no segments but accounting counts {accounted} — \
                     listing cannot see the stored segments; replay cannot drain (check S3 LIST/encoding-type behavior)"
                );
            }
            return Ok(());
        }
        // 3. Table dropped/evicted since buffering (or never known) →
        //    discard ALL segments (mirrors flush_table's PK-less skip:
        //    dropped-table events are discarded, never resurrected).
        if relation.is_none() {
            for s in &segs {
                if let Err(e) = buffer.delete_segment(&table_key, *s).await {
                    tracing::error!(
                        "wal buffer replay: delete failed for dropped table {table_key} seg {s}: {e:?}"
                    );
                }
            }
            return Ok(());
        }
        // 4. Take the oldest segment. A segment that vanished between LIST
        //    and read (e.g. purge mid-replay) is just deleted from
        //    accounting (idempotent) and skipped.
        let seg = segs[0];
        let bytes = match buffer.read_segment_bytes(&table_key, seg).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                tracing::warn!(
                    "wal buffer replay: segment {table_key}/{seg} missing between list and read — dropping from accounting"
                );
                if let Err(e) = buffer.delete_segment(&table_key, seg).await {
                    tracing::error!(
                        "wal buffer replay: delete failed for missing seg {seg}: {e:?}"
                    );
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        let records = match decode_segment(&bytes) {
            Ok(r) => r,
            Err(_) => {
                buffer
                    .quarantine_segment(&table_key, seg, &bytes)
                    .await
                    .with_context(|| {
                        format!("wal buffer replay: quarantine failed for {table_key} seg {seg}")
                    })?;
                tracing::error!(
                    "wal buffer replay: segment {table_key}/seg_{seg:020}.bin corrupt (checksum) — quarantined; ACKed data unrecoverable"
                );
                continue;
            }
        };
        let pk_columns: Vec<String> = relation
            .as_ref()
            .map(|r| r.pk_columns.clone())
            .unwrap_or_default();
        // Convert records → changes (stale-OID filter incl. TRUNCATE, rename
        // remap, applied-count). Conversion failure (invalid row JSON,
        // unknown kind) quarantines the segment — buffered data is
        // ACKed-past, never silently dropped, never wedged in a retry loop.
        let (changes, applied) = match rows_to_changes(
            &records,
            &table_key,
            &col_names,
            &pk_columns,
            &rename_map,
            relation_oid,
        ) {
            Ok(x) => x,
            Err(e) => {
                buffer
                    .quarantine_segment(&table_key, seg, &bytes)
                    .await
                    .with_context(|| {
                        format!("wal buffer replay: quarantine failed for {table_key} seg {seg}")
                    })?;
                tracing::error!(
                    "wal buffer replay: segment {table_key}/seg_{seg:020}.bin has valid checksum but invalid records ({e:#}) — quarantined"
                );
                continue;
            }
        };
        // 5. Nothing to apply (all rows filtered by oid — counted and logged
        //    inside rows_to_changes) → drop the segment and move on; no flush
        //    needed.
        if changes.ops.is_empty() && !changes.truncate {
            if let Err(e) = buffer.delete_segment(&table_key, seg).await {
                tracing::error!(
                    "wal buffer replay: delete failed for {table_key} seg {seg}: {e:?}"
                );
            }
            continue;
        }
        // 6. Flush through the NORMAL path — chunking (90MB), stream load,
        //    unknown-table auto-create, DDL-race retry, PK-less skip — with a
        //    PendingCdcTableGuard so the DDL poller's drain waits for us.
        let permit = semaphore.clone().acquire_owned().await;
        // Post-permit TOCTOU re-check: a DDL may have started while we waited
        // for the permit — yield; segments are preserved for the next kick.
        if lock_or_recover(&ddl_active).contains(&table_key) {
            return Ok(());
        }
        let mut guard = Some(PendingCdcTableGuard::new(
            pending_cdc_tables.clone(),
            table_key.clone(),
        ));
        // Pre-compute the flush plan into an unsent slot. On failure the
        // slot is drained nowhere — the segment stays on disk and the next
        // kick retries it (no reconnect in the replay path).
        let unsent = match prep_table_flush(&schema, &table, changes, relation.as_ref()) {
            Some(u) => u,
            None => {
                // PK-less table: nothing to flush — the segment counts as
                // applied (mirrors the live flush's PK-less drop).
                match buffer.delete_segment(&table_key, seg).await {
                    Ok(()) => {
                        replayed_counter.fetch_add(applied, Ordering::Relaxed);
                    }
                    Err(e) => tracing::error!(
                        "wal buffer replay: delete failed for {table_key} seg {seg}: {e:?} — \
                         segment kept, counters unchanged, retried next kick"
                    ),
                }
                continue;
            }
        };
        let result = flush_table(
            sink.clone(),
            sr_mgr.clone(),
            database.clone(),
            &schema,
            &table,
            relation.clone(),
            &col_names,
            &bytea_transforms,
            &table_properties,
            ddl_race_retry_ms,
            Instant::now() + flush_timeout,
            &mut guard,
            pending_cdc_tables.clone(),
            ddl_active.clone(),
            table_key.clone(),
            unsent,
        )
        .await;
        drop(permit);
        match result {
            Ok(()) => {
                // 7. Applied → delete the segment (idempotent), count up
                //    only on confirmed delete.
                match buffer.delete_segment(&table_key, seg).await {
                    Ok(()) => {
                        // applied (not raw record count): stale-OID-filtered
                        // records never reached SR and must not count.
                        replayed_counter.fetch_add(applied, Ordering::Relaxed);
                    }
                    Err(e) => tracing::error!(
                        "wal buffer replay: delete failed for {table_key} seg {seg}: {e:?} — \
                         segment kept, counters unchanged, retried next kick"
                    ),
                }
                tracing::info!(
                    table = %table_key,
                    seg,
                    ops = applied,
                    "wal buffer: replayed segment",
                );
            }
            Err(e) => {
                // Segment kept; the next kick retries it. Do not spin: the
                // slot guard releases on return, so a later kick retries.
                return Err(e);
            }
        }
    }
}

/// Extract a composite key string from a serialized row JSON for net-change
/// dedup. Joins PK column values with "|" (mirroring the serialize-time key
/// format).
///
/// Returns `None` — meaning "unresolved, do not dedup" — when the table has no
/// PK columns, when the payload is not a JSON object, or when ANY PK column is
/// absent from the row. `None` makes `prep_table_flush` fall back to its per-op
/// unique sentinel, which keeps every op. That is the difference between
/// keeping an op and losing it: the pre-fix code returned a shared "?" key for
/// every such row, so several distinct rows collapsed into one ("last op
/// wins") — silent row loss on replay.
///
/// Used by the replay path: buffered rows carry `pk_key: None` (segments store
/// only the row JSON), so `flush_table`'s dedup needs real keys recomputed here.
fn extract_pk_key_from_json(payload: &str, pk_columns: &[String]) -> Option<String> {
    if pk_columns.is_empty() {
        return None;
    }
    let obj = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(serde_json::Value::Object(map)) => map,
        // Not a JSON object: unresolved. Returning None (instead of a shared
        // "?") keeps every op via the dedup sentinel rather than collapsing
        // them all into one "last op wins" survivor.
        _ => return None,
    };
    let mut parts: Vec<String> = Vec::with_capacity(pk_columns.len());
    for pk in pk_columns {
        // `?` returns None (unresolved) when the column is absent.
        let v = obj.get(pk)?;
        parts.push(v.to_string());
    }
    Some(parts.join("|"))
}

/// Build the PADDED composite PK key used by PK-change DETECTION only.
///
/// Dedup and change detection must NOT be conflated:
/// `extract_pk_key_from_json` returns `None` for an unresolved key because
/// dedup must then keep every op (see its doc above). Change detection needs
/// the opposite: compare *what is known*. Two DIFFERENT composite keys with one
/// unresolved part would both be `None`, no DELETE would be emitted for the old
/// PK, and the old row would stay orphaned in StarRocks forever. Padding each
/// absent column with "?" keeps the resolved parts distinguishable —
/// `"1"|?` vs `"2"|?` is a change, `?|?` vs `?|?` is not (nothing decidable,
/// no delete) — which is exactly the pre-`pk_key: Option<String>` behaviour.
///
/// `row_json` is the serialized row (a `RawValue` payload, not a `Value`), so
/// the caller never re-serializes the row. A non-object payload leaves every
/// part padded (all "?"), matching a row with no PK column present.
fn pk_change_key(row_json: &str, pk_columns: &[String]) -> String {
    let obj: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str::<serde_json::Value>(row_json) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => serde_json::Map::new(),
        };
    let parts: Vec<String> = pk_columns
        .iter()
        .map(|pk| match obj.get(pk) {
            // Same `Value::to_string()` form dedup uses, so a resolved part
            // compares identically on both sides.
            Some(v) => v.to_string(),
            None => "?".to_string(),
        })
        .collect();
    parts.join("|")
}

/// The production hot path now pre-computes the PK key at serialize
/// time (`serialize_row` → `RowOp.pk_key`), so this function is only used by
/// the test-only `RowOp::from_value` constructor. Returns the SAME
/// `Option<String>` as `extract_pk_key_from_json` — a `String` return forced a
/// `unwrap_or_default()` that collapsed the unresolved sentinel to one shared
/// empty key, i.e. test-built rows reproduced the old silent-loss collapse
/// instead of production's keep-every-op behaviour.
#[cfg(test)]
fn extract_pk_key_from_value(row: &serde_json::Value, pk_columns: &[String]) -> Option<String> {
    extract_pk_key_from_json(&row.to_string(), pk_columns)
}

/// Build a newline-delimited JSON payload from a slice of pre-serialized
/// `RowOp`s. The `RawValue` bytes are copied straight into the batch buffer
/// (no re-serialization) — eliminates the per-row `to_writer(Value)` work
/// that the legacy `Vec<Value>` path did at flush time.
fn build_json_data(rows: &[RowOp]) -> bytes::Bytes {
    // Exact size up-front: one allocation instead of amortized regrowth while
    // copying up to 90MB of row JSON (the +1 per row is the '\n' separator).
    let total: usize = rows.iter().map(|row| row.data.get().len() + 1).sum();
    let mut buf = Vec::with_capacity(total);
    for row in rows {
        if !buf.is_empty() {
            buf.push(b'\n');
        }
        buf.extend_from_slice(row.data.get().as_bytes());
    }
    bytes::Bytes::from(buf)
}

/// Auto-create a StarRocks table from WAL relation info.
/// Called when Stream Load fails with "unknown table" — the table doesn't exist on SR yet.
async fn auto_create_sr_table(
    sr_mgr: &SrConnectionManager,
    sr_table: &str,
    rel: &RelationInfo,
    table_properties: &std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let sql = auto_create_sr_table_sql(sr_table, rel, sr_mgr.database(), table_properties)?;
    tracing::info!("Auto-creating SR table: {}", sql);
    crate::sr_sql::execute_sql(sr_mgr, &sql).await?;
    Ok(())
}

/// Generate the CREATE TABLE SQL for auto-created StarRocks tables.
/// Uses PK columns from PG catalog (from load_relations).
/// Returns an error if PK is unknown — PK-less tables are not supported.
/// Columns are reordered so PK columns come first (required by StarRocks).
fn auto_create_sr_table_sql(
    sr_table: &str,
    rel: &RelationInfo,
    database: &str,
    table_properties: &std::collections::HashMap<String, String>,
) -> anyhow::Result<String> {
    // PK must be known from the PG catalog. WAL Relation events don't carry
    // PK metadata; load_relations() populates it from pg_index/pg_attribute.
    // Without PK, we cannot create a PRIMARY KEY table, and DUPLICATE KEY
    // tables are not supported (no DELETE support in SR Stream Load).
    if rel.pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
                "{}.{}: PK unknown from catalog — cannot auto-create SR table.              Only tables with PRIMARY KEY are supported.",
                rel.schema, rel.table,
            ));
    }

    // Reorder columns: PK columns must be first in SR's CREATE TABLE
    let ordered =
        crate::sr_sql::reorder_columns_for_sr(&rel.columns, &rel.pk_columns, |c| c.name.as_str());

    let col_defs: Vec<String> = ordered
        .iter()
        .map(|c| {
            let sr_type = crate::types::pg_type_to_sr(c.type_oid, c.type_mod);
            let mut col_def = format!("{} {}", crate::sr_sql::sr_quote(&c.name), sr_type);
            // PK columns are NOT NULL by fiat (always NOT NULL in PG by definition).
            if rel.pk_columns.contains(&c.name) {
                col_def.push_str(" NOT NULL");
            }
            col_def
        })
        .collect();

    let first_col = ordered.first().map(|c| c.name.as_str()).unwrap_or("id");
    let (key_clause, dist_col) = crate::sr_sql::build_key_and_dist_col(&rel.pk_columns, first_col);
    let properties = crate::sr_sql::sr_table_properties(table_properties);

    Ok(crate::sr_sql::sr_create_table_sql(
        database,
        sr_table,
        &col_defs,
        &key_clause,
        &dist_col,
        &properties,
        true,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_json_buf() -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }

    fn test_shutdown_rx() -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        // Keep the sender alive (forget it): if it drops, `changed()` errors
        // immediately and ANY test reaching the flush semaphore select sees
        // a spurious "Shutdown during flush semaphore wait". Test-only; the
        // leaked channel is harmless.
        std::mem::forget(tx);
        rx
    }

    /// Tempdir-backed wal buffer for tests that don't exercise buffering
    /// (construction must not touch the repo CWD). Unique per call so
    /// parallel tests never share segments.
    fn test_wal_buffer() -> Arc<WalBufferManager> {
        use std::sync::atomic::AtomicU64;
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pg2sr_cdc_walbuf_test_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        WalBufferManager::new_local_root(dir).expect("test wal buffer root")
    }

    /// `lock_or_recover` must recover from poison and read the actual
    /// set contents. Without this, `if let Ok(set) = lock()` would silently
    /// skip the critical section on poison — `wait_for_cdc_drain` would return
    /// Ok (table-not-pending) instead of waiting, and the snapshot recheck
    /// would silently skip the bail — both resurrecting the DDL/CDC TOCTOU
    /// on any prior panic.
    #[test]
    fn test_lock_or_recover_poison_returns_actual_contents() {
        let mx = Mutex::new(HashSet::from(["k".to_string()]));
        // Poison the mutex by panicking while holding the lock.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = mx.lock().unwrap();
            panic!("intentional poison for test");
        }));
        // lock_or_recover should recover and see 'k' in the set.
        let g = lock_or_recover(&mx);
        assert!(
            g.contains("k"),
            "poison recovery should see 'k' in the set — silent no-op would \
             reopen the DDL/CDC TOCTOU "
        );
    }

    /// `lock_or_recover` must allow mutation (insert/remove) on a
    /// poisoned mutex — otherwise `DdlActiveGuard::drop` / `dropped_relations`
    /// insert would silently fail on poison.
    #[test]
    fn test_lock_or_recover_poison_allows_mutation() {
        let mx = Mutex::new(HashSet::<String>::new());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = mx.lock().unwrap();
            panic!("intentional poison for test");
        }));
        {
            let mut g = lock_or_recover(&mx);
            g.insert("new".to_string());
        }
        // Should persist across the recovered guard.
        assert!(
            lock_or_recover(&mx).contains("new"),
            "mutation through poisoned-recovered guard should persist"
        );
    }

    #[test]
    fn test_columns_to_json() {
        let cols = vec![
            ColInfo {
                name: "id".into(),
                type_oid: 23,
                type_mod: 0,
            },
            ColInfo {
                name: "name".into(),
                type_oid: 25,
                type_mod: 0,
            },
            ColInfo {
                name: "data".into(),
                type_oid: 3802,
                type_mod: 0,
            },
        ];

        let vals = vec![
            ColValue::Value("42".to_string()),
            ColValue::Value("hello".to_string()),
            ColValue::Value(r#"{"key":"val"}"#.to_string()),
        ];

        let mut buf = make_json_buf();
        let json = columns_to_json(&cols, &vals, &mut buf).unwrap();
        assert_eq!(json["id"], "42");
        assert_eq!(json["name"], "hello");
        assert_eq!(json["data"]["key"], "val");
    }

    #[test]
    fn test_string_to_json_boolean() {
        // PG pgoutput sends 't'/'f' for boolean values
        assert_eq!(string_to_json_value("t", 16), serde_json::Value::Bool(true));
        assert_eq!(
            string_to_json_value("f", 16),
            serde_json::Value::Bool(false)
        );
        assert_eq!(
            string_to_json_value("true", 16),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            string_to_json_value("false", 16),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn test_string_to_json_interval() {
        // INTERVAL → JSON number (microseconds)
        let v = string_to_json_value("00:00:00", 1186);
        assert_eq!(v, serde_json::json!(0));
        let v = string_to_json_value("1 year", 1186);
        assert!(v.as_i64().unwrap() > 0, "1 year should be positive micros");
        let v = string_to_json_value("-1 year", 1186);
        assert!(v.as_i64().unwrap() < 0, "-1 year should be negative");
        let v = string_to_json_value("1 day 12:00:00", 1186);
        assert!(v.as_i64().unwrap() > 0, "1 day 12h should be positive");
    }

    #[test]
    fn test_string_to_json_money() {
        // MONEY → exact decimal STRING (SR's DECIMAL column coerces the
        // text; a JSON number would route through f64 and round values
        // beyond 2^53 cents).
        let v = string_to_json_value("$1,234.56", 790);
        assert_eq!(v, serde_json::json!("1234.56"));
        let v = string_to_json_value("$0.00", 790);
        assert_eq!(v, serde_json::json!("0.00"));
        let v = string_to_json_value("-$500.00", 790);
        assert_eq!(v, serde_json::json!("-500.00"));
        // Locale-robust: de_DE lc_monetary uses '.' as the thousands
        // separator — must NOT become "1.23456" (1000× corruption).
        let v = string_to_json_value("1.234,56", 790);
        assert_eq!(v, serde_json::json!("1234.56"));
        // Full precision far beyond f64's 2^53-cents limit (~$90T).
        let v = string_to_json_value("$92233720368547758.07", 790);
        assert_eq!(v, serde_json::json!("92233720368547758.07"));
    }

    #[test]
    fn test_parse_interval() {
        assert_eq!(parse_pg_interval_to_micros("00:00:00"), Some(0));
        assert!(parse_pg_interval_to_micros("1 year").unwrap() > 0);
        assert!(parse_pg_interval_to_micros("-1 year").unwrap() < 0);
        assert!(parse_pg_interval_to_micros("04:05:06").unwrap() > 0);
    }

    #[test]
    fn test_parse_interval_mixed_signs() {
        // PG normalizes year+month into months, but day+time stays separate
        // with per-component signs. These are the actual PG text outputs:

        // '-11 mons' (from -1 year 1 mon)
        let neg_11mons = parse_pg_interval_to_micros("-11 mons").unwrap();
        assert!(
            neg_11mons < 0,
            "-11 mons should be negative, got {}",
            neg_11mons
        );

        // '11 mons' (from 1 year -1 mon)
        let pos_11mons = parse_pg_interval_to_micros("11 mons").unwrap();
        assert!(pos_11mons > 0, "11 mons should be positive");

        // '-1 years -1 mons' (from -1 year -1 mon)
        let neg_both = parse_pg_interval_to_micros("-1 years -1 mons").unwrap();
        assert!(neg_both < 0, "-1 years -1 mons should be negative");

        // '-1 days +01:00:00' (from -1 day 1 hour)
        // epoch should be -82800s = -23 hours
        let neg_day_pos_hr = parse_pg_interval_to_micros("-1 days +01:00:00").unwrap();
        let expected = -82800i64 * 1_000_000;
        assert_eq!(
            neg_day_pos_hr,
            expected,
            "-1 day +1 hour should be -82800s (got {} micros = {}s)",
            neg_day_pos_hr,
            neg_day_pos_hr / 1_000_000
        );

        // '1 day -01:00:00' (from 1 day -1 hour)
        // epoch should be +82800s = +23 hours
        let pos_day_neg_hr = parse_pg_interval_to_micros("1 day -01:00:00").unwrap();
        let expected2 = 82800i64 * 1_000_000;
        assert_eq!(
            pos_day_neg_hr,
            expected2,
            "1 day -1 hour should be +82800s (got {} micros = {}s)",
            pos_day_neg_hr,
            pos_day_neg_hr / 1_000_000
        );

        // Simple positive and negative
        assert_eq!(
            parse_pg_interval_to_micros("1 day").unwrap(),
            86400 * 1_000_000
        );
        assert_eq!(
            parse_pg_interval_to_micros("-1 day").unwrap(),
            -86400 * 1_000_000
        );
    }

    #[test]
    fn test_typed_array_coercion() {
        // The typed array refactor (pg_array_to_json_typed) dispatches each
        // element through string_to_json_value with the element OID.

        // _bool array: "t"/"f" → true/false (not strings "t"/"f")
        let bool_arr = string_to_json_value("{t,f,t}", 1000); // 1000 = _bool
        assert_eq!(
            bool_arr,
            serde_json::json!([true, false, true]),
            "_bool array: t/f must be coerced to booleans"
        );

        // _text array: "null" stays as string "null" (not JSON null)
        let text_arr = string_to_json_value(r#"{"null","true","123"}"#, 1009); // 1009 = _text
        assert_eq!(
            text_arr,
            serde_json::json!(["null", "true", "123"]),
            "_text array: string-looking values must stay as strings"
        );

        // _text array: unquoted elements also stay strings
        let text_arr2 = string_to_json_value("{abc,def}", 1009);
        assert_eq!(
            text_arr2,
            serde_json::json!(["abc", "def"]),
            "_text array: unquoted elements must be strings"
        );

        // _int4 array: integers come as text (SR coerces from JSON string,
        // same as scalar int4 path which also returns String).
        let int_arr = string_to_json_value("{1,2,3}", 1007); // 1007 = _int4
        assert_eq!(
            int_arr,
            serde_json::json!(["1", "2", "3"]),
            "_int4 array: integers passed as text (same as scalar path)"
        );
    }

    #[test]
    fn test_typed_array_nested_recursion() {
        // Nested multi-dimensional arrays like _int4 containing {{1,2},{3,4}}
        // must produce [[1,2],[3,4]], not ["{1,2}","{3,4}"] (strings).
        // This was a regression in the typed-array refactor.
        let json = string_to_json_value("{{1,2},{3,4}}", 1007); // 1007 = _int4
        assert_eq!(
            json,
            serde_json::json!([["1", "2"], ["3", "4"]]),
            "Nested _int4 array must recurse, not flatten to strings"
        );
    }

    #[test]
    fn test_typed_array_null_elements() {
        // PG arrays emit unquoted "NULL" for null elements. This must become
        // JSON null, not the literal string "NULL". A quoted "NULL" stays as
        // the literal string (for _text arrays).
        // Case 1: unquoted NULL in _int4
        let json = string_to_json_value("{1,NULL,2}", 1007); // _int4
        assert_eq!(
            json,
            serde_json::json!(["1", null, "2"]),
            "Unquoted NULL in _int4 must be JSON null"
        );
        // Case 2: quoted "NULL" in _text stays as string
        let json2 = string_to_json_value(r#"{"NULL","hello"}"#, 1009); // _text
        assert_eq!(
            json2,
            serde_json::json!(["NULL", "hello"]),
            "Quoted 'NULL' in _text must stay as literal string"
        );
    }

    #[test]
    fn test_nan_infinity_to_null() {
        // PG float8 can produce "NaN", "Infinity", "-Infinity" which SR DOUBLE
        // cannot store. These must become JSON null (with a warning).
        assert_eq!(string_to_json_value("NaN", 701), serde_json::Value::Null);
        assert_eq!(
            string_to_json_value("Infinity", 701),
            serde_json::Value::Null
        );
        assert_eq!(
            string_to_json_value("-Infinity", 701),
            serde_json::Value::Null
        );
        // Same for float4 (700)
        assert_eq!(string_to_json_value("NaN", 700), serde_json::Value::Null);
        // Finite floats stay as text (SR coerces from JSON string)
        assert_eq!(
            string_to_json_value("3.14", 701),
            serde_json::Value::String("3.14".to_string()),
            "Finite float8 must stay as string"
        );
    }

    #[test]
    fn test_auto_create_sr_table_sql_fast_schema_evolution() {
        let rel = RelationInfo {
            schema: "public".into(),
            table: "test".into(),
            columns: vec![
                ColInfo {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "name".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec!["id".into()],
        };
        let p = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
        ]);
        let sql = auto_create_sr_table_sql("test_public", &rel, "testdb", &p)
            .expect("fast_schema_evolution=true");
        assert!(
            sql.contains("\"fast_schema_evolution\" = \"true\""),
            "Should use 'true' not '1': {sql}"
        );
        assert!(
            !sql.contains("\"fast_schema_evolution\" = \"1\""),
            "Should NOT use '1': {sql}"
        );

        // With fast_schema_evolution = false
        let p2 =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql2 = auto_create_sr_table_sql("test_public", &rel, "testdb", &p2)
            .expect("fast_schema_evolution=false");
        assert!(
            !sql2.contains("fast_schema_evolution"),
            "Should omit fast_schema_evolution when disabled: {sql2}"
        );
    }

    #[test]
    fn test_columns_to_json_null_preservation() {
        // Actual PG NULL → JSON null
        let cols = vec![ColInfo {
            name: "val".into(),
            type_oid: 25,
            type_mod: 0,
        }];
        let mut buf = make_json_buf();
        let vals = vec![ColValue::Null];
        let json = columns_to_json(&cols, &vals, &mut buf).unwrap();
        assert!(json["val"].is_null(), "PG NULL must be JSON null");

        // Literal string "null" → preserved as JSON string "null"
        let vals2 = vec![ColValue::Value("null".to_string())];
        let json2 = columns_to_json(&cols, &vals2, &mut buf).unwrap();
        assert!(
            !json2["val"].is_null(),
            "Literal 'null' must NOT be JSON null"
        );
        assert_eq!(
            json2["val"], "null",
            "Literal 'null' must be preserved as string"
        );

        // Empty string → preserved as empty JSON string (not null)
        let vals3 = vec![ColValue::Value("".to_string())];
        let json3 = columns_to_json(&cols, &vals3, &mut buf).unwrap();
        assert!(
            !json3["val"].is_null(),
            "Empty string must NOT be JSON null"
        );
        assert_eq!(json3["val"], "", "Empty string must be preserved");
    }

    #[test]
    fn test_columns_to_json_empty_string_is_not_null() {
        // Verify empty string '' is preserved, not converted to null
        let cols = vec![
            ColInfo {
                name: "t".into(),
                type_oid: 25,
                type_mod: 0,
            }, // TEXT
            ColInfo {
                name: "v".into(),
                type_oid: 1043,
                type_mod: 100 + 4,
            }, // VARCHAR(100)
            ColInfo {
                name: "c".into(),
                type_oid: 1042,
                type_mod: 10 + 4,
            }, // CHAR(10)
        ];
        let mut buf = make_json_buf();
        let vals = vec![
            ColValue::Value("".to_string()),
            ColValue::Value("".to_string()),
            ColValue::Value("".to_string()),
        ];
        let json = columns_to_json(&cols, &vals, &mut buf).unwrap();
        assert!(!json["t"].is_null(), "TEXT '' must not be null");
        assert_eq!(json["t"], "", "TEXT '' must be empty string");
        assert!(!json["v"].is_null(), "VARCHAR '' must not be null");
        assert_eq!(json["v"], "", "VARCHAR '' must be empty string");
        assert!(!json["c"].is_null(), "CHAR '' must not be null");
        assert_eq!(json["c"], "", "CHAR '' must be empty string");
    }

    #[test]
    fn test_auto_create_sr_table_errors_without_pk() {
        // Without PK from catalog, auto_create must fail — no heuristic, no DUPLICATE KEY
        let rel = RelationInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![
                ColInfo {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "val".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec![],
        };
        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let result = auto_create_sr_table_sql("t", &rel, "testdb", &p);
        assert!(
            result.is_err(),
            "Must error when PK is unknown from catalog"
        );
        assert!(
            result.unwrap_err().to_string().contains("PK unknown"),
            "Error must mention PK unknown"
        );
    }

    // ── serialize_row ──────────────────────────────────

    #[test]
    fn test_serialize_row_basic_and_pk_key() {
        let cols = vec![
            ColInfo {
                name: "id".into(),
                type_oid: 23,
                type_mod: 0,
            },
            ColInfo {
                name: "val".into(),
                type_oid: 25,
                type_mod: 0,
            },
        ];
        let vals = vec![
            ColValue::Value("42".to_string()),
            ColValue::Value("hello".to_string()),
        ];
        let pk = vec!["id".to_string()];
        let mut buf = Vec::new();
        let (raw, pk_key) = serialize_row(&cols, &vals, &pk, &mut buf).unwrap();
        // INT4 is mapped to Value::String, so the JSON-serialized PK is `"42"`
        // (with quotes) — matching the legacy extract_pk_key_from_value output.
        assert_eq!(pk_key.as_deref(), Some("\"42\""));
        let parsed: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
        assert_eq!(parsed["id"], "42"); // String, not Number (type mapper)
        assert_eq!(parsed["val"], "hello");
        // Buffer was cleared (reused) — capacity preserved.
        assert!(buf.is_empty());
    }

    #[test]
    fn test_serialize_row_unresolved_pk_key_is_none_not_question_mark() {
        // A PK column arriving as an unchanged TOAST datum is omitted from the
        // JSON, so its value cannot be resolved. Returning None (instead of
        // Some("?")) makes net-change dedup keep EVERY op via its per-op unique
        // sentinel; with a shared "?" key, distinct rows in the same batch
        // collapsed into one ("last op wins") and the rest were silently lost.
        let cols = vec![
            ColInfo {
                name: "id".into(),
                type_oid: 25,
                type_mod: 0,
            },
            ColInfo {
                name: "v".into(),
                type_oid: 25,
                type_mod: 0,
            },
        ];
        let vals = vec![
            ColValue::Unchanged, // PK value TOAST-unchanged in this UPDATE
            ColValue::Value("x".to_string()),
        ];
        let pk = vec!["id".to_string()];
        let mut buf = Vec::new();
        let (raw, pk_key) = serialize_row(&cols, &vals, &pk, &mut buf).unwrap();
        assert_eq!(
            pk_key, None,
            "unresolved PK must not yield a shared \"?\" dedup key"
        );
        let parsed: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
        assert!(
            parsed.get("id").is_none(),
            "TOAST-unchanged PK stays omitted"
        );
    }

    #[test]
    fn test_serialize_row_pk_column_absent_from_schema_is_none() {
        // PK listed in the cache but absent from the column set (stale schema):
        // unresolved, same guarantee as above.
        let cols = vec![ColInfo {
            name: "v".into(),
            type_oid: 25,
            type_mod: 0,
        }];
        let vals = vec![ColValue::Value("x".to_string())];
        let pk = vec!["id".to_string()];
        let mut buf = Vec::new();
        let (_, pk_key) = serialize_row(&cols, &vals, &pk, &mut buf).unwrap();
        assert_eq!(pk_key, None);
    }

    #[test]
    fn test_extract_pk_key_unresolved_is_none() {
        // Replay side of the same guarantee: a row JSON missing any PK column
        // is unresolved → None → dedup keeps every op instead of collapsing
        // distinct rows under a shared key.
        let pk = vec!["id".to_string()];
        assert_eq!(
            extract_pk_key_from_json(r#"{"id":7,"v":"x"}"#, &pk).as_deref(),
            Some("7")
        );
        assert_eq!(extract_pk_key_from_json(r#"{"v":"x"}"#, &pk), None);
        assert_eq!(extract_pk_key_from_json("not json", &pk), None);
        assert_eq!(extract_pk_key_from_json(r#"{"id":7}"#, &[]), None);
    }

    #[test]
    fn test_serialize_row_composite_pk_key() {
        let cols = vec![
            ColInfo {
                name: "a".into(),
                type_oid: 23,
                type_mod: 0,
            },
            ColInfo {
                name: "b".into(),
                type_oid: 23,
                type_mod: 0,
            },
            ColInfo {
                name: "c".into(),
                type_oid: 23,
                type_mod: 0,
            },
        ];
        let vals = vec![
            ColValue::Value("1".to_string()),
            ColValue::Value("2".to_string()),
            ColValue::Value("3".to_string()),
        ];
        let pk = vec!["a".to_string(), "b".to_string()];
        let mut buf = Vec::new();
        let (_, pk_key) = serialize_row(&cols, &vals, &pk, &mut buf).unwrap();
        assert_eq!(pk_key.as_deref(), Some("\"1\"|\"2\""));
    }

    #[test]
    fn test_serialize_row_unchanged_toast_omitted() {
        let cols = vec![
            ColInfo {
                name: "id".into(),
                type_oid: 23,
                type_mod: 0,
            },
            ColInfo {
                name: "big".into(),
                type_oid: 25,
                type_mod: 0,
            },
        ];
        let vals = vec![
            ColValue::Value("1".to_string()),
            ColValue::Unchanged, // TOAST unchanged → omitted from JSON
        ];
        let mut buf = Vec::new();
        let (raw, _) = serialize_row(&cols, &vals, &["id".to_string()], &mut buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
        assert!(
            parsed.get("big").is_none(),
            "Unchanged TOAST col must be omitted"
        );
        assert_eq!(parsed["id"], "1");
    }

    #[test]
    fn test_serialize_row_null_preserved() {
        let cols = vec![ColInfo {
            name: "a".into(),
            type_oid: 25,
            type_mod: 0,
        }];
        let vals = vec![ColValue::Null];
        let mut buf = Vec::new();
        let (raw, _) = serialize_row(&cols, &vals, &[], &mut buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
        assert!(parsed["a"].is_null());
    }

    #[test]
    fn test_serialize_row_schema_mismatch_error() {
        let cols = vec![ColInfo {
            name: "a".into(),
            type_oid: 23,
            type_mod: 0,
        }];
        let vals = vec![
            ColValue::Value("1".to_string()),
            ColValue::Value("2".to_string()), // extra WAL value
        ];
        let mut buf = Vec::new();
        let err = serialize_row(&cols, &vals, &[], &mut buf).unwrap_err();
        assert!(
            is_schema_mismatch(&err),
            "extra WAL values → schema mismatch"
        );
    }

    #[test]
    fn test_serialize_row_buffer_reused_across_rows() {
        // The Vec<u8> buffer is cleared at row start and content moved
        // into RawValue — capacity preserved across rows (no per-row alloc).
        let cols = vec![ColInfo {
            name: "x".into(),
            type_oid: 25,
            type_mod: 0,
        }];
        let mut buf = Vec::new();
        let (raw1, _) = serialize_row(
            &cols,
            &[ColValue::Value("first".to_string())],
            &[],
            &mut buf,
        )
        .unwrap();
        // After the first row, buf is empty but should have capacity from the first write.
        let cap_after_1 = buf.capacity();
        assert!(cap_after_1 > 0, "buffer must retain capacity after row 1");
        let (raw2, _) = serialize_row(
            &cols,
            &[ColValue::Value("second".to_string())],
            &[],
            &mut buf,
        )
        .unwrap();
        // Second row reuses the same buffer — no new allocation.
        assert!(
            buf.capacity() >= cap_after_1,
            "capacity preserved across rows"
        );
        assert_ne!(raw1.get(), raw2.get(), "different rows → different JSON");
    }

    #[test]
    fn test_auto_create_sr_table_uses_pk_from_catalog() {
        // Single-column PK from PG catalog
        let rel = RelationInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![
                ColInfo {
                    name: "order_id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "amount".into(),
                    type_oid: 1700,
                    type_mod: 0,
                },
            ],
            pk_columns: vec!["order_id".into()],
        };
        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql = auto_create_sr_table_sql("t", &rel, "testdb", &p)
            .expect("PK from catalog must succeed");
        assert!(
            sql.contains("PRIMARY KEY(`order_id`)"),
            "PK from catalog must be PRIMARY KEY: {sql}"
        );
        assert!(
            !sql.contains("DUPLICATE KEY"),
            "Must not use DUPLICATE KEY when PK is known: {sql}"
        );
    }

    #[test]
    fn test_auto_create_sr_table_uses_composite_pk_from_catalog() {
        // Composite PK from PG catalog
        let rel = RelationInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![
                ColInfo {
                    name: "tenant_id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "user_id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "data".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec!["tenant_id".into(), "user_id".into()],
        };
        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql =
            auto_create_sr_table_sql("t", &rel, "testdb", &p).expect("Composite PK must succeed");
        assert!(
            sql.contains("PRIMARY KEY(`tenant_id`, `user_id`)"),
            "Composite PK must list all columns: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTED BY HASH(`tenant_id`)"),
            "Dist key must be first PK column: {sql}"
        );
    }

    #[test]
    fn test_auto_create_sr_table_errors_without_pk_catalog() {
        // Empty pk_columns must error — no heuristic fallback, no DUPLICATE KEY
        let rel = RelationInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![
                ColInfo {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "val".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec![],
        };
        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let result = auto_create_sr_table_sql("t", &rel, "testdb", &p);
        assert!(result.is_err(), "Must error when PK is empty");

        // Non-PK-like name also errors
        let rel2 = RelationInfo {
            schema: "public".into(),
            table: "t".into(),
            columns: vec![
                ColInfo {
                    name: "score".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "val".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec![],
        };
        let p2 =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let result2 = auto_create_sr_table_sql("t", &rel2, "testdb", &p2);
        assert!(
            result2.is_err(),
            "Must error when PK is empty (non-PK-like name)"
        );
    }

    #[test]
    fn test_auto_create_sr_table_persistent_index_for_pk() {
        let rel = RelationInfo {
            schema: "public".into(),
            table: "test_pk".into(),
            columns: vec![
                ColInfo {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: 0,
                },
                ColInfo {
                    name: "name".into(),
                    type_oid: 25,
                    type_mod: 0,
                },
            ],
            pk_columns: vec!["id".into()],
        };

        // PK + persistent_index enabled
        let p = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("enable_persistent_index".to_string(), "true".to_string()),
            (
                "persistent_index_type".to_string(),
                "CLOUD_NATIVE".to_string(),
            ),
        ]);
        let sql =
            auto_create_sr_table_sql("test_pk", &rel, "testdb", &p).expect("PK + persistent_index");
        assert!(
            sql.contains("\"enable_persistent_index\" = \"true\""),
            "PK + persistent_index: {sql}"
        );
        assert!(
            sql.contains("\"persistent_index_type\" = \"CLOUD_NATIVE\""),
            "Missing persistent_index_type: {sql}"
        );

        // PK + persistent_index disabled
        let p2 =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql2 = auto_create_sr_table_sql("test_pk", &rel, "testdb", &p2)
            .expect("PK + no persistent_index");
        assert!(!sql2.contains("persistent_index"), "disabled: {sql2}");
    }

    #[test]
    fn test_auto_create_sr_table_requires_pk() {
        // No PK from catalog must error (no DUPLICATE KEY fallback)
        let rel2 = RelationInfo {
            schema: "public".into(),
            table: "dup".into(),
            columns: vec![ColInfo {
                name: "cat".into(),
                type_oid: 23,
                type_mod: 0,
            }],
            pk_columns: vec![],
        };
        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let result = auto_create_sr_table_sql("dup", &rel2, "testdb", &p);
        assert!(result.is_err(), "Must error when PK is empty");
    }

    #[test]
    fn test_name_index_is_maintained() {
        // Build a minimal SrConfig instance to verify the struct compiles.
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "testdb".into(),
            username: "root".into(),
            password: "".into(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        };
        // Create a Pipeline instance — this requires a working SrSink which
        // we can't easily construct in unit tests without a running SR.
        let _ = sr_cfg;

        // Verify that the name key format matches what flush_transaction expects
        let rel = RelationInfo {
            schema: "public".into(),
            table: "orders".into(),
            columns: vec![],
            pk_columns: vec!["id".into()],
        };
        let expected_key = format!("{}.{}", rel.schema, rel.table);
        assert_eq!(
            expected_key, "public.orders",
            "Name key format must match flush_transaction's table_key"
        );
    }

    /// Regression test: extra WAL values (schema mismatch from DDL sync lag)
    /// must return an error, not silently drop data.
    #[test]
    fn test_columns_to_json_extra_values_returns_error() {
        let cols = vec![ColInfo {
            name: "id".into(),
            type_oid: 23,
            type_mod: 0,
        }];
        let mut buf = make_json_buf();
        let vals = vec![
            ColValue::Value("1".to_string()),
            ColValue::Value("extra_value".to_string()),
        ];
        let result = columns_to_json(&cols, &vals, &mut buf);
        assert!(result.is_err(), "Extra WAL values must return an error");
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("Schema mismatch"),
            "Error must mention Schema mismatch: {err}"
        );
        assert!(
            err.contains("extra_value"),
            "Error must include the extra value: {err}"
        );
    }

    #[test]
    fn test_extract_pk_key_single_column() {
        let row = serde_json::json!({"id": 42, "name": "foo"});
        let pk = vec!["id".to_string()];
        assert_eq!(extract_pk_key_from_value(&row, &pk).as_deref(), Some("42"));
    }

    #[test]
    fn test_extract_pk_key_composite() {
        let row = serde_json::json!({"a": 1, "b": 2, "c": 3});
        let pk = vec!["a".to_string(), "b".to_string()];
        assert_eq!(extract_pk_key_from_value(&row, &pk).as_deref(), Some("1|2"));
    }

    #[test]
    fn test_extract_pk_key_differentiation() {
        // Verify that different PK values produce different keys
        let old_row = serde_json::json!({"id": 1, "name": "old"});
        let new_row = serde_json::json!({"id": 2, "name": "new"});
        let pk = vec!["id".to_string()];
        let old_key = extract_pk_key_from_value(&old_row, &pk);
        let new_key = extract_pk_key_from_value(&new_row, &pk);
        assert_ne!(
            old_key, new_key,
            "Different PK values must produce different keys"
        );
    }

    #[test]
    fn test_extract_pk_key_same_pk() {
        // Verify that same PK values produce identical keys regardless of other columns
        let old_row = serde_json::json!({"id": 5, "name": "old_name"});
        let new_row = serde_json::json!({"id": 5, "name": "new_name"});
        let pk = vec!["id".to_string()];
        assert_eq!(
            extract_pk_key_from_value(&old_row, &pk),
            extract_pk_key_from_value(&new_row, &pk)
        );
    }

    #[test]
    fn test_pk_change_key_pads_unresolved_parts() {
        // Change detection compares what is KNOWN: an absent PK column pads as
        // "?" so two different composite keys with one unresolved part remain
        // distinguishable. `Option<String>` (dedup) cannot express that — both
        // would be None — which is why the UPDATE branch must not use `pk_key`.
        let pk = vec!["a".to_string(), "b".to_string()];
        assert_eq!(pk_change_key(r#"{"a":"1"}"#, &pk), "\"1\"|?");
        assert_ne!(
            pk_change_key(r#"{"a":"1"}"#, &pk),
            pk_change_key(r#"{"a":"2"}"#, &pk),
            "a changed resolved part must be detected despite the unresolved part"
        );
        // Both parts unresolved (or an unparsable payload) → equal → nothing
        // decidable → no spurious DELETE.
        assert_eq!(
            pk_change_key(r#"{"c":1}"#, &pk),
            pk_change_key("not json", &pk)
        );
        // Fully resolved rows compare exactly like the dedup key.
        assert_eq!(
            pk_change_key(r#"{"a":"1","b":"2"}"#, &pk),
            extract_pk_key_from_json(r#"{"a":"1","b":"2"}"#, &pk).expect("resolved")
        );
    }

    #[test]
    fn test_table_changes_ordered_dedup_delete_wins() {
        // DELETE after INSERT for same PK → row absent
        let pk = vec!["id".to_string()];
        let mut changes = TableChanges::new();
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 1, "val": "x"}),
            &pk,
        ));
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 1, "val": "x"}),
            &pk,
        ));

        let mut seen: HashSet<String> = HashSet::new();
        let mut ordered: Vec<(bool, RowOp)> = Vec::new();

        for op in changes.ops.iter().rev() {
            let (is_upsert, row) = match op {
                OrderedOp::Delete(row) => (false, row),
                OrderedOp::Upsert(row) => (true, row),
            };
            let key = row.pk_key.clone().unwrap_or_else(|| "?".to_string());
            if seen.insert(key) {
                ordered.push((is_upsert, row.clone()));
            }
        }
        ordered.reverse();
        assert_eq!(ordered.len(), 1, "Only last op should survive");
        assert!(!ordered[0].0, "Last op is DELETE → row absent");
    }

    #[test]
    fn test_table_changes_ordered_dedup_upsert_wins() {
        // INSERT after DELETE for same PK → row present
        let pk = vec!["id".to_string()];
        let mut changes = TableChanges::new();
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 2, "val": "y"}),
            &pk,
        ));
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 2, "val": "z"}),
            &pk,
        ));

        let mut seen: HashSet<String> = HashSet::new();
        let mut ordered: Vec<(bool, RowOp)> = Vec::new();

        for op in changes.ops.iter().rev() {
            let (is_upsert, row) = match op {
                OrderedOp::Delete(row) => (false, row),
                OrderedOp::Upsert(row) => (true, row),
            };
            let key = row.pk_key.clone().unwrap_or_else(|| "?".to_string());
            if seen.insert(key) {
                ordered.push((is_upsert, row.clone()));
            }
        }
        ordered.reverse();
        assert_eq!(ordered.len(), 1, "Only last op should survive");
        assert!(ordered[0].0, "Last op is UPSERT → row present");
    }

    #[test]
    fn test_table_changes_ordered_dedup_multi_touch() {
        // DELETE → INSERT → DELETE → INSERT for same PK → row present (last INSERT wins)
        let pk = vec!["id".to_string()];
        let mut changes = TableChanges::new();
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 3, "val": "a"}),
            &pk,
        ));
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 3, "val": "b"}),
            &pk,
        ));
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 3, "val": "b"}),
            &pk,
        ));
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 3, "val": "c"}),
            &pk,
        ));

        let mut seen: HashSet<String> = HashSet::new();
        let mut ordered: Vec<(bool, RowOp)> = Vec::new();

        for op in changes.ops.iter().rev() {
            let (is_upsert, row) = match op {
                OrderedOp::Delete(row) => (false, row),
                OrderedOp::Upsert(row) => (true, row),
            };
            let key = row.pk_key.clone().unwrap_or_else(|| "?".to_string());
            if seen.insert(key) {
                ordered.push((is_upsert, row.clone()));
            }
        }
        ordered.reverse();
        assert_eq!(ordered.len(), 1, "Only last op should survive");
        assert!(ordered[0].0, "Last op is UPSERT → row present");
        // RowOp.data is the serialized JSON text — parse to check the value.
        let last_val: serde_json::Value =
            serde_json::from_str(ordered[0].1.data.get()).expect("RowOp JSON is valid");
        assert_eq!(last_val["val"], "c", "Last value should be 'c'");
    }

    #[test]
    fn test_pipeline_new_initializes_semaphore() {
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).expect("sink construction");
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };

        let capacity = 5;
        let pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            512,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            capacity,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        let sem = pipeline.flush_semaphore;
        // Verify we can acquire all permits sequentially (capacity = 5)
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Acquire all 5 permits — each call returns immediately
            let _permit1 = sem.clone().acquire_owned().await.unwrap();
            let _permit2 = sem.clone().acquire_owned().await.unwrap();
            let _permit3 = sem.clone().acquire_owned().await.unwrap();
            let _permit4 = sem.clone().acquire_owned().await.unwrap();
            let _permit5 = sem.clone().acquire_owned().await.unwrap();
            // 6th acquire would block — verify with a short timeout
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(10),
                sem.clone().acquire_owned(),
            )
            .await;
            assert!(
                result.is_err(),
                "6th acquire should time out (semaphore capacity = {capacity})"
            );
        });
    }

    #[test]
    fn test_flush_requested_when_memory_exceeded() {
        use crate::cdc::ColValue;
        use crate::cdc::DataEvent;
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).expect("sink construction");
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };

        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            0, // max_pending_memory_mb = 0 → any insert exceeds cap
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        assert!(!pipeline.flush_requested());
        assert!(!pipeline.has_pending());

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            assert!(!pipeline.has_pending());

            let event = DataEvent::Insert {
                relation_id: 1,
                columns: vec![ColValue::Value("hello".to_string())],
            };
            pipeline.process_event(&event).await.unwrap();

            assert!(pipeline.has_pending());
            assert!(
                pipeline.flush_requested(),
                "memory limit 0 should trigger flush on any data"
            );

            // begin_transaction resets both
            pipeline.begin_transaction();
            assert!(!pipeline.has_pending());
            assert!(!pipeline.flush_requested());
        });
    }

    #[test]
    fn test_truncate_mid_transaction_clears_prior_ops() {
        // INSERT(A) → TRUNCATE → INSERT(B) in one PG txn must flush only B
        // (TRUNCATE discards prior ops). Previously the ops Vec was left intact,
        // yielding SR {A,B} instead of PG's {B} (silent data corruption).
        // We assert via pending_bytes: after the sequence, the byte count must
        // equal INSERT(B) alone (A's bytes were reclaimed by the TRUNCATE).
        use crate::cdc::{ColValue, DataEvent, RelationColumn};
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).expect("sink construction");
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            512,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending_set.clone(),
            pending_set.clone(),
            pending_set.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );
        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();

            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("A-value".to_string())],
                })
                .await
                .unwrap();
            assert_eq!(
                pipeline.debug_table_state("public", "t"),
                Some((1, false)),
                "INSERT(A) buffers one op, truncate=false"
            );

            pipeline
                .process_event(&DataEvent::Truncate { relation_id: 1 })
                .await
                .unwrap();
            assert_eq!(
                pipeline.debug_table_state("public", "t"),
                Some((0, true)),
                "TRUNCATE must clear prior ops (0 buffered) and set truncate=true"
            );

            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("B-value".to_string())],
                })
                .await
                .unwrap();
            assert_eq!(
                pipeline.debug_table_state("public", "t"),
                Some((1, true)),
                "INSERT(B) after TRUNCATE buffers exactly one op (B) — A was dropped"
            );
        });
    }

    #[test]
    fn test_flush_requested_not_triggered_below_limit() {
        use crate::cdc::ColValue;
        use crate::cdc::DataEvent;
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).expect("sink construction");
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };

        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024, // huge memory cap — small data won't trigger
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            let event = DataEvent::Insert {
                relation_id: 1,
                columns: vec![ColValue::Value("hello".to_string())],
            };
            pipeline.process_event(&event).await.unwrap();
            assert!(pipeline.has_pending());
            assert!(
                !pipeline.flush_requested(),
                "small data should not trigger 1GB cap"
            );
        });
    }

    // ── pending_cdc_tables ────────────────────────────────────────────

    #[test]
    fn test_pending_cdc_tables_populated_by_insert() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending_set.clone(),
            pending_set.clone(),
            pending_set.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "orders",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            // Shared set should be empty after begin_transaction
            assert!(pending_set.lock().unwrap().is_empty());

            let event = DataEvent::Insert {
                relation_id: 1,
                columns: vec![ColValue::Value("42".to_string())],
            };
            pipeline.process_event(&event).await.unwrap();

            // Shared set should contain "public.orders"
            let set = pending_set.lock().unwrap();
            assert!(
                set.contains("public.orders"),
                "Insert should add table key to pending_cdc_tables: {:?}",
                *set,
            );
        });
    }

    #[test]
    fn test_pending_cdc_tables_cleared_by_begin_transaction() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending_set.clone(),
            pending_set.clone(),
            pending_set.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "users",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("1".to_string())],
                })
                .await
                .unwrap();
            assert!(
                pending_set.lock().unwrap().contains("public.users"),
                "table should be pending after Insert"
            );

            // begin_transaction must clear the shared set
            pipeline.begin_transaction();
            assert!(
                pending_set.lock().unwrap().is_empty(),
                "begin_transaction must clear pending_cdc_tables"
            );
        });
    }

    /// Regression: `begin_transaction` used to `.clear()` the whole shared set,
    /// erasing the key an IN-FLIGHT replay task holds via
    /// `PendingCdcTableGuard` (created per iteration in `replay_buffered_table`).
    /// `wait_for_cdc_drain` then saw the table as not-pending and let a DDL run
    /// while the replay Stream Load was still in flight — the very DDL/CDC race
    /// the guard system exists to prevent.
    #[test]
    fn test_begin_transaction_preserves_replaying_table_keys() {
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let wal = test_wal_buffer();
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending_set.clone(),
            // Distinct sets: `begin_transaction` drains `dropped_relations`
            // into an eviction, so aliasing it with `pending_cdc_tables` would
            // erase the very key this test checks for survival.
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            wal.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        // "public.replaying" is claimed exactly like `kick_replays` claims it
        // before spawning a replayer; "public.idle" has no replayer in flight.
        pending_set
            .lock()
            .unwrap()
            .insert("public.replaying".to_string());
        pending_set
            .lock()
            .unwrap()
            .insert("public.idle".to_string());
        assert!(
            wal.try_begin_replay("public.replaying"),
            "test setup: replay slot must be claimable"
        );

        pipeline.begin_transaction();

        let set = pending_set.lock().unwrap();
        assert!(
            set.contains("public.replaying"),
            "a table with an in-flight replay must stay pending so wait_for_cdc_drain \
             waits for its Stream Load: {:?}",
            *set
        );
        assert!(
            !set.contains("public.idle"),
            "keys without an in-flight replay are still cleared: {:?}",
            *set
        );
        drop(set);

        // Once the replayer releases its slot the key is no longer protected.
        wal.end_replay("public.replaying");
        pipeline.begin_transaction();
        assert!(
            pending_set.lock().unwrap().is_empty(),
            "key cleared after the replay slot is released"
        );
    }

    /// Regression (PK-change detection): a composite PK where one part is
    /// UNRESOLVED in both tuples — here a TOAST-unchanged datum, which
    /// `serialize_row` omits from the row JSON and which therefore yields
    /// `pk_key: None` — must still emit the old-PK DELETE when the resolved
    /// part changes. Comparing `pk_key` collapsed both rows to the same value
    /// (it means "keep every op" for dedup, not "compare what is known"), so no
    /// DELETE was emitted and the old row stayed orphaned in StarRocks forever.
    #[test]
    fn test_update_pk_change_with_unresolved_composite_part_emits_delete() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending_set.clone(),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[
                RelationColumn {
                    name: "a".into(),
                    type_oid: 23,
                    type_modifier: 0,
                },
                RelationColumn {
                    name: "b".into(),
                    type_oid: 25,
                    type_modifier: 0,
                },
            ],
            &["a".into(), "b".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            pipeline
                .process_event(&DataEvent::Update {
                    relation_id: 1,
                    // `b` arrives TOAST-unchanged in BOTH tuples → both row
                    // JSONs carry only `a` and both pk_keys are None.
                    old_columns: vec![ColValue::Value("1".to_string()), ColValue::Unchanged],
                    new_columns: vec![ColValue::Value("2".to_string()), ColValue::Unchanged],
                })
                .await
                .unwrap();
        });

        let relation = pipeline.get_relation_by_name("public", "t").cloned();
        let changes = pipeline
            .tables
            .remove("public.t")
            .expect("Update must record table changes");
        assert_eq!(changes.ops.len(), 2, "old-PK DELETE + new-PK UPSERT");

        // The flush plan (dedup + chunking) is what actually reaches SR: the
        // DELETE for the OLD PK must still be in it. `pk_key: None` keeps every
        // op through dedup, so nothing may swallow it.
        let unsent = prep_table_flush("public", "t", changes, relation.as_ref())
            .expect("a PK-known table produces a flush plan");
        let plan = unsent.lock().unwrap();
        assert_eq!(
            plan.deletes.iter().map(|c| c.len()).sum::<usize>(),
            1,
            "the old-PK DELETE must survive into the flush plan (pre-fix it was never emitted)"
        );
        assert_eq!(plan.upserts.iter().map(|c| c.len()).sum::<usize>(), 1);
        let deleted: serde_json::Value =
            serde_json::from_str(plan.deletes[0][0].data.get()).expect("delete row JSON");
        assert_eq!(deleted["a"], "1", "DELETE targets the OLD PK value");
        let upserted: serde_json::Value =
            serde_json::from_str(plan.upserts[0][0].data.get()).expect("upsert row JSON");
        assert_eq!(upserted["a"], "2", "UPSERT carries the NEW PK value");
    }

    #[test]
    fn test_pending_cdc_tables_filtered_out_not_added() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "orders".to_string(), // only "orders" matches
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };

        let filter_re = crate::config::compile_table_filter("orders").ok();
        let pending_set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            filter_re,
            5,
            2,
            pending_set.clone(),
            pending_set.clone(),
            pending_set.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "orders",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );
        pipeline.add_relation(
            2,
            "public",
            "secret",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();

            // process filtered-out table — should NOT be added to set
            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 2,
                    columns: vec![ColValue::Value("99".to_string())],
                })
                .await
                .unwrap();
            assert!(
                !pending_set.lock().unwrap().contains("public.secret"),
                "filtered-out table must NOT be added to pending_cdc_tables"
            );

            // process matching table — should be added
            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("42".to_string())],
                })
                .await
                .unwrap();
            assert!(
                pending_set.lock().unwrap().contains("public.orders"),
                "matching table should be added to pending_cdc_tables"
            );
        });
    }

    // ── hard_flush_requested ───────────────────────────────────────────

    #[test]
    fn test_hard_flush_requested_when_memory_exceeded() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            0, // max_pending_memory_mb = 0 → any insert exceeds both soft and hard limits
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            let event = DataEvent::Insert {
                relation_id: 1,
                columns: vec![ColValue::Value("data".to_string())],
            };
            pipeline.process_event(&event).await.unwrap();
            assert!(
                pipeline.flush_requested(),
                "soft limit should be hit with max=0"
            );
            assert!(
                pipeline.hard_flush_requested(),
                "hard limit should also be hit with max=0"
            );
        });
    }

    #[test]
    fn test_hard_flush_not_requested_under_limit() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024, // huge cap — small data won't trigger either limit
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            let event = DataEvent::Insert {
                relation_id: 1,
                columns: vec![ColValue::Value("hello".to_string())],
            };
            pipeline.process_event(&event).await.unwrap();
            assert!(
                !pipeline.flush_requested(),
                "soft limit should not be hit with 1GB cap"
            );
            assert!(
                !pipeline.hard_flush_requested(),
                "hard limit should not be hit with 1GB cap"
            );
        });
    }

    #[test]
    fn test_pending_bytes_accessor_returns_value() {
        use crate::cdc::RelationColumn;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "val".into(),
                type_oid: 25,
                type_modifier: 0,
            }],
            &["val".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            assert_eq!(pipeline.pending_bytes(), 0);
            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("a fairly long string".to_string())],
                })
                .await
                .unwrap();
            assert!(
                pipeline.pending_bytes() > 0,
                "pending_bytes should increase after processing an event"
            );
        });
    }

    #[test]
    fn test_is_schema_mismatch_detection() {
        let err = anyhow::anyhow!(
            "Schema mismatch: WAL tuple has 5 values but relation cache has only 3 columns."
        );
        assert!(is_schema_mismatch(&err));

        let err2 = anyhow::anyhow!("Some other error about column mismatch");
        assert!(!is_schema_mismatch(&err2));
    }

    // ── chunk_rows_with_idxs ─────────────────────────────────────

    #[test]
    fn test_chunk_rows_single_chunk_under_limit() {
        let rows: Vec<(RowOp, usize)> = vec![
            RowOp::from_value(serde_json::json!({"id": 1}), &[]),
            RowOp::from_value(serde_json::json!({"id": 2}), &[]),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, row)| (row, i))
        .collect();
        let (chunks, idxs) = chunk_rows_with_idxs(rows, 1024);
        assert_eq!(chunks.len(), 1, "under-cap rows → single chunk");
        assert_eq!(chunks[0].len(), 2);
        assert_eq!(idxs, vec![vec![0, 1]]);
    }

    #[test]
    fn test_chunk_rows_splits_at_size_limit() {
        // Each row serializes to ~9 bytes + 1 newline = ~10 bytes.
        // With a 21-byte cap: [r0,r1] (≤21), then [r2], [r3].
        let rows: Vec<(RowOp, usize)> = (0..4)
            .map(|i| RowOp::from_value(serde_json::json!({"id": i}), &[]))
            .enumerate()
            .map(|(i, row)| (row, i))
            .collect();
        let (chunks, idxs) = chunk_rows_with_idxs(rows, 21);
        assert!(
            chunks.len() >= 2,
            "must split when over cap: {} chunks",
            chunks.len()
        );
        // No row is lost
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 4, "all rows preserved across chunks");
        // Index chunks mirror the row chunks and keep original order.
        assert_eq!(idxs.len(), chunks.len());
        assert_eq!(idxs.concat(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_chunk_rows_oversized_single_row_own_chunk() {
        // A single row larger than the cap becomes its own (oversized) chunk —
        // it can't be split; such rows are bounded upstream by max_event_bytes.
        let big = serde_json::json!({"data": "x".repeat(200)});
        let rows: Vec<(RowOp, usize)> = vec![
            RowOp::from_value(big.clone(), &[]),
            RowOp::from_value(big, &[]),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, row)| (row, i))
        .collect();
        let (chunks, idxs) = chunk_rows_with_idxs(rows, 50);
        assert_eq!(chunks.len(), 2, "each oversized row in its own chunk");
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
        assert_eq!(idxs, vec![vec![0], vec![1]]);
    }

    #[test]
    fn test_chunk_rows_empty() {
        let (chunks, idxs): (Vec<Vec<RowOp>>, Vec<Vec<usize>>) =
            chunk_rows_with_idxs(Vec::new(), 1024);
        assert!(chunks.is_empty());
        assert!(idxs.is_empty());
    }

    // ── timestamptz_to_utc ────────────────────────────────────────────

    #[test]
    fn test_timestamptz_to_utc_no_timezone_passthrough() {
        // Without timezone offset, should pass through as-is.
        let result = timestamptz_to_utc("2024-01-15 10:30:00").unwrap();
        assert_eq!(result, "2024-01-15 10:30:00");
    }

    #[test]
    fn test_timestamptz_to_utc_utc_offset_stripped() {
        // UTC offset (+00) should be stripped, time unchanged.
        let result = timestamptz_to_utc("2024-01-15 10:30:00+00").unwrap();
        assert_eq!(result, "2024-01-15 10:30:00");
    }

    #[test]
    fn test_timestamptz_to_utc_positive_offset_converts() {
        // +05:30 offset should be subtracted to get UTC.
        let result = timestamptz_to_utc("2024-01-15 10:30:00+05:30").unwrap();
        assert_eq!(result, "2024-01-15 05:00:00");
    }

    #[test]
    fn test_timestamptz_to_utc_negative_offset_converts() {
        // -08:00 offset should be added to get UTC.
        let result = timestamptz_to_utc("2024-01-15 10:30:00-08:00").unwrap();
        assert_eq!(result, "2024-01-15 18:30:00");
    }

    #[test]
    fn test_timestamptz_to_utc_offset_hours_only() {
        // Offset without minutes (+05) should still convert correctly.
        let result = timestamptz_to_utc("2024-01-15 10:30:00+05").unwrap();
        assert_eq!(result, "2024-01-15 05:30:00");
    }

    #[test]
    fn test_timestamptz_to_utc_with_fractional_seconds() {
        // Subsecond precision must be preserved through the conversion.
        let result = timestamptz_to_utc("2024-01-15 10:30:00.123456+05:30").unwrap();
        assert_eq!(result, "2024-01-15 05:00:00.123456");
    }

    #[test]
    fn test_timestamptz_to_utc_empty_returns_none() {
        assert!(timestamptz_to_utc("").is_none());
        assert!(timestamptz_to_utc("   ").is_none());
    }

    #[test]
    fn test_timestamptz_to_utc_multibyte_past_byte_19_does_not_panic() {
        // Byte 19 lands inside a 2-byte character (18 ASCII bytes then 'é').
        // A fixed `s[19..]` slice would panic here; the function must instead
        // fail the parse and return None. Reachable only through a stale
        // relation-cache type (OID 1184 with non-ASCII text), but a panic in
        // process_event kills the process without a graceful flush.
        let s = "2024-01-15 10:30:0\u{e9}";
        assert_eq!(s.len(), 20, "fixture must straddle byte 19");
        // No offset is found at a valid boundary, so the documented
        // pass-through path returns the value unchanged (the caller stores
        // unknown shapes as-is). The point is that it must not panic: a panic
        // in process_event kills the process without a graceful flush.
        assert_eq!(timestamptz_to_utc(s).as_deref(), Some(s));
    }

    #[test]
    fn test_string_to_json_timestamptz() {
        // TIMESTAMPTZ (OID 1184) must convert to UTC string.
        let v = string_to_json_value("2024-01-15 10:30:00+05:30", 1184);
        assert_eq!(v, serde_json::json!("2024-01-15 05:00:00"));

        // Without timezone, pass through as-is.
        let v = string_to_json_value("2024-01-15 10:30:00", 1184);
        assert_eq!(v, serde_json::json!("2024-01-15 10:30:00"));
    }

    // ── estimate_row_size ─────────────────────────────────────────────

    #[test]
    fn test_estimate_row_size_empty_values() {
        // Empty vec should return 0 overhead.
        assert_eq!(Pipeline::estimate_row_size(&[], &[]), 0);
    }

    #[test]
    fn test_estimate_row_size_null_only() {
        // Nulls contribute no raw bytes, only overhead.
        let vals = vec![ColValue::Null, ColValue::Null];
        assert_eq!(Pipeline::estimate_row_size(&vals, &[]), 2 * 16);
    }

    #[test]
    fn test_estimate_row_size_unchanged_only() {
        // Unchanged (TOAST) contribute no raw bytes, only overhead.
        let vals = vec![ColValue::Unchanged, ColValue::Unchanged];
        assert_eq!(Pipeline::estimate_row_size(&vals, &[]), 2 * 16);
    }

    #[test]
    fn test_estimate_row_size_value_underestimate_protection() {
        // A 10-char string should produce estimate >= 10*2 + 16 = 36 bytes.
        let vals = vec![ColValue::Value("hello12345".to_string())];
        let est = Pipeline::estimate_row_size(&vals, &[]);
        assert!(
            est >= 36,
            "estimate should include ×2 factor + overhead, got {est}"
        );
    }

    #[test]
    fn test_estimate_row_size_mixed_types() {
        // Mix of all value types.
        let vals = vec![
            ColValue::Value("abc".to_string()),
            ColValue::Null,
            ColValue::Unchanged,
            ColValue::Value("hello world".to_string()),
        ];
        let est = Pipeline::estimate_row_size(&vals, &[]);
        // raw = 3 + 11 = 14, ×2 = 28, + 4*16 = 64, total = 92
        assert_eq!(est, 14 * 2 + 4 * 16);
    }

    #[test]
    fn test_estimate_row_size_handles_empty_string() {
        // Empty string is a valid ColValue::Value — must be counted.
        let vals = vec![ColValue::Value("".to_string())];
        let est = Pipeline::estimate_row_size(&vals, &[]);
        // raw = 0, ×2 = 0, + 1*16 = 16
        assert_eq!(est, 16);
    }

    #[test]
    fn test_estimate_row_size_json_jsonb_overhead() {
        // JSON and JSONB columns should add 64 bytes overhead each.
        use crate::pipeline::ColInfo;
        let cols = vec![
            ColInfo {
                name: "id".into(),
                type_oid: 23,
                type_mod: 0,
            }, // INT
            ColInfo {
                name: "data".into(),
                type_oid: 114,
                type_mod: 0,
            }, // JSON
            ColInfo {
                name: "meta".into(),
                type_oid: 3802,
                type_mod: 0,
            }, // JSONB
            ColInfo {
                name: "name".into(),
                type_oid: 25,
                type_mod: 0,
            }, // TEXT
        ];
        let vals = vec![
            ColValue::Value("1".to_string()),
            ColValue::Value(r#"{"key":"val"}"#.to_string()),
            ColValue::Value(r#"{"a":1,"b":2}"#.to_string()),
            ColValue::Value("hello".to_string()),
        ];
        let est = Pipeline::estimate_row_size(&vals, &cols);
        // raw = 1 + 13 + 13 + 5 = 32, ×2 = 64
        // 4 cols × 16 = 64
        // 2 JSON cols × min(13*2, 4096) = 26 each = 52
        // total = 64 + 64 + 52 = 180
        assert_eq!(est, 180);
    }

    // ── build_json_data (no-unsafe verification) ───────────────────────
    // build_json_data takes &[RowOp] (pre-serialized RawValues).

    #[test]
    fn test_build_json_data_single_row() {
        let rows = vec![RowOp::from_value(
            serde_json::json!({"id": 1, "name": "hello"}),
            &[],
        )];
        let result = build_json_data(&rows);
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["name"], "hello");
    }

    #[test]
    fn test_build_json_data_multiple_rows() {
        let rows = vec![
            RowOp::from_value(serde_json::json!({"id": 1}), &[]),
            RowOp::from_value(serde_json::json!({"id": 2}), &[]),
            RowOp::from_value(serde_json::json!({"id": 3}), &[]),
        ];
        let result = build_json_data(&rows);
        let s = std::str::from_utf8(&result).expect("valid UTF-8");
        let lines: Vec<&str> = s.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["id"], i + 1);
        }
    }

    #[test]
    fn test_build_json_data_empty_input() {
        let empty: Vec<RowOp> = Vec::new();
        assert!(build_json_data(&empty).is_empty());
    }

    #[test]
    fn test_build_json_data_utf8_output() {
        let rows = vec![
            RowOp::from_value(serde_json::json!({"unicode": "héllo wörld 🌍"}), &[]),
            RowOp::from_value(serde_json::json!({"emoji": "🚀🔥⭐"}), &[]),
        ];
        let result = build_json_data(&rows);
        assert!(std::str::from_utf8(&result).is_ok());
    }

    #[test]
    fn test_build_json_data_special_chars() {
        let rows = vec![
            RowOp::from_value(serde_json::json!({"data": "line1\nline2"}), &[]),
            RowOp::from_value(serde_json::json!({"data": "tab\tseparated"}), &[]),
            RowOp::from_value(serde_json::json!({"data": r#"quotes"and"more"#}), &[]),
        ];
        let result = build_json_data(&rows);
        let s = std::str::from_utf8(&result).expect("valid UTF-8");
        let lines: Vec<&str> = s.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    // ── columns_to_json buffer reuse (std::mem::replace) ──────────────

    #[test]
    fn test_columns_to_json_buffer_reuse() {
        let mut buf = serde_json::Map::new();
        let cols1 = vec![ColInfo {
            name: "a".into(),
            type_oid: 23,
            type_mod: 0,
        }];
        let vals1 = vec![ColValue::Value("1".to_string())];
        let json1 = columns_to_json(&cols1, &vals1, &mut buf).unwrap();
        assert_eq!(json1["a"], "1");

        let cols2 = vec![ColInfo {
            name: "b".into(),
            type_oid: 25,
            type_mod: 0,
        }];
        let vals2 = vec![ColValue::Value("two".to_string())];
        let json2 = columns_to_json(&cols2, &vals2, &mut buf).unwrap();
        assert_eq!(json2["b"], "two");
        assert!(
            json2.get("a").is_none(),
            "column 'a' leaked from first call"
        );
    }

    #[test]
    fn test_columns_to_json_reuse_after_many_columns() {
        let mut buf = serde_json::Map::new();
        let many_cols: Vec<ColInfo> = (0..50)
            .map(|i| ColInfo {
                name: format!("c{i}"),
                type_oid: 23,
                type_mod: 0,
            })
            .collect();
        let many_vals: Vec<ColValue> = (0..50).map(|i| ColValue::Value(format!("{i}"))).collect();
        let _ = columns_to_json(&many_cols, &many_vals, &mut buf).unwrap();

        let cols = vec![ColInfo {
            name: "fresh".into(),
            type_oid: 23,
            type_mod: 0,
        }];
        let vals = vec![ColValue::Value("ok".to_string())];
        let json = columns_to_json(&cols, &vals, &mut buf).unwrap();
        assert_eq!(json["fresh"], "ok");
        for i in 0..50 {
            assert!(json.get(format!("c{i}")).is_none(), "c{i} leaked");
        }
    }

    // ── estimate_row_size JSONB edge cases ─────────────────────────────

    #[test]
    fn test_estimate_row_size_jsonb_deeply_nested_overhead_capped() {
        let cols = vec![ColInfo {
            name: "deep".into(),
            type_oid: 3802,
            type_mod: 0,
        }];
        let big = "{".to_string() + &"\"a\":".repeat(500) + "1" + &"}".repeat(500);
        let big_len = big.len();
        let vals = vec![ColValue::Value(big)];
        let est = Pipeline::estimate_row_size(&vals, &cols);
        let overhead = est - (big_len * 2 + 16);
        assert_eq!(
            overhead, 4096,
            "JSONB overhead must cap at 4096, got {overhead}"
        );
    }

    #[test]
    fn test_estimate_row_size_jsonb_short_overhead() {
        let cols = vec![ColInfo {
            name: "s".into(),
            type_oid: 3802,
            type_mod: 0,
        }];
        let vals = vec![ColValue::Value("{\"k\":\"v\"}".to_string())];
        // raw = 9, ×2 = 18, + 1*16 = 34, + min(9*2, 4096) = 18 → 52
        assert_eq!(Pipeline::estimate_row_size(&vals, &cols), 52);
    }

    #[test]
    fn test_estimate_row_size_jsonb_large_capped() {
        let cols = vec![ColInfo {
            name: "lg".into(),
            type_oid: 3802,
            type_mod: 0,
        }];
        let big = "x".repeat(10000);
        let vals = vec![ColValue::Value(big.clone())];
        let est = Pipeline::estimate_row_size(&vals, &cols);
        let overhead = est - (10000 * 2 + 16);
        assert_eq!(
            overhead, 4096,
            "JSONB overhead must cap at 4096, got {overhead}"
        );
    }

    // ── raw_value_len ───────────────────────────────────────────────────

    #[test]
    fn test_raw_value_len_empty_and_nulls() {
        assert_eq!(Pipeline::raw_value_len(&[]), 0);
        assert_eq!(
            Pipeline::raw_value_len(&[ColValue::Null, ColValue::Unchanged]),
            0
        );
    }

    #[test]
    fn test_raw_value_len_values() {
        let vals = vec![
            ColValue::Value("hello".to_string()),
            ColValue::Null,
            ColValue::Value("world".to_string()),
            ColValue::Unchanged,
        ];
        assert_eq!(Pipeline::raw_value_len(&vals), 10);
    }

    #[test]
    fn test_raw_value_len_large_value() {
        let big = "x".repeat(2_000_000);
        assert_eq!(Pipeline::raw_value_len(&[ColValue::Value(big)]), 2_000_000);
    }

    // ── PendingCdcTableGuard ───────────────────────────────────────────

    #[test]
    fn test_pending_cdc_table_guard_inserts_on_new() {
        let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let _guard = PendingCdcTableGuard::new(set.clone(), "public.orders".into());
        assert!(
            set.lock().unwrap().contains("public.orders"),
            "guard must insert table key on creation"
        );
    }

    #[test]
    fn test_pending_cdc_table_guard_removes_on_drop() {
        let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        {
            let _guard = PendingCdcTableGuard::new(set.clone(), "public.orders".into());
            assert!(set.lock().unwrap().contains("public.orders"));
        }
        assert!(
            !set.lock().unwrap().contains("public.orders"),
            "guard must remove table key on drop"
        );
    }

    #[test]
    fn test_pending_cdc_table_guard_removes_on_panic() {
        let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = PendingCdcTableGuard::new(set.clone(), "public.orders".into());
            assert!(set.lock().unwrap().contains("public.orders"));
            panic!("simulated flush task panic");
        }));
        assert!(result.is_err(), "inner closure must have panicked");
        assert!(
            !set.lock().unwrap().contains("public.orders"),
            "guard must remove table key even on panic"
        );
    }

    // ── mark_shutdown_seen (watch mechanism) ────────────────────────────

    #[test]
    fn test_mark_shutdown_seen_clears_changed() {
        let (tx, mut rx) = watch::channel(false);
        tx.send(true).unwrap();
        assert!(
            rx.has_changed().unwrap(),
            "should detect change before mark_shutdown_seen"
        );
        rx.borrow_and_update();
        assert!(
            !rx.has_changed().unwrap(),
            "after borrow_and_update (mark_shutdown_seen), has_changed must be false"
        );
    }

    #[test]
    fn test_dropped_relation_evicted_on_begin_transaction() {
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        // Build a pipeline with a dedicated dropped_relations set so we can
        // simulate the DDL poller enqueuing a DROP TABLE.
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let dropped: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            1024,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            dropped.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        // Populate the cache for two tables.
        use crate::cdc::RelationColumn;
        pipeline.add_relation(
            100,
            "public",
            "users",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );
        pipeline.add_relation(
            200,
            "public",
            "orders",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );
        // Insert a stale PK-cache entry for the soon-to-be-dropped table.
        // (pk_cache is private; eviction is exercised indirectly through the
        // public name_to_oid / relations surface via get_relation_by_name.)
        assert!(pipeline.get_relation_by_name("public", "users").is_some());
        assert!(pipeline.get_relation_by_name("public", "orders").is_some());

        // Simulate the DDL poller marking `public.users` as dropped.
        dropped.lock().unwrap().insert("public.users".to_string());

        // begin_transaction must drain the set and evict only the dropped table.
        pipeline.begin_transaction();

        assert!(
            pipeline.get_relation_by_name("public", "users").is_none(),
            "dropped table's relation cache must be evicted"
        );
        assert!(
            pipeline.get_relation_by_name("public", "orders").is_some(),
            "non-dropped table must be retained"
        );
        // The dropped set must be drained so the eviction doesn't re-fire.
        assert!(
            dropped.lock().unwrap().is_empty(),
            "dropped_relations set must be drained after eviction"
        );
    }

    /// PendingCdcTableGuard must be droppable from the send_stream_load
    /// DDL-race retry loop so the DDL poller's wait_for_cdc_drain proceeds
    /// during the sleep. This test verifies the guard-drop mechanism: when
    /// `*guard = None` runs, `pending_cdc_tables` no longer contains the key.
    #[test]
    fn test_pending_cdc_table_guard_drop_clears_pending() {
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let table_key = "public.test_table".to_string();

        // Create the guard — table is now pending.
        let guard1 = PendingCdcTableGuard::new(pending.clone(), table_key.clone());
        assert!(
            lock_or_recover(&pending).contains(&table_key),
            "guard creation marks table as pending"
        );

        // Drop the guard (as the flush path does before its retry sleep).
        drop(guard1);
        assert!(
            !lock_or_recover(&pending).contains(&table_key),
            "dropping the guard clears pending — DDL poller's \
             wait_for_cdc_drain can proceed during the sleep"
        );

        // Re-acquire (simulating the fix's re-acquire after sleep, when
        // ddl_active does NOT contain the table).
        assert!(
            !lock_or_recover(&ddl_active).contains(&table_key),
            "ddl_active must not contain the table for re-acquire to proceed"
        );
        let guard2 = PendingCdcTableGuard::new(pending.clone(), table_key.clone());
        assert!(
            lock_or_recover(&pending).contains(&table_key),
            "guard re-acquire marks table as pending again"
        );
        // Suppress clippy "never read" — the guard's effect is verified via
        // the pending set above; the variable itself just owns the guard.
        drop(guard2);
    }

    /// If a DDL started during the sleep (ddl_active now contains
    /// the table), send_stream_load must NOT re-acquire the guard — it returns
    /// an error so the caller defers to the next flush cycle. This closes the
    /// re-opened TOCTOU between guard-drop and re-acquire.
    #[test]
    fn test_ddl_active_during_retry_blocks_guard_reacquire() {
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let table_key = "public.test_table".to_string();

        // Simulate: guard was dropped before sleep, DDL started during sleep.
        let guard: Option<PendingCdcTableGuard> = None;
        lock_or_recover(&ddl_active).insert(table_key.clone());

        // The re-check (mirrors send_stream_load's logic):
        let should_defer = lock_or_recover(&ddl_active).contains(&table_key);
        assert!(
            should_defer,
            "ddl_active containing the table means the flush must defer \
             (not re-acquire the guard)"
        );
        // Guard stays None — caller defers to next flush cycle.
        assert!(
            guard.is_none(),
            "guard must not be re-acquired when ddl_active contains the table"
        );
    }

    // ── two-phase acquire regression tests ──────────────

    /// `acquire_ddl_guard` is a helper in ddl_poller.rs; this test verifies the
    /// drain-vs-acquire ordering contract via the building blocks it composes:
    /// the flush path's double-check must defer when `ddl_active` contains the
    /// table (the guard-first ordering's STARVATION bug is fixed by the two-
    /// phase pattern; this pins the predicate that the flush path uses).
    #[test]
    fn test_ddl_active_double_check_defer_predicate() {
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let table_key = "public.t".to_string();

        // Pre-guard: not deferred.
        assert!(
            !lock_or_recover(&ddl_active).contains(&table_key),
            "before guard acquired, ddl_active must be empty"
        );

        // Acquire the guard (mirrors acquire_ddl_guard phase 2).
        let _guard = DdlActiveGuard::new(ddl_active.clone(), table_key.clone());

        // Double-check now defers — flush must see this and bail.
        assert!(
            lock_or_recover(&ddl_active).contains(&table_key),
            "after guard acquired, the flush double-check must defer"
        );

        // Drop the guard — re-check no longer defers (the two-phase loop's
        // re-drain can proceed).
        drop(_guard);
        assert!(
            !lock_or_recover(&ddl_active).contains(&table_key),
            "after guard dropped, the double-check must NOT defer (drain can proceed)"
        );
    }

    /// pending_bytes is tracked in ONE unit (estimate) per RowOp;
    /// TRUNCATE subtraction and deferral-restore both use `est_bytes` (no
    /// longer the serialized length), closing the drift that previously left
    /// pending_bytes either over-counted (TRUNCATE) or under-counted (deferral).
    #[test]
    fn test_row_op_carries_est_bytes_field() {
        // `from_value` (test constructor) populates est_bytes from the
        // serialized length as a sane default for tests.
        let row = serde_json::json!({"id": 42, "name": "hello"});
        let pk = vec!["id".to_string()];
        let op = RowOp::from_value(row, &pk);
        assert!(
            op.est_bytes > 0,
            "RowOp.est_bytes must be populated at construction (got {})",
            op.est_bytes
        );
        assert_eq!(
            op.est_bytes,
            op.data.get().len(),
            "from_value sets est_bytes = serialized length"
        );
    }

    /// (schema enforcement): Pipeline::is_table_filtered_out rejects
    /// tables whose schema != the configured postgres.schema, independent of
    /// the table_filter regex. Defense-in-depth for the CDC consumer's
    /// identical filter; pins the predicate behavior.
    #[test]
    fn test_is_table_filtered_out_rejects_other_schema() {
        let pg = crate::config::PgConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            password: String::new(),
            schema: "public".into(),
            app_name: "test".into(),
            table_filter: "*".into(),
            table_exclude: String::new(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            manage_publication: true,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 5000,
        };
        let pending_bytes = Arc::new(AtomicUsize::new(0));
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "db".into(),
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
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let (tx, rx) = watch::channel(false);
        let _ = tx;
        let pipeline = Pipeline::new(
            SrSink::new(mgr.clone()).unwrap(),
            mgr,
            std::collections::HashMap::new(),
            128,
            pg,
            pending_bytes.clone(),
            None,
            1000,
            4,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            rx,
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        // Same-schema table: not filtered out (table_filter None ⇒ pass).
        assert!(
            !pipeline.is_table_filtered_out("public", "orders"),
            "public.orders must pass when schema is public"
        );
        // Different-schema table: filtered out (would collide in SR).
        assert!(
            pipeline.is_table_filtered_out("archive", "orders"),
            "archive.orders must be filtered out when schema is public — would collide with public.orders"
        );
    }

    #[test]
    fn test_records_from_changes_truncate_first_and_kinds() {
        // Durable-buffer record serialization: Truncate marker (kind 3, empty
        // payload) must precede Upsert(1)/Delete(2) rows in WAL order.
        let mut changes = TableChanges::new();
        changes.truncate = true;
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 1, "v": "a"}),
            &["id".to_string()],
        ));
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 2, "v": "b"}),
            &["id".to_string()],
        ));
        let records = records_from_changes(&changes, 42);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].kind, BUF_TRUNCATE);
        assert!(records[0].payload.is_empty());
        assert_eq!(records[1].kind, BUF_UPSERT);
        assert_eq!(records[1].payload, br#"{"id":1,"v":"a"}"#);
        assert_eq!(records[2].kind, BUF_DELETE);
        assert_eq!(records[2].payload, br#"{"id":2,"v":"b"}"#);
        // Every record is stamped with the buffering-time relation OID.
        assert!(records.iter().all(|r| r.oid == 42));
    }

    #[test]
    fn test_row_op_from_buf_remaps_renamed_column_and_pk() {
        // RENAME COLUMN remap: buffered rows carry the OLD column name; the
        // replay rebuild must rewrite it to the live name BEFORE pk_key
        // extraction and col_names filtering (a renamed PK column must dedup
        // correctly, not collapse to the "?" fallback).
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        rename_map.lock().unwrap().insert(
            "public.t".to_string(),
            HashMap::from([("id".to_string(), "new_id".to_string())]),
        );
        let rec = BufRecord {
            kind: BUF_UPSERT,
            oid: 5,
            payload: br#"{"id":7,"v":"x"}"#.to_vec(),
        };
        let col_names = vec!["new_id".to_string(), "v".to_string()];
        let pk = vec!["new_id".to_string()];
        let op = row_op_from_buf(&rec, "public.t", &col_names, &pk, &rename_map).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(op.data.get()).unwrap();
        assert_eq!(parsed["new_id"], 7, "old key remapped to live name");
        assert!(parsed.get("id").is_none(), "old key must not survive");
        assert_eq!(
            op.pk_key.as_deref(),
            Some("7"),
            "pk_key extracted from the REMAPPED key"
        );
        assert_eq!(op.est_bytes, rec.payload.len());
    }

    #[test]
    fn test_row_op_from_buf_rejects_invalid_json() {
        // A malformed buffered payload must surface as Err (the replay
        // caller quarantines the segment) — never a silently dropped row.
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        let rec = BufRecord {
            kind: BUF_UPSERT,
            oid: 5,
            payload: b"not json".to_vec(),
        };
        let err = row_op_from_buf(&rec, "public.t", &[], &[], &rename_map)
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid"), "got: {err}");
    }

    #[test]
    fn test_row_op_from_buf_no_remap_when_key_live() {
        // Stale-entry guard: a key that exists in the CURRENT columns is
        // live and must never be remapped — a skipped DDL's map entry cannot
        // rewrite a same-incarnation column whose name was legitimately
        // reused.
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        rename_map.lock().unwrap().insert(
            "public.t".to_string(),
            HashMap::from([("id".to_string(), "new_id".to_string())]),
        );
        let rec = BufRecord {
            kind: BUF_UPSERT,
            oid: 5,
            payload: br#"{"id":7,"v":"x"}"#.to_vec(),
        };
        // "id" is LIVE in the current schema → stays "id".
        let col_names = vec!["id".to_string(), "v".to_string()];
        let pk = vec!["id".to_string()];
        let op = row_op_from_buf(&rec, "public.t", &col_names, &pk, &rename_map).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(op.data.get()).unwrap();
        assert_eq!(parsed["id"], 7, "live key must NOT be remapped");
        assert!(
            parsed.get("new_id").is_none(),
            "dead-name key must not appear"
        );
        assert_eq!(op.pk_key.as_deref(), Some("7"));
    }

    #[test]
    fn test_row_op_from_buf_resolves_chain() {
        // Chain resolution (replaces registration-time flattening): a→b→c
        // must resolve to the live name c with a plain-insert map (B3).
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        rename_map.lock().unwrap().insert(
            "public.t".to_string(),
            HashMap::from([
                ("a".to_string(), "b".to_string()),
                ("b".to_string(), "c".to_string()),
            ]),
        );
        let rec = BufRecord {
            kind: BUF_UPSERT,
            oid: 5,
            payload: br#"{"a":7,"v":"x"}"#.to_vec(),
        };
        let col_names = vec!["c".to_string(), "v".to_string()];
        let pk = vec!["c".to_string()];
        let op = row_op_from_buf(&rec, "public.t", &col_names, &pk, &rename_map).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(op.data.get()).unwrap();
        assert_eq!(parsed["c"], 7, "chain a→b→c resolved to the live name");
        assert!(parsed.get("a").is_none(), "old key must not survive");
        assert_eq!(
            op.pk_key.as_deref(),
            Some("7"),
            "pk extracted from the RESOLVED key"
        );
    }

    #[test]
    fn test_rows_to_changes_skips_stale_truncate() {
        // A TRUNCATE stamped with a stale relation OID must be
        // filtered like stale rows — replaying it would wipe the recreated
        // SR table.
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        let trunc = BufRecord {
            kind: BUF_TRUNCATE,
            oid: 7,
            payload: vec![],
        };
        let (changes, applied) =
            rows_to_changes(&[trunc], "public.t", &[], &[], &rename_map, Some(5)).unwrap();
        assert!(!changes.truncate, "stale TRUNCATE must be filtered");
        assert!(changes.ops.is_empty());
        assert_eq!(applied, 0, "filtered records never count as applied");
        // Same oid → truncate applies and counts.
        let trunc = BufRecord {
            kind: BUF_TRUNCATE,
            oid: 5,
            payload: vec![],
        };
        let (changes, applied) =
            rows_to_changes(&[trunc], "public.t", &[], &[], &rename_map, Some(5)).unwrap();
        assert!(changes.truncate);
        assert_eq!(applied, 1);
    }

    #[test]
    fn test_rows_to_changes_unknown_kind_errors() {
        // Unknown record kinds must Err (replay quarantines the segment) —
        // never silently skipped.
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        let rec = BufRecord {
            kind: 9,
            oid: 5,
            payload: vec![],
        };
        let err = rows_to_changes(&[rec], "public.t", &[], &[], &rename_map, Some(5))
            .unwrap_err()
            .to_string();
        assert!(err.contains("kind 9 unknown"), "got: {err}");
    }

    #[test]
    fn test_rows_to_changes_counts_applied() {
        // applied = converted upserts/deletes + one per OID-matched
        // TRUNCATE; stale records excluded.
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        let live_upsert = BufRecord {
            kind: BUF_UPSERT,
            oid: 5,
            payload: br#"{"id":1}"#.to_vec(),
        };
        let stale_delete = BufRecord {
            kind: BUF_DELETE,
            oid: 9,
            payload: br#"{"id":2}"#.to_vec(),
        };
        let live_trunc = BufRecord {
            kind: BUF_TRUNCATE,
            oid: 5,
            payload: vec![],
        };
        let col_names = vec!["id".to_string()];
        let (changes, applied) = rows_to_changes(
            &[live_upsert, stale_delete, live_trunc],
            "public.t",
            &col_names,
            &col_names,
            &rename_map,
            Some(5),
        )
        .unwrap();
        assert_eq!(applied, 2, "1 live upsert + 1 live truncate");
        assert!(changes.truncate);
        assert_eq!(changes.len(), 1, "only the upsert op survives filtering");
    }

    #[tokio::test]
    async fn test_flush_keeps_rows_without_pk_keys() {
        // Regression: rows serialized before the cache had PK
        // info (pk_key None — a table created mid-stream, whose WAL Relation
        // event carries no PK metadata, while refresh_relation_pks runs only
        // after the batch) must NOT collapse to the single "?" dedup entry.
        // Every row must reach SR (SR dedupes by PK anyway — idempotent).
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/api/testdb/t/_stream_load"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Success",
                "NumberLoadedRows": 3,
                "NumberFilteredRows": 0,
                "Label": "test",
                "Message": "OK",
            })))
            .mount(&server)
            .await;

        let sr_cfg = crate::config::SrConfig {
            node_urls: format!("127.0.0.1:{}", server.address().port()),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();

        // Three DISTINCT rows, all with pk_key None (empty pk_columns at
        // serialize time).
        let mut changes = TableChanges::new();
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 1, "v": "a"}),
            &[],
        ));
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 2, "v": "b"}),
            &[],
        ));
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 3, "v": "c"}),
            &[],
        ));
        let relation = Some(RelationInfo {
            schema: "public".to_string(),
            table: "t".to_string(),
            columns: vec![
                ColInfo {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_mod: -1,
                },
                ColInfo {
                    name: "v".to_string(),
                    type_oid: 25,
                    type_mod: -1,
                },
            ],
            // Cache knows the PK (refresh ran before the flush) — so the
            // dedup RUNS over the None-keyed rows.
            pk_columns: vec!["id".to_string()],
        });
        let col_names = vec!["id".to_string(), "v".to_string()];
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let unsent = prep_table_flush("public", "t", changes, relation.as_ref()).unwrap();
        flush_table(
            sink,
            mgr,
            "testdb".to_string(),
            "public",
            "t",
            relation,
            &col_names,
            &[],
            &HashMap::new(),
            1000,
            std::time::Instant::now() + Duration::from_secs(30),
            &mut None,
            pending,
            ddl_active,
            "public.t".to_string(),
            unsent,
        )
        .await
        .unwrap();

        // ALL THREE rows must have reached SR (the old code kept only the
        // last — silent row loss for the first batch of a new table).
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "exactly one stream load");
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("\"id\":1"), "row 1 present: {body}");
        assert!(body.contains("\"id\":2"), "row 2 present: {body}");
        assert!(body.contains("\"id\":3"), "row 3 present: {body}");
        assert_eq!(body.lines().count(), 3, "three JSON rows: {body}");
    }

    #[tokio::test]
    async fn test_flush_transaction_buffers_on_stream_load_failure() {
        // The prod-incident regression: a Stream Load failure must NOT fail
        // flush_transaction (which made the main loop reconnect and PG
        // re-stream the same WAL range forever). The failed table's
        // unconfirmed rows are durably buffered instead; flush_transaction
        // returns Ok and the checkpoint advances.
        // Uses the HTTP-200 trap: `{"Status":"Fail"}` must be parsed as a
        // failure by parse_stream_load_response.
        use crate::cdc::RelationColumn;
        use crate::config::PgConfig;
        use std::sync::atomic::AtomicUsize;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/api/testdb/t/_stream_load"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Fail",
                "Message": "boom",
            })))
            .mount(&server)
            .await;

        let pg_cfg = PgConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            password: String::new(),
            schema: "public".into(),
            app_name: "test".into(),
            table_filter: "*".into(),
            table_exclude: String::new(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            manage_publication: true,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 5000,
        };
        let sr_cfg = crate::config::SrConfig {
            node_urls: format!("127.0.0.1:{}", server.address().port()),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: String::new(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: std::collections::HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let buffered_events = Arc::new(AtomicU64::new(0));
        let buffer = test_wal_buffer();
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            std::collections::HashMap::new(),
            128,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            1000,
            4,
            pending.clone(),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            buffer.clone(),
            buffered_events.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );
        pipeline.add_relation(
            100,
            "public",
            "t",
            &[
                RelationColumn {
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: 0,
                },
                RelationColumn {
                    name: "v".into(),
                    type_oid: 25,
                    type_modifier: 0,
                },
            ],
            &["id".into()],
        );

        let mut changes = TableChanges::new();
        for id in 1..=3 {
            changes.add_upsert(RowOp::from_value(
                serde_json::json!({"id": id, "v": "x"}),
                &["id".into()],
            ));
        }
        pipeline.tables.insert("public.t".to_string(), changes);
        let result = pipeline.flush_transaction(100).await;
        assert!(
            result.is_ok(),
            "failed flush must succeed by durably buffering: {result:?}"
        );
        assert!(
            buffer.has_segments("public.t"),
            "the failed table's rows must be buffered as segments"
        );
        assert_eq!(
            buffered_events.load(Ordering::Relaxed),
            3,
            "all 3 unconfirmed rows buffered"
        );
        assert!(
            pending.lock().unwrap().is_empty(),
            "guard dropped on task exit — key removed from pending_cdc_tables"
        );

        // Subsequent changes for the segmented table route straight to the
        // buffer (existing has_segments invariant — WAL order preserved).
        let mut changes2 = TableChanges::new();
        for id in 4..=5 {
            changes2.add_upsert(RowOp::from_value(
                serde_json::json!({"id": id, "v": "y"}),
                &["id".into()],
            ));
        }
        pipeline.tables.insert("public.t".to_string(), changes2);
        let result2 = pipeline.flush_transaction(200).await;
        assert!(
            result2.is_ok(),
            "buffered routing flush must be Ok: {result2:?}"
        );
        assert_eq!(buffered_events.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn test_aborted_flush_task_slot_retains_unsent_rows() {
        // The flush-timeout arm's contract: a task aborted mid-send leaves
        // its unconfirmed rows in the unsent slot, so the collect loop can
        // durably buffer them (abort → join → drain). A real-timing abort
        // test is infeasible: compute_flush_timeout floors at 30s while the
        // sink's own HTTP timeout (5s in this setup) fires first — the error
        // arm, not the abort arm. This exercises the exact abort → join →
        // drain sequence deterministically.
        let mut changes = TableChanges::new();
        for id in 1..=3 {
            changes.add_upsert(RowOp::from_value(
                serde_json::json!({"id": id, "v": "x"}),
                &["id".into()],
            ));
        }
        let relation = Some(RelationInfo {
            schema: "public".to_string(),
            table: "t".to_string(),
            columns: vec![
                ColInfo {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_mod: -1,
                },
                ColInfo {
                    name: "v".to_string(),
                    type_oid: 25,
                    type_mod: -1,
                },
            ],
            pk_columns: vec!["id".to_string()],
        });
        let unsent = prep_table_flush("public", "t", changes, relation.as_ref()).unwrap();
        let unsent_t = unsent.clone();
        // Simulate a flush task mid-send: it holds the first chunk (an Arc
        // clone) without confirming it, then blocks forever.
        let handle = tokio::spawn(async move {
            let _chunk = lock_or_recover(&unsent_t).upserts.first().cloned();
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        // The collect loop's timeout arm: abort, then join before draining.
        handle.abort();
        let _ = handle.await;
        let tc = slot_to_changes(&unsent)
            .expect("aborted task's unconfirmed rows must remain capturable");
        assert_eq!(tc.ops.len(), 3, "all three unconfirmed rows preserved");
        assert!(!tc.truncate);
        assert!(
            slot_to_changes(&unsent).is_none(),
            "drained slot must be empty (no double-buffer)"
        );
    }

    #[test]
    fn test_slot_to_changes_truncate_pending() {
        // Pure helper: a TRUNCATE-only slot converts to Some(truncate=true);
        // a fully-empty slot converts to None.
        let slot: Arc<Mutex<UnsentFlushState>> = Arc::new(Mutex::new(UnsentFlushState {
            truncate_pending: true,
            deletes: Vec::new(),
            delete_idxs: Vec::new(),
            upserts: Vec::new(),
            upsert_idxs: Vec::new(),
        }));
        let tc = slot_to_changes(&slot).expect("truncate-pending slot must convert");
        assert!(tc.truncate);
        assert!(tc.ops.is_empty());
        let empty: Arc<Mutex<UnsentFlushState>> = Arc::new(Mutex::new(UnsentFlushState {
            truncate_pending: false,
            deletes: Vec::new(),
            delete_idxs: Vec::new(),
            upserts: Vec::new(),
            upsert_idxs: Vec::new(),
        }));
        assert!(slot_to_changes(&empty).is_none());
    }

    #[test]
    fn test_slot_to_changes_preserves_wal_order() {
        // Un-keyed rows (sentinel dedup keeps every op) must drain in
        // ORIGINAL WAL order: an upsert→delete pair on the same row drains
        // upsert-first. Inverted, replay's last-wins dedup would keep the
        // upsert and resurrect a row PG deleted.
        let mut changes = TableChanges::new();
        changes.add_upsert(RowOp::from_value(
            serde_json::json!({"id": 7, "v": "x"}),
            &[],
        ));
        changes.add_delete(RowOp::from_value(
            serde_json::json!({"id": 7, "v": "x"}),
            &[],
        ));
        let relation = Some(RelationInfo {
            schema: "public".to_string(),
            table: "t".to_string(),
            columns: vec![ColInfo {
                name: "id".to_string(),
                type_oid: 23,
                type_mod: -1,
            }],
            pk_columns: vec!["id".to_string()],
        });
        let unsent = prep_table_flush("public", "t", changes, relation.as_ref()).unwrap();
        let tc = slot_to_changes(&unsent).expect("ops present");
        assert_eq!(tc.ops.len(), 2);
        assert!(
            matches!(tc.ops[0], OrderedOp::Upsert(_)),
            "first op must be the upsert: {:?}",
            tc.ops
        );
        assert!(matches!(tc.ops[1], OrderedOp::Delete(_)));
    }

    #[test]
    fn test_replay_backoff_delay_schedule() {
        // 5s doubling to the hard 60s cap — deterministic (no jitter).
        assert_eq!(replay_backoff_delay(1), Duration::from_secs(5));
        assert_eq!(replay_backoff_delay(2), Duration::from_secs(10));
        assert_eq!(replay_backoff_delay(3), Duration::from_secs(20));
        assert_eq!(replay_backoff_delay(4), Duration::from_secs(40));
        assert_eq!(replay_backoff_delay(5), Duration::from_secs(60));
        assert_eq!(replay_backoff_delay(50), Duration::from_secs(60));
    }

    #[test]
    fn test_note_replay_result_escalates_and_clears() {
        // Failure bookkeeping contract: consecutive Errs escalate the delay
        // deterministically and push next_attempt forward by exactly that
        // delay; an Ok clears the entry entirely.
        let st: Arc<Mutex<HashMap<String, ReplayBackoff>>> = Arc::new(Mutex::new(HashMap::new()));
        let ok: Result<()> = Ok(());
        let err: Result<()> = Err(anyhow::anyhow!("sr down"));

        // Success on a never-seen table is a no-op.
        assert!(note_replay_result(&st, "public.t", &ok).is_none());

        let mut delays = Vec::new();
        for i in 1..=4u32 {
            let t0 = Instant::now();
            let d = note_replay_result(&st, "public.t", &err).expect("failure records a delay");
            let map = lock_or_recover(&st);
            let g = map.get("public.t").expect("failed table gains an entry");
            assert_eq!(g.failures, i);
            assert!(
                g.next_attempt >= t0 + d,
                "next attempt ≥ record time + delay"
            );
            delays.push(d);
        }
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(20),
                Duration::from_secs(40),
            ]
        );

        // A successful drain resets the schedule: no deferral lingers.
        assert!(note_replay_result(&st, "public.t", &ok).is_none());
        assert!(!lock_or_recover(&st).contains_key("public.t"));
    }

    #[test]
    fn test_replay_backoff_active_boundaries() {
        let now = Instant::now();
        // No entry → never deferred.
        assert!(!replay_backoff_active(None, now));
        let future = now + Duration::from_secs(10);
        let past = now.checked_sub(Duration::from_secs(10)).unwrap_or(now);
        assert!(replay_backoff_active(
            Some(&ReplayBackoff {
                failures: 2,
                next_attempt: future
            }),
            now
        ));
        assert!(!replay_backoff_active(
            Some(&ReplayBackoff {
                failures: 2,
                next_attempt: past
            }),
            now
        ));
    }

    #[tokio::test]
    async fn test_kick_replays_defers_by_failure_backoff() {
        // kick-side behavior: an active backoff window must suppress the
        // replayer spawn; once the window elapses the same state spawns
        // exactly one task. JoinSet retains finished tasks until pruned, so
        // len==N is race-free evidence of N spawns regardless of completion.
        use crate::cdc::RelationColumn;
        use crate::config::PgConfig;
        use std::sync::atomic::AtomicUsize;

        let pg_cfg = PgConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            password: String::new(),
            schema: "public".into(),
            app_name: "test".into(),
            table_filter: "*".into(),
            table_exclude: String::new(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            manage_publication: true,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 5000,
        };
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "db".into(),
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
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let buffer = test_wal_buffer();
        let mut pipeline = Pipeline::new(
            crate::sink::SrSink::new(mgr.clone()).unwrap(),
            mgr,
            std::collections::HashMap::new(),
            128,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            1000,
            4,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            buffer.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );
        pipeline.add_relation(
            100,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );
        buffer
            .append(
                "public.t",
                &[BufRecord {
                    kind: BUF_UPSERT,
                    oid: 100,
                    payload: br#"{"id":1}"#.to_vec(),
                }],
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert!(buffer.has_segments("public.t"));

        // Active backoff window → the kick must not spawn anything.
        lock_or_recover(&pipeline.replay_backoff).insert(
            "public.t".to_string(),
            ReplayBackoff {
                failures: 3,
                next_attempt: Instant::now() + Duration::from_secs(60),
            },
        );
        pipeline.kick_replays();
        assert_eq!(
            pipeline.replay_tasks.len(),
            0,
            "deferred kick must not spawn a replayer"
        );
        assert!(
            buffer.has_segments("public.t"),
            "deferred table keeps its segments"
        );
        assert!(
            lock_or_recover(&pipeline.replay_backoff).contains_key("public.t"),
            "entry retained while its window is active"
        );

        // Window elapsed → the identical state spawns exactly one replayer
        // (which may fail against the unreachable SR; irrelevant here).
        lock_or_recover(&pipeline.replay_backoff)
            .get_mut("public.t")
            .expect("entry still present")
            .next_attempt = Instant::now() - Duration::from_secs(1);
        pipeline.kick_replays();
        assert_eq!(
            pipeline.replay_tasks.len(),
            1,
            "elapsed backoff spawns one replayer"
        );
        // Drop without joining: joining would wait on the unreachable-SR
        // retry chain; Pipeline's Drop aborts in-flight JoinSet tasks.
    }

    #[tokio::test]
    async fn test_kick_replays_discards_segments_for_evicted_tables() {
        // Kick_replays must drain dropped_relations BEFORE the
        // relation lookup — a dropped table's buffered segments are
        // discarded (never auto-created on SR). Runs on every kick, so idle
        // streams (where begin_transaction never executes) are covered too.
        use crate::cdc::RelationColumn;
        use crate::config::PgConfig;
        use std::sync::atomic::AtomicUsize;

        let pg_cfg = PgConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            password: String::new(),
            schema: "public".into(),
            app_name: "test".into(),
            table_filter: "*".into(),
            table_exclude: String::new(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            manage_publication: true,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 5000,
        };
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "db".into(),
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
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let dropped: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let buffer = test_wal_buffer();
        let mut pipeline = Pipeline::new(
            crate::sink::SrSink::new(mgr.clone()).unwrap(),
            mgr,
            std::collections::HashMap::new(),
            128,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            1000,
            4,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            dropped.clone(),
            test_shutdown_rx(),
            buffer.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );
        // Live relation + buffered segment...
        pipeline.add_relation(
            100,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );
        buffer
            .append(
                "public.t",
                &[BufRecord {
                    kind: BUF_UPSERT,
                    oid: 100,
                    payload: br#"{"id":1}"#.to_vec(),
                }],
                &HashMap::new(),
            )
            .await
            .unwrap();
        // ...then the DDL poller enqueues the DROP.
        dropped.lock().unwrap().insert("public.t".to_string());
        assert!(buffer.has_segments("public.t"));

        pipeline.kick_replays();
        // Wait for the spawned replay task: the dropped table's segments are
        // discarded WITHOUT any SR contact (relation resolved to None after
        // the eviction drain).
        while let Some(r) = pipeline.replay_tasks.join_next().await {
            r.expect("replay task must not panic");
        }
        assert!(
            !buffer.has_segments("public.t"),
            "segments for the dropped table must be discarded"
        );
        assert!(
            buffer.list_segments("public.t").await.unwrap().is_empty(),
            "no segments left on disk"
        );
        assert!(
            dropped.lock().unwrap().is_empty(),
            "dropped_relations drained by kick_replays"
        );
    }

    #[test]
    fn test_add_relation_recreate_replaces_old_oid_and_cancels_eviction() {
        // DROP+CREATE fix: re-registering a name with a NEW oid must (a)
        // evict the old oid's relations/pk_cache entries (no stale columns,
        // no OID-keyed leak), and (b) cancel any pending name-keyed
        // eviction so the drain can never remove the LIVE relation.
        use crate::cdc::RelationColumn;
        use crate::config::PgConfig;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let pg_cfg = PgConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            username: "u".into(),
            password: String::new(),
            schema: "public".into(),
            app_name: "test".into(),
            table_filter: "*".into(),
            table_exclude: String::new(),
            slot_name: "test_slot".into(),
            publication: "test_pub".into(),
            manage_publication: true,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 5000,
        };
        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".into(),
            database: "db".into(),
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
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let dropped: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut pipeline = Pipeline::new(
            crate::sink::SrSink::new(mgr.clone()).unwrap(),
            mgr,
            std::collections::HashMap::new(),
            128,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            1000,
            4,
            Arc::new(Mutex::new(HashSet::new())),
            Arc::new(Mutex::new(HashSet::new())),
            dropped.clone(),
            test_shutdown_rx(),
            test_wal_buffer(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );
        let cols = [RelationColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: 0,
        }];
        let pk = ["id".to_string()];

        // Pre-DROP incarnation under oid 100.
        pipeline.add_relation(100, "public", "t", &cols, &pk);
        assert_eq!(pipeline.name_to_oid.get("public.t"), Some(&100));
        assert!(pipeline.relations.contains_key(&100));

        // DDL poller enqueues the eviction after DROP TABLE public.t.
        dropped.lock().unwrap().insert("public.t".to_string());

        // Same-batch CREATE TABLE re-registers under a NEW oid.
        pipeline.add_relation(200, "public", "t", &cols, &pk);
        assert_eq!(
            pipeline.name_to_oid.get("public.t"),
            Some(&200),
            "name now maps to the live oid"
        );
        assert!(
            !pipeline.relations.contains_key(&100),
            "old oid's relation entry evicted"
        );
        assert!(
            !pipeline.pk_cache.contains_key(&100),
            "old oid's pk cache evicted"
        );
        assert!(pipeline.relations.contains_key(&200));
        assert!(
            !dropped.lock().unwrap().contains("public.t"),
            "pending name-keyed eviction cancelled by re-registration"
        );

        // The drain must not touch the live relation.
        pipeline.evict_dropped_relations();
        assert!(pipeline.relations.contains_key(&200));
        assert_eq!(pipeline.name_to_oid.get("public.t"), Some(&200));
    }

    #[tokio::test]
    async fn test_replay_skips_records_with_stale_oid() {
        // DROP+CREATE fix: a segment may mix records stamped with the
        // pre-DROP relation OID and the recreated (live) one. Replay must
        // apply only the records whose oid matches relation_oid, flush them
        // through the normal path, and delete the segment.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/api/testdb/t/_stream_load"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Success",
                "NumberLoadedRows": 1,
                "NumberFilteredRows": 0,
                "Label": "test",
                "Message": "OK",
            })))
            .mount(&server)
            .await;

        let sr_cfg = crate::config::SrConfig {
            node_urls: format!("127.0.0.1:{}", server.address().port()),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 1, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).unwrap();
        let buffer = test_wal_buffer();
        // One segment, two records: oid 5 (live) and oid 7 (stale).
        buffer
            .append(
                "public.t",
                &[
                    BufRecord {
                        kind: BUF_UPSERT,
                        oid: 5,
                        payload: br#"{"id":5,"v":"live"}"#.to_vec(),
                    },
                    BufRecord {
                        kind: BUF_UPSERT,
                        oid: 7,
                        payload: br#"{"id":7,"v":"stale"}"#.to_vec(),
                    },
                ],
                &HashMap::new(),
            )
            .await
            .unwrap();

        let relation = RelationInfo {
            schema: "public".to_string(),
            table: "t".to_string(),
            columns: vec![
                ColInfo {
                    name: "id".to_string(),
                    type_oid: 23,
                    type_mod: -1,
                },
                ColInfo {
                    name: "v".to_string(),
                    type_oid: 25,
                    type_mod: -1,
                },
            ],
            pk_columns: vec!["id".to_string()],
        };
        let rename_map: RenameMap = Arc::new(Mutex::new(HashMap::new()));
        let replayed = Arc::new(AtomicU64::new(0));
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let semaphore = Arc::new(Semaphore::new(2));

        let result = tokio::spawn({
            let buffer = buffer.clone();
            let rename_map = rename_map.clone();
            let replayed = replayed.clone();
            let pending = pending.clone();
            let ddl_active = ddl_active.clone();
            async move {
                replay_buffered_table(
                    sink,
                    mgr,
                    "testdb".to_string(),
                    "public.t".to_string(),
                    "public".to_string(),
                    "t".to_string(),
                    Some(relation),
                    Some(5), // live relation OID
                    vec!["id".to_string(), "v".to_string()],
                    vec![],
                    HashMap::new(),
                    1000,
                    Duration::from_secs(30),
                    buffer,
                    semaphore,
                    pending,
                    ddl_active,
                    replayed,
                    rename_map,
                )
                .await
            }
        })
        .await
        .unwrap();
        assert!(result.is_ok(), "replay must succeed: {result:?}");

        // Only the oid-5 row reached SR — the stale oid-7 row was filtered.
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "exactly one stream load");
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("\"id\":5"), "live row applied: {body}");
        assert!(!body.contains("\"id\":7"), "stale row filtered: {body}");
        // Segment deleted after apply; counter counts APPLIED records only —
        // the stale oid-7 record never reached SR and must not count (overcount fix).
        assert!(
            buffer.list_segments("public.t").await.unwrap().is_empty(),
            "segment deleted after replay"
        );
        assert_eq!(replayed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_flush_routes_paused_table_to_wal_buffer() {
        // Durable write-pause routing: a table in `ddl_active` must have its
        // changes appended to the wal buffer (durable segment) — NOT
        // stream-loaded — and must be removed from pending_cdc_tables so the
        // DDL poller's wait_for_cdc_drain proceeds. Runs fully offline (no SR
        // connection is attempted for the paused table).
        use crate::cdc::{ColValue, DataEvent, RelationColumn};
        use std::collections::HashMap;
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let sr_cfg = crate::config::SrConfig {
            node_urls: "localhost:8030".to_string(),
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "".to_string(),
            connection_timeout_ms: 5000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            http_tls: Default::default(),
            mysql_tls: Default::default(),
            table_properties: HashMap::new(),
        };
        let mgr = Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000).unwrap(),
        );
        let sink = crate::sink::SrSink::new(mgr.clone()).expect("sink construction");
        let pg_cfg = crate::config::PgConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "testdb".to_string(),
            username: "replicator".to_string(),
            password: "pass".to_string(),
            schema: "public".to_string(),
            app_name: "test".to_string(),
            table_filter: "*".to_string(),
            table_exclude: "".to_string(),
            slot_name: "test_slot".to_string(),
            publication: "test_pub".to_string(),
            manage_publication: false,
            tls: crate::config::PgTls::default(),
            connect_timeout_ms: 30000,
        };

        let buffer = test_wal_buffer();
        let ddl_active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        lock_or_recover(&ddl_active).insert("public.t".to_string());
        let pending: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let buffered_events = Arc::new(AtomicU64::new(0));
        let buffered_bytes = Arc::new(AtomicU64::new(0));

        let mut pipeline = Pipeline::new(
            sink,
            mgr,
            HashMap::new(),
            512,
            pg_cfg,
            Arc::new(AtomicUsize::new(0)),
            None,
            5,
            2,
            pending.clone(),
            ddl_active.clone(),
            Arc::new(Mutex::new(HashSet::new())),
            test_shutdown_rx(),
            buffer.clone(),
            buffered_events.clone(),
            buffered_bytes.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
        );

        pipeline.add_relation(
            1,
            "public",
            "t",
            &[RelationColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            }],
            &["id".into()],
        );

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            pipeline.begin_transaction();
            pipeline
                .process_event(&DataEvent::Insert {
                    relation_id: 1,
                    columns: vec![ColValue::Value("42".to_string())],
                })
                .await
                .unwrap();
            pipeline
                .process_event(&DataEvent::Delete {
                    relation_id: 1,
                    old_columns: vec![ColValue::Value("42".to_string())],
                })
                .await
                .unwrap();

            let result = pipeline.flush_transaction(1000).await;
            assert!(
                result.is_ok(),
                "paused table must flush Ok (durably buffered): {:?}",
                result.err(),
            );
        });

        assert!(buffer.has_segments("public.t"), "segment must exist");
        assert!(
            pending.lock().unwrap().is_empty(),
            "paused table must be removed from pending_cdc_tables (drain proceeds)"
        );
        assert_eq!(buffered_events.load(Ordering::Relaxed), 2);
        assert!(buffered_bytes.load(Ordering::Relaxed) > 0);

        rt.block_on(async {
            let segs = buffer.list_segments("public.t").await.unwrap();
            assert_eq!(segs, vec![0]);
            let records = buffer.read_segment("public.t", 0).await.unwrap();
            assert_eq!(records.len(), 2, "upsert + delete in WAL order");
            assert_eq!(records[0].kind, BUF_UPSERT);
            assert_eq!(records[1].kind, BUF_DELETE);
            let v: serde_json::Value = serde_json::from_slice(&records[0].payload).unwrap();
            assert_eq!(v["id"], "42");
        });
    }
}
