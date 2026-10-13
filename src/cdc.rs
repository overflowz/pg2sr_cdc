//! PostgreSQL CDC consumer — connects via logical replication, receives WAL events.
//! Handles the pgwire-replication protocol: BEGIN/COMMIT, Relation messages,
//! Insert/Update/Delete/Truncate events, and Type mappings. Maintains a relation
//! cache for OID resolution and supports table_filter-based event filtering.
use anyhow::{Context, Result};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};
use regex::Regex;
use tokio::sync::watch;

use crate::config::PgConfig;
use crate::util::AbortOnDrop;

/// Distinguishes PG NULL, unchanged TOAST columns, and actual values.
/// This prevents TOAST corruption: `PGUnchangedToastedValue` was previously
/// mapped to `None` (same as PGNull), causing UPDATE to send `null` to SR
/// for unchanged large values, silently overwriting real data.
#[derive(Debug, Clone, PartialEq)]
pub enum ColValue {
    /// PG NULL — maps to JSON `null` for INSERT/DELETE, skipped for UPDATE.
    Null,
    /// Unchanged TOAST column (PGUnchangedToastedValue) — skip in JSON output
    /// so Stream Load doesn't modify the column.
    Unchanged,
    /// An actual value (includes empty string, which is distinct from Null).
    Value(String),
}

#[derive(Debug, Clone)]
pub enum DataEvent {
    Insert {
        relation_id: u32,
        columns: Vec<ColValue>,
    },
    Update {
        relation_id: u32,
        old_columns: Vec<ColValue>,
        new_columns: Vec<ColValue>,
    },
    Delete {
        relation_id: u32,
        old_columns: Vec<ColValue>,
    },
    Relation {
        relation_id: u32,
        namespace: String,
        name: String,
        columns: Vec<RelationColumn>,
        pk_columns: Vec<String>,
    },
    Truncate {
        relation_id: u32,
    },
    Type,
}

#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug)]
pub struct TransactionEvent {
    /// LSN at which this transaction committed. Used by feed_raw_data to
    /// tag the event batch; callers typically read last_commit_lsn instead.
    #[allow(dead_code)]
    pub commit_lsn: i64,
    /// The events (Insert/Update/Delete/Relation/Truncate) in this transaction.
    pub events: Vec<DataEvent>,
}

fn tuple_column_to_colvalue(
    col: &pgoutput::events::base::tuple_data::TupleDataColumn<
        pgoutput::options::BinaryValueTraitOff,
    >,
) -> ColValue {
    match col {
        pgoutput::events::base::tuple_data::TupleDataColumn::PGNull => ColValue::Null,
        // 🔴 CRITICAL: Previously returned None (same as PGNull), which caused
        // UPDATE to send null to SR for unchanged TOAST columns, silently
        // overwriting real data. Now returns Unchanged so columns_to_json
        // omits these columns from the JSON output, leaving SR data intact.
        pgoutput::events::base::tuple_data::TupleDataColumn::PGUnchangedToastedValue => {
            ColValue::Unchanged
        }
        pgoutput::events::base::tuple_data::TupleDataColumn::Value(v) => {
            // PG empty string '' arrives as a zero-length Value (distinct from PGNull).
            // Pass it through — do NOT treat empty String as Null.
            ColValue::Value(v.clone())
        }
    }
}

fn parse_tuple_data(
    data: &[pgoutput::events::base::tuple_data::TupleDataColumn<
        pgoutput::options::BinaryValueTraitOff,
    >],
) -> Vec<ColValue> {
    data.iter().map(tuple_column_to_colvalue).collect()
}

fn parse_old_data(
    old: &pgoutput::events::base::tuple_data::OldDataOrPrimaryKeyTupleData<
        pgoutput::options::BinaryValueTraitOff,
    >,
) -> Vec<ColValue> {
    match old {
        pgoutput::events::base::tuple_data::OldDataOrPrimaryKeyTupleData::OldTupleData(data)
        | pgoutput::events::base::tuple_data::OldDataOrPrimaryKeyTupleData::PrimaryKeyTupleData(
            data,
        ) => parse_tuple_data(data),
    }
}

pub struct CdcConsumer {
    client: ReplicationClient,
    relations: std::collections::HashMap<u32, (String, String, Vec<RelationColumn>, Vec<String>)>,
    tx_events: Vec<DataEvent>,
    /// Last commit LSN received from pgoutput, used for checkpointing
    pub last_commit_lsn: i64,
    /// Highest LSN from filtered-out commits (tx_events was empty after filtering).
    /// Used to advance WAL checkpoint so filtered events don't cause unbounded WAL growth.
    last_filtered_lsn: i64,
    /// PG config for lazy relation resolution on resume
    pg_config: PgConfig,
    /// Compiled table_filter regex — compiled once in connect(), not per-event
    table_filter_regex: Regex,
    /// Compiled table_exclude regex (None = no exclusion)
    table_exclude_regex: Option<Regex>,
    /// Shared PG connection for fetch_relation (lazily created, reused across calls)
    catalog_conn: Option<tokio_postgres::Client>,
    /// Background task for the shared catalog connection — aborted on drop.
    catalog_conn_task: Option<AbortOnDrop>,
    /// DDL capture table name (from config) for infrastructure filtering.
    ddl_table_name: String,
    /// Latest WAL end position from KeepAlive events.
    /// Used during idle periods to advance the slot position so PG can
    /// discard system WAL (autovacuum, checkpoints) that has no data events.
    last_wal_end: i64,
    /// Set to true when a KeepAlive arrives while the batch is empty (no
    /// pending transaction data). Signalled to read_transaction_batch so
    /// it returns immediately instead of waiting for batch_timeout, allowing
    /// the idle ACK in main.rs to fire right away. This keeps the slot's
    /// confirmed_flush_lsn advancing in lockstep with WAL writes during idle.
    idle_keepalive_pending: bool,
    /// True between a Begin and its Commit. Gates the idle ACK: PG sends
    /// keepalives with `wal_end` advancing THROUGH large uncommitted
    /// transactions (no changes delivered yet), and the batch-timeout arm can
    /// return an empty batch mid-transaction — ACKing to `wal_end` in either
    /// window would discard WAL the pipeline still needs (at-least-once).
    in_transaction: bool,
    /// Number of consecutive pgoutput parse errors in the current connection.
    /// Reset on any successful parse. When this exceeds `max_parse_errors`,
    /// the consumer returns an error to trigger process exit — forcing operator
    /// intervention when the WAL stream contains permanently corrupt data.
    consecutive_parse_errors: u32,
    /// Maximum allowed consecutive parse errors before giving up.
    max_parse_errors: u32,
    /// Maximum WAL events within a single PG transaction.
    /// Prevents OOM on bulk INSERT/DELETE in one transaction.
    max_tx_events: usize,
    /// Per-event byte limit. Prevents a single oversized event (e.g. a 2 GB
    /// BYTEA) from blowing past `max_pending_memory_mb`. Defaults to
    /// `max_pending_memory_mb / 4` (minimum 1 MB), clamped to 90MB (SR limit).
    max_event_bytes: usize,
    /// The single PG schema this pipeline replicates (`[postgres] schema`).
    /// Previously validated but unused — events for tables in OTHER schemas
    /// are filtered here (and in the snapshot/DDL paths) so same-named tables
    /// from different schemas cannot collide in one SR table.
    pg_schema: String,
}

impl Drop for CdcConsumer {
    fn drop(&mut self) {
        // Signal the replication client to send a clean CopyDone to PG.
        // Best-effort — if the channel is closed this is a no-op.
        self.client.stop();
    }
}

/// Highest LSN safe to record as the checkpoint during an idle period.
///
/// `wal_end` (the KeepAlive WAL head) is deliberately IGNORED: it can outpace
/// actually-delivered transactions, and recording it as the checkpoint would
/// let PG discard WAL for an un-flushed transaction → permanent data loss on
/// crash. Kept as a parameter (instead of dropped at the call site) so a
/// regression test can assert the exclusion holds even when `wal_end` exceeds
/// the commit/filtered LSNs.
fn idle_checkpoint_lsn(last_commit_lsn: i64, last_filtered_lsn: i64, wal_end: i64) -> i64 {
    let _ = wal_end; // intentionally excluded — see doc comment
    last_commit_lsn.max(last_filtered_lsn)
}

fn check_tx_events_limit(current: usize, max: usize) -> Result<()> {
    if current >= max {
        anyhow::bail!(
            "tx_events exceeded max_tx_events limit ({} events in current transaction). \
             This indicates a bulk INSERT/DELETE affecting too many rows in a single PG transaction. \
             Split the operation or increase pipeline.max_tx_events in config.",
            current,
        );
    }
    Ok(())
}

/// Estimate the byte size of a DataEvent for memory-bound checks.
/// Sums all `ColValue::Value` string lengths. Relation/Truncate/Type events
/// return 0 (metadata, not data).
pub fn data_event_size(event: &DataEvent) -> usize {
    fn col_size(cols: &[ColValue]) -> usize {
        cols.iter()
            .map(|v| match v {
                ColValue::Value(s) => s.len(),
                _ => 0,
            })
            .sum()
    }
    match event {
        DataEvent::Insert { columns, .. } => col_size(columns),
        DataEvent::Update {
            old_columns,
            new_columns,
            ..
        } => col_size(old_columns) + col_size(new_columns),
        DataEvent::Delete { old_columns, .. } => col_size(old_columns),
        _ => 0,
    }
}

/// If `oid` belongs to a partition of a partitioned table (relkind='p'),
/// return the parent partitioned table's (schema, name). Otherwise returns None.
/// PG logical replication sends events for the partition OID, but the parent
/// table identity must be used for StarRocks table naming.
async fn resolve_partition_parent(
    client: &tokio_postgres::Client,
    oid: u32,
) -> Result<Option<(String, String)>> {
    let row = client
        .query_opt(
            "SELECT n.nspname, p.relname \
             FROM pg_inherits i \
             JOIN pg_class p ON p.oid = i.inhparent \
             JOIN pg_namespace n ON n.oid = p.relnamespace \
             WHERE i.inhrelid = $1 AND p.relkind = 'p'",
            &[&oid],
        )
        .await?;
    Ok(row.map(|r| (r.get::<_, String>(0), r.get::<_, String>(1))))
}

impl CdcConsumer {
    /// Check if the given table name matches the configured DDL capture table.
    fn is_infrastructure_table(&self, table: &str) -> bool {
        table == self.ddl_table_name
    }

    /// Check if a table should be replicated based on infrastructure + schema
    /// + table_filter + table_exclude.
    fn should_replicate_table(&self, schema: &str, table: &str) -> bool {
        if self.is_infrastructure_table(table) {
            return false;
        }
        // Single-schema enforcement: the publication is FOR ALL TABLES (all
        // schemas), but `sr_table_name` drops the schema prefix — replicating
        // a second schema would silently collide same-named tables into one
        // SR table. Non-matching schemas are filtered here (their LSNs still
        // advance via last_filtered_lsn, so WAL doesn't grow unbounded).
        if schema != self.pg_schema {
            return false;
        }
        if !crate::config::table_matches_filter(&self.table_filter_regex, schema, table) {
            return false;
        }
        if crate::config::is_table_excluded(&self.table_exclude_regex, schema, table) {
            return false;
        }
        true
    }

    /// Tell PostgreSQL we've durably stored everything up to this LSN.
    pub fn acknowledge_lsn(&self, lsn: u64) {
        self.client.update_applied_lsn(Lsn::from_u64(lsn));
    }

    /// Drain any filtered-out LSN that needs acknowledgment.
    /// Returns the highest LSN from filtered-only commits (tables outside
    /// table_filter) and resets the counter. The caller should include this
    /// in the WAL checkpoint to prevent unbounded growth of filtered events.
    pub fn drain_filtered_lsn(&mut self) -> i64 {
        let lsn = self.last_filtered_lsn;
        self.last_filtered_lsn = 0;
        lsn
    }

    /// Return the highest safely-ackable LSN (commit or filtered) without
    /// resetting any state. Used for idle-period checkpoint advancement.
    ///
    /// `last_wal_end` (the KeepAlive WAL head) is intentionally EXCLUDED. It
    /// reflects PG's current WAL insertion point, which can include
    /// transactions whose Begin/Commit events have not yet been delivered to
    /// this consumer. Advancing the in-memory checkpoint to it (and persisting
    /// it via the background saver) would let PG discard WAL for a
    /// not-yet-flushed transaction — permanent data loss on crash. The idle
    /// ACK path still ACKs only up to the durably-saved LSN (`safe_ack`), but
    /// we must not even *record* a checkpoint LSN past what we've actually
    /// committed/filtered. `last_wal_end` remains available via `last_wal_end()`
    /// for status reporting only.
    pub fn max_known_lsn(&self) -> i64 {
        idle_checkpoint_lsn(
            self.last_commit_lsn,
            self.last_filtered_lsn,
            self.last_wal_end,
        )
    }

    /// Accessor for logging: last KeepAlive WAL end.
    pub fn last_wal_end(&self) -> i64 {
        self.last_wal_end
    }

    /// Populate relation cache by querying PG catalog.
    /// Needed on resume because PG doesn't re-send Relation messages
    /// when starting from a non-zero LSN.
    pub async fn load_relations(&mut self) -> Result<()> {
        let conn_str = self.pg_config.conn_str();
        let tls = crate::config::PgTlsConnector::from_config(&self.pg_config.tls)
            .context("Failed to configure PG TLS for relation lookup")?;
        let (client, conn) = tls
            .connect(&conn_str)
            .await
            .context("Failed to connect PG for relation lookup")?;
        let _conn_guard = crate::util::AbortOnDrop(conn);

        // Single-schema enforcement: only load relations for the configured
        // schema. Previously all schemas were loaded; with `sr_table_name`
        // dropping the schema prefix, same-named tables from different
        // schemas collided in one SR table.
        let rows = client
            .query(
                "SELECT c.oid::int4, n.nspname, c.relname \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r', 'p') AND n.nspname = $1",
                &[&self.pg_schema],
            )
            .await
            .context("Failed to query PG relations")?;

        // Collect all OIDs for bulk catalog queries (columns + PKs in 2 queries
        // instead of 2*N individual queries).
        let mut oids: Vec<u32> = Vec::with_capacity(rows.len());
        let mut table_infos: Vec<(u32, String, String)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let oid: i32 = row.get(0);
            let oid = oid as u32;
            let schema: String = row.get(1);
            let table: String = row.get(2);
            oids.push(oid);
            table_infos.push((oid, schema, table));
        }

        let bulk_columns = crate::pg_catalog::fetch_bulk_columns(&client, &oids).await?;
        let bulk_pks = crate::pg_catalog::fetch_bulk_primary_keys(&client, &oids).await?;

        // Batch partition parent resolution: one query for all OIDs instead of
        // N individual queries. Uses ANY($1::int4[]) for a single round trip.
        let partition_parents: std::collections::HashMap<u32, (String, String)> = {
            // Convert u32→i32 for _int4 serialization (same bit-cast rationale as pg_catalog).
            let oids_i32: Vec<i32> = oids.iter().map(|&o| o as i32).collect();
            let rows = client
                .query(
                    "SELECT i.inhrelid::int4, n.nspname, p.relname \
                     FROM pg_inherits i \
                     JOIN pg_class p ON p.oid = i.inhparent \
                     JOIN pg_namespace n ON n.oid = p.relnamespace \
                     WHERE i.inhrelid = ANY($1::int4[]) AND p.relkind = 'p'",
                    &[&oids_i32],
                )
                .await
                .context("Failed to batch-resolve partition parents")?;
            rows.iter()
                .map(|row| {
                    let oid: i32 = row.get(0);
                    let schema: String = row.get(1);
                    let table: String = row.get(2);
                    tracing::debug!(
                        "Partition oid={} → parent {}.{} in load_relations",
                        oid,
                        schema,
                        table,
                    );
                    (oid as u32, (schema, table))
                })
                .collect()
        };

        for (oid, schema, table) in table_infos {
            let (schema, table) = match partition_parents.get(&oid) {
                Some((parent_schema, parent_table)) => {
                    (parent_schema.clone(), parent_table.clone())
                }
                None => (schema, table),
            };

            let pcols = bulk_columns.get(&oid).cloned().unwrap_or_default();
            let columns: Vec<RelationColumn> = pcols
                .into_iter()
                .map(|c| RelationColumn {
                    name: c.name,
                    type_oid: c.type_oid,
                    type_modifier: c.type_modifier,
                })
                .collect();

            if !columns.is_empty() && self.should_replicate_table(&schema, &table) {
                let pk_columns = bulk_pks.get(&oid).cloned().unwrap_or_default();

                tracing::debug!(
                    "Loaded relation oid={} → {}.{} ({} cols, PK={:?})",
                    oid,
                    schema,
                    table,
                    columns.len(),
                    pk_columns
                );

                let pk_for_cache = pk_columns.clone();
                self.push_event(DataEvent::Relation {
                    relation_id: oid,
                    namespace: schema.clone(),
                    name: table.clone(),
                    columns: columns.clone(),
                    pk_columns,
                })?;
                self.relations
                    .insert(oid, (schema, table, columns, pk_for_cache));
            }
        }

        tracing::info!("Loaded {} relations from PG catalog", self.relations.len());
        Ok(())
    }

    pub async fn connect(
        cfg: &PgConfig,
        start_lsn: i64,
        ddl_table_name: &str,
        max_tx_events: usize,
        max_event_bytes: usize,
    ) -> Result<Self> {
        let config = ReplicationConfig {
            host: cfg.host.clone(),
            port: cfg.port,
            user: cfg.username.clone(),
            password: cfg.password.clone(),
            database: cfg.database.clone(),
            slot: cfg.slot_name.clone(),
            publication: cfg.publication.clone(),
            start_lsn: Lsn::from_u64(start_lsn as u64),
            tls: crate::config::pg_replication_tls_config(&cfg.tls)?,
            // Fast feedback: tell PG about our progress every second
            // so WAL is released promptly after SR confirms writes
            status_interval: std::time::Duration::from_secs(1),
            // Wake up every 1s to detect silent connection drops
            // (pgwire blocks on TCP read up to this interval, so a lower
            //  value means faster disconnect detection and reconnection)
            idle_wakeup_interval: std::time::Duration::from_secs(1),
            ..Default::default()
        };

        tracing::info!(
            "Connecting to PG replication at {}:{} db={} slot={} publication={}",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.slot_name,
            cfg.publication
        );

        // Apply a connection timeout so the pipeline doesn't hang forever when PG is unreachable.
        let connect_timeout = cfg.connect_timeout();
        let client =
            match tokio::time::timeout(connect_timeout, ReplicationClient::connect(config)).await {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => return Err(e).context("Failed to connect PG replication client"),
                Err(_elapsed) => {
                    return Err(anyhow::anyhow!(
                        "PG replication connection timed out after {:?}",
                        connect_timeout,
                    ))
                }
            };
        tracing::info!("Connected to PG replication");

        let table_filter_regex = crate::config::compile_table_filter(&cfg.table_filter)
            .context("Failed to compile table_filter regex at CDC connect")?;
        let table_exclude_regex = crate::config::compile_table_exclude(&cfg.table_exclude)
            .context("Failed to compile table_exclude regex at CDC connect")?;
        Ok(Self {
            client,
            relations: std::collections::HashMap::new(),
            tx_events: Vec::new(),
            last_commit_lsn: 0,
            last_filtered_lsn: 0,
            last_wal_end: 0,
            idle_keepalive_pending: false,
            in_transaction: false,
            pg_config: cfg.clone(),
            table_filter_regex,
            table_exclude_regex,
            catalog_conn: None,
            catalog_conn_task: None,
            ddl_table_name: ddl_table_name.to_string(),
            consecutive_parse_errors: 0,
            max_parse_errors: 10,
            max_tx_events,
            max_event_bytes,
            pg_schema: cfg.schema.clone(),
        })
    }

    /// Is a transaction open in the consumer (Begin received, Commit not yet)?
    /// main.rs gates the idle ACK on this: ACKing to `wal_end` while a
    /// transaction is mid-flight would discard WAL whose changes are still in
    /// the consumer's buffer (at-least-once contract).
    pub fn has_in_flight_tx(&self) -> bool {
        self.in_transaction
    }

    /// Drain synthetic events from `load_relations()` so they can be fed to
    /// `Pipeline::process_event()` directly (before the CDC loop starts).
    /// WAL BEGIN events clear `tx_events`, so we must extract them early.
    pub fn drain_tx_events(&mut self) -> Vec<DataEvent> {
        std::mem::take(&mut self.tx_events)
    }

    /// Push a WAL event with guards against unbounded tx_events growth.
    /// Returns an error if the event count or per-event byte limit is exceeded.
    fn push_event(&mut self, event: DataEvent) -> Result<()> {
        check_tx_events_limit(self.tx_events.len(), self.max_tx_events)?;
        let event_bytes = data_event_size(&event);
        if event_bytes > self.max_event_bytes {
            anyhow::bail!(
                "Single event size {} bytes ({:.1} MB) exceeds max_event_bytes limit \
                 ({} bytes / {:.1} MB). This indicates a single WAL event with extremely \
                 large column values (e.g. a multi-GB BYTEA or TEXT). \
                 Increase max_pending_memory_mb or split the operation.",
                event_bytes,
                event_bytes as f64 / (1024.0 * 1024.0),
                self.max_event_bytes,
                self.max_event_bytes as f64 / (1024.0 * 1024.0),
            );
        }
        self.tx_events.push(event);
        Ok(())
    }

    /// Resolve an OID to a (schema, table) pair that passes table_filter.
    ///
    /// Checks the local relation cache first. On miss, queries PG catalog via
    /// `fetch_relation`, caches the result, and pushes a synthetic `Relation`
    /// event so the pipeline knows the columns. Returns `Ok(None)` if the table
    /// doesn't exist or is filtered out. Logs errors/catalog-misses via tracing.
    ///
    /// The synthetic Relation event goes through `push_event` so the
    /// `max_tx_events` guard applies uniformly (previously it bypassed the
    /// limit check).
    async fn resolve_table(
        &mut self,
        oid: u32,
        event_label: &str,
    ) -> Result<Option<(String, String)>> {
        if let Some((ns, name, _, _)) = self.relations.get(&oid) {
            if self.should_replicate_table(ns, name) {
                return Ok(Some((ns.clone(), name.clone())));
            }
            return Ok(None);
        }
        // Cache miss — try via catalog
        match self.fetch_relation(oid).await {
            Ok(Some((ns, name, cols, pk_cols))) => {
                tracing::info!(
                    "Auto-resolved relation oid={} → {}.{} ({} cols, PK={:?})",
                    oid,
                    ns,
                    name,
                    cols.len(),
                    pk_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                );
                let pk_for_cache = pk_cols.clone();
                self.relations
                    .insert(oid, (ns.clone(), name.clone(), cols.clone(), pk_for_cache));
                if !self.should_replicate_table(&ns, &name) {
                    return Ok(None);
                }
                // Push synthetic Relation event so pipeline has column metadata
                // AND PK info from the PG catalog (unlike WAL Relation events).
                self.push_event(DataEvent::Relation {
                    relation_id: oid,
                    namespace: ns.clone(),
                    name: name.clone(),
                    columns: cols,
                    pk_columns: pk_cols,
                })?;
                Ok(Some((ns, name)))
            }
            Ok(None) => {
                tracing::warn!("{event_label} dropped: relation_id={oid} not found in PG catalog");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(
                    "{event_label} dropped: failed to auto-resolve relation_id={oid}: {e:?}"
                );
                Ok(None)
            }
        }
    }

    /// Process a single replication event. Returns Some(events) when a transaction commits.
    async fn process_replication_event(
        &mut self,
        ev: ReplicationEvent,
    ) -> Result<Option<Vec<DataEvent>>> {
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                if let Some(tx) = self.feed_raw_data(wal_end, &data).await? {
                    if !tx.events.is_empty() {
                        return Ok(Some(tx.events));
                    }
                }
            }
            ReplicationEvent::Begin { final_lsn, .. } => {
                tracing::trace!("BEGIN at LSN {}", final_lsn);
                // pgwire-replication 0.3+ intercepts pgoutput Begin/Commit at the wire
                // level and emits them as ReplicationEvent::Begin/Commit — the raw
                // pgoutput bytes never reach feed_raw_data. Clear tx_events here.
                self.tx_events.clear();
                self.in_transaction = true;
                // Transaction started — we're no longer idle. Clear the flag so
                // a stale KeepAlive doesn't cause early batch return.
                self.idle_keepalive_pending = false;
            }
            ReplicationEvent::Commit { lsn, .. } => {
                tracing::trace!("COMMIT at LSN {}", lsn);
                self.in_transaction = false;
                self.last_commit_lsn = lsn.as_u64() as i64;
                // A cleanly-committed transaction clears the consecutive
                // parse-error counter so intermittent corruption can't bleed
                // across transaction boundaries (defense-in-depth alongside
                // the per-successful-parse reset in feed_raw_data).
                self.consecutive_parse_errors = 0;
                // pgwire-replication 0.3+ intercepts pgoutput Commit, so feed_raw_data
                // never sees the 'C' byte. Return accumulated tx_events here.
                if self.tx_events.is_empty() {
                    // All filtered out or no events — track the LSN so the
                    // caller can acknowledge WAL progress for filtered tables.
                    // Without this, filtered-out transactions would cause
                    // unbounded WAL growth on reconnect.
                    if lsn.as_u64() as i64 > self.last_filtered_lsn {
                        self.last_filtered_lsn = lsn.as_u64() as i64;
                    }
                    return Ok(None);
                }
                let events = std::mem::take(&mut self.tx_events);
                return Ok(Some(events));
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                tracing::trace!("KeepAlive at {}", wal_end);
                let wal_end_u64 = wal_end.as_u64();
                // Monotonic: a post-CR KeepAlive could theoretically arrive
                // with a smaller wal_end; never rewind (guards the checkpoint
                // math in the caller).
                self.last_wal_end = self.last_wal_end.max(wal_end_u64 as i64);
                // If the batch is empty (no pending transaction data), signal
                // read_transaction_batch to return immediately so the idle ACK
                // in main.rs fires right away. This keeps confirmed_flush_lsn
                // advancing in lockstep with pg_current_wal_lsn() during idle
                // periods, preventing unbounded replication lag.
                //
                // Safety of ACKing to wal_end during idle depends on TWO
                // conditions checked in main.rs:
                //   1. tx_events.is_empty() — the walsender has decoded all WAL
                //      up to wal_end and delivered any replicated-table txns.
                //   2. !pipeline.has_pending() — all received events have been
                //      flushed to StarRocks and checkpointed.
                // When both hold, ACKing to wal_end can only discard WAL
                // containing non-replicated system operations (autovacuum,
                // checkpoints) — no data loss. A THIRD condition guards the
                // Begin→first-change window: during large transactions PG
                // sends keepalives whose wal_end advances THROUGH the unsent
                // txn, so the idle ACK must not fire while a transaction is
                // open (its changes are in WAL, not yet delivered).
                if self.tx_events.is_empty() && !self.in_transaction {
                    self.idle_keepalive_pending = true;
                }
            }
            ReplicationEvent::StoppedAt { .. } => {
                tracing::warn!("Replication stopped");
                return Err(anyhow::anyhow!("Replication stopped at target LSN"));
            }
            ReplicationEvent::Message { .. } => {}
        }
        Ok(None)
    }

    /// Read multiple committed transactions, batching them for higher throughput.
    /// Returns when `max_rows` total events are accumulated across all transactions,
    /// after `batch_timeout` from the first event in the batch, or on idle timeout
    /// (returns empty batch so caller can perform idle housekeeping).
    /// Returns empty Vec on shutdown.
    pub async fn read_transaction_batch(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
        max_rows: usize,
        batch_timeout: std::time::Duration,
        max_batch_bytes: usize,
    ) -> Result<Vec<(i64, Vec<DataEvent>)>> {
        let mut batch: Vec<(i64, Vec<DataEvent>)> = Vec::new();
        let mut total_events: usize = 0;
        let mut total_bytes: usize = 0;

        // Age of the batch — set when the first event is added, used for
        // batch-age timeout. Guarantees a flush within batch_timeout of the
        // first event, even at full throughput where the idle timeout never fires.
        let mut batch_start: Option<std::time::Instant> = None;

        loop {
            // Row limit
            if total_events >= max_rows {
                return Ok(batch);
            }
            // Byte limit — prevents OOM from many large events in one batch
            if total_bytes >= max_batch_bytes {
                return Ok(batch);
            }
            // Batch-age timeout: flush if the first event has been waiting
            // longer than batch_timeout, even if new events keep arriving.
            if let Some(start) = batch_start {
                if start.elapsed() >= batch_timeout {
                    return Ok(batch);
                }
            }

            // Compute how long to wait for the next event.
            // - Empty batch: wait up to batch_timeout (idle detect still works)
            // - Non-empty batch: wait at most the remaining age, so the age
            //   check at the top fires promptly once batch_timeout has elapsed.
            let wait = match batch_start {
                Some(start) => {
                    let elapsed = start.elapsed();
                    if elapsed >= batch_timeout {
                        return Ok(batch);
                    }
                    batch_timeout - elapsed
                }
                None => batch_timeout,
            };

            let result = tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    return Ok(batch);
                }
                result = tokio::time::timeout(wait, self.client.recv()) => {
                    result
                }
            };

            match result {
                Ok(Ok(Some(ev))) => {
                    if let Some(events) = (self.process_replication_event(ev).await)? {
                        let lsn = self.last_commit_lsn;
                        let event_count = events.len();
                        let event_bytes: usize = events.iter().map(data_event_size).sum();
                        // Record batch start time on the first event
                        if batch.is_empty() {
                            batch_start = Some(std::time::Instant::now());
                        }
                        batch.push((lsn, events));
                        total_events += event_count;
                        total_bytes += event_bytes;
                        // Data arrived — clear idle keepalive flag so we don't
                        // return early while there's work to do.
                        self.idle_keepalive_pending = false;
                        continue;
                    }
                    // Keepalive or other non-transaction event.
                    // If a KeepAlive just set idle_keepalive_pending and the
                    // batch is empty, return immediately so main.rs can advance
                    // the slot's confirmed_flush_lsn without waiting for the
                    // full batch_timeout. This keeps replication lag bounded
                    // during idle periods.
                    if self.idle_keepalive_pending && batch.is_empty() {
                        return Ok(batch);
                    }
                    continue;
                }
                Ok(Ok(None)) => {
                    tracing::warn!("Replication stream ended");
                    return Err(anyhow::anyhow!("Stream ended"));
                }
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("PG replication error: {e:?}"));
                }
                Err(_) => {
                    // Timeout — return the empty batch so the caller (main.rs)
                    // can perform idle-period housekeeping: advance the slot
                    // position to the latest KeepAlive WAL end, save the
                    // checkpoint, and ack to PG. Without this return, the
                    // loop spins forever inside read_transaction_batch and
                    // the idle ack/cp code in main.rs is dead.
                    if batch.is_empty() {
                        return Ok(batch);
                    }
                    // Non-empty batch — the age or row-limit check at the
                    // loop top will handle the return.
                    continue;
                }
            }
        }
    }

    /// Fetch a relation's metadata from PG catalog on-the-fly when not cached.
    /// Reuses a shared catalog connection to avoid connection churn on burst OID misses.
    async fn fetch_relation(
        &mut self,
        oid: u32,
    ) -> Result<Option<(String, String, Vec<RelationColumn>, Vec<String>)>> {
        // Lazily create the shared catalog connection if not yet established
        if self.catalog_conn.is_none() {
            let conn_str = self.pg_config.conn_str();
            let tls = crate::config::PgTlsConnector::from_config(&self.pg_config.tls)
                .context("Failed to configure PG TLS for catalog connection")?;
            match tls.connect(&conn_str).await {
                Ok((client, _conn)) => {
                    let task = AbortOnDrop(tokio::spawn(async move {
                        if let Err(e) = _conn.await {
                            tracing::error!("Catalog connection error: {:?}", e);
                        }
                    }));
                    self.catalog_conn_task = Some(task);
                    self.catalog_conn = Some(client);
                }
                Err(e) => {
                    tracing::warn!("Failed to connect catalog conn for fetch_relation oid={}: {:?}, falling back to new connection", oid, e);
                }
            }
        }

        // Try to use the shared connection; if it fails, fall back to one-shot
        let result = if let Some(ref client) = self.catalog_conn {
            Self::query_relation(client, oid).await
        } else {
            Self::query_relation_one_shot(&self.pg_config, oid).await
        };

        // If the shared connection failed (stale/broken), reset it for next time
        if result.is_err() && self.catalog_conn.is_some() {
            tracing::warn!("Catalog connection failed on oid={oid}, discarding for reconnect");
            self.catalog_conn = None;
            self.catalog_conn_task = None; // abort old background task
        }

        result
    }

    /// Query relation metadata using an existing PG connection.
    /// Returns (schema, table, columns, pk_columns).
    async fn query_relation(
        client: &tokio_postgres::Client,
        oid: u32,
    ) -> Result<Option<(String, String, Vec<RelationColumn>, Vec<String>)>> {
        let row = client
            .query(
                "SELECT n.nspname, c.relname \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = $1 AND c.relkind IN ('r', 'p')",
                &[&oid],
            )
            .await?;

        let (schema, table) = match row.first() {
            Some(r) => (r.get::<_, String>(0), r.get::<_, String>(1)),
            None => return Ok(None),
        };

        // If this table is a partition of a partitioned table (relkind='p'),
        // map to the parent table's identity for StarRocks naming.
        // PG logical replication sends events for the partition's OID,
        // but the parent table name must be used so CDC data goes to
        // the correct SR table (e.g. `s31_part` not `s31_part_default`).
        let (schema, table) = match resolve_partition_parent(client, oid).await? {
            Some((parent_schema, parent_table)) => {
                tracing::info!(
                    "Partition oid={} resolved to parent {}.{} (was {}.{})",
                    oid,
                    parent_schema,
                    parent_table,
                    schema,
                    table,
                );
                (parent_schema, parent_table)
            }
            None => (schema, table),
        };

        let pcols = crate::pg_catalog::fetch_columns(client, oid).await?;
        let columns: Vec<RelationColumn> = pcols
            .into_iter()
            .map(|c| RelationColumn {
                name: c.name,
                type_oid: c.type_oid,
                type_modifier: c.type_modifier,
            })
            .collect();

        if columns.is_empty() {
            return Ok(None);
        }

        // Query primary key columns — mirrors the query in load_relations().
        // Without this, tables resolved via fetch_relation during CDC get empty
        // pk_columns, causing DELETEs to be silently skipped for PK tables and
        // auto_create_sr_table to fall back to heuristics.
        let pk_columns = crate::pg_catalog::fetch_primary_keys(client, oid).await?;

        tracing::info!(
            "Fetched relation oid={} → {} ({} cols, PK={:?})",
            oid,
            table,
            columns.len(),
            pk_columns
        );
        Ok(Some((schema, table, columns, pk_columns)))
    }

    /// One-shot fallback: create a new PG connection, query, and discard.
    async fn query_relation_one_shot(
        pg_config: &PgConfig,
        oid: u32,
    ) -> Result<Option<(String, String, Vec<RelationColumn>, Vec<String>)>> {
        let conn_str = pg_config.conn_str();
        let tls = crate::config::PgTlsConnector::from_config(&pg_config.tls)
            .context("Failed to configure PG TLS for one-shot relation fetch")?;
        let (client, conn) = tls
            .connect(&conn_str)
            .await
            .context("Failed to connect PG for one-shot relation fetch")?;
        let _conn_guard = AbortOnDrop(tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!("One-shot catalog connection ended: {:?}", e);
            }
        }));
        Self::query_relation(&client, oid).await
    }

    async fn feed_raw_data(
        &mut self,
        wal_end: Lsn,
        data: &[u8],
    ) -> Result<Option<TransactionEvent>> {
        self.last_wal_end = self.last_wal_end.max(wal_end.as_u64() as i64);
        if data.is_empty() {
            return Ok(None);
        }

        let event_type = match pgoutput::events::event::EventType::from_char(data[0]) {
            Some(et) => et,
            None => {
                self.consecutive_parse_errors += 1;
                tracing::error!(
                    wal_end = wal_end.as_u64(),
                    "Unknown pgoutput type byte: 0x{:02x} (error #{}/{}) at WAL {}",
                    data[0],
                    self.consecutive_parse_errors,
                    self.max_parse_errors,
                    wal_end,
                );
                if self.consecutive_parse_errors >= self.max_parse_errors {
                    return Err(anyhow::anyhow!(
                        "Too many consecutive pgoutput parse errors ({} >= {}) at WAL {} — \
                         corrupt WAL stream requires operator intervention \
                         (pg_replication_slot_advance past this LSN to skip it)",
                        self.consecutive_parse_errors,
                        self.max_parse_errors,
                        wal_end,
                    ));
                }
                return Ok(None);
            }
        };

        let event = match pgoutput::events::event::Event::<
            pgoutput::options::BinaryValueTraitOff,
            pgoutput::options::StreamingValueTraitOff,
        >::parse(&event_type, &data[1..])
        {
            Ok(e) => {
                self.consecutive_parse_errors = 0;
                e
            }
            Err(e) => {
                self.consecutive_parse_errors += 1;
                tracing::error!(
                        wal_end = wal_end.as_u64(),
                        "FATAL: pgoutput parse error for event type byte 0x{:02x} (error #{}/{}) at WAL {} — {}.\
                         WAL record skipped; advance will come when a subsequent event is acknowledged.",
                        data[0],
                        self.consecutive_parse_errors,
                        self.max_parse_errors,
                        wal_end,
                        e,
                    );
                if self.consecutive_parse_errors >= self.max_parse_errors {
                    return Err(anyhow::anyhow!(
                        "Too many consecutive pgoutput parse errors ({} >= {}) at WAL {} — \
                         corrupt WAL stream requires operator intervention \
                         (pg_replication_slot_advance past this LSN to skip it)",
                        self.consecutive_parse_errors,
                        self.max_parse_errors,
                        wal_end,
                    ));
                }
                return Ok(None);
            }
        };

        if let pgoutput::events::event::Event::Base(base) = event {
            match base {
                pgoutput::events::base::event::BaseEvent::Begin(begin) => {
                    tracing::trace!("BEGIN txn lsn={}", begin.final_transaction_lsn);
                    self.tx_events.clear();
                    self.in_transaction = true;
                }
                pgoutput::events::base::event::BaseEvent::Commit(commit) => {
                    tracing::trace!(
                        "COMMIT at lsn={}, {} events",
                        commit.lsn,
                        self.tx_events.len()
                    );
                    self.last_commit_lsn = commit.lsn;
                    let tx = TransactionEvent {
                        commit_lsn: commit.lsn,
                        events: std::mem::take(&mut self.tx_events),
                    };
                    return Ok(Some(tx));
                }
                pgoutput::events::base::event::BaseEvent::Relation(rel) => {
                    // Always cache the relation mapping (needed even for infra tables
                    // so we can skip their events), but don't push to tx_events.
                    let columns: Vec<RelationColumn> = rel
                        .columns
                        .iter()
                        .map(|c| RelationColumn {
                            name: c.name.clone(),
                            type_oid: c.oid as u32,
                            type_modifier: c.type_modifier,
                        })
                        .collect();

                    // Query catalog for PK info AND partition→parent resolution.
                    // `fetch_relation` (→ `query_relation`) now calls
                    // `resolve_partition_parent()` which maps partition OIDs to
                    // the parent partitioned table. This ensures CDC data for
                    // partitions goes to the correct SR table (e.g. `s31_part`
                    // not `s31_part_default`).
                    let fetch_result = match self.fetch_relation(rel.oid as u32).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                "Failed to fetch relation info for oid={} ({}.{}) — \
                                 falling back to pgoutput names with empty PK. \
                                 Partitioned tables may route to the wrong SR table: {:?}",
                                rel.oid,
                                rel.relation_namespace,
                                rel.name,
                                e,
                            );
                            None
                        }
                    };

                    // Use the PG catalog-resolved names (handles partition→parent
                    // mapping) if available; fall back to the pgoutput event names.
                    let (namespace, name): (String, String) = match &fetch_result {
                        Some((ns, n, _, _)) => (ns.clone(), n.clone()),
                        None => (rel.relation_namespace.clone(), rel.name.clone()),
                    };
                    let pk_cols: Vec<String> =
                        fetch_result.map(|(_, _, _, pk)| pk).unwrap_or_default();

                    let should_replicate = self.should_replicate_table(&namespace, &name);

                    tracing::info!(
                        "Relation {}.{} (oid={}, {} cols, pk=?) {}",
                        namespace,
                        name,
                        rel.oid,
                        columns.len(),
                        if !should_replicate {
                            " [SKIP filtered]"
                        } else {
                            ""
                        }
                    );

                    let pk_for_cache = pk_cols.clone();
                    self.relations.insert(
                        rel.oid as u32,
                        (
                            namespace.clone(),
                            name.clone(),
                            columns.clone(),
                            pk_for_cache,
                        ),
                    );
                    if should_replicate {
                        self.push_event(DataEvent::Relation {
                            relation_id: rel.oid as u32,
                            namespace,
                            name,
                            columns,
                            pk_columns: pk_cols,
                        })?;
                    }
                }
                pgoutput::events::base::event::BaseEvent::Insert(ins) => {
                    let oid = ins.oid as u32;
                    if let Some((ns, name)) = self.resolve_table(oid, "INSERT").await? {
                        let values = parse_tuple_data(&ins.data);
                        tracing::trace!("INSERT {}.{} ({} cols)", ns, name, values.len());
                        self.push_event(DataEvent::Insert {
                            relation_id: oid,
                            columns: values,
                        })?;
                    }
                }
                pgoutput::events::base::event::BaseEvent::Update(upd) => {
                    let oid = upd.oid as u32;
                    if let Some((ns, name)) = self.resolve_table(oid, "UPDATE").await? {
                        let new_values = parse_tuple_data(&upd.data);
                        let old_values = upd
                            .old_data_or_primary_key
                            .as_ref()
                            .map(parse_old_data)
                            .unwrap_or_default();
                        tracing::trace!("UPDATE {}.{}", ns, name);
                        self.push_event(DataEvent::Update {
                            relation_id: oid,
                            old_columns: old_values,
                            new_columns: new_values,
                        })?;
                    }
                }
                pgoutput::events::base::event::BaseEvent::Delete(del) => {
                    let oid = del.oid as u32;
                    if let Some((ns, name)) = self.resolve_table(oid, "DELETE").await? {
                        let old_values = del
                            .old_data_or_primary_key
                            .as_ref()
                            .map(parse_old_data)
                            .unwrap_or_default();
                        tracing::trace!("DELETE {}.{}", ns, name);
                        self.push_event(DataEvent::Delete {
                            relation_id: oid,
                            old_columns: old_values,
                        })?;
                    }
                }
                pgoutput::events::base::event::BaseEvent::Truncate(trunc) => {
                    // Propagate TRUNCATE to SR — send a Truncate event for each affected OID.
                    // Use resolve_table to fetch schema/name from PG catalog if the OID
                    // is not yet in our relation cache (e.g. when CDC resumes mid-stream
                    // and the first event for a table is a TRUNCATE). Without this,
                    // the TRUNCATE would be silently dropped, leaving SR with stale rows
                    // that PG just removed.
                    let oids: &[i32] = &trunc.oids;
                    for &oid in oids {
                        let oid = oid as u32;
                        if let Some((ns, name)) = self.resolve_table(oid, "TRUNCATE").await? {
                            tracing::info!("TRUNCATE {}.{} (oid={})", ns, name, oid);
                            self.push_event(DataEvent::Truncate { relation_id: oid })?;
                        }
                    }
                }
                pgoutput::events::base::event::BaseEvent::Type(typ) => {
                    tracing::debug!(
                        "PG custom type: oid={} schema={} name={}",
                        typ.oid,
                        typ.type_namespace,
                        typ.name,
                    );
                    self.push_event(DataEvent::Type)?;
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::ColValue;

    #[test]
    fn test_tuple_column_to_colvalue_empty_string() {
        use super::tuple_column_to_colvalue;
        use pgoutput::events::base::tuple_data::TupleDataColumn;
        use pgoutput::options::BinaryValueTraitOff;

        // PG empty string '' → Value("") → ColValue::Value(""), not ColValue::Null
        let empty = TupleDataColumn::<BinaryValueTraitOff>::Value(String::new());
        assert_eq!(
            tuple_column_to_colvalue(&empty),
            ColValue::Value(String::new())
        );

        // Non-empty string → ColValue::Value(value)
        let val = TupleDataColumn::<BinaryValueTraitOff>::Value("hello".to_string());
        assert_eq!(
            tuple_column_to_colvalue(&val),
            ColValue::Value("hello".to_string())
        );

        // PGNull → ColValue::Null
        let null: TupleDataColumn<BinaryValueTraitOff> = TupleDataColumn::PGNull;
        assert_eq!(tuple_column_to_colvalue(&null), ColValue::Null);

        // PGUnchangedToastedValue → ColValue::Unchanged (NOT ColValue::Null!)
        let unchanged: TupleDataColumn<BinaryValueTraitOff> =
            TupleDataColumn::PGUnchangedToastedValue;
        assert_eq!(tuple_column_to_colvalue(&unchanged), ColValue::Unchanged,
            "PGUnchangedToastedValue must map to ColValue::Unchanged, not ColValue::Null — prevents TOAST corruption");
    }

    #[test]
    fn test_check_tx_events_limit_allows_below_max() {
        use super::check_tx_events_limit;
        assert!(check_tx_events_limit(0, 10).is_ok());
        assert!(check_tx_events_limit(9, 10).is_ok());
    }

    #[test]
    fn test_check_tx_events_limit_rejects_at_max() {
        use super::check_tx_events_limit;
        let err = check_tx_events_limit(10, 10).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("max_tx_events"),
            "error must mention max_tx_events: {msg}"
        );
        assert!(
            msg.contains("10"),
            "error must mention current count 10: {msg}"
        );
    }

    #[test]
    fn test_check_tx_events_limit_rejects_above_max() {
        use super::check_tx_events_limit;
        assert!(check_tx_events_limit(15, 10).is_err());
    }

    #[test]
    fn test_data_event_size_insert() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Insert {
            relation_id: 100,
            columns: vec![
                ColValue::Value("42".to_string()),
                ColValue::Value("hello world".to_string()),
            ],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 13); // "42" (2) + "hello world" (11) = 13
    }

    #[test]
    fn test_data_event_size_update() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Update {
            relation_id: 200,
            old_columns: vec![ColValue::Value("old".to_string())],
            new_columns: vec![ColValue::Value("newer_value".to_string())],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 14); // "old" (3) + "newer_value" (11) = 14
    }

    #[test]
    fn test_data_event_size_delete() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Delete {
            relation_id: 300,
            old_columns: vec![ColValue::Value("ABC-123".to_string())],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 7); // "ABC-123" = 7
    }

    #[test]
    fn test_data_event_size_null_columns() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Insert {
            relation_id: 400,
            columns: vec![ColValue::Null, ColValue::Value("data".to_string())],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 4); // Null (0) + "data" (4) = 4
    }

    #[test]
    fn test_data_event_size_empty_columns() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Insert {
            relation_id: 500,
            columns: vec![],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 0); // empty columns = 0 bytes
    }

    #[test]
    fn test_data_event_size_unchanged() {
        use super::{data_event_size, DataEvent};

        let event = DataEvent::Update {
            relation_id: 600,
            old_columns: vec![ColValue::Unchanged],
            new_columns: vec![ColValue::Unchanged],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 0); // Unchanged columns contribute 0 bytes
    }

    #[test]
    fn test_data_event_size_large_value() {
        use super::{data_event_size, DataEvent};

        let large = "x".repeat(1_000_000);
        let event = DataEvent::Insert {
            relation_id: 700,
            columns: vec![ColValue::Value(large)],
        };

        let size = data_event_size(&event);
        assert_eq!(size, 1_000_000);
    }

    #[test]
    fn test_idle_checkpoint_lsn_excludes_keepalive_wal_end() {
        use super::idle_checkpoint_lsn;
        // wal_end (KeepAlive WAL head) MUST NOT advance the idle checkpoint
        // past what's actually been committed/filtered — it can include
        // un-delivered transactions whose WAL would then be discarded by PG
        // after the ACK, causing permanent data loss on crash.
        assert_eq!(
            idle_checkpoint_lsn(100, 50, 9999),
            100,
            "wal_end must be excluded"
        );
        assert_eq!(
            idle_checkpoint_lsn(0, 0, 9999),
            0,
            "no commits/filtered → 0, never wal_end"
        );
        assert_eq!(
            idle_checkpoint_lsn(100, 200, 50),
            200,
            "filtered can dominate"
        );
        assert_eq!(idle_checkpoint_lsn(300, 200, 50), 300, "commit dominates");
        // Even a stale (smaller) wal_end must not rewind.
        assert_eq!(idle_checkpoint_lsn(0, 0, 0), 0);
    }
}
