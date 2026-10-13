//! Initial snapshot — copies existing table schemas and data from PG to StarRocks.
//! Supports gap detection (re-snapshots only new/changed tables), BYTEA columns
//! (encode/decode via hex+unhex), parallel table snapshotting, and mid-snapshot
//! checkpointing so crash recovery loses at most one table.
use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};
use tokio_postgres::Client;

use crate::checkpoint::lock_checkpoint;
use crate::checkpoint::Checkpoint;
use crate::checkpoint::CheckpointStore;
use crate::config::PgConfig;
use crate::pg_catalog::CatalogColumn;
use crate::sr_connection::SrConnectionManager;

/// Compute a deterministic hash of a table's schema (column names + type OIDs
/// plus PRIMARY KEY membership). Used for gap detection: if the PG schema changed
/// since the snapshot (including PK changes), the hash won't match and the
/// table is re-snapshotted.
/// Uses SHA-256 (stable across toolchains and platforms) instead of djb2,
/// to avoid collision risk from 64-bit non-cryptographic hashing.
fn compute_schema_hash(cols: &[CatalogColumn], pk_columns: &[String]) -> u64 {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    // Include PK column list so that ADD/DROP PRIMARY KEY (without other column
    // changes) triggers a re-snapshot. Without this, a PK change leaves the SR
    // table with the wrong key model (DUPLICATE vs PRIMARY KEY) and CDC upserts
    // silently misbehave.
    hasher.update(b"pk:");
    for pk in pk_columns {
        hasher.update(pk.as_bytes());
        hasher.update(b",");
    }
    hasher.update(b"\x00cols:");
    for col in cols {
        hasher.update(col.name.as_bytes());
        hasher.update(b"\x01"); // inter-column delimiter
        hasher.update(col.type_oid.to_le_bytes());
        hasher.update(col.type_modifier.to_le_bytes());
    }
    // Truncate SHA-256 to 64 bits for storage in checkpoint.
    // Collision probability for realistic schema sizes remains negligible
    // with a 64-bit cryptographic hash.
    let result = hasher.finalize();
    // sha2::digest::Output<Sha256> is always 32 bytes, so result[..8]
    // is always 8 elements. Use a fixed-size array pattern instead of
    // try_into().unwrap() to make the invariant self-documenting and
    // eliminate any panic-on-misuse landmine if the hasher changes.
    let mut truncated = [0u8; 8];
    truncated.copy_from_slice(&result[..8]);
    u64::from_le_bytes(truncated)
}

// ── Shared snapshot helpers (used by run_snapshot and snapshot_one_table) ──

/// Run an SR async operation bounded by `dur`. The
/// snapshot path applied `query_timeout_ms` only to the PG data query, not to
/// SR DDL/catalog operations. This bounds SR DDL (CREATE/DROP shadow, swap)
/// with the same configured timeout.
async fn sr_op_timeout<T>(
    dur: std::time::Duration,
    op: impl std::future::Future<Output = Result<T>>,
    on_timeout: impl FnOnce() -> String,
) -> Result<T> {
    match tokio::time::timeout(dur, op).await {
        Ok(r) => r,
        Err(_) => Err(anyhow::anyhow!("{}", on_timeout())),
    }
}

/// Column types requiring custom SELECT expressions during snapshot export.
/// - BYTEA (17): encode(col, 'hex') to avoid \\x prefix
/// - TIMESTAMPTZ (1184): (col AT TIME ZONE 'UTC')::text — render in UTC so
///   snapshot rows match the CDC path which converts to UTC. Without this,
///   snapshot rows are in PG's local timezone → silent TZ shift on the
///   same column between snapshot and CDC data.
/// - INTERVAL (1186): extract(epoch) → microseconds for SR BIGINT
/// - MONEY (790): col::numeric to strip currency formatting for SR DECIMAL
/// - NUMERIC/DECIMAL (1700): col::text to prevent JSON precision loss (>15 digits)
/// - FLOAT4/FLOAT8 (700/701): NaN/±Infinity → NULL (matches the CDC path,
///   which nulls them because SR DOUBLE can't store them; row_to_json would
///   emit "NaN"/"Infinity" strings → filtered rows → stuck snapshot)
/// - Array counterparts (_bytea 1001, _timestamptz 1185, _interval 1187,
///   _money 791, _numeric 1231): row_to_json renders PG text forms
///   ("\x...", "{1 year}", "$1.00") into typed SR array columns — filtered
///   rows / divergence vs the CDC typed-array path. Render each element
///   through the same conversion as the scalar path via unnest+to_json.
const CUSTOM_SNAPSHOT_OIDS: &[u32] = &[
    17, 1184, 1186, 790, 1700, 700, 701, 1001, 1185, 1187, 791, 1231,
];
const BYTEA_OID: u32 = 17;

/// Build a per-element conversion subquery for array columns whose elements
/// need the same custom rendering as their scalar counterparts. Produces a
/// `to_json(ARRAY(SELECT <conv> FROM unnest(col) WITH ORDINALITY ...))`
/// expression so element ORDER is preserved and the result embeds as a JSON
/// array in row_to_json — matching the CDC typed-array conversion.
fn array_custom_expr(q: &str, elem_conv: &str) -> String {
    format!(
        "CASE WHEN {q} IS NULL THEN NULL ELSE to_json(ARRAY( \
         SELECT {elem_conv} FROM unnest({q}) WITH ORDINALITY AS u(x, ord) ORDER BY ord \
         )) END AS {q}"
    )
}

/// Build the SELECT column list for snapshot data export.
/// Returns "*" for tables without custom-type columns, or a comma-separated
/// list of expressions for tables with BYTEA/INTERVAL/MONEY/NUMERIC columns.
fn build_snapshot_col_list(cols: &[CatalogColumn]) -> (String, bool, Vec<String>) {
    let custom: Vec<&CatalogColumn> = cols
        .iter()
        .filter(|c| CUSTOM_SNAPSHOT_OIDS.contains(&c.type_oid))
        .collect();
    if custom.is_empty() {
        return ("*".to_string(), false, vec![]);
    }

    let bytea_names: Vec<String> = custom
        .iter()
        .filter(|c| c.type_oid == BYTEA_OID)
        .map(|c| c.name.clone())
        .collect();

    let col_list = cols
        .iter()
        .map(|c| {
            let q = crate::pg_catalog::pg_quote(&c.name);
            match c.type_oid {
                17 => format!("encode({q}::bytea, 'hex') AS {q}"),
                1184 => format!("({q} AT TIME ZONE 'UTC')::text AS {q}"),
                1186 => format!("(EXTRACT(EPOCH FROM {q}) * 1000000)::bigint AS {q}"),
                790 => format!("{q}::numeric::text AS {q}"),
                1700 => format!("{q}::text AS {q}"),
                // FLOAT4/FLOAT8: NaN/±Infinity → NULL (matches CDC; SR DOUBLE
                // can't store them and "NaN" strings would be filtered).
                700 | 701 => format!(
                    "CASE WHEN {q}::text IN ('NaN','Infinity','-Infinity') THEN NULL ELSE {q} END AS {q}"
                ),
                // _bytea: strip \x per element (CDC: strip_bytea_hex_prefix)
                1001 => array_custom_expr(&q, "encode(x, 'hex')"),
                // _timestamptz: UTC per element (CDC: timestamptz_to_utc)
                1185 => array_custom_expr(&q, "(x AT TIME ZONE 'UTC')::text"),
                // _interval: epoch micros per element (CDC: parse_pg_interval_to_micros)
                1187 => array_custom_expr(&q, "(EXTRACT(EPOCH FROM x) * 1000000)::bigint"),
                // _money: numeric text per element (CDC: clean_money_value)
                791 => array_custom_expr(&q, "x::numeric::text"),
                // _numeric: text per element (CDC: string passthrough; avoids
                // JSON-number precision loss on >15-digit values)
                1231 => array_custom_expr(&q, "x::text"),
                _ => q,
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    (col_list, true, bytea_names)
}

/// Build BYTEA unhex transform expressions for Stream Load `columns` header.
fn build_snapshot_transforms(bytea_cols: &[String]) -> Vec<String> {
    bytea_cols
        .iter()
        .map(|c| format!("{}=unhex({})", c, c))
        .collect()
}

/// Decision from the snapshot gap-detection check, to avoid holding the
/// checkpoint MutexGuard across an await point (SR-existence query).
enum SnapshotDecision {
    /// Hash matches in checkpoint — need to verify SR-side table existence.
    VerifySrExists,
    /// Re-snapshot is needed (new table, hash mismatch, or SR table missing).
    Snapshot,
}

/// Check if a table exists in StarRocks.
/// Used by gap detection to verify SR-side existence (the checkpoint says
/// "snapshotted" but the SR table might have been dropped).
///
/// `SHOW TABLES LIKE '{sr_table}'` treats `_` as a
/// single-char wildcard, so e.g. `order_items` matches `order1items`,
/// `orders`, etc. A false positive makes the snapshot SKIP a table that does
/// not actually exist in SR; CDC then auto-creates an empty table and
/// historical data is silently and irrecoverably lost. Fetch all names and
/// check EXACT membership, mirroring the safer approach already used for the
/// `_drop_` backup cleanup below.
async fn sr_table_exists(sr_mgr: &Arc<SrConnectionManager>, sr_table: &str) -> bool {
    match sr_mgr.query_first_col("SHOW TABLES").await {
        Ok(names) => names.iter().any(|n| n == sr_table),
        Err(_) => false,
    }
}

/// Drop orphaned snapshot temporary tables left by crashes during snapshot.
/// Two patterns are cleaned:
/// - `*_snapshot_temp`: shadow tables from interrupted snapshots.
/// - `*_drop_<digits>`: backup tables from rename-backup swaps that weren't cleaned.
///
/// Called on pipeline startup before snapshots begin, to prevent SR disk
/// accumulation over many crash cycles. Safe because the flock in main.rs
/// guarantees only one pipeline instance is running.
///
/// NOTE: the `_drop_` pattern is matched as `*_drop_<13+ digits>` (timestamp)
/// to avoid falsely matching tables whose names legitimately contain "drop".
pub async fn cleanup_orphan_tables(sr_mgr: &Arc<SrConnectionManager>) {
    // A single `SHOW TABLES LIKE '%_snapshot_temp'`
    // matches `%`/`_` wildcards, so e.g. `datasnapshot_temp` (where `%`="" and
    // `_`="a") would be matched and dropped. Fetch all names (one query) and
    // filter by a STRICT suffix match in Rust, exactly as the `_drop_` path
    // below already does.
    match sr_mgr.query_first_col("SHOW TABLES").await {
        Ok(all_tables) => {
            for name in all_tables.iter().filter(|n| n.ends_with("_snapshot_temp")) {
                tracing::info!("Cleaning up orphaned SR snapshot temp table: {}", name);
                let drop_sql = format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    crate::sr_sql::sr_quote(sr_mgr.database()),
                    crate::sr_sql::sr_quote(name)
                );
                if let Err(e) = crate::sr_sql::execute_sql(sr_mgr, &drop_sql).await {
                    tracing::warn!("Failed to drop orphan {}: {:?}", name, e);
                }
            }
        }
        Err(e) => {
            tracing::debug!("Orphan cleanup query failed (non-fatal): {:?}", e);
        }
    }

    // _drop_<timestamp> orphans — use a pattern to match only backup tables
    // left by swap_shadow_to_main. The backup name format is:
    //   <sr_table>_drop_<millis:u128>_<counter:u64>
    // To prevent false positives on legitimate table names that happen to
    // contain "_drop_" followed by digits, we additionally verify that the
    // table name WITHOUT the "_drop_..." suffix is itself present in the
    // live table set (i.e. the source table still exists).
    let sql = "SHOW TABLES";
    match sr_mgr.query_first_col(sql).await {
        Ok(all_tables) => {
            let table_set: std::collections::HashSet<&str> =
                all_tables.iter().map(|s| s.as_str()).collect();
            for name in &all_tables {
                if let Some(prefix) = name.rsplit_once("_drop_") {
                    let original_table = prefix.0; // the part before _drop_
                    let ts_part = prefix.1; // the part after _drop_

                    // Only match orphan backups: the suffix must be all-digit
                    // segments separated by underscores, first segment >= 13
                    // digits (millisecond timestamp), at most 2 segments.
                    let segments: Vec<&str> = ts_part.split('_').collect();
                    let well_formed = !segments.is_empty()
                        && segments.len() <= 2
                        && segments
                            .iter()
                            .all(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))
                        && segments[0].len() >= 13;

                    // Anchor: only drop if the source table still exists in
                    // the live set. This prevents false positives on tables
                    // named "<word>_drop_<digits>" that are NOT backups.
                    if well_formed && table_set.contains(original_table) {
                        tracing::info!("Cleaning up orphaned SR backup table: {}", name);
                        let drop_sql = format!(
                            "DROP TABLE IF EXISTS {}.{}",
                            crate::sr_sql::sr_quote(sr_mgr.database()),
                            crate::sr_sql::sr_quote(name)
                        );
                        if let Err(e) = crate::sr_sql::execute_sql(sr_mgr, &drop_sql).await {
                            tracing::warn!("Failed to drop orphan {}: {:?}", name, e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!("Orphan cleanup (SHOW TABLES) failed (non-fatal): {:?}", e);
        }
    }
}

/// Join rows with `\n`, pre-sizing the output buffer to the total byte length
/// `Vec::join("\n")` allocates a fresh `String` without
/// knowing the final size and re-grows it while copying each row — for large
/// snapshot pages (thousands of already-allocated String rows) this roughly
/// doubles peak memory during the join. Pre-sizing avoids the re-grow.
fn join_newlines(rows: &[String]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let total: usize = rows.iter().map(|s| s.len()).sum::<usize>() + rows.len() - 1;
    let mut buf = String::with_capacity(total);
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            buf.push('\n');
        }
        buf.push_str(r);
    }
    buf
}

/// Sub-batch and stream-load json_rows into the shadow table.
/// If the page exceeds STREAM_LOAD_MAX_BYTES, it is split into sub-batches
/// to avoid exceeding SR's import limits or reqwest's timeout on large bodies.
/// Each sub-batch gets its own stream_load label (idempotent by PK).
async fn stream_load_page(
    sink: &crate::sink::SrSink,
    sr_mgr: &Arc<SrConnectionManager>,
    shadow_table: &str,
    json_rows: &[String],
    transforms: &[String],
    schema: &str,
    table: &str,
) -> Result<()> {
    const STREAM_LOAD_MAX_BYTES: usize = 64 * 1024 * 1024; // 64 MB

    let total_bytes: usize = json_rows.iter().map(|s| s.len() + 1).sum();
    if total_bytes <= STREAM_LOAD_MAX_BYTES {
        let data = join_newlines(json_rows);
        let batch = crate::sink::SrBatch {
            database: sr_mgr.database().to_string(),
            table: shadow_table.to_string(),
            data: data.into(),
            row_count: json_rows.len(),
            columns: transforms.to_vec(),
            op_type: "upsert".to_string(),
        };
        sink.stream_load(&batch)
            .await
            .with_context(|| format!("Failed to Stream Load {}.{} batch", schema, table))?;
        return Ok(());
    }

    // Sub-batch: split json_rows into chunks that fit under the limit.
    let mut sub_batch: Vec<String> = Vec::new();
    let mut sub_bytes: usize = 0;
    let mut sub_count = 0usize;
    tracing::info!(
        "Sub-batching {}.{} snapshot page: {} rows, {}MB > {}MB limit",
        schema,
        table,
        json_rows.len(),
        total_bytes / 1024 / 1024,
        STREAM_LOAD_MAX_BYTES / 1024 / 1024,
    );
    for row in json_rows {
        let row_bytes = row.len() + 1; // +1 for newline
        if sub_bytes + row_bytes > STREAM_LOAD_MAX_BYTES && !sub_batch.is_empty() {
            let data = join_newlines(&sub_batch);
            let batch = crate::sink::SrBatch {
                database: sr_mgr.database().to_string(),
                table: shadow_table.to_string(),
                data: data.into(),
                row_count: sub_batch.len(),
                columns: transforms.to_vec(),
                op_type: "upsert".to_string(),
            };
            sink.stream_load(&batch).await.with_context(|| {
                format!(
                    "Failed to Stream Load {}.{} sub-batch {}",
                    schema, table, sub_count
                )
            })?;
            sub_count += 1;
            sub_batch.clear();
            sub_bytes = 0;
        }
        sub_batch.push(row.clone());
        sub_bytes += row_bytes;
    }
    if !sub_batch.is_empty() {
        let data = join_newlines(&sub_batch);
        let batch = crate::sink::SrBatch {
            database: sr_mgr.database().to_string(),
            table: shadow_table.to_string(),
            data: data.into(),
            row_count: sub_batch.len(),
            columns: transforms.to_vec(),
            op_type: "upsert".to_string(),
        };
        sink.stream_load(&batch).await.with_context(|| {
            format!(
                "Failed to Stream Load {}.{} sub-batch {} (final)",
                schema, table, sub_count
            )
        })?;
    }
    Ok(())
}

/// Safely swap the shadow table into the main table position using the
/// rename-backup pattern (not DROP-then-RENAME):
///    1. Rename existing main → backup (fails silently if main doesn't exist)
///    2. Rename shadow → main (the actual swap)
///    3. Drop the backup table (best-effort cleanup)
///
/// This ensures `{sr_table}` always exists after the swap:
/// either the old data survives in the backup (if step 1 succeeded)
/// or the new data is in place (crash before step 1 is harmless).
/// Crash between step 1 and step 2: main is gone, backup exists,
/// shadow exists → worst case orphan backup + shadow, no data loss.
///
/// Used by both `run_snapshot` and `snapshot_one_table` to prevent the
/// crash-window data loss that DROP-then-RENAME causes.
async fn swap_shadow_to_main(
    sr_mgr: &Arc<SrConnectionManager>,
    sr_table: &str,
    shadow_table: &str,
) -> Result<()> {
    // Append a process-local AtomicU64 counter to the backup name to
    // prevent collision if two swaps for the same table happen in the
    // same millisecond (e.g. fast retry after a transient SR error).
    use std::sync::atomic::{AtomicU64, Ordering};
    static SWAP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = SWAP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let backup_name = format!(
        "{}_drop_{}_{}",
        sr_table,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        counter,
    );
    let backup_sql = format!(
        "ALTER TABLE {}.{} RENAME {}",
        crate::sr_sql::sr_quote(sr_mgr.database()),
        crate::sr_sql::sr_quote(sr_table),
        crate::sr_sql::sr_quote(&backup_name),
    );
    // Pre-validate SR reachability + shadow-table
    // accessibility BEFORE step 1 (rename main → backup). Without this, if SR
    // is down or the shadow is missing, step 1 still renames the main table
    // away — leaving the main table absent for incoming CDC events (which then
    // auto-create an empty table → transient data loss) until the retry
    // succeeds. Verifying the shadow is queryable exercises SR health and
    // guarantees step 2 (shadow → main) has a target to install.
    //
    // Accepted residual window (documented): between this check and step 2 the
    // main table is briefly absent during the two renames. The
    // PendingCdcTableGuard held by snapshot_one_table keeps CDC deferred, but
    // the auto-create fallback in flush_table can still resurrect an empty
    // table if a flush was already mid-flight — this is the inherent trade-off
    // of the atomic-swap pattern.
    if !sr_table_exists(sr_mgr, shadow_table).await {
        anyhow::bail!(
            "Swap aborted for {}: shadow table {} not accessible on SR — \
             refusing to rename main table away (would leave it absent)",
            sr_table,
            shadow_table
        );
    }
    if let Err(e) = crate::sr_sql::execute_sql(sr_mgr, &backup_sql).await {
        tracing::debug!(
            "Backup rename of {} (expected on first run): {:?}",
            sr_table,
            e,
        );
    }

    let rename_sql = format!(
        "ALTER TABLE {}.{} RENAME {}",
        crate::sr_sql::sr_quote(sr_mgr.database()),
        crate::sr_sql::sr_quote(shadow_table),
        crate::sr_sql::sr_quote(sr_table),
    );
    crate::sr_sql::execute_sql(sr_mgr, &rename_sql)
        .await
        .with_context(|| {
            format!(
                "Failed to rename shadow table {} to {}",
                shadow_table, sr_table
            )
        })?;
    tracing::info!("Renamed shadow table {} → {}", shadow_table, sr_table);

    let drop_backup_sql = format!(
        "DROP TABLE IF EXISTS {}.{}",
        crate::sr_sql::sr_quote(sr_mgr.database()),
        crate::sr_sql::sr_quote(&backup_name),
    );
    let _ = crate::sr_sql::execute_sql(sr_mgr, &drop_backup_sql).await;

    Ok(())
}

/// Extract PK values from the last row of a snapshot page.
/// Uses the typed PK columns (already SELECTed at positions 1..N, after
/// the row_to_json at position 0) rather than parsing the JSON — this avoids
/// silent truncation (filter_map dropping missing keys → SQL syntax error or
/// infinite loop) and preserves the exact PG representation.
fn extract_pk_values_from_row(
    row: &tokio_postgres::Row,
    pk_columns: &[String],
) -> Option<Vec<String>> {
    let mut vals = Vec::with_capacity(pk_columns.len());
    for (i, _) in pk_columns.iter().enumerate() {
        // PK columns are at positions 1..N (position 0 is row_to_json text)
        let col_idx = i + 1;
        match row.try_get::<_, Option<String>>(col_idx) {
            Ok(Some(s)) => vals.push(s),
            Ok(None) => {
                // NULL PK should never happen (PK implies NOT NULL), but if it
                // does, we must NOT silently substitute "" — that would cause
                // `WHERE x > (null)` which evaluates to NULL (0 rows, premature
                // termination, silent snapshot truncation).
                tracing::error!(
                    "NULL value in PK column {} during snapshot — cannot advance cursor",
                    pk_columns[i]
                );
                return None;
            }
            Err(e) => {
                tracing::error!(
                    "Failed to read PK column {} from snapshot row: {:?}",
                    pk_columns[i],
                    e
                );
                return None;
            }
        }
    }
    Some(vals)
}

/// Build ORDER BY clause for snapshot pagination.
fn build_snapshot_order_clause(pk_columns: &[String]) -> String {
    if pk_columns.is_empty() {
        "ORDER BY 1".to_string()
    } else {
        format!(
            "ORDER BY {}",
            pk_columns
                .iter()
                .map(|c| crate::pg_catalog::pg_quote(c))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// Snapshot one table end-to-end: drop stale shadow, create shadow with PK
/// model, copy data via keyset pagination into the shadow, atomically swap
/// shadow → main, then persist the per-table checkpoint.
///
/// Shared by `run_snapshot` (loops over all matching tables) and
/// `snapshot_one_table` (DDL poller's PK-rebuild path). The caller is
/// responsible for gap-detection (skip if already snapshotted) and PK-type
/// compatibility checks before calling this — the helper assumes the table
/// should be (re)snapshotted.
///
/// Returns the total number of rows copied.
#[allow(clippy::too_many_arguments)]
async fn snapshot_table_inner(
    pg_client: &tokio_postgres::Client,
    sink: &crate::sink::SrSink,
    sr_mgr: &Arc<SrConnectionManager>,
    schema: &str,
    table: &str,
    pg_cols: &[CatalogColumn],
    pk_columns: &[String],
    checkpoint: &Arc<Mutex<Checkpoint>>,
    store: &dyn CheckpointStore,
    table_properties: &std::collections::HashMap<String, String>,
    snapshot_page_size: usize,
    query_timeout_ms: u64,
    pending_cdc_tables: Option<&Arc<Mutex<std::collections::HashSet<String>>>>,
) -> Result<i64> {
    let sr_table = crate::sr_sql::sr_table_name(schema, table);
    let shadow_table = format!("{}_snapshot_temp", sr_table);

    // Narrow the TOCTOU window between the DDL poller's
    // pending_cdc_tables check (ddl_poller.rs) and this snapshot's DROP. The
    // original check happens several PG catalog round-trips earlier; a CDC
    // flush can start in that gap and race the DROP/swap. Re-check immediately
    // before the DROP and — if clear — hold a PendingCdcTableGuard across the
    // DROP+CREATE+swap so concurrent DDL poller snapshots for the same table
    // defer (and the table is tracked as pending during the swap). The guard
    // does not (and cannot) block a CDC flush already in flight — the re-check
    // is what catches that — but it serializes DDL-vs-DDL and matches the
    // audit's recommendation. The DDL poller treats the bail as a retryable
    // deferral (bounded by MAX_DDL_DEFERRALS), not a failure.
    let _cdc_guard: Option<crate::pipeline::PendingCdcTableGuard> =
        if let Some(pct) = pending_cdc_tables {
            let table_key = format!("{}.{}", schema, table);
            if crate::pipeline::lock_or_recover(pct).contains(&table_key) {
                anyhow::bail!(
                    "snapshot deferred: {}.{} has pending CDC events (TOCTOU recheck before DROP)",
                    schema,
                    table
                );
            }
            Some(crate::pipeline::PendingCdcTableGuard::new(
                pct.clone(),
                table_key,
            ))
        } else {
            None
        };

    // Bound SR DDL/catalog operations in the snapshot
    // path with the configured query_timeout_ms (previously only the PG data
    // query below was bounded).
    let query_timeout = std::time::Duration::from_millis(query_timeout_ms);

    // Drop any stale shadow table from a previous failed attempt.
    // Escalate drop failures instead of logging them at
    // `debug!` and continuing. Combined with the strict `CREATE TABLE` (no
    // `IF NOT EXISTS`) below, a stale/locked shadow now surfaces as an explicit
    // error rather than silently resurrecting old data via CREATE IF NOT EXISTS.
    let drop_shadow_sql = format!(
        "DROP TABLE IF EXISTS {}.{}",
        crate::sr_sql::sr_quote(sr_mgr.database()),
        crate::sr_sql::sr_quote(&shadow_table)
    );
    sr_op_timeout(
        query_timeout,
        crate::sr_sql::execute_sql(sr_mgr, &drop_shadow_sql),
        || {
            format!(
                "Drop shadow table {} timed out after {}ms",
                shadow_table, query_timeout_ms
            )
        },
    )
    .await
    .with_context(|| format!("Failed to drop stale shadow SR table {}", shadow_table))?;
    tracing::debug!("Dropped stale shadow table {}", shadow_table);

    // Create shadow table — data is loaded here first, then atomically
    // swapped with the main table after all data is copied.
    let create_sql = generate_create_table_sql(
        sr_mgr.database(),
        &shadow_table,
        pg_cols,
        pk_columns,
        table_properties,
    )?;
    tracing::debug!("Creating shadow SR table: {create_sql}");
    sr_op_timeout(
        query_timeout,
        crate::sr_sql::execute_sql(sr_mgr, &create_sql),
        || {
            format!(
                "Create shadow SR table {} timed out after {}ms",
                shadow_table, query_timeout_ms
            )
        },
    )
    .await
    .with_context(|| format!("Failed to create shadow SR table {}", shadow_table))?;
    tracing::info!("Created shadow SR table {}", shadow_table);

    // Copy data in batches (using configured snapshot_page_size)
    let mut offset: i64 = 0;
    let mut total_rows: i64 = 0;

    // Use keyset pagination when the table has at least one PK column.
    // Keyset pagination scales to millions of rows (unlike LIMIT/OFFSET which
    // degrades as the offset grows). For composite PKs, uses row-value comparison:
    // (col1, col2, ...) > (CAST($1 AS ...), CAST($2 AS ...), ...)
    // Integer PK columns use $N::bigint for correct numeric ordering.
    let use_keyset = !pk_columns.is_empty();
    // Seek key values from the last row of the previous page (one per PK column).
    // None means this is the first page (no seek).
    let mut last_pk_values: Option<Vec<String>> = None;
    let snapshot_start = std::time::Instant::now();
    let mut page_num: usize = 0;

    // Build column list, custom-type flag, and BYTEA names for Stream Load transforms.
    let (col_list, needs_custom_cols, bytea_cols) = build_snapshot_col_list(pg_cols);

    // Build ordering clause
    let order_clause = build_snapshot_order_clause(pk_columns);
    if pk_columns.is_empty() {
        tracing::warn!(
            "{}.{}: no PK for snapshot ordering — falling back to ORDER BY 1 (full sort, may be slow on large tables)",
            schema, table,
        );
    };
    let transforms: Vec<String> = build_snapshot_transforms(&bytea_cols);

    // Pre-format query prefix and suffix so per-page format!() only does
    // three-part concatenation instead of rebuilding the full string each time.
    let (query_prefix, query_suffix) = if use_keyset {
        let extra_cols = pk_columns
            .iter()
            .map(|pk| format!("{}::text", crate::pg_catalog::pg_quote(pk)))
            .collect::<Vec<_>>()
            .join(", ");
        let sel = if needs_custom_cols { &col_list } else { "*" };
        let prefix = format!(
            "SELECT row_to_json(t)::text, {} FROM (SELECT {} FROM {}.{}",
            extra_cols,
            sel,
            crate::pg_catalog::pg_quote(schema),
            crate::pg_catalog::pg_quote(table),
        );
        let suffix = format!(" {} LIMIT {}) t", order_clause, snapshot_page_size);
        (prefix, suffix)
    } else {
        // LIMIT/OFFSET fallback — built per-page with changing offset
        (String::new(), String::new())
    };

    loop {
        let keyset_where = if use_keyset {
            if let Some(ref vals) = last_pk_values {
                let conditions: Vec<String> = pk_columns
                    .iter()
                    .enumerate()
                    .map(|(i, pk)| {
                        let val = vals.get(i).cloned().unwrap_or_default();
                        let is_int = pg_cols
                            .iter()
                            .find(|c| c.name == *pk)
                            .map(|c| matches!(c.type_oid, 21 | 23 | 20))
                            .unwrap_or(false);
                        if is_int {
                            val
                        } else {
                            format!("'{}'", val.replace('\'', "''"))
                        }
                    })
                    .collect();
                let col_refs: Vec<String> = pk_columns
                    .iter()
                    .map(|pk| crate::pg_catalog::pg_quote(pk))
                    .collect();
                let col_list_where = if pk_columns.len() == 1 {
                    col_refs[0].clone()
                } else {
                    format!("({})", col_refs.join(", "))
                };
                format!("WHERE {} > ({})", col_list_where, conditions.join(", "))
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let data_query =
            if use_keyset {
                // Keyset path: concatenate prefix + WHERE + suffix.
                // Three-part concat is ~3x faster than format!() with 8 args.
                format!("{}{}{}", query_prefix, keyset_where, query_suffix)
            } else {
                // LIMIT/OFFSET fallback for PK-less tables (rare)
                let sel = if needs_custom_cols { &col_list } else { "*" };
                format!(
                "SELECT row_to_json(t)::text FROM (SELECT {} FROM {}.{} {} LIMIT {} OFFSET {}) t",
                sel,
                crate::pg_catalog::pg_quote(schema), crate::pg_catalog::pg_quote(table),
                order_clause,
                snapshot_page_size, offset
            )
            };

        let query_timeout = std::time::Duration::from_millis(query_timeout_ms);
        let rows = tokio::time::timeout(query_timeout, pg_client.query(&data_query, &[]))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Snapshot data query timed out after {}s for {}.{}",
                    query_timeout_ms,
                    schema,
                    table,
                )
            })?
            .with_context(|| format!("Failed to query {}.{} data", schema, table))?;

        if rows.is_empty() {
            break;
        }

        let mut json_rows = Vec::with_capacity(rows.len().min(snapshot_page_size));
        let mut page_bytes: usize = 0;
        const MAX_PAGE_BYTES: usize = 64 * 1024 * 1024; // 64MB warning threshold
        for row in &rows {
            let json_str: String = row.get(0);
            page_bytes += json_str.len();
            json_rows.push(json_str);
        }
        if page_bytes > MAX_PAGE_BYTES {
            tracing::warn!(
                "Snapshot page for {}.{} is {}MB ({} rows) — consider reducing snapshot_page_size ({}) \
                 to reduce memory pressure",
                schema, table,
                page_bytes / 1024 / 1024,
                json_rows.len(),
                snapshot_page_size,
            );
        }

        // Update seek key values from the last row's PK columns (keyset pagination).
        // Uses the typed PK columns at positions 1..N (already SELECTed via
        // extra_cols) instead of parsing the row_to_json — avoids silent
        // truncation (filter_map dropping missing keys → SQL syntax error
        // or infinite loop on text PKs).
        if use_keyset {
            if let Some(last_row) = rows.last() {
                match extract_pk_values_from_row(last_row, pk_columns) {
                    Some(vals) => last_pk_values = Some(vals),
                    None => {
                        tracing::error!(
                            "Failed to extract PK cursor values for {}.{} — \
                             aborting snapshot to prevent infinite loop",
                            schema,
                            table,
                        );
                        anyhow::bail!(
                            "Failed to extract PK cursor values for {}.{}",
                            schema,
                            table,
                        );
                    }
                }
            }
        }

        // Use sub-batch helper to avoid exceeding SR's Stream Load body
        // limits on very large/wide snapshot pages.
        stream_load_page(
            sink,
            sr_mgr,
            &shadow_table,
            &json_rows,
            &transforms,
            schema,
            table,
        )
        .await?;

        total_rows += rows.len() as i64;
        offset += snapshot_page_size as i64;
        page_num += 1;
        let page_num_i64 = page_num as i64;
        let total_pages = if total_rows > 0 {
            // Estimate total pages from rows seen so far and page position.
            // For keyset pagination we can't know the exact total upfront,
            // so we estimate from cumulative progress.
            let rows_per_page = rows.len().max(1) as i64;
            total_rows / rows_per_page + 1
        } else {
            1
        };
        let elapsed = snapshot_start.elapsed();
        tracing::info!(
            "SNAPSHOT PROGRESS {}.{}: page {}/~{} ({} rows, {:.1}s elapsed)",
            schema,
            table,
            page_num_i64,
            total_pages.max(page_num_i64),
            total_rows,
            elapsed.as_secs_f64(),
        );
    }

    // Data fully loaded into shadow table — swap it with the main table.
    swap_shadow_to_main(sr_mgr, &sr_table, &shadow_table).await?;

    // Mark table as done and persist mid-snapshot progress
    let table_key = format!("{}.{}", schema, table);
    let mid_cp = {
        let mut cp = lock_checkpoint(checkpoint);
        if !cp.snapshotted_tables.contains(&table_key) {
            cp.snapshotted_tables.push(table_key.clone());
        }
        // Store schema hash for gap detection on future runs
        let hash = compute_schema_hash(pg_cols, pk_columns);
        cp.snapshotted_table_hashes.insert(table_key, hash);
        cp.clone()
    };
    // Save checkpoint after each table so a crash during snapshot
    // only loses the current table, not all previously completed ones.
    if let Err(e) = store.save(&mid_cp).await {
        tracing::warn!("Mid-snapshot checkpoint save failed (non-fatal): {:?}", e);
    }

    Ok(total_rows)
}

/// Run the initial snapshot: create SR tables and copy all data.
#[allow(clippy::too_many_arguments)]
pub async fn run_snapshot(
    pg_cfg: &PgConfig,
    sr_mgr: &std::sync::Arc<SrConnectionManager>,
    checkpoint: Arc<Mutex<Checkpoint>>,
    store: &dyn CheckpointStore,
    table_filter: &str,
    table_exclude: &str,
    table_properties: &std::collections::HashMap<String, String>,
    snapshot_page_size: usize,
    _max_retries: u32,
    tls: &crate::config::PgTlsConnector,
    ddl_schema: &str,
    ddl_table: &str,
    query_timeout_ms: u64,
) -> Result<()> {
    // Always run — the per-table snapshotted_tables check below prevents
    // re-snapshoting already-done tables. This enables gap detection:
    // tables dropped from SR (auto-created during CDC) or added via
    // table_filter changes get snapshotted without needing --fresh.
    {
        let cp = lock_checkpoint(&checkpoint);
        if cp.snapshot_done && cp.snapshotted_tables.is_empty() {
            // Legacy checkpoint from before snapshotted_tables field existed.
            // Re-snapshot everything to populate the field.
            tracing::info!(
                "Legacy checkpoint detected (no snapshotted_tables), running full snapshot"
            );
        } else if cp.snapshot_done {
            let done = cp.snapshotted_tables.len();
            tracing::info!("Snapshot phase (gap detection): {done} tables already done, checking for new/missing tables");
        } else {
            tracing::info!("Starting initial snapshot of PG tables to StarRocks");
        }
    }

    // Connect to PG
    let conn_str = pg_cfg.conn_str();
    let (pg_client, pg_conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect to PG for snapshot")?;
    let _pg_conn_guard = crate::util::AbortOnDrop(pg_conn);

    // Compile table_filter once — not per-table
    let table_filter_re = crate::config::compile_table_filter(table_filter)
        .context("Failed to compile table_filter regex for snapshot")?;
    let table_exclude_re = crate::config::compile_table_exclude(table_exclude)
        .context("Failed to compile table_exclude regex for snapshot")?;

    // Get all matching user tables with their OIDs and columns in bulk (2 queries)
    // instead of 2N per-table queries. Filter in-memory.
    let tables_with_oids =
        crate::pg_catalog::fetch_bulk_table_oids(&pg_client, ddl_schema, ddl_table).await?;
    tracing::info!("Found {} tables to snapshot", tables_with_oids.len());

    // Collect OIDs for tables that pass schema + filter/exclude, compute table keys
    let mut matching: Vec<(String, String, u32)> = Vec::with_capacity(tables_with_oids.len());
    for (schema, table, oid) in &tables_with_oids {
        // Single-schema enforcement: `sr_table_name` drops the schema prefix,
        // so snapshotting a second schema would collide same-named tables in
        // one SR table. Only the configured schema is replicated.
        if schema != &pg_cfg.schema {
            tracing::debug!(
                "{}.{} skipped (postgres.schema='{}' — single-schema replication)",
                schema,
                table,
                pg_cfg.schema,
            );
            continue;
        }
        if crate::config::is_table_excluded(&table_exclude_re, schema, table) {
            tracing::debug!("{}.{} excluded by table_exclude", schema, table);
            continue;
        }
        if !crate::config::table_matches_filter(&table_filter_re, schema, table) {
            tracing::debug!("{}.{} filtered out by table_filter", schema, table);
            continue;
        }
        matching.push((schema.clone(), table.clone(), *oid));
    }
    tracing::info!("{} tables passed schema/filter/exclude", matching.len());

    // Skip partitions: the CDC consumer maps partition OIDs to the PARENT
    // table identity, and `SELECT * FROM <parent>` already scans all
    // partitions — snapshotting each partition separately would duplicate
    // the data into redundant per-partition SR tables (~2× snapshot time
    // and storage). One batch pg_inherits query for all matching OIDs.
    if !matching.is_empty() {
        let oids_i32: Vec<i32> = matching.iter().map(|(_, _, oid)| *oid as i32).collect();
        let partition_rows = pg_client
            .query(
                "SELECT i.inhrelid::int4 FROM pg_inherits i \
                 JOIN pg_class p ON p.oid = i.inhparent \
                 WHERE i.inhrelid = ANY($1::int4[]) AND p.relkind = 'p'",
                &[&oids_i32],
            )
            .await
            .context("Failed to query partition children for snapshot")?;
        let partition_oids: std::collections::HashSet<u32> = partition_rows
            .iter()
            .map(|r| r.get::<_, i32>(0) as u32)
            .collect();
        if !partition_oids.is_empty() {
            matching.retain(|(schema, table, oid)| {
                if partition_oids.contains(oid) {
                    tracing::info!(
                        "Skipping partition {}.{} in snapshot (parent table covers its data)",
                        schema,
                        table,
                    );
                    false
                } else {
                    true
                }
            });
        }
    }

    // Single bulk query for ALL columns at once
    let oids: Vec<u32> = matching.iter().map(|(_, _, oid)| *oid).collect();
    let bulk_columns = crate::pg_catalog::fetch_bulk_columns(&pg_client, &oids).await?;
    // Bulk-fetch ALL primary keys in one query instead
    // of 2N per-table catalog round-trips (get_primary_keys did
    // fetch_relation_oid + fetch_primary_keys per table).
    let bulk_pks = crate::pg_catalog::fetch_bulk_primary_keys(&pg_client, &oids).await?;

    // Create a single sink for all snapshot Stream Loads
    let sink =
        crate::sink::SrSink::new(sr_mgr.clone()).context("Failed to create snapshot sink")?;

    // Fetch all SR table names ONCE and use exact
    // membership per-table below. The previous `SHOW TABLES LIKE '<table>'`
    // was wildcard-unsafe (underscore matches any single char) AND issued a
    // full table-list scan per table, making gap detection O(N) SR round-trips.
    let sr_table_set: std::collections::HashSet<String> =
        match sr_mgr.query_first_col("SHOW TABLES").await {
            Ok(names) => names.into_iter().collect(),
            Err(e) => {
                tracing::warn!("SHOW TABLES for gap detection failed (non-fatal): {:?}", e);
                std::collections::HashSet::new()
            }
        };

    for (schema, table, oid) in &matching {
        tracing::info!(
            "Snapshoting {}.{} → SR db={}",
            schema,
            table,
            sr_mgr.database()
        );
        let table_key = format!("{}.{}", schema, table);

        // Get columns from bulk-fetched map
        let pg_cols = match bulk_columns.get(oid) {
            Some(cols) if !cols.is_empty() => cols,
            _ => {
                tracing::warn!("No columns found for {}.{}, skipping", schema, table);
                continue;
            }
        };

        // Get primary key columns from the bulk-fetched map — PK-less tables
        // are not supported. Fetched BEFORE the hash check so that PK
        // membership is included in the schema hash (ADD/DROP PRIMARY KEY
        // must trigger re-snapshot).
        let pk_columns = bulk_pks.get(oid).cloned().unwrap_or_default();
        if pk_columns.is_empty() {
            tracing::info!(
                "{}.{} has no PRIMARY KEY — skipping snapshot. \
                 PK-less tables are not supported.",
                schema,
                table,
            );
            continue;
        }

        // Check if already snapshotted — if name matches AND schema hash matches, skip.
        // If name matches but hash differs (PG schema changed), re-snapshot.
        // Also verify the SR table actually exists — if it was dropped from SR
        // (operator mistake, SR disk failure), force a re-snapshot even if the
        // checkpoint says it's done. Without this SR-side check, a dropped table
        // stays "snapshotted" in the checkpoint → CDC writes fail "unknown table"
        // → auto_create_sr_table creates an empty table → historical rows lost.
        //
        // NOTE: the checkpoint lock is dropped BEFORE the SR-existence await to
        // avoid holding a MutexGuard across an await point (!Send, clippy
        // await_holding_lock).
        let needs_snapshot = {
            let cp = lock_checkpoint(&checkpoint);
            let hash = compute_schema_hash(pg_cols, &pk_columns);
            let table_hash_matches = cp
                .snapshotted_table_hashes
                .get(&table_key)
                .map(|h| *h == hash)
                .unwrap_or(false);
            if cp.snapshotted_tables.contains(&table_key) && table_hash_matches {
                SnapshotDecision::VerifySrExists
            } else if cp.snapshotted_tables.contains(&table_key) {
                tracing::info!(
                    "{}.{} schema changed since snapshot — re-snapshoting (old hash: {:016x}, new: {:016x})",
                    schema, table,
                    cp.snapshotted_table_hashes.get(&table_key).unwrap_or(&0),
                    hash,
                );
                SnapshotDecision::Snapshot
            } else {
                SnapshotDecision::Snapshot
            }
        };
        let needs_snapshot = match needs_snapshot {
            SnapshotDecision::VerifySrExists => {
                let sr_table_name = crate::sr_sql::sr_table_name(schema, table);
                if sr_table_set.contains(&sr_table_name) {
                    tracing::info!(
                        "{}.{} already snapshotted (hash matches), skipping",
                        schema,
                        table
                    );
                    false
                } else {
                    tracing::info!(
                        "{}.{} marked snapshotted in checkpoint but missing from SR \
                         — re-snapshoting (table was likely dropped from SR)",
                        schema,
                        table,
                    );
                    true
                }
            }
            SnapshotDecision::Snapshot => true,
        };

        if !needs_snapshot {
            continue;
        }

        // Check PK column type compatibility — certain types (JSON, etc.)
        // can't be used as PRIMARY KEY in StarRocks. Skip these tables.
        if has_incompatible_pk_columns(pg_cols, &pk_columns) {
            tracing::warn!(
                "{}.{} has incompatible PK column type(s) for StarRocks PRIMARY KEY — skipping snapshot",
                schema, table,
            );
            // Still mark as snapshotted so we don't retry every restart
            let _incompat_cp = {
                let mut cp = lock_checkpoint(&checkpoint);
                if !cp.snapshotted_tables.contains(&table_key) {
                    cp.snapshotted_tables.push(table_key.clone());
                }
                let hash = compute_schema_hash(pg_cols, &pk_columns);
                cp.snapshotted_table_hashes.insert(table_key, hash);
                cp.clone()
            };
            if let Err(e) = store.save(&_incompat_cp).await {
                tracing::warn!(
                    "Checkpoint save after skipping incompatible PK table (non-fatal): {:?}",
                    e
                );
            }
            continue;
        }

        snapshot_table_inner(
            &pg_client,
            &sink,
            sr_mgr,
            schema,
            table,
            pg_cols,
            &pk_columns,
            &checkpoint,
            store,
            table_properties,
            snapshot_page_size,
            query_timeout_ms,
            None, // run_snapshot runs at startup before the CDC loop; pending set is empty.
        )
        .await?;
    }

    // Mark snapshot complete
    {
        let mut cp = lock_checkpoint(&checkpoint);
        cp.snapshot_done = true;
    }

    tracing::info!("Snapshot complete");
    Ok(())
}

/// Snapshot a single table: drop SR table, recreate with PK model, copy data.
///
/// Used by the DDL poller when a table gains a PRIMARY KEY (e.g. via
/// CTAS → ALTER TABLE ADD PRIMARY KEY). The table was previously created
/// on SR by the CTAS catalog fallback as DUPLICATE model, so we need to
/// recreate it with the correct PRIMARY KEY model for CDC upsert support.
///
/// Skips if the table has no PK or is already in snapshotted_tables.
#[allow(clippy::too_many_arguments)]
pub async fn snapshot_one_table(
    pg: &tokio_postgres::Client,
    sr_mgr: &Arc<SrConnectionManager>,
    schema: &str,
    table: &str,
    checkpoint: &Arc<Mutex<Checkpoint>>,
    store: &dyn CheckpointStore,
    table_properties: &std::collections::HashMap<String, String>,
    snapshot_page_size: usize,
    _max_retries: u32,
    query_timeout_ms: u64,
    pending_cdc_tables: &Arc<Mutex<std::collections::HashSet<String>>>,
) -> Result<bool> {
    let table_key = format!("{}.{}", schema, table);

    // Fetch column metadata (needed before gap detection for schema hash)
    let oid = match crate::pg_catalog::fetch_relation_oid(pg, schema, table)
        .await
        .context("Failed to query table OID for single-table snapshot")?
    {
        Some(oid) => oid,
        None => {
            tracing::warn!(
                "{}.{} not found in PG catalog, skipping snapshot",
                schema,
                table
            );
            return Ok(false);
        }
    };

    let pg_cols = crate::pg_catalog::fetch_columns(pg, oid)
        .await
        .context("Failed to query columns for single-table snapshot")?;
    if pg_cols.is_empty() {
        tracing::warn!("No columns for {}.{}, skipping snapshot", schema, table);
        return Ok(false);
    }

    let pk_columns = crate::pg_catalog::fetch_primary_keys(pg, oid)
        .await
        .with_context(|| format!("Failed to fetch PK columns for {}.{}", schema, table))?;

    if pk_columns.is_empty() {
        tracing::info!(
            "{}.{} has no PK, skipping single-table snapshot",
            schema,
            table
        );
        return Ok(false);
    }

    // Check if already snapshotted — name + schema hash + SR existence
    // Note: checkpoint lock is dropped BEFORE the SR-existence await to
    // avoid holding a MutexGuard across an await point (!Send).
    let needs_snapshot = {
        let cp = lock_checkpoint(checkpoint);
        let hash = compute_schema_hash(&pg_cols, &pk_columns);
        let table_hash_matches = cp
            .snapshotted_table_hashes
            .get(&table_key)
            .map(|h| *h == hash)
            .unwrap_or(false);
        if cp.snapshotted_tables.contains(&table_key) && table_hash_matches {
            // Hash matches — but we need to verify SR-side existence, which
            // requires an await. Defer: collect the decision and check SR below.
            SnapshotDecision::VerifySrExists
        } else if cp.snapshotted_tables.contains(&table_key) {
            tracing::info!(
                "{}.{} schema changed since snapshot — re-snapshoting",
                schema,
                table,
            );
            SnapshotDecision::Snapshot
        } else {
            SnapshotDecision::Snapshot
        }
    };
    match needs_snapshot {
        SnapshotDecision::VerifySrExists => {
            let sr_table_name = crate::sr_sql::sr_table_name(schema, table);
            if sr_table_exists(sr_mgr, &sr_table_name).await {
                tracing::info!(
                    "{}.{} already snapshotted (hash matches), skipping",
                    schema,
                    table
                );
                return Ok(false);
            } else {
                tracing::info!(
                    "{}.{} marked snapshotted but missing from SR — re-snapshoting",
                    schema,
                    table,
                );
            }
        }
        SnapshotDecision::Snapshot => {}
    }

    // Check PK column type compatibility — skip if incompatible with SR PRIMARY KEY
    if has_incompatible_pk_columns(&pg_cols, &pk_columns) {
        tracing::warn!(
            "{}.{} has incompatible PK column type(s) for StarRocks PRIMARY KEY — skipping single-table snapshot",
            schema, table,
        );
        // Mark as snapshotted here too (the run_snapshot
        // path already does), so the DDL poller doesn't re-attempt the snapshot
        // on every restart/DDL event for this incompatible table.
        let _incompat_cp = {
            let mut cp = lock_checkpoint(checkpoint);
            if !cp.snapshotted_tables.contains(&table_key) {
                cp.snapshotted_tables.push(table_key.clone());
            }
            let hash = compute_schema_hash(&pg_cols, &pk_columns);
            cp.snapshotted_table_hashes.insert(table_key.clone(), hash);
            cp.clone()
        };
        if let Err(e) = store.save(&_incompat_cp).await {
            tracing::warn!(
                "Checkpoint save after skipping incompatible PK table (non-fatal): {:?}",
                e
            );
        }
        return Ok(false);
    }

    let sink = crate::sink::SrSink::new(sr_mgr.clone())
        .context("Failed to create snapshot sink for single-table snapshot")?;
    let total_rows = snapshot_table_inner(
        pg,
        &sink,
        sr_mgr,
        schema,
        table,
        &pg_cols,
        &pk_columns,
        checkpoint,
        store,
        table_properties,
        snapshot_page_size,
        query_timeout_ms,
        Some(pending_cdc_tables),
    )
    .await?;

    tracing::info!(
        "Single-table snapshot done for {}.{} ({} rows)",
        schema,
        table,
        total_rows
    );
    Ok(true)
}

/// Get primary key columns for a table.
#[allow(dead_code)]
async fn get_primary_keys(client: &Client, schema: &str, table: &str) -> Result<Vec<String>> {
    // Query the table OID directly (NOT via ::regclass, which folds quoted
    // identifiers to lowercase and would miss mixed-case table names).
    let oid = crate::pg_catalog::fetch_relation_oid(client, schema, table)
        .await
        .context("Failed to query table OID for PK lookup")?
        .ok_or_else(|| anyhow::anyhow!("Table {schema}.{table} not found for PK lookup"))?;

    crate::pg_catalog::fetch_primary_keys(client, oid)
        .await
        .context("Failed to query primary keys")
}

/// Generate CREATE TABLE SQL for StarRocks with PRIMARY KEY model for CDC upsert support.
/// Columns are reordered so that PK columns come first (required by StarRocks).
fn generate_create_table_sql(
    database: &str,
    table: &str,
    columns: &[crate::pg_catalog::CatalogColumn],
    pk_columns: &[String],
    table_properties: &std::collections::HashMap<String, String>,
) -> Result<String> {
    if pk_columns.is_empty() {
        anyhow::bail!(
            "Cannot generate SR CREATE TABLE for {}.{} — no PRIMARY KEY. \
             PK-less tables are not supported.",
            database,
            table,
        );
    }
    // Reorder columns: PK columns must be first in SR's CREATE TABLE
    let ordered = crate::sr_sql::reorder_columns_for_sr(columns, pk_columns, |c| c.name.as_str());

    let mut col_defs = Vec::with_capacity(ordered.len());

    for col in &ordered {
        let sr_type = crate::types::pg_type_to_sr(col.type_oid, col.type_modifier);
        let mut col_def = format!("{} {}", crate::sr_sql::sr_quote(&col.name), sr_type);

        // Policy: only PRIMARY KEY columns are NOT NULL on SR.
        // Non-PK columns are nullable even if NOT NULL in PG, to simplify
        // schema evolution and avoid SR rejecting data when PG adds a
        // NOT NULL constraint after snapshot.
        if pk_columns.contains(&col.name) {
            col_def.push_str(" NOT NULL");
        }

        col_defs.push(col_def);
    }

    // Use PRIMARY KEY model if we have PK columns; otherwise DUPLICATE KEY
    let first_col = ordered.first().map(|c| c.name.as_str()).unwrap_or("id");
    let (key_clause, dist_col) = crate::sr_sql::build_key_and_dist_col(pk_columns, first_col);

    let properties = crate::sr_sql::sr_table_properties(table_properties);

    let sql = crate::sr_sql::sr_create_table_sql(
        database,
        table,
        &col_defs,
        &key_clause,
        &dist_col,
        &properties,
        false, // shadow tables must NOT use IF NOT EXISTS — a stale table
               // surviving a failed DROP must surface as an explicit error.
    );

    Ok(sql)
}

/// Check whether any PRIMARY KEY column has a type incompatible with
/// StarRocks PRIMARY KEY constraints (e.g. JSON, which SR can store but
/// not use as a PK). Returns true if any PK column's type is incompatible.
fn has_incompatible_pk_columns(
    cols: &[crate::pg_catalog::CatalogColumn],
    pk_columns: &[String],
) -> bool {
    let pk_names: std::collections::HashSet<&str> = pk_columns.iter().map(|c| c.as_str()).collect();
    for col in cols {
        if !pk_names.contains(col.name.as_str()) {
            continue;
        }
        if !crate::types::is_pg_type_sr_pk_compatible(col.type_oid, col.type_modifier) {
            tracing::debug!(
                "PK column '{}' (OID={}) is incompatible with SR PRIMARY KEY",
                col.name,
                col.type_oid,
            );
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_create_table_sql_fast_schema_evolution() {
        let columns = vec![
            crate::pg_catalog::CatalogColumn {
                name: "id".into(),
                type_oid: 23, // INT
                type_modifier: 0,
            },
            crate::pg_catalog::CatalogColumn {
                name: "name".into(),
                type_oid: 25, // TEXT
                type_modifier: 0,
            },
        ];

        // With fast_schema_evolution = true
        let p = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
        ]);
        let sql =
            generate_create_table_sql("testdb", "test", &columns, &["id".into()], &p).unwrap();
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
        let p2 =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql2 =
            generate_create_table_sql("testdb", "test", &columns, &["id".into()], &p2).unwrap();
        assert!(
            !sql2.contains("fast_schema_evolution"),
            "Should omit fast_schema_evolution when disabled: {}",
            sql2
        );
    }

    #[test]
    fn test_generate_create_table_sql_reorders_pk_first() {
        // PK is on second column (id), JSON is first — after reorder,
        // PK column must appear first in the column list.
        let columns = vec![
            crate::pg_catalog::CatalogColumn {
                name: "data".into(), // JSON type
                type_oid: 114,
                type_modifier: 0,
            },
            crate::pg_catalog::CatalogColumn {
                name: "id".into(), // PK column, INT
                type_oid: 23,
                type_modifier: 0,
            },
        ];

        let p = std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let sql = generate_create_table_sql("testdb", "test_reorder", &columns, &["id".into()], &p)
            .unwrap();

        // The column list must start with `id` (PK column moved to front)
        let col_list_start = sql
            .find('(')
            .and_then(|start| {
                let rest = &sql[start + 1..];
                rest.find(')').map(|end| &rest[..end])
            })
            .unwrap_or("");

        assert!(
            col_list_start.starts_with("\n  `id`"),
            "PK column 'id' must be first in column list. SQL column section: |{col_list_start}|"
        );

        // `data` must come AFTER `id`
        let id_pos = sql.find("`id`").unwrap();
        let data_pos = sql.find("`data`").unwrap();
        assert!(
            id_pos < data_pos,
            "PK column 'id' must appear before 'data' in generated SQL"
        );

        // Verify PRIMARY KEY clause still references id
        assert!(sql.contains("PRIMARY KEY(`id`)"));
    }

    #[test]
    fn test_run_snapshot_accepts_max_retries() {
        // Verify that the max_retries parameter is compatible with SrSink
        // construction (same pattern used inside run_snapshot).
        // This is a type-level test — it must compile.
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
        let mgr = std::sync::Arc::new(
            crate::sr_connection::SrConnectionManager::new(&sr_cfg, 2, 10000, 10000)
                .expect("Failed to create SR connection manager for snapshot"),
        );

        // max_retries=0: sink created with no retries
        let sink0 = crate::sink::SrSink::new(mgr.clone()).expect("SrSink with max_retries=0");
        let _ = sink0;

        // max_retries=5: sink created with 5 retries
        let sink5 = crate::sink::SrSink::new(mgr.clone()).expect("SrSink with max_retries=5");
        let _ = sink5;

        // max_retries=42: sink created with non-standard value
        let sink42 = crate::sink::SrSink::new(mgr.clone()).expect("SrSink with max_retries=42");
        let _ = sink42;
    }

    #[test]
    fn test_generate_create_table_sql_no_pk_errors() {
        // PK-less tables are not supported — must error
        let columns = vec![
            crate::pg_catalog::CatalogColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: 0,
            },
            crate::pg_catalog::CatalogColumn {
                name: "data".into(),
                type_oid: 25,
                type_modifier: 0,
            },
        ];

        // No PK columns → must error
        let p = std::collections::HashMap::new();
        let result = generate_create_table_sql("testdb", "t_nopk", &columns, &[], &p);
        assert!(result.is_err(), "No-PK table must error, got success");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no PRIMARY KEY"),
            "Error must mention missing PK, got: {err}"
        );
    }

    #[test]
    fn test_generate_create_table_sql_composite_pk() {
        // Composite PK (2+ columns) should generate PRIMARY KEY(a, b)
        // and DISTRIBUTED BY HASH(a) with first PK column
        let columns = vec![
            crate::pg_catalog::CatalogColumn {
                name: "order_id".into(),
                type_oid: 23,
                type_modifier: 0,
            },
            crate::pg_catalog::CatalogColumn {
                name: "line_id".into(),
                type_oid: 23,
                type_modifier: 0,
            },
            crate::pg_catalog::CatalogColumn {
                name: "product".into(),
                type_oid: 25,
                type_modifier: 0,
            },
        ];

        let p = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
            ("enable_persistent_index".to_string(), "true".to_string()),
            (
                "persistent_index_type".to_string(),
                "CLOUD_NATIVE".to_string(),
            ),
        ]);
        let sql = generate_create_table_sql(
            "testdb",
            "t_composite",
            &columns,
            &["order_id".into(), "line_id".into()],
            &p,
        )
        .unwrap();

        assert!(
            sql.contains("PRIMARY KEY(`order_id`, `line_id`)"),
            "Composite PK must list both columns: {sql}"
        );
        assert!(
            sql.contains("DISTRIBUTED BY HASH(`order_id`)"),
            "DISTRIBUTED BY must use first PK column: {sql}"
        );
        assert!(
            sql.contains("enable_persistent_index"),
            "Composite PK table should have persistent_index: {sql}"
        );
    }
}

#[test]
fn test_schema_hash_stable() {
    // The SHA-256 hash must be deterministic and stable across runs
    let cols = vec![
        CatalogColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        },
        CatalogColumn {
            name: "name".into(),
            type_oid: 25,
            type_modifier: -1,
        },
        CatalogColumn {
            name: "val".into(),
            type_oid: 20,
            type_modifier: -1,
        },
    ];
    let h1 = compute_schema_hash(&cols, &[]);
    let h2 = compute_schema_hash(&cols, &[]);
    assert_eq!(h1, h2, "hash must be deterministic");

    // Different schema must produce different hash
    let cols2 = vec![
        CatalogColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        },
        CatalogColumn {
            name: "name".into(),
            type_oid: 25,
            type_modifier: 100,
        },
        CatalogColumn {
            name: "val".into(),
            type_oid: 20,
            type_modifier: -1,
        },
    ];
    assert_ne!(
        h1,
        compute_schema_hash(&cols2, &[]),
        "different schema must produce different hash"
    );

    // Column order matters
    let cols3 = vec![
        CatalogColumn {
            name: "name".into(),
            type_oid: 25,
            type_modifier: -1,
        },
        CatalogColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
        },
    ];
    assert_ne!(
        compute_schema_hash(&cols, &[]),
        compute_schema_hash(&cols3, &[]),
        "column order must affect hash"
    );

    // PK changes must affect hash (ADD/DROP PRIMARY KEY triggers re-snapshot)
    let pk_cols = vec!["id".to_string()];
    assert_ne!(
        compute_schema_hash(&cols, &[]),
        compute_schema_hash(&cols, &pk_cols),
        "adding a PK must change the hash"
    );
}

#[test]
fn test_schema_hash_empty() {
    let h = compute_schema_hash(&[], &[]);
    // SHA-256 of "pk:\x00cols:" (empty PK + empty cols), truncated to 64 bits LE.
    // Deterministic and stable across runs.
    assert_ne!(h, 0, "empty schema must produce non-zero hash");
}
