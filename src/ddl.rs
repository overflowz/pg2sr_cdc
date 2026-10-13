//! DDL capture — PG event trigger infrastructure for capturing schema changes.
//! Creates the `pg2sr_cdc_ddl_events` table and two event triggers
//! (`ddl_command_end` for CREATE/ALTER, `sql_drop` for DROP) that log all
//! DDL events for the DDL poller to consume and transpile to StarRocks.
use anyhow::{Context, Result};

use crate::config::{PgConfig, PgTlsConnector};

/// SQL to create the DDL capture table.
const DDL_CAPTURE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL NOT NULL,
    ddl TEXT NULL,
    tag VARCHAR(64) NULL,
    schema VARCHAR(64) DEFAULT CURRENT_SCHEMA NULL,
    user_name VARCHAR(64) DEFAULT CURRENT_USER NULL,
    txid VARCHAR(16) DEFAULT txid_current()::VARCHAR(16) NULL,
    db VARCHAR(64) DEFAULT current_database() NULL,
    client_addr VARCHAR(64) DEFAULT inet_client_addr() NULL,
    client_port INT4 DEFAULT inet_client_port() NULL,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT {table}_pkey PRIMARY KEY (id)
)"#;

/// SQL to create the event trigger function for DDL capture WITHOUT
/// publication management. Used when manage_publication = false — the
/// DBA manages the publication externally, so the trigger only logs DDL
/// events and doesn't attempt ALTER PUBLICATION.
const DDL_TRIGGER_FUNC_SQL_NO_PUB: &str = r#"
CREATE OR REPLACE FUNCTION {schema}.{func_ddl}()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    ddl_text text;
    schema_name text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE', 'SELECT INTO')
    LOOP
        ddl_text := current_query();
        IF ddl_text IS NULL THEN
            ddl_text := obj.command_tag || ' ' || obj.object_identity;
        END IF;

        schema_name := COALESCE(obj.schema_name, 'public');

        INSERT INTO {schema}.{table} (ddl, tag, schema)
        VALUES (ddl_text, obj.command_tag, schema_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;"#;

/// SQL to create the event trigger function for DROP capture.
/// Uses pg_event_trigger_dropped_objects() which only works in sql_drop triggers.
const DDL_DROP_TRIGGER_FUNC_SQL: &str = r#"
CREATE OR REPLACE FUNCTION {schema}.{func_drop}()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    ddl_text text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        WHERE object_type = 'table'
    LOOP
        -- Emit a single-target DROP TABLE IF EXISTS per dropped
        -- object instead of current_query(). current_query() returns the
        -- entire multi-target DROP statement (e.g. "DROP TABLE a, b, c")
        -- for ALL dropped objects in the trigger fire, so the poller's
        -- guard+drain coordination only protected the first table the
        -- DROP_TABLE_RE regex captured — tables b/c raced the SR DROP
        -- unguarded, and auto_create_sr_table resurrected empty phantoms.
        -- Per-row identity (obj.object_identity is schema-qualified) lets
        -- the poller guard+drain EACH table separately.
        ddl_text := 'DROP TABLE IF EXISTS ' || obj.object_identity;

        INSERT INTO {schema}.{table} (ddl, tag, schema)
        VALUES (ddl_text, 'DROP TABLE', COALESCE(obj.schema_name, 'public'));
    END LOOP;
END;
$$ LANGUAGE plpgsql;"#;

/// SQL to create the ddl_command_end event trigger (create/alter).
/// NOTE: on fresh setup we DROP first for a clean slate, but only if the
/// trigger is actually present. We check pg_event_trigger before issuing
/// DROP (rather than `DROP EVENT TRIGGER IF EXISTS`) because the latter
/// emits a NOTICE through tokio_postgres that surfaces as a confusing log
/// line: "event trigger pg2sr_cdc_ddl_trigger does not exist, skipping".
/// This is the same `DO $$ ... $$` pattern as ENSURE_DDL_TRIGGER_SQL but
/// with a conditional DROP so stale triggers get cleaned up on --fresh.
const DDL_TRIGGER_SQL: &str = r#"
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '{evt_ddl}') THEN
        DROP EVENT TRIGGER {evt_ddl};
    END IF;
    CREATE EVENT TRIGGER {evt_ddl}
        ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE', 'SELECT INTO')
        EXECUTE FUNCTION {schema}.{func_ddl}();
END;
$$;"#;

/// SQL to create the sql_drop event trigger (drop table).
/// NOTE: on fresh setup we DROP first for a clean slate, but only if the
/// trigger is actually present (see DDL_TRIGGER_SQL comment for rationale).
const DDL_DROP_TRIGGER_SQL: &str = r#"
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '{evt_drop}') THEN
        DROP EVENT TRIGGER {evt_drop};
    END IF;
    CREATE EVENT TRIGGER {evt_drop}
        ON sql_drop
        WHEN TAG IN ('DROP TABLE', 'DROP SCHEMA', 'DROP OWNED')
        EXECUTE FUNCTION {schema}.{func_drop}();
END;
$$;"#;

/// SQL to ensure the ddl_command_end event trigger exists without dropping it.
/// Uses a DO block to check pg_event_trigger before creating.
const ENSURE_DDL_TRIGGER_SQL: &str = r#"
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '{evt_ddl}') THEN
        CREATE EVENT TRIGGER {evt_ddl}
            ON ddl_command_end
            WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE', 'SELECT INTO')
            EXECUTE FUNCTION {schema}.{func_ddl}();
    END IF;
END;
$$;"#;

/// SQL to ensure the sql_drop event trigger exists without dropping it.
/// Uses a DO block to check pg_event_trigger before creating.
const ENSURE_DDL_DROP_TRIGGER_SQL: &str = r#"
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = '{evt_drop}') THEN
        CREATE EVENT TRIGGER {evt_drop}
            ON sql_drop
            WHEN TAG IN ('DROP TABLE', 'DROP SCHEMA', 'DROP OWNED')
            EXECUTE FUNCTION {schema}.{func_drop}();
    END IF;
END;
$$;"#;

/// Fixed DDL capture infrastructure object names.
///
/// Single-DB-per-pipeline design: the PG database owns a single set of DDL
/// capture objects. Multiple pipelines sharing one DB are not supported
/// because PG event triggers are global database-level objects — a trigger
/// fires for *every* DDL across the whole DB, so pipeline-scoped naming only
/// avoided `CREATE EVENT TRIGGER` collisions while still allowing global
/// capture into a shared table (data collision). The previous design derived
/// these names from `pipeline_name`; it has been replaced with fixed names
/// that match the default table name `public.pg2sr_cdc_ddl_events`.
const FUNC_DDL_NAME: &str = "pg2sr_cdc_ddl_func";
const FUNC_DROP_NAME: &str = "pg2sr_cdc_ddl_drop_func";
const EVT_DDL_NAME: &str = "pg2sr_cdc_ddl_trigger";
const EVT_DROP_NAME: &str = "pg2sr_cdc_drop_trigger";

/// Teardown DDL capture infrastructure — drops the capture table, event triggers,
/// and trigger functions. Called on --fresh to ensure a clean slate.
/// All failures are fatal: --fresh requires a clean infrastructure.
pub async fn teardown_ddl_capture(
    pg_cfg: &PgConfig,
    ddl_schema: &str,
    ddl_table: &str,
    tls: &PgTlsConnector,
) -> Result<()> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for DDL teardown")?;
    let _conn_guard = crate::util::AbortOnDrop(conn);

    // Fixed DDL capture infrastructure names (single-DB-per-pipeline).
    let func_ddl = FUNC_DDL_NAME;
    let func_drop = FUNC_DROP_NAME;
    let evt_ddl = EVT_DDL_NAME;
    let evt_drop = EVT_DROP_NAME;

    // ── Step 1: Drop event triggers first ──
    // Must drop event triggers BEFORE the table. Otherwise DROP TABLE triggers
    // the sql_drop event trigger, which fires the drop function attempting to
    // INSERT into the very table being dropped — causing the Aurora "relation
    // does not exist" quirk (table is in a deleting state, INSERT fails).
    for stmt in [
        format!("DROP EVENT TRIGGER IF EXISTS {evt_ddl}"),
        format!("DROP EVENT TRIGGER IF EXISTS {evt_drop}"),
    ] {
        if let Err(e) = client.execute(&stmt, &[]).await {
            tracing::error!("DDL teardown failed (--fresh requires clean infrastructure): {e:#}");
            return Err(e).context(format!("Failed to drop DDL infrastructure object: {stmt}"));
        }
    }

    // ── Step 2: Drop the DDL capture table ──
    // Now that event triggers are gone, DROP TABLE won't fire any triggers.
    // Use a two-step approach (pg_class check + bare DROP TABLE) instead of
    // DROP TABLE IF EXISTS because Aurora PG can error with "does not exist"
    // even with IF EXISTS when the table is in a broken state.
    let check_sql = "SELECT 1 FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'"
        .to_string();
    let table_in_catalog = client
        .query_opt(&check_sql, &[&ddl_schema, &ddl_table])
        .await
        .context("Failed to check DDL capture table existence during teardown")?
        .is_some();

    if table_in_catalog {
        let drop_sql = format!("DROP TABLE {ddl_schema}.{ddl_table}");
        client
            .execute(&drop_sql, &[])
            .await
            .inspect_err(|e| {
                let pg_msg = error_source_chain(e);
                tracing::error!(
                    "DDL teardown: table {ddl_schema}.{ddl_table} found in pg_class but \
                     DROP TABLE failed: {pg_msg} — this should not happen with triggers \
                     already dropped"
                );
            })
            .context(format!(
                "Failed to drop existing DDL capture table {ddl_schema}.{ddl_table} \
                 (table confirmed present in pg_class/pg_namespace — drop failure is fatal)"
            ))?;
        tracing::info!(
            "Dropped existing DDL capture table {}.{}",
            ddl_schema,
            ddl_table
        );
    }

    // ── Step 3: Drop trigger functions with CASCADE ──
    // The table is already gone from Step 2, so CASCADE won't cascade to it.
    for stmt in [
        format!("DROP FUNCTION IF EXISTS {ddl_schema}.{func_ddl}() CASCADE"),
        format!("DROP FUNCTION IF EXISTS {ddl_schema}.{func_drop}() CASCADE"),
    ] {
        if let Err(e) = client.execute(&stmt, &[]).await {
            tracing::error!("DDL teardown failed (--fresh requires clean infrastructure): {e:#}");
            return Err(e).context(format!("Failed to drop DDL infrastructure object: {stmt}"));
        }
    }

    tracing::info!("DDL capture infrastructure cleaned");
    Ok(())
}

/// Set up DDL capture infrastructure in PostgreSQL from scratch (for --fresh).
/// teardown_ddl_capture must have run first — this function expects a clean slate.
/// Creates the table, function, and event triggers.
pub async fn create_ddl_capture(
    pg_cfg: &PgConfig,
    ddl_schema: &str,
    ddl_table: &str,
    tls: &PgTlsConnector,
) -> Result<()> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for DDL create")?;
    let _conn_guard = crate::util::AbortOnDrop(conn);

    // Fixed DDL capture infrastructure names (single-DB-per-pipeline).
    let func_ddl = FUNC_DDL_NAME;
    let func_drop = FUNC_DROP_NAME;
    let evt_ddl = EVT_DDL_NAME;
    let evt_drop = EVT_DROP_NAME;

    let create_table_sql = DDL_CAPTURE_TABLE_SQL
        .replace("{schema}", ddl_schema)
        .replace("{table}", ddl_table);

    // Diagnose connection context
    match client
        .query_one(
            "SELECT current_database(), current_schema, current_user",
            &[],
        )
        .await
    {
        Ok(row) => {
            let db: String = row.get(0);
            let schema: String = row.get(1);
            let user: String = row.get(2);
            tracing::info!(
                "Connected as {} to database {} (search_path schema={})",
                user,
                db,
                schema,
            );
        }
        Err(e) => tracing::warn!("Failed to query connection context: {e:#}"),
    }

    // Safety: teardown_ddl_capture should have dropped this table.
    // If it still exists in pg_class, teardown failed to clean it — fatal.
    let check_sql = "SELECT 1 FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'"
        .to_string();
    let table_still_exists = client
        .query_opt(&check_sql, &[&ddl_schema, &ddl_table])
        .await
        .context("Failed to check DDL capture table existence before create")?
        .is_some();
    if table_still_exists {
        // Broader search to help diagnose schema mismatches
        if let Ok(rows) = client
            .query(
                "SELECT n.nspname FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname = $1 AND c.relkind = 'r'",
                &[&ddl_table],
            )
            .await
        {
            for row in &rows {
                let schema: String = row.get(0);
                tracing::error!(
                    "DDL capture table '{}' found in schema '{}' — expected it to be gone after teardown",
                    ddl_table, schema,
                );
            }
        }
        return Err(anyhow::anyhow!(
            "DDL capture table {}.{} still exists after teardown — \
             --fresh requires a clean slate. The table was not dropped during teardown. \
             Check the teardown logs above for why DROP TABLE failed.",
            ddl_schema,
            ddl_table
        ));
    }

    tracing::info!("Creating DDL capture table {}.{}", ddl_schema, ddl_table);
    client
        .execute(&create_table_sql, &[])
        .await
        .context("Failed to create DDL capture table")?;

    let func_sql = DDL_TRIGGER_FUNC_SQL_NO_PUB
        .replace("{schema}", ddl_schema)
        .replace("{table}", ddl_table)
        .replace("{func_ddl}", func_ddl);

    tracing::info!("Creating DDL trigger function");
    client
        .batch_execute(&func_sql)
        .await
        .context("Failed to create DDL trigger function")?;

    // Create event trigger (separate execution)
    let trigger_sql = DDL_TRIGGER_SQL
        .replace("{schema}", ddl_schema)
        .replace("{func_ddl}", func_ddl)
        .replace("{evt_ddl}", evt_ddl);

    tracing::info!("Creating DDL event trigger (ddl_command_end)");
    client
        .batch_execute(&trigger_sql)
        .await
        .context("Failed to create DDL event trigger")?;

    // Create the sql_drop event trigger function and trigger for DROP TABLE
    let drop_func_sql = DDL_DROP_TRIGGER_FUNC_SQL
        .replace("{schema}", ddl_schema)
        .replace("{table}", ddl_table)
        .replace("{func_drop}", func_drop);

    tracing::info!("Creating DDL drop trigger function");
    client
        .batch_execute(&drop_func_sql)
        .await
        .context("Failed to create DDL drop trigger function")?;

    let drop_trigger_sql = DDL_DROP_TRIGGER_SQL
        .replace("{schema}", ddl_schema)
        .replace("{func_drop}", func_drop)
        .replace("{evt_drop}", evt_drop);

    tracing::info!("Creating DDL drop event trigger (sql_drop)");
    client
        .batch_execute(&drop_trigger_sql)
        .await
        .context("Failed to create DDL drop event trigger")?;

    tracing::info!("DDL capture infrastructure setup complete (fresh)");
    Ok(())
}

/// Ensure DDL capture infrastructure exists without dropping/recreating.
/// Safe to call repeatedly — uses IF NOT EXISTS / conditional checks.
/// This is the non-fresh path: it will not drop any existing triggers or tables,
/// preserving all captured DDL events across restarts.
pub async fn ensure_ddl_capture(
    pg_cfg: &PgConfig,
    ddl_schema: &str,
    ddl_table: &str,
    tls: &PgTlsConnector,
) -> Result<()> {
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for DDL ensure")?;
    let _conn_guard = crate::util::AbortOnDrop(conn);

    // Fixed DDL capture infrastructure names (single-DB-per-pipeline).
    let func_ddl = FUNC_DDL_NAME;
    let func_drop = FUNC_DROP_NAME;
    let evt_ddl = EVT_DDL_NAME;
    let evt_drop = EVT_DROP_NAME;

    // Create DDL capture table if it doesn't exist.
    // We first check pg_class ourselves rather than relying on
    // `CREATE TABLE IF NOT EXISTS`'s no-op log: PG emits a NOTICE
    // ("relation X already exists, skipping") that surfaces through
    // tokio_postgres's notice handler as a confusing INFO log line on
    // every DDL poller restart. Skipping the CREATE entirely when the
    // table already exists silences that noise while preserving
    // first-time creation semantics.
    let check_table_sql = "SELECT 1 FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'"
        .to_string();
    let table_exists = client
        .query_opt(&check_table_sql, &[&ddl_schema, &ddl_table])
        .await
        .map(|r| r.is_some())
        .unwrap_or(false);
    if table_exists {
        tracing::debug!(
            "DDL capture table {}.{} already exists — skipping CREATE",
            ddl_schema,
            ddl_table,
        );
    } else {
        let create_table_sql = DDL_CAPTURE_TABLE_SQL
            .replace("{schema}", ddl_schema)
            .replace("{table}", ddl_table);
        tracing::info!("Ensuring DDL capture table {}.{}", ddl_schema, ddl_table);
        client
            .execute(&create_table_sql, &[])
            .await
            .context("Failed to ensure DDL capture table")?;
    }

    // Schema migration: ALTER TABLE ADD COLUMN IF NOT EXISTS for any columns
    // missing from an older pipeline version's capture table. CREATE TABLE
    // IF NOT EXISTS is a no-op on existing tables, so if a table from an
    // older version (without `txid`, `client_addr`, etc.) exists, the old
    // schema is left untouched without this migration step.
    let expected_columns = [
        ("ddl", "TEXT"),
        ("tag", "VARCHAR(64)"),
        ("schema", "VARCHAR(64)"),
        ("user_name", "VARCHAR(64)"),
        ("txid", "VARCHAR(16)"),
        ("db", "VARCHAR(64)"),
        ("client_addr", "VARCHAR(64)"),
        ("client_port", "INT4"),
        ("event_time", "TIMESTAMP"),
    ];
    for (col_name, col_type) in &expected_columns {
        let check_sql = "SELECT 1 FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 AND column_name = $3";
        let exists = client
            .query_opt(check_sql, &[&ddl_schema, &ddl_table, &col_name])
            .await
            .map(|r| r.is_some())
            .unwrap_or(false);
        if !exists {
            let alter_sql = format!(
                "ALTER TABLE {}.{table} ADD COLUMN IF NOT EXISTS {col} {typ}",
                crate::pg_catalog::pg_quote(ddl_schema),
                table = crate::pg_catalog::pg_quote(ddl_table),
                col = crate::pg_catalog::pg_quote(col_name),
                typ = col_type,
            );
            tracing::info!("DDL capture: adding missing column {} ({})", col_name, col_type);
            if let Err(e) = client.execute(&alter_sql, &[]).await {
                tracing::warn!(
                    "DDL capture: failed to add column {} to {}.{}: {:?}",
                    col_name, ddl_schema, ddl_table, e
                );
            }
        }
    }

    // Check if the trigger functions already exist. If they do, skip
    // function creation — the DBA pre-configured them.
    // This avoids permission errors when the pipeline user is not a
    // superuser/owner of the functions (production setup).
    //
    // NOTE: this check gates FUNCTION creation only. Event trigger creation
    // (below) is checked independently in pg_event_trigger — functions and
    // triggers have independent lifecycles, and a trigger dropped by a DBA
    // (or lost in a partial teardown/restore) while the functions survive
    // would otherwise silently disable DDL capture forever.
    let funcs_exist: bool = {
        let sql = "SELECT COUNT(*) = 2 FROM pg_proc p \
             JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = $1 \
             AND p.proname IN ($2, $3)"
            .to_string();
        client
            .query_one(&sql, &[&ddl_schema, &func_ddl, &func_drop])
            .await
            .map(|row| row.get::<_, bool>(0))
            .unwrap_or(false)
    };

    if funcs_exist {
        tracing::info!(
            "DDL trigger functions already exist in schema '{ddl_schema}' — skipping function setup (triggers still ensured below)"
        );
    } else {
        // Create the DDL trigger function.
        // This runs only on first-time setup or if infra was torn down.
        // On production with a pre-configured DBA, this block is skipped.
        let func_sql = DDL_TRIGGER_FUNC_SQL_NO_PUB
            .replace("{schema}", ddl_schema)
            .replace("{table}", ddl_table)
            .replace("{func_ddl}", func_ddl);

        tracing::info!("Ensuring DDL trigger function");
        client
            .batch_execute(&func_sql)
            .await
            .context("Failed to ensure DDL trigger function")?;
    }

    if !funcs_exist {
        // Create or replace drop trigger function (only when we own the
        // functions — DBA-managed functions are left untouched, see above).
        let drop_func_sql = DDL_DROP_TRIGGER_FUNC_SQL
            .replace("{schema}", ddl_schema)
            .replace("{table}", ddl_table)
            .replace("{func_drop}", func_drop);

        tracing::info!("Ensuring DDL drop trigger function");
        client
            .batch_execute(&drop_func_sql)
            .await
            .context("Failed to ensure DDL drop trigger function")?;
    }

    // Create event triggers only if they don't exist — checked independently
    // of the functions (pg_event_trigger), so a trigger dropped while the
    // functions survived is re-created here instead of silently disabling
    // DDL capture. Do NOT drop first: DROP EVENT TRIGGER can cause phantom
    // rows in the capture table on some PG versions, and we must never
    // destroy DDL infrastructure on restart.
    let ensure_trigger_sql = ENSURE_DDL_TRIGGER_SQL
        .replace("{schema}", ddl_schema)
        .replace("{func_ddl}", func_ddl)
        .replace("{evt_ddl}", evt_ddl);

    tracing::info!("Ensuring DDL event trigger (ddl_command_end)");
    client
        .execute(&ensure_trigger_sql, &[])
        .await
        .context("Failed to ensure DDL event trigger")?;

    // Create drop event trigger only if it doesn't exist
    let ensure_drop_trigger_sql = ENSURE_DDL_DROP_TRIGGER_SQL
        .replace("{schema}", ddl_schema)
        .replace("{func_drop}", func_drop)
        .replace("{evt_drop}", evt_drop);

    tracing::info!("Ensuring DDL drop event trigger (sql_drop)");
    client
        .execute(&ensure_drop_trigger_sql, &[])
        .await
        .context("Failed to ensure DDL drop event trigger")?;

    tracing::info!("DDL capture infrastructure ensure complete");
    Ok(())
}

/// Walk the error source chain to find the actual PG error message.
/// tokio_postgres::Error's Display only shows "db error" for Db variants —
/// the real PG message (e.g. "relation ... does not exist") is in the source.
fn error_source_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut current: &dyn std::error::Error = err;
    loop {
        let msg = format!("{current}");
        if !msg.is_empty() {
            parts.push(msg);
        }
        match current.source() {
            Some(src) => current = src,
            None => break,
        }
    }
    parts.join(": ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── SQL constants contain expected markers ──────────────────────────

    #[test]
    fn test_ddl_trigger_sql_has_schema_table_placeholders() {
        assert!(DDL_CAPTURE_TABLE_SQL.contains("{schema}"));
        assert!(DDL_CAPTURE_TABLE_SQL.contains("{table}"));
    }

    #[test]
    fn test_create_sql_has_pkey() {
        assert!(DDL_CAPTURE_TABLE_SQL.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_create_sql_uses_if_not_exists() {
        assert!(DDL_CAPTURE_TABLE_SQL.contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_ensure_trigger_sql_is_non_destructive() {
        // Must NOT contain DROP EVENT TRIGGER
        assert!(!ENSURE_DDL_TRIGGER_SQL.contains("DROP EVENT TRIGGER"));
        assert!(!ENSURE_DDL_DROP_TRIGGER_SQL.contains("DROP EVENT TRIGGER"));
        // Must NOT contain DROP TABLE command (the tag 'DROP TABLE' inside
        // WHEN TAG IN is a PG construct, not a command — accept that)
        assert!(!ENSURE_DDL_TRIGGER_SQL.contains("DROP TABLE"));
        // Must check pg_event_trigger before creating
        assert!(ENSURE_DDL_TRIGGER_SQL
            .contains("IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname"));
        assert!(ENSURE_DDL_DROP_TRIGGER_SQL
            .contains("IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname"));
    }

    #[test]
    fn test_ensure_trigger_sql_has_schema_placeholder() {
        assert!(ENSURE_DDL_TRIGGER_SQL.contains("{schema}"));
        assert!(ENSURE_DDL_DROP_TRIGGER_SQL.contains("{schema}"));
    }

    #[test]
    fn test_ensure_trigger_is_do_block() {
        assert!(ENSURE_DDL_TRIGGER_SQL.trim().starts_with("DO $$"));
        assert!(ENSURE_DDL_DROP_TRIGGER_SQL.trim().starts_with("DO $$"));
    }

    // ── manage_publication = false: no-pub trigger ─────────────────────

    #[test]
    fn test_no_pub_trigger_has_no_publication_management() {
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("ALTER PUBLICATION"));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("CREATE PUBLICATION"));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("{publication}"));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("tbl_identity"));
    }

    /// The pipeline's DDL event trigger does NOT manage REPLICA IDENTITY.
    ///
    /// PG's default `relreplident = 'd'` (DEFAULT) on a PK table is sufficient:
    /// the old tuple carries the PK columns, which is all the pipeline needs to
    /// emit a DELETE or detect a PK-change. The pipeline only replicates PK
    /// tables — no-PK tables are skipped by the DDL poller and never snapshotted,
    /// so their replica identity is irrelevant.
    ///
    /// Historically an external init script (init/02-replica-identity.sql in the
    /// parent repo, replicated here as init/01-pg2sr_cdc-setup.sql sections 4-5)
    /// set `REPLICA IDENTITY FULL` on no-PK tables via a `trg_replica_identity_*`
    /// event trigger. That logic has been removed: it caused needless WAL bloat
    /// (every UPDATE/DELETE carried the full row image) with no benefit, since
    /// those tables were never replicated.
    ///
    /// Guard rail: if a future contributor adds `ALTER TABLE ... REPLICA IDENTITY`
    /// to DDL_TRIGGER_FUNC_SQL_NO_PUB, this test will catch it. The pipeline's
    /// DDL trigger should only persist DDL events to the capture table —
    /// publication membership is handled separately (FOR ALL TABLES publication
    /// set up by reset_replication_slot/ensure_replication_slot in main.rs), and
    /// replica identity should remain the DBA's responsibility.
    #[test]
    fn test_no_pub_trigger_does_not_manage_replica_identity() {
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("REPLICA IDENTITY"));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("relreplident"));
    }

    #[test]
    fn test_no_pub_trigger_still_has_ddl_logging() {
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("INSERT INTO"));
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("ddl_text"));
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("{schema}"));
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("{table}"));
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains(
            "command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE', 'SELECT INTO')"
        ));
    }

    #[test]
    fn test_no_pub_trigger_has_null_fallback() {
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("IF ddl_text IS NULL THEN"));
        assert!(
            DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("obj.command_tag || ' ' || obj.object_identity")
        );
    }

    // ── Mixed-case / quoted identifier support ─────────────────────

    #[test]
    fn test_no_pub_trigger_uses_obj_schema_name() {
        // The no-pub variant must also use obj.schema_name for correct
        // schema extraction with quoted/mixed-case identifiers.
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("COALESCE(obj.schema_name, 'public')"));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("split_part(obj.object_identity"));
    }

    // ── DDL capture infrastructure names (single-DB-per-pipeline) ─────

    /// SQL templates use `{func_ddl}` / `{func_drop}` / `{evt_ddl}` / `{evt_drop}`
    /// placeholders, substituted at runtime with the fixed constant names.
    #[test]
    fn test_ddl_sql_has_name_placeholders() {
        assert!(DDL_TRIGGER_FUNC_SQL_NO_PUB.contains("{func_ddl}"));
        assert!(DDL_DROP_TRIGGER_FUNC_SQL.contains("{func_drop}"));
        assert!(DDL_TRIGGER_SQL.contains("{func_ddl}"));
        assert!(DDL_TRIGGER_SQL.contains("{evt_ddl}"));
        assert!(DDL_DROP_TRIGGER_SQL.contains("{func_drop}"));
        assert!(DDL_DROP_TRIGGER_SQL.contains("{evt_drop}"));
    }

    #[test]
    fn test_ensure_ddl_sql_has_name_placeholders() {
        assert!(ENSURE_DDL_TRIGGER_SQL.contains("{func_ddl}"));
        assert!(ENSURE_DDL_TRIGGER_SQL.contains("{evt_ddl}"));
        assert!(ENSURE_DDL_DROP_TRIGGER_SQL.contains("{func_drop}"));
        assert!(ENSURE_DDL_DROP_TRIGGER_SQL.contains("{evt_drop}"));
    }

    /// SQL templates must NOT contain hardcoded trigger names — substitution is
    /// done at runtime from the constants below.
    #[test]
    fn test_ddl_sql_templates_no_hardcoded_names() {
        assert!(!DDL_TRIGGER_SQL.contains("pg2sr_cdc_ddl_trigger"));
        assert!(!DDL_DROP_TRIGGER_SQL.contains("pg2sr_cdc_drop_trigger"));
        assert!(!DDL_DROP_TRIGGER_FUNC_SQL.contains("trg_capture_drop"));
    }

    /// The drop trigger function MUST emit per-row `DROP TABLE IF EXISTS
    /// <obj.object_identity>` instead of `current_query()`. current_query()
    /// returns the entire multi-target DROP statement for all dropped objects,
    /// leaving tables b/c unguarded against in-flight CDC flushes — they can
    /// be resurrected as empty phantoms by auto_create_sr_table. Per-row
    /// identity lets the poller guard+drain EACH table separately.
    #[test]
    fn test_drop_trigger_func_emits_per_object_identity() {
        // Look for the actual assignment pattern, not just the substring
        // (which would match our own explanatory comment in the SQL body).
        assert!(
            !DDL_DROP_TRIGGER_FUNC_SQL.contains("ddl_text := current_query()"),
            "drop trigger function must not assign ddl_text from current_query() \
             — (multi-target DROP TABLE leaves b/c unguarded)"
        );
        assert!(
            DDL_DROP_TRIGGER_FUNC_SQL.contains("obj.object_identity"),
            "drop trigger function must use obj.object_identity for per-row DROPs"
        );
        assert!(
            DDL_DROP_TRIGGER_FUNC_SQL
                .contains("ddl_text := 'DROP TABLE IF EXISTS ' || obj.object_identity"),
            "drop trigger function must emit a single-target DROP TABLE IF EXISTS \
             per dropped object"
        );
    }

    /// DDL capture infrastructure uses fixed names (single-DB-per-pipeline
    /// design). These are no longer derived from `pipeline_name`.
    #[test]
    fn test_ddl_infrastructure_names_are_fixed() {
        assert_eq!(FUNC_DDL_NAME, "pg2sr_cdc_ddl_func");
        assert_eq!(FUNC_DROP_NAME, "pg2sr_cdc_ddl_drop_func");
        assert_eq!(EVT_DDL_NAME, "pg2sr_cdc_ddl_trigger");
        assert_eq!(EVT_DROP_NAME, "pg2sr_cdc_drop_trigger");
    }

    // ── Default DDL capture table rename ─────────────────────────

    /// No SQL template should contain the legacy table name.
    #[test]
    fn test_sql_templates_dont_contain_legacy_table_name() {
        let legacy = "cc_pg_ddl_capture_tab";
        assert!(!DDL_CAPTURE_TABLE_SQL.contains(legacy));
        assert!(!DDL_TRIGGER_FUNC_SQL_NO_PUB.contains(legacy));
        assert!(!DDL_DROP_TRIGGER_FUNC_SQL.contains(legacy));
        assert!(!DDL_TRIGGER_SQL.contains(legacy));
        assert!(!DDL_DROP_TRIGGER_SQL.contains(legacy));
        assert!(!ENSURE_DDL_TRIGGER_SQL.contains(legacy));
        assert!(!ENSURE_DDL_DROP_TRIGGER_SQL.contains(legacy));
    }

    /// Substituting the new default table name into CREATE TABLE SQL produces
    /// the expected qualified name.
    #[test]
    fn test_create_sql_uses_substituted_table_name() {
        let sql = DDL_CAPTURE_TABLE_SQL
            .replace("{schema}", "public")
            .replace("{table}", "pg2sr_cdc_ddl_events");
        assert!(sql.contains("public.pg2sr_cdc_ddl_events"));
        assert!(sql.contains("pg2sr_cdc_ddl_events_pkey"));
    }

    /// Substituting the new table name into the no-pub DDL trigger function
    /// routes INSERT INTO to the right table.
    #[test]
    fn test_no_pub_trigger_inserts_into_correct_table() {
        let sql = DDL_TRIGGER_FUNC_SQL_NO_PUB
            .replace("{schema}", "public")
            .replace("{table}", "pg2sr_cdc_ddl_events")
            .replace("{func_ddl}", "test_ddl_func");
        assert!(sql.contains("INSERT INTO public.pg2sr_cdc_ddl_events"));
        assert!(!sql.contains("cc_pg_ddl_capture_tab"));
    }

    /// Substituting the new table name into the DROP trigger function
    /// routes INSERT INTO to the right table.
    #[test]
    fn test_drop_trigger_inserts_into_correct_table() {
        let sql = DDL_DROP_TRIGGER_FUNC_SQL
            .replace("{schema}", "public")
            .replace("{table}", "pg2sr_cdc_ddl_events")
            .replace("{func_drop}", "test_drop_func");
        assert!(sql.contains("INSERT INTO public.pg2sr_cdc_ddl_events"));
        assert!(sql.contains("DROP TABLE"));
        assert!(!sql.contains("cc_pg_ddl_capture_tab"));
    }

    /// The default_ddl_capture_table() helper returns the new name.
    #[test]
    fn test_default_ddl_capture_table_is_new_name() {
        assert_eq!(
            crate::config::default_ddl_capture_table(),
            "public.pg2sr_cdc_ddl_events"
        );
    }
}
