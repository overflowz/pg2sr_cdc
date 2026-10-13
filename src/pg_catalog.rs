//! Shared PostgreSQL catalog query utilities.
//!
//! Extracts duplicated PG catalog queries (OID resolution, column metadata,
//! primary key lookup) into single functions so all callers stay in sync.
//! If a query changes (e.g. to handle expression indexes or new column types),
//! it changes here — not in 4 separate files.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::time::Duration;
use tokio_postgres::Client;

/// Default timeout for PG catalog queries.
/// These are simple lookups (OID, columns, PKs) that should complete in
/// milliseconds. If PG is unresponsive, 30s is a generous upper bound.
/// Configurable at startup via `set_catalog_query_timeout` (threaded from
/// `[pipeline].query_timeout_ms`) — kept as an atomic rather than a parameter
/// because the many call sites make threading a parameter invasive.
static CATALOG_QUERY_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(30000);

/// Set the catalog query timeout from `[pipeline].query_timeout_ms`.
/// Called once at startup before any catalog queries run.
pub fn set_catalog_query_timeout(ms: u64) {
    CATALOG_QUERY_TIMEOUT_MS.store(ms.max(1000), std::sync::atomic::Ordering::Relaxed);
}

fn catalog_query_timeout() -> Duration {
    Duration::from_millis(CATALOG_QUERY_TIMEOUT_MS.load(std::sync::atomic::Ordering::Relaxed))
}

/// A column as returned by PG's `pg_attribute` catalog.
/// Used by relation resolution (CDC, DDL fallback, snapshot).
#[derive(Debug, Clone)]
pub struct CatalogColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// Resolve a (schema, table) pair to its PG relation OID.
/// Returns `None` if no matching relation exists.
pub async fn fetch_relation_oid(client: &Client, schema: &str, table: &str) -> Result<Option<u32>> {
    tokio::time::timeout(catalog_query_timeout(), async {
        let row = client
            .query_opt(
                "SELECT c.oid::int4 \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')",
                &[&schema, &table],
            )
            .await?;
        Ok(row.map(|r| r.get::<_, i32>(0) as u32))
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!(
        "PG query 'fetch_relation_oid({schema}.{table})' timed out after {to:?}"
    )
    })?
}

/// Fetch column metadata for a relation OID from `pg_attribute`.
/// Returns columns in ordinal position order, excluding dropped columns.
/// Includes NOT NULL status via `attnotnull`.
///
/// GENERATED columns are EXCLUDED (attgenerated = ''): pgoutput omits them
/// from both Relation messages and WAL tuples (proto.c skips attgenerated),
/// so any catalog-loaded cache that includes them misaligns every tuple's
/// values by one ordinal position when serializing (silent column
/// corruption). Generated columns are simply not replicated.
pub async fn fetch_columns(client: &Client, oid: u32) -> Result<Vec<CatalogColumn>> {
    tokio::time::timeout(catalog_query_timeout(), async {
        let rows = client
            .query(
                "SELECT a.attname, a.atttypid::int4, a.atttypmod
                 FROM pg_attribute a \
                 WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
                   AND a.attgenerated = '' \
                 ORDER BY a.attnum",
                &[&oid],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| CatalogColumn {
                name: r.get(0),
                type_oid: r.get::<_, i32>(1) as u32,
                type_modifier: r.get(2),
            })
            .collect())
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!("PG query 'fetch_columns({oid})' timed out after {to:?}")
    })?
}

/// Fetch PRIMARY KEY column names for a relation OID.
/// Returns PK columns in index column order, or an empty vec if no PK exists.
pub async fn fetch_primary_keys(client: &Client, oid: u32) -> Result<Vec<String>> {
    tokio::time::timeout(catalog_query_timeout(), async {
        let rows = client
            .query(
                "SELECT a.attname \
                 FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = $1 AND i.indisprimary \
                 ORDER BY array_position(i.indkey, a.attnum)",
                &[&oid],
            )
            .await?;

        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!(
            "PG query 'fetch_primary_keys({oid})' timed out after {to:?}"
        )
    })?
}

/// Check if a table has a PRIMARY KEY.
/// Returns `Ok(true)` if the table has at least one PK column, `Ok(false)` otherwise.
/// Errors if the table doesn't exist or the query fails.
#[allow(dead_code)]
pub async fn table_has_pk(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let oid = fetch_relation_oid(client, schema, table)
        .await
        .context("Failed to query table OID for PK check")?
        .ok_or_else(|| anyhow::anyhow!("Table {schema}.{table} not found for PK check"))?;

    let pks = fetch_primary_keys(client, oid).await?;
    Ok(!pks.is_empty())
}

/// Quote a PostgreSQL identifier with double quotes, escaping embedded quotes.
/// Use everywhere PG identifiers appear in dynamically-built SQL to prevent
/// SQL injection and handle mixed-case or special-character names.
pub fn pg_quote(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

/// Batch-fetch OIDs for all user tables.
/// Returns a map of (schema, table) → oid. Excludes system schemas and
/// the DDL capture infrastructure table (by schema + name pair).
pub async fn fetch_bulk_table_oids(
    client: &Client,
    exclude_schema: &str,
    exclude_table: &str,
) -> Result<Vec<(String, String, u32)>> {
    tokio::time::timeout(catalog_query_timeout(), async {
        let rows = client
            .query(
                "SELECT n.nspname, c.relname, c.oid::int4 \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r', 'p') \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                   AND NOT (n.nspname = $2 AND c.relname = $1) \
                 ORDER BY n.nspname, c.relname",
                &[&exclude_table, &exclude_schema],
            )
            .await
            .context("Failed to query PG table OIDs")?;

        Ok(rows
            .iter()
            .map(|r| {
                let schema: String = r.get(0);
                let table: String = r.get(1);
                let oid: i32 = r.get(2);
                (schema, table, oid as u32)
            })
            .collect())
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!(
            "PG query 'fetch_bulk_table_oids' timed out after {to:?}"
        )
    })?
}

/// Batch-fetch column metadata for multiple relation OIDs.
/// Returns a map of oid → Vec<CatalogColumn>.
pub async fn fetch_bulk_columns(
    client: &Client,
    oids: &[u32],
) -> Result<HashMap<u32, Vec<CatalogColumn>>> {
    if oids.is_empty() {
        return Ok(HashMap::new());
    }

    tokio::time::timeout(catalog_query_timeout(), async {
        let sql = "SELECT a.attrelid::int4, a.attname, a.atttypid::int4, a.atttypmod \
              FROM pg_attribute a \
              WHERE a.attrelid = ANY($1::int4[]) AND a.attnum > 0 AND NOT a.attisdropped \
                AND a.attgenerated = '' \
              ORDER BY a.attrelid, a.attnum".to_string();
        // Convert u32→i32 for _int4 serialization: PostgreSQL stores OIDs as
        // uint32 but int4[] is signed; bit-cast is safe (same underlying bytes).
        let oids_i32: Vec<i32> = oids.iter().map(|&o| o as i32).collect();
        let rows = client
            .query(&sql, &[&oids_i32])
            .await
            .context("Failed to query bulk PG columns")?;

        let mut map: HashMap<u32, Vec<CatalogColumn>> = HashMap::new();
        for r in &rows {
            let relid: i32 = r.get(0);
            let oid = relid as u32;
            map.entry(oid).or_default().push(CatalogColumn {
                name: r.get(1),
                type_oid: r.get::<_, i32>(2) as u32,
                type_modifier: r.get(3),
            });
        }
        Ok(map)
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!("PG query 'fetch_bulk_columns' timed out after {to:?}")
    })?
}

/// Batch-fetch PRIMARY KEY column names for multiple relation OIDs.
/// Returns a map of oid → Vec<pk_column_name> in index column order.
pub async fn fetch_bulk_primary_keys(
    client: &Client,
    oids: &[u32],
) -> Result<HashMap<u32, Vec<String>>> {
    if oids.is_empty() {
        return Ok(HashMap::new());
    }

    tokio::time::timeout(catalog_query_timeout(), async {
        let sql = "SELECT i.indrelid::int4, a.attname \
              FROM pg_index i \
              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
              WHERE i.indrelid = ANY($1::int4[]) AND i.indisprimary \
              ORDER BY i.indrelid, array_position(i.indkey, a.attnum)".to_string();
        // Convert u32→i32 for _int4 serialization (same bit-cast rationale above).
        let oids_i32: Vec<i32> = oids.iter().map(|&o| o as i32).collect();
        let rows = client
            .query(&sql, &[&oids_i32])
            .await
            .context("Failed to query bulk PG primary keys")?;

        let mut map: HashMap<u32, Vec<String>> = HashMap::new();
        for r in &rows {
            let relid: i32 = r.get(0);
            let oid = relid as u32;
            map.entry(oid).or_default().push(r.get::<_, String>(1));
        }
        Ok(map)
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!(
            "PG query 'fetch_bulk_primary_keys' timed out after {to:?}"
        )
    })?
}
/// Batch-fetch REPLICA IDENTITY (`relreplident`) for multiple relation OIDs.
/// Returns a map of oid → one of 'd' (DEFAULT), 'f' (FULL), 'i' (INDEX),
/// 'n' (NOTHING). Tables with no row in the map did not match (dropped, or
/// relkind not in ('r','p')).
pub async fn fetch_bulk_replica_identities(
    client: &Client,
    oids: &[u32],
) -> Result<HashMap<u32, String>> {
    if oids.is_empty() {
        return Ok(HashMap::new());
    }

    tokio::time::timeout(catalog_query_timeout(), async {
        let sql = "SELECT c.oid::int4, c.relreplident::text \
              FROM pg_class c \
              WHERE c.oid = ANY($1::int4[]) AND c.relkind IN ('r', 'p')".to_string();
        // Convert u32→i32 for _int4 serialization (same bit-cast rationale
        // as fetch_bulk_columns).
        let oids_i32: Vec<i32> = oids.iter().map(|&o| o as i32).collect();
        let rows = client
            .query(&sql, &[&oids_i32])
            .await
            .context("Failed to query bulk PG replica identities")?;

        let mut map: HashMap<u32, String> = HashMap::new();
        for r in &rows {
            let relid: i32 = r.get(0);
            let oid = relid as u32;
            map.insert(oid, r.get(1));
        }
        Ok(map)
    })
    .await
    .map_err(|_| {
        let to = catalog_query_timeout();
        anyhow::anyhow!(
            "PG query 'fetch_bulk_replica_identities' timed out after {to:?}"
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_quote_basic() {
        assert_eq!(pg_quote("hello"), "\"hello\"");
    }

    #[test]
    fn test_pg_quote_embedded_quote() {
        assert_eq!(pg_quote("he\"llo"), "\"he\"\"llo\"");
    }

    #[test]
    fn test_pg_quote_empty() {
        assert_eq!(pg_quote(""), "\"\"");
    }
}
