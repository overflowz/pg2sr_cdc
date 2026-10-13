//! StarRocks SQL executor via mysql_async (native Rust, no subprocess).
//! Uses text protocol (stmt_cache_size=0) and prefer_socket=false for SR compatibility.
//! Provides pure utility functions for SR DDL generation: CREATE TABLE, PROPERTIES,
//! KEY/DISTRIBUTED BY clauses, and column reordering to satisfy SR's
//! key-columns-must-be-first constraint.
///
/// Most callers should use [`SrConnectionManager::execute_sql`] directly for
/// HA multi-node support with failover. This module re-exports that same
/// function alongside the pure utility functions.
use anyhow::Result;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::sr_connection::SrConnectionManager;

/// Generate StarRocks CREATE TABLE PROPERTIES clause.
///
/// Takes a map of property key → value pairs and formats them into
/// a `PROPERTIES (...)` clause. Callers should use
/// [`SrConfig::build_table_properties()`] to assemble the map from
/// config (replication_num, fast_schema_evolution, persistent_index, user props).
///
/// Output format:
/// ```sql
/// PROPERTIES (
///   "replication_num" = "1",
///   "fast_schema_evolution" = "true"
/// )
/// ```
pub fn sr_table_properties(props: &HashMap<String, String>) -> String {
    if props.is_empty() {
        return String::new();
    }
    // Sort by key for deterministic PROPERTIES clause ordering.
    // HashMap iteration order is randomized per-process (SipHash seed),
    // which would make CREATE TABLE SQL non-deterministic across runs —
    // a latent hazard for SQL-diff tooling and log comparisons.
    let mut entries: Vec<(&String, &String)> = props.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let entries: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| {
            format!(
                "\"{}\" = \"{}\"",
                k.replace('"', "\\\""),
                v.replace('"', "\\\""),
            )
        })
        .collect();
    format!("PROPERTIES (\n  {}\n)", entries.join(",\n  "))
}

/// Quote an identifier with backticks for StarRocks SQL.
/// Used everywhere column/table names appear in SR DDL.
/// Escapes embedded backticks (`` ` `` → `` `` ``) per MySQL/SR quoting rules.
pub fn sr_quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Build the StarRocks table name from a PG schema and table pair.
///
/// Return the bare table name for StarRocks.
///
/// Since the user configures a separate SR pipeline database per PG schema,
/// no schema prefix is needed.
pub fn sr_table_name(_schema: &str, table: &str) -> String {
    table.to_string()
}

/// Build the StarRocks KEY clause and DISTRIBUTED BY column for CREATE TABLE.
///
/// When `pk_columns` is non-empty, produces `PRIMARY KEY(...)` with all PK columns
/// and uses the first PK column as the distribution column. Otherwise produces
/// `DUPLICATE KEY(...)` with the given `first_col` as both key and distribution column.
///
/// All column names are backtick-quoted via [`sr_quote`] for consistency.
/// This is the single source of truth — all CREATE TABLE paths must use this.
pub fn build_key_and_dist_col(pk_columns: &[String], first_col: &str) -> (String, String) {
    let fc = sr_quote(first_col);
    if pk_columns.is_empty() {
        (format!("DUPLICATE KEY({fc})"), fc)
    } else {
        let pk = pk_columns
            .iter()
            .map(|c| sr_quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        (format!("PRIMARY KEY({pk})"), sr_quote(&pk_columns[0]))
    }
}

/// Assemble a full `CREATE TABLE` statement for StarRocks.
///
/// Produces consistent multi-line DDL with ENGINE=OLAP, key clause,
/// DISTRIBUTED BY HASH, and PROPERTIES. All four call sites now share
/// this single function instead of maintaining separate format strings.
///
/// When `database` is empty, the table reference omits the database prefix
/// (used by the DDL poller path where the connection already targets a db).
///
/// `if_not_exists`: pass `true` for user-facing/main tables (idempotent
/// re-creation on replay). Pass `false` for shadow/swap-target tables — a
/// stale shadow surviving a failed DROP must produce an explicit error
/// rather than being silently resurrected via `CREATE TABLE IF NOT EXISTS`
pub fn sr_create_table_sql(
    database: &str,
    table: &str,
    col_defs: &[String],
    key_clause: &str,
    dist_col: &str,
    properties: &str,
    if_not_exists: bool,
) -> String {
    let table_ref = if database.is_empty() {
        sr_quote(table)
    } else {
        format!("{}.{}", sr_quote(database), sr_quote(table))
    };
    let header = if if_not_exists {
        "CREATE TABLE IF NOT EXISTS"
    } else {
        "CREATE TABLE"
    };
    format!(
        "{header} {table_ref} (\n  {}\n) ENGINE=OLAP\n{}\nDISTRIBUTED BY HASH({})\n{}",
        col_defs.join(",\n  "),
        key_clause,
        dist_col,
        properties,
    )
}

/// Reorder columns so that PRIMARY KEY columns come first, preserving
/// relative order within the PK group and within the non-PK group.
///
/// StarRocks requires key columns to be the first N columns in the
/// column list of CREATE TABLE statements. This function ensures that
/// constraint is met regardless of PG's column ordering.
///
/// `get_name` extracts the column name from each element for matching
/// against `pk_columns`. Elements whose name matches a PK column are
/// moved to the front in the order they appear in `pk_columns`.
pub fn reorder_columns_for_sr<T: Clone>(
    columns: &[T],
    pk_columns: &[String],
    get_name: impl Fn(&T) -> &str,
) -> Vec<T> {
    let pk_set: HashSet<&str> = pk_columns.iter().map(|s| s.as_str()).collect();

    // Pre-compute column names for PK-missing validation.
    let col_names: Vec<&str> = columns.iter().map(&get_name).collect();
    let col_name_set: HashSet<&str> = col_names.iter().copied().collect();

    // Warn if any PK column is not found in the columns list — this would
    // produce a CREATE TABLE with PRIMARY KEY referencing a non-existent
    // column, which SR rejects at table creation time.
    let missing: Vec<&String> = pk_columns
        .iter()
        .filter(|p| !col_name_set.contains(p.as_str()))
        .collect();
    if !missing.is_empty() {
        tracing::warn!(
            ?missing,
            "PK columns not found in column list — will produce invalid PRIMARY KEY clause"
        );
    }

    let mut ordered: Vec<T> = Vec::with_capacity(columns.len());

    // PK columns first, in the order they appear in pk_columns
    for pk_name in pk_columns {
        if let Some(col) = columns.iter().find(|c| get_name(c) == pk_name.as_str()) {
            ordered.push(col.clone());
        }
    }

    // Non-PK columns, in original relative order
    for col in columns {
        if !pk_set.contains(get_name(col)) {
            ordered.push(col.clone());
        }
    }

    ordered
}

/// Execute one or more SQL statements on StarRocks with HA multi-node failover.
///
/// Delegates to [`SrConnectionManager::execute_sql`] which handles node
/// selection, failover, and active-node caching.
///
/// Returns "OK" on success. Silently ignores "already exists" / "duplicate" errors.
pub async fn execute_sql(sr_mgr: &SrConnectionManager, sql: &str) -> Result<String> {
    sr_mgr.execute_sql(sql).await
}

/// (Tier 1) optimization: execute pre-split SQL statements. Skip the
/// redundant third polyglot parse call (the DDL path's `post_process_ddl_stmts`
/// already produces the per-statement `Vec<String>`). Used by the DDL poller
/// to eliminate a `spawn_blocking` per DDL.
pub async fn execute_sql_stmts(
    sr_mgr: &SrConnectionManager,
    stmts: Vec<String>,
) -> Result<String> {
    sr_mgr.execute_sql_stmts(stmts).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sr_table_properties_none() {
        let p = sr_table_properties(&std::collections::HashMap::new());
        assert!(p.is_empty(), "empty props -> no properties clause: {p}");
    }

    #[test]
    fn test_sr_table_properties_replication_num() {
        let map =
            std::collections::HashMap::from([("replication_num".to_string(), "1".to_string())]);
        let p = sr_table_properties(&map);
        assert!(
            p.contains(r#""replication_num" = "1""#),
            "has replication_num: {p}"
        );
    }

    #[test]
    fn test_sr_table_properties_fse() {
        let map = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
        ]);
        let p = sr_table_properties(&map);
        assert!(p.contains(r#""fast_schema_evolution" = "true""#));
        assert!(!p.contains("persistent_index"));
    }

    #[test]
    fn test_sr_table_properties_persistent_index() {
        let map = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("enable_persistent_index".to_string(), "true".to_string()),
            (
                "persistent_index_type".to_string(),
                "CLOUD_NATIVE".to_string(),
            ),
        ]);
        let p = sr_table_properties(&map);
        assert!(p.contains(r#""enable_persistent_index" = "true""#));
        assert!(p.contains(r#""persistent_index_type" = "CLOUD_NATIVE""#));
    }

    #[test]
    fn test_sr_table_properties_all() {
        let map = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
            ("enable_persistent_index".to_string(), "true".to_string()),
            (
                "persistent_index_type".to_string(),
                "CLOUD_NATIVE".to_string(),
            ),
        ]);
        let p = sr_table_properties(&map);
        assert!(p.contains(r#""replication_num" = "1""#));
        assert!(p.contains(r#""fast_schema_evolution" = "true""#));
        assert!(p.contains(r#""enable_persistent_index" = "true""#));
        assert!(p.contains(r#""persistent_index_type" = "CLOUD_NATIVE""#));
    }

    #[test]
    fn test_sr_table_properties_multi_line_format() {
        let map = std::collections::HashMap::from([
            ("replication_num".to_string(), "1".to_string()),
            ("fast_schema_evolution".to_string(), "true".to_string()),
        ]);
        let p = sr_table_properties(&map);
        // Must be multi-line with indentation
        assert!(p.contains('\n'), "multi-line: {p}");
        assert!(p.contains("  "), "indented: {p}");
    }

    #[derive(Clone)]
    struct TestCol {
        name: String,
    }

    #[test]
    fn test_reorder_columns_pk_first() {
        let cols = vec![
            TestCol {
                name: "data".into(),
            },
            TestCol { name: "id".into() },
            TestCol {
                name: "name".into(),
            },
        ];
        let pk = vec!["id".to_string()];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].name, "id"); // PK first
        assert_eq!(ordered[1].name, "data"); // then original order
        assert_eq!(ordered[2].name, "name");
    }

    #[test]
    fn test_reorder_columns_multiple_pk() {
        let cols = vec![
            TestCol { name: "val".into() },
            TestCol { name: "id".into() },
            TestCol { name: "ts".into() },
            TestCol {
                name: "name".into(),
            },
        ];
        let pk = vec!["id".to_string(), "ts".to_string()];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered[0].name, "id"); // PK order
        assert_eq!(ordered[1].name, "ts");
        assert_eq!(ordered[2].name, "val"); // then original order
        assert_eq!(ordered[3].name, "name");
    }

    #[test]
    fn test_reorder_columns_no_pk() {
        let cols = vec![TestCol { name: "a".into() }, TestCol { name: "b".into() }];
        let pk: Vec<String> = vec![];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].name, "a"); // unchanged order
        assert_eq!(ordered[1].name, "b");
    }

    #[test]
    fn test_reorder_columns_empty() {
        let cols: Vec<TestCol> = vec![];
        let pk = vec!["id".to_string()];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert!(ordered.is_empty());
    }

    #[test]
    fn test_reorder_columns_all_pk() {
        let cols = vec![TestCol { name: "b".into() }, TestCol { name: "a".into() }];
        let pk = vec!["a".to_string(), "b".to_string()];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].name, "a"); // pk_columns order
        assert_eq!(ordered[1].name, "b");
    }

    #[test]
    fn test_reorder_columns_pk_not_found_is_skipped() {
        let cols = vec![TestCol { name: "x".into() }];
        let pk = vec!["nonexistent".to_string()];

        let ordered = reorder_columns_for_sr(&cols, &pk, |c| c.name.as_str());
        assert_eq!(ordered.len(), 1);
        assert_eq!(ordered[0].name, "x"); // non-PK columns preserved
    }

    #[test]
    fn test_sr_table_name_basic() {
        assert_eq!(sr_table_name("public", "users"), "users");
    }

    #[test]
    fn test_sr_table_name_non_public_schema() {
        assert_eq!(sr_table_name("app", "orders"), "orders");
    }

    // ── Shadow tables must NOT use IF NOT EXISTS ──

    #[test]
    fn test_sr_create_table_sql_if_not_exists_true_includes_clause() {
        let sql = sr_create_table_sql(
            "db", "tbl", &["`id` INT NOT NULL".into()],
            "PRIMARY KEY(`id`)", "`id`", "PROPERTIES (\"replication_num\" = \"1\")",
            true,
        );
        assert!(
            sql.starts_with("CREATE TABLE IF NOT EXISTS"),
            "if_not_exists=true must include IF NOT EXISTS: {sql}"
        );
    }

    #[test]
    fn test_sr_create_table_sql_if_not_exists_false_omits_clause() {
        // Shadow tables use if_not_exists=false so a stale shadow surviving
        // a failed DROP surfaces as an explicit error instead of silent resurrection.
        let sql = sr_create_table_sql(
            "db", "shadow_tbl", &["`id` INT NOT NULL".into()],
            "PRIMARY KEY(`id`)", "`id`", "PROPERTIES (\"replication_num\" = \"1\")",
            false,
        );
        assert!(
            sql.starts_with("CREATE TABLE "),
            "if_not_exists=false must omit IF NOT EXISTS: {sql}"
        );
        assert!(
            !sql.contains("IF NOT EXISTS"),
            "if_not_exists=false must not contain IF NOT EXISTS anywhere: {sql}"
        );
    }
}
