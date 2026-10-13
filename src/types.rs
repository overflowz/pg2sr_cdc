//! PG → StarRocks type mapping.
//!
//! Maps PostgreSQL column types to StarRocks-compatible SQL type strings.
//! Reference: https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-types/
//! PG type OIDs: https://www.postgresql.org/docs/current/catalog-pg-type.html
//!
//! Design notes:
//! - Unbounded PG text types (TEXT, XML, BYTEA) → STRING (SR's native large-text type)
//! - Bounded types (CHAR(n), VARCHAR(n)) → length-mapped ×3 for UTF-8 byte safety
//! - PG array types → ARRAY<element_type> with {a,b,c} → [a,b,c] transform in pipeline
//! - PG types with no SR equivalent (TIME, geometric, etc.) → VARCHAR(n) as formatted text

use std::collections::HashSet;
use std::sync::Mutex;

/// Log a warning about an unknown PG type OID, rate-limited to once per OID.
fn warn_unknown_oid(oid: u32) {
    static LOGGED: std::sync::OnceLock<Mutex<HashSet<u32>>> = std::sync::OnceLock::new();
    let set = LOGGED.get_or_init(|| Mutex::new(HashSet::new()));
    if set.lock().unwrap_or_else(|e| e.into_inner()).insert(oid) {
        tracing::warn!(
            "Unknown PG type OID {} → mapped to STRING — data may be stored as plain text",
            oid
        );
    }
}

/// Strip PG bytea hex prefix (`\x`) from a hex string and return just the hex digits.
/// If the value doesn't have the `\x` prefix, returns it as-is.
///
/// # Requirements
/// This assumes PG's `bytea_output` setting is `hex` (default since PG 9.0).
/// In `escape` format, BYTEA values use backslash-octal encoding and this function
/// won't strip the prefix — SR's `unhex()` will reject or corrupt the value.
/// If you changed `bytea_output` to `escape`, set it back to `hex` or exclude
/// BYTEA columns from replication.
pub fn strip_bytea_hex_prefix(s: &str) -> &str {
    // PG bytea hex output format: \xDEADBEEF (backslash, x, then hex digits)
    if s.starts_with('\\') && s.len() >= 2 && (s.as_bytes()[1] == b'x' || s.as_bytes()[1] == b'X') {
        &s[2..]
    } else {
        s
    }
}

/// Given a PG type OID and type modifier, return the StarRocks column type string.
///
/// Extract (precision, scale) from an UPPERCASE "DECIMAL(p[,s])" / "DEC(p,s)"
/// type name. Returns None for unparseable text (caller falls back to
/// passthrough).
fn parse_decimal_precision_scale(upper: &str) -> Option<(i32, i32)> {
    let inner = upper
        .strip_prefix("DECIMAL(")
        .or_else(|| upper.strip_prefix("DEC("))?
        .strip_suffix(')')?;
    let mut parts = inner.split(',');
    let p: i32 = parts.next()?.trim().parse().ok()?;
    let s: i32 = match parts.next() {
        Some(v) => v.trim().parse().ok()?,
        None => 0,
    };
    Some((p, s))
}

/// This is used for CREATE TABLE / ADD COLUMN DDL generation.
pub fn pg_type_to_sr(pg_type_oid: u32, type_modifier: i32) -> String {
    // Check for array types first — map to ARRAY<element_type> in SR
    if let Some(element_type) = pg_array_element_type(pg_type_oid) {
        return format!("ARRAY<{}>", element_type);
    }

    // NOTE: type_modifier encodes length/precision info.
    // - char(n) / varchar(n): modifier = n + 4 (header length offset)
    // - numeric(p,s): modifier = (p << 16) | s
    let (precision, scale) = if type_modifier > 0 {
        // For numeric/n Charmodifier: extract
        let raw = type_modifier - 4; // VARHDRSZ
        let p = (raw >> 16) as u32;
        let s = (raw & 0xFFFF) as u32;
        (p, s)
    } else {
        (0, 0)
    };

    match pg_type_oid {
        // ── Numeric ──
        21 => "SMALLINT".to_string(), // int2
        23 => "INT".to_string(),      // int4
        20 => "BIGINT".to_string(),   // int8
        700 => "FLOAT".to_string(),   // float4
        701 => "DOUBLE".to_string(),  // float8
        1700 => {
            // numeric
            // PG allows precision up to 1000, but StarRocks DECIMAL caps at
            // precision 38 and scale 18. If the PG precision exceeds 38, map
            // to STRING to avoid CREATE TABLE rejection by SR (which would
            // break replication for the entire table). The value path already
            // serializes NUMERIC as text, so STRING is lossless.
            if precision > 38 {
                tracing::warn!(
                    "NUMERIC precision {} > 38 — mapping to STRING to avoid SR DECIMAL limit",
                    precision
                );
                "STRING".to_string()
            } else if scale > 18 {
                // PG allows NUMERIC(38, 20) (up to 16383 fractional digits), but
                // StarRocks DECIMAL caps scale at 18. Clamping would silently drop
                // the extra fractional digits on every value (silent TOAST-like
                // corruption). Map to STRING instead — the CDC value path serializes
                // NUMERIC as JSON text, so STRING is lossless.
                tracing::warn!(
                    "NUMERIC scale {} > 18 — mapping to STRING to avoid SR DECIMAL scale limit",
                    scale
                );
                "STRING".to_string()
            } else if precision > 0 && scale > 0 {
                let s = scale.min(18).min(precision);
                format!("DECIMAL({precision},{s})")
            } else if precision > 0 {
                format!("DECIMAL({precision})")
            } else {
                // No explicit precision/scale — PG NUMERIC can hold up to 131072 digits.
                // DECIMAL(38,18) would silently truncate. Map to STRING to preserve data.
                "STRING".to_string()
            }
        }
        790 => "DECIMAL(38,2)".to_string(), // money

        // ── Character (bounded → STRING for UTF-8 safety, SR has no CHAR limit issues) ──
        // PG CHAR(N) and VARCHAR(N) both arrive as text in WAL. STRING handles any length.
        1042 => "STRING".to_string(), // char(n) → STRING
        1043 => "STRING".to_string(), // varchar(n) → STRING

        // ── Unbounded text → STRING (SR's native large-text type) ──
        25 => "STRING".to_string(),    // text
        17 => "VARBINARY".to_string(), // bytea → SR's binary type
        142 => "STRING".to_string(),   // xml

        // ── JSON ──
        114 => "JSON".to_string(),  // json
        3802 => "JSON".to_string(), // jsonb (SR has only JSON)

        // ── Date/Time ──
        1082 => "DATE".to_string(),        // date
         1083 => "VARCHAR(20)".to_string(), // time (SR has no TIME type)
        1266 => "VARCHAR(32)".to_string(), // timetz
        1114 => "DATETIME".to_string(),    // timestamp
        1184 => "DATETIME".to_string(),    // timestamptz (SR has no TZ support)
        1186 => "BIGINT".to_string(),      // interval (microseconds)

        // ── Boolean ──
        16 => "BOOLEAN".to_string(), // bool

        // ── UUID ──
        2950 => "VARCHAR(64)".to_string(), // uuid

        // ── Network (text from WAL) ──
        869 => "VARCHAR(64)".to_string(), // inet
        650 => "VARCHAR(64)".to_string(), // cidr

        // ── Bit ──
        // PG BIT/VARBIT arrive as text "01010101" (ASCII 0/1 chars).
        // Mapping to VARBINARY caused silent corruption: SR interpreted the
        // text as hex pairs. VARCHAR preserves the textual bit-string.
        1560 => "VARCHAR(255)".to_string(), // bit
        1562 => "VARCHAR(255)".to_string(), // varbit

        // ── Enum / custom types → STRING (values arrive as text) ──
        // record/composite: PG text representation looks like "(field1,field2,...)" 
        // and routinely exceeds 255 chars for composite types with multiple fields.
        // VARCHAR(255) silently truncates; STRING is unbounded in SR.
        2249 => "STRING".to_string(), // record/composite
        2275 => "STRING".to_string(),       // internal representation (cstring)

        // ── Range types (text from WAL) ──
        3904 => "VARCHAR(255)".to_string(), // int4range
        3906 => "VARCHAR(255)".to_string(), // numrange
        3908 => "VARCHAR(255)".to_string(), // tsrange
        3910 => "VARCHAR(255)".to_string(), // tstzrange
        3912 => "VARCHAR(255)".to_string(), // daterange
        3926 => "VARCHAR(255)".to_string(), // int8range
        // NOTE: _int4range (3905), _numrange (3907) are array OIDs

        // ── Network addresses (text) ──
        150 => "VARCHAR(255)".to_string(),  // macaddr
        4144 => "VARCHAR(255)".to_string(), // macaddr8

        // ── Geometric (text) ──
        600 => "VARCHAR(255)".to_string(), // point
        601 => "VARCHAR(255)".to_string(), // line
        602 => "VARCHAR(255)".to_string(), // lseg
        603 => "VARCHAR(255)".to_string(), // box
        718 => "VARCHAR(255)".to_string(), // path
        719 => "VARCHAR(255)".to_string(), // polygon
        620 => "VARCHAR(255)".to_string(), // circle

        // ── Default (unknown) → STRING ──
        _ => {
            warn_unknown_oid(pg_type_oid);
            "STRING".to_string()
        }
    }
}

/// Resolve a PG type OID to a human-readable PG type name for diagnostics
/// (validation logs, error messages). Covers the well-known OIDs handled by
/// `pg_type_to_sr`; unknown OIDs return "OID <n>". Used so validation logs
/// report the type NAME (e.g. "json") instead of duplicating the numeric OID
/// Not a PG catalog lookup — purely a static map of builtin OIDs.
pub fn pg_type_oid_to_name(type_oid: u32) -> String {
    match type_oid {
        16 => "bool",
        17 => "bytea",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        25 => "text",
        114 => "json",
        142 => "xml",
        600 => "point",
        601 => "line",
        602 => "lseg",
        603 => "box",
        620 => "circle",
        718 => "path",
        719 => "polygon",
        700 => "float4",
        701 => "float8",
        790 => "money",
        791 => "_money",
        869 => "inet",
        650 => "cidr",
        1042 => "char",
        1043 => "varchar",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        1266 => "timetz",
        1560 => "bit",
        1562 => "varbit",
        1700 => "numeric",
        2249 => "record",
        2275 => "cstring",
        2950 => "uuid",
        3904 => "int4range",
        3905 => "_int4range",
        3906 => "numrange",
        3907 => "_numrange",
        3908 => "tsrange",
        3910 => "tstzrange",
        3912 => "daterange",
        3926 => "int8range",
        4144 => "macaddr8",
        150 => "macaddr",
        // Arrays (1xxx)
        1000 => "_bool",
        1005 => "_int2",
        1007 => "_int4",
        1009 => "_text",
        1016 => "_int8",
        1021 => "_float4",
        1022 => "_float8",
        1115 => "_timestamp",
        1182 => "_date",
        1183 => "_time",
        1231 => "_numeric",
        199 => "_json",
        2951 => "_uuid",
        1561 => "_bit",
        _ => {
            // Unknown — report the OID so the operator can look it up.
            // (pg_type_to_sr already warned via warn_unknown_oid at the call site.)
            return format!("OID {}", type_oid);
        }
    }
    .to_string()
}

/// Given a PG array type OID, return the StarRocks element type string.
/// Returns None if the OID is not a known array type.
///
/// Covers all commonly-used PG array types from pg_type where typcategory = 'A'.
/// For the test schema arrays created on-the-fly, falls back to STRING.
pub fn pg_array_element_type(array_oid: u32) -> Option<&'static str> {
    match array_oid {
        // Numeric arrays
        1005 => Some("SMALLINT"),       // _int2
        1007 => Some("INT"),            // _int4
        1016 => Some("BIGINT"),         // _int8
        1021 => Some("FLOAT"),          // _float4
        1022 => Some("DOUBLE"),         // _float8
         1231 => Some("STRING"), // _numeric (consistent with scalar NUMERIC→STRING)
        791 => Some("DECIMAL(38,2)"),   // _money

        // Boolean
        1000 => Some("BOOLEAN"), // _bool

        // Text / character arrays
        1002 => Some("CHAR"),                 // _char
        1003 => Some("VARCHAR(64)"),          // _name
        1009 | 1014 | 1015 => Some("STRING"), // _text, _bpchar, _varchar
        1001 | 143 => Some("STRING"),         // _bytea, _xml
        1263 => Some("STRING"),               // _cstring

        // JSON
        199 | 3807 => Some("JSON"), // _json, _jsonb

        // Date/time arrays
        1115 => Some("DATETIME"),    // _timestamp
        1182 => Some("DATE"),        // _date
         1183 => Some("VARCHAR(20)"), // _time
        1185 => Some("DATETIME"),    // _timestamptz
        1270 => Some("VARCHAR(32)"), // _timetz
        1187 => Some("BIGINT"),      // _interval

        // UUID
        2951 => Some("VARCHAR(64)"), // _uuid

        // Network
        1041 => Some("VARCHAR(64)"),  // _inet
        651 => Some("VARCHAR(64)"),   // _cidr
        1040 => Some("VARCHAR(255)"), // _macaddr
        775 => Some("VARCHAR(255)"),  // _macaddr8

        // Bit arrays → consistent with scalar BIT/VARBIT → VARCHAR
        1561 | 1563 => Some("VARCHAR(255)"), // _bit, _varbit

        // Range arrays (text from WAL)
        3905 | 3907 | 3909 | 3911 | 3913 | 3927 => Some("VARCHAR(255)"),

        // Geometric arrays (text from WAL) — 719 (polygon) is scalar, not array
        1017 | 1018 | 1019 | 1020 | 1027 | 629 => Some("STRING"),

        // Fallback: not a known array type
        _ => None,
    }
}

/// Given a PG array type OID, return the scalar PG element type OID.
/// This is the reverse mapping of pg_array_element_type — it returns the
/// PG OID (not the SR type string) so callers can dispatch each element
/// through the same scalar coercion logic as non-array values.
///
/// Returns None if the OID is not a known array type.
pub fn pg_array_element_oid(array_oid: u32) -> Option<u32> {
    match array_oid {
        1005 => Some(21),    // _int2 → int2
        1007 => Some(23),    // _int4 → int4
        1016 => Some(20),    // _int8 → int8
        1021 => Some(700),   // _float4 → float4
        1022 => Some(701),   // _float8 → float8
        1231 => Some(1700),  // _numeric → numeric
        791 => Some(790),    // _money → money
        1000 => Some(16),    // _bool → bool
        1002 => Some(1042),  // _char → char
        1003 => Some(19),    // _name → name
        1009 | 1014 | 1015 => Some(25), // _text, _bpchar, _varchar → text
        1001 => Some(17),          // _bytea → bytea (strip \x prefix per element)
        143 => Some(142),          // _xml → xml (maps to STRING, not bytea)
        1263 => Some(25),    // _cstring → text
        199 | 3807 => Some(114), // _json, _jsonb → json (use 114 for both)
        1115 => Some(1114),    // _timestamp → timestamp
        1182 => Some(1082),    // _date → date
        1183 => Some(1083),    // _time → time
        1185 => Some(1184),    // _timestamptz → timestamptz
        1270 => Some(1266),    // _timetz → timetz
        1187 => Some(1186),    // _interval → interval
        2951 => Some(2950),    // _uuid → uuid
        1041 => Some(869),     // _inet → inet
        651 => Some(650),      // _cidr → cidr
        1040 => Some(150),     // _macaddr → macaddr
        775 => Some(4144),     // _macaddr8 → macaddr8
        1561 | 1563 => Some(25), // _bit, _varbit → text (mapped to VARCHAR)
        3905 | 3907 | 3909 | 3911 | 3913 | 3927 => Some(25), // range arrays → text
        1017 | 1018 | 1019 | 1020 | 1027 | 629 => Some(25),  // geometric arrays → text
        _ => None,
    }
}

/// Parse a PostgreSQL array string like `{1,2,3}` or `{a,"b, c"}`
/// into its raw string elements (quotes preserved).
///
/// Handles nested arrays (`{{1,2},{3,4}}`), quoted elements,
/// and escaped quotes within quoted strings.
pub fn parse_pg_array(s: &str) -> Option<Vec<String>> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = &s[1..s.len() - 1];

    // Empty array
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut depth = 0i32;
    let mut escaped = false;

    for ch in inner.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => {
                escaped = true;
                current.push(ch);
            }
            '"' if !in_quotes => {
                in_quotes = true;
                current.push(ch);
            }
            '"' if in_quotes => {
                in_quotes = false;
                current.push(ch);
            }
            '{' if !in_quotes => {
                depth += 1;
                current.push(ch);
            }
            '}' if !in_quotes => {
                if depth > 0 {
                    depth -= 1;
                }
                current.push(ch);
            }
            ',' if !in_quotes && depth == 0 => {
                elements.push(current);
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    elements.push(current);

    Some(elements)
}

/// Convert a single PG array element string to a serde_json::Value.
/// Handles quoted strings, numbers, booleans, nulls, and nested arrays.
/// `depth` limits recursion for nested arrays to prevent stack overflow.
fn pg_element_to_json(elem: &str, depth: u32) -> serde_json::Value {
    if depth > 16 {
        return serde_json::Value::String(elem.to_string());
    }
    let trimmed = elem.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }

    // Nested array
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return pg_array_to_json_depth(trimmed, depth + 1);
    }

    // Quoted string — strip the surrounding quotes, unescape PG-style \\", then try JSON parse
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        let mut inner = trimmed[1..trimmed.len() - 1].to_string();
        // Unescape PG array escaping: \" → " and \\ → \
        // (PG uses backslash-escaping inside quoted array elements)
        if inner.contains('\\') {
            inner = inner.replace("\\\"", "\"").replace("\\\\", "\\");
        }
        // If the inner content looks like a JSON object or array, parse it
        if inner.starts_with('{') || inner.starts_with('[') {
            if let Ok(v) = serde_json::from_str(&inner) {
                return v;
            }
        }
        return serde_json::Value::String(inner);
    }

    // Try integer
    if let Ok(n) = trimmed.parse::<i64>() {
        return serde_json::Value::Number(n.into());
    }

    // Try float
    if let Ok(n) = trimmed.parse::<f64>() {
        if let Some(v) = serde_json::Number::from_f64(n) {
            return serde_json::Value::Number(v);
        }
    }

    // Boolean
    if trimmed.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }

    // Plain string
    serde_json::Value::String(trimmed.to_string())
}

/// Convert a PostgreSQL array string (`{1,2,3}`) to a `serde_json::Value::Array`.
///
/// Handles nested arrays, quoted elements, nulls, numbers, and booleans.
/// If the input is not a valid PG array, returns it as a plain JSON string.
/// `depth` limits recursion for nested arrays to prevent stack overflow.
#[allow(dead_code)]
pub fn pg_array_to_json(s: &str) -> serde_json::Value {
    pg_array_to_json_depth(s, 0)
}

#[allow(dead_code)]
fn pg_array_to_json_depth(s: &str, depth: u32) -> serde_json::Value {
    if depth > 16 {
        return serde_json::Value::String(s.to_string());
    }
    match parse_pg_array(s) {
        Some(elements) => {
            let arr: Vec<serde_json::Value> = elements
                .into_iter()
                .map(|e| pg_element_to_json(&e, depth + 1))
                .collect();
            serde_json::Value::Array(arr)
        }
        None => serde_json::Value::String(s.to_string()),
    }
}

/// Check whether a PG type OID maps to an SR type that can be used as a PRIMARY KEY.
///
/// SR does not support all data types as key columns. Incompatible types include:
/// JSON, VARBINARY, FLOAT, DOUBLE, DECIMAL, ARRAY, MAP, STRUCT.
/// Compatible types: BOOLEAN, SMALLINT, INT, BIGINT, STRING/VARCHAR, DATE, DATETIME.
///
/// Uses the existing `pg_type_to_sr` mapping internally — if the resulting SR type
/// is one of the known-compatible key types, returns true.
pub fn is_pg_type_sr_pk_compatible(type_oid: u32, type_modifier: i32) -> bool {
    let sr_type = pg_type_to_sr(type_oid, type_modifier);
    // SR types that ARE compatible as PRIMARY KEY columns.
    // Based on empirical testing against StarRocks 3.5.18.
    // DECIMAL is supported as a PK column in StarRocks 3.x (primary key model).
    matches!(
        sr_type.as_str(),
        "BOOLEAN" | "SMALLINT" | "INT" | "BIGINT" | "STRING" | "DATE" | "DATETIME"
    ) || sr_type.starts_with("VARCHAR(")
        || sr_type.starts_with("DECIMAL(")
}

/// Map a SR type name string (from polyglot-sql transpilation) to a canonical
/// StarRocks type name. This is the type-name-based equivalent of `pg_type_to_sr`
/// (which maps PG OIDs), used by the DDL poller's post-processing path.
///
/// Known SR type names pass through (with minor normalization: FLOAT4/REAL → FLOAT,
/// FLOAT8 → DOUBLE). Parameterized types (VARCHAR(n), DECIMAL(p,s), ARRAY<...>,
/// MAP<...>, STRUCT<...>) pass through as-is. PG-only types (INTERVAL → BIGINT,
/// MONEY → DECIMAL(38,2)) are remapped. Everything else falls back to STRING.
///
/// This is the shared canonical implementation — `map_unknown_sr_type` in the
/// DDL poller delegates here so all type-name-based mappings are centralized.
///
/// This function MUST mirror the OID-path mapping in `pg_type_to_sr`
/// above for every type. A table created via the AST DDL path
/// (`add_sr_table_clauses_from_expr` → `pg_type_name_to_sr`) and a table
/// created via the catalog fallback path (`pg_type_to_sr` with the OID) MUST
/// end up with identical SR column types — otherwise the live CDC path
/// (which uses the OID path) inserts into a column whose declared type
/// differs from the table's actual storage type, and queries may behave
/// inconsistently (e.g. MONEY stored as DECIMAL(38,18) vs DECIMAL(38,2),
/// TIMESTAMPTZ(6) stored as STRING vs DATETIME).
pub fn pg_type_name_to_sr(type_name: &str) -> String {
    let upper = type_name.to_uppercase();
    match true {
        // ── Exact-match passthroughs (polyglot already emits SR-native names) ──
        _ if upper == "BOOLEAN"
            || upper == "TINYINT"
            || upper == "SMALLINT"
            || upper == "INT"
            || upper == "INTEGER"
            || upper == "BIGINT"
            || upper == "LARGEINT"
            || upper == "FLOAT"
            || upper == "FLOAT4"
            || upper == "REAL"
            || upper == "DOUBLE"
            || upper == "FLOAT8"
            || upper == "NUMBER"
            || upper == "DATE"
            || upper == "DATETIME"
            || upper == "STRING"
            || upper == "TEXT"
            || upper == "JSON"
            || upper == "JSONB"
            || upper == "BITMAP"
            || upper == "HLL"
            || upper == "PERCENTILE"
            || upper == "VARBINARY"
            || upper == "BINARY"
            || upper == "BYTEA"
            || upper == "TIME" =>
        {
            match upper.as_str() {
                "FLOAT4" | "REAL" => "FLOAT".to_string(),
                "FLOAT8" => "DOUBLE".to_string(),
                // Normalize INTEGER → INT to match the OID path's
                // spelling (types.rs:84 `23 => "INT"`). Both are valid SR
                // type names but DDL path and CDC path must agree on the
                // exact string for downstream schema diff / consistency.
                "INTEGER" => "INT".to_string(),
                // Normalize JSONB → JSON (the OID path's spelling
                // for OID 3802). SR's JSON type is what's actually used.
                "JSONB" => "JSON".to_string(),
                "TIME" => "VARCHAR(20)".to_string(),
                "BYTEA" => "VARBINARY".to_string(),
                other => other.to_string(),
            }
        }
        // PG internal type-name aliases for INT/SMALLINT/BIGINT
        // (pg_type names int4/int2/int8 surface as DataType::Custom { name } in
        // polyglot; if the generator emits them by name, normalize them to the
        // SR canonical spelling so the column type matches the OID path).
        _ if upper == "INT4" => "INT".to_string(),
        _ if upper == "INT2" => "SMALLINT".to_string(),
        _ if upper == "INT8" => "BIGINT".to_string(),
        // TIMESTAMP / TIMESTAMPTZ — exact-match (no precision suffix) → DATETIME.
        // Existing exact-match arm catches "TIMESTAMP"/"TIMESTAMPTZ" but
        // not "TIMESTAMP(6)" / "TIMESTAMPTZ(6)" — those fall through to the
        // catch-all → STRING (silently wrong column type). Use starts_with arms
        // below to catch the precision-bearing forms too. Precision is dropped
        // (matches the OID path: 1114/1184 → DATETIME, no precision preserved).
        _ if upper == "TIMESTAMP" || upper == "TIMESTAMPTZ" => "DATETIME".to_string(),
        _ if upper.starts_with("TIMESTAMPTZ") || upper.starts_with("TIMESTAMP") => {
            "DATETIME".to_string()
        }
        _ if upper == "TIMETZ" => "VARCHAR(32)".to_string(),
        // Bounded-string PG types that previously fell through to STRING
        // (unbounded). Mirror the OID path's VARCHAR(N) declarations so DDL path
        // and CDC path agree.
        _ if upper == "UUID" => "VARCHAR(64)".to_string(),
        _ if upper == "INET" => "VARCHAR(64)".to_string(),
        _ if upper == "CIDR" => "VARCHAR(64)".to_string(),
        _ if upper == "MACADDR" => "VARCHAR(255)".to_string(),
        _ if upper == "MACADDR8" => "VARCHAR(255)".to_string(),
        _ if upper == "BIT"
            || upper == "VARBIT"
            || upper == "BIT VARYING"
            || upper.starts_with("BIT(")
            || upper.starts_with("VARBIT(")
            || upper.starts_with("BIT VARYING(") =>
        {
            "VARCHAR(255)".to_string()
        }
        // PG range types — mirror the OID path (3904/3906/3908/3910/3912/3926
        // → VARCHAR(255)). polyglot parses ranges to Custom{name}; the generator
        // emits the lowercase name verbatim, ergo the to_uppercase() above.
        _ if upper == "INT4RANGE"
            || upper == "INT8RANGE"
            || upper == "NUMRANGE"
            || upper == "TSRANGE"
            || upper == "TSTZRANGE"
            || upper == "DATERANGE" =>
        {
            "VARCHAR(255)".to_string()
        }
        _ if upper.starts_with("VARCHAR")
            || upper.starts_with("CHAR(")
            || upper.starts_with("DECIMAL(")
            || upper.starts_with("DEC(")
            || upper.starts_with("ARRAY<")
            || upper.starts_with("MAP<")
            || upper.starts_with("STRUCT<") =>
        {
            // SR caps DECIMAL precision at 38 and scale at 18. The OID path
            // (pg_type_to_sr above) maps oversized NUMERIC to STRING; the
            // name path must agree or CREATE TABLE / ADD COLUMN DDL is
            // rejected by SR (1064 → 3-retry → FATAL-ABORT under
            // ddl_failure_abort=true) for any NUMERIC(p,s) with p > 38.
            if upper.starts_with("DECIMAL(") || upper.starts_with("DEC(") {
                if let Some((p, s)) = parse_decimal_precision_scale(&upper) {
                    if p > 38 || s > 18 {
                        tracing::warn!(
                            "DECIMAL precision {p} / scale {s} exceeds SR limits (38/18) — \
                             mapping to STRING to avoid SR DDL rejection"
                        );
                        return "STRING".to_string();
                    }
                }
            }
            type_name.to_string()
        }
        _ if upper == "DECIMAL" || upper == "DEC" => "STRING".to_string(), // bare DECIMAL → STRING to avoid truncation (same as CDC path for NUMERIC without precision)
        _ if upper == "INTERVAL" => "BIGINT".to_string(),
        // MONEY must be DECIMAL(38,2) to match the OID path
        // (types.rs:110 `790 => "DECIMAL(38,2)"`). The prior DECIMAL(38,18)
        // declaration on the name path diverged from the CDC path's
        // DECIMAL(38,2) — same column stored at different scales depending on
        // whether the table was created via the AST path or the catalog
        // fallback path.
        _ if upper == "MONEY" => "DECIMAL(38,2)".to_string(),
        _ => {
            tracing::debug!("Mapping unknown SR type '{}' → STRING", type_name);
            "STRING".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping() {
        assert_eq!(pg_type_to_sr(21, 0), "SMALLINT");
        assert_eq!(pg_type_to_sr(23, 0), "INT");
        assert_eq!(pg_type_to_sr(20, 0), "BIGINT");
        assert_eq!(pg_type_to_sr(700, 0), "FLOAT");
        assert_eq!(pg_type_to_sr(701, 0), "DOUBLE");
        assert_eq!(pg_type_to_sr(16, 0), "BOOLEAN");
        assert_eq!(pg_type_to_sr(114, 0), "JSON");
        assert_eq!(pg_type_to_sr(3802, 0), "JSON");
        assert_eq!(pg_type_to_sr(1082, 0), "DATE");
        assert_eq!(pg_type_to_sr(1114, 0), "DATETIME");
        assert_eq!(pg_type_to_sr(1184, 0), "DATETIME");
        assert_eq!(pg_type_to_sr(2950, 0), "VARCHAR(64)");
        assert_eq!(pg_type_to_sr(25, 0), "STRING");
        // Range types — must map to VARCHAR(255)
        assert_eq!(pg_type_to_sr(3904, 0), "VARCHAR(255)"); // int4range
        assert_eq!(pg_type_to_sr(3906, 0), "VARCHAR(255)"); // numrange
        assert_eq!(pg_type_to_sr(3908, 0), "VARCHAR(255)"); // tsrange
        assert_eq!(pg_type_to_sr(3910, 0), "VARCHAR(255)"); // tstzrange
        assert_eq!(pg_type_to_sr(3912, 0), "VARCHAR(255)"); // daterange
        assert_eq!(pg_type_to_sr(3926, 0), "VARCHAR(255)"); // int8range
                                                            // Array range OIDs — now map to ARRAY<VARCHAR(255)>
        assert_eq!(pg_type_to_sr(3905, 0), "ARRAY<VARCHAR(255)>"); // _int4range
        assert_eq!(pg_type_to_sr(3907, 0), "ARRAY<VARCHAR(255)>"); // _numrange
        assert_eq!(pg_type_to_sr(17, 0), "VARBINARY");
        assert_eq!(pg_type_to_sr(142, 0), "STRING");
        // Composite / domain / custom types
        assert_eq!(pg_type_to_sr(2249, 0), "STRING"); // composite/record → STRING (unbounded)
        assert_eq!(pg_type_to_sr(2275, 0), "STRING"); // cstring (domains → STRING)
        assert_eq!(pg_type_to_sr(99999, 0), "STRING");
    }

    #[test]
    fn test_pg_type_name_to_sr_custom_types() {
        // pg_type_name_to_sr is the shared canonical type-name-based mapping.
        // Custom type names (enums, composites, domains) fall to the catch-all → STRING.
        assert_eq!(pg_type_name_to_sr("mood"), "STRING");
        assert_eq!(pg_type_name_to_sr("mood2"), "STRING");
        assert_eq!(pg_type_name_to_sr("us_postal_code"), "STRING");
        assert_eq!(pg_type_name_to_sr("address"), "STRING");
        assert_eq!(pg_type_name_to_sr("public.mood"), "STRING");
        assert_eq!(pg_type_name_to_sr("pg_catalog.tsvector"), "STRING");
        // Known types pass through correctly
        assert_eq!(pg_type_name_to_sr("INT"), "INT");
        assert_eq!(pg_type_name_to_sr("TEXT"), "TEXT"); // TEXT passes through (remapped to STRING by DDL post-process)
        assert_eq!(pg_type_name_to_sr("VARCHAR(50)"), "VARCHAR(50)");
        assert_eq!(pg_type_name_to_sr("BIGINT"), "BIGINT");
        assert_eq!(pg_type_name_to_sr("DECIMAL"), "STRING"); // bare DECIMAL → STRING to avoid truncation
        // MONEY must be DECIMAL(38,2) to match the OID path (types.rs:110).
        // Prior DECIMAL(38,18) drifted from the CDC runtime mapping.
        assert_eq!(pg_type_name_to_sr("MONEY"), "DECIMAL(38,2)");
        assert_eq!(pg_type_name_to_sr("INTERVAL"), "BIGINT");
        assert_eq!(pg_type_name_to_sr("REAL"), "FLOAT");
        assert_eq!(pg_type_name_to_sr("FLOAT4"), "FLOAT");
        assert_eq!(pg_type_name_to_sr("FLOAT8"), "DOUBLE");
        // PG internal type-name aliases (pg_type.int4/int2/int8) — polyglot
        // parses these to DataType::Custom{name}; the generator emits the name
        // verbatim. These MUST normalize to the canonical SR spellings.
        assert_eq!(pg_type_name_to_sr("INT4"), "INT");
        assert_eq!(pg_type_name_to_sr("INT2"), "SMALLINT");
        assert_eq!(pg_type_name_to_sr("INT8"), "BIGINT");
        // Bounded-string PG types — must mirror the OID path's VARCHAR(N)
        // rather than catch-all STRING (unbounded).
        assert_eq!(pg_type_name_to_sr("UUID"), "VARCHAR(64)");
        assert_eq!(pg_type_name_to_sr("INET"), "VARCHAR(64)");
        assert_eq!(pg_type_name_to_sr("CIDR"), "VARCHAR(64)");
        assert_eq!(pg_type_name_to_sr("MACADDR"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("MACADDR8"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("BIT"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("BIT(5)"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("VARBIT"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("VARBIT(8)"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("BIT VARYING"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("BIT VARYING(16)"), "VARCHAR(255)");
        // TIMESTAMP/TIMESTAMPTZ with precision — exact-match arm catches
        // bare "TIMESTAMP"/"TIMESTAMPTZ"; the starts_with arm catches the
        // precision-bearing forms. Both → DATETIME (precision dropped, matches
        // the OID path: 1114/1184 → DATETIME).
        assert_eq!(pg_type_name_to_sr("TIMESTAMP"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("TIMESTAMPTZ"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("TIMESTAMP(6)"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("TIMESTAMPTZ(6)"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("TIMESTAMP(0)"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("TIMESTAMPTZ(0)"), "DATETIME");
        // PG range types — mirror the OID path (3904/3906/3908/3910/3912/3926).
        assert_eq!(pg_type_name_to_sr("INT4RANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("INT8RANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("NUMRANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("TSRANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("TSTZRANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("DATERANGE"), "VARCHAR(255)");
        assert_eq!(pg_type_name_to_sr("int4range"), "VARCHAR(255)"); // case-insensitive
    }

    /// Assert the name path (`pg_type_name_to_sr`, used by the DDL
    /// AST path) agrees with the OID path (`pg_type_to_sr`, used by the
    /// catalog fallback path AND the live CDC runtime) for every type that
    /// can appear in both. If this test fails, a table created via the AST
    /// path vs the catalog fallback path would have different SR column
    /// types for the same PG schema → silent data-loss / query failures.
    #[test]
    fn test_pg_type_name_to_sr_matches_oid_path() {
        // (PG type name, OID, expected SR type) — both paths must agree.
        // For OIDs without a meaningful "name" string the name path is N/A;
        // we only test types whose DDL text and pg_catalog both expose.
        let cases: &[(&str, u32, &str)] = &[
            ("SMALLINT", 21, "SMALLINT"),
            ("INT", 23, "INT"),
            ("INTEGER", 23, "INT"),
            ("BIGINT", 20, "BIGINT"),
            ("REAL", 700, "FLOAT"),
            ("FLOAT4", 700, "FLOAT"),
            ("DOUBLE", 701, "DOUBLE"),
            ("FLOAT8", 701, "DOUBLE"),
            ("DATE", 1082, "DATE"),
            ("TIME", 1083, "VARCHAR(20)"),
            ("TIMETZ", 1266, "VARCHAR(32)"),
            ("TIMESTAMP", 1114, "DATETIME"),
            ("TIMESTAMPTZ", 1184, "DATETIME"),
            ("BYTEA", 17, "VARBINARY"),
            ("JSONB", 3802, "JSON"),
            ("UUID", 2950, "VARCHAR(64)"),
            ("INET", 869, "VARCHAR(64)"),
            ("CIDR", 650, "VARCHAR(64)"),
            ("MACADDR", 150, "VARCHAR(255)"),
            ("MACADDR8", 4144, "VARCHAR(255)"),
            ("BIT", 1560, "VARCHAR(255)"),
            ("VARBIT", 1562, "VARCHAR(255)"),
            ("INTERVAL", 1186, "BIGINT"),
            ("MONEY", 790, "DECIMAL(38,2)"),
            ("INT4RANGE", 3904, "VARCHAR(255)"),
            ("INT8RANGE", 3926, "VARCHAR(255)"),
            ("NUMRANGE", 3906, "VARCHAR(255)"),
            ("TSRANGE", 3908, "VARCHAR(255)"),
            ("TSTZRANGE", 3910, "VARCHAR(255)"),
            ("DATERANGE", 3912, "VARCHAR(255)"),
        ];
        for (name, oid, expected) in cases {
            let from_oid = pg_type_to_sr(*oid, 0);
            let from_name = pg_type_name_to_sr(name);
            assert_eq!(
                from_oid, *expected,
                "OID-path mismatch for {} (oid={}): expected {}, got {}",
                name, oid, expected, from_oid
            );
            assert_eq!(
                from_name, from_oid,
                "name path ({:?} → {}) diverges from OID path (oid={} → {}). \
                 A table created via the AST DDL path would have a different \
                 SR column type than one created via the catalog fallback path, \
                 and the live CDC path (which uses the OID) would also disagree.",
                name, from_name, oid, from_oid
            );
        }
    }

    #[test]
    fn test_type_mapping_bounded_types_capped() {
        // CHAR(N) and VARCHAR(N) both map to STRING (no length issues)
        assert_eq!(pg_type_to_sr(1042, 204), "STRING"); // CHAR(200)
        assert_eq!(pg_type_to_sr(1042, 14), "STRING"); // CHAR(10)
        assert_eq!(pg_type_to_sr(1042, 0), "STRING"); // CHAR w/o modifier

        assert_eq!(pg_type_to_sr(1043, 30004), "STRING"); // VARCHAR(30000)
        assert_eq!(pg_type_to_sr(1043, 14), "STRING"); // VARCHAR(10)
        assert_eq!(pg_type_to_sr(1043, 0), "STRING"); // VARCHAR w/o modifier
    }

    #[test]
    fn test_array_type_mapping() {
        // PG array types now map to ARRAY<element_type> in SR
        assert_eq!(pg_type_to_sr(1007, 0), "ARRAY<INT>"); // _int4
        assert_eq!(pg_type_to_sr(1005, 0), "ARRAY<SMALLINT>"); // _int2
        assert_eq!(pg_type_to_sr(1016, 0), "ARRAY<BIGINT>"); // _int8
        assert_eq!(pg_type_to_sr(1000, 0), "ARRAY<BOOLEAN>"); // _bool
        assert_eq!(pg_type_to_sr(1009, 0), "ARRAY<STRING>"); // _text
        assert_eq!(pg_type_to_sr(199, 0), "ARRAY<JSON>"); // _json
        assert_eq!(pg_type_to_sr(1115, 0), "ARRAY<DATETIME>"); // _timestamp
        assert_eq!(pg_type_to_sr(1182, 0), "ARRAY<DATE>"); // _date
        assert_eq!(pg_type_to_sr(2951, 0), "ARRAY<VARCHAR(64)>"); // _uuid
        assert_eq!(pg_type_to_sr(1021, 0), "ARRAY<FLOAT>"); // _float4
        assert_eq!(pg_type_to_sr(1022, 0), "ARRAY<DOUBLE>"); // _float8
        assert_eq!(pg_type_to_sr(1231, 0), "ARRAY<STRING>"); // _numeric
        assert_eq!(pg_type_to_sr(1561, 0), "ARRAY<VARCHAR(255)>"); // _bit (VARCHAR to preserve bit-string text)
    }

    #[test]
    fn test_parse_pg_array_simple() {
        let elements = parse_pg_array("{1,2,3}").unwrap();
        assert_eq!(elements, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_parse_pg_array_strings() {
        let elements = parse_pg_array("{abc,def,ghi}").unwrap();
        assert_eq!(elements, vec!["abc", "def", "ghi"]);
    }

    #[test]
    fn test_parse_pg_array_quoted() {
        let elements = parse_pg_array(r#"{a,"b, c",d}"#).unwrap();
        assert_eq!(elements, vec!["a", r#""b, c""#, "d"]);
    }

    #[test]
    fn test_parse_pg_array_nested() {
        let elements = parse_pg_array("{{1,2},{3,4}}").unwrap();
        assert_eq!(elements, vec!["{1,2}", "{3,4}"]);
    }

    #[test]
    fn test_parse_pg_array_unbalanced_braces() {
        // Extra close brace — should not panic and should parse what it can
        let elements = parse_pg_array("{1,2}}").unwrap();
        assert_eq!(elements, vec!["1", "2}"]);
    }

    #[test]
    fn test_parse_pg_array_empty() {
        let elements = parse_pg_array("{}").unwrap();
        assert!(elements.is_empty());
    }

    #[test]
    fn test_pg_array_to_json_numbers() {
        let json = pg_array_to_json("{1,2,3}");
        assert_eq!(json, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_pg_array_to_json_strings() {
        let json = pg_array_to_json("{abc,def}");
        assert_eq!(json, serde_json::json!(["abc", "def"]));
    }

    #[test]
    fn test_pg_array_to_json_mixed() {
        let json = pg_array_to_json(r#"{1,NULL,true,hello}"#);
        assert_eq!(json, serde_json::json!([1, null, true, "hello"]));
    }

    #[test]
    fn test_pg_array_to_json_quoted() {
        let json = pg_array_to_json(r#"{"hello world","foo,bar"}"#);
        assert_eq!(json, serde_json::json!(["hello world", "foo,bar"]));
    }

    #[test]
    fn test_pg_array_to_json_nested() {
        let json = pg_array_to_json("{{1,2},{3,4}}");
        assert_eq!(json, serde_json::json!([[1, 2], [3, 4]]));
    }

    #[test]
    fn test_pg_array_to_json_empty() {
        let json = pg_array_to_json("{}");
        assert_eq!(json, serde_json::json!([]));
    }

    #[test]
    fn test_pg_array_to_json_floats() {
        let json = pg_array_to_json("{1.5,2.5,3.0}");
        assert_eq!(json, serde_json::json!([1.5, 2.5, 3.0]));
    }

    #[test]
    fn test_pg_array_to_json_jsonb_elements() {
        // PG sends JSONB array with quoted JSON objects: {"{\"key\": 1}","{\"key\": 2}"}
        let json = pg_array_to_json(r#"{"{\"key\": 1}","{\"key\": 2}"}"#);
        assert_eq!(json, serde_json::json!([{"key": 1}, {"key": 2}]));
    }

    #[test]
    fn test_pg_array_to_json_not_array() {
        let json = pg_array_to_json("just a string");
        assert_eq!(json, serde_json::Value::String("just a string".to_string()));
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_integer_types() {
        assert!(is_pg_type_sr_pk_compatible(20, 0)); // BIGINT
        assert!(is_pg_type_sr_pk_compatible(21, 0)); // SMALLINT
        assert!(is_pg_type_sr_pk_compatible(23, 0)); // INT
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_string_types() {
        assert!(is_pg_type_sr_pk_compatible(25, 0)); // TEXT → STRING
        assert!(is_pg_type_sr_pk_compatible(1042, 0)); // CHAR → STRING
        assert!(is_pg_type_sr_pk_compatible(1043, 0)); // VARCHAR → STRING
        assert!(is_pg_type_sr_pk_compatible(142, 0)); // XML → STRING
        assert!(is_pg_type_sr_pk_compatible(2275, 0)); // CSTRING → STRING
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_date_time() {
        assert!(is_pg_type_sr_pk_compatible(1082, 0)); // DATE
        assert!(is_pg_type_sr_pk_compatible(1114, 0)); // TIMESTAMP
        assert!(is_pg_type_sr_pk_compatible(1184, 0)); // TIMESTAMPTZ
        assert!(is_pg_type_sr_pk_compatible(1083, 0)); // TIME → VARCHAR(20)
        assert!(is_pg_type_sr_pk_compatible(1266, 0)); // TIMETZ → VARCHAR(32)
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_varchar_mapped() {
        assert!(is_pg_type_sr_pk_compatible(2950, 0)); // UUID → VARCHAR(64)
        assert!(is_pg_type_sr_pk_compatible(869, 0)); // INET → VARCHAR(64)
        assert!(is_pg_type_sr_pk_compatible(650, 0)); // CIDR → VARCHAR(64)
        assert!(is_pg_type_sr_pk_compatible(150, 0)); // MACADDR → VARCHAR(255)
        assert!(is_pg_type_sr_pk_compatible(4144, 0)); // MACADDR8 → VARCHAR(255)
        assert!(is_pg_type_sr_pk_compatible(3904, 0)); // INT4RANGE → VARCHAR(255)
        assert!(is_pg_type_sr_pk_compatible(600, 0)); // POINT → VARCHAR(255)
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_composite_domain() {
        // Composite types (OID 2249 → STRING) are PK compatible
        assert!(is_pg_type_sr_pk_compatible(2249, 0)); // record/composite → STRING
                                                       // Domain types (dynamic OID → STRING) are PK compatible via catch-all
        assert!(is_pg_type_sr_pk_compatible(2275, 0)); // cstring → STRING
                                                       // Custom unknown types (any other dynamic OID → STRING) are PK compatible
        assert!(is_pg_type_sr_pk_compatible(88888, 0)); // unknown → STRING
    }

    #[test]
    fn test_pg_type_sr_pk_incompatible_json() {
        assert!(!is_pg_type_sr_pk_compatible(114, 0)); // JSON
        assert!(!is_pg_type_sr_pk_compatible(3802, 0)); // JSONB
    }

    #[test]
    fn test_pg_type_sr_pk_incompatible_binary() {
        assert!(!is_pg_type_sr_pk_compatible(17, 0)); // BYTEA → VARBINARY
        // BIT/VARBIT now map to VARCHAR(255) (PK-compatible) to avoid
        // silent hex-corruption when stored as VARBINARY.
        assert!(is_pg_type_sr_pk_compatible(1560, 0)); // BIT → VARCHAR(255)
        assert!(is_pg_type_sr_pk_compatible(1562, 0)); // VARBIT → VARCHAR(255)
    }

    #[test]
    fn test_pg_type_sr_pk_incompatible_float_double() {
        assert!(!is_pg_type_sr_pk_compatible(700, 0)); // FLOAT
        assert!(!is_pg_type_sr_pk_compatible(701, 0)); // DOUBLE
    }

    #[test]
    fn test_pg_type_sr_pk_compatible_decimal() {
        // DECIMAL is supported as a PK column in StarRocks 3.x (primary key model).
        // NUMERIC without precision defaults to STRING — PK compatible.
        assert!(is_pg_type_sr_pk_compatible(1700, 0)); // NUMERIC → STRING
        assert!(is_pg_type_sr_pk_compatible(1700, 4 << 16 | 2)); // NUMERIC(2,1) → DECIMAL(2,1)
        assert!(is_pg_type_sr_pk_compatible(790, 0)); // MONEY → DECIMAL(38,2)
    }

    #[test]
    fn test_pg_type_to_sr_numeric_no_precision_falls_back_to_string() {
        // NUMERIC without explicit precision/scale must map to STRING,
        // not DECIMAL(38,18), to avoid silent truncation of high-precision values.
        assert_eq!(pg_type_to_sr(1700, 0), "STRING");
        // NUMERIC with explicit precision/scale still uses DECIMAL.
        // type_modifier encodes (precision << 16 | scale) + 4 (VARHDRSZ).
        let mod_dec_4_2 = (4 << 16 | 2) + 4;
        assert_eq!(pg_type_to_sr(1700, mod_dec_4_2), "DECIMAL(4,2)");
        // NUMERIC with only precision still uses DECIMAL.
        let mod_dec_10 = (10 << 16) + 4;
        assert_eq!(pg_type_to_sr(1700, mod_dec_10), "DECIMAL(10)");
    }

    #[test]
    fn test_pg_type_sr_pk_incompatible_array() {
        assert!(!is_pg_type_sr_pk_compatible(1007, 0)); // _int4 → ARRAY<INT>
        assert!(!is_pg_type_sr_pk_compatible(1009, 0)); // _text → ARRAY<STRING>
        assert!(!is_pg_type_sr_pk_compatible(199, 0)); // _json → ARRAY<JSON>
    }

    #[test]
    fn test_pg_type_name_to_sr_bytea_maps_to_varbinary() {
        // DDL transpile path must map BYTEA to VARBINARY, not STRING, to match
        // the CDC catalog path (OID 17 → VARBINARY) which applies unhex().
        assert_eq!(pg_type_name_to_sr("BYTEA"), "VARBINARY");
        assert_eq!(pg_type_name_to_sr("bytea"), "VARBINARY");
    }

    #[test]
    fn test_pg_type_name_to_sr_timestamptz_maps_to_datetime() {
        // DDL transpile path must map TIMESTAMPTZ to DATETIME to match CDC path.
        assert_eq!(pg_type_name_to_sr("TIMESTAMPTZ"), "DATETIME");
        assert_eq!(pg_type_name_to_sr("timestamptz"), "DATETIME");
    }

    #[test]
    fn test_pg_type_name_to_sr_time_timetz_mapped_correctly() {
        // DDL transpile path must match CDC path.
        assert_eq!(pg_type_name_to_sr("TIME"), "VARCHAR(20)");
        assert_eq!(pg_type_name_to_sr("TIMETZ"), "VARCHAR(32)");
    }

    #[test]
    fn test_pg_type_name_to_sr_bare_decimal_maps_to_string() {
        // Bare DECIMAL/DEC must map to STRING to avoid truncation, matching CDC path.
        assert_eq!(pg_type_name_to_sr("DECIMAL"), "STRING");
        assert_eq!(pg_type_name_to_sr("DEC"), "STRING");
        assert_eq!(pg_type_name_to_sr("decimal"), "STRING");
        assert_eq!(pg_type_name_to_sr("dec"), "STRING");
        // DECIMAL with precision still passes through.
        assert_eq!(pg_type_name_to_sr("DECIMAL(38,18)"), "DECIMAL(38,18)");
        assert_eq!(pg_type_name_to_sr("DECIMAL(10,2)"), "DECIMAL(10,2)");
        // Oversized precision/scale must map to STRING (SR caps at 38/18) —
        // matching the OID path, so the DDL is never rejected by SR.
        assert_eq!(pg_type_name_to_sr("DECIMAL(50,10)"), "STRING");
        assert_eq!(pg_type_name_to_sr("DECIMAL(38,20)"), "STRING");
        assert_eq!(pg_type_name_to_sr("DEC(40,2)"), "STRING");
    }

    #[test]
    fn test_pg_type_to_sr_time_has_margin() {
        // TIME → VARCHAR(20) not VARCHAR(15) — must have safety margin.
        assert_eq!(pg_type_to_sr(1083, 0), "VARCHAR(20)");
        // _time array too.
        assert_eq!(pg_type_to_sr(1183, 0), "ARRAY<VARCHAR(20)>");
    }

    #[test]
    fn test_pg_array_to_json_depth_limit() {
        // Deeply nested arrays must NOT cause stack overflow. The function should
        // gracefully fall back to string representation for elements past depth 16.
        // Build a deep array: {{{...{1}...}}}
        let mut input = String::from("{");
        for _ in 0..8 {
            input.push('{');
        }
        input.push('1');
        for _ in 0..8 {
            input.push('}');
        }
        // Must not panic (stack overflow) — this is the real test.
        let json = pg_array_to_json(&input);
        // Should produce some valid JSON value (array or string), never panic.
        assert!(json.is_array() || json.is_string(),
            "Deeply nested array should produce valid JSON, got {json:?}");
    }

    #[test]
    fn test_pg_array_to_json_depth_within_limit() {
        // 2-deep nested array should still parse correctly.
        let json = pg_array_to_json("{{1,2},{3,4}}");
        assert_eq!(json, serde_json::json!([[1, 2], [3, 4]]));
    }

    // ── Regression tests ──────────────────────────────

    #[test]
    fn test_numeric_precision_clamp_to_string() {
        // NUMERIC(50, 10) — precision > 38 must map to STRING, not
        // DECIMAL(50,10) which SR rejects.
        let typmod = ((50_i64 << 16) | 10) as i32 + 4; // PG adds VARHDRSZ=4
        let result = pg_type_to_sr(1700, typmod);
        assert_eq!(
            result, "STRING",
            "NUMERIC(50,10) must map to STRING (SR DECIMAL max is 38)"
        );

        // NUMERIC(38, 18) — at the SR limit, should produce DECIMAL(38,18)
        let typmod_38 = ((38_i64 << 16) | 18) as i32 + 4;
        assert_eq!(
            pg_type_to_sr(1700, typmod_38),
            "DECIMAL(38,18)",
            "NUMERIC(38,18) should fit SR's limit"
        );

        // NUMERIC(1000, 0) — extreme PG value
        let typmod_1000 = (1000_i64 << 16) as i32 + 4;
        assert_eq!(
            pg_type_to_sr(1700, typmod_1000),
            "STRING",
            "NUMERIC(1000) must map to STRING"
        );

        // NUMERIC(38, 20) — precision within SR's limit, but scale > 18 must map
        // to STRING instead of silently clamping the fractional digits.
        let typmod_38_20 = ((38_i64 << 16) | 20) as i32 + 4;
        assert_eq!(
            pg_type_to_sr(1700, typmod_38_20),
            "STRING",
            "NUMERIC(38,20) scale > 18 must map to STRING (no silent truncation)"
        );

        // NUMERIC(10, 20) — precision < scale is invalid in PG, but if it ever
        // arrives over the wire, scale > 18 must still funnel to STRING.
        let typmod_10_20 = ((10_i64 << 16) | 20) as i32 + 4;
        assert_eq!(
            pg_type_to_sr(1700, typmod_10_20),
            "STRING",
            "NUMERIC(10,20) scale > 18 must map to STRING"
        );
    }

    #[test]
    fn test_bit_varbit_mapped_to_varchar() {
        // BIT and VARBIT must map to VARCHAR, not VARBINARY.
        // VARBINARY would cause silent hex corruption (bit-string "10101010"
        // interpreted as hex pairs → 4 bytes of 0x10 instead of 1 byte 0xAA).
        assert_eq!(
            pg_type_to_sr(1560, 0),
            "VARCHAR(255)",
            "BIT must map to VARCHAR(255)"
        );
        assert_eq!(
            pg_type_to_sr(1562, 0),
            "VARCHAR(255)",
            "VARBIT must map to VARCHAR(255)"
        );
        // Array types too
        assert_eq!(
            pg_type_to_sr(1561, 0),
            "ARRAY<VARCHAR(255)>",
            "_bit array must map to ARRAY<VARCHAR(255)>"
        );
        assert_eq!(
            pg_type_to_sr(1563, 0),
            "ARRAY<VARCHAR(255)>",
            "_varbit array must map to ARRAY<VARCHAR(255)>"
        );
    }

    #[test]
    fn test_pg_array_element_oid_mapping() {
        // Verify the reverse mapping (array OID → element OID) used by
        // the typed array refactor to dispatch per-element coercion.
        assert_eq!(pg_array_element_oid(1000), Some(16));    // _bool → bool
        assert_eq!(pg_array_element_oid(1007), Some(23));    // _int4 → int4
        assert_eq!(pg_array_element_oid(1009), Some(25));    // _text → text
        assert_eq!(pg_array_element_oid(1185), Some(1184));  // _timestamptz → timestamptz
        assert_eq!(pg_array_element_oid(1187), Some(1186));  // _interval → interval
        assert_eq!(pg_array_element_oid(791), Some(790));    // _money → money
        assert_eq!(pg_array_element_oid(1001), Some(17));    // _bytea → bytea
        // Unknown array OID returns None
        assert_eq!(pg_array_element_oid(99999), None);
    }
}
