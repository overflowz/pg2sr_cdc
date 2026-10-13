//! Value conversion — type-aware PG → JSON coercion for CDC events.
//!
//! These functions are called by `Pipeline::process_event` to convert WAL
//! column values (`ColValue::Value(s)`) into the appropriate JSON
//! representation based on the column's PG type OID (JSON/JSONB, BYTEA,
//! BOOL, INTERVAL, TIMESTAMPTZ, MONEY, FLOAT4/8, arrays, etc.).
//!
//! Pure functions: no I/O, no shared mutable state (modulo the lazy OnceLock
//! regex and format descriptors). Safe to call from any async context.

use anyhow::Result;
use std::sync::LazyLock;

use crate::cdc::ColValue;

use super::ColInfo;

static INTERVAL_TIME_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    // LazyLock replaces OnceLock::get_or_init(...).unwrap().
    regex::Regex::new(r"([+-]?)(\d+):(\d+):(\d+)(?:\.(\d+))?")
        .expect("INTERVAL_TIME_RE: hardcoded valid regex")
});

fn time_fmt_with_subsecond() -> &'static Vec<time::format_description::FormatItem<'static>> {
    static FMT: LazyLock<Vec<time::format_description::FormatItem<'static>>> =
        LazyLock::new(|| {
            time::format_description::parse(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]",
            )
            .expect("Invalid time format with subsecond")
        });
    &FMT
}

fn time_fmt_no_subsecond() -> &'static Vec<time::format_description::FormatItem<'static>> {
    static FMT: LazyLock<Vec<time::format_description::FormatItem<'static>>> =
        LazyLock::new(|| {
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
                .expect("Invalid time format without subsecond")
        });
    &FMT
}

/// Convert a Vec of raw column values to a JSON object using the column names.
/// `ColValue::Unchanged` columns are OMITTED from the JSON output — Stream Load
/// won't modify them, preventing TOAST corruption on UPDATE. `ColValue::Null`
/// maps to JSON `null`, and `ColValue::Value(s)` maps to the appropriate JSON value
/// based on the column's type OID.
/// Returns an error when the WAL tuple has more values than the relation cache columns,
/// which indicates a schema mismatch (e.g., a new column added via DDL that hasn't
/// been reflected in the relation cache yet). The caller should reload relations and retry.
///
/// The production hot path now uses `serialize_row` (which writes JSON
/// directly to a buffer + produces a `Box<RawValue>`, eliminating the per-row
/// `Map` allocation). This function is retained for tests that assert on the
/// returned `Value` structure.
#[cfg(test)]
pub(crate) fn columns_to_json(
    columns: &[ColInfo],
    values: &[ColValue],
    buf: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value> {
    buf.clear();
    let map = buf;
    for (i, col) in columns.iter().enumerate() {
        if i >= values.len() {
            tracing::warn!(
                "Column count mismatch: table has {} columns but WAL tuple has only {} values. \
                 Missing values for columns: {:?} — filling remaining with null",
                columns.len(),
                values.len(),
                columns[i..].iter().map(|c| &c.name).collect::<Vec<_>>()
            );
            // Fill remaining columns as null rather than silently truncating
            for col in &columns[i..] {
                map.insert(col.name.clone(), serde_json::Value::Null);
            }
            break;
        }
        let col_value = &values[i];
        let col_name = &col.name;

        // ColValue::Unchanged means the TOAST column wasn't modified by this
        // UPDATE — omit the column from JSON output entirely so Stream Load
        // leaves the existing SR value intact.
        if matches!(col_value, ColValue::Unchanged) {
            continue;
        }

        let json_val = match col_value {
            ColValue::Null => serde_json::Value::Null,
            ColValue::Value(s) => string_to_json_value(s, col.type_oid),
            ColValue::Unchanged => {
                // Safety invariant: `Unchanged` is caught by the `matches!` above
                // and continued. If this arm is reached, the invariant is violated.
                // Return Null rather than panicking to avoid crashing the pipeline.
                serde_json::Value::Null
            }
        };

        map.insert(col_name.clone(), json_val);
    }

    // Detect extra WAL values beyond the known columns — this means the relation
    // cache is stale (a DDL added columns that haven't been reflected yet).
    // Return an error so the caller reloads relations and retries.
    if values.len() > columns.len() {
        return Err(anyhow::anyhow!(
            "Schema mismatch: WAL tuple has {} values but relation cache has only {} columns. \
             Extra values (probable DDL-sync lag): {:?}",
            values.len(),
            columns.len(),
            &values[columns.len()..],
        ));
    }

    Ok(serde_json::Value::Object(std::mem::replace(
        map,
        serde_json::Map::new(),
    )))
}

/// Returns `true` if the error is a schema mismatch (WAL has more values than
/// cached columns), which happens after ALTER TABLE ADD COLUMN when pgoutput
/// hasn't sent a Relation message before the next DML event.
pub(crate) fn is_schema_mismatch(err: &anyhow::Error) -> bool {
    err.to_string().contains("Schema mismatch")
}

/// Serialize one row directly into a reused `Vec<u8>` buffer and wrap the
/// result in a `Box<RawValue>` (1 allocation per row — the JSON text moved
/// into an `Arc<str>`), plus a pre-computed PK key string extracted during
/// serialization (so dedup never re-parses the JSON).
///
/// Replaces the legacy `columns_to_json` hot path which built a whole
/// `serde_json::Map` per row (N `String`-key clones + BTreeMap node
/// allocations, then re-serialized at flush). Here the JSON object is written
/// column-by-column straight into `buf` (cleared at the start, content moved
/// into the `RawValue` at the end so the buffer is reusable), and each PK
/// column's serialized-bytes are captured for the PK key.
///
/// `ColValue::Unchanged` (TOAST) is omitted — matches `columns_to_json`.
/// Schema-mismatch (more WAL values than cached columns) returns the same
/// `Err` shape as `columns_to_json` so `is_schema_mismatch` still routes the
/// caller's reload-and-retry.
///
/// `pk_columns` empty → no PK key is captured (returns `Ok((raw, None))`),
/// matching the legacy dedup fallback (keep every op when no PK).
pub(crate) fn serialize_row(
    columns: &[ColInfo],
    values: &[ColValue],
    pk_columns: &[String],
    buf: &mut Vec<u8>,
) -> Result<(Box<serde_json::value::RawValue>, Option<String>)> {
    buf.clear();
    buf.push(b'{');
    let mut first = true;
    // PK column indices into `columns` (resolved by name once per row).
    // `None` = the PK column is not in the cached schema → the row's PK key is
    // UNRESOLVED and `serialize_row` returns `None` for it (see below), which
    // keeps every op through dedup's per-op sentinel instead of merging
    // distinct rows under one key.
    let pk_idx: Vec<Option<usize>> = if pk_columns.is_empty() {
        Vec::new()
    } else {
        pk_columns
            .iter()
            .map(|pk| columns.iter().position(|c| &c.name == pk))
            .collect()
    };
    // PK key parts: parallel to pk_columns; a shorter vec than pk_idx means at
    // least one PK value was unresolved (absent from the WAL tuple, missing
    // column, or an unchanged TOAST datum) → the caller returns `None`.
    let mut pk_parts: Vec<String> = Vec::with_capacity(pk_idx.len());

    for (i, col) in columns.iter().enumerate() {
        // Missing WAL values → fill remaining columns as null (logs + fills,
        // matching columns_to_json). Don't capture PKs for missing values.
        if i >= values.len() {
            if !first {
                buf.push(b',');
            }
            first = false;
            // "name":null  (name written as raw bytes — no String alloc)
            buf.push(b'"');
            buf.extend_from_slice(col.name.as_bytes());
            buf.extend_from_slice(b"\":null");
            continue;
        }
        let col_value = &values[i];
        // ColValue::Unchanged → omit (TOAST; partial_columns header keeps the
        // existing SR value).
        if matches!(col_value, ColValue::Unchanged) {
            continue;
        }
        if !first {
            buf.push(b',');
        }
        first = false;
        buf.push(b'"');
        buf.extend_from_slice(col.name.as_bytes());
        buf.push(b'"');
        buf.push(b':');
        // Serialize the JSON value via the same type-aware path, then write.
        let json_val = match col_value {
            ColValue::Null => serde_json::Value::Null,
            ColValue::Value(s) => string_to_json_value(s, col.type_oid),
            ColValue::Unchanged => unreachable!(), // handled above
        };
        let value_start = buf.len();
        serde_json::to_writer(&mut *buf, &json_val)
            .map_err(|e| anyhow::anyhow!("Failed to serialize column {}: {e}", col.name))?;
        if pk_idx.contains(&Some(i)) {
            // Capture the serialized value text (matches legacy `v.to_string()`
            // which `extract_pk_key` produced). e.g. 42 → "42", "foo" → "\"foo\"".
            let val_text = String::from_utf8_lossy(&buf[value_start..]).into_owned();
            pk_parts.push(val_text);
        }
    }

    buf.push(b'}');

    if values.len() > columns.len() {
        return Err(anyhow::anyhow!(
            "Schema mismatch: WAL tuple has {} values but relation cache has only {} columns. \
             Extra values (probable DDL-sync lag): {:?}",
            values.len(),
            columns.len(),
            &values[columns.len()..],
        ));
    }

    let pk_key = if pk_columns.is_empty() {
        None
    } else if pk_idx.iter().any(|i| i.is_none()) || pk_parts.len() != pk_idx.len() {
        // Unresolved PK: a PK column missing from the cached schema, a WAL
        // value absent for it, or the value arriving as an unchanged TOAST
        // datum (omitted from the JSON above). Returning None makes
        // net-change dedup fall back to its per-op UNIQUE sentinel
        // (`__no_pk_key_{idx}__` in prep_table_flush), which keeps every op.
        //
        // The old behavior padded the missing parts with "?" and returned
        // Some("?|?"): every such row in a batch then shared ONE dedup key, so
        // only the last survived — silent row loss for a table whose PK is a
        // large TOASTable column (multi-KB text PK) that PG reports as
        // unchanged. Keeping ops is always safe: SR dedupes by PK on load.
        None
    } else {
        Some(pk_parts.join("|"))
    };
    // Move the serialized bytes into the RawValue WITHOUT copying, then put a
    // fresh Vec of the same capacity back into `buf` for the next row. A plain
    // `mem::take` would zero the capacity and force a regrow per row; a clone
    // (the previous approach) copied the whole row twice.
    let json_str = String::from_utf8(std::mem::replace(buf, Vec::with_capacity(buf.len())))
        .map_err(|e| anyhow::anyhow!("serialize_row produced invalid UTF-8: {e}"))?;
    let raw = serde_json::value::RawValue::from_string(json_str)
        .map_err(|e| anyhow::anyhow!("serialize_row produced invalid JSON: {e}"))?;
    Ok((raw, pk_key))
}

/// Parse a PG INTERVAL text representation into microseconds.
///
/// PG sends INTERVAL in its standard output format:
///   "1 year", "2 mons", "3 days", "04:05:06",
///   "1 year 2 mons 3 days 04:05:06.123456", "-1 year", "00:00:00"
///
/// Returns microseconds as i64, or None if parsing fails.
pub(crate) fn parse_pg_interval_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() || s == "00:00:00" {
        return Some(0);
    }

    // PG interval text output uses per-component signs, NOT a single global
    // sign. Examples:
    //   '-1 year -1 mon'  → '-1 years -1 mons'
    //   '-1 day 1 hour'   → '-1 days +01:00:00'
    //   '1 year -1 mon'   → '11 mons'    (PG normalizes year+month to months)
    //
    // Strategy: DON'T strip a global leading sign. Parse each token with its
    // own sign. The time component (HH:MM:SS) may have a '+' or '-' prefix
    // (e.g., '+01:00:00' or '-02:30:00').

    let mut years: i64 = 0;
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut hours: i64 = 0;
    let mut minutes: i64 = 0;
    let mut seconds: f64 = 0.0;

    // Try to parse time component HH:MM:SS[.ffffff] with optional +/- prefix
    // Look for a time-like pattern at the end
    let mut remaining = s.to_string();
    if let Some(caps) = INTERVAL_TIME_RE.captures(&remaining) {
        let time_sign = if caps.get(1)?.as_str() == "-" {
            -1i64
        } else {
            1i64
        };
        hours = caps.get(2)?.as_str().parse::<i64>().ok()? * time_sign;
        minutes = caps.get(3)?.as_str().parse::<i64>().ok()? * time_sign;
        let secs_str = caps.get(4)?.as_str();
        let frac = caps.get(5).map(|m| m.as_str()).unwrap_or("0");
        seconds = format!("{}.{}", secs_str, frac).parse::<f64>().ok()? * time_sign as f64;
        // Remove the matched time from remaining
        let matched = caps.get(0)?.as_str();
        remaining = remaining.replace(matched, "");
    }

    // Parse year/month/day tokens from remaining text. Each number carries
    // its own sign (PG emits '-1' not '-' + '1').
    let tokens: Vec<&str> = remaining.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i];
        // Try parsing as i64 (handles '-1', '1', '-365', etc.)
        if let Ok(num) = token.parse::<i64>() {
            i += 1;
            if i < tokens.len() {
                match tokens[i] {
                    "years" | "year" | "yrs" | "yr" => years = num,
                    "mons" | "mon" | "months" | "month" => months = num,
                    "days" | "day" | "d" => days = num,
                    "hours" | "hour" | "hrs" | "hr" => hours = num,
                    "mins" | "min" | "minutes" | "minute" | "m" => minutes = num,
                    "secs" | "sec" | "seconds" | "second" | "s" => {
                        seconds = token.parse::<f64>().unwrap_or(num as f64)
                    }
                    _ => {} // unknown unit, skip
                }
            }
        }
        i += 1;
    }

    // Convert to microseconds
    // Standard approximation: 1 year = 365.25 days, 1 month = 30 days
    let days_from_ym = years as f64 * 365.25 + months as f64 * 30.0;
    let total_days = days_from_ym + days as f64;
    let total_seconds =
        total_days * 86400.0 + hours as f64 * 3600.0 + minutes as f64 * 60.0 + seconds;
    let micros = (total_seconds * 1_000_000.0) as i64;

    Some(micros)
}

/// Strip currency formatting from a PG MONEY value, locale-robustly.
/// "$1,234.56" → "1234.56", "-$500.00" → "-500.00", "$0.00" → "0.00",
/// and "1.234,56" (de_DE lc_monetary: '.' is the thousands separator)
/// → "1234.56" (the old filter kept '.', producing "1.23456" — a 1000×
/// silent corruption under non-en_US locales).
///
/// The LAST '.' or ',' in the formatted text is the decimal separator (PG
/// money always prints a 2-digit fraction); all other punctuation is a
/// group separator and is dropped. Returns None when no digits survive.
fn clean_money_value(s: &str) -> Option<String> {
    let t = s.trim();
    let digits: String = t
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '-' || *c == '+')
        .collect();
    if digits.is_empty() {
        return None;
    }
    match t.rfind(['.', ',']) {
        Some(sep) => {
            let frac_digits = t[sep + 1..].chars().filter(|c| c.is_ascii_digit()).count();
            if frac_digits > 0 {
                let cut = digits.len().saturating_sub(frac_digits);
                if cut > 0 && cut < digits.len() {
                    let mut out = String::with_capacity(digits.len() + 1);
                    out.push_str(&digits[..cut]);
                    out.push('.');
                    out.push_str(&digits[cut..]);
                    return Some(out);
                }
            }
            // Separator with no fraction (defensive — PG money always has
            // 2 decimals): keep the plain digits.
            Some(digits)
        }
        None => Some(digits),
    }
}

pub(crate) fn string_to_json_value(s: &str, type_oid: u32) -> serde_json::Value {
    match type_oid {
        // JSON types — try to parse as JSON
        114 | 3802 => {
            match serde_json::from_str(s) {
                Ok(v) => v,
                Err(e) => {
                    // Log a warning so operators know the data is stored as a string
                    // instead of silently converting malformed JSON to a plain string.
                    tracing::warn!(
                        "Failed to parse JSON value (OID={}): {:?} — value: {}",
                        type_oid,
                        e,
                        crate::util::truncate_str(s, 200),
                    );
                    serde_json::Value::String(s.to_string())
                }
            }
        }
        // BYTEA — strip \x prefix so SR can unhex() it
        17 => serde_json::Value::String(crate::types::strip_bytea_hex_prefix(s).to_string()),
        // BOOLEAN — PG sends 't'/'f' text, SR needs JSON true/false
        16 => match s {
            "t" | "true" => serde_json::Value::Bool(true),
            "f" | "false" => serde_json::Value::Bool(false),
            _ => {
                tracing::warn!("Unexpected boolean value from PG: {:?}", s);
                serde_json::Value::Null
            }
        },
        // INTERVAL → BIGINT (microseconds)
        // PG sends text like "1 year", "00:00:00", "1 day 12:00:00"
        1186 => match parse_pg_interval_to_micros(s) {
            Some(micros) => serde_json::Value::Number(serde_json::Number::from(micros)),
            None => {
                tracing::warn!("Failed to parse INTERVAL value: {:?}", s);
                serde_json::Value::Null
            }
        },
        // TIMESTAMPTZ → UTC string (SR DATETIME has no timezone support)
        // PG sends text like "2024-01-01 12:00:00+05:30". We parse the timezone offset,
        // convert to UTC, and output without offset so SR DATETIME stores the correct value.
        1184 => match timestamptz_to_utc(s) {
            Some(utc) => serde_json::Value::String(utc),
            None => {
                tracing::warn!("Failed to parse TIMESTAMPTZ value: {:?} — storing as-is", s);
                serde_json::Value::String(s.to_string())
            }
        },
        // MONEY → DECIMAL (strip currency formatting)
        // PG sends text like "$1,234.56", "-$500.00"
        790 => {
            match clean_money_value(s) {
                Some(cleaned) => {
                    // Emit as a JSON STRING: SR's DECIMAL column coerces the
                    // exact decimal text (same pattern as NUMERIC → string).
                    // Parsing as a JSON number would route through f64
                    // (serde_json without arbitrary_precision) and silently
                    // round any value beyond 2^53 cents (~$90T) — the old
                    // code's "preserves full precision" claim was wrong.
                    serde_json::Value::String(cleaned)
                }
                None => {
                    tracing::warn!("Failed to parse MONEY value: {:?}", s);
                    serde_json::Value::Null
                }
            }
        }
        // FLOAT4/FLOAT8 (700/701) — NaN/Infinity cannot be stored in SR DOUBLE.
        // SR either silently converts to NULL (non-strict) or rejects the row
        // (strict-mode, triggering the NumberFilteredRows check). We proactively
        // null these out to match the twilight NULL behavior with a warning.
        // Finite numbers stay as text (SR coerces from JSON string).
        700 | 701 => match s {
            "NaN" | "Infinity" | "-Infinity" => {
                tracing::warn!(
                    "float{} value {:?} cannot be stored in SR DOUBLE — converting to null",
                    if type_oid == 700 { 4 } else { 8 },
                    s
                );
                serde_json::Value::Null
            }
            _ => serde_json::Value::String(s.to_string()),
        },
        // Array types — parse PG {a,b,c} format → JSON array.
        // Use typed array parsing: each element is dispatched through the
        // same scalar coercion as non-array values (e.g. _bool "t"→true,
        // _text "null"→string "null" not NULL, _timestamptz→UTC, etc.).
        // Without this, _text arrays silently convert "null"→NULL, "true"→bool,
        // "123"→number, and "{}"→empty JSON object.
        _ if crate::types::pg_array_element_type(type_oid).is_some() => {
            let elem_oid = crate::types::pg_array_element_oid(type_oid).unwrap_or(25); // default to text
            pg_array_to_json_typed(s, elem_oid, 0)
        }
        // All others — pass as string
        _ => serde_json::Value::String(s.to_string()),
    }
}

/// Convert a PG array string to a typed JSON array, dispatching each element
/// through `string_to_json_value` with the element's PG type OID. This ensures
/// array element coercion matches scalar coercion:
/// - `_bool`: "t"/"f" → true/false (not strings "t"/"f")
/// - `_text`: "null" stays as string "null" (not JSON null), "123" stays
///   as string "123" (not number), "{}" stays as string "{}" (not JSON object)
/// - `_timestamptz`: elements converted to UTC (not left as "+05:30" text)
/// - `_interval`: elements parsed to micros (not left as text in BIGINT array)
/// - `_money`: elements cleaned of "$" prefix
/// - `_bytea`: elements hex-stripped
pub(crate) fn pg_array_to_json_typed(s: &str, elem_oid: u32, depth: u32) -> serde_json::Value {
    if depth > 16 {
        return serde_json::Value::String(s.to_string());
    }
    match crate::types::parse_pg_array(s) {
        Some(elements) => {
            let arr: Vec<serde_json::Value> = elements
                .into_iter()
                .map(|e| {
                    // Determine if the element was quoted (affects NULL/JSON
                    // interpretation — a quoted "NULL" is the literal string,
                    // not PG's null marker).
                    let was_quoted = e.starts_with('"') && e.ends_with('"') && e.len() >= 2;
                    let unquoted = if was_quoted {
                        let mut inner = e[1..e.len() - 1].to_string();
                        if inner.contains('\\') {
                            inner = inner.replace("\\\"", "\"").replace("\\\\", "\\");
                        }
                        inner
                    } else {
                        e.to_string()
                    };
                    let trimmed = unquoted.trim();

                    // NULL handling: PG arrays emit unquoted "NULL" for null
                    // elements. A quoted "NULL" is the literal string "NULL"
                    // and must stay as a string (especially for _text arrays).
                    if !was_quoted && trimmed.eq_ignore_ascii_case("null") {
                        return serde_json::Value::Null;
                    }

                    // Nested array recursion: an unquoted element that looks
                    // like {a,b} is a nested sub-array. Recurse through the
                    // typed parser so multi-dimensional arrays like {{1,2},{3,4}}
                    // produce [[1,2],[3,4]] instead of ["{1,2}","{3,4}"].
                    if !was_quoted && trimmed.starts_with('{') && trimmed.ends_with('}') {
                        return pg_array_to_json_typed(trimmed, elem_oid, depth + 1);
                    }

                    // Dispatch through scalar coercion for typed elements.
                    string_to_json_value(trimmed, elem_oid)
                })
                .collect();
            serde_json::Value::Array(arr)
        }
        None => serde_json::Value::String(s.to_string()),
    }
}

/// Convert a PostgreSQL TIMESTAMPTZ text value to UTC, returning a string
/// without timezone offset, suitable for SR DATETIME.
///
/// PG format: "YYYY-MM-DD HH:MM:SS[.frac][+HH:MM|+HH|-HH:MM|-HH]"
/// Returns None if parsing fails (value stored as-is in caller).
pub(crate) fn timestamptz_to_utc(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Find the LAST '+' or '-' after position 19 (YYYY-MM-DD HH:MM:SS).
    // This avoids matching '-' in negative years (rare for timestamps).
    // `get` (not `s[19..]`): a fixed byte index panics when byte 19 is not a
    // UTF-8 char boundary. PG's own timestamptz output is ASCII so the real
    // case is unreachable, but a stale TYPE in the relation cache (OID 1184
    // kept after `ALTER COLUMN ... TYPE text`, which a count-based reload
    // check cannot detect) can hand arbitrary text to this function — and a
    // panic here runs on the main task, killing the process without a
    // graceful flush or checkpoint save.
    let offset_pos = s.get(19..).and_then(|tail| tail.rfind(['+', '-']));

    let (datetime_str, offset_str) = match offset_pos {
        Some(pos) => {
            let abs_pos = 19 + pos;
            (&s[..abs_pos], &s[abs_pos..])
        }
        None => (s, ""),
    };

    if offset_str.is_empty() {
        return Some(datetime_str.to_string());
    }

    // Parse timezone offset: +HH:MM, -HH:MM, +HH, -HH
    let sign: i64 = if offset_str.starts_with('-') { -1 } else { 1 };
    let offset_digits = &offset_str[1..];

    let (off_hours, off_mins): (i64, i64) = match offset_digits.find(':') {
        Some(pos) => (
            offset_digits[..pos].parse().ok()?,
            offset_digits[pos + 1..].parse().ok()?,
        ),
        None => (offset_digits.parse().ok()?, 0),
    };

    let total_offset_minutes = (off_hours * 60 + off_mins) * sign;

    // Parse datetime portion with time::PrimitiveDateTime for safe calendar arithmetic
    let parsed = time::PrimitiveDateTime::parse(datetime_str, time_fmt_with_subsecond())
        .or_else(|_| time::PrimitiveDateTime::parse(datetime_str, time_fmt_no_subsecond()))
        .ok()?;

    // Apply offset to get UTC (subtract the offset from local time)
    let utc = parsed.checked_sub(time::Duration::minutes(total_offset_minutes))?;

    // Format without timezone, preserving subsecond precision
    if datetime_str.contains('.') {
        utc.format(time_fmt_with_subsecond()).ok()
    } else {
        utc.format(time_fmt_no_subsecond()).ok()
    }
}
