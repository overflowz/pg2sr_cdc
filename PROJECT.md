# pg2sr_cdc — PostgreSQL → StarRocks CDC Pipeline

Native Rust CDC pipeline for PG→SR replication. No Java, no Flink, no subprocesses.
Pure Rust with zero external runtime dependencies — all HTTP (Stream Load) and SQL (DDL)
use Rust crates directly.

**Runtime**: `./target/release/pg2sr_cdc -c config.toml --fresh`
**Status**: Feature-complete prototype, exercised on dev and staging only — never in production, use at your own risk.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Quick Start](#quick-start)
3. [Setup](#setup)
4. [Configuration Reference](#configuration-reference)
5. [Type Compatibility Matrix](#type-compatibility-matrix)
6. [Array Data Transformation](#array-data-transformation)
7. [Components](#components)
8. [Data Flow](#data-flow)
9. [Performance](#performance)
10. [Dependencies](#dependencies)
11. [Project Structure](#project-structure)
12. [Key Design Decisions](#key-design-decisions)
13. [Known Limitations](#known-limitations)
14. [Remaining Work](#remaining-work)
15. [table_filter Change Plan](#table_filter-change--re-snapshot--cdc-resume)
---



## Architecture

```
┌─────────────┐  pgoutput/WAL    ┌──────────────────────┐  HTTP Stream Load   ┌──────────────┐
│  PostgreSQL  │◄────────────────│  pg2sr_cdc (Rust)       │────────────────────►│  StarRocks   │
│  (logical    │  slot+pub       │                      │  reqwest PUT        │  (FE: 8030)  │
│   replication│                 │                      │  307→BE preserved   │              │
└──────┬───────┘                 └────────┬─────────────┘                     └──────┬───────┘
       │                                  │                                          │
        │  DDL event trigger →             │  DDL poller: query pg2sr_cdc_ddl_events   │
        │  pg2sr_cdc_ddl_events             │  → transpile (polyglot-sql) → execute   │
       └──────────────────────────────────┘─────────────────────────────────────────┘
                                          │
                                          │  mysql_async (MySQL protocol)
                                          │  SR SQL: CREATE/ALTER/DROP TABLE
                                          └──────────────────────────────────────────►
```

## Quick Start

```bash
# First run (snapshot + streaming):
RUST_LOG=info cargo run --release -- --config config.toml --fresh

# Resume from checkpoint (path from config.toml [checkpoint] section):
RUST_LOG=info cargo run --release -- --config config.toml
```

## Setup

### PostgreSQL

```sql
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'pass';
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO replicator;
CREATE PUBLICATION pg2sr_cdc_publication FOR ALL TABLES;
```

### StarRocks

```sql
CREATE DATABASE IF NOT EXISTS testdb;
```

### TLS Configuration (optional)

All connections support TLS (disabled by default):

```toml
# PostgreSQL connections (replication + direct)
[postgres.tls]
mode = "require"                 # disable|prefer|require|verify-ca|verify-full
ca_cert = "/path/to/ca.pem"     # optional, uses system CA store if unset

# StarRocks Stream Load HTTP connections
[starrocks.http_tls]
enable = true                    # false by default
ca_cert = "/path/to/ca.pem"     # optional

# StarRocks MySQL SQL connections
[starrocks.mysql_tls]
enable = true                    # false by default
ca_cert = "/path/to/ca.pem"     # optional
```

TLS uses rustls + system CA store via `rustls-native-certs`. Custom CA certs load via
PEM files. PG connections use tokio-postgres-rustls + pgwire-replication's native
`TlsConfig` with `SslMode`. MySQL uses `mysql_async::SslOpts`. All four connection
types independently configurable.

### Configuration (`config.toml`)

Full reference below. Minimal working example:

```toml
[postgres]
host = "localhost"
port = 5432
database = "testdb"
username = "replicator"
password = "pass"
schema = "public"
table_filter = "*"
slot_name = "pg2sr_cdc_slot"
publication = "pg2sr_cdc_publication"

[starrocks]
node_urls = "localhost:8030"
database = "testdb"
username = "root"
password = ""
table_properties = { fast_schema_evolution = "true" }

[pipeline]
schema_sync = true
ddl_poll_interval_ms = 5000
ddl_capture_table = "public.pg2sr_cdc_ddl_events"
```

### Table Naming Convention

PG table `Y` in schema `X` → SR table `Y` in configured database
(e.g., `public.orders` → `orders` in `testdb`). The schema prefix is not reflected in
SR; all tables live flat in `[starrocks].database`. To avoid name collisions across PG
schemas, use separate SR databases or filter per-schema with `table_filter`.

---

## Configuration Reference

### `[postgres]` section

| Field | Default | Description |
|-------|---------|-------------|
| `host` | — | PG host |
| `port` | — | PG port (5432) |
| `database` | — | PG database name |
| `username` | — | PG user (must have REPLICATION privilege) |
| `password` | — | PG password |
| `schema` | `"public"` | PG schema to replicate. Must be a plain identifier (no glob/regex chars); validated at startup. |
| `table_filter` | `"*"` | Glob pattern filtering replicated tables by **bare table name** (NOT `schema.table`). Supports `*` (any sequence) and `?` (any single char). Invalid pattern is a **fatal error** (fail-fast via `compile_table_filter`). |
| `table_exclude` | `""` | Regex to exclude tables from replication (applied after `table_filter`). Matching tables are skipped entirely — no SR table created, no CDC data, no snapshot. Empty string = no exclusion. |
| `slot_name` | `"pg2sr_cdc_slot"` | Logical replication slot name |
| `publication` | `"pg2sr_cdc_publication"` | Publication name |
| `manage_publication` | `true` | If true (default), the pipeline creates/drops/alters the publication and adds tables automatically. If false, the publication is expected to be pre-created by a DBA. |
| `connect_timeout_ms` | `30000` | Connection timeout in milliseconds for PG replication connections (TCP connect + auth + START_REPLICATION negotiation). |

#### `[postgres.tls]` section (optional)

| Field | Default | Description |
|-------|---------|-------------|
| `mode` | `"disable"` | SSL mode: `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Maps to PostgreSQL sslmode semantics. |
| `ca_cert` | — | Optional path to PEM file with trusted CA certificates. Uses system CA store if unset. |

### `[starrocks]` section

| Field | Default | Description |
|-------|---------|-------------|
| `node_urls` | — | Semicolon-separated FE node URLs. Each entry: `host`, `host:http_port`, or `host:http_port:jdbc_port`. HTTP defaults to 8030, JDBC to 9030. |
| `database` | — | SR database name |
| `username` | — | SR user (default `root`) |
| `password` | — | SR password (default empty) |
| `connection_timeout_ms` | `10000` | Initial connection timeout in ms — bounds TCP connect + TLS/auth handshake for BOTH HTTP (Stream Load) and MySQL (DDL/catalog) connections. Does not bound request execution. |
| `table_properties` | `{}` (built-in `replication_num = "1"`) | StarRocks table properties merged into all CREATE TABLE statements. Format: `{ "fast_schema_evolution" = "true" }`. Previously under `[pipeline]` — moved here in P1 refactoring. |
| `http_pool_idle_timeout_ms` | `30000` | Idle timeout in ms for the HTTP connection pool. Should be ~50-60% of StarRocks' `be_http_timeout` (default 60s). |
| `mysql_max_pool_connections` | `32` | Maximum MySQL connections per SR FE node in the pool. Bounds memory + FD usage under burst load. Must be ≥ 4. |

**Important**: `fast_schema_evolution` must be `"true"` (lowercase string), not `"1"`.

#### Multi-node HA behavior

When multiple nodes are specified (e.g. `"fe1:8030;fe2:8030;fe3:8030"`),
`SrConnectionManager`:

1. Tries the **last successful node** first (cached in memory)
2. On connection failure, falls through to the next node
3. On write success, caches that node for subsequent operations
4. If the cached node later fails, clears the cache and tries all nodes from scratch
5. Applies the configurable `connection_timeout_ms` per attempt

Covers both SQL (DDL via MySQL protocol on JDBC port) and Stream Load (HTTP on HTTP port)
— both share the same active-node cache.

#### `[starrocks.http_tls]` section (optional)

| Field | Default | Description |
|-------|---------|-------------|
| `enable` | `false` | Enable HTTPS for Stream Load connections. |
| `ca_cert` | — | Optional path to PEM file with trusted CA certificates. Uses system CA store if unset. |

#### `[starrocks.mysql_tls]` section (optional)

| Field | Default | Description |
|-------|---------|-------------|
| `enable` | `false` | Enable SSL for MySQL SQL connections. |
| `ca_cert` | — | Optional path to PEM file with trusted CA certificates. Uses system CA store if unset. |

### `[pipeline]` section

| Field | Default | Description |
|-------|---------|-------------|
| `schema_sync` | `true` | Enable automatic DDL evolution. When true, spawns a DDL poller that captures PG DDL via event trigger, transpiles to SR SQL via `polyglot-sql`, and executes it. |
| `ddl_poll_interval_ms` | `5000` | Milliseconds between DDL poller queries to `pg2sr_cdc_ddl_events`. |
| `max_retries` | `2` | Max Stream Load retries after initial attempt (exponential backoff: 1s, 2s, 4s, 8s). With default 2, total attempts = 3 (1 initial + 2 retries). |
| `batch_rows` | `5000` | Max row changes to accumulate before flushing to SR. |
| `batch_timeout_ms` | `500` | Max idle ms before flush even when `batch_rows` not reached. |
| `snapshot_page_size` | `50000` | Rows per snapshot query page. Independent from `batch_rows`. Larger = fewer round trips but longer per-query duration for tables with custom SELECT expressions (BYTEA, INTERVAL, MONEY, NUMERIC). Keyset-paginated tables use this as the LIMIT. |
| `query_timeout_ms` | `30000` | Timeout for PG catalog queries (OID resolution, column metadata, PK lookup, DDL capture SELECT). Applied via `tokio::time::timeout` in `pg_catalog.rs`. |
| `sink_flush_timeout_ms` | `10000` | HTTP Stream Load per-attempt timeout in ms. Bounds the FE PUT + (if 307 redirected) BE PUT chain — one budget per attempt shared via a deadline Instant. |
| `mysql_query_timeout_ms` | `10000` | Timeout in ms for ALL MySQL queries sent via `mysql_async`: DDL (ALTER/CREATE/DROP/RENAME), catalog queries (SHOW TABLES gap detection), SHOW LOAD label polling. Does NOT bound pool connection acquisition — see `connection_timeout_ms`. |
| `ddl_failure_abort` | `true` | If true, exit the pipeline on permanent DDL failures (after 3 retries). Default true: abort so the operator can investigate. When false, the event is FATAL-logged and checkpoint-advanced (soft-skip) — schema divergence is accepted for uptime. |
| `ddl_capture_table` | `"public.pg2sr_cdc_ddl_events"` | DDL capture infrastructure table in `schema.table` format. |
| `max_pending_memory_mb` | `512` | Max pipeline buffer memory in MB before forcing a flush. Prevents unbounded growth during PG write bursts when SR is slow. Per-event byte cap = value / 4 (warned when ≥ 100MB to stay under SR's `streaming_load_max_batch_size_mb`). |
| `reconnect_delay_ms` | `3000` | Base delay in ms between PG replication reconnection attempts. Uses exponential backoff (2× each attempt) up to 30s cap. Increase when slot contention is expected (multiple processes sharing a slot) to reduce log spam. Note: PG's 55006 "slot is active for PID" is a fatal-exit, not retried. |
| `schema_change_retry_ms` | `1500` | Sleep in milliseconds after StarRocks returns "schema change in progress" error before retrying the same DDL on the next poll cycle. |
| `ddl_race_retry_ms` | `5000` | Sleep in milliseconds when a CDC Stream Load fails with "unknown column" due to DDL poller lag. After this sleep, the flush retries once. |
| `max_concurrent_flushes` | `20` | Maximum concurrent per-table Stream Load requests during CDC flushes. Limits how many tables can be flushed simultaneously. |
| `max_tx_events` | `100000` | Max WAL events to accumulate within a single PG transaction before returning an error to trigger a reconnect. Prevents OOM on bulk INSERT/DELETE. |
| `name` | `"pg2sr_cdc"` | Pipeline instance name. Sets `application_name` on PG connections, lock file path (`/tmp/{name}.lock`), and log file path (`logs/{name}.log`). |

> **Note**: When a table has a PRIMARY KEY, the PROPERTIES clause also includes
> `"persistent_index" = "true"` for optimal StarRocks PK table performance.
> DUPLICATE KEY tables do not include this property.

### `[checkpoint]` section

| Field | Default | Description |
|-------|---------|-------------|
| `store_type` | `"local"` | `"local"` (filesystem) or `"s3"` (S3-compatible object store) |
| `path` | `"pg2sr_cdc_checkpoint.json"` | File path (local) or S3 object key (S3) |
| `save_interval_ms` | `10000` | Milliseconds between automatic checkpoint saves. Higher values reduce S3 PUTs but increase WAL replay on crash. In-memory checkpoint always updated immediately. Must be > 0 (else busy-loop). |
| `bucket` | — | S3 bucket name (required for `store_type = "s3"`) |
| `region` | `"us-east-1"` | S3 region |
| `endpoint` | `"https://s3.amazonaws.com"` | Custom S3-compatible endpoint URL (MinIO, Ceph, etc.) |
| `path_style` | `true` | Use path-style addressing (required for most S3-compatible stores) |
| `access_key_id` | — | S3 access key (falls back to env/IRSA/IMDS if unset) |
| `secret_access_key` | — | S3 secret key (falls back to env/IRSA/IMDS if unset) |

### `[metrics]` section (optional)

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable the Prometheus `/metrics` + JSON `/health` HTTP endpoint. When false, no HTTP listener is started. |
| `port` | `9091` | Port for the `/metrics` and `/health` HTTP endpoint. |
| `host` | `127.0.0.1` | Bind address for the HTTP listener. Default loopback so LSN/throughput aren't scrapable by other hosts; set `0.0.0.0` only when external scraping is intended (and firewall the port). |

When enabled, a lightweight raw tokio TCP listener (`src/metrics_server.rs`)
serves two routes: `GET /metrics` (Prometheus text exposition format rendering
the `Metrics` struct's `AtomicU64` counters + the `metrics`-crate Prometheus
recorder) and `GET /health` (JSON health summary including `checkpoint_save_lag`,
`reconnect_count`, `pending_bytes`, `last_lsn`; returns HTTP 503 when the
checkpoint saver has been stuck for > 300s or during graceful shutdown).
No external HTTP framework (hyper/axum) is used.

---

## Type Compatibility Matrix

### Scalar Types

| PG Type | SR Type | Notes |
|---------|---------|-------|
| SMALLINT | SMALLINT | |
| INTEGER | INT | |
| BIGINT/BIGSERIAL | BIGINT | |
| REAL/FLOAT4 | FLOAT | |
| DOUBLE PRECISION | FLOAT8 | |
| NUMERIC(p,s) | DECIMAL(p,s) — or STRING if p>38 or s>18 | SR's DECIMAL caps precision at 38 and scale at 18; values beyond map to STRING (lossless via JSON text) |
| MONEY | DECIMAL(38,2) | |
| CHAR(n) | STRING | SR's native unbounded text type; length-safe |
| VARCHAR(n) | STRING | unbounded — no ×3 multiplier needed |
| TEXT | STRING | SR's native unbounded text type |
| BYTEA | VARBINARY | hex from pgoutput; snapshot uses `encode(col,'hex')`, CDC strips `\\x` prefix, Stream Load uses `unhex()` column transform |
| JSON/JSONB | JSON | JSONB uses SR's JSON type |
| DATE | DATE | |
| TIME | VARCHAR(20) | SR has no TIME type |
| TIMETZ | VARCHAR(32) | |
| TIMESTAMP | DATETIME | |
| TIMESTAMPTZ | DATETIME | SR has no TZ support |
| INTERVAL | BIGINT | microseconds |
| BOOLEAN | BOOLEAN | |
| UUID | VARCHAR(64) | |
| INET/CIDR | VARCHAR(64) | |
| BIT/VARBIT | VARCHAR(255) | bit-string text preserved (CDC path) |
| XML | STRING | |
| ENUM | VARCHAR(255) | values arrive as text |
| Range types | VARCHAR(255) | `[1,10)`, etc. as text |
| MACADDR | VARCHAR(255) | |
| Geometric types | VARCHAR(255) | point, line, lseg, box, path, polygon, circle |

### Array Types

PG `{a,b,c}` format is parsed and transformed to JSON arrays for SR's `ARRAY<type>`.
Supports nested arrays, quoted elements, backslash escaping, and mixed types.

| PG Array Type | SR Type | Example PG → SR |
|---|---|---|
| `SMALLINT[]` | `ARRAY<SMALLINT>` | `{1,2,3}` → `[1,2,3]` |
| `INTEGER[]` | `ARRAY<INT>` | `{1,2,3}` → `[1,2,3]` |
| `BIGINT[]` | `ARRAY<BIGINT>` | |
| `REAL[]` / `FLOAT4[]` | `ARRAY<FLOAT>` | |
| `FLOAT8[]` / `DOUBLE PRECISION[]` | `ARRAY<DOUBLE>` | |
| `NUMERIC[]` | `ARRAY<DECIMAL>` | |
| `MONEY[]` | `ARRAY<DECIMAL(38,2)>` | |
| `BOOLEAN[]` | `ARRAY<BOOLEAN>` | `{t,f,t}` → `[true,false,true]` |
| `TEXT[]` / `VARCHAR[]` | `ARRAY<STRING>` | `{a,b}` → `["a","b"]` |
| `JSONB[]` / `JSON[]` | `ARRAY<JSON>` | elements parsed as JSON objects |
| `DATE[]` | `ARRAY<DATE>` | |
| `TIMESTAMP[]` | `ARRAY<DATETIME>` | |
| `UUID[]` | `ARRAY<VARCHAR(64)>` | |
| `INET[]` / `CIDR[]` | `ARRAY<VARCHAR(64)>` | |
| `BIT[]` / `VARBIT[]` | `ARRAY<VARCHAR(255)>` | |
| Unknown arrays | `ARRAY<STRING>` | fallback for system types |
| Nested (2D+) | `ARRAY<...>` | `{{1,2},{3,4}}` → `[[1,2],[3,4]]` |

---

## Array Data Transformation

Pipeline converts PG's text array format to JSON arrays for Stream Load:

| PG Input | SR Output | Notes |
|---|---|---|
| `{1,2,3}` | `[1,2,3]` | Integer elements → JSON numbers |
| `{1.5,2.5}` | `[1.5,2.5]` | Float elements → JSON numbers |
| `{abc,def}` | `["abc","def"]` | Unquoted strings → JSON strings |
| `{"hello world"}` | `["hello world"]` | Quoted strings → single JSON string |
| `{1,NULL,3}` | `[1,null,3]` | NULL → JSON null |
| `{true,false}` | `[true,false]` | Booleans → JSON booleans |
| `{"{\\"key\\":1}"}` | `[{"key":1}]` | JSONB → nested JSON objects |
| `{{1,2},{3,4}}` | `[[1,2],[3,4]]` | Nested arrays preserved |
| `{}` | `[]` | Empty array |

---

## Components

### `src/main.rs` — Entry point & orchestration

- CLI args: `-c`/`--config` (config file, default `config.toml`), `--fresh` (delete checkpoint, drop & recreate replication slot + publication, tear down DDL capture infra, re-snapshot all matching tables from PG into StarRocks — replaces SR data via per-table shadow-table swap; does NOT directly drop SR tables), `--name` (override pipeline name)
- **TLS CryptoProvider initialization**: `init_tls()` at startup selects `ring` provider
- **Single-instance flock**: `fs2::try_lock_exclusive()` on `/tmp/{database}.lock`
- **Dual-layer tracing**: Console INFO+ via `RUST_LOG`, file WARN+ERROR via size-capped rotating logger (50MB+50MB)
- **Slot/publication auto-creation**: `ensure_replication_slot()` runs before CDC loop AND
  inside every reconnect attempt — creates slot (already-exists is non-fatal), creates
  publication (idempotent), sets REPLICA IDENTITY on matching tables
- **Startup table validation**: `validate_replicated_tables()` — verifies each matching
  table has a PRIMARY KEY with SR-compatible column types, checks REPLICA IDENTITY is
  `d` (DEFAULT) or `f` (FULL) per table, checks `bytea_output = hex`
  if BYTEA columns present
- Phases: DDL setup → Table validation → Snapshot (gap detection) → CDC streaming + DDL poller
- **Graceful shutdown**: SIGINT + SIGTERM via watch channel with final checkpoint save.
  Second Ctrl+C or SIGTERM forces `std::process::exit(130)` (or `143`) immediately.
- **Memory-bounded pipeline**: `max_pending_memory_mb` config (default 512 MB)
- **CDC loop**: outer reconnect loop (exponential backoff 500ms→30s) → stream
- **Background checkpoint saver**: spawned task saves checkpoint on `save_interval_ms`
  (default 10000ms) with automatic restart on panic. In-memory checkpoint updated immediately;
  durable save decoupled. Final save on shutdown.
- **DDL transpilation post-processing**: AST-based via `polyglot-sql` — strips schema
  prefixes, PG-only modifiers, constraints, and DEFAULTs. Adds SR clauses (ENGINE, KEY,
  DISTRIBUTED BY, PROPERTIES). Columns reordered for SR key-first constraint.
  `map_unknown_sr_type()` handles MONEY→DECIMAL(38,2), bare DECIMAL→DECIMAL(38,2),
  INTERVAL→BIGINT, and ARRAY<MONEY>→ARRAY<DECIMAL(38,2)>. **Name-path mapping**: extended
  `pg_type_name_to_sr` to mirror the OID path for every type (UUID→VARCHAR(64),
  INET/CIDR→VARCHAR(64), MACADDR/MACADDR8→VARCHAR(255), BIT/VARBIT→VARCHAR(255),
  range types→VARCHAR(255), INT4/INT2/INT8→INT/SMALLINT/BIGINT, TIMESTAMP(p)/
  TIMESTAMPTZ(p)→DATETIME, INTEGER→INT, JSONB→JSON). Cross-path consistency test
  (`test_pg_type_name_to_sr_matches_oid_path`) asserts the DDL name-path output
  equals the OID-path output for all affected types — prevents future drift
  between AST/catalog/CDC paths.
- **Catalog fallback**: when polyglot-sql can't transpile a CREATE TABLE (custom types),
  queries PG catalog directly for column types + PK and generates SR-native DDL

### `src/config.rs` — Configuration

TOML-based config with `serde` defaults and `deny_unknown_fields` on all structs for
typo detection. All fields have `#[serde(default = "...")]`. `table_properties` lives
under `SrConfig` (not `PipelineConfig`).

### `src/cdc.rs` — PG WAL consumer

- `pgwire-replication` + `pgoutput` crates for WAL parsing
- `connect()` with config-driven TLS and connection timeout
- `load_relations()` → queries PG catalog for columns AND primary keys
- `should_replicate_table()` — combines infrastructure table check + `table_filter` regex
- **TOAST-safe**: `ColValue` enum distinguishes `Null`, `Unchanged`, and `Value` — prevents
  silent data corruption on UPDATE of large TOAST values. `partial_columns: true` Stream Load
  header is sent on UPSERT so unchanged TOAST columns (omitted from the JSON payload by
  `columns_to_json`) preserve their existing SR value instead of being overwritten with NULL.
- Handles BEGIN/COMMIT/Relation/Insert/Update/Delete/Truncate/Type events
- **Idle slot advancement**: `idle_keepalive_pending` flag triggers immediate batch return
  when a KeepAlive arrives on an empty batch, eliminating the up-to-`batch_timeout` latency
  between KeepAlive arrival and `confirmed_flush_lsn` advancement. Keeps replication lag
  bounded to sub-200-byte levels during idle periods.

### `src/pipeline.rs` + `src/pipeline/value_conv.rs` — Event buffering & SR flush

- Per-table change buffers: INSERT→upsert, UPDATE→upsert+delete, DELETE→delete, TRUNCATE→flag
- `process_event()` routes using relation cache; tracks `pending_bytes` for memory cap.
  On schema mismatch (WAL values > cached columns after ALTER TABLE ADD COLUMN), reloads
  the relation from PG catalog and retries once — prevents batch abort & event loss.
- `string_to_json_value()` (in `pipeline/value_conv.rs`) — type-aware value conversion
  (JSON, arrays, BYTEA hex stripping). Split out of `pipeline.rs` for isolation/testability.
- `flush_transaction()` — **concurrent per-table flushes via `tokio::spawn`** (limited by
  `max_concurrent_flushes` semaphore), DELETE first, then UPSERT (with `unhex()` transforms
  for BYTEA columns). DDL-race retry loop bounded by the computed `flush_timeout` (incl.
  inter-attempt backoff). Per-table Stream Load payloads are chunked to ≤90 MB
  (`SR_STREAM_LOAD_MAX_BYTES`) so a single oversized batch doesn't trip SR's
  `streaming_load_max_batch_size_mb`.
  **`partial_columns: true` Stream Load header** is sent on UPSERT so unchanged TOAST columns
  (omitted from the JSON payload) preserve their existing SR value.
- **`RawValue` row serialization**: `serialize_row()` writes the JSON object directly
  into a reused `Vec<u8>` buffer (1 allocation per row — the `Box<RawValue>`'s `Arc<str>`),
  instead of building a `serde_json::Map` (N `String`-key clones + BTreeMap nodes per row).
  The PK key is pre-computed during serialization (col→index lookup), so net-change dedup
  never re-parses the JSON. `build_json_data()` copies the `RawValue` bytes directly (no
  re-serialization at flush time).
- Auto-create SR table on "unknown table" error
- **`table_filter` enforcement**: CDC data events filtered in both `CdcConsumer` and
  `Pipeline` — non-matching tables exist in SR (created by DDL poller) but receive no CDC data
- **`max_tx_events` guard**: single-transaction WAL event limit prevents OOM on bulk operations

### `src/types.rs` — Type mapping

- `pg_type_to_sr(oid, modifier)` → SR type string (scalars + arrays + BYTEA→VARBINARY)
- `pg_type_name_to_sr(type_name)` → type-name-based mapping (used by DDL poller)
- `is_pg_type_sr_pk_compatible()` → checks if PG type maps to SR PK-compatible type
- `pg_array_element_type()` → maps 35+ PG array OIDs to SR element types
- `parse_pg_array()`, `pg_element_to_json()`, `pg_array_to_json()` — full array pipeline
- `map_unknown_sr_type()` — MONEY→DECIMAL(38,2), bare DECIMAL→DECIMAL(38,2), INTERVAL→BIGINT

### `src/pg_catalog.rs` — PG catalog queries

- `fetch_relation_oid()`, `fetch_columns()`, `fetch_primary_keys()`, `fetch_user_tables()`
- `table_has_pk()` — check for PRIMARY KEY without fetching all columns
- `pg_quote()` — double-quoted PG identifier with embedded quote escaping

### `src/snapshot.rs` — Initial data COPY

- DROP TABLE IF EXISTS before CREATE for clean snapshot
- **Gap detection**: re-runs for tables not in `snapshotted_tables` — supports
  `table_filter` changes without `--fresh`
- Column metadata via `pg_catalog::fetch_columns()`, type mapping via `types::pg_type_to_sr()`
- **Column reordering**: PK columns moved to front for SR key-first constraint
- **Keyset pagination**: `WHERE pk::bigint > $1 ORDER BY pk LIMIT N` for single integer PK
  tables. Falls back to LIMIT/OFFSET for composite/non-integer/no-PK tables.
- BYTEA columns: `encode(col, 'hex')` in SELECT, `unhex()` in Stream Load
- **Shared snapshot helpers**: `build_snapshot_col_list()`, `build_snapshot_transforms()`,
  `build_snapshot_order_clause()` — extracted from duplicated code in `run_snapshot` and
  `snapshot_one_table`. Shared BYTEA/INTERVAL/MONEY/NUMERIC custom-column logic.
- **Skips CTAS for PK-less tables** (not supported by pipeline)
- Mid-snapshot checkpoint save after each table

### `src/sink.rs` — StarRocks Stream Load (reqwest) with HA failover

- `reqwest::Client` with `redirect::Policy::none()` — manual 307 FE→BE redirect, auth preserved
- Configurable `http_pool_idle_timeout_ms` (default 30000ms) to align with SR's `be_http_timeout`
- Per-attempt budget from `sink_flush_timeout_ms` (default 10000ms) shared across FE PUT + 307 BE PUT
- HTTP response body size capped (64 KB) via `read_capped_body()` to bound memory on bad responses
- DELETE via `__op` column prepended to NDJSON rows
- Column transforms via `build_stream_load_columns()` — `unhex()` for BYTEA
- Exponential backoff retry: 1s, 2s, 4s, ... (configurable `max_retries`)
- Duplicate label detection: `ExistingJobStatus=FINISHED` treated as success (idempotency)

### `src/ddl_poller.rs` — DDL poller (separate module)

- `run_ddl_poller_loop()`: async loop gated by `schema_sync` config; outer reconnect loop
  with panic recovery
- Queries `pg2sr_cdc_ddl_events` for new DDL events (`WHERE id > last_ddl_id`)
- Type mapping: FLOAT4/REAL → FLOAT, FLOAT8/DOUBLE PRECISION → DOUBLE, INTERVAL → BIGINT,
  MONEY → DECIMAL(38,2), bare DECIMAL → DECIMAL(38,2), ARRAY<MONEY> → ARRAY<DECIMAL(38,2)>
- Skips REPLICA IDENTITY ALTER TABLEs immediately (PG-only)
- **3-tier DDL error handling**: transient network errors retried indefinitely; permanent
  SQL/schema failures counted against 3-retry limit then skipped with FATAL log; consecutive
  PG query errors trigger outer reconnect
- **Schema-change pacing**: on "schema change in progress" error, sleeps 1.5s and defers
  remaining same-table ALTERs to next poll cycle. **Critical: uses `break` (not `continue`)**
  to prevent checkpoint advancement past the blocked DDL id — with `continue`, a later successful
  DDL in the same batch would advance the checkpoint past this id, permanently losing it
- Catalog fallback for CREATE TABLE with custom types (enums, composites, domains)
- **DDL capture preservation across restarts**: `setup_ddl_capture` split into
  `create_ddl_capture` (fresh: full DROP/CREATE) and `ensure_ddl_capture` (restart:
  idempotent, conditional trigger via `DO $$ IF NOT EXISTS`, no DROP)
- **DDL poller creates ALL tables** in SR for schema sync; CDC data events are filtered
  by `table_filter` (changed from skip to WARN-only for non-matching tables)

### `src/sr_sql.rs` — StarRocks SQL DDL helpers

- Pure utility functions (`sr_quote`, `sr_table_properties`, `sr_create_table_sql`,
  `build_key_and_dist_col`, `reorder_columns_for_sr`) — no connection management
- `execute_sql()` wraps `SrConnectionManager::execute_sql` for HA multi-node failover
- DDL execution via `mysql_async` with `prefer_socket(false)` and `stmt_cache_size(0)`
- `sr_table_properties()` generates `PROPERTIES (...)` clause from config-provided map
- `reorder_columns_for_sr()` moves PK columns first, generic over any column type

### `src/sr_connection.rs` — StarRocks HA connection manager

- Multi-node FE failover with in-memory active-node cache
- Shared cache between SQL (JDBC) and HTTP (Stream Load) paths
- `execute_sql()` tries MySQL connections across nodes with configurable timeout

### `src/ddl.rs` — DDL capture infrastructure

- Creates `pg2sr_cdc_ddl_events` table
- **Two event triggers**: `pg2sr_cdc_ddl_func()` on `ddl_command_end` (CREATE/ALTER),
  `pg2sr_cdc_ddl_drop_func()` on `sql_drop` (DROP TABLE) — fixed names, single-DB-per-pipeline
- `current_query() IS NULL` fallback for Aurora PG
- `teardown_ddl_capture()` — drops all for `--fresh` start

### `src/checkpoint/` — LSN checkpoint persistence (directory module)

- `mod.rs` — `Checkpoint` struct + `CheckpointStore` trait + in-memory state.
  Atomic write (temp file + rename), mutex poison recovery. Fields: `last_committed_lsn`,
  `snapshot_done`, `snapshotted_tables`, `snapshotted_table_hashes`, `last_ddl_id`.
  `advance_ddl_checkpoint()` advances `last_ddl_id` in-memory only — durable persistence
  delegated to the background checkpoint saver (avoids S3 PUTs on every DDL event).
- `local.rs` — `LocalStore` (filesystem, atomic temp+rename). Corrupted checkpoints
  renamed to `.corrupted` for forensics.
- `s3.rs` — `S3Store` (S3-compatible via `rusty-s3` presigned URLs + `reqwest`).
  Two-stage atomic upload via `.tmp` key → overwrite main key.

### `src/metrics_server.rs` — Prometheus metrics endpoint

- Optional raw tokio TCP listener (no hyper/axum) — renders the `Metrics` struct's
  `AtomicU64` counters into Prometheus text exposition format.
- Two routes: `GET /metrics` (Prometheus exposition incl. `metrics`-crate recorder
  histograms for flush latency p50/p95/p99) and `GET /health` (JSON health summary
  with `checkpoint_save_lag`, LSN, reconnect count).
- Gated by `[metrics].enabled`; port via `[metrics].port` (default 9091).

### `src/util.rs` — Shared utilities

- `AbortOnDrop(JoinHandle)` — aborts tokio background task on drop

---

## Data Flow

### Snapshot Phase

1. Query `pg_catalog::fetch_user_tables()` for table list
2. Resolve relation OID, fetch column metadata
3. Skip tables already in `snapshotted_tables` (gap detection)
4. `DROP TABLE IF EXISTS` on SR for clean slate
5. Generate `CREATE TABLE IF NOT EXISTS` — **columns reordered for PK-first constraint**
6. Execute on SR via `mysql_async`
7. Stream each table: `SELECT row_to_json(t)::text` in pages (BYTEA uses `encode(col, 'hex')`)
8. Send NDJSON via `reqwest` Stream Load (with `unhex()` transforms for BYTEA)
9. Save mid-snapshot checkpoint after each table
10. Mark `snapshot_done = true`

### CDC Streaming Phase

1. Connect to PG with `replication=database`
2. Ensure replication slot and publication exist (auto-recover from accidental drops)
3. `read_transaction_batch()` accumulates WAL events
4. `process_event()` buffers per-table changes (type-aware JSON conversion)
5. `flush_transaction()` → concurrent per-table Stream Loads via `tokio::spawn`:
   - DELETE first (with `__op` column)
   - Then UPSERT (with `unhex()` transforms for BYTEA columns)
   - Auto-create on "unknown table" → retry
6. `refresh_relation_pks()` queries PG catalog for PK metadata on new tables
7. Update checkpoint + acknowledge WAL

### DDL Schema Evolution

1. PG event trigger captures DDL → `pg2sr_cdc_ddl_events`
2. DDL poller queries `WHERE id > last_ddl_id` every `ddl_poll_interval_ms` (default 5000ms)
3. For each DDL:
   a. Skip REPLICA IDENTITY ALTER TABLEs immediately
   b. **Inline PK sub-command filter**: skip ALTER TABLE ADD PK if it matches
      the immediately preceding CREATE TABLE (avoids redundant snapshot-then-recreate)
   c. **Per-row DROP emission**: multi-target `DROP TABLE a, b, c` — trigger emits per-row
      `obj.object_identity` (not `current_query()`), poller-side `seen_ddl_text`
      HashSet dedups legacy rows; `wait_for_cdc_drain` + `DdlActiveGuard`
      protect EACH dropped table (previously only the first was guarded).
   d. **PG-only metadata ALTERs**: (SET SCHEMA, VALIDATE CONSTRAINT,
      CLUSTER, ENABLE/DISABLE TRIGGER, *ROW LEVEL SECURITY, ATTACH/DETACH
      PARTITION, RENAME CONSTRAINT) detected via `is_pg_only_metadata_alter`
      and skipped before transpilation — polyglot-sql can't parse them.
   e. Apply type mapping overrides (FLOAT4→FLOAT, INTERVAL→BIGINT,
      MONEY→DECIMAL(38,2), UUID→VARCHAR(64), INET→VARCHAR(64), etc.)
   f. `polyglot-sql::transpile(PG → StarRocks)`
   g. Post-process AST: strip schema prefixes, strip DEFAULTs, remap
      VARCHAR/CHAR/TEXT→STRING, add engine/PK/DISTRIBUTED BY/PROPERTIES
   h. **Optimization**: `post_process_ddl_stmts` returns `Vec<String>` directly,
      skipping the third polyglot parse round-trip in `execute_sql_with_db`
      (HA-failover resume semantics preserved via `applied_count`).
   i. Execute on SR via `mysql_async`
   j. Advance `last_ddl_id` on success or permanent skip; retry on transient failures
4. **Resnapshot path** (ALTER COLUMN TYPE / RENAME COLUMN / RENAME TO /
   ADD PRIMARY KEY): drops SR table, re-snapshots from PG catalog (which
   already has the change applied — including `USING expr` conversions).
   Atomic shadow-table swap. Guard-first ordering + TOCTOU
   recheck at snapshot.rs protect against in-flight CDC flushes.
5. Skipped DDL types (logged at WARN): `ALTER SEQUENCE`, `SET`/`RESET`,
   unparseable ALTER TABLE inside DO blocks, PG-only metadata ALTERs
6. Catalog fallback: when polyglot-sql can't transpile, query PG catalog directly.
   Supports enums, composites, and domain types via `catalog_create_table_fallback()`.

---

## Performance

| Property | Behaviour |
|--------|-------|
| Delivery | At-least-once; PK-upsert idempotency absorbs duplicates |
| End-to-end latency | Bounded by `batch_timeout_ms` (default 500ms) plus the Stream Load round-trip |
| Batch flush overhead | One Stream Load request per table per batch |
| Compaction | No "too many versions" / "delay commit" warnings observed in shared-data SR (CLOUD_NATIVE) under sustained load |
| Reconnect | ~0.3s for RST, ~1.5s for a silent drop |

### Batching Configuration

Every flush is one Stream Load request per table, so batching is the main
throughput/latency knob:

| Setting | Trade-off |
|---------|-----------|
| `batch_rows=1000, batch_timeout_ms=100` | Fastest flush — lowest latency, more requests |
| `batch_rows=5000, batch_timeout_ms=500` | **Default** — balanced throughput and sub-second latency |
| `batch_rows=5000, batch_timeout_ms=1000` | Same throughput as the default, fewer idle flushes, higher small-batch latency |
| `batch_rows=10000, batch_timeout_ms=1000` | Highest throughput, larger latency hit — only for write-very-heavy workloads |

Larger batches trade latency for throughput; shared-data SR stays healthy at
all of these settings. Measure against your own PG/SR pair before changing the
defaults.

---

## Dependencies

See `Cargo.toml` for the authoritative list. Key crates:

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `pgwire-replication` | PG logical replication client (CopyBoth + pgoutput) |
| `pgoutput` | pgoutput WAL event parser |
| `polyglot-sql` | SQL dialect transpiler (PG → StarRocks, with `stacker` feature) |
| `tokio-postgres` | Direct PG connections (DDL setup, snapshot, DDL poller catalog) |
| `tokio-postgres-rustls` | TLS for PG direct connections |
| `reqwest` | Stream Load HTTP client (rustls-tls, 307 redirect, HTTPS) |
| `mysql_async` | StarRocks MySQL protocol (DDL, snapshot table creation; `prefer_socket(false)`) |
| `rustls` + `rustls-pemfile` + `rustls-native-certs` | TLS library + PEM parsing + system CA loading |
| `serde` / `serde_json` | JSON serialization (Stream Load NDJSON, config) |
| `clap` | CLI argument parsing |
| `tracing` / `tracing-subscriber` / `tracing-appender` | Structured logging + rotating file appender |
| `anyhow` | Error handling |
| `regex` | table_exclude regex + compiled glob-to-regex table_filter |
| `toml` | Config parsing (`deny_unknown_fields` for typo detection) |
| `bytes` | Zero-copy Stream Load payload clone |
| `metrics` + `metrics-exporter-prometheus` | SLO histograms + Prometheus recorder for `/metrics` |
| `sha2` | Stable schema hashing for gap detection (avoids djb2 collisions) |
| `fs2` | flock-based single-instance enforcement |
| `rusty-s3` + `url` + `percent-encoding` | S3-compatible checkpoint store (presigned URLs) |
| `fastrand` | Jittered exponential backoff (thundering-herd prevention) |
| `futures` | Parallel pool shutdown (`join_all`) |
| `async-trait` | Async trait bounds |
| `time` | S3 presigned-URL signing timestamps |

---

## Project Structure

```
pg2sr_cdc/
├── Cargo.toml
├── Cargo.lock
├── config.toml              # Local working config (dev defaults; override per environment)
├── config-sample.toml        # Starting template (TLS, 3-node HA, S3 checkpoint, metrics)
├── docker-compose.yml        # PG + SR + MinIO stack for local/dev
├── PROJECT.md                # This file
├── README.md                 # Quick start reference
└── src/
    ├── main.rs               # CLI + orchestration + DDL post-processing + catalog fallback + signal handling
    ├── config.rs             # TOML config (serde defaults, deny_unknown_fields, TLS, glob→regex)
    ├── cdc.rs                # PG WAL consumer + table_filter + relation cache + PK + OID resolution + idle ACK
    ├── checkpoint/           # LSN checkpoint persistence (trait-backed store)
    │   ├── mod.rs            #   Checkpoint struct + CheckpointStore trait + in-memory state
    │   ├── local.rs          #   LocalStore (filesystem, atomic temp+rename, .corrupted recovery)
    │   └── s3.rs             #   S3Store (rusty-s3 presigned URLs + reqwest, two-stage upload)
    ├── ddl.rs                # DDL capture table + event trigger setup/teardown
    ├── ddl_poller.rs         # DDL poller loop: transpilation, catalog fallback, 3-tier error handling
    ├── metrics_server.rs     # Optional Prometheus /metrics + JSON /health HTTP endpoint (raw tokio TCP)
    ├── pipeline.rs           # Event buffering + concurrent per-table flush + auto-create + table_filter
    ├── pipeline/
    │   └── value_conv.rs     #   Type-aware JSON value conversion (string_to_json_value, BYTEA hex strip)
    ├── pg_catalog.rs         # PG catalog queries (OID, columns, PK, user tables)
    ├── sink.rs               # reqwest Stream Load (HA failover, 307 redirect, retry, unhex transforms)
    ├── snapshot.rs           # Initial COPY + BYTEA encode + gap detection + keyset pagination
    ├── sr_connection.rs      # StarRocks HA connection manager (multi-node failover, active-node cache)
    ├── sr_sql.rs             # SR DDL helpers (quote, table_props, create_sql, reorder)
    ├── types.rs              # PG→SR type mapping + ARRAY parser + BYTEA hex strip + PK compatibility
    └── util.rs               # Shared utilities (AbortOnDrop, jittered backoff, duration formatting)
```

---

## Key Design Decisions

### reqwest for Stream Load (not curl)
`reqwest` handles `Expect: 100-continue` correctly via hyper. The 307 FE→BE redirect
strips auth headers, solved with `redirect::Policy::none()` + manual Location header
extraction. Zero subprocess overhead.

### mysql_async for SR SQL (not python3)
All Rust MySQL libraries send `SET @@socket` during handshake, which StarRocks rejects.
Fixed with `prefer_socket(false)` and `stmt_cache_size(0)`.

### pgwire-replication for WAL (not tokio-postgres)
`tokio-postgres` doesn't expose the CopyBoth protocol needed for logical replication.

### Row-based batching (not transaction-based)
Counts individual row changes, not PG transactions. Configurable via `batch_rows` and
`batch_timeout_ms`.

### Multi-node StarRocks HA (SrConnectionManager)
Semicolon-separated FE nodes in `node_urls`. Active-node cache shared between SQL and
HTTP paths.

### Auto-create missing SR tables
When Stream Load fails with "unknown table", generates CREATE TABLE from WAL relation
info and retries. Self-healing for CDC-before-DDL race conditions.

### Native ARRAY type support
PG `{a,b,c}` → JSON arrays for SR `ARRAY<type>`. 35+ type OIDs, nested arrays, JSONB
elements. Previously mapped to flat `VARCHAR(65533)`.

### BYTEA→VARBINARY with unhex transforms
Snapshot uses `encode(col, 'hex')`, CDC strips `\\x` prefix, Stream Load uses
`unhex()` column transform.

### STRING for unbounded text
PG TEXT, XML, and unknown types map to SR's native `STRING` instead of arbitrary
VARCHAR limits.

### Reconnect with exponential backoff + idle detection
500ms→30s exponential backoff. Idle detection via pgwire's internal `idle_wakeup_interval=1s`
(wakes the recv loop every second to detect silent TCP drops).

### Single-instance enforcement via flock
`fs2::FileExt::try_lock_exclusive()` on `/tmp/{database}.lock` prevents multiple pipeline
instances from writing to the same SR database simultaneously. Lock released by kernel on
process exit (normal, panic, or kill -9).

### Cross-host single-instance guard via PG slot active_pid
The local flock above only protects against same-host duplicates — two instances on different
machines would both pass the flock and race into the snapshot/CDC phases. The authoritative
guard is PostgreSQL's replication slot invariant: only one WAL sender can attach to a logical
replication slot at a time. The pipeline enforces this at three points:

1. **Pre-snapshot** (`main.rs`): queries `pg_replication_slots.active_pid` and refuses to start
   if another process is already streaming. Fails fast before the expensive snapshot phase.
2. **Pre-CDC-connect** (`main.rs` outer reconnect loop): re-checks `active_pid` before each
   attach attempt, catching the case where another instance started during the snapshot phase
   (which can take minutes to hours for large databases).
3. **55006 fatal-exit** (`main.rs` CDC connect error path): PG's atomic rejection
   `"replication slot X is active for PID Y"` (SQLSTATE 55006) is treated as a fatal
   misconfiguration, not a transient retry. Previously this case spun forever in a 3s→30s
   backoff loop; it now exits with a clear error message.

**Snapshot-phase TOCTOU race**: two instances starting within ~10ms of each other could
theoretically both pass the pre-snapshot `active_pid` check before either attaches. This
mirrors SeaTunnel's posture of punting snapshot-phase coordination to operator responsibility
("avoid restarting during the snapshot phase"). The 55006 fatal-exit catches the residual
race at connect time.

### Size-capped rotating file logger
WARN+ERROR logs routed to `logs/{name}.log` via `tracing_appender::non_blocking`.
When the file exceeds ~50MB, rotated to `.log.1` (one backup kept). Total disk cap ≈ 100MB.
Console layer (INFO+ via `RUST_LOG`) unaffected.

### Double Ctrl+C handler
First SIGINT/SIGTERM triggers graceful shutdown (flush pending + checkpoint save).
A second SIGINT/SIGTERM immediately calls `std::process::exit(130)` (or `143`),
so a stalled graceful flush (e.g. SR unresponsive for ~340s worst case) can be
escape-hatched in seconds. The first `ctrl_c()` future is consumed catching
the first SIGINT; a second await catches the escape SIGINT.

### Background checkpoint saver with panic recovery
Periodic checkpoint saves (default every 10s) run in a background task with automatic
restart on panic. In-memory checkpoint updated immediately; durable persistence decoupled.
HEALTH SUMMARY includes `checkpoint_save_lag` metric for operator visibility.

### Config validation: max_retries × node_count
Stream Load retry budget is plain: 1 initial attempt per node + `max_retries`
retries per node, across all nodes in `[starrocks].node_urls`. No cross-field
validation — operators are responsible for sizing. With defaults
(3 nodes, `max_retries=2`, `sink_flush_timeout_ms=10000`): total worst-case wait
= 10000ms × 3 × 3 = 90000ms = 90s (+ 10s SHOW LOAD poll budget when
label-already-exists-RUNNING fires). Per-attempt HTTP budget uses
`max(c, 10) × 2` (clamped) to mirror `sink.rs`'s 10s floor on reqwest
per-request timeout — without the clamp, `connection_timeout_ms=5000` would be
approved by the runtime but actually consume 20s per attempt at runtime.

### StarRocks-side limits constraining the pipeline
Several SR-side configuration parameters impose hard or soft limits on the
pipeline's flush behavior. Tune pg2sr_cdc knobs to stay below them:

| SR parameter | Default | pg2sr_cdc knob | Constraint |
|---|---|---|---|
| `streaming_load_max_batch_size_mb` | 100 MB | `pipeline.max_pending_memory_mb` (per-event cap = value / 4) | Per-event cap should be ≤ 100 MB; lower `max_pending_memory_mb` to ≤ 400 MB. Startup warning emitted when exceeded. |
| `tablet_max_versions` | 1000 (shared-nothing) | `pipeline.batch_rows` × `pipeline.max_concurrent_flushes` × flush rate | "too many versions" error if exceeded; reduce `max_concurrent_flushes` or raise `batch_rows`. |
| `lake_ingest_slowdown_threshold` | 100 (shared-data) | same as above | Shared-data: triggers commit delay ("Partition's compaction score is larger than 100.0, delay commit for xxxms"). |
| `lake_compaction_score_upper_bound` | 2000 (shared-data) | same | Shared-data: triggers ingestion stop ("too large compaction score"). |
| `streaming_load_rpc_max_alive_time_sec` | 1200s | Hardcoded 90s per-table-flush timeout (`pipeline.rs:929`) | Pipeline cuts off well before SR-side RPC timeout ✓. |
| `streaming_load_max_mb` | 100 GB | `pipeline.batch_rows` × typical event size | 3–4 orders of magnitude larger than pipeline batches ✓. |
| `transaction_publish_version_worker_count` | 0 (= CPU cores) | `pipeline.max_concurrent_flushes` | Bounded by BE CPU; no hard cap on pipeline side. |

If SR logs "too many versions" (shared-nothing) or "delay commit for xxxms /
too large compaction score" (shared-data), the fix is to reduce concurrent
version generation: raise `batch_rows` (fewer, larger versions) and/or lower
`max_concurrent_flushes`. See `Variant 5` in `config-sample.toml` for a
high-throughput single-table workload template.

### Checkpoint resilience
Corrupted checkpoints renamed to `.corrupted` for forensics. Mutex poison recovery.

### DDL poller resilience
Own reconnect loop with panic recovery. 3-tier error handling: transient network
(retry indefinitely), permanent SQL (3 retries then skip with FATAL log), consecutive
PG query failures (trigger outer reconnect). **Task supervision**: inner DDL poller task wrapped
in `AbortOnDrop` (main.rs:1288) so outer-supervisor abort propagates down the 3-level
chain (outer → `run_ddl_poller_loop` → `run_ddl_poller` → in-flight `conn.query_drop`),
preventing detached DDL tasks from racing `sr_mgr.shutdown()`:
`MAX_DDL_DEFERRALS` FATAL shutdown gated on `ddl_failure_abort` (operators with
`ddl_failure_abort=false` survive stuck-deferral DDLs the same way as other permanent
failures). `PendingCdcTableGuard` dropped before DDL-race retry sleep in
`send_stream_load`, re-acquired with `ddl_active` re-check after — eliminates the
25-72s stall where the flush held the guard across the sleep, blocking the DDL
poller's `wait_for_cdc_drain`.

### TLS crypto provider selection
`ring` explicitly selected via `default_provider().install_default()` to resolve
feature unification conflicts between `aws-lc-rs` and `ring`.

### Graceful shutdown: SIGINT + SIGTERM
Both signals write to a watch channel, propagating to CDC loop, DDL poller, and
metrics reporter. Final checkpoint save before exit.

### Memory-bounded pipeline buffer
`max_pending_memory_mb` (default 512 MB) caps buffered WAL data. `flush_requested()`
returns true when limit approached, forcing immediate flush.

### Slot & publication auto-creation on every start (and every reconnect)
`ensure_replication_slot()` runs at startup AND in reconnect loop. Creates slot
(already-exists non-fatal), publication (idempotent), sets REPLICA IDENTITY.

When `manage_publication = false`, the pipeline verifies the publication exists
in the configured database on every start (and on every reconnect) via
`publication_exists()`. If missing, it exits immediately with an actionable
error — publications are database-local objects, so a publication created in a
different database (e.g. `postgres`) is invisible to the replication connection.
Additionally, if the publication is dropped mid-run (after the early check
passes), the CDC loop exits fatally via `is_publication_missing_error()`
instead of spinning in a reconnect loop, since SQLSTATE 42704 is otherwise
treated as transient.

### Only PRIMARY KEY tables supported
No-PK tables rejected at every path: startup validator, DDL poller, snapshot, CDC.
No DUPLICATE KEY model, no heuristics, no silent data loss.

### PG DEFAULTS stripped in DDL transpilation
SR rejects most PG-style DEFAULT expressions. CDC always provides explicit values.

### Snapshot uses keyset pagination (not LIMIT/OFFSET)
`(pk1, pk2) > (val1, val2)` for single integer PK tables. O(1) per page regardless
of table size.

### CDC per-table flushes are concurrent
Each table flushes in a spawned `tokio::spawn` task. Checkpoint advances only after
all flushes succeed.

### TOAST safety
`ColValue::Unchanged` vs `ColValue::Null` distinction prevents silent data corruption
on UPDATE of large TOAST values.

### DDL capture preserved across restarts
`setup_ddl_capture` split into `create_ddl_capture` (fresh: full DROP/CREATE) and
`ensure_ddl_capture` (restart: idempotent, conditional trigger, no DROP). Prevents
phantom trigger rows on some PG versions.

### DDL poller table_filter changed to WARN-only
DDL poller creates ALL tables in SR for schema sync. CDC data events are filtered by
`table_filter` in both `CdcConsumer` and `Pipeline`. Enables future `table_filter`
expansion without manual table creation.

### Immediate idle KeepAlive ACK
When the CDC stream has no data events, PG sends KeepAlive messages with the current
WAL end position. An `idle_keepalive_pending` flag on `CdcConsumer` triggers immediate
batch return instead of waiting for `batch_timeout`. The idle ACK in `main.rs` then
advances `confirmed_flush_lsn` right away. Keeps replication lag bounded to ~158 bytes
(the minimum from PG internal WAL writes between KeepAlive emission and ACK delivery).

### Background checkpoint saver (decoupled from hot paths)
Checkpoint saves are rate-limited via `save_interval_ms` (default 10000ms). The in-memory
checkpoint (`last_committed_lsn`, `last_ddl_id`) is updated immediately in the batch
flush and idle ACK paths; a background task persists it durably on a timer. This avoids
spamming S3 with PUTs on every batch (potentially 100+ PUTs/sec during idle) and keeps
the hot paths fast. Final save on shutdown via SIGINT/SIGTERM.

---

## Known Limitations

- **DDL transpilation skips unsupported PG statements** (ALTER SEQUENCE, SET/RESET).
  REPLICA IDENTITY ALTER TABLEs skipped before transpilation. Silently dropped after 3 retries.
- **Single-DB-per-pipeline design** — The PG database owns a single set of DDL capture
  objects (`pg2sr_cdc_ddl_trigger`, `pg2sr_cdc_drop_trigger`, `pg2sr_cdc_ddl_func`,
  `pg2sr_cdc_ddl_drop_func`). Names are fixed; they are no longer derived from
  `pipeline_name`. PG event triggers are global database-level objects — running multiple
  pipelines against one PG database is unsupported (the triggers fire on all DDL and would
  duplicate-capture into a shared capture table).
- **DDL capture is global** — The PG event trigger captures ALL table DDL regardless of
  `table_filter`. DDL poller creates all tables in SR; CDC data filtered per `table_filter`.
- **DDL trigger deletion = silent schema evolution loss** — If someone deletes the
  `pg2sr_cdc_ddl_trigger` or `pg2sr_cdc_ddl_events`, schema evolution stops. `--fresh` to recover.
- **`current_query()` NULL on Aurora PG** — both trigger functions handle NULL by
  reconstructing DDL from `object_identity` + `command_tag`.
- **Multidimensional PG arrays** — PG OIDs don't encode array depth. Snapshot-time type
  mapping always produces 1D `ARRAY<INT>`. Workaround: manually alter SR table to
  `ARRAY<ARRAY<INT>>` or `JSON`.
- **Not all PG types can be PRIMARY KEY in SR** — JSON, VARBINARY, FLOAT, DOUBLE, DECIMAL,
  and ARRAY types incompatible as key columns. Startup validator catches these.
- **StarRocks requires key columns first** — automatically handled by
  `reorder_columns_for_sr()` across all DDL paths.
- **Single replication slot** — One PG logical replication slot. Multi-consumer requires
  additional slots. Accidental drop requires `--fresh`.
- **Local file checkpoint** — Default `pg2sr_cdc_checkpoint.json`. Container restart requires
  manual restore or `--fresh`. Corrupted files auto-renamed to `.corrupted` for forensics.
- **Memory buffer per active table** — For hundreds of concurrently written tables, memory
  scales linearly. Configurable cap via `max_pending_memory_mb` (default 512MB).
- **MySQL TLS custom CA** — `mysql_async` doesn't expose `PathOrBuf`, so custom CA certs
  for SR MySQL connections can't load from file path. Use system CA store or disable MySQL TLS.
- **NOT NULL policy** — Only PRIMARY KEY columns are NOT NULL on SR. Non-PK columns nullable
  even if NOT NULL in PG. Applies to all four CREATE TABLE paths.
- **Snapshot BYTEA tables are slow** — `encode(col, 'hex')` produces 2× binary size.
  Large binary columns cause ~2MB NDJSON lines.
- **Metrics off by default** — Runtime metrics (rows, batches, flush latency
  p50/p95/p99, LSN, reconnect count, pending memory, checkpoint save lag) are
  tracked via `AtomicU64` counters and the `metrics`-crate Prometheus recorder.
  A `/metrics` + `/health` HTTP endpoint exists (`src/metrics_server.rs`) but is
  **disabled by default** — opt in via `[metrics].enabled = true`. When
  disabled, metrics are still emitted as periodic `tracing::debug!` (every 5s)
  and `tracing::info!` (every 5min) log messages for log-scraping setups.
- **No dead-letter queue (DLQ) for corrupt WAL** — On `pgoutput` parse error, the WAL record
  is skipped after `consecutive_parse_errors` (hardcoded 10) triggers a fatal exit. There is no
  DLQ table, error topic, or replay buffer. This is intentional: corrupt WAL indicates a serious
  upstream issue (PG bug, incompatible pgoutput version, data corruption) that requires operator
  intervention. An automatic DLQ would silently route poison data to a sidecar table, hiding the
  problem until downstream consumers discover the gap. The pipeline fails fast so operators can
  investigate root cause and manually advance the replication slot past the corrupt record or
  restore from backup.
- **JSONB → JSON operator loss** — SR's JSON type does not support PG JSONB operators
  (`@>`, `<@`, `?`, `?|`, `?&`, `#>>`, etc.). Data fidelity is preserved; query
  semantics are not. Migrate JSONB predicates to SR's `json_query`/`json_array` functions.
- **`NUMERIC[]` → `ARRAY<STRING>`** — PG `NUMERIC[]` arrays map to `ARRAY<STRING>` in SR
  for the same truncation-avoidance reason as bare NUMERIC. Operators expecting
  `ARRAY<DECIMAL>` will get `ARRAY<STRING>`.
- **`bytea_output = hex` required** — CDC path assumes PG's default `bytea_output = hex`
  (default since PG 9.0). Setting `bytea_output = escape` silently corrupts BYTEA data.
  Startup validation now checks this and fails fast if violated.
- **Single-transaction event limit** — A single PG transaction with >100K WAL events
  (bulk INSERT/DELETE) triggers a reconnect error. Split large transactions or increase
  `max_tx_events` in config.

---



## Remaining Work

### Medium Priority

#### Multi-region / HA checkpoint
**Effort**: ~1-2h
Store checkpoint in a PG table or SR table for durability across restarts.

#### Parallel snapshot
**Effort**: ~2-3h
Spawn `tokio::spawn` per-table tasks during snapshot. PG connection pool needed.

### Low Priority / Large Effort

#### BYTEA snapshot base64
**Effort**: ~1-2h
Switch from `encode(col, 'hex')` to `encode(col, 'base64')` — 33% smaller NDJSON.
Compatible with `from_base64()` or keep `unhex()` (SR 3.x supports both).

#### Multi-threaded CDC reader
**Effort**: ~1-2 days
Pipeline stages (reader → buffer → flusher) or multiple slots. Single-task reader is
already well above the flush rate — unlikely bottleneck.

---

## table_filter Change → Re-Snapshot + CDC Resume

**Status**: ✅ **Implemented** (v5.6.0+)

### Problem
When a user changes `table_filter` (e.g., from `"*"` to `"orders_*"`), newly
matching tables need bulk copy to SR. (`table_filter` is a glob over bare table
names; the PG schema is configured separately via `postgres.schema`.)

### Implementation (Option A — startup hash comparison)

A `table_filter: Option<String>` field on the `Checkpoint` struct (in
`src/checkpoint/mod.rs`). On startup, the normalized (trimmed) `table_filter`
value is saved alongside the checkpoint. On subsequent starts, the current
value is compared with the saved one:

- If the filter expanded (e.g. `*` → `orders_*` to add a prefix set, or
  switching `schema` to cover more tables), the **existing gap detection** in
  `run_snapshot()` handles it automatically — newly matching tables aren't in
  `snapshotted_tables` and get snapshotted. A log message is emitted:
  `table_filter changed: 'X' → 'Y' — gap detection will snapshot newly
  matching tables`.
- If the filter was restricted (e.g. `*` → `orders_*`), existing SR tables for
  non-matching tables remain (no cascade drop) and CDC stops for them
  automatically via `CdcConsumer.should_replicate_table()`.
- On `--fresh` start, the filter is saved fresh.
- Default `None` (backward compatible with checkpoints from before this feature).

### Current State
- ✅ **Filter-change detection** — startup hash comparison of the normalized filter string
- ✅ **Gap detection** — already re-snapshots tables not in `snapshotted_tables`
- ✅ **CDC consumer** — reloads relations on reconnect via `load_relations()`
- ✅ **DDL poller** — creates all tables in SR regardless of filter (WARN-only for non-matching)
- ✅ **Publication** — DDL trigger adds all PK tables automatically

