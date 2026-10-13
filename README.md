# pg2sr_cdc — PostgreSQL → StarRocks CDC Pipeline

A real-time Change Data Capture (CDC) pipeline from PostgreSQL (logical replication)
to StarRocks (Stream Load), built for Aurora PG and self-managed PostgreSQL instances.

## Status & Provenance

**Do not use in production. Use at your own risk.**

This is **AI-assisted generated code** — an experiment to see how a purpose-built
PostgreSQL → StarRocks CDC pipeline would behave, written after every existing solution we
evaluated turned out to be broken in some way.

It has only ever been exercised against **dev and staging environments; it has never run in
production**. Treat it as a prototype: no production support, no warranty, and the guarantees
described below (and in `PROJECT.md`) reflect intent and test coverage rather than long-running
operational experience.

## Architecture

```
┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  PostgreSQL   │───▶│   pg2sr_cdc      │───▶│   StarRocks      │
│  Logical      │    │                  │    │   (Stream Load)  │
│  Replication  │    │  ┌────────────┐  │    │                  │
│  + DDL Event  │    │  │ CDC Loop   │──┼───▶│  upsert DELETE   │
│  Triggers     │    │  │ (pgoutput) │  │    │  by PK           │
└──────────────┘    │  ├────────────┤  │    └──────────────────┘
                    │  │ DDL Poller │  │
                    │  │ (schema    │  │
                    │  │  sync)     │  │
                    │  ├────────────┤  │
                    │  │ Snapshot   │  │
                    │  │ (initial + │  │
                    │  │  gap)      │  │
                    │  └────────────┘  │
                    └──────────────────┘
                    (optional Prometheus /metrics + /health
                     endpoint via the [metrics] config section)
```

### Components

- **CDC Loop** — consumes the PG logical replication stream (`pgoutput` plugin).
  Handles INSERT → UPSERT, UPDATE → UPSERT, DELETE in StarRocks.
  Uses PK-upsert idempotency for at-least-once delivery.
  TOAST-safe: unchanged TOAST columns are omitted from the Stream Load JSON payload
  and the `partial_columns: true` header preserves their existing SR value.

- **DDL Poller** — polls a PG event-trigger capture table for schema changes.
  Transpiles PG DDL to StarRocks-compatible SQL via `polyglot-sql`.
  Falls back to catalog introspection for types `polyglot-sql` can't handle.

- **Snapshot** — bulk-copies existing table data from PG to SR on first run
  (`--fresh`) or when gap detection detects schema changes. Uses shadow-table
  pattern to prevent data loss if the snapshot fails mid-way.

- **Metrics server** — optional Prometheus text-format `/metrics` and JSON
  `/health` HTTP endpoint served via `[metrics]` config (default off; port 9091).
  Runtime counters are tracked via `AtomicU64` and rendered by a lightweight
  raw tokio TCP listener (`src/metrics_server.rs`).

## Quick Start

### Prerequisites

- PostgreSQL 14+ with `wal_level = logical` and a replication slot
- StarRocks 3.x with Stream Load enabled
- Rust 1.75+ toolchain

### PostgreSQL Setup (RDS / Aurora)

Run this once on the PostgreSQL side before using the tool. On RDS/Aurora,
logical replication must be enabled first — set the parameter in the DB
parameter group and reboot the cluster. On self-managed PostgreSQL, set
`wal_level = logical` in `postgresql.conf` instead.

```sql
-- before we start anything, database needs logical replication enabled.
-- set the following parameter (parameter group) and reboot the cluster.
rds.logical_replication = 1

-- step 1, create replication user, global.
CREATE ROLE replication_user WITH LOGIN PASSWORD 'mysecurepassword';
GRANT rds_replication TO replication_user;
GRANT rds_superuser TO replication_user;
GRANT pg_read_all_data TO replication_user;

-- step 2, create publication per database.
CREATE PUBLICATION test_pub FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate');

-- step 3, revoke superuser privileges from the user when
-- initial setup is done (slot/trigger creation).
-- to be more safe, run when CDC phase starts.
REVOKE rds_superuser FROM replication_user;
```

**IMPORTANT NOTE.** If you have tables without a PK, you need to grant
`REPLICA IDENTITY FULL`, otherwise updates/deletes won't work with those
tables (on the postgres side!). Also, PK-less tables are not synced to the
SR side — this is only used to avoid update/delete failures on the postgres
side. The following script outputs which tables require `REPLICA IDENTITY
FULL`; run the output with `rds_superuser` to grant full identity:

```sql
SELECT format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', n.nspname, c.relname)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND c.relpersistence = 'p'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND (c.relreplident = 'n'
       OR (c.relreplident = 'd' AND NOT EXISTS (
             SELECT 1 FROM pg_catalog.pg_index i
             WHERE i.indrelid = c.oid AND i.indisprimary)));
```

### Configuration

All timeouts/intervals/delays are specified in **milliseconds** (field names end
in `_ms`). `table_filter` and `table_exclude` are bare-table-name filters (one
glob, one regex — see `[config.rs](src/config.rs)` for semantics).

```toml
[pipeline]
name = "my_pipeline"
schema_sync = true
ddl_capture_table = "public.my_ddl_log"
snapshot_page_size = 50000
batch_rows = 5000
batch_timeout_ms = 500
max_retries = 2
query_timeout_ms = 30000
ddl_failure_abort = true
ddl_poll_interval_ms = 5000
schema_change_retry_ms = 1500

[postgres]
host = "localhost"
port = 5432
database = "mydb"
username = "replicator"
password = "secret"
schema = "public"
table_filter = "*"
table_exclude = ""
slot_name = "my_pipeline_slot"
publication = "my_pipeline_pub"
manage_publication = true
connect_timeout_ms = 30000

# Optional — TLS for PG replication + direct connections
# [postgres.tls]
# mode = "disable"   # disable | prefer | require | verify-ca | verify-full

[starrocks]
# Each entry: host | host:http_port | host:http_port:jdbc_port
# HTTP port defaults to 8030, JDBC to 9030.
node_urls = "localhost:8030"
database = "sr_db"
username = "root"
password = ""
connect_timeout_ms = 10000

# Optional — Prometheus metrics endpoint
# [metrics]
# enabled = true
# port = 9091
```

### Running

For new deployments, copy `config-sample.toml` (fully-commented starting template with TLS,
3-node HA StarRocks, S3 checkpointing, metrics, and `CHANGE_ME` credential
placeholders) and edit it — see in-file comments for the flush-timeout budget
rule and per-field guidance.

```bash
# Fresh start (drops all SR tables, re-snapshots everything)
pg2sr_cdc -c config.toml --fresh

# Normal restart (resumes from checkpoint)
pg2sr_cdc -c config.toml
```

## CDC Guarantees

| Property | Guarantee | Notes |
|---|---|---|
| **Delivery** | At-least-once | PK-upsert idempotency handles duplicates |
| **Ordering** | Per-table causal | WAL position tracked; cross-table order best-effort |
| **Cross-table txns** | ❌ Not atomic | Stream Load is per-table; no two-phase commit |
| **DDL sync** | Near-real-time | Poll interval configurable (default 5s) |
| **Snapshot consistency** | Best-effort | No exported snapshot handshake; PK-upsert reconciles minor skew |

## CLI Reference

| Flag | Description |
|---|---|
| `-c PATH` / `--config PATH` | Path to TOML config file (default: `config.toml`) |
| `--fresh` | Fresh start: delete checkpoint, drop & recreate replication slot + publication, tear down DDL capture infra, and re-snapshot all matching tables from PG into StarRocks (replacing SR data via per-table shadow-table swap). Does **not** drop SR tables directly. |
| `--name NAME` | Override pipeline name (default: from config) |

## Operational Notes

### Checkpoint

The pipeline checkpoint is stored locally (`pg2sr_cdc_checkpoint.json`) or in
S3 (`s3://bucket/path`) via the `[checkpoint]` config section. It tracks:
- Last committed LSN (WAL position)
- Snapshotted tables and their schema hashes (for gap detection)
- Last DDL id processed

### Logging & Metrics

- **Console**: INFO+ via `RUST_LOG` env var
- **File**: `logs/{name}.log`, ~50 MB rotating with one backup (WARN+ routed here)
- **Prometheus**: optional `/metrics` (text exposition format) and JSON `/health`
  HTTP endpoint via the `[metrics]` config section (default off; `port = 9091`).
  Binds to `127.0.0.1` by default (configurable via `host`); set `host = "0.0.0.0"`
  only when external scraping is intended.
  Runtime counters (rows, batches, flush latency histograms, LSN, reconnect count,
  pending memory, checkpoint save lag, error count) are tracked via `AtomicU64` +
  the `metrics` crate with a Prometheus recorder. `/health` returns 503 when the
  checkpoint saver has been stuck for > 300s (saver dead/stuck).

### Graceful Shutdown (SIGTERM/SIGINT)

1. Signal received → shutdown watch channel fires
2. CDC loop finishes current batch and flushes to SR
3. DDL poller finishes current cycle
4. Checkpoint saved with final LSN

A second Ctrl+C (or SIGTERM) forces `std::process::exit(130)` (or `143`),
so a stalled graceful flush can be escape-hatched in seconds.

On restart, the pipeline resumes from the last checkpoint LSN.
The `--fresh` flag is only needed when you want to discard all existing
SR data and rebuild from scratch.

### Restart Behaviour

- **Without `--fresh`**: DDL infrastructure is preserved. The capture table
  retains all DDL events. DDL triggers are checked via `CREATE IF NOT EXISTS`.
- **With `--fresh`**: All DDL infrastructure is torn down and recreated.
  Existing checkpoints are deleted. Replication slot is reset.

## Schema Evolution (DDL Sync)

The DDL poller captures these events and transpiles them to StarRocks:

| PG DDL | SR Handling |
|---|---|
| CREATE TABLE | Creates SR table with PRIMARY KEY model, snapshots existing data |
| ALTER TABLE ADD COLUMN | Syncs column to SR (idempotent — `IF NOT EXISTS` semantics) |
| ALTER TABLE DROP COLUMN | Drops column in SR (strips `IF EXISTS` which SR doesn't support) |
| ALTER TABLE ADD PRIMARY KEY | Drops SR table, recreates with PK, re-snapshots all data |
| ALTER COLUMN TYPE / RENAME COLUMN / RENAME TO | Drops SR table, re-snapshots from PG catalog (handles `USING expr`; atomic shadow-swap) |
| ALTER TABLE SET SCHEMA / CLUSTER / RLS / TRIGGER / VALIDATE CONSTRAINT | Skipped (PG-only metadata, no SR equivalent) |
| DROP TABLE (multi-target) | Emits per-row identity, guards+drains each table separately |
| DROP TABLE | Drops SR table |

For ALTER COLUMN TYPE / RENAME COLUMN / RENAME TO, the pipeline drops the
SR table and re-snapshots it from the PG catalog (which already has the
change applied). The new schema — including `USING expr` conversions — is
captured atomically via shadow-table swap. If a resnapshot fails, the old
SR table remains intact and the DDL poller retries on the next cycle.

## Known Limitations

1. **No exported snapshot handshake** — Snapshot and CDC slot are independent,
   so a small window of CDC data between snapshot start and slot creation may
   be duplicate or missed. PK-upsert idempotency reconciles duplicates.
2. **PK-less tables not supported** — StarRocks requires a PRIMARY KEY for
   Stream Load upsert/deletes. Tables without PK are skipped.
3. **Cross-table transaction atomicity** — StarRocks Stream Load operates on
   one table at a time. A multi-table PG transaction may be partially applied.
4. **Snapshot safety** — Uses a shadow-table pattern (`{table}_snapshot_temp`).
   If a snapshot fails mid-way, the shadow table is left as a "safe zombie"
   with all data intact. The original table is untouched. Manually rename
   the shadow table to recover.

## Development

### Running Tests

```bash
cargo test                    # unit + integration tests (no DB needed)
cargo test -- --nocapture     # with output
cargo clippy --all-targets    # lint
```

For end-to-end checks against a real stack, bring up the bundled
`docker-compose.yml` (PostgreSQL + StarRocks shared-data + MinIO) and run a
`--fresh` pass against it.

### Required PG Permissions

See [PostgreSQL Setup (RDS / Aurora)](#postgresql-setup-rds--aurora) for the
full one-time setup (user creation, publication, and the recommended
`REVOKE rds_superuser` once the initial slot/trigger setup is done).

The pipeline user needs:
- `REPLICATION` attribute (for logical replication slot)
- `CREATE` on the capture table schema (for DDL trigger infrastructure)
- `CREATE PUBLICATION` (if `manage_publication = true`)
- Event trigger privileges for DDL capture

## Config Reference

See [config.rs](src/config.rs) for the complete list of fields with defaults
and documentation, and `config-sample.toml` for a fully-commented starting
template.

## License

MIT — see [LICENSE](LICENSE).
