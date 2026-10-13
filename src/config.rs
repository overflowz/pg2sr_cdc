//! Configuration for the pg2sr_cdc CDC pipeline.
//! TOML-based config with serde defaults, TLS configuration for all connection
//! types (PG replication, PG direct, SR HTTP, SR MySQL), node URL parser for
//! multi-node StarRocks HA, and table filter regex compilation.
use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub postgres: PgConfig,
    pub starrocks: SrConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    /// Checkpoint store configuration. Optional — defaults to local file store
    /// with path `"pg2sr_cdc_checkpoint.json"` when this section is absent.
    #[serde(default)]
    pub checkpoint: Option<CheckpointConfig>,
    /// Prometheus metrics endpoint configuration.
    /// When absent or `enabled = false`, no HTTP listener is started.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    /// PostgreSQL schema to replicate. Must be a plain identifier (no regex/glob chars).
    /// Default: "public"
    #[serde(default = "default_pg_schema")]
    pub schema: String,
    /// Pipeline instance name — set on every PostgreSQL connection as
    /// `application_name` for identification in pg_stat_activity.
    #[serde(skip)]
    pub app_name: String,
    /// Glob pattern to filter replicated tables by bare table name.
    /// Only tables matching this glob are snapshotted and CDC-replicated.
    /// Supports `*` (any sequence) and `?` (any single char).
    /// Default: "*" (all tables)
    #[serde(default = "default_table_filter")]
    pub table_filter: String,
    /// Glob pattern to exclude tables from replication (applied after table_filter).
    /// Tables matching this pattern are skipped even if they match table_filter.
    /// Supports `*` and `?` wildcards.
    /// Default: "" (no exclusion)
    #[serde(default = "default_table_exclude")]
    pub table_exclude: String,
    /// Replication slot name
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    /// Publication name
    #[serde(default = "default_publication")]
    pub publication: String,
    /// If true (default), the pipeline creates/drops/alters the publication
    /// and adds tables to it automatically.
    /// If false, the publication is expected to be pre-created and managed
    /// by a DBA — the pipeline will not CREATE, DROP, or ALTER PUBLICATION.
    #[serde(default = "default_manage_publication")]
    pub manage_publication: bool,
    /// TLS configuration for PostgreSQL connections.
    /// Default: disabled (plain TCP).
    #[serde(default)]
    pub tls: PgTls,
    /// Connection timeout in milliseconds for PostgreSQL replication connections.
    /// Bounds the initial TCP connect + auth + START_REPLICATION negotiation.
    /// Default: 30000 (30s).
    #[serde(default = "default_pg_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

// Manual Debug impl: redacts password to prevent accidental credential leaks
// in logs or panic messages.
impl std::fmt::Debug for PgConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"[redacted]")
            .field("schema", &self.schema)
            .field("app_name", &self.app_name)
            .field("table_filter", &self.table_filter)
            .field("table_exclude", &self.table_exclude)
            .field("slot_name", &self.slot_name)
            .field("publication", &self.publication)
            .field("manage_publication", &self.manage_publication)
            .field("tls", &self.tls)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .finish()
    }
}

/// TLS configuration for PostgreSQL connections.
/// Mirrors PostgreSQL's sslmode parameter semantics.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgTls {
    /// SSL mode: "disable" (default), "prefer", "require", "verify-ca", "verify-full".
    #[serde(default = "default_tls_mode")]
    pub mode: String,
    /// Path to PEM file containing trusted CA certificates.
    /// Required for verify-ca and verify-full modes.
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
}

impl Default for PgTls {
    fn default() -> Self {
        Self {
            mode: default_tls_mode(),
            ca_cert: None,
        }
    }
}

/// Default StarRocks FE HTTP port.
pub const DEFAULT_FE_HTTP_PORT: u16 = 8030;
/// Default StarRocks FE JDBC (MySQL) port.
pub const DEFAULT_FE_JDBC_PORT: u16 = 9030;

/// A single StarRocks FE node with its own host and optional per-node ports.
/// Parsed from the semicolon-separated `node_urls` field in `SrConfig`.
#[derive(Debug, Clone)]
pub struct SrNodeConfig {
    pub host: String,
    pub fe_http_port: u16,
    pub fe_jdbc_port: u16,
}

/// Parse a single node segment from `node_urls`.
/// Supports formats:
///   - `host` — uses default ports (8030/9030)
///   - `host:http_port` — explicit HTTP port, JDBC defaults to 9030
///   - `host:http_port:jdbc_port` — both ports explicit
fn parse_node_segment(segment: &str) -> Result<SrNodeConfig, String> {
    let segment = segment.trim();
    if segment.is_empty() {
        return Err("Empty node segment".to_string());
    }
    let parts: Vec<&str> = segment.split(':').collect();
    match parts.len() {
        1 => Ok(SrNodeConfig {
            host: parts[0].to_string(),
            fe_http_port: DEFAULT_FE_HTTP_PORT,
            fe_jdbc_port: DEFAULT_FE_JDBC_PORT,
        }),
        2 => {
            let http_port = parts[1].parse::<u16>().map_err(|e| {
                format!("Invalid HTTP port '{}' in '{}': {e}", parts[1], segment)
            })?;
            Ok(SrNodeConfig {
                host: parts[0].to_string(),
                fe_http_port: http_port,
                fe_jdbc_port: DEFAULT_FE_JDBC_PORT,
            })
        }
        3 => {
            let http_port = parts[1].parse::<u16>().map_err(|e| {
                format!("Invalid HTTP port '{}' in '{}': {e}", parts[1], segment)
            })?;
            let jdbc_port = parts[2].parse::<u16>().map_err(|e| {
                format!("Invalid JDBC port '{}' in '{}': {e}", parts[2], segment)
            })?;
            Ok(SrNodeConfig {
                host: parts[0].to_string(),
                fe_http_port: http_port,
                fe_jdbc_port: jdbc_port,
            })
        }
        _ => Err(format!(
            "Invalid node URL '{segment}'. Expected format: host, host:http_port, or host:http_port:jdbc_port"
        )),
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SrConfig {
    /// Semicolon-separated list of FE node URLs.
    /// Each entry is one of:
    ///   - `host` — uses default ports (8030/9030)
    ///   - `host:http_port` — explicit HTTP port, JDBC defaults to 9030
    ///   - `host:http_port:jdbc_port` — both ports explicit
    ///
    /// Example: "fe1;fe2:8031;fe3:8030:9030"
    pub node_urls: String,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Initial connection timeout in milliseconds — bounds the TCP connect +
    /// TLS/auth handshake for BOTH HTTP (Stream Load via reqwest) and MySQL
    /// (JDBC via mysql_async) connections. Does NOT bound query/request
    /// execution — see [pipeline].sink_flush_timeout_ms and
    /// [pipeline].mysql_query_timeout_ms for those.
    /// Default: 10000 (10s).
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    /// StarRocks table properties for CREATE TABLE statements.
    /// This is the single source of truth for user-controlled properties.
    /// Built-in default: replication_num = "1".
    /// Common properties: fast_schema_evolution = "true", enable_persistent_index = "true",
    /// persistent_index_type = "CLOUD_NATIVE", bloom_filter_columns = "id,name"
    /// Format: key-value pairs.
    /// Example: table_properties = { "replication_num" = "3", "fast_schema_evolution" = "true" }
    #[serde(default)]
    pub table_properties: HashMap<String, String>,
    /// Idle timeout in milliseconds for the HTTP connection pool (reqwest's pool_idle_timeout).
    /// Idle connections are closed after this duration to prevent SR from closing them
    /// first (which causes unnecessary TCP+TLS handshakes). Should be ~50-60% of
    /// StarRocks' `be_http_timeout` (default 60s in SR 3.x). Default: 30000ms.
    #[serde(default = "default_http_pool_idle_timeout_ms")]
    pub http_pool_idle_timeout_ms: u64,
    /// Maximum number of MySQL connections per SR FE node in the connection pool.
    /// Bounds memory + FD usage under burst load (concurrent flushes, DDL poller,
    /// and catalog queries). Must be >= 4. Default: 32.
    #[serde(default = "default_mysql_max_pool_connections")]
    pub mysql_max_pool_connections: usize,
    /// TLS configuration for StarRocks Stream Load HTTP connections.
    #[serde(default)]
    pub http_tls: SrTlsConfig,
    /// TLS configuration for StarRocks MySQL SQL connections.
    #[serde(default)]
    pub mysql_tls: SrTlsConfig,
}

// Manual Debug impl: redacts password to prevent accidental credential leaks
// in logs or panic messages.
impl std::fmt::Debug for SrConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SrConfig")
            .field("node_urls", &self.node_urls)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"[redacted]")
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .field("http_pool_idle_timeout_ms", &self.http_pool_idle_timeout_ms)
            .field(
                "mysql_max_pool_connections",
                &self.mysql_max_pool_connections,
            )
            .field("table_properties", &self.table_properties)
            .field("http_tls", &self.http_tls)
            .field("mysql_tls", &self.mysql_tls)
            .finish()
    }
}

/// TLS configuration for a StarRocks connection (HTTP or MySQL).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SrTlsConfig {
    /// Enable TLS for this connection.
    #[serde(default)]
    pub enable: bool,
    /// Path to PEM file containing trusted CA certificates.
    /// If None, uses system CA store.
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Enable automatic schema evolution (DDL polling + transpilation)
    #[serde(default = "default_schema_sync")]
    pub schema_sync: bool,
    /// DDL poll interval in milliseconds
    #[serde(default = "default_ddl_poll_interval_ms")]
    pub ddl_poll_interval_ms: u64,
    /// Max retries for stream load
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Max rows to batch before flushing (default 5000)
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// Max idle time between flushes in ms (default 500)
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    /// DDL capture table in "schema.table" format (e.g. "public.pg2sr_cdc_ddl_events")
    #[serde(default = "default_ddl_capture_table")]
    pub ddl_capture_table: String,
    /// Max pending memory in MB for the pipeline buffer before forcing a flush.
    /// Prevents unbounded memory growth during PG write bursts when SR is slow.
    /// Default: 512 MB.
    #[serde(default = "default_max_pending_memory_mb")]
    pub max_pending_memory_mb: usize,
    /// Hard cap on the total bytes held in durable wal-buffer segments
    /// (`<checkpoint.path>.buffer`) across ALL tables, in MB. The buffer holds
    /// rows for DDL-paused and failed tables until async replay drains them;
    /// with no cap, a permanently failing table — or a replay that cannot list
    /// the segments it wrote — grows the buffer until the disk fills.
    /// Exceeding the cap fails the append closed: the flush returns Err, the
    /// checkpoint does NOT advance, and PG re-sends the WAL once space is freed
    /// by the operator (no row is ever dropped).
    /// 0 disables the cap (pre-cap behavior: unbounded, warned only).
    /// Default: 8192 MB.
    #[serde(default = "default_wal_buffer_max_mb")]
    pub wal_buffer_max_mb: u64,
    /// Snapshot page size in rows (default: 50000).
    /// Controls the number of rows fetched per snapshot query, independent from
    /// batch_rows which controls CDC flush batch size.
    #[serde(default = "default_snapshot_page_size")]
    pub snapshot_page_size: usize,
    /// Timeout in milliseconds for PG catalog queries (DDL poll, relation resolution,
    /// column metadata, snapshot data export). Also bounds the hardcoded
    /// CATALOG_QUERY_TIMEOUT previously in pg_catalog.rs.
    /// Default: 30000 (30s).
    #[serde(default = "default_query_timeout_ms")]
    pub query_timeout_ms: u64,
    /// HTTP Stream Load per-attempt timeout in milliseconds. Bounds the
    /// FE PUT + (if 307 redirected) BE PUT chain — one budget per attempt
    /// shared via a deadline Instant.
    /// Default: 10000 (10s).
    #[serde(default = "default_sink_flush_timeout_ms")]
    pub sink_flush_timeout_ms: u64,
    /// MySQL query execution timeout in milliseconds. Bounds ALL MySQL
    /// queries sent via mysql_async: DDL (ALTER/CREATE/DROP/RENAME), catalog
    /// queries (SHOW TABLES gap detection), SHOW LOAD label polling. Does
    /// NOT bound pool.get_conn() acquisition — see connection_timeout_ms.
    /// Default: 10000 (10s).
    #[serde(default = "default_mysql_query_timeout_ms")]
    pub mysql_query_timeout_ms: u64,
    /// Abort the pipeline on permanent DDL failures (3 retries exhausted).
    /// When true (default), a permanent DDL failure triggers pipeline shutdown
    /// so the operator can investigate and restart after fixing the issue.
    /// When false, the DDL event is FATAL-logged and checkpoint-advanced
    /// (soft-skip) — schema divergence is accepted for uptime.
    /// Default: true (abort on permanent failure).
    #[serde(default = "default_ddl_failure_abort")]
    pub ddl_failure_abort: bool,
    /// Sleep in milliseconds after StarRocks returns "schema change in progress"
    /// error before retrying the same DDL on the next poll cycle. Longer values
    /// give SR more time to complete the schema change but increase backlog delay.
    /// Default: 1500ms (1.5 seconds).
    #[serde(default = "default_schema_change_retry_ms")]
    pub schema_change_retry_ms: u64,
    /// Sleep in milliseconds when a CDC Stream Load fails with "unknown column" due to
    /// DDL poller lag (the column exists in PG WAL but hasn't been applied to SR yet).
    /// After this sleep, the flush retries once. If that also fails, the
    /// pipeline reconnects and replays from WAL.
    /// Default: 5000ms (5s).
    #[serde(default = "default_ddl_race_retry_ms")]
    pub ddl_race_retry_ms: u64,
    /// Maximum number of concurrent per-table Stream Load requests during CDC
    /// flushes. Limits how many tables can be flushed simultaneously to avoid
    /// overwhelming StarRocks with too many concurrent load jobs.
    /// Default: 20.
    #[serde(default = "default_max_concurrent_flushes")]
    pub max_concurrent_flushes: usize,
    /// Max WAL events to accumulate within a single PG transaction before
    /// returning an error to trigger a reconnect. Prevents OOM when a bulk
    /// INSERT/DELETE affecting millions of rows arrives in one transaction.
    /// Default: 100,000 events.
    #[serde(default = "default_max_tx_events")]
    pub max_tx_events: usize,
    /// Base delay in milliseconds between PG replication reconnection attempts.
    /// Uses exponential backoff from this value up to 30s max.
    /// Set higher (e.g. 5000-10000) for tolerance of brief overlap during
    /// rolling deploys. Note: PG's 55006 "replication slot is active for PID"
    /// is now a fatal-exit (not retried), so a slot genuinely held by another
    /// instance will not spin forever — only transient connect failures use
    /// this backoff.
    /// Default: 3000ms (3s).
    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
    /// Pipeline instance name. Used as:
    ///   - `application_name` on all PostgreSQL connections
    ///   - lock file path: `/tmp/{name}.lock` (single-instance enforcement)
    ///   - log file path: `logs/{name}.log`
    ///
    /// Must be a valid filename (no `/`, no null bytes, not empty).
    /// Default: "pg2sr_cdc".
    #[serde(default = "default_pipeline_name")]
    pub name: String,
}

/// Checkpoint store configuration.
/// Controls where and how the pipeline's checkpoint is persisted.
///
/// # Local (default)
/// ```toml
/// [checkpoint]
/// store_type = "local"
/// path = "pg2sr_cdc_checkpoint.json"
/// ```
///
/// # S3 / S3-compatible (MinIO, Ceph Object Store, etc.)
/// ```toml
/// [checkpoint]
/// store_type = "s3"
/// path = "pg2sr_cdc/pipeline-checkpoint.json"
/// bucket = "my-bucket"
/// endpoint = "https://s3.example.com"
/// region = "us-east-1"
/// access_key_id = "YOUR_ACCESS_KEY"
/// secret_access_key = "YOUR_SECRET_KEY"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    /// Store type: "local" (default) or "s3"
    #[serde(default = "default_checkpoint_type")]
    pub store_type: String,
    /// Path for local store, or S3 object key for S3 store.
    #[serde(default = "default_checkpoint_path_str")]
    pub path: String,
    /// Interval in milliseconds between automatic checkpoint saves.
    /// Higher values reduce I/O (critical for S3 stores where every PUT
    /// incurs cost + latency) but increase potential WAL replay on crash.
    /// The in-memory checkpoint is always updated immediately; this only
    /// controls how often it is durably persisted.
    /// Default: 10000ms (10s).
    #[serde(default = "default_checkpoint_save_interval_ms")]
    pub save_interval_ms: u64,
    // ── S3-specific options ──
    /// S3 bucket name (required when store_type = "s3")
    #[serde(default)]
    pub bucket: Option<String>,
    /// S3 region. Default: "us-east-1"
    #[serde(default)]
    pub region: Option<String>,
    /// Custom S3-compatible endpoint URL (e.g., MinIO, Ceph Object Store).
    /// Default: https://s3.amazonaws.com (AWS Standard)
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Use path-style addressing instead of virtual-host-style.
    /// Required for most S3-compatible stores (MinIO, Ceph, etc.).
    /// Default: true (path-style)
    #[serde(default = "default_path_style")]
    pub path_style: bool,
    /// S3 access key ID. Falls back to env/IRSA/IMDS if not set.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// S3 secret access key. Falls back to env/IRSA/IMDS if not set.
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            store_type: default_checkpoint_type(),
            path: default_checkpoint_path_str(),
            save_interval_ms: default_checkpoint_save_interval_ms(),
            bucket: None,
            region: None,
            endpoint: None,
            path_style: default_path_style(),
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

/// Prometheus metrics endpoint configuration.
///
/// When enabled, the pipeline serves a lightweight HTTP listener on the
/// configured port with two routes:
///   - `GET /metrics` — Prometheus text exposition format
///   - `GET /health`   — JSON health status
///
/// No external crates (hyper/axum/prometheus) are used — the listener is
/// a raw tokio TCP listener that formats the existing `Metrics` struct's
/// `AtomicU64` counters into Prometheus text format.
///
/// ```toml
/// [metrics]
/// enabled = true
/// port = 9091
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Enable the Prometheus /metrics HTTP endpoint.
    /// When false (default), no HTTP listener is started.
    #[serde(default)]
    pub enabled: bool,

    /// Port for the /metrics and /health HTTP endpoint.
    /// Default: 9091
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Bind address for the metrics/health HTTP server.
    /// Default `127.0.0.1` (loopback only) so LSN, throughput, reconnect
    /// counts, etc. aren't scrapable by anyone on the network. Set to
    /// `0.0.0.0` explicitly when external scraping (Prometheus across hosts)
    /// is intended — combined with a firewalled port / `auth_token` (future).
    #[serde(default = "default_metrics_host")]
    pub host: String,
}

fn default_metrics_port() -> u16 {
    9091
}

fn default_metrics_host() -> String {
    "127.0.0.1".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            enabled: false,
            port: default_metrics_port(),
            host: default_metrics_host(),
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            schema_sync: default_schema_sync(),
            ddl_poll_interval_ms: default_ddl_poll_interval_ms(),
            max_retries: default_max_retries(),
            batch_rows: default_batch_rows(),
            batch_timeout_ms: default_batch_timeout_ms(),
            ddl_capture_table: default_ddl_capture_table(),
            max_pending_memory_mb: default_max_pending_memory_mb(),
            wal_buffer_max_mb: default_wal_buffer_max_mb(),
            snapshot_page_size: default_snapshot_page_size(),
            query_timeout_ms: default_query_timeout_ms(),
            ddl_failure_abort: default_ddl_failure_abort(),
            schema_change_retry_ms: default_schema_change_retry_ms(),
            ddl_race_retry_ms: default_ddl_race_retry_ms(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
            max_tx_events: default_max_tx_events(),
            max_concurrent_flushes: default_max_concurrent_flushes(),
            name: default_pipeline_name(),
            sink_flush_timeout_ms: default_sink_flush_timeout_ms(),
            mysql_query_timeout_ms: default_mysql_query_timeout_ms(),
        }
    }
}

fn default_pg_schema() -> String {
    "public".to_string()
}
fn default_table_filter() -> String {
    "*".to_string()
}
fn default_table_exclude() -> String {
    String::new()
}
fn default_slot_name() -> String {
    "pg2sr_cdc_slot".to_string()
}
fn default_publication() -> String {
    "pg2sr_cdc_publication".to_string()
}
fn default_manage_publication() -> bool {
    true
}
fn default_schema_sync() -> bool {
    true
}
fn default_ddl_poll_interval_ms() -> u64 {
    5000
}
fn default_max_retries() -> u32 {
    2
}
fn default_batch_rows() -> usize {
    5000
}
fn default_batch_timeout_ms() -> u64 {
    500
}
pub(crate) fn default_ddl_capture_table() -> String {
    "public.pg2sr_cdc_ddl_events".to_string()
}
fn default_pg_connect_timeout_ms() -> u64 {
    30000
}
fn default_tls_mode() -> String {
    "disable".to_string()
}
fn default_connection_timeout_ms() -> u64 {
    10000
}
fn default_http_pool_idle_timeout_ms() -> u64 {
    30000
}
fn default_sink_flush_timeout_ms() -> u64 {
    10000
}
fn default_mysql_query_timeout_ms() -> u64 {
    10000
}
fn default_mysql_max_pool_connections() -> usize {
    32
}
fn default_max_pending_memory_mb() -> usize {
    512
}
/// Durable wal-buffer byte cap (MB). 8 GiB by default: large enough for hours
/// of a stalled table, small enough to protect a typical data volume. 0 disables.
fn default_wal_buffer_max_mb() -> u64 {
    8192
}
fn default_snapshot_page_size() -> usize {
    50000
}
fn default_query_timeout_ms() -> u64 {
    30000
}
fn default_ddl_failure_abort() -> bool {
    true
}
fn default_schema_change_retry_ms() -> u64 {
    1500
}
fn default_ddl_race_retry_ms() -> u64 {
    5000
}
fn default_max_concurrent_flushes() -> usize {
    20
}
fn default_max_tx_events() -> usize {
    100_000
}
fn default_reconnect_delay_ms() -> u64 {
    3000
}
fn default_pipeline_name() -> String {
    "pg2sr_cdc".to_string()
}

/// Validate that a pipeline name is safe for use in filenames and PG identifiers.
/// Returns an error message if invalid.
pub fn validate_pipeline_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("pipeline name must not be empty");
    }
    if name == "." || name == ".." {
        anyhow::bail!("pipeline name must not be '.' or '..'");
    }
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        anyhow::bail!(
            "pipeline name must not contain path separators or null bytes, got: {name:?}"
        );
    }
    // Reject names that are too long (255 bytes is typical FS limit)
    if name.len() > 255 {
        anyhow::bail!("pipeline name too long ({} bytes, max 255)", name.len());
    }
    Ok(())
}
fn default_checkpoint_type() -> String {
    "local".to_string()
}
fn default_checkpoint_path_str() -> String {
    "pg2sr_cdc_checkpoint.json".to_string()
}
fn default_checkpoint_save_interval_ms() -> u64 {
    10000
}
fn default_path_style() -> bool {
    true
}

impl Config {
    /// Load config from a TOML file.
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {path}"))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {path}"))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate config at load time.  Catches zero/empty values that would
    /// cause busy loops, no-op snapshots, or confusing failures at runtime.
    fn validate(&self) -> Result<()> {
        let p = &self.pipeline;
        let pg = &self.postgres;
        let sr = &self.starrocks;

        if p.snapshot_page_size == 0 {
            anyhow::bail!("snapshot_page_size must be > 0 (got 0)");
        }
        if p.batch_rows == 0 {
            anyhow::bail!("batch_rows must be > 0 (got 0)");
        }
        if p.batch_timeout_ms < 100 {
            anyhow::bail!(
                "batch_timeout_ms must be >= 100 (got {})",
                p.batch_timeout_ms
            );
        }
        if p.max_retries == 0 {
            // Allowed for HA clusters where immediate node failover
            // (no same-node retry) is preferred. Stream Load labels are
            // idempotent across node failover (sink.rs:128-133), so a
            // duplicate-label response on the next node is treated as
            // success — no risk of double-application. Not recommended
            // for single-node deployments (no failover target).
            tracing::warn!(
                "max_retries=0 disables same-node Stream Load retries — \
                 immediate failover to next node on any error. Safe for HA \
                 clusters (idempotent labels), risky for single-node SR."
            );
        }
        if sr.connection_timeout_ms < 100 {
            anyhow::bail!(
                "starrocks.connection_timeout_ms must be >= 100ms (got {})",
                sr.connection_timeout_ms
            );
        }
        if p.sink_flush_timeout_ms < 100 {
            anyhow::bail!(
                "pipeline.sink_flush_timeout_ms must be >= 100ms (got {})",
                p.sink_flush_timeout_ms
            );
        }
        if p.mysql_query_timeout_ms < 100 {
            anyhow::bail!(
                "pipeline.mysql_query_timeout_ms must be >= 100ms (got {})",
                p.mysql_query_timeout_ms
            );
        }
        if p.query_timeout_ms < 100 {
            anyhow::bail!(
                "query_timeout_ms must be >= 100ms (got {})",
                p.query_timeout_ms
            );
        }
        if pg.table_filter.is_empty() {
            anyhow::bail!("table_filter must not be empty (got '')");
        }
        // Schema must be a plain PG identifier -- no regex/glob metacharacters
        if pg.schema.is_empty() {
            anyhow::bail!("postgres.schema must not be empty");
        }
        let invalid_chars = [
            '.', '*', '?', '+', '^', '$', '{', '}', '(', ')', '|', '[', ']', '\\',
        ];
        if pg.schema.contains(&invalid_chars[..]) {
            anyhow::bail!(
                "postgres.schema must be a plain PostgreSQL schema name. Got: '{}'",
                pg.schema
            );
        }
        // Connection essentials — empty values cause confusing runtime errors
        if pg.host.is_empty() {
            anyhow::bail!("postgres.host must not be empty");
        }
        if pg.database.is_empty() {
            anyhow::bail!("postgres.database must not be empty");
        }
        if pg.username.is_empty() {
            anyhow::bail!("postgres.username must not be empty");
        }
        if pg.connect_timeout_ms == 0 {
            anyhow::bail!("postgres.connect_timeout_ms must be > 0 (got 0)");
        }
        if sr.database.is_empty() {
            anyhow::bail!("starrocks.database must not be empty");
        }
        // sr.database (and pg.database) feed the single-instance flock
        // path (`/tmp/pg2sr_{database}.lock`). Reject path-traversal/separators
        // so a `database = "../../etc/foo"` can't escape /tmp via the lock
        // path (+ File::create follows symlinks). Reuses the pipeline-name
        // path-safety check. (O_NOFOLLOW symlink hardening needs libc and is
        // left as defense-in-depth future work; name validation blocks the
        // more likely path-traversal attack.)
        validate_pipeline_name(&sr.database)
            .context("starrocks.database is used in the flock path and must be path-safe")?;
        validate_pipeline_name(&pg.database)
            .context("postgres.database must be path-safe (used in identifiers)")?;
        if sr.username.is_empty() {
            anyhow::bail!("starrocks.username must not be empty");
        }
        if sr.node_urls.is_empty() {
            anyhow::bail!("starrocks.node_urls must not be empty");
        }
        // Validate that node_urls parses correctly (catches malformed
        // host:port syntax early instead of at first Stream Load)
        sr.parse_hosts()?;
        if p.max_concurrent_flushes == 0 {
            anyhow::bail!("max_concurrent_flushes must be > 0 (got 0)");
        }
        // Upper bounds: the flush-timeout formula multiplies these together
        // (per_attempt × attempts × nodes), so an unbounded value saturates the
        // Duration arithmetic and the final ×1.2 can panic. Cap at 24h, which is
        // far beyond any sane deployment.
        const MAX_TIMEOUT_MS: u64 = 86_400_000; // 24h
        for (name, value) in [
            ("pipeline.sink_flush_timeout_ms", p.sink_flush_timeout_ms),
            ("pipeline.mysql_query_timeout_ms", p.mysql_query_timeout_ms),
            ("pipeline.query_timeout_ms", p.query_timeout_ms),
            ("starrocks.connection_timeout_ms", sr.connection_timeout_ms),
            ("pipeline.batch_timeout_ms", p.batch_timeout_ms),
            ("pipeline.reconnect_delay_ms", p.reconnect_delay_ms),
        ] {
            if value > MAX_TIMEOUT_MS {
                anyhow::bail!("{name} must be <= {MAX_TIMEOUT_MS}ms (got {value})");
            }
        }
        // max_retries is consumed as `max_retries + 1` (plain u32 add) and as a
        // shift-count sum in compute_flush_timeout; bound it so the arithmetic
        // can never overflow-panic on an accepted config.
        if p.max_retries > 1000 {
            anyhow::bail!("max_retries must be <= 1000 (got {})", p.max_retries);
        }
        // max_pending_memory_mb = 0 would make every event trip the soft AND hard
        // flush limits (max_pending_bytes = 0), turning the pipeline into a
        // flush-per-event loop and disabling the wal-buffer growth warning.
        if p.max_pending_memory_mb == 0 {
            anyhow::bail!(
                "max_pending_memory_mb must be > 0 (got 0 — every event would trip the \
                 flush limits and the wal-buffer growth warning would be disabled)"
            );
        }
        // wal_buffer_max_mb: 0 = uncapped (explicit opt-out). Anything small
        // enough to fail immediately on the first buffered batch is a footgun.
        if p.wal_buffer_max_mb != 0 && p.wal_buffer_max_mb < 16 {
            anyhow::bail!(
                "pipeline.wal_buffer_max_mb must be 0 (uncapped) or >= 16 (got {})",
                p.wal_buffer_max_mb
            );
        }
        if p.wal_buffer_max_mb != 0 && p.wal_buffer_max_mb < 1024 {
            tracing::warn!(
                "pipeline.wal_buffer_max_mb={} is small — a single stalled table can fill it, \
                 after which every buffered flush fails closed until replay drains",
                p.wal_buffer_max_mb
            );
        }
        if sr.mysql_max_pool_connections < 4 {
            anyhow::bail!(
                "mysql_max_pool_connections must be >= 4 (got {})",
                sr.mysql_max_pool_connections
            );
        }
        // Validate table_exclude regex at load time (fail-fast instead of
        // at snapshot/CDC runtime).
        crate::config::compile_table_exclude(&pg.table_exclude)
            .with_context(|| format!("Invalid table_exclude pattern '{}'", pg.table_exclude))?;
        // Validate PG TLS mode string at load time (fail-fast instead of
        // at first PG connection).
        pg_tls_ssl_mode(&pg.tls.mode)
            .with_context(|| format!("Invalid postgres.tls.mode '{}'", pg.tls.mode))?;

        // Validate checkpoint save_interval_ms — 0 causes a busy-loop
        // where the background saver spins as fast as the runtime allows,
        // thrashing disk (LocalStore) or incurring S3 PUT costs (S3Store).
        if let Some(ref cp) = self.checkpoint {
            if cp.save_interval_ms == 0 {
                anyhow::bail!(
                    "checkpoint.save_interval_ms must be > 0 (got 0 — \
                     would busy-loop saves and thrash disk/S3)"
                );
            }
            if cp.save_interval_ms > 3_600_000 {
                tracing::warn!(
                    "checkpoint.save_interval_ms={} is very large — crash \
                     replay may be expensive",
                    cp.save_interval_ms
                );
            }
        }
        // Warn when max_pending_memory_mb implies an uncapped per-event cap
        // above SR's streaming_load_max_batch_size_mb (100 MB JSON body limit).
        // The effective cap = min(max_pending_memory_mb / 4, 90MB) — the 90MB
        // clamp (see main.rs) routes any larger single event to the FATAL
        // poison-pill path instead of an infinite SR-rejection loop. Events in
        // the (90MB, cap) band that used to be accepted are now FATAL.
        let per_event_cap_mb = p.max_pending_memory_mb / 4;
        if per_event_cap_mb > 90 {
            tracing::warn!(
                "max_pending_memory_mb={} implies per-event cap {}MB, above the \
                 90MB clamp derived from StarRocks' streaming_load_max_batch_size_mb=100MB \
                 (JSON body limit). The effective max_event_bytes is clamped to 90MB: \
                 any single WAL event larger than 90MB will FATAL-exit the pipeline \
                 (poison pill) instead of looping on SR rejection. This is intended — \
                 remove oversized rows or advance the replication slot to recover.",
                p.max_pending_memory_mb,
                per_event_cap_mb
            );
        }
        // Validate that configured CA cert paths exist when TLS is actually
        // enabled — fails fast with a clear config error instead of a confusing
        // TLS handshake failure later at first connect. (Only checked when TLS
        // is enabled: a ca_cert set with TLS disabled is a harmless no-op the
        // operator may pre-stage, and lets config templates carry placeholder
        // paths without requiring the files to exist at parse time.)
        fn check_ca_cert(tls_name: &str, tls_enabled: bool, ca: &Option<PathBuf>) -> Result<()> {
            if !tls_enabled {
                return Ok(());
            }
            if let Some(ref p) = ca {
                if !p.exists() {
                    anyhow::bail!(
                        "{}.ca_cert path '{}' does not exist — check the config",
                        tls_name,
                        p.display()
                    );
                }
            }
            Ok(())
        }
        check_ca_cert("postgres.tls", pg.tls.mode != "disable", &pg.tls.ca_cert)?;
        check_ca_cert(
            "starrocks.http_tls",
            sr.http_tls.enable,
            &sr.http_tls.ca_cert,
        )?;
        check_ca_cert(
            "starrocks.mysql_tls",
            sr.mysql_tls.enable,
            &sr.mysql_tls.ca_cert,
        )?;
        Ok(())
    }
}

impl SrConfig {
    /// Parse the semicolon-separated `node_urls` field into a list of FE nodes.
    /// Each entry must be `host:http_port` or `host:http_port:jdbc_port`.
    /// Returns an error if no valid nodes could be parsed.
    pub fn parse_hosts(&self) -> Result<Vec<SrNodeConfig>> {
        let mut nodes = Vec::new();
        for segment in self.node_urls.split(';') {
            let seg = segment.trim();
            if seg.is_empty() {
                continue;
            }
            match parse_node_segment(seg) {
                Ok(node) => nodes.push(node),
                Err(e) => {
                    tracing::warn!("Failed to parse SR node URL '{seg}': {e} — skipping");
                }
            }
        }
        if nodes.is_empty() {
            anyhow::bail!(
                "No valid SR node URLs found in '{}' — at least one node is required. \
                 Format: host:http_port or host:http_port:jdbc_port, semicolon-separated.",
                self.node_urls
            );
        }
        Ok(nodes)
    }

    /// Build the StarRocks table properties map.
    /// Only `replication_num = "1"` is always set as a built-in default.
    /// All other properties (fast_schema_evolution, persistent_index, etc.)
    /// come from the user's `table_properties` config section.
    pub fn build_table_properties(&self) -> HashMap<String, String> {
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert("replication_num".to_string(), "1".to_string());
        // NOTE: partial-update is NOT a table property on SR 4.x (rejected
        // with "Unknown properties: {partial_update=true}"). Instead it is
        // requested per Stream Load via the `partial_columns: true` HTTP header
        // (see sink.rs try_stream_load / send_to_be) so an UPDATE whose TOAST
        // column is unchanged (omitted from the JSON) keeps the existing value
        // instead of being overwritten with NULL/default.
        // User-specified properties override built-in defaults
        for (k, v) in &self.table_properties {
            props.insert(k.clone(), v.clone());
        }
        props
    }
}

impl PgConfig {
    /// Build a PostgreSQL connection string from config.
    /// Used by all PG direct connections (snapshot, DDL setup, catalog queries).
    /// Values are escaped per libpq connection-string rules: backslash → \\,
    /// single-quote → \', and values containing spaces are single-quoted.
    pub fn conn_str(&self) -> String {
        let esc = |s: &str| -> String {
            let e = s.replace('\\', "\\\\").replace('\'', "\\'");
            // Libpq key-value pairs: values with spaces, '=', or empty must
            // be single-quoted. Without this, a password containing '=' would
            // be parsed as multiple key-value pairs (e.g. 'pass=word' → 'pass'
            // as the password and 'word' as an unknown parameter).
            if e.contains(' ') || e.contains('=') || e.is_empty() {
                format!("'{}'", e)
            } else {
                e
            }
        };
        format!(
            "host={} port={} dbname={} user={} password={} application_name={}",
            esc(&self.host),
            self.port,
            esc(&self.database),
            esc(&self.username),
            esc(&self.password),
            esc(&self.app_name),
        )
    }

    /// Connection timeout for PG replication connections.
    pub fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.connect_timeout_ms)
    }
}

/// Parse a "schema.table" string into its components.
/// Returns an error if malformed (no dot separator).
pub fn parse_ddl_capture_table(s: &str) -> Result<(&str, &str)> {
    let dot = s.find('.').ok_or_else(|| {
        anyhow::anyhow!("ddl_capture_table must be in 'schema.table' format, got: {s}")
    })?;
    Ok((&s[..dot], &s[dot + 1..]))
}

/// Convert a simple glob pattern (supports * and ?) to a regex string.
/// `*` matches any sequence, `?` matches any single character.
/// All regex metacharacters are escaped.
fn glob_to_regex(pattern: &str) -> String {
    let mut re = String::with_capacity(pattern.len() + 4);
    re.push('^');
    for ch in pattern.chars() {
        match ch {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            '.' => re.push_str("\\."),
            '+' => re.push_str("\\+"),
            '^' => re.push_str("\\^"),
            '$' => re.push_str("\\$"),
            '{' => re.push_str("\\{"),
            '}' => re.push_str("\\}"),
            '(' => re.push_str("\\("),
            ')' => re.push_str("\\)"),
            '|' => re.push_str("\\|"),
            '[' => re.push_str("\\["),
            ']' => re.push_str("\\]"),
            '\\' => re.push_str("\\\\"),
            c => re.push(c),
        }
    }
    re.push('$');
    re
}

/// Compile a table_filter glob pattern into a regex.
/// Fail-fast: an invalid pattern is a configuration error, not a fallback.
///
/// `glob_to_regex` escapes every regex metacharacter (including `+`,
/// `^`, `$`, `.`, brackets, parens, etc.), so for any well-formed glob the
/// `Regex::new` below always succeeds and the fail-fast is effectively
/// defense-in-depth (it cannot be reached via a normal glob). It is retained
/// so a future glob-translation bug (an unescaped metachar) surfaces at
/// startup instead of silently matching nothing. The comprehensive-escaping
/// test below pins this contract.
pub fn compile_table_filter(filter: &str) -> Result<Regex> {
    let glob_re = glob_to_regex(filter);
    regex::Regex::new(&glob_re).map_err(|e| {
        anyhow::anyhow!(
            "Invalid table_filter pattern '{filter}': {e} — the pipeline will not start until this is fixed"
        )
    })
}

/// Compile a table_exclude regex pattern.
/// Returns Ok(None) for empty/no-op.
/// Returns an error if the pattern is invalid (fail-fast).
/// Uses raw regex (not glob) since exclusion patterns often need alternation,
/// anchors, and other regex features that glob cannot express.
pub fn compile_table_exclude(filter: &str) -> Result<Option<Regex>> {
    if filter.is_empty() {
        return Ok(None);
    }
    regex::Regex::new(filter)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("Invalid table_exclude pattern '{filter}': {e}"))
}

/// Check whether a bare table name matches the exclude pattern.
/// Returns false when exclude_re is None (no exclusion configured).
pub fn is_table_excluded(exclude_re: &Option<Regex>, _schema: &str, table: &str) -> bool {
    match exclude_re {
        Some(re) => re.is_match(table),
        None => false,
    }
}

/// Test whether a bare table name matches the compiled table_filter regex.
/// O(log n) — does NOT re-compile the regex on each call.
pub fn table_matches_filter(re: &Regex, _schema: &str, table: &str) -> bool {
    re.is_match(table)
}

/// Map the user-facing TLS mode string to pgwire_replication's SslMode.
/// Rejects unknown modes instead of silently falling back to Disable.
pub fn pg_tls_ssl_mode(mode: &str) -> Result<pgwire_replication::config::SslMode> {
    use pgwire_replication::config::SslMode;
    match mode {
        "disable" => Ok(SslMode::Disable),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        "verify-ca" => Ok(SslMode::VerifyCa),
        "verify-full" => Ok(SslMode::VerifyFull),
        other => Err(anyhow::anyhow!(
            "Unknown TLS mode '{}'. Valid modes: disable, prefer, require, verify-ca, verify-full",
            other
        )),
    }
}

/// Build a pgwire_replication TlsConfig from our PgTls config.
pub fn pg_replication_tls_config(tls: &PgTls) -> Result<pgwire_replication::config::TlsConfig> {
    let mode = pg_tls_ssl_mode(&tls.mode)?;
    Ok(pgwire_replication::config::TlsConfig {
        mode,
        ca_pem_path: tls.ca_cert.clone(),
        ..Default::default()
    })
}

/// Build a rustls ClientConfig suitable for tokio-postgres-rustls.
/// Returns Ok(None) if TLS is disabled, Ok(Some(config)) otherwise.
pub fn pg_direct_tls_config(
    tls: &PgTls,
) -> Result<Option<tokio_postgres_rustls::MakeRustlsConnect>> {
    let mode = pg_tls_ssl_mode(&tls.mode)?;
    if mode == pgwire_replication::config::SslMode::Disable {
        return Ok(None);
    }

    let mut root_store = rustls::RootCertStore::empty();

    // Load CA cert if provided, otherwise use system store
    if let Some(ref ca_path) = tls.ca_cert {
        let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(ca_path)
                .with_context(|| format!("Failed to open CA cert file: {:?}", ca_path))?,
        ))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Failed to parse CA cert file: {:?}", ca_path))?;
        for cert in certs {
            root_store
                .add(cert)
                .with_context(|| format!("Failed to add CA cert from {:?}", ca_path))?;
        }
    } else {
        let result = rustls_native_certs::load_native_certs();
        // Warn on any errors (benign CA store quirks), but don't fail
        // if valid certs were also loaded.
        if !result.errors.is_empty() {
            for err in &result.errors {
                tracing::warn!("System CA cert warning: {}", err);
            }
        }
        if result.certs.is_empty() {
            return Err(anyhow::anyhow!(
                "Failed to load system CA certs: no valid certificates found ({} warning(s))",
                result.errors.len(),
            ));
        }
        for cert in result.certs {
            root_store
                .add(cert)
                .context("Failed to add system CA cert")?;
        }
    }

    // Ensure a CryptoProvider is installed before calling builder().
    // In production this is set at main.rs startup, but this function can
    // also be called from tests or other contexts. Idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Some(tokio_postgres_rustls::MakeRustlsConnect::new(config)))
}

/// TLS connector for tokio-postgres — either NoTls or rustls.
pub enum PgTlsConnector {
    None(tokio_postgres::NoTls),
    Rustls(tokio_postgres_rustls::MakeRustlsConnect),
}

impl PgTlsConnector {
    pub fn from_config(tls: &PgTls) -> Result<Self> {
        match pg_direct_tls_config(tls)? {
            Some(c) => Ok(PgTlsConnector::Rustls(c)),
            None => Ok(PgTlsConnector::None(tokio_postgres::NoTls)),
        }
    }

    /// Connect to PostgreSQL using the configured TLS mode.
    /// Returns the client and a spawned task handle for the connection.
    pub async fn connect(
        &self,
        conn_str: &str,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
        match self {
            PgTlsConnector::None(_) => {
                let (client, conn) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
                    .await
                    .context("Failed to connect to PostgreSQL")?;
                let handle = tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!("PG connection error: {:?}", e);
                    }
                });
                Ok((client, handle))
            }
            PgTlsConnector::Rustls(tls) => {
                let (client, conn) = tokio_postgres::connect(conn_str, tls.clone())
                    .await
                    .context("Failed to connect to PostgreSQL with TLS")?;
                let handle = tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!("PG TLS connection error: {:?}", e);
                    }
                });
                Ok((client, handle))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only helper: parse + validate from a TOML string (mirrors `from_file`
    /// but without touching the filesystem).
    fn parse_config_from_str(toml_str: &str) -> Result<Config> {
        let config: Config = toml::from_str(toml_str).context("Failed to parse TOML string")?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn test_compile_table_filter_valid() {
        let re = compile_table_filter("*").unwrap();
        assert!(table_matches_filter(&re, "public", "anytable"));
        assert!(table_matches_filter(&re, "public", ""));
    }

    #[test]
    fn test_compile_table_filter_invalid_is_fatal() {
        // With glob_to_regex, `[` is a valid glob (matches literal `[`).
        // Use a pattern that actually produces an invalid regex.
        let result = compile_table_filter("[");
        assert!(
            result.is_ok(),
            "glob `[` should be valid (escaped to `\\[`)"
        );
        let re = result.unwrap();
        assert!(table_matches_filter(&re, "s", "["));
        assert!(!table_matches_filter(&re, "s", "a"));
    }

    #[test]
    fn test_compile_table_filter_specific() {
        let re = compile_table_filter("cc_*").unwrap();
        assert!(table_matches_filter(&re, "public", "cc_users"));
        assert!(table_matches_filter(&re, "other", "cc_orders"));
        assert!(!table_matches_filter(&re, "public", "users"));
        assert!(!table_matches_filter(&re, "public", "cc"));
    }

    #[test]
    fn test_compile_table_filter_regex_metachars_are_escaped() {
        // glob_to_regex must escape EVERY regex metachar that is not a glob
        // wildcard, so a glob containing a literal metachar compiles to a valid
        // regex matching that literal (not a false "invalid pattern" failure,
        // and not a regex operator). This pins the fail-fast contract as
        // defense-in-depth: for any well-formed glob, Regex::new succeeds.
        for ch in ['.', '+', '^', '$', '{', '}', '(', ')', '|', '[', ']', '\\'] {
            let glob = format!("pre{ch}post");
            let re = compile_table_filter(&glob)
                .unwrap_or_else(|e| panic!("glob {glob:?} (metachar {ch:?}) must compile: {e}"));
            assert!(
                table_matches_filter(&re, "s", &glob),
                "glob {glob:?} must match itself literally (metachar {ch:?} escaped)"
            );
            assert!(!table_matches_filter(&re, "s", "preXpost"));
        }
        // A lone regex quantifier-as-glob must also compile (it's a literal).
        assert!(compile_table_filter("+").is_ok());
        assert!(compile_table_filter("*").is_ok());
        assert!(compile_table_filter("?").is_ok());
    }

    #[test]
    fn test_table_matches_filter_edge_cases() {
        let re = compile_table_filter("*").unwrap();
        assert!(table_matches_filter(&re, "public", "anytable"));
        assert!(table_matches_filter(&re, "public", ""));
        let re2 = compile_table_filter("cc_*").unwrap();
        assert!(!table_matches_filter(&re2, "public", "users"));
    }

    #[test]
    fn test_tls_config_defaults_to_disabled() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.postgres.tls.mode, "disable");
        assert!(config.postgres.tls.ca_cert.is_none());
        assert!(!config.starrocks.http_tls.enable);
        assert!(config.starrocks.http_tls.ca_cert.is_none());
        assert!(!config.starrocks.mysql_tls.enable);
        assert!(config.starrocks.mysql_tls.ca_cert.is_none());
    }

    #[test]
    fn test_tls_config_pg_require_mode() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [postgres.tls]
            mode = "require"
            ca_cert = "/etc/ssl/ca.pem"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.postgres.tls.mode, "require");
        assert_eq!(
            config.postgres.tls.ca_cert,
            Some(std::path::PathBuf::from("/etc/ssl/ca.pem"))
        );
    }

    #[test]
    fn test_tls_config_sr_enabled() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""

            [starrocks.http_tls]
            enable = true
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.starrocks.http_tls.enable);
        assert!(config.starrocks.http_tls.ca_cert.is_none()); // uses system CA
        assert!(!config.starrocks.mysql_tls.enable); // separate, still disabled
    }

    #[test]
    fn test_pg_tls_ssl_mode_mapping() {
        assert_eq!(
            pg_tls_ssl_mode("disable").unwrap(),
            pgwire_replication::config::SslMode::Disable
        );
        assert_eq!(
            pg_tls_ssl_mode("prefer").unwrap(),
            pgwire_replication::config::SslMode::Prefer
        );
        assert_eq!(
            pg_tls_ssl_mode("require").unwrap(),
            pgwire_replication::config::SslMode::Require
        );
        assert_eq!(
            pg_tls_ssl_mode("verify-ca").unwrap(),
            pgwire_replication::config::SslMode::VerifyCa
        );
        assert_eq!(
            pg_tls_ssl_mode("verify-full").unwrap(),
            pgwire_replication::config::SslMode::VerifyFull
        );
        // Unknown/invalid → error
        assert!(pg_tls_ssl_mode("garbage").is_err());
    }

    #[test]
    fn test_pg_direct_tls_config_disabled() {
        let tls = PgTls::default();
        let conn = PgTlsConnector::from_config(&tls).unwrap();
        assert!(matches!(conn, PgTlsConnector::None(_)));
    }

    #[test]
    fn test_pg_direct_tls_config_require() {
        // rustls needs a crypto provider — normally installed by reqwest at startup.
        // In unit tests we must install it manually.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let tls = PgTls {
            mode: "require".into(),
            ca_cert: None,
        };
        let conn = PgTlsConnector::from_config(&tls).unwrap();
        assert!(
            matches!(conn, PgTlsConnector::Rustls(_)),
            "require mode must produce Rustls connector"
        );
    }

    #[test]
    fn test_pg_replication_tls_config_disabled() {
        let tls = PgTls::default();
        let cfg = pg_replication_tls_config(&tls).unwrap();
        assert_eq!(cfg.mode, pgwire_replication::config::SslMode::Disable);
        assert!(cfg.ca_pem_path.is_none());
    }

    #[test]
    fn test_pg_replication_tls_config_verify_full() {
        let tls = PgTls {
            mode: "verify-full".into(),
            ca_cert: Some(std::path::PathBuf::from("/tmp/ca.pem")),
        };
        let cfg = pg_replication_tls_config(&tls).unwrap();
        assert_eq!(cfg.mode, pgwire_replication::config::SslMode::VerifyFull);
        assert_eq!(
            cfg.ca_pem_path,
            Some(std::path::PathBuf::from("/tmp/ca.pem"))
        );
    }

    #[test]
    fn test_rustls_connector_with_system_certs() {
        // Verify system CA certs can be loaded (proves real TLS would work).
        // This fails fast at startup if the system has no CA certs installed.
        let tls = PgTls {
            mode: "require".into(),
            ca_cert: None, // use system CA store
        };
        let conn = PgTlsConnector::from_config(&tls).unwrap();
        assert!(matches!(conn, PgTlsConnector::Rustls(_)));
        // Construction succeeded — system certs loaded, ready for TLS handshake
    }

    #[test]
    fn test_parse_ddl_capture_table_valid() {
        let (schema, table) = parse_ddl_capture_table("public.my_table").unwrap();
        assert_eq!(schema, "public");
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_parse_ddl_capture_table_multiple_dots() {
        // First dot is the separator — schema is everything before it
        let (schema, table) = parse_ddl_capture_table("my.schema.my_table").unwrap();
        assert_eq!(schema, "my");
        assert_eq!(table, "schema.my_table");
    }

    #[test]
    fn test_parse_ddl_capture_table_no_dot() {
        let result = parse_ddl_capture_table("no_dot");
        assert!(result.is_err(), "Should error on missing dot");
        assert!(
            format!("{}", result.unwrap_err()).contains("schema.table"),
            "Error must mention 'schema.table' format"
        );
    }

    #[test]
    fn test_parse_ddl_capture_table_default() {
        let (schema, table) = parse_ddl_capture_table("public.pg2sr_cdc_ddl_events").unwrap();
        assert_eq!(schema, "public");
        assert_eq!(table, "pg2sr_cdc_ddl_events");
    }

    // ---------------------------------------------------------------------------
    // Config parsing negative tests
    // ---------------------------------------------------------------------------

    /// Return a minimal valid TOML config string for use in negative tests.
    /// Callers can replace any field by appending overrides TOML under the
    /// appropriate section.
    fn base_config_toml() -> String {
        r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#
        .to_string()
    }

    /// Return a valid config TOML with a [starrocks] section override injected
    /// (replaces the stock `[starrocks]` block from base_config_toml).
    fn config_toml_with_starrocks(sr_overrides: &str) -> String {
        let base = base_config_toml();
        // If sr_overrides is empty, just append [pipeline] to the base.
        if sr_overrides.is_empty() {
            return format!("{base}\n[pipeline]\nname = \"test\"\n");
        }
        // Otherwise, append the overrides to the [starrocks] section.
        // TOML allows adding keys to a section as long as there's no
        // second [section] header between them.
        format!("{base}\n{sr_overrides}\n[pipeline]\nname = \"test\"\n")
    }

    /// Return a valid config TOML with a [pipeline] section override appended.
    fn config_toml_with_pipeline(pipeline_toml: &str) -> String {
        format!(
            r#"
            {base}

            [pipeline]
            {pipeline}
        "#,
            base = base_config_toml(),
            pipeline = pipeline_toml,
        )
    }

    #[test]
    fn test_invalid_toml_syntax() {
        let result = toml::from_str::<Config>("this is not valid toml === {{{");
        assert!(result.is_err(), "Invalid TOML should fail to parse");
    }

    #[test]
    fn test_missing_postgres_section() {
        let toml = r#"
            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let result = toml::from_str::<Config>(toml);
        assert!(
            result.is_err(),
            "Expected error when [postgres] section is missing"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("postgres"),
            "Error should mention missing 'postgres' field, got: {err}"
        );
    }

    #[test]
    fn test_missing_starrocks_section() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"
        "#;
        let result = toml::from_str::<Config>(toml);
        assert!(
            result.is_err(),
            "Expected error when [starrocks] section is missing"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("starrocks"),
            "Error should mention missing 'starrocks' field, got: {err}"
        );
    }

    #[test]
    fn test_empty_host_parses_ok() {
        // host is a plain String — serde does not validate emptiness.
        // Connection-time validation must catch empty hosts.
        let toml = base_config_toml().replace(r#""localhost""#, r#""""#);
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(config.postgres.host.is_empty());
    }

    #[test]
    fn test_port_zero_parses_ok() {
        // u16 accepts 0 (valid range 0–65535).
        // Port 0 is technically invalid for connections; validation happens later.
        let toml = base_config_toml().replace("port = 5432", "port = 0");
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.postgres.port, 0);
    }

    #[test]
    fn test_port_out_of_range() {
        // u16 maximum is 65535, so 99999 must be rejected.
        let toml = base_config_toml().replace("port = 5432", "port = 99999");
        let result = toml::from_str::<Config>(&toml);
        assert!(result.is_err(), "Port 99999 exceeds u16 range");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("99999") && err.contains("u16"),
            "Error should mention the value and u16, got: {err}"
        );
    }

    #[test]
    fn test_port_negative() {
        // u16 does not accept negative values.
        let toml = base_config_toml().replace("port = 5432", "port = -1");
        let result = toml::from_str::<Config>(&toml);
        assert!(result.is_err(), "Negative port should fail");
    }

    #[test]
    fn test_port_string_instead_of_integer() {
        // Type mismatch: port expects an integer, not a string.
        let toml = base_config_toml().replace("port = 5432", r#"port = "abc""#);
        let result = toml::from_str::<Config>(&toml);
        assert!(result.is_err(), "String port should fail type validation");
    }

    #[test]
    fn test_empty_database_parses_ok() {
        // database is a plain String — serde does not validate emptiness.
        // Connection-time validation must catch empty database names.
        let toml = base_config_toml().replace(r#""db""#, r#""""#);
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(config.postgres.database.is_empty());
    }

    #[test]
    fn test_batch_timeout_ms_zero_parses_ok() {
        // u64 accepts 0. Runtime validation should enforce a positive value if needed.
        let toml = config_toml_with_pipeline("batch_timeout_ms = 0");
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.pipeline.batch_timeout_ms, 0);
    }

    #[test]
    fn test_max_retries_negative() {
        // u32 does not accept negative values.
        let toml = config_toml_with_pipeline("max_retries = -1");
        let result = toml::from_str::<Config>(&toml);
        assert!(result.is_err(), "Negative max_retries should fail");
    }

    #[test]
    fn test_max_retries_huge() {
        // u32 max is 4_294_967_295; a value well beyond that in the TOML should fail.
        let toml = config_toml_with_pipeline("max_retries = 9999999999999");
        let result = toml::from_str::<Config>(&toml);
        assert!(result.is_err(), "Huge max_retries should fail");
    }

    #[test]
    fn test_trailing_garbage() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        garbage at the end
        "#;
        let result = toml::from_str::<Config>(toml);
        assert!(result.is_err(), "Trailing garbage should fail to parse");
    }

    #[test]
    fn test_unicode_hostname_parses_ok() {
        // Unicode strings are valid in TOML strings — serde stores them as-is.
        let toml = base_config_toml().replace(r#""localhost""#, r#""本地主机""#);
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.postgres.host, "本地主机");
    }

    #[test]
    fn test_empty_table_filter_parses_ok() {
        // table_filter has a Default but can be explicitly overridden to "".
        // Callers should validate at connection time.
        let toml = base_config_toml().replace(
            "password = \"p\"",
            "password = \"p\"\n            table_filter = \"\"",
        );
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(config.postgres.table_filter.is_empty());
    }

    #[test]
    fn test_node_urls_missing() {
        // node_urls in [starrocks] is required; omitting it should error.
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            database = "db"
            username = "root"
            password = ""
        "#;
        let result = toml::from_str::<Config>(toml);
        assert!(
            result.is_err(),
            "Missing required field 'node_urls' should fail"
        );
    }

    #[test]
    fn test_pg_config_defaults() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.postgres.schema, "public");
        assert_eq!(config.postgres.table_filter, "*");
        assert_eq!(config.postgres.table_exclude, "");
    }

    #[test]
    fn test_roundtrip_representative_config() {
        // A representative config round-trips through the parser with every
        // field reflecting the file contents (defaults filled in elsewhere).
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "testdb"
            username = "replicator"
            password = "pass"

            [starrocks]
            node_urls = "localhost:8030:9030"
            database = "testdb"
            username = "root"
            password = ""

            [pipeline]
            ddl_poll_interval_ms = 1000
            max_retries = 2
            batch_rows = 1000
        "#;
        let config: Config = toml::from_str(toml).expect("representative config must parse");
        assert_eq!(config.postgres.host, "localhost");
        assert_eq!(config.postgres.port, 5432);
        assert_eq!(config.postgres.database, "testdb");
        assert_eq!(config.starrocks.node_urls, "localhost:8030:9030");
        assert_eq!(config.pipeline.ddl_poll_interval_ms, 1000);
        assert_eq!(config.pipeline.max_retries, 2);
        assert_eq!(config.pipeline.batch_rows, 1000);
    }

    #[test]
    fn test_validate_rejects_zero_snapshot_page_size() {
        let toml = config_toml_with_pipeline("snapshot_page_size = 0");
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("snapshot_page_size"),
            "Expected error about snapshot_page_size, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_batch_rows() {
        let toml = config_toml_with_pipeline("batch_rows = 0");
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("batch_rows"),
            "Expected error about batch_rows, got: {err}"
        );
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        let toml = config_toml_with_pipeline("snapshot_page_size = 10000\nbatch_rows = 1000");
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(
            config.validate().is_ok(),
            "Valid config should pass validation"
        );
    }

    #[test]
    fn test_validate_rejects_empty_pg_host() {
        let toml = base_config_toml().replace(r#""localhost""#, r#""""#);
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("host"),
            "Expected error about postgres.host, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_pg_database() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = ""
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("postgres.database"),
            "Expected error about postgres.database, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_pg_connect_timeout() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"
            connect_timeout_ms = 0

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("connect_timeout_ms"),
            "Expected error about connect_timeout_ms, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_sr_database() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = ""
            username = "root"
            password = ""
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("starrocks.database"),
            "Expected error about starrocks.database, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_sr_username() {
        let toml = base_config_toml().replace(r#""root""#, r#""""#);
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("username"),
            "Expected error about starrocks.username, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_sr_connection_timeout() {
        let toml = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
            connection_timeout_ms = 0
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("connection_timeout_ms"),
            "Expected error about connection_timeout_ms, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_node_urls() {
        let toml = base_config_toml().replace(r#""localhost:8030""#, r#""""#);
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("node_urls"),
            "Expected error about node_urls, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_malformed_node_urls() {
        let toml = base_config_toml().replace(r#""localhost:8030""#, r#""not:a:valid:url""#);
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("node"),
            "Expected error about malformed node URL, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_zero_max_concurrent_flushes() {
        let toml = config_toml_with_pipeline("max_concurrent_flushes = 0");
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("max_concurrent_flushes"),
            "Expected error about max_concurrent_flushes, got: {err}"
        );
    }

    #[test]
    fn test_validate_rejects_mysql_pool_max_below_4() {
        let toml = config_toml_with_starrocks("mysql_max_pool_connections = 2");
        let config: Config = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("mysql_max_pool_connections"),
            "Expected error about mysql_max_pool_connections, got: {err}"
        );
    }

    #[test]
    fn test_validate_accepts_mysql_pool_max_default() {
        let toml = config_toml_with_starrocks("");
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(
            config.validate().is_ok(),
            "Default mysql_max_pool_connections should pass validation"
        );
    }

    #[test]
    fn test_metrics_config_default_disabled() {
        let toml = config_toml_with_starrocks("");
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(
            !config.metrics.enabled,
            "Metrics should be disabled by default"
        );
        assert_eq!(config.metrics.port, 9091, "Default port should be 9091");
    }

    #[test]
    fn test_metrics_config_accepts_enabled() {
        let toml = format!(
            r#"
            {base}
            [metrics]
            enabled = true
            port = 9099
            "#,
            base = base_config_toml(),
        );
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(config.metrics.enabled, "Metrics should be enabled");
        assert_eq!(config.metrics.port, 9099, "Port should be 9099");
    }

    #[test]
    fn test_conn_str_escapes_special_chars() {
        let pg = PgConfig {
            host: "host".into(),
            port: 5432,
            database: "db with spaces".into(),
            username: "u'ser".into(),
            password: "p\\ass".into(),
            schema: "public".to_string(),
            app_name: "myapp".into(),
            table_filter: String::new(),
            table_exclude: String::new(),
            slot_name: "slot".into(),
            publication: "pub".into(),
            manage_publication: false,
            tls: PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let cs = pg.conn_str();
        assert!(cs.contains("\\'"), "conn_str: {cs}");
        assert!(cs.contains("\\\\"), "conn_str: {cs}");
        assert!(cs.contains("'"), "conn_str: {cs}");
    }

    // ── conn_str password with `=` ──

    #[test]
    fn test_conn_str_escapes_password_with_equals() {
        let pg = PgConfig {
            host: "h".into(),
            port: 5432,
            database: "d".into(),
            username: "u".into(),
            password: "pass=word".into(),
            schema: "public".to_string(),
            app_name: "a".into(),
            table_filter: String::new(),
            table_exclude: String::new(),
            slot_name: "s".into(),
            publication: "p".into(),
            manage_publication: false,
            tls: PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let cs = pg.conn_str();
        // Password with `=` must be single-quoted so libpq doesn't parse it as k=v
        assert!(
            cs.contains("password='pass=word'"),
            "conn_str must quote passwords with '=': {cs}"
        );
    }

    // ── Debug redacts passwords ──

    #[test]
    fn test_pg_config_debug_redacts_password() {
        let pg = PgConfig {
            host: "h".into(),
            port: 5432,
            database: "d".into(),
            username: "u".into(),
            password: "supersecret".into(),
            schema: "public".to_string(),
            app_name: "a".into(),
            table_filter: String::new(),
            table_exclude: String::new(),
            slot_name: "s".into(),
            publication: "p".into(),
            manage_publication: false,
            tls: PgTls::default(),
            connect_timeout_ms: 30000,
        };
        let debug = format!("{:?}", pg);
        assert!(
            debug.contains("[redacted]"),
            "Debug output must redact password: {debug}"
        );
        assert!(
            !debug.contains("supersecret"),
            "Debug output must NOT contain actual password: {debug}"
        );
    }

    #[test]
    fn test_sr_config_debug_redacts_password() {
        let sr = SrConfig {
            node_urls: "host:8030".into(),
            database: "db".into(),
            username: "admin".into(),
            password: "hunter2".into(),
            connection_timeout_ms: 30000,
            http_pool_idle_timeout_ms: 60000,
            mysql_max_pool_connections: 32,
            table_properties: HashMap::new(),
            http_tls: SrTlsConfig::default(),
            mysql_tls: SrTlsConfig::default(),
        };
        let debug = format!("{:?}", sr);
        assert!(
            debug.contains("[redacted]"),
            "SrConfig Debug must redact password: {debug}"
        );
        assert!(
            !debug.contains("hunter2"),
            "SrConfig Debug must NOT contain actual password: {debug}"
        );
    }

    // ── validate rejects invalid table_exclude ──

    #[test]
    fn test_validate_rejects_invalid_table_exclude() {
        let config = Config {
            postgres: PgConfig {
                host: "localhost".into(),
                port: 5432,
                database: "testdb".into(),
                username: "user".into(),
                password: "pass".into(),
                schema: "public".to_string(),
                app_name: "test".into(),
                table_filter: "*".to_string(),
                table_exclude: "[invalid".to_string(), // unclosed bracket
                slot_name: "slot".into(),
                publication: "pub".into(),
                manage_publication: false,
                tls: PgTls::default(),
                connect_timeout_ms: 10000,
            },
            starrocks: SrConfig {
                node_urls: "host:8030".into(),
                database: "sr_db".into(),
                username: "root".into(),
                password: "".into(),
                connection_timeout_ms: 10000,
                http_pool_idle_timeout_ms: 60000,
                mysql_max_pool_connections: 32,
                table_properties: HashMap::new(),
                http_tls: SrTlsConfig::default(),
                mysql_tls: SrTlsConfig::default(),
            },
            pipeline: PipelineConfig::default(),
            checkpoint: None,
            metrics: Default::default(),
        };
        let err = config.validate().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("table_exclude"),
            "error must mention table_exclude: {msg}"
        );
    }

    // ── validate rejects invalid TLS mode ──

    #[test]
    fn test_validate_rejects_invalid_tls_mode() {
        let config = Config {
            postgres: PgConfig {
                host: "localhost".into(),
                port: 5432,
                database: "testdb".into(),
                username: "user".into(),
                password: "pass".into(),
                schema: "public".to_string(),
                app_name: "test".into(),
                table_filter: "*".to_string(),
                table_exclude: String::new(),
                slot_name: "slot".into(),
                publication: "pub".into(),
                manage_publication: false,
                tls: PgTls {
                    mode: "garbage_mode".into(),
                    ca_cert: None,
                },
                connect_timeout_ms: 10000,
            },
            starrocks: SrConfig {
                node_urls: "host:8030".into(),
                database: "sr_db".into(),
                username: "root".into(),
                password: "".into(),
                connection_timeout_ms: 10000,
                http_pool_idle_timeout_ms: 60000,
                mysql_max_pool_connections: 32,
                table_properties: HashMap::new(),
                http_tls: SrTlsConfig::default(),
                mysql_tls: SrTlsConfig::default(),
            },
            pipeline: PipelineConfig::default(),
            checkpoint: None,
            metrics: Default::default(),
        };
        let err = config.validate().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("TLS") || msg.contains("garbage_mode"),
            "error must mention TLS mode: {msg}"
        );
    }

    // ── max_retries validation (no cross-field budget math) ──

    #[test]
    fn test_validate_accepts_any_max_retries() {
        // The old budget validator that cross-checked max_retries × connect_timeout ×
        // node_count against a hardcoded 90s flush timeout has been removed.
        // Operators are responsible for sizing — the simpler model has no
        // cross-field arithmetic. max_retries=0, 1, 2, 100 should all pass.
        for n in [0u32, 1, 2, 5, 100] {
            let toml = config_toml_with_pipeline(&format!("max_retries = {n}"));
            let config: Config = toml::from_str(&toml).unwrap();
            assert!(
                config.validate().is_ok(),
                "max_retries={n} should pass validation under the new simple model"
            );
        }
    }

    #[test]
    fn test_validate_accepts_low_max_retries_with_default_timeout() {
        let toml = config_toml_with_pipeline("max_retries = 1");
        let config: Config = toml::from_str(&toml).unwrap();
        assert!(
            config.validate().is_ok(),
            "max_retries=1 with default timeout should pass: {}",
            config.validate().unwrap_err(),
        );
    }

    #[test]
    fn test_http_pool_idle_timeout_default_works() {
        // Verify that the new http_pool_idle_timeout_ms field has a default value
        // when not specified in TOML.
        let toml = base_config_toml();
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.starrocks.http_pool_idle_timeout_ms, 30000);
    }

    #[test]
    fn test_http_pool_idle_timeout_custom_value() {
        let custom = r#"
            [postgres]
            host = "localhost"
            port = 5432
            database = "db"
            username = "u"
            password = "p"

            [starrocks]
            node_urls = "localhost:8030"
            database = "db"
            username = "root"
            password = ""
            http_pool_idle_timeout_ms = 120000
        "#;
        let config: Config = toml::from_str(custom).unwrap();
        assert_eq!(config.starrocks.http_pool_idle_timeout_ms, 120000);
    }

    #[test]
    fn test_max_tx_events_default() {
        let toml = base_config_toml();
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.pipeline.max_tx_events, 100_000);
    }

    #[test]
    fn test_max_tx_events_custom_value() {
        let toml = config_toml_with_pipeline("max_tx_events = 50000");
        let config: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config.pipeline.max_tx_events, 50_000);
    }

    #[test]
    fn test_stream_idle_timeout_secs_rejected() {
        // Field removed entirely; deny_unknown_fields must reject any TOML still setting it.
        let toml = config_toml_with_pipeline("stream_idle_timeout_secs = 60");
        let err = toml::from_str::<Config>(&toml).unwrap_err();
        assert!(
            err.to_string().contains("stream_idle_timeout_secs"),
            "expected unknown-field error mentioning stream_idle_timeout_secs, got: {err}"
        );
    }

    #[test]
    fn test_validate_accepts_zero_max_retries_for_ha() {
        // max_retries=0 is a legitimate choice for HA SR clusters —
        // immediate node failover with no same-node retry. Stream Load
        // labels are idempotent across node failover (sink.rs:128-133),
        // so a duplicate-label response on the next node is treated as
        // success. Validator emits a warning but does not bail.
        let config = Config {
            postgres: PgConfig {
                host: "localhost".into(),
                port: 5432,
                database: "testdb".into(),
                username: "user".into(),
                password: "pass".into(),
                schema: "public".to_string(),
                app_name: "test".into(),
                table_filter: "*".to_string(),
                table_exclude: String::new(),
                slot_name: "slot".into(),
                publication: "pub".into(),
                manage_publication: false,
                tls: PgTls::default(),
                connect_timeout_ms: 10000,
            },
            starrocks: SrConfig {
                node_urls: "n1:8030;n2:8030;n3:8030".into(),
                database: "sr_db".into(),
                username: "root".into(),
                password: "".into(),
                connection_timeout_ms: 10000,
                http_pool_idle_timeout_ms: 30000,
                mysql_max_pool_connections: 32,
                table_properties: HashMap::new(),
                http_tls: SrTlsConfig::default(),
                mysql_tls: SrTlsConfig::default(),
            },
            pipeline: PipelineConfig {
                max_retries: 0,
                ..PipelineConfig::default()
            },
            checkpoint: None,
            metrics: Default::default(),
        };
        // Should pass validation (warning emitted, not an error).
        config
            .validate()
            .expect("max_retries=0 with 3 FE should pass validation for HA");
    }

    #[test]
    fn test_config_sample_parses_and_validates() {
        // Drift protection: ensures config-sample.toml (the production template)
        // stays in sync with the Config struct. If a field is renamed, added, or
        // removed without updating the sample, this test fails. Catches both
        // TOML parse errors (deny_unknown_fields) and validate() failures
        // (e.g. flush timeout budget arithmetic).
        //
        // Note: this test does NOT connect to PG or StarRocks — it only checks
        // that the sample parses and passes config-time validation. Credential
        // placeholders like "CHANGE_ME" are fine because validation does not
        // attempt to authenticate.
        let toml_str = include_str!("../config-sample.toml");
        let config =
            parse_config_from_str(toml_str).expect("config-sample.toml should parse and validate");
        // Spot-check a few production-tuned values to catch wholesale reverts.
        assert_eq!(config.pipeline.name, "pg2sr_cdc_prod");
        assert_eq!(config.pipeline.max_retries, 0);
        assert!(!config.postgres.manage_publication);
        assert!(config.metrics.enabled);
    }
}
