//! Distributed single-instance guard via PostgreSQL advisory locks.
//!
//! The flock in `main.rs` is host-local; this module extends the same
//! exclusion across hosts. PG advisory locks are session-scoped and
//! cluster-visible: any two processes reaching the same PG cluster
//! (implied by sharing an SR database or the checkpoint store) serialize
//! on the same key. A crash or connection drop releases the lock
//! automatically — no stale-lock cleanup, no expiry, no clock skew.
//!
//! Keying: `pg_advisory_lock(int4, int4)` with a fixed magic `classid`
//! and an `objid` = SHA-256 of the shared artifact's identity (SR
//! database, checkpoint store), so unrelated advisory-lock users cannot
//! collide accidentally. PG scopes advisory locks per database
//! (empirically verified), which isolates us from other applications on
//! the same cluster.
//!
//! An owner claim (`<checkpoint.path>.lock.json`, same backend as the
//! checkpoint) records who held the lock last, so a crash takeover is
//! visible in logs instead of silent.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{CheckpointConfig, PgConfig, PgTlsConnector};
use tokio_postgres::Client;

/// Magic `classid` shared by every lock this binary takes: bytes "pg2s".
const CLASSID: i32 = 0x7067_3273;

/// Lock key for a namespace: first 4 bytes of SHA-256, little-endian.
/// Deterministic across processes and Rust versions (no DefaultHasher).
pub fn lock_key(namespace: &str) -> i32 {
    let d = Sha256::digest(namespace.as_bytes());
    i32::from_le_bytes([d[0], d[1], d[2], d[3]])
}

/// Owner claim for one lock, stored next to the checkpoint.
#[derive(Serialize, Deserialize)]
struct LockClaim {
    owner: String,
    namespace: String,
    acquired_at_epoch_secs: u64,
}

/// Holds one PG session with advisory locks taken on it. The session IS
/// the lock: dropping/aborting the driver task closes the socket and PG
/// releases every lock held by the session.
pub struct PgAdvisoryGuard {
    client: Client,
    conn_task: tokio::task::JoinHandle<()>,
    keys: Vec<(i32, i32)>,
    /// Session-loss signal: set by the driver task when the PG connection ends
    /// (PG then releases every lock the session held). Cross-host exclusion is
    /// gone at that point, so main.rs watches this and shuts down.
    lost: Arc<AtomicBool>,
    lost_notify: Arc<tokio::sync::Notify>,
}

/// Cloneable handle that resolves when the guard's lock session ends.
/// The guard itself stays owned by the pipeline; the watcher is what a
/// background task awaits.
#[derive(Clone)]
pub struct PgLockLossWatcher {
    lost: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl PgLockLossWatcher {
    /// Whether the session is already known to be gone.
    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    /// Resolve as soon as the lock session ends (immediately if it already did).
    pub async fn wait_lost(&self) {
        if self.lost() {
            return;
        }
        // Register interest BEFORE re-checking so a loss racing this call
        // cannot be missed.
        let notified = self.notify.notified();
        if self.lost() {
            return;
        }
        notified.await;
    }
}

impl Drop for PgAdvisoryGuard {
    fn drop(&mut self) {
        // Closing the client errors the driver future; aborting the task
        // guarantees the socket closes even if the driver were stuck.
        self.conn_task.abort();
    }
}

impl PgAdvisoryGuard {
    /// Explicitly release every lock (graceful shutdown path). Session
    /// close releases anything still held regardless (see `Drop`).
    pub async fn release(self) {
        for (classid, objid) in &self.keys {
            if let Err(e) = self
                .client
                .query("SELECT pg_advisory_unlock($1, $2)", &[classid, objid])
                .await
            {
                tracing::warn!(
                    "pg_advisory_unlock failed for ({classid}, {objid}) — session close releases it anyway: {e}"
                );
            }
        }
    }
}

/// Acquire one session and take every `locks` (label, namespace) pair on
/// it. Fails if ANY key is already held; the error names the holder
/// (PID, application_name, client address, query) via `pg_locks`.
pub async fn acquire(
    pg_cfg: &PgConfig,
    tls: &PgTlsConnector,
    cp_cfg: &CheckpointConfig,
    locks: &[(&str, String)],
) -> Result<PgAdvisoryGuard> {
    if locks.is_empty() {
        anyhow::bail!("pg_lock::acquire called with no locks");
    }
    let conn_str = pg_cfg.conn_str();
    let (client, conn) = tls
        .connect(&conn_str)
        .await
        .context("Failed to connect PG for distributed single-instance lock")?;
    let lost = Arc::new(AtomicBool::new(false));
    let lost_notify = Arc::new(tokio::sync::Notify::new());
    let conn_task = {
        let lost = lost.clone();
        let lost_notify = lost_notify.clone();
        tokio::spawn(async move {
            let _ = conn.await;
            // Driver future finished → the socket is closed and PG has
            // released every advisory lock this session held. Signal it: the
            // guard's caller treats this as "no longer the single instance".
            lost.store(true, Ordering::Release);
            lost_notify.notify_waiters();
        })
    };

    let mut keys = Vec::with_capacity(locks.len());
    for (label, ns) in locks {
        let classid = CLASSID;
        let objid = lock_key(ns);
        let row = client
            .query_one(
                "SELECT pg_try_advisory_lock($1::int4, $2::int4)",
                &[&classid, &objid],
            )
            .await
            .with_context(|| format!("pg_try_advisory_lock query failed for {label}"))?;
        let granted: bool = row.get(0);
        if !granted {
            let holder = holder_info(&client, classid, objid).await;
            conn_task.abort();
            return Err(anyhow!(
                "Distributed single-instance lock is held by another process — refusing to start.\n\
                 \x20 resource: {label}\n\
                 \x20 namespace: {ns}\n\
                 \x20 holder: {}",
                holder.unwrap_or_else(|| "unknown (pg_locks query failed)".into()),
            ));
        }
        keys.push((classid, objid));

        // Owner claim: audit trail for crash takeover. Best-effort — the
        // session lock is the actual guarantee.
        let claim = LockClaim {
            owner: owner_id(),
            namespace: ns.clone(),
            acquired_at_epoch_secs: unix_now(),
        };
        match claim_store(cp_cfg) {
            Ok(store) => match read_claim(&store).await {
                Ok(Some(prev)) if prev.owner != claim.owner => {
                    tracing::warn!(
                        "distributed lock for {label}: previous owner {} (acquired at epoch {}) \
                         no longer holds the session — taking over (crash recovery)",
                        prev.owner,
                        prev.acquired_at_epoch_secs,
                    );
                }
                _ => {}
            },
            Err(e) => tracing::warn!("owner claim store unavailable (non-fatal): {e:#}"),
        }
        if let Ok(store) = claim_store(cp_cfg) {
            if let Err(e) = write_claim(&store, &claim).await {
                tracing::warn!("distributed lock owner claim write failed (non-fatal): {e:#}");
            }
        }
    }

    tracing::info!(
        "Distributed single-instance guard acquired: {} PG advisory lock(s) on session",
        keys.len(),
    );
    Ok(PgAdvisoryGuard {
        client,
        conn_task,
        keys,
        lost,
        lost_notify,
    })
}

impl PgAdvisoryGuard {
    /// Handle for a background task that fails the process closed if the lock
    /// session ever ends mid-run (PG releases the locks on disconnect, so the
    /// guard would otherwise vanish silently while we keep writing).
    pub fn watcher(&self) -> PgLockLossWatcher {
        PgLockLossWatcher {
            lost: self.lost.clone(),
            notify: self.lost_notify.clone(),
        }
    }
}

/// Who holds (classid, objid) right now, for the refusal error message.
async fn holder_info(client: &Client, classid: i32, objid: i32) -> Option<String> {
    let rows = client
        .query(
            "SELECT a.pid, a.application_name, COALESCE(host(a.client_addr), 'local'), \
                    to_char(a.query_start, 'YYYY-MM-DD HH24:MI:SS'), \
                    left(COALESCE(a.query, ''), 120) \
             FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE l.locktype = 'advisory' AND l.classid = $1 AND l.objid = $2 AND l.granted",
            &[&classid, &objid],
        )
        .await
        .ok()?;
    let row = rows.first()?;
    let pid: i32 = row.get(0);
    let app: String = row.get(1);
    let addr: String = row.get(2);
    let since: String = row.get(3);
    let query: String = row.get(4);
    Some(format!(
        "PID {pid}, application_name '{app}', client {addr}, holding since {since}, query: {query}"
    ))
}

// ── Owner claim (same backend as the checkpoint; diagnostic only) ──

enum ClaimStore {
    Local(PathBuf),
    /// Boxed: `S3Backend` is ~250 bytes, dwarfing the `Local` variant.
    S3(Box<crate::wal_buffer::S3Backend>, String),
}

fn claim_path(cp_cfg: &CheckpointConfig) -> String {
    format!("{}.lock.json", cp_cfg.path)
}

fn claim_store(cp_cfg: &CheckpointConfig) -> Result<ClaimStore> {
    if cp_cfg.store_type == "s3" {
        let backend = crate::wal_buffer::S3Backend::new(cp_cfg)
            .context("failed to build S3 claim backend")?;
        Ok(ClaimStore::S3(Box::new(backend), claim_path(cp_cfg)))
    } else {
        Ok(ClaimStore::Local(PathBuf::from(claim_path(cp_cfg))))
    }
}

async fn read_claim(store: &ClaimStore) -> Result<Option<LockClaim>> {
    let bytes = match store {
        ClaimStore::Local(p) => match std::fs::read(p) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context("read owner claim"),
        },
        ClaimStore::S3(backend, key) => match backend.get_object(key).await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e).context("read owner claim"),
        },
    };
    match serde_json::from_slice(&bytes) {
        Ok(c) => Ok(Some(c)),
        Err(_) => {
            tracing::warn!("owner claim corrupt — ignoring");
            Ok(None)
        }
    }
}

async fn write_claim(store: &ClaimStore, claim: &LockClaim) -> Result<()> {
    let bytes = serde_json::to_vec(claim).context("serialize owner claim")?;
    match store {
        ClaimStore::Local(p) => {
            if let Some(parent) = p.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create claim dir {}", parent.display()))?;
            }
            std::fs::write(p, &bytes).with_context(|| format!("write owner claim {}", p.display()))
        }
        ClaimStore::S3(backend, key) => backend.put_object(key, bytes).await,
    }
}

fn owner_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    format!("{host}:{}", std::process::id())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_key_deterministic_and_namespaced() {
        let a = lock_key("pg2sr_cdc_sr:testdb");
        let b = lock_key("pg2sr_cdc_sr:testdb");
        assert_eq!(a, b);
        assert_ne!(a, lock_key("pg2sr_cdc_sr:otherdb"));
        assert_ne!(a, lock_key("pg2sr_cdc_ckpt:s3:ep:bucket:path"));
        // Magic classid is fixed ("pg2s").
        assert_eq!(CLASSID, 0x7067_3273);
    }

    #[tokio::test]
    async fn test_claim_roundtrip_local() {
        let dir = tempfile::tempdir().unwrap();
        let cp_cfg = CheckpointConfig {
            store_type: "local".into(),
            path: dir.path().join("ckpt.json").to_str().unwrap().to_string(),
            save_interval_ms: 10000,
            ..Default::default()
        };
        let store = claim_store(&cp_cfg).unwrap();
        assert!(read_claim(&store).await.unwrap().is_none());

        let claim = LockClaim {
            owner: "host-a:1".into(),
            namespace: "ns".into(),
            acquired_at_epoch_secs: 42,
        };
        write_claim(&store, &claim).await.unwrap();
        let got = read_claim(&store).await.unwrap().unwrap();
        assert_eq!(got.owner, "host-a:1");
        assert_eq!(got.namespace, "ns");
        assert_eq!(got.acquired_at_epoch_secs, 42);

        // Overwrite (takeover) works and read-back shows the new owner.
        let claim2 = LockClaim {
            owner: "host-b:2".into(),
            namespace: "ns".into(),
            acquired_at_epoch_secs: 43,
        };
        write_claim(&store, &claim2).await.unwrap();
        assert_eq!(read_claim(&store).await.unwrap().unwrap().owner, "host-b:2");
    }

    #[test]
    fn test_claim_path_suffix() {
        let cfg = CheckpointConfig {
            path: "ck.json".into(),
            ..Default::default()
        };
        assert_eq!(claim_path(&cfg), "ck.json.lock.json");
    }
}
