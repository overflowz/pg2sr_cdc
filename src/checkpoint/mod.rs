//! Checkpoint persistence — stores last committed LSN to local file or S3.
//!
//! Architecture: `CheckpointStore` trait with two implementations:
//! - `LocalStore` — atomic writes via temp-file + rename (existing behavior)
//! - `S3Store` — S3-compatible object stores (AWS S3, MinIO, Ceph, etc.)
//!
//! A factory function `create_checkpoint_store()` dispatches based on config.
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::config::CheckpointConfig;

mod local;
mod s3;

pub use local::LocalStore;
pub use s3::S3Store;

// ── Checkpoint data ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    /// Checkpoint schema version for forward/backward compatibility.
    /// Current version: 2. Version 1 (pre-this-field) is auto-migrated on load.
    #[serde(default = "default_checkpoint_version")]
    pub version: u32,
    /// The WAL flush LSN (as integer) that was last safely committed to StarRocks.
    pub last_committed_lsn: u64,
    /// Snapshot flag — true if initial snapshot has been completed.
    pub snapshot_done: bool,
    /// Tables that have been snapshotted (schema.table format).
    #[serde(default)]
    pub snapshotted_tables: Vec<String>,
    /// Schema hashes for each snapshotted table (schema.table → hash of column names + type OIDs + PK membership).
    /// Used for gap detection: if the PG schema changed since snapshot, the hash won't match
    /// and the table is re-snapshotted.
    #[serde(default)]
    pub snapshotted_table_hashes: HashMap<String, u64>,
    /// Last processed DDL capture id.
    /// Note: this field uses serde(default). If missing (old checkpoint), it
    /// defaults to 0, which means the DDL poller will re-apply ALL DDLs from
    /// id 1. This is safe for idempotent DDLs (CREATE TABLE) but DANGEROUS for
    /// non-idempotent DDLs (DROP TABLE, RENAME). The version field guards this:
    /// if version < 2, load() refuses to start to prevent catastrophic re-execution.
    #[serde(default)]
    pub last_ddl_id: i64,
    /// Normalized table_filter value at last snapshot. Used to detect filter changes
    /// on restart — if the filter expanded, newly-matching tables are re-snapshotted
    /// via gap detection (they won't be in snapshotted_tables).
    #[serde(default)]
    pub table_filter: Option<String>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            version: CURRENT_CHECKPOINT_VERSION,
            last_committed_lsn: 0,
            snapshot_done: false,
            snapshotted_tables: Vec::new(),
            snapshotted_table_hashes: HashMap::new(),
            last_ddl_id: 0,
            table_filter: None,
        }
    }
}

impl Checkpoint {
    /// Returns true if `self` represents strictly more progress than `other`.
    ///
    /// Used by the S3 `load()` path to recover from a crash between `save()`'s
    /// `.tmp` PUT and the `main` overwrite (window ~1 S3 PUT latency). Both
    /// `last_committed_lsn` and `last_ddl_id` are non-decreasing across saves
    /// (the pipeline never rewinds either marker), so `self` is newer iff:
    /// both fields are ≥ `other`'s, and at least one is strictly greater.
    /// Equal tuples mean the two checkpoints are equivalent — caller keeps
    /// `main` and deletes the stale `.tmp`.
    ///
    /// Partial-order incomparability (one field higher, the other lower)
    /// should not occur in normal operation; if it does, this returns false
    /// and `main` wins — a conservative choice that may redo some replay but
    /// never loses committed progress.
    pub(crate) fn is_newer_than(&self, other: &Checkpoint) -> bool {
        let le_lsn = self.last_committed_lsn >= other.last_committed_lsn;
        let le_ddl = self.last_ddl_id >= other.last_ddl_id;
        let gt_lsn = self.last_committed_lsn > other.last_committed_lsn;
        let gt_ddl = self.last_ddl_id > other.last_ddl_id;
        le_lsn && le_ddl && (gt_lsn || gt_ddl)
    }
}

const CURRENT_CHECKPOINT_VERSION: u32 = 2;

fn default_checkpoint_version() -> u32 {
    1 // old checkpoints without version field are version 1
}

// ── CheckpointStore trait ──────────────────────────────────────────────────

/// Abstract checkpoint persistence backend.
///
/// Implementations must be:
/// - **Atomic-safe**: a crash during `save()` should not corrupt the stored checkpoint.
///   For local files this means write-to-temp-then-rename. For S3, the implementation
///   uses a two-stage upload (write `.tmp` key, then overwrite the main key).
/// - **Idempotent**: `delete()` on a non-existent checkpoint is a no-op.
#[async_trait::async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Load checkpoint from the store. Returns `Checkpoint::default()` if no
    /// checkpoint exists yet (fresh start).
    async fn load(&self) -> Result<Checkpoint>;
    /// Save checkpoint to the store atomically.
    async fn save(&self, checkpoint: &Checkpoint) -> Result<()>;
    /// Save a checkpoint captured INSIDE the store's write lock. The
    /// background saver and the DDL poller both clone-then-save; without
    /// atomic capture, the older clone's write can land after the newer
    /// one and regress the durable checkpoint (a crash then re-executes
    /// DDLs the newer checkpoint had already applied). Default impl
    /// captures then calls `save` (identical for stores without an
    /// internal write lock).
    ///
    /// Returns the checkpoint that was durably written — callers must use
    /// ITS values for last-saved bookkeeping (a post-save re-clone could be
    /// newer than the durable state and over-advance the ACK watermark).
    async fn save_captured<'a>(
        &self,
        capture: Box<dyn FnOnce() -> Checkpoint + Send + 'a>,
    ) -> Result<Checkpoint> {
        let cp = capture();
        self.save(&cp).await?;
        Ok(cp)
    }
    /// Delete the checkpoint from the store. No-op if it doesn't exist.
    async fn delete(&self) -> Result<()>;
    /// Human-readable store name for logging (e.g. "local", "s3").
    fn store_name(&self) -> &str;
    /// Verify the backend is reachable and writable before CDC starts.
    ///
    /// Called once at startup, before any other state-changing work. Lets the
    /// pipeline fail fast on misconfiguration (missing bucket, wrong creds,
    /// unreachable endpoint, unwritable path) instead of erroring out later
    /// during snapshot/CDC. Default impl is a no-op so backends that have
    /// nothing meaningful to verify stay unaffected.
    async fn verify(&self) -> Result<()> {
        Ok(())
    }
}

// ── Factory ────────────────────────────────────────────────────────────────

/// Create a checkpoint store from config. Supports `"local"` and `"s3"` store types.
pub fn create_checkpoint_store(config: &CheckpointConfig) -> Result<Arc<dyn CheckpointStore>> {
    match config.store_type.as_str() {
        "local" => Ok(Arc::new(LocalStore::new(&config.path)?)),
        "s3" => Ok(Arc::new(S3Store::new(config)?)),
        other => {
            bail!("Unknown checkpoint store type: '{other}'. Expected 'local' or 's3'.")
        }
    }
}

// ── Mutex helpers ──────────────────────────────────────────────────────────

/// Lock the checkpoint mutex, recovering from poison errors.
pub fn lock_checkpoint(cp: &Arc<Mutex<Checkpoint>>) -> MutexGuard<'_, Checkpoint> {
    cp.lock().unwrap_or_else(|poison| {
        tracing::error!("Checkpoint mutex poisoned! Recovering inner state.");
        poison.into_inner()
    })
}

/// Atomically advance the DDL checkpoint pointer if the given `ddl_id` is newer.
/// Only updates in-memory state — the background checkpoint saver handles
/// durable persistence on its configured interval. This avoids S3 PUTs on
/// every DDL event while still tracking progress correctly.
/// This is the single checkpoint-advance helper — all DDL poller paths must use it.
pub fn advance_ddl_checkpoint(checkpoint: &Arc<Mutex<Checkpoint>>, ddl_id: i64) {
    let mut cp = lock_checkpoint(checkpoint);
    if ddl_id > cp.last_ddl_id {
        cp.last_ddl_id = ddl_id;
    }
}

/// Advance the DDL checkpoint and durably persist it to the store.
/// Unlike `advance_ddl_checkpoint` which only updates in-memory state,
/// this also calls `store.save()` to prevent re-execution of
/// non-idempotent DDLs (DROP TABLE, DROP COLUMN, RENAME) after a crash.
/// Use on DDL-success paths only (not skip/error paths) to bound S3 PUTs
/// to DDL frequency rather than per-batch CDC frequency.
pub async fn advance_and_save_ddl_checkpoint(
    checkpoint: &Arc<Mutex<Checkpoint>>,
    store: &dyn CheckpointStore,
    ddl_id: i64,
) {
    let needs_save = {
        let mut cp = lock_checkpoint(checkpoint);
        if ddl_id > cp.last_ddl_id {
            cp.last_ddl_id = ddl_id;
            true
        } else {
            false
        }
    };
    if needs_save {
        // save_captured clones the CURRENT in-memory state while holding the
        // store's write lock, so a concurrent background-saver write can
        // never land AFTER this one with an older capture (the plain
        // clone-then-save window regressed the durable checkpoint).
        if let Err(e) = store
            .save_captured(Box::new(|| lock_checkpoint(checkpoint).clone()))
            .await
        {
            // Non-fatal — the background checkpoint saver will eventually
            // persist this on its regular interval. The worst case is a
            // replayed DDL on restart, which is idempotent for most DDLs.
            let cur = lock_checkpoint(checkpoint).clone();
            tracing::warn!(
                "Failed to persist DDL checkpoint after advancement (lsn={}, ddl_id={}): {:?}",
                cur.last_committed_lsn, cur.last_ddl_id, e,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ── LocalStore tests ──────────────────────────────────────────────

    #[test]
    fn test_local_store_save_load() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_cp.json");
            let _ = std::fs::remove_file(&path);

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();

            let cp = Checkpoint {
                last_committed_lsn: 42,
                snapshot_done: true,
                snapshotted_tables: vec!["public.t1".into(), "public.t2".into()],
                last_ddl_id: 7,
                ..Default::default()
            };

            store.save(&cp).await.expect("Save should succeed");
            assert!(path.exists(), "Checkpoint file should exist");

            let loaded = store.load().await.expect("Load should succeed");
            assert_eq!(loaded.last_committed_lsn, 42);
            assert!(loaded.snapshot_done);
            assert_eq!(loaded.snapshotted_tables, vec!["public.t1", "public.t2"]);
            assert_eq!(loaded.last_ddl_id, 7);

            let _ = std::fs::remove_file(&path);
        });
    }

    #[test]
    fn test_local_store_mid_snapshot_resumability() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_mid_snapshot.json");
            let _ = std::fs::remove_file(&path);

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();

            // Phase 1: snapshot table A, save mid-snapshot
            let mut cp = Checkpoint::default();
            cp.snapshotted_tables.push("public.orders".into());
            store.save(&cp).await.expect("Mid-snapshot save");

            // Phase 2: simulate restart after crash
            let loaded = store.load().await.expect("Resume");
            assert!(
                !loaded.snapshot_done,
                "snapshot should not be marked complete"
            );
            assert_eq!(loaded.snapshotted_tables, vec!["public.orders"]);
            assert_eq!(loaded.last_committed_lsn, 0, "CDC should not have started");

            let _ = std::fs::remove_file(&path);
        });
    }

    #[test]
    fn test_local_store_empty_file_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_empty.json");
            std::fs::write(&path, "").expect("Write empty file");

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();
            let result = store.load().await;
            assert!(result.is_err(), "Empty file should return error");
            assert!(
                format!("{:?}", result.unwrap_err()).contains("empty"),
                "Error should mention empty file"
            );

            let _ = std::fs::remove_file(&path);
        });
    }

    #[test]
    fn test_local_store_missing_file_returns_default() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_nonexistent.json");
            let _ = std::fs::remove_file(&path);

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();
            let cp = store
                .load()
                .await
                .expect("Missing file should return default");
            assert_eq!(cp.last_committed_lsn, 0);
        });
    }

    #[test]
    fn test_local_store_mutex_poison_recovery() {
        let cp = Arc::new(Mutex::new(Checkpoint::default()));

        // Poison the mutex
        let cp2 = cp.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = cp2.lock().unwrap();
            panic!("simulated panic while holding lock");
        }));

        // Recovery should succeed and return the inner data
        let guard = lock_checkpoint(&cp);
        assert_eq!(guard.last_committed_lsn, 0);
        assert!(!guard.snapshot_done);
    }

    #[test]
    fn test_local_store_corrupted_file_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_corrupted.json");
            let corrupted_path =
                std::env::temp_dir().join("pg2sr_cdc_test_corrupted.json.corrupted");
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(&corrupted_path);

            std::fs::write(&path, "this is not json {{{[").expect("Write corrupted file");

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();
            let result = store.load().await;
            assert!(result.is_err(), "Corrupted file should return error");
            assert!(
                format!("{:?}", result.unwrap_err()).contains("corrupted"),
                "Error should mention corruption"
            );

            assert!(!path.exists(), "Corrupted file should be gone");
            assert!(
                corrupted_path.exists(),
                "Corrupted file should be preserved as .corrupted"
            );

            let _ = std::fs::remove_file(&corrupted_path);
        });
    }

    #[test]
    fn test_local_store_corrupted_partial_json_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_partial.json");
            let corrupted_path = std::env::temp_dir().join("pg2sr_cdc_test_partial.json.corrupted");
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(&corrupted_path);

            std::fs::write(&path, r#"{"last_committed_lsn": 42, "snapshot_done":"#)
                .expect("Write partial JSON");

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();
            let result = store.load().await;
            assert!(result.is_err(), "Partial JSON should return error");
            assert!(
                format!("{:?}", result.unwrap_err()).contains("corrupted"),
                "Error should mention corruption"
            );
            assert!(!path.exists(), "Corrupted file should be gone");

            let _ = std::fs::remove_file(&corrupted_path);
        });
    }

    #[test]
    fn test_local_store_advance_ddl_only_forward() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_advance.json");
            let _ = std::fs::remove_file(&path);
            let _store = LocalStore::new(path.to_str().unwrap()).unwrap();

            let cp = Arc::new(Mutex::new(Checkpoint {
                last_ddl_id: 42,
                ..Default::default()
            }));

            // Same ID — should not advance
            advance_ddl_checkpoint(&cp, 42);
            assert_eq!(lock_checkpoint(&cp).last_ddl_id, 42);

            // Older ID — should not advance
            advance_ddl_checkpoint(&cp, 10);
            assert_eq!(lock_checkpoint(&cp).last_ddl_id, 42);

            // Newer ID — must advance
            advance_ddl_checkpoint(&cp, 100);
            assert_eq!(lock_checkpoint(&cp).last_ddl_id, 100);

            let _ = std::fs::remove_file(&path);
        });
    }

    #[test]
    fn test_local_store_advance_ddl_initial_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::env::temp_dir().join("pg2sr_cdc_test_advance_init.json");
            let _ = std::fs::remove_file(&path);
            let _store = LocalStore::new(path.to_str().unwrap()).unwrap();

            let cp = Arc::new(Mutex::new(Checkpoint::default()));

            advance_ddl_checkpoint(&cp, 1);
            assert_eq!(lock_checkpoint(&cp).last_ddl_id, 1);

            let _ = std::fs::remove_file(&path);
        });
    }

    // ── Regression tests ──────────────────────────────

    #[test]
    fn test_checkpoint_default_has_current_version() {
        let cp = Checkpoint::default();
        assert_eq!(cp.version, CURRENT_CHECKPOINT_VERSION,
            "Default Checkpoint must have current version");
    }

    #[test]
    fn test_is_newer_than_higher_lsn_is_newer() {
        // tmp has higher LSN, same ddl_id → newer (the crash window:
        // save() wrote tmp with an advanced LSN, crashed before overwriting main)
        let main = Checkpoint { last_committed_lsn: 100, last_ddl_id: 5, ..Checkpoint::default() };
        let tmp  = Checkpoint { last_committed_lsn: 150, last_ddl_id: 5, ..Checkpoint::default() };
        assert!(tmp.is_newer_than(&main));
        assert!(!main.is_newer_than(&tmp));
    }

    #[test]
    fn test_is_newer_than_higher_ddl_id_is_newer() {
        // tmp has same LSN, higher ddl_id → newer (DDL-only checkpoint advance
        // crashed between tmp PUT and main overwrite)
        let main = Checkpoint { last_committed_lsn: 100, last_ddl_id: 5, ..Checkpoint::default() };
        let tmp  = Checkpoint { last_committed_lsn: 100, last_ddl_id: 7, ..Checkpoint::default() };
        assert!(tmp.is_newer_than(&main));
        assert!(!main.is_newer_than(&tmp));
    }

    #[test]
    fn test_is_newer_than_equal_is_not_newer() {
        // Equal LSN + ddl_id → equivalent checkpoints, neither is "newer" →
        // caller keeps main and deletes the stale tmp
        let main = Checkpoint { last_committed_lsn: 100, last_ddl_id: 5, ..Checkpoint::default() };
        let tmp  = Checkpoint { last_committed_lsn: 100, last_ddl_id: 5, ..Checkpoint::default() };
        assert!(!tmp.is_newer_than(&main));
        assert!(!main.is_newer_than(&tmp));
    }

    #[test]
    fn test_is_newer_than_incomparable_is_not_newer() {
        // Partial-order incomparability (shouldn't happen in normal operation):
        // tmp has higher LSN but LOWER ddl_id → conservatively not "newer" →
        // main wins (may cause some redundant replay but never loses progress)
        let main = Checkpoint { last_committed_lsn: 100, last_ddl_id: 7, ..Checkpoint::default() };
        let tmp  = Checkpoint { last_committed_lsn: 150, last_ddl_id: 5, ..Checkpoint::default() };
        assert!(!tmp.is_newer_than(&main));
    }

    #[test]
    fn test_is_newer_than_both_higher_is_newer() {
        // tmp strictly ahead on both axes → newer
        let main = Checkpoint { last_committed_lsn: 100, last_ddl_id: 5, ..Checkpoint::default() };
        let tmp  = Checkpoint { last_committed_lsn: 200, last_ddl_id: 8, ..Checkpoint::default() };
        assert!(tmp.is_newer_than(&main));
    }

    #[test]
    fn test_local_store_rejects_old_version_checkpoint() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cp.json");
            // Write a version-1 checkpoint manually (missing version field
            // defaults to version=1 via serde default).
            let old_json = r#"{"last_committed_lsn":12345,"snapshot_done":true,"last_ddl_id":50}"#;
            std::fs::write(&path, old_json).unwrap();

            let store = LocalStore::new(path.to_str().unwrap()).unwrap();
            let result = store.load().await;
            assert!(result.is_err(), "Old-version checkpoint must be rejected");
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("version"),
                "Error must mention version: {err_msg}");
        });
    }

    #[test]
    fn test_local_store_save_sets_version() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cp.json");
            let store = LocalStore::new(path.to_str().unwrap()).unwrap();

            // Even a manually-constructed checkpoint with version=0
            // should be upgraded on save.
            let cp = Checkpoint {
                version: 0, // simulate old struct
                last_committed_lsn: 999,
                ..Default::default()
            };
            store.save(&cp).await.unwrap();

            // Read back raw JSON and check version field
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.contains(r#""version": 2"#),
                "Saved checkpoint must have version=2: {content}");
        });
    }

    #[test]
    fn test_local_store_save_lock_exists() {
        // Verify the save_lock field exists and is usable — this is the
        // mechanism that prevents concurrent save() races (S3 last-PUT-wins
        // could regress ddl_id → DDL re-execution → data loss).
        // Full concurrency testing requires S3Store (tested via wiremock).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cp.json");
            let store = LocalStore::new(path.to_str().unwrap()).unwrap();

            // Sequential saves with increasing LSN — the lock ensures
            // each completes fully before the next starts.
            for lsn in [100u64, 200, 300] {
                let cp = Checkpoint {
                    last_committed_lsn: lsn,
                    ..Default::default()
                };
                store.save(&cp).await.unwrap();
            }

            // Read back — should have the last LSN value
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.contains(r#""last_committed_lsn": 300"#),
                "Last save must win: {content}");
        });
    }

    #[test]
    fn test_checkpoint_version_serialization() {
        // Verify that serialization includes the version field and
        // deserialization reads it correctly.
        let cp = Checkpoint {
            version: 2,
            last_committed_lsn: 42,
            snapshot_done: true,
            ..Default::default()
        };
        let json = serde_json::to_string(&cp).unwrap();
        assert!(json.contains(r#""version":2"#), "JSON must contain version field");

        let loaded: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.last_committed_lsn, 42);
    }
}
