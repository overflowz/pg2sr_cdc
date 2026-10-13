//! Local filesystem checkpoint store.
//!
//! Uses an atomic write pattern: write to a `.tmp` file, then rename to the
//! target path. Corrupted files are renamed to `.corrupted` for forensics.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use super::{CURRENT_CHECKPOINT_VERSION, Checkpoint, CheckpointStore};

/// Local filesystem checkpoint store.
/// Uses atomic write pattern: write to `.tmp` file, then rename to target path.
/// Corrupted files are renamed to `.corrupted` for forensics.
pub struct LocalStore {
    path: PathBuf,
    /// Serializes save() calls to prevent concurrent write races.
    save_lock: tokio::sync::Mutex<()>,
}

impl LocalStore {
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            path: PathBuf::from(path),
            save_lock: tokio::sync::Mutex::new(()),
        })
    }
}

#[async_trait::async_trait]
impl CheckpointStore for LocalStore {
    fn store_name(&self) -> &str {
        "local"
    }

    async fn load(&self) -> Result<Checkpoint> {
        let path = &self.path;
        if !path.exists() {
            tracing::info!("No checkpoint file at {:?}, starting fresh", path);
            return Ok(Checkpoint::default());
        }
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read checkpoint: {:?}", path))?;
        if content.trim().is_empty() {
            bail!(
                "Checkpoint file at {:?} is empty — remove it manually and restart",
                path
            );
        }
        let cp: Checkpoint = match serde_json::from_str(&content) {
            Ok(cp) => cp,
            Err(e) => {
                // Corrupted checkpoint — rename for forensics and bail.
                let corrupted_path = path.with_extension("json.corrupted");
                if let Err(rename_err) = fs::rename(path, &corrupted_path) {
                    tracing::error!(
                        "Failed to rename corrupted checkpoint {:?}: {} — removing instead",
                        path,
                        rename_err
                    );
                    let _ = fs::remove_file(path);
                }
                bail!(
                    "Checkpoint at {:?} is corrupted (parse error: {}) — renamed to {:?}. Inspect and fix or remove manually.",
                    path, e, corrupted_path
                );
            }
        };
        // Version check: old checkpoints (version < 2) may have
        // last_ddl_id = 0 (missing field defaulted), causing DDL re-execution.
        if cp.version < CURRENT_CHECKPOINT_VERSION {
            bail!(
                "Checkpoint at {:?} is version {} (current is {}). \
                 The last_ddl_id field may be unreliable. \
                 Run with --fresh to re-snapshot from scratch.",
                path, cp.version, CURRENT_CHECKPOINT_VERSION,
            );
        }
        tracing::info!(
            "Loaded checkpoint: lsn={}, snapshot={}, ddl_id={}",
            cp.last_committed_lsn,
            cp.snapshot_done,
            cp.last_ddl_id
        );
        Ok(cp)
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<()> {
        // Acquire the save_lock to serialize concurrent save() calls.
        let _guard = self.save_lock.lock().await;
        self.save_locked(checkpoint).await
    }

    /// Capture-and-save atomically (see the trait doc): the checkpoint is
    /// cloned while the store write lock is held so an older concurrent
    /// capture can never overwrite a newer save (clone-then-lock window).
    async fn save_captured<'a>(
        &self,
        capture: Box<dyn FnOnce() -> Checkpoint + Send + 'a>,
    ) -> Result<Checkpoint> {
        let _guard = self.save_lock.lock().await;
        let cp = capture();
        self.save_locked(&cp).await?;
        Ok(cp)
    }

    async fn delete(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

impl LocalStore {
    /// Checkpoint write body, run with `save_lock` held (by `save` or
    /// `save_captured`).
    async fn save_locked(&self, checkpoint: &Checkpoint) -> Result<()> {
        // Ensure version is current.
        let cp = if checkpoint.version < CURRENT_CHECKPOINT_VERSION {
            let mut c = checkpoint.clone();
            c.version = CURRENT_CHECKPOINT_VERSION;
            c
        } else {
            checkpoint.clone()
        };
        let path = &self.path;
        let tmp_path = path.with_extension("tmp");
        let content =
            serde_json::to_string_pretty(&cp).context("Failed to serialize checkpoint")?;

        // Write to tmp file + fsync the file data, then atomically rename
        // and fsync the parent directory. Without the file fsync, a power
        // loss after rename can leave the checkpoint file with zero or
        // partial data on disk (the OS page cache is lost). Without the
        // directory fsync, the rename itself may not be durable after
        // power loss. The S3 backend is immune (PUT success = durable),
        // but the local backend needs explicit fsync for at-least-once
        // correctness under power failure.
        use std::io::Write;
        let mut file = fs::File::create(&tmp_path)
            .with_context(|| format!("Failed to create temp checkpoint: {:?}", tmp_path))?;
        file.write_all(content.as_bytes())
            .with_context(|| format!("Failed to write temp checkpoint: {:?}", tmp_path))?;
        file.sync_all()
            .with_context(|| format!("Failed to fsync temp checkpoint: {:?}", tmp_path))?;
        drop(file); // close before rename (Windows requirement)

        fs::rename(&tmp_path, path).with_context(|| {
            format!("Failed to rename checkpoint: {:?} -> {:?}", tmp_path, path)
        })?;

        // fsync the parent directory to make the rename durable.
        // On Linux this is required for the rename to survive power loss.
        // On macOS/iOS it's a no-op. On Windows it's not needed (NTFS
        // rename is atomic + durable). We best-effort it — if it fails
        // (e.g. /tmp on some systems), we log and continue; the file
        // fsync above already protects the data.
        if let Some(parent) = path.parent() {
            match fs::File::open(parent) {
                Ok(dir) => {
                    if let Err(e) = dir.sync_all() {
                        tracing::warn!(
                            "Failed to fsync checkpoint parent directory {:?}: {} \
                             — rename may not be durable after power loss",
                            parent, e
                        );
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        "Could not open parent dir {:?} for fsync: {}",
                        parent, e
                    );
                }
            }
        }

        tracing::debug!("Checkpoint saved: lsn={}", cp.last_committed_lsn);
        Ok(())
    }
}
