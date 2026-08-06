use crate::chunk::compact::{BlockGcConfig, BlockStoreGC, CompactResult, Compactor};
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::config::{CompactConfig, LockTtlConfig};
use crate::meta::store::{LockName, MetaError, MetaStore};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

type CompactionHook = Arc<dyn Fn(u64) + Send + Sync>;

pub struct ChunkLockGuard<M: MetaStore + ?Sized + 'static> {
    chunk_id: u64,
    locked_chunks: Arc<RwLock<HashSet<u64>>>,
    meta_store: Arc<M>,
    unlocked: bool,
}

impl<M: MetaStore + ?Sized + 'static> ChunkLockGuard<M> {
    fn new(chunk_id: u64, locked_chunks: Arc<RwLock<HashSet<u64>>>, meta_store: Arc<M>) -> Self {
        Self {
            chunk_id,
            locked_chunks,
            meta_store,
            unlocked: false,
        }
    }

    pub async fn unlock(&mut self) {
        if self.unlocked {
            return;
        }
        self.unlocked = true;

        // Best-effort explicit release on the normal path. Crash paths still
        // rely on TTL expiry for self-healing.
        let released = self
            .meta_store
            .release_global_lock(LockName::ChunkCompactLock(self.chunk_id))
            .await;
        if !released {
            warn!(
                chunk_id = self.chunk_id,
                "Failed to release global compact lock explicitly, TTL fallback applies"
            );
        }

        let mut locked = self.locked_chunks.write().await;
        locked.remove(&self.chunk_id);
    }
}

impl<M: MetaStore + ?Sized + 'static> Drop for ChunkLockGuard<M> {
    fn drop(&mut self) {
        if !self.unlocked {
            let chunk_id = self.chunk_id;
            let locked_chunks = Arc::clone(&self.locked_chunks);
            let meta_store = Arc::clone(&self.meta_store);
            tokio::spawn(async move {
                // Best-effort release of global lock on unexpected drop (panic, early return).
                // TTL expiry is still the ultimate fallback for crash scenarios.
                let _ = meta_store
                    .release_global_lock(LockName::ChunkCompactLock(chunk_id))
                    .await;
                let mut locked = locked_chunks.write().await;
                locked.remove(&chunk_id);
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionWorkerConfig {
    pub scan_interval: Duration,
    pub max_chunks_per_run: usize,
    /// Maximum number of chunks compacted concurrently in one cycle.
    pub max_concurrent_tasks: usize,
    pub enabled: bool,
}

impl Default for CompactionWorkerConfig {
    fn default() -> Self {
        Self {
            // Scan every 10 minutes instead of every hour so fragmentation is
            // caught sooner.  Scanning is cheap — it only queries metadata.
            scan_interval: Duration::from_secs(600),
            max_chunks_per_run: 100,
            max_concurrent_tasks: 4,
            enabled: true,
        }
    }
}

/// Chunk compaction lock manager with both local and global lock support.
///
/// Uses local in-memory locks for fast checking within the same process,
/// and global locks (via MetaStore) for cross-process synchronization.
/// Supports dynamic TTL calculation based on compaction type and slice count.
#[derive(Debug)]
pub struct CompactLockManager<M: MetaStore + ?Sized> {
    locked_chunks: Arc<RwLock<HashSet<u64>>>,
    meta_store: Arc<M>,
    ttl_config: LockTtlConfig,
}

impl<M: MetaStore + ?Sized + 'static> CompactLockManager<M> {
    #[allow(dead_code)]
    pub fn new(meta_store: Arc<M>) -> Self {
        Self {
            locked_chunks: Arc::new(RwLock::new(HashSet::new())),
            meta_store,
            ttl_config: LockTtlConfig::default(),
        }
    }

    #[allow(dead_code)]
    pub fn with_ttl_config(meta_store: Arc<M>, ttl_config: LockTtlConfig) -> Self {
        Self {
            locked_chunks: Arc::new(RwLock::new(HashSet::new())),
            meta_store,
            ttl_config,
        }
    }

    /// Try to acquire lock for chunk compaction with dynamic TTL.
    ///
    /// # Arguments
    /// * `chunk_id` - The chunk ID to lock
    /// * `slice_count` - Number of slices in the chunk (used for TTL calculation)
    /// * `is_sync` - Whether this is a sync compaction (needs longer TTL)
    pub async fn try_lock(
        &self,
        chunk_id: u64,
        slice_count: usize,
        is_sync: bool,
    ) -> Option<ChunkLockGuard<M>> {
        // Calculate dynamic TTL based on compaction type and slice count
        let ttl_secs = self.ttl_config.calculate_ttl(is_sync, slice_count);

        {
            let mut locked = self.locked_chunks.write().await;
            if locked.contains(&chunk_id) {
                return None;
            }
            locked.insert(chunk_id);
        }

        let global_acquired = self
            .meta_store
            .get_global_lock(LockName::ChunkCompactLock(chunk_id), ttl_secs)
            .await;

        if !global_acquired {
            let mut locked = self.locked_chunks.write().await;
            locked.remove(&chunk_id);
            return None;
        }

        info!(
            chunk_id,
            ttl_secs, slice_count, is_sync, "Acquired compact lock with dynamic TTL"
        );

        Some(ChunkLockGuard::new(
            chunk_id,
            Arc::clone(&self.locked_chunks),
            Arc::clone(&self.meta_store),
        ))
    }

    #[allow(dead_code)]
    pub async fn is_locally_locked(&self, chunk_id: u64) -> bool {
        let locked = self.locked_chunks.read().await;
        locked.contains(&chunk_id)
    }
}

pub struct CompactionWorker<M, B>
where
    M: MetaStore + ?Sized,
{
    meta_store: Arc<M>,
    compactor: Arc<Compactor<B, M>>,
    lock_manager: Arc<CompactLockManager<M>>,
    compaction_hook: Option<CompactionHook>,
}

impl<M: ?Sized, B> CompactionWorker<M, B>
where
    M: MetaStore + Send + Sync + 'static,
    B: BlockStore + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub fn new(meta_store: Arc<M>, block_store: Arc<B>) -> Self {
        Self::with_config(
            meta_store,
            block_store,
            ChunkLayout::default(),
            CompactConfig::default(),
            LockTtlConfig::default(),
        )
    }

    pub fn with_config(
        meta_store: Arc<M>,
        block_store: Arc<B>,
        layout: ChunkLayout,
        compact_config: CompactConfig,
        lock_ttl_config: LockTtlConfig,
    ) -> Self {
        let compactor: Arc<Compactor<B, M>> = Arc::new(Compactor::with_config(
            Arc::clone(&meta_store),
            block_store,
            layout,
            compact_config,
        ));
        let lock_manager = Arc::new(CompactLockManager::with_ttl_config(
            Arc::clone(&meta_store),
            lock_ttl_config,
        ));
        Self {
            meta_store,
            compactor,
            lock_manager,
            compaction_hook: None,
        }
    }

    pub fn with_compaction_hook(mut self, hook: CompactionHook) -> Self {
        self.compaction_hook = Some(hook);
        self
    }

    pub fn start(
        self,
        worker_config: CompactionWorkerConfig,
        gc_config: BlockGcConfig,
    ) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
        let compactor = self.compactor.clone();
        let meta_store = self.meta_store.clone();
        let lock_manager = self.lock_manager.clone();
        let compaction_hook = self.compaction_hook.clone();
        let compaction_handle = tokio::spawn(async move {
            if !worker_config.enabled {
                return;
            }

            let mut ticker = interval(worker_config.scan_interval);
            loop {
                ticker.tick().await;

                if let Err(e) = run_compaction_cycle(
                    &meta_store,
                    &compactor,
                    &lock_manager,
                    &worker_config,
                    compaction_hook.as_ref(),
                )
                .await
                {
                    error!(error = %e, "Compaction cycle failed");
                }
            }
        });
        let gc_handle = BlockStoreGC::new(self.meta_store, self.compactor.block_store().clone())
            .start(gc_config);

        (compaction_handle, gc_handle)
    }

    #[allow(dead_code)]
    pub async fn is_chunk_locally_locked(&self, chunk_id: u64) -> bool {
        self.lock_manager.is_locally_locked(chunk_id).await
    }
}

async fn run_compaction_cycle<M, B>(
    meta_store: &Arc<M>,
    compactor: &Arc<Compactor<B, M>>,
    lock_manager: &Arc<CompactLockManager<M>>,
    config: &CompactionWorkerConfig,
    compaction_hook: Option<&CompactionHook>,
) -> anyhow::Result<()>
where
    M: MetaStore + Send + Sync + ?Sized + 'static,
    B: BlockStore + Send + Sync + 'static,
{
    let chunk_ids = match meta_store.list_chunk_ids(config.max_chunks_per_run).await {
        Ok(ids) => ids,
        Err(MetaError::NotImplemented) => {
            debug!(
                "list_chunk_ids not implemented for this MetaStore backend, skipping compaction cycle"
            );
            return Ok(());
        }
        Err(e) => return Err(anyhow::anyhow!("Failed to list chunk IDs: {}", e)),
    };

    // Bounded concurrency: chunks in one cycle are distinct, and the per-chunk
    // global lock already prevents two nodes (or two tasks) from compacting the
    // same chunk, so parallel processing only overlaps independent chunks.
    let sem = Arc::new(tokio::sync::Semaphore::new(
        config.max_concurrent_tasks.max(1),
    ));
    let mut handles = Vec::with_capacity(chunk_ids.len());
    for chunk_id in chunk_ids {
        // Acquire before spawning so the semaphore actually bounds in-flight
        // work instead of merely bounding spawned-but-queued tasks.
        let permit = match sem.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => sem.clone().acquire_owned().await?,
        };
        let compactor = Arc::clone(compactor);
        let lock_manager = Arc::clone(lock_manager);
        let hook = compaction_hook.cloned();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            compact_chunk_if_needed(&compactor, &lock_manager, chunk_id, hook.as_ref()).await;
        }));
    }
    for handle in handles {
        if let Err(e) = handle.await {
            warn!(error = %e, "Compaction task panicked");
        }
    }

    Ok(())
}

/// Inspect one chunk and compact it if needed, honoring the per-chunk compact
/// lock. Extracted from `run_compaction_cycle` so a cycle can process multiple
/// independent chunks with bounded concurrency.
async fn compact_chunk_if_needed<M, B>(
    compactor: &Arc<Compactor<B, M>>,
    lock_manager: &Arc<CompactLockManager<M>>,
    chunk_id: u64,
    compaction_hook: Option<&CompactionHook>,
) where
    M: MetaStore + Send + Sync + ?Sized + 'static,
    B: BlockStore + Send + Sync + 'static,
{
    match compactor.should_compact(chunk_id).await {
        Ok((true, is_sync)) => {
            // Get slice count for dynamic TTL calculation
            let slice_count = match compactor.analyze_chunk(chunk_id).await {
                Ok((count, _, _)) => count,
                Err(e) => {
                    warn!(chunk_id, error = %e, "Failed to analyze chunk for lock TTL");
                    return;
                }
            };

            // Both sync and async compaction require the distributed lock
            // to prevent multiple nodes from compacting the same chunk.
            let mut lock_guard = lock_manager.try_lock(chunk_id, slice_count, is_sync).await;

            if lock_guard.is_none() {
                debug!(
                    chunk_id,
                    is_sync, "Could not acquire compact lock (local or global), skipping"
                );
                return;
            }

            // Re-read slice count after acquiring lock to reduce TOCTOU window.
            // Another node may have already compacted this chunk.
            let slice_count = match compactor.analyze_chunk(chunk_id).await {
                Ok((count, _, _)) => count,
                Err(e) => {
                    warn!(chunk_id, error = %e, "Failed to re-analyze chunk after lock");
                    if let Some(ref mut guard) = lock_guard {
                        guard.unlock().await;
                    }
                    return;
                }
            };

            // Re-check if compaction is still needed after lock
            match compactor.should_compact(chunk_id).await {
                Ok((true, _)) => {}
                _ => {
                    debug!(chunk_id, "Compaction no longer needed after re-check");
                    if let Some(ref mut guard) = lock_guard {
                        guard.unlock().await;
                    }
                    return;
                }
            }

            if is_sync {
                info!(
                    chunk_id,
                    slice_count, "Acquired sync compact lock, blocking writes"
                );
            } else {
                debug!(chunk_id, slice_count, "Acquired async compact lock");
            }

            let result = compactor.compact_sequential(chunk_id).await;
            if let Some(ref mut guard) = lock_guard {
                guard.unlock().await;
            }

            match result {
                Ok(CompactResult::Skipped) => {
                    debug!(chunk_id, "Compaction skipped");
                }
                Ok(CompactResult::Light { removed }) => {
                    info!(chunk_id, removed, is_sync, "Light compaction completed");
                    if let Some(hook) = compaction_hook {
                        hook(chunk_id);
                    }
                }
                Ok(CompactResult::Heavy { .. }) => {
                    info!(chunk_id, is_sync, "Heavy compaction completed");
                    if let Some(hook) = compaction_hook {
                        hook(chunk_id);
                    }
                }
                Err(e) => {
                    warn!(chunk_id, error = %e, is_sync, "Compaction failed");
                }
            }
        }
        Ok((false, _)) => {}
        Err(e) => {
            warn!(chunk_id, error = %e, "Error checking compaction status");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::config::CompactConfig;
    use crate::vfs::fs::VfsBackgroundConfig;

    #[test]
    fn compaction_worker_config_default_has_bounded_concurrency() {
        let cfg = CompactionWorkerConfig::default();
        assert!(
            cfg.max_concurrent_tasks >= 1,
            "default compaction concurrency must be at least 1"
        );
    }

    #[test]
    fn compaction_worker_config_propagates_max_concurrent_tasks() {
        let mut compact = CompactConfig::default();
        compact.max_concurrent_tasks = 7;
        let bg = VfsBackgroundConfig::from_compact_config(&ChunkLayout::default(), compact, true);
        assert_eq!(bg.compaction.max_concurrent_tasks, 7);
        assert_eq!(bg.compaction.max_chunks_per_run, 1000);
    }
}
