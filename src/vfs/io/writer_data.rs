//! Data writer: high-level write entry point for the VFS.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): owns the
//! per-inode FileWriter handles and drives flush/flush_all/close. Pure code
//! motion — no behavior changes.

use crate::chunk::BlockStore;
use crate::meta::MetaLayer;
use crate::utils::NumCastExt;
use crate::vfs::Inode;
use crate::vfs::backend::Backend;
use crate::vfs::chunk_id_for;
use crate::vfs::config::WriteConfig;
use crate::vfs::io::reader::DataReader;
use crate::vfs::io::split_chunk_spans;
use crate::vfs::io::writer::FileWriter;
use crate::vfs::io::writer_accounting::RecentPendingUploadState;
use crate::vfs::io::writer_handle::DirtyOverlayPatch;
use crate::vfs::io::writer_policy::*;
use crate::vfs::io::writer_upload::WriteOriginKind;
use crate::vfs::memory::MemoryBudget;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::time::interval;

pub(crate) struct DataWriter<B, M> {
    config: Arc<WriteConfig>,
    backend: Arc<Backend<B, M>>,
    reader: Arc<DataReader<B, M>>,
    files: DashMap<u64, Arc<FileWriter<B, M>>>,
    buffer_usage: Arc<AtomicU64>,
    write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    memory_budget: Option<MemoryBudget>,
    recent_pending_upload: Arc<RecentPendingUploadState>,
}

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct WritebackDirtyBreakdown {
    pub live_bytes: u64,
    pub live_slices: u64,
    pub live_normal_only_bytes: u64,
    pub live_normal_only_slices: u64,
    pub live_cached_only_bytes: u64,
    pub live_cached_only_slices: u64,
    pub live_mixed_origin_bytes: u64,
    pub live_mixed_origin_slices: u64,
    pub live_unknown_origin_bytes: u64,
    pub live_unknown_origin_slices: u64,
    pub recently_committed_pending_upload_bytes: u64,
    pub recently_committed_pending_upload_slices: u64,
    pub recently_committed_uploaded_bytes: u64,
    pub recently_committed_uploaded_slices: u64,
    pub backpressure_soft_sleep_ops: u64,
    pub backpressure_soft_sleep_us: u64,
    pub backpressure_hard_wait_ops: u64,
    pub backpressure_hard_wait_us: u64,
    pub buffer_soft_sleep_ops: u64,
    pub buffer_soft_sleep_us: u64,
    pub buffer_moderate_sleep_ops: u64,
    pub buffer_moderate_sleep_us: u64,
    pub buffer_hard_sleep_ops: u64,
    pub buffer_hard_sleep_us: u64,
    pub stage_inflight_bytes: u64,
    pub remote_upload_inflight_bytes: u64,
    pub stage_ops: u64,
    pub stage_bytes: u64,
    pub stage_us: u64,
    pub stage_failures: u64,
    pub commit_before_stage_ops: u64,
    pub commit_wait_upload_ops: u64,
    pub commit_wait_upload_us: u64,
    pub commit_wait_upload_size_ops: u64,
    pub commit_wait_upload_size_us: u64,
    pub commit_wait_upload_max_unflushed_ops: u64,
    pub commit_wait_upload_max_unflushed_us: u64,
    pub commit_wait_upload_explicit_flush_ops: u64,
    pub commit_wait_upload_explicit_flush_us: u64,
    pub commit_wait_upload_auto_ops: u64,
    pub commit_wait_upload_auto_us: u64,
    pub commit_wait_upload_commit_age_ops: u64,
    pub commit_wait_upload_commit_age_us: u64,
    pub commit_wait_upload_unknown_reason_ops: u64,
    pub commit_wait_upload_unknown_reason_us: u64,
    pub commit_wait_upload_normal_only_ops: u64,
    pub commit_wait_upload_normal_only_us: u64,
    pub commit_wait_upload_cached_only_ops: u64,
    pub commit_wait_upload_cached_only_us: u64,
    pub commit_wait_upload_mixed_origin_ops: u64,
    pub commit_wait_upload_mixed_origin_us: u64,
    pub commit_wait_upload_unknown_origin_ops: u64,
    pub commit_wait_upload_unknown_origin_us: u64,
    pub commit_wait_retry_ops: u64,
    pub commit_wait_retry_us: u64,
    pub flush_wait_ops: u64,
    pub flush_wait_us: u64,
    pub flush_wait_slices: u64,
    pub flush_fragmentation_ops: u64,
    pub flush_fragmentation_slices: u64,
    pub flush_fragmentation_bytes: u64,
    pub flush_fragmentation_cached_sub_block_slices: u64,
    pub flush_fragmentation_cached_sub_block_bytes: u64,
    pub flush_fragmentation_full_block_slices: u64,
    pub flush_fragmentation_full_block_bytes: u64,
    pub slice_create_ops: u64,
    pub slice_reuse_ops: u64,
    pub slice_reject_older_unique_ops: u64,
    pub slice_reject_dispatched_prefix_ops: u64,
    pub freeze_size_ops: u64,
    pub freeze_size_bytes: u64,
    pub freeze_max_unflushed_ops: u64,
    pub freeze_max_unflushed_bytes: u64,
    pub freeze_explicit_flush_ops: u64,
    pub freeze_explicit_flush_bytes: u64,
    pub freeze_auto_ops: u64,
    pub freeze_auto_bytes: u64,
    pub freeze_commit_age_ops: u64,
    pub freeze_commit_age_bytes: u64,
    pub upload_batch_ops: u64,
    pub upload_batch_bytes: u64,
    pub upload_batch_blocks: u64,
    pub upload_batch_single_block_ops: u64,
    pub upload_batch_multi_block_ops: u64,
    pub upload_partial_tail_ops: u64,
    pub upload_partial_tail_size_ops: u64,
    pub upload_partial_tail_max_unflushed_ops: u64,
    pub upload_partial_tail_explicit_flush_ops: u64,
    pub upload_partial_tail_auto_ops: u64,
    pub upload_partial_tail_normal_only_ops: u64,
    pub upload_partial_tail_cached_only_ops: u64,
    pub upload_partial_tail_mixed_origin_ops: u64,
    pub upload_partial_tail_unknown_origin_ops: u64,
    pub upload_partial_tail_auto_age_ops: u64,
    pub upload_partial_tail_auto_idle_ops: u64,
    pub upload_partial_tail_auto_pressure_ops: u64,
    pub upload_partial_tail_auto_too_many_ops: u64,
    pub upload_partial_tail_auto_buffer_high_ops: u64,
    pub upload_partial_tail_auto_flush_duration_ops: u64,
    pub upload_partial_tail_auto_unknown_ops: u64,
    pub upload_partial_tail_auto_normal_only_ops: u64,
    pub upload_partial_tail_auto_cached_only_ops: u64,
    pub upload_partial_tail_auto_mixed_origin_ops: u64,
    pub upload_partial_tail_auto_unknown_origin_ops: u64,
    pub upload_partial_tail_commit_age_ops: u64,
}

impl<B, M> DataWriter<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    ) -> Self {
        Self {
            config,
            backend,
            reader,
            files: DashMap::new(),
            buffer_usage: Arc::new(AtomicU64::new(0)),
            write_back,
            memory_budget: None,
            recent_pending_upload: Arc::new(RecentPendingUploadState::new()),
        }
    }

    pub(crate) fn with_memory_budget(mut self, memory_budget: MemoryBudget) -> Self {
        self.memory_budget = Some(memory_budget);
        self
    }

    pub(crate) fn ensure_file(&self, inode: Arc<Inode>) -> Arc<FileWriter<B, M>> {
        let writer = self
            .files
            .entry(inode.ino() as u64)
            .or_insert_with(|| {
                Arc::new(FileWriter::new_with_memory_budget(
                    inode.clone(),
                    self.config.clone(),
                    self.backend.clone(),
                    self.reader.clone(),
                    self.buffer_usage.clone(),
                    self.write_back.clone(),
                    self.memory_budget.clone(),
                    self.recent_pending_upload.clone(),
                ))
            })
            .clone();
        writer.mark_active();
        writer
    }

    pub(crate) fn recent_pending_upload_bytes(&self) -> u64 {
        self.recent_pending_upload.bytes.load(Ordering::Acquire)
    }

    pub(crate) async fn dirty_breakdown(&self) -> WritebackDirtyBreakdown {
        let writers: Vec<Arc<FileWriter<B, M>>> = self
            .files
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let mut breakdown = WritebackDirtyBreakdown {
            backpressure_soft_sleep_ops: self
                .recent_pending_upload
                .soft_sleep_ops
                .load(Ordering::Relaxed),
            backpressure_soft_sleep_us: self
                .recent_pending_upload
                .soft_sleep_us
                .load(Ordering::Relaxed),
            backpressure_hard_wait_ops: self
                .recent_pending_upload
                .hard_wait_ops
                .load(Ordering::Relaxed),
            backpressure_hard_wait_us: self
                .recent_pending_upload
                .hard_wait_us
                .load(Ordering::Relaxed),
            buffer_soft_sleep_ops: self
                .recent_pending_upload
                .buffer_soft_sleep_ops
                .load(Ordering::Relaxed),
            buffer_soft_sleep_us: self
                .recent_pending_upload
                .buffer_soft_sleep_us
                .load(Ordering::Relaxed),
            buffer_moderate_sleep_ops: self
                .recent_pending_upload
                .buffer_moderate_sleep_ops
                .load(Ordering::Relaxed),
            buffer_moderate_sleep_us: self
                .recent_pending_upload
                .buffer_moderate_sleep_us
                .load(Ordering::Relaxed),
            buffer_hard_sleep_ops: self
                .recent_pending_upload
                .buffer_hard_sleep_ops
                .load(Ordering::Relaxed),
            buffer_hard_sleep_us: self
                .recent_pending_upload
                .buffer_hard_sleep_us
                .load(Ordering::Relaxed),
            stage_inflight_bytes: self
                .recent_pending_upload
                .stage_inflight_bytes
                .load(Ordering::Acquire),
            remote_upload_inflight_bytes: self
                .recent_pending_upload
                .remote_upload_inflight_bytes
                .load(Ordering::Acquire),
            stage_ops: self.recent_pending_upload.stage_ops.load(Ordering::Relaxed),
            stage_bytes: self
                .recent_pending_upload
                .stage_bytes
                .load(Ordering::Relaxed),
            stage_us: self.recent_pending_upload.stage_us.load(Ordering::Relaxed),
            stage_failures: self
                .recent_pending_upload
                .stage_failures
                .load(Ordering::Relaxed),
            commit_before_stage_ops: self
                .recent_pending_upload
                .commit_before_stage_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_ops: self
                .recent_pending_upload
                .commit_wait_upload_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_us: self
                .recent_pending_upload
                .commit_wait_upload_us
                .load(Ordering::Relaxed),
            commit_wait_upload_size_ops: self
                .recent_pending_upload
                .commit_wait_upload_size_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_size_us: self
                .recent_pending_upload
                .commit_wait_upload_size_us
                .load(Ordering::Relaxed),
            commit_wait_upload_max_unflushed_ops: self
                .recent_pending_upload
                .commit_wait_upload_max_unflushed_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_max_unflushed_us: self
                .recent_pending_upload
                .commit_wait_upload_max_unflushed_us
                .load(Ordering::Relaxed),
            commit_wait_upload_explicit_flush_ops: self
                .recent_pending_upload
                .commit_wait_upload_explicit_flush_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_explicit_flush_us: self
                .recent_pending_upload
                .commit_wait_upload_explicit_flush_us
                .load(Ordering::Relaxed),
            commit_wait_upload_auto_ops: self
                .recent_pending_upload
                .commit_wait_upload_auto_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_auto_us: self
                .recent_pending_upload
                .commit_wait_upload_auto_us
                .load(Ordering::Relaxed),
            commit_wait_upload_commit_age_ops: self
                .recent_pending_upload
                .commit_wait_upload_commit_age_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_commit_age_us: self
                .recent_pending_upload
                .commit_wait_upload_commit_age_us
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_reason_ops: self
                .recent_pending_upload
                .commit_wait_upload_unknown_reason_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_reason_us: self
                .recent_pending_upload
                .commit_wait_upload_unknown_reason_us
                .load(Ordering::Relaxed),
            commit_wait_upload_normal_only_ops: self
                .recent_pending_upload
                .commit_wait_upload_normal_only_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_normal_only_us: self
                .recent_pending_upload
                .commit_wait_upload_normal_only_us
                .load(Ordering::Relaxed),
            commit_wait_upload_cached_only_ops: self
                .recent_pending_upload
                .commit_wait_upload_cached_only_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_cached_only_us: self
                .recent_pending_upload
                .commit_wait_upload_cached_only_us
                .load(Ordering::Relaxed),
            commit_wait_upload_mixed_origin_ops: self
                .recent_pending_upload
                .commit_wait_upload_mixed_origin_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_mixed_origin_us: self
                .recent_pending_upload
                .commit_wait_upload_mixed_origin_us
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_origin_ops: self
                .recent_pending_upload
                .commit_wait_upload_unknown_origin_ops
                .load(Ordering::Relaxed),
            commit_wait_upload_unknown_origin_us: self
                .recent_pending_upload
                .commit_wait_upload_unknown_origin_us
                .load(Ordering::Relaxed),
            commit_wait_retry_ops: self
                .recent_pending_upload
                .commit_wait_retry_ops
                .load(Ordering::Relaxed),
            commit_wait_retry_us: self
                .recent_pending_upload
                .commit_wait_retry_us
                .load(Ordering::Relaxed),
            flush_wait_ops: self
                .recent_pending_upload
                .flush_wait_ops
                .load(Ordering::Relaxed),
            flush_wait_us: self
                .recent_pending_upload
                .flush_wait_us
                .load(Ordering::Relaxed),
            flush_wait_slices: self
                .recent_pending_upload
                .flush_wait_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_ops: self
                .recent_pending_upload
                .flush_fragmentation_ops
                .load(Ordering::Relaxed),
            flush_fragmentation_slices: self
                .recent_pending_upload
                .flush_fragmentation_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_bytes: self
                .recent_pending_upload
                .flush_fragmentation_bytes
                .load(Ordering::Relaxed),
            flush_fragmentation_cached_sub_block_slices: self
                .recent_pending_upload
                .flush_fragmentation_cached_sub_block_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_cached_sub_block_bytes: self
                .recent_pending_upload
                .flush_fragmentation_cached_sub_block_bytes
                .load(Ordering::Relaxed),
            flush_fragmentation_full_block_slices: self
                .recent_pending_upload
                .flush_fragmentation_full_block_slices
                .load(Ordering::Relaxed),
            flush_fragmentation_full_block_bytes: self
                .recent_pending_upload
                .flush_fragmentation_full_block_bytes
                .load(Ordering::Relaxed),
            slice_create_ops: self
                .recent_pending_upload
                .slice_create_ops
                .load(Ordering::Relaxed),
            slice_reuse_ops: self
                .recent_pending_upload
                .slice_reuse_ops
                .load(Ordering::Relaxed),
            slice_reject_older_unique_ops: self
                .recent_pending_upload
                .slice_reject_older_unique_ops
                .load(Ordering::Relaxed),
            slice_reject_dispatched_prefix_ops: self
                .recent_pending_upload
                .slice_reject_dispatched_prefix_ops
                .load(Ordering::Relaxed),
            freeze_size_ops: self
                .recent_pending_upload
                .freeze_size_ops
                .load(Ordering::Relaxed),
            freeze_size_bytes: self
                .recent_pending_upload
                .freeze_size_bytes
                .load(Ordering::Relaxed),
            freeze_max_unflushed_ops: self
                .recent_pending_upload
                .freeze_max_unflushed_ops
                .load(Ordering::Relaxed),
            freeze_max_unflushed_bytes: self
                .recent_pending_upload
                .freeze_max_unflushed_bytes
                .load(Ordering::Relaxed),
            freeze_explicit_flush_ops: self
                .recent_pending_upload
                .freeze_explicit_flush_ops
                .load(Ordering::Relaxed),
            freeze_explicit_flush_bytes: self
                .recent_pending_upload
                .freeze_explicit_flush_bytes
                .load(Ordering::Relaxed),
            freeze_auto_ops: self
                .recent_pending_upload
                .freeze_auto_ops
                .load(Ordering::Relaxed),
            freeze_auto_bytes: self
                .recent_pending_upload
                .freeze_auto_bytes
                .load(Ordering::Relaxed),
            freeze_commit_age_ops: self
                .recent_pending_upload
                .freeze_commit_age_ops
                .load(Ordering::Relaxed),
            freeze_commit_age_bytes: self
                .recent_pending_upload
                .freeze_commit_age_bytes
                .load(Ordering::Relaxed),
            upload_batch_ops: self
                .recent_pending_upload
                .upload_batch_ops
                .load(Ordering::Relaxed),
            upload_batch_bytes: self
                .recent_pending_upload
                .upload_batch_bytes
                .load(Ordering::Relaxed),
            upload_batch_blocks: self
                .recent_pending_upload
                .upload_batch_blocks
                .load(Ordering::Relaxed),
            upload_batch_single_block_ops: self
                .recent_pending_upload
                .upload_batch_single_block_ops
                .load(Ordering::Relaxed),
            upload_batch_multi_block_ops: self
                .recent_pending_upload
                .upload_batch_multi_block_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_ops: self
                .recent_pending_upload
                .upload_partial_tail_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_size_ops: self
                .recent_pending_upload
                .upload_partial_tail_size_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_max_unflushed_ops: self
                .recent_pending_upload
                .upload_partial_tail_max_unflushed_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_explicit_flush_ops: self
                .recent_pending_upload
                .upload_partial_tail_explicit_flush_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_normal_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_normal_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_cached_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_cached_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_mixed_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_mixed_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_unknown_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_unknown_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_age_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_age_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_idle_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_idle_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_pressure_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_pressure_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_too_many_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_too_many_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_buffer_high_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_buffer_high_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_flush_duration_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_flush_duration_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_unknown_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_unknown_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_normal_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_normal_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_cached_only_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_cached_only_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_mixed_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_mixed_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_auto_unknown_origin_ops: self
                .recent_pending_upload
                .upload_partial_tail_auto_unknown_origin_ops
                .load(Ordering::Relaxed),
            upload_partial_tail_commit_age_ops: self
                .recent_pending_upload
                .upload_partial_tail_commit_age_ops
                .load(Ordering::Relaxed),
            ..WritebackDirtyBreakdown::default()
        };

        for writer in writers {
            let guard = writer.shared.inner.lock().await;
            for chunk in guard.chunks.values() {
                for slice in &chunk.slices {
                    let state = slice.lock();
                    let bytes = state.data.alloc_bytes();
                    breakdown.live_slices = breakdown.live_slices.saturating_add(1);
                    breakdown.live_bytes = breakdown.live_bytes.saturating_add(bytes);
                    match state.write_origin_kind() {
                        WriteOriginKind::NormalOnly => {
                            breakdown.live_normal_only_slices =
                                breakdown.live_normal_only_slices.saturating_add(1);
                            breakdown.live_normal_only_bytes =
                                breakdown.live_normal_only_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::CachedOnly => {
                            breakdown.live_cached_only_slices =
                                breakdown.live_cached_only_slices.saturating_add(1);
                            breakdown.live_cached_only_bytes =
                                breakdown.live_cached_only_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::Mixed => {
                            breakdown.live_mixed_origin_slices =
                                breakdown.live_mixed_origin_slices.saturating_add(1);
                            breakdown.live_mixed_origin_bytes =
                                breakdown.live_mixed_origin_bytes.saturating_add(bytes);
                        }
                        WriteOriginKind::Unknown => {
                            breakdown.live_unknown_origin_slices =
                                breakdown.live_unknown_origin_slices.saturating_add(1);
                            breakdown.live_unknown_origin_bytes =
                                breakdown.live_unknown_origin_bytes.saturating_add(bytes);
                        }
                    }
                }
                for slice in &chunk.recently_committed {
                    let state = slice.lock();
                    let bytes = state.data.alloc_bytes();
                    if state.upload_complete() {
                        breakdown.recently_committed_uploaded_slices = breakdown
                            .recently_committed_uploaded_slices
                            .saturating_add(1);
                        breakdown.recently_committed_uploaded_bytes = breakdown
                            .recently_committed_uploaded_bytes
                            .saturating_add(bytes);
                    } else {
                        breakdown.recently_committed_pending_upload_slices = breakdown
                            .recently_committed_pending_upload_slices
                            .saturating_add(1);
                        breakdown.recently_committed_pending_upload_bytes = breakdown
                            .recently_committed_pending_upload_bytes
                            .saturating_add(bytes);
                    }
                }
            }
        }

        breakdown
    }

    pub(crate) fn start_flush_background(self: &Arc<Self>) {
        let flush_interval = self.config.flush_all_interval;
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            let mut ticker = interval(flush_interval);
            loop {
                ticker.tick().await;
                let Some(writer) = weak.upgrade() else {
                    return;
                };
                writer.flush_once().await;
            }
        });
    }

    pub(crate) async fn flush_if_exists(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let _ = writer.flush().await;
        }
    }

    pub(crate) async fn has_dirty_state(&self, ino: u64) -> bool {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer.has_pending().await || writer.has_overlay_state().await
        } else {
            false
        }
    }

    pub(crate) async fn overlay_dirty_if_exists(
        &self,
        ino: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        match writer {
            Some(ref writer) if writer.has_overlay_state().await => {
                writer.overlay_dirty(offset, buf).await?;
            }
            #[cfg(not(test))]
            None => {
                // SSD fallback: no in-memory writer exists for this inode.
                // Covers the crash recovery window where dirty data is on
                // SSD but hasn't been re-uploaded yet.
                if let Some(wb) = &self.write_back {
                    let layout = self.config.layout;
                    let spans = split_chunk_spans(layout, offset, buf.len());
                    for span in spans {
                        let cid = chunk_id_for(ino as i64, span.index)?;
                        let chunk_start = span.index * layout.chunk_size;
                        let dst_start = (chunk_start + span.offset - offset) as usize;
                        let dst_end = dst_start + span.len.as_usize();
                        let _ = wb
                            .overlay_dirty_range(
                                ino as i64,
                                cid,
                                span.offset,
                                &mut buf[dst_start..dst_end],
                            )
                            .await;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) async fn snapshot_dirty_overlay_if_exists(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Vec<DirtyOverlayPatch>> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        match writer {
            Some(ref writer) if writer.has_overlay_state().await => {
                writer.snapshot_dirty_overlay(offset, len).await
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(crate) async fn read_dirty_if_fully_covered(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        match writer {
            Some(ref writer) if writer.has_overlay_state().await => {
                writer.read_dirty_if_fully_covered(offset, len).await
            }
            #[cfg(not(test))]
            None => {
                if let Some(wb) = &self.write_back {
                    let layout = self.config.layout;
                    let spans = split_chunk_spans(layout, offset, len);
                    if spans.is_empty() {
                        return Ok(Some(Vec::new()));
                    }

                    let mut out = Vec::with_capacity(len);
                    for span in spans {
                        let cid = chunk_id_for(ino as i64, span.index)?;
                        let Some(mut data) = wb
                            .read_dirty_range_if_fully_covered(
                                ino as i64,
                                cid,
                                span.offset,
                                span.len.as_usize(),
                            )
                            .await?
                        else {
                            return Ok(None);
                        };
                        out.append(&mut data);
                    }
                    Ok(Some(out))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Like `flush_if_exists` but propagates errors.  Used in truncate paths
    /// where a failed flush means data would be silently lost.
    pub(crate) async fn flush_required(&self, ino: u64) -> anyhow::Result<bool> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let start = std::time::Instant::now();
            writer.flush().await?;
            let ms = start.elapsed().as_millis();
            if ms > 100 {
                tracing::info!(ino, elapsed_ms = ms, "flush_required: slow flush");
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn flush_required_snapshot(&self, ino: u64) -> anyhow::Result<bool> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let start = std::time::Instant::now();
            writer.flush_snapshot().await?;
            let ms = start.elapsed().as_millis();
            if ms > 100 {
                tracing::info!(ino, elapsed_ms = ms, "flush_required_snapshot: slow flush");
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn wait_committed_uploads_for_range(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
    ) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer
                .wait_committed_uploads_for_range(offset, len, FLUSH_DEADLINE)
                .await?;
        }
        Ok(())
    }

    /// Truncate/ftruncate runs on the kernel SETATTR path.  A 300s writeback
    /// wait looks like a stuck FUSE request, so use a short, explicit deadline
    /// and log the operation boundary for xfstests-style debugging.
    pub(crate) async fn flush_required_for_truncate(&self, ino: u64) -> anyhow::Result<()> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            let deadline = truncate_flush_deadline();
            let start = Instant::now();
            tracing::debug!(
                ino,
                timeout_ms = deadline.as_millis() as u64,
                "truncate flush_required: start"
            );
            writer.flush_with_deadline(deadline).await.map_err(|err| {
                anyhow::anyhow!(
                    "truncate flush failed after {:?} for ino {ino}: {err}",
                    deadline
                )
            })?;
            tracing::debug!(
                ino,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "truncate flush_required: completed"
            );
        }
        Ok(())
    }

    /// Flush for close: uses a shorter deadline because FUSE already called
    /// flush() before close() for write handles.  This only drains residual
    /// in-flight work that was already kicked off by the preceding flush.
    pub(crate) async fn flush_for_close(&self, ino: u64) -> anyhow::Result<bool> {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer
            && writer.has_pending().await
        {
            writer.flush_with_deadline(CLOSE_FLUSH_DEADLINE).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn clear(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer.clear().await;
        }
    }

    pub(crate) async fn discard(&self, ino: u64) {
        if let Some((_, removed)) = self.files.remove(&ino) {
            removed.shared.inode.bump_data_epoch();
            removed.clear().await;
        }
    }

    pub(crate) async fn release(&self, ino: u64) {
        let writer = self.files.get(&ino).map(|entry| entry.value().clone());
        if let Some(writer) = writer {
            writer.mark_released();
        }
    }

    pub(crate) fn has_file(&self, ino: u64) -> bool {
        self.files.contains_key(&ino)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn flush_once(&self) {
        let writers: Vec<(u64, Arc<FileWriter<B, M>>)> = self
            .files
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        for (ino, writer) in writers {
            let released = writer.shared.released.load(Ordering::Acquire);
            if released && writer.has_pending().await {
                let _ = writer.flush().await;
            }
            if writer.released_cleanup_ready().await
                && let Some((_, removed)) = self.files.remove_if(&ino, |_, current| {
                    Arc::ptr_eq(current, &writer) && current.shared.released.load(Ordering::Acquire)
                })
            {
                removed.clear().await;
            }
        }
    }
}
