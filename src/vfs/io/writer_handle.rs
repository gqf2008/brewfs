//! Per-chunk/per-slice write machinery shared by the writer.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): slice
//! handles, chunk handles, the shared writer state and its inner chunk map.
//! Pure code motion — no behavior changes.

use crate::chunk::BlockStore;
use crate::chunk::SliceDesc;
use crate::meta::MetaLayer;
use crate::utils::NumCastExt;
use crate::vfs::Inode;
use crate::vfs::backend::Backend;
use crate::vfs::cache::config::WriteBackMode;
use crate::vfs::cache::page::WriteAction as PageWriteAction;
use crate::vfs::cache::write_back::WriteBackCache;
use crate::vfs::config::WriteConfig;
use crate::vfs::io::reader::DataReader;
use crate::vfs::io::writer::{
    AutoFreezeTrigger, ChunkState, SliceFreezeReason, SliceState, SliceStatus, WriteOrigin,
};
use crate::vfs::io::writer_accounting::RecentPendingUploadState;
use crate::vfs::io::writer_cached_write::CachedWriteRange;
use crate::vfs::io::writer_cached_write::*;
use crate::vfs::io::writer_policy::*;
use crate::vfs::io::writer_upload::UploadPlan;
use crate::vfs::io::writer_upload::WriteOriginKind;
use crate::vfs::memory::MemoryBudget;
use parking_lot::Mutex as ParkingMutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, Notify, Semaphore};
use tracing::warn;

pub(crate) struct SliceHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    pub(crate) slice: &'a Arc<ParkingMutex<SliceState>>,
    pub(crate) shared: &'a Shared<B, M>,
}

impl<'a, B, M> SliceHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    pub(crate) fn with_mut<T>(&self, f: impl FnOnce(&mut SliceState) -> T) -> T {
        let mut guard = self.slice.lock();
        f(&mut guard)
    }

    pub(crate) fn with_ref<T>(&self, f: impl FnOnce(&SliceState) -> T) -> T {
        let guard = self.slice.lock();
        f(&guard)
    }

    fn can_write(
        &self,
        offset: u64,
        len: usize,
        allow_gap_from: Option<u64>,
    ) -> Option<PageWriteAction> {
        self.with_ref(|s| s.can_write(offset, len, allow_gap_from))
    }

    fn has_written_overlap(&self, offset: u64, len: usize) -> bool {
        self.with_ref(|s| s.write_range_has_written_overlap(offset, len))
    }

    fn rejects_dispatched_prefix(&self, offset: u64, len: usize) -> bool {
        self.with_ref(|s| {
            if !matches!(s.state, SliceStatus::Writable) || offset < s.offset {
                return false;
            }
            let write_end = offset.saturating_add(len as u64);
            let slice_end = s.offset.saturating_add(s.data.len());
            if offset >= slice_end || s.offset >= write_end {
                return false;
            }

            let block_size = s.data.block_size();
            let pending_start = s.dispatched_end as u64 * block_size as u64;
            let prefix_end = pending_start.max(s.uploaded);
            offset - s.offset < prefix_end
        })
    }

    fn can_freeze_for_max_unflushed(&self) -> bool {
        self.with_ref(|s| matches!(s.state, SliceStatus::Writable) && s.has_idle_block())
    }

    fn try_write(
        &self,
        offset: u64,
        buf: &[u8],
        origin: WriteOrigin,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<bool> {
        let wrote = self.with_mut(|s| match s.can_write(offset, buf.len(), allow_gap_from) {
            Some(action) => {
                s.write(offset, buf, action, origin)?;
                s.update_usage(s.data.alloc_bytes());
                Ok::<bool, anyhow::Error>(true)
            }
            None => Ok::<bool, anyhow::Error>(false),
        })?;

        Ok(wrote)
    }

    pub(crate) fn freeze_with_reason(&self, reason: SliceFreezeReason) -> bool {
        self.freeze_with_reason_and_auto_trigger(reason, None)
    }

    pub(crate) fn freeze_auto_with_trigger(&self, trigger: AutoFreezeTrigger) -> bool {
        self.freeze_with_reason_and_auto_trigger(SliceFreezeReason::Auto, Some(trigger))
    }

    fn freeze_with_reason_and_auto_trigger(
        &self,
        reason: SliceFreezeReason,
        auto_trigger: Option<AutoFreezeTrigger>,
    ) -> bool {
        let mut empty_committed = false;
        let mut frozen_bytes = 0u64;
        let froze = self.with_mut(|s| {
            if !matches!(s.state, SliceStatus::Writable) {
                return false;
            }

            if s.data.len() == 0 {
                s.state = SliceStatus::Committed;
                s.err = None;
                s.notify.notify_waiters();
                empty_committed = true;
                return false;
            }

            frozen_bytes = s.data.len();
            s.state = SliceStatus::Readonly;
            s.frozen_epoch = self.shared.inode.data_epoch();
            s.freeze_reason = Some(reason);
            s.auto_freeze_trigger = if matches!(reason, SliceFreezeReason::Auto) {
                auto_trigger
            } else {
                None
            };
            s.data.freeze();

            if s.in_flight == 0 && !s.has_idle_block() {
                s.state = SliceStatus::Uploaded;
                s.err = None;
                s.notify.notify_waiters();
            }
            true
        });

        if empty_committed {
            self.shared.flush_notify.notify_waiters();
        }
        if froze {
            self.shared
                .recent_pending_upload
                .record_freeze(reason, frozen_bytes);
        }

        froze
    }

    /// Called when a block range `[start_idx, end_idx)` finishes uploading.
    /// Marks the blocks done in the bitmask and advances `uploaded` through
    /// the highest contiguous completed boundary.
    pub(crate) fn advance_upload_range(&self, start_idx: usize, end_idx: usize, _len: u64) {
        let made_progress = self.with_mut(|s| {
            let previous_uploaded = s.uploaded;
            let previous_state = s.state;

            // Mark completed blocks in bitmask (guard against overflow).
            for idx in start_idx..end_idx {
                if idx < 64 {
                    s.block_done |= 1u64 << idx;
                }
            }
            s.in_flight = s.in_flight.saturating_sub(1);

            // Advance `uploaded` through contiguous completed blocks.
            let block_size = s.data.block_size() as u64;
            let mut current_block = (s.uploaded / block_size) as usize;
            while current_block < 64 && (s.block_done >> current_block) & 1 == 1 {
                current_block += 1;
            }
            let new_uploaded = current_block as u64 * block_size;
            if new_uploaded > s.uploaded {
                s.uploaded = new_uploaded;
            }

            // Keep uploaded pages resident until metadata commit removes the slice.
            s.update_usage(s.data.alloc_bytes());

            if matches!(s.state, SliceStatus::Readonly | SliceStatus::Failed)
                && s.in_flight == 0
                && !s.has_idle_block()
            {
                s.state = SliceStatus::Uploaded;
                s.err = None;
            }
            self.clear_recent_pending_if_complete(s);
            s.notify.notify_waiters();
            s.uploaded != previous_uploaded || s.state != previous_state
        });
        if made_progress {
            self.shared.flush_notify.notify_waiters();
        }
    }

    /// Legacy advance_upload for backward compatibility with single-batch callers.
    fn advance_upload(&self, len: u64, _uploaded_blocks: Vec<usize>) {
        let made_progress = self.with_mut(|s| {
            let previous_uploaded = s.uploaded;
            let previous_state = s.state;

            s.in_flight = s.in_flight.saturating_sub(1);
            s.uploaded += len;

            // Mark all blocks up to uploaded as done.
            let block_size = s.data.block_size() as u64;
            let done_end = (s.uploaded / block_size) as usize;
            for idx in 0..done_end.min(64) {
                s.block_done |= 1u64 << idx;
            }

            s.update_usage(s.data.alloc_bytes());

            if matches!(s.state, SliceStatus::Readonly | SliceStatus::Failed)
                && s.in_flight == 0
                && !s.has_idle_block()
            {
                s.state = SliceStatus::Uploaded;
                s.err = None;
            }
            self.clear_recent_pending_if_complete(s);
            s.notify.notify_waiters();
            s.uploaded != previous_uploaded || s.state != previous_state
        });
        if made_progress {
            self.shared.flush_notify.notify_waiters();
        }
    }

    fn clear_recent_pending_accounting(&self, s: &mut SliceState) {
        if !s.recent_pending_accounted {
            return;
        }
        let bytes = s.recent_pending_accounted_bytes;
        s.recent_pending_accounted = false;
        s.recent_pending_accounted_bytes = 0;
        if bytes > 0 {
            self.shared
                .recent_pending_upload
                .bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
        self.shared.recent_pending_upload.notify.notify_waiters();
    }

    fn clear_recent_pending_if_complete(&self, s: &mut SliceState) {
        if s.upload_complete() {
            self.clear_recent_pending_accounting(s);
        }
    }

    fn should_freeze(&self) -> bool {
        self.with_ref(|s| {
            let ready_len = s.upload_ready_len();
            if ready_len < s.data.len() {
                return false;
            }
            let end = s.offset + ready_len;
            let freeze_min = self.shared.config.freeze_min_bytes;
            end >= self.shared.config.layout.chunk_size || ready_len >= freeze_min
        })
    }

    pub(crate) fn runtime_snapshot(&self) -> SliceRuntime {
        self.with_ref(|s| SliceRuntime {
            status: s.state,
            err: s.err.clone(),
            frozen: !matches!(s.state, SliceStatus::Writable),
            freeze_reason: s.freeze_reason,
            write_origin: s.write_origin_kind(),
            started: s.started,
            notify: s.notify.clone(),
        })
    }

    pub(crate) fn can_continue_upload(&self) -> bool {
        self.with_ref(|s| s.has_idle_block() && !s.upload_task_active)
    }

    // Mark data upload failure and wake commit waiters.
    pub(crate) fn mark_failed(&self, err: anyhow::Error) {
        let message = err.to_string();
        self.with_mut(|s| {
            s.state = SliceStatus::Failed;
            s.in_flight = 0;
            s.upload_task_active = false;
            s.err = Some(message.clone());
            self.clear_recent_pending_accounting(s);

            s.notify.notify_waiters();
        });
        self.shared.record_writeback_error(message);
        self.shared.flush_notify.notify_waiters();
    }

    pub(crate) fn mark_writeback_persisted(&self, bytes: u64) {
        self.with_mut(|s| {
            s.record_writeback_persisted_bytes(bytes);
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }

    pub(crate) fn claim_writeback_record_seal(
        &self,
    ) -> Option<(crate::vfs::cache::keys::DirtySliceKey, u64, u64)> {
        let ino = self.shared.inode.ino();
        self.with_mut(|s| {
            if matches!(s.state, SliceStatus::Writable)
                || !s.writeback_data_fully_persisted()
                || s.writeback_record_sealed
                || s.writeback_record_sealing
            {
                return None;
            }

            let slice_id = s.slice_id?;
            s.writeback_record_sealing = true;
            Some((
                crate::vfs::cache::keys::DirtySliceKey {
                    ino,
                    chunk_id: s.chunk_id,
                    local_seq: slice_id,
                    epoch: 0,
                },
                s.offset,
                s.data.len(),
            ))
        })
    }

    pub(crate) fn mark_writeback_record_sealed(&self) {
        self.with_mut(|s| {
            s.writeback_record_sealing = false;
            s.writeback_record_sealed = true;
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }

    pub(crate) fn prepare_upload(&self) -> anyhow::Result<Option<UploadPlan>> {
        self.with_mut(|s| {
            if matches!(s.state, SliceStatus::Failed) {
                return Ok(None);
            }
            if !s.has_idle_block() {
                return Ok(None);
            }

            let (start, end) = s.idx_need_upload();

            if end <= start {
                return Ok(None);
            }

            s.data.freeze_blocks(start, end);

            let data = s.data.collect_pages(start, end)?;
            let data_len = data
                .iter()
                .flat_map(|(_, chunks)| chunks.iter())
                .map(|chunk| chunk.len() as u64)
                .sum();
            // Pipeline: track dispatched frontier and in-flight count instead
            // of a single exclusive `uploading` range.
            s.dispatched_end = end;
            s.in_flight += 1;

            // Compute the byte offset for this batch based on block indices.
            let block_size = s.data.block_size() as u64;
            let batch_offset = start as u64 * block_size;
            let partial_tail = matches!(
                s.state,
                SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
            ) && s.data.len() % block_size != 0
                && end as u64 * block_size >= s.data.len();
            let partial_tail_reason = s.freeze_reason;
            let partial_tail_auto_trigger = s.auto_freeze_trigger;
            let write_origin = s.write_origin_kind();
            self.shared.recent_pending_upload.record_upload_batch(
                data_len,
                (end - start) as u64,
                partial_tail,
                partial_tail_reason,
                partial_tail_auto_trigger,
                write_origin,
            );

            Ok(Some(UploadPlan {
                chunk_id: s.chunk_id,
                data,
                slice_id: s.slice_id,
                uploaded: batch_offset,
                write_origin,
            }))
        })
    }

    pub(crate) fn set_slice_id(&self, id: u64) {
        self.with_mut(|s| {
            if s.slice_id.is_none() {
                s.slice_id = Some(id);
            }
        })
    }

    pub(crate) fn desc_for_commit(&self) -> Option<SliceDesc> {
        self.with_ref(|s| {
            let length = s.data.len();
            let slice_id = match s.slice_id {
                Some(id) => id,
                None => return None,
            };
            if length == 0 {
                return None;
            }
            Some(SliceDesc {
                slice_id,
                chunk_id: s.chunk_id,
                offset: s.offset,
                length,
            })
        })
    }

    pub(crate) fn mark_committed(&self) {
        self.with_mut(|s| {
            s.state = SliceStatus::Committed;
            s.notify.notify_waiters();
        });
        self.shared.flush_notify.notify_waiters();
    }
}

impl<'a, B, M> SliceHandle<'a, B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    /// Attempt to commit a fully-uploaded slice immediately.
    /// Called from the upload task when all blocks have been transferred,
    /// so that flush() callers do not wait on the commit_chunk poll loop.
    ///
    /// To preserve metadata ordering (later slices must appear after earlier
    /// ones), we only commit if this slice is at the front of the chunk's
    /// deque — i.e. all preceding slices have already been popped.
    pub(crate) async fn try_commit(&self) {
        if !self.runtime_snapshot().can_commit() {
            return;
        }

        let slice_epoch = self.slice.lock().frozen_epoch;
        if slice_epoch != 0 && self.shared.inode.data_epoch() != slice_epoch {
            tracing::warn!(
                ino = self.shared.inode.ino(),
                slice_epoch,
                current_epoch = self.shared.inode.data_epoch(),
                "skipping stale try_commit after inode epoch change"
            );
            self.mark_committed();
            return;
        }

        // Claim the right to write metadata for this slice.  Both try_commit
        // (from upload task) and commit_chunk (from commit loop) race here;
        // the first to set `meta_write_started` wins and the other skips.
        let chunk_id = {
            let mut s = self.slice.lock();
            if s.meta_write_started {
                return;
            }
            s.meta_write_started = true;
            s.chunk_id
        };

        // Only commit if we are the front slice.  Out-of-order metadata
        // appends would let an older slice win over a newer one in the
        // "last writer wins" resolution used by readers.
        {
            let guard = self.shared.inner.lock().await;
            let is_front = guard
                .chunks
                .get(&chunk_id)
                .and_then(|c| c.slices.front())
                .is_some_and(|front| Arc::ptr_eq(front, self.slice));
            if !is_front {
                // Revert the flag so commit_chunk can handle it when it
                // becomes the front slice.
                self.slice.lock().meta_write_started = false;
                return;
            }
        }

        let desc = match self.desc_for_commit() {
            Some(d) => d,
            None => return,
        };

        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(desc.chunk_id);
        let new_size =
            chunk_index * self.shared.config.layout.chunk_size + desc.offset + desc.length;

        let mut attempts = 0u32;
        loop {
            match self
                .shared
                .backend
                .meta()
                .write(ino, desc.chunk_id, desc, new_size)
                .await
            {
                Ok(()) => {
                    self.shared.inode.set_committed_size(new_size);
                    self.shared
                        .inode
                        .add_estimated_allocated_bytes(desc.length.as_usize() as u64);

                    // Invalidate reader cache BEFORE marking committed so that
                    // when the flush loop sees the Committed state, the reader
                    // already has fresh data.  Otherwise flush can return while
                    // the reader still serves stale cached pages.
                    let file_offset =
                        chunk_index * self.shared.config.layout.chunk_size + desc.offset;
                    let _ = self
                        .shared
                        .reader
                        .invalidate(ino as u64, file_offset, desc.length.as_usize())
                        .await;

                    self.mark_committed();

                    if let Some(wb) = &self.shared.write_back {
                        let key = crate::vfs::cache::keys::DirtySliceKey {
                            ino,
                            chunk_id: desc.chunk_id,
                            local_seq: desc.slice_id,
                            epoch: 0,
                        };
                        let _ = wb.remove(&key).await;
                    }
                    return;
                }
                Err(err) => {
                    let retryable = should_retry_meta_write(&err);
                    attempts = attempts.saturating_add(1);
                    if retryable && attempts < COMMIT_META_MAX_RETRIES {
                        tokio::time::sleep(commit_retry_backoff(attempts)).await;
                        continue;
                    }
                    if retryable {
                        tracing::debug!(
                            ino,
                            chunk_id = desc.chunk_id,
                            slice_id = desc.slice_id,
                            attempts,
                            error = ?err,
                            "try_commit exhausted retries, deferring to commit_chunk"
                        );
                        // Reset so commit_chunk can pick this up.
                        self.slice.lock().meta_write_started = false;
                    } else {
                        self.mark_failed(anyhow::anyhow!(
                            "try_commit failed for ino {ino}, chunk {}, slice {}: {err}",
                            desc.chunk_id,
                            desc.slice_id
                        ));
                        if let Some(wb) = &self.shared.write_back {
                            let key = crate::vfs::cache::keys::DirtySliceKey {
                                ino,
                                chunk_id: desc.chunk_id,
                                local_seq: desc.slice_id,
                                epoch: 0,
                            };
                            let _ = wb.remove(&key).await;
                        }
                    }
                    return;
                }
            }
        }
    }
}

/// A snapshot of a slice, allowing us to check slice status without lock.
pub(crate) struct SliceRuntime {
    pub(crate) status: SliceStatus,
    pub(crate) err: Option<String>,
    pub(crate) frozen: bool,
    pub(crate) freeze_reason: Option<SliceFreezeReason>,
    pub(crate) write_origin: WriteOriginKind,
    pub(crate) started: Instant,
    pub(crate) notify: Arc<Notify>,
}

impl SliceRuntime {
    pub(crate) fn upload_done(&self) -> bool {
        matches!(self.status, SliceStatus::Uploaded | SliceStatus::Committed)
    }

    pub(crate) fn can_commit(&self) -> bool {
        matches!(self.status, SliceStatus::Uploaded)
    }
}

pub(crate) struct WriteAction {
    pub(crate) start_commit: bool,
    pub(crate) flush: Vec<Arc<ParkingMutex<SliceState>>>,
}

pub(crate) struct DirtyOverlayPatch {
    pub(crate) offset: usize,
    pub(crate) data: Vec<u8>,
}

pub(crate) struct ChunkHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    chunk_id: u64,
    inner: &'a mut Inner,
    shared: &'a Shared<B, M>,
}

impl<'a, B, M> ChunkHandle<'a, B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    /// Find or create the next slice which can be written.
    /// A slice is append-only.
    fn find_slice_or_create(
        &mut self,
        offset: u64,
        len: usize,
        creation_unique: u64,
        write_order: u64,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<(Arc<ParkingMutex<SliceState>>, WriteAction)> {
        let (chunk_id, mut slices, recently_committed) = {
            let chunk = self
                .inner
                .chunks
                .get_mut(&self.chunk_id)
                .ok_or_else(|| anyhow::anyhow!("invalid chunk id"))?;
            let slices = std::mem::take(&mut chunk.slices);
            let recently_committed = chunk.recently_committed.iter().cloned().collect::<Vec<_>>();
            (chunk.chunk_id, slices, recently_committed)
        };

        anyhow::ensure!(
            offset + len as u64 <= self.shared.config.layout.chunk_size,
            "A write operation cannot exceed the chunk size"
        );

        let mut found: Option<Arc<ParkingMutex<SliceState>>> = None;
        let mut flush = Vec::new();
        let mut rejected_dispatched_prefix = false;
        let mut newer_written_overlap = false;
        for (idx, slice) in slices.iter().rev().enumerate() {
            let handle = SliceHandle {
                slice,
                shared: self.shared,
            };

            if let Some(write_action) = handle.can_write(offset, len, allow_gap_from) {
                if matches!(write_action, PageWriteAction::Overlap)
                    && handle.has_written_overlap(offset, len)
                {
                    newer_written_overlap = true;
                    continue;
                }
                let candidate_order = slice.lock().write_order;
                // Do not place a newer overlapping write into an older slice
                // when a later slice already covers part of the same range.
                // Dirty overlay is slice-ordered, so reusing the older slice
                // would let the later slice overwrite this write on readback.
                let newer_recently_committed_overlap = recently_committed.iter().any(|recent| {
                    let state = recent.lock();
                    let recent_order = state.write_order;
                    (candidate_order == 0 || recent_order == 0 || recent_order > candidate_order)
                        && state.write_range_has_written_overlap(offset, len)
                });
                if newer_written_overlap || newer_recently_committed_overlap {
                    continue;
                }
                // Reject reuse if this write is older than the newest write
                // already in the slice.  Without this check, an older concurrent
                // FUSE write (lower unique) processed after a newer one could
                // overwrite the newer data in the overlapping region.  Sparse
                // gap append tracks actual user-written ranges, so an older
                // request may still fill a zero gap left by a newer tail write.
                if creation_unique != 0 {
                    let max_u = slice.lock().max_write_unique;
                    if max_u != 0
                        && creation_unique < max_u
                        && handle.has_written_overlap(offset, len)
                    {
                        self.shared
                            .recent_pending_upload
                            .record_slice_reject_older_unique();
                        continue;
                    }
                }
                found = Some(slice.clone());
                break;
            } else if handle.rejects_dispatched_prefix(offset, len) {
                rejected_dispatched_prefix = true;
            }
            if handle.has_written_overlap(offset, len) {
                newer_written_overlap = true;
            }

            // Prevent slices from remaining unflushed for too long.
            if idx > MAX_UNFLUSHED_SLICES
                && handle.can_freeze_for_max_unflushed()
                && handle.freeze_with_reason(SliceFreezeReason::MaxUnflushed)
            {
                flush.push(slice.clone());
            }
        }

        let slice = match found {
            Some(slice) => {
                self.shared.recent_pending_upload.record_slice_reuse();
                // Update max_write_unique so future older writes won't reuse this slice.
                if creation_unique != 0 {
                    let mut s = slice.lock();
                    if creation_unique > s.max_write_unique {
                        s.max_write_unique = creation_unique;
                    }
                }
                slice
            }
            None => {
                let mut state = SliceState::new(
                    chunk_id,
                    offset,
                    self.shared.config.clone(),
                    self.shared.buffer_usage.clone(),
                    self.shared.memory_budget.clone(),
                    creation_unique,
                );
                state.write_order = write_order;
                let slice = Arc::new(ParkingMutex::new(state));
                self.shared.recent_pending_upload.record_slice_create();
                if rejected_dispatched_prefix {
                    self.shared
                        .recent_pending_upload
                        .record_slice_reject_dispatched_prefix();
                }
                // Insert in sorted position by writer-local order so that slices
                // committed in FIFO (front-first) order reflect the kernel's
                // temporal write ordering. This prevents a race where concurrent
                // FUSE request processing reorders overlapping writes.
                // write_order=0 means the ordering is unknown; append to back to
                // preserve original FIFO behavior for legacy/recovered slices.
                let insert_pos = if write_order == 0 {
                    slices.len()
                } else {
                    slices
                        .iter()
                        .position(|s| {
                            let existing = s.lock().write_order;
                            existing != 0 && existing > write_order
                        })
                        .unwrap_or(slices.len())
                };
                slices.insert(insert_pos, slice.clone());
                slice
            }
        };

        let chunk = self
            .inner
            .chunks
            .get_mut(&self.chunk_id)
            .ok_or_else(|| anyhow::anyhow!("invalid chunk id"))?;

        // This `slices` includes the newly created slice.
        chunk.slices = slices;
        let mut start_commit = false;

        // Enable the background commit thread if there is already a slice.
        if !chunk.commit_started && !chunk.slices.is_empty() {
            chunk.commit_started = true;
            start_commit = true;
        }

        Ok((
            slice,
            WriteAction {
                start_commit,
                flush,
            },
        ))
    }

    /// Append data to a writable slice. If the slice reaches chunk end, freeze + flush it.
    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    pub(crate) fn write_at(
        &mut self,
        offset: u64,
        buf: &[u8],
        creation_unique: u64,
        write_order: u64,
        origin: WriteOrigin,
        allow_gap_from: Option<u64>,
    ) -> anyhow::Result<WriteAction> {
        let mut start_commit = false;
        let mut flush = Vec::new();

        // There is a potential race condition in the time window between `find_slice_or_create` and `try_append`.
        // `find_slice_or_create` checks and returns a slice that can be appended, but after it selects the slice,
        // it releases the lock. `auto_flush` and `commit_chunk` can freeze a slice without holding the lock,
        // so when handle trying appending buf, the slice may have become readonly. This is highly unlikely to happen,
        // therefore, it is ok to retry briefly, but not forever.
        let mut failed_cnt = 0;

        loop {
            let (slice, action) = self.find_slice_or_create(
                offset,
                buf.len(),
                creation_unique,
                write_order,
                allow_gap_from,
            )?;
            start_commit |= action.start_commit;
            flush.extend(action.flush);

            let handle = SliceHandle {
                slice: &slice,
                shared: self.shared,
            };

            if handle.try_write(offset, buf, origin, allow_gap_from)? {
                let should_flush = if matches!(
                    self.shared.config.writeback_mode,
                    WriteBackMode::CommitBeforeUpload
                ) || matches!(origin, WriteOrigin::Normal)
                {
                    handle.should_freeze()
                        && handle.freeze_with_reason(SliceFreezeReason::SizeOrChunkEnd)
                } else {
                    handle.can_continue_upload()
                        || handle.should_freeze()
                            && handle.freeze_with_reason(SliceFreezeReason::SizeOrChunkEnd)
                };
                if should_flush {
                    flush.push(slice);
                }

                return Ok(WriteAction {
                    start_commit,
                    flush,
                });
            }

            failed_cnt += 1;
            if failed_cnt == 10 {
                warn!(
                    chunk_id = self.chunk_id,
                    offset,
                    len = buf.len(),
                    "write_at retried {failed_cnt} times due to concurrent slice freezing"
                );
            }
            if failed_cnt >= WRITE_SLICE_MAX_RETRIES {
                return Err(anyhow::anyhow!(
                    "write_at failed to append after {failed_cnt} retries due to concurrent slice freezing"
                ));
            }
            std::thread::yield_now();
        }
    }
}

pub(crate) struct Shared<B, M> {
    pub(crate) inode: Arc<Inode>,
    pub(crate) config: Arc<WriteConfig>,
    pub(crate) buffer_usage: Arc<AtomicU64>,
    pub(crate) inner: Mutex<Inner>,
    /// Notify signal to wait write.
    pub(crate) write_notify: Notify,
    /// Notify signal to wait flush.
    pub(crate) flush_notify: Notify,
    pub(crate) backend: Arc<Backend<B, M>>,
    pub(crate) reader: Arc<DataReader<B, M>>,
    /// Local SSD write-back cache for persisting frozen slices before upload.
    pub(crate) write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
    pub(crate) memory_budget: Option<MemoryBudget>,
    /// Monotonically incremented for dirty-slice ordering.
    pub(crate) write_order: AtomicU64,
    /// Monotonically incremented on each write.  Used together with
    /// `last_flushed_gen` to let `has_pending()` avoid a lock acquisition
    /// when no new data has arrived since the last successful flush.
    pub(crate) write_gen: AtomicU64,
    /// Snapshot of `write_gen` taken after a flush completes successfully.
    pub(crate) last_flushed_gen: AtomicU64,
    /// Bytes in recently committed slices whose object upload is not complete.
    pub(crate) recent_pending_upload: Arc<RecentPendingUploadState>,
    /// First durable writeback error observed by background upload/commit.
    pub(crate) writeback_error: ParkingMutex<Option<String>>,
    /// Per-writer limit for concurrently dispatched block uploads.
    pub(crate) upload_limit: Arc<Semaphore>,
    /// The last user handle was released, but writeback overlay may still be
    /// needed until committed slices finish uploading and age out.
    pub(crate) released: AtomicBool,
}

impl<B, M> Shared<B, M>
where
    B: BlockStore,
    M: MetaLayer,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inode: Arc<Inode>,
        config: Arc<WriteConfig>,
        backend: Arc<Backend<B, M>>,
        reader: Arc<DataReader<B, M>>,
        buffer_usage: Arc<AtomicU64>,
        write_back: Option<Arc<crate::vfs::cache::write_back::FsWriteBackCache>>,
        memory_budget: Option<MemoryBudget>,
        recent_pending_upload: Arc<RecentPendingUploadState>,
    ) -> Self {
        let upload_concurrency = config.upload_concurrency.max(1);
        Self {
            inode,
            config,
            buffer_usage,
            inner: Mutex::new(Inner {
                flush_waiting: 0,
                write_waiting: 0,
                chunks: BTreeMap::default(),
                cached_write_watermarks: BTreeMap::default(),
                sparse_fallocate_ranges: BTreeMap::default(),
            }),
            write_notify: Notify::new(),
            flush_notify: Notify::new(),
            backend,
            reader,
            write_back,
            memory_budget,
            write_order: AtomicU64::new(0),
            write_gen: AtomicU64::new(0),
            last_flushed_gen: AtomicU64::new(0),
            recent_pending_upload,
            writeback_error: ParkingMutex::new(None),
            upload_limit: Arc::new(Semaphore::new(upload_concurrency)),
            released: AtomicBool::new(false),
        }
    }

    fn record_writeback_error(&self, err: String) {
        let mut guard = self.writeback_error.lock();
        if guard.is_none() {
            *guard = Some(err);
        }
        self.flush_notify.notify_waiters();
        self.recent_pending_upload.notify.notify_waiters();
    }

    pub(crate) fn writeback_error(&self) -> Option<String> {
        self.writeback_error.lock().clone()
    }

    pub(crate) fn writeback_result(&self) -> anyhow::Result<()> {
        match self.writeback_error() {
            Some(err) => Err(anyhow::anyhow!("writeback failed: {err}")),
            None => Ok(()),
        }
    }

    pub(crate) fn next_write_order(&self) -> u64 {
        self.write_order
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }
}

pub(crate) struct Inner {
    pub(crate) flush_waiting: u16,
    pub(crate) write_waiting: u16,
    pub(crate) chunks: BTreeMap<u64, ChunkState>,
    pub(crate) cached_write_watermarks: BTreeMap<u64, Vec<CachedWriteRange>>,
    pub(crate) sparse_fallocate_ranges: BTreeMap<u64, Vec<(u64, u64)>>,
}

impl Inner {
    pub(crate) fn chunk_handle<'a, B, M>(
        &'a mut self,
        shared: &'a Shared<B, M>,
        chunk_id: u64,
    ) -> ChunkHandle<'a, B, M>
    where
        B: BlockStore,
        M: MetaLayer,
    {
        ChunkHandle {
            chunk_id,
            inner: self,
            shared,
        }
    }

    pub(crate) fn get_or_create_chunk(&mut self, cid: u64) -> u64 {
        if self.chunks.contains_key(&cid) {
            return cid;
        }
        self.chunks.insert(cid, ChunkState::new(cid));
        cid
    }

    pub(crate) fn allowed_cached_write_ranges(
        &mut self,
        chunk_id: u64,
        offset: u64,
        len: usize,
        creation_unique: u64,
    ) -> Vec<(u64, usize)> {
        if len == 0 {
            return Vec::new();
        }
        if creation_unique == 0 {
            return vec![(offset, len)];
        }

        let end = offset + len as u64;
        let ranges = self.cached_write_watermarks.entry(chunk_id).or_default();
        let mut allowed = crate::utils::Intervals::new(offset, end);
        for range in ranges.iter() {
            if range.max_unique > creation_unique {
                allowed.cut(range.start.max(offset), range.end.min(end));
            }
        }
        let allowed = allowed
            .collect()
            .into_iter()
            .map(|(start, end)| (start, (end - start).as_usize()))
            .collect();

        record_cached_write_watermark(ranges, offset, end, creation_unique);
        allowed
    }

    pub(crate) fn record_sparse_fallocate_range(&mut self, chunk_id: u64, start: u64, end: u64) {
        if start >= end {
            return;
        }
        let ranges = self.sparse_fallocate_ranges.entry(chunk_id).or_default();
        ranges.push((start, end));
        ranges.sort_unstable_by_key(|range| (range.0, range.1));

        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges.drain(..) {
            if start >= end {
                continue;
            }
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
                continue;
            }
            merged.push((start, end));
        }
        *ranges = merged;
    }

    pub(crate) fn sparse_fallocate_subranges(
        &self,
        chunk_id: u64,
        start: u64,
        len: usize,
    ) -> Vec<(u64, u64)> {
        let end = start.saturating_add(len as u64);
        self.sparse_fallocate_ranges
            .get(&chunk_id)
            .map(|ranges| {
                ranges
                    .iter()
                    .filter_map(|&(range_start, range_end)| {
                        let overlap_start = range_start.max(start);
                        let overlap_end = range_end.min(end);
                        (overlap_start < overlap_end).then_some((overlap_start, overlap_end))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn chunk_ids(&self) -> Vec<u64> {
        self.chunks.keys().copied().collect()
    }

    pub(crate) fn has_chunks(&self) -> bool {
        !self.chunks.is_empty()
    }
}
