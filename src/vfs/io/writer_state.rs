//! Slice/chunk state definitions for the write path.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): the slice
//! state machine, chunk state and the cached-coalesce candidate helper. Pure
//! code motion — no behavior changes.

use crate::utils::UsageGuard;
use crate::vfs::cache::page::CacheSlice;
use crate::vfs::cache::page::WriteAction as PageWriteAction;
use crate::vfs::config::WriteConfig;
use crate::vfs::io::writer::{AutoFreezeTrigger, SliceFreezeReason, SliceStatus, WriteOrigin};
use crate::vfs::io::writer_upload::WriteOriginKind;
use crate::vfs::memory::{MemoryBudget, MemoryConsumer, MemoryUsageGuard};
use parking_lot::Mutex as ParkingMutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;
use tokio::sync::Notify;

pub(crate) struct SliceState {
    pub(crate) state: SliceStatus,
    /// ID of the chunk it belongs to.
    pub(crate) chunk_id: u64,
    /// ID of this slice (assigned on flush).
    pub(crate) slice_id: Option<u64>,
    /// Offset relative to the chunk start.
    pub(crate) offset: u64,
    /// Contiguous byte boundary of confirmed uploads (all blocks below this
    /// offset have completed their S3 PUT).
    pub(crate) uploaded: u64,
    /// Highest block index that has been dispatched for upload.  Blocks in
    /// `[uploaded/block_size .. dispatched_end)` are in-flight.
    pub(crate) dispatched_end: usize,
    /// Bitmask of completed block indices.  Bit N is set when block N's upload
    /// has been confirmed.  Max 64 blocks per slice (256MB/4MB = 64).
    pub(crate) block_done: u64,
    /// Number of upload batches currently in-flight.
    pub(crate) in_flight: u32,
    /// Set to `true` when a pipeline upload task has been spawned for this
    /// slice to prevent duplicate top-level upload tasks.
    pub(crate) upload_task_active: bool,
    /// True while this slice's bytes are included in pending-upload accounting.
    pub(crate) recent_pending_accounted: bool,
    /// Stable logical byte count added to pending-upload accounting.
    pub(crate) recent_pending_accounted_bytes: u64,
    /// Bytes successfully persisted to the local writeback stage for this
    /// slice.
    pub(crate) writeback_persisted_bytes: u64,
    /// True while a task is writing the recoverable local dirty record.
    pub(crate) writeback_record_sealing: bool,
    /// Commit-before-upload may publish metadata only after staged data covers
    /// the whole sealed slice and this recoverable dirty record is sealed.
    pub(crate) writeback_record_sealed: bool,
    pub(crate) data: CacheSlice,
    pub(crate) usage: UsageGuard,
    pub(crate) memory_usage: Option<MemoryUsageGuard>,
    /// Error occurred at background thread.
    pub(crate) err: Option<String>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) started: Instant,
    pub(crate) last_mod: Instant,
    /// Inode data_epoch captured when this slice is frozen.
    /// If the inode epoch advances (truncate/setattr), stale commits are skipped.
    pub(crate) frozen_epoch: u64,
    /// Set to `true` when a meta.write() has been initiated (or completed)
    /// to prevent both try_commit and commit_chunk from writing the same slice.
    pub(crate) meta_write_started: bool,
    /// Reason this slice was sealed. Used to attribute partial-tail uploads.
    pub(crate) freeze_reason: Option<SliceFreezeReason>,
    /// More precise trigger for auto freezes. Used only when `freeze_reason` is `Auto`.
    pub(crate) auto_freeze_trigger: Option<AutoFreezeTrigger>,
    /// FUSE request unique id that created this slice, used to order overlapping
    /// slices for correct commit sequencing (lower unique = older data = commit first).
    pub(crate) creation_unique: u64,
    /// Writer-local monotonic order used for dirty overlay and commit order.
    /// Unlike `creation_unique`, this is assigned to every write path, so
    /// ordinary buffered writes and FUSE_WRITE_CACHE writes share one ordering
    /// domain inside this writer.
    pub(crate) write_order: u64,
    /// Highest FUSE unique that has written to this slice.  A write with
    /// unique < max_write_unique is rejected (must go to its own slice) to
    /// prevent an older concurrent write from overwriting newer data.
    pub(crate) max_write_unique: u64,
    /// Bitmask of write paths that have successfully appended to this slice.
    pub(crate) write_origin_mask: u8,
}

impl SliceState {
    pub(crate) fn new(
        chunk_id: u64,
        offset: u64,
        config: Arc<WriteConfig>,
        usage: Arc<AtomicU64>,
        memory_budget: Option<MemoryBudget>,
        creation_unique: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            state: SliceStatus::Writable,
            slice_id: None,
            chunk_id,
            offset,
            uploaded: 0,
            dispatched_end: 0,
            block_done: 0,
            in_flight: 0,
            upload_task_active: false,
            recent_pending_accounted: false,
            recent_pending_accounted_bytes: 0,
            writeback_persisted_bytes: 0,
            writeback_record_sealing: false,
            writeback_record_sealed: false,
            data: CacheSlice::new(config),
            usage: UsageGuard::new(usage),
            memory_usage: memory_budget
                .map(|budget| MemoryUsageGuard::new(budget, MemoryConsumer::Writer)),
            err: None,
            notify: Arc::new(Notify::new()),
            started: now,
            last_mod: now,
            frozen_epoch: 0,
            meta_write_started: false,
            freeze_reason: None,
            auto_freeze_trigger: None,
            creation_unique,
            write_order: creation_unique,
            max_write_unique: creation_unique,
            write_origin_mask: 0,
        }
    }

    pub(crate) fn update_usage(&mut self, bytes: u64) {
        self.usage.update_bytes(bytes);
        if let Some(memory_usage) = &mut self.memory_usage {
            memory_usage.update_bytes(bytes);
        }
    }

    pub(crate) fn record_writeback_persisted_bytes(&mut self, bytes: u64) {
        self.writeback_persisted_bytes = self.writeback_persisted_bytes.saturating_add(bytes);
    }

    pub(crate) fn writeback_data_fully_persisted(&self) -> bool {
        self.writeback_persisted_bytes >= self.data.len()
    }

    pub(crate) fn writeback_fully_persisted(&self) -> bool {
        self.writeback_data_fully_persisted() && self.writeback_record_sealed
    }

    pub(crate) fn can_write(
        &self,
        offset: u64,
        len: usize,
        allow_gap_from: Option<u64>,
    ) -> Option<PageWriteAction> {
        if !matches!(self.state, SliceStatus::Writable) || offset < self.offset {
            return None;
        }

        let size = self.data.block_size();
        let pending_start = self.dispatched_end as u64 * size as u64;

        let off_to_slice = offset - self.offset;

        // Uploaded/dispatched blocks cannot be overlapped.
        if off_to_slice < pending_start.max(self.uploaded) {
            return None;
        }

        let allow_gap = allow_gap_from
            .is_some_and(|safe_from| self.offset.saturating_add(self.data.len()) >= safe_from);

        // For this function, the `offset` is relative to the chunk start,
        // whereas in `CacheSlice.append`, it is relative to the slice start.
        self.data
            .can_write_with_gap(off_to_slice, len as u64, allow_gap)
    }

    #[tracing::instrument(level = "trace", skip(self, buf), fields(len = buf.len()))]
    pub(crate) fn write(
        &mut self,
        offset: u64,
        buf: &[u8],
        action: PageWriteAction,
        origin: WriteOrigin,
    ) -> anyhow::Result<()> {
        self.data.write(offset - self.offset, buf, action)?;
        self.write_origin_mask |= origin.mask();
        self.last_mod = Instant::now();
        Ok(())
    }

    pub(crate) fn write_origin_kind(&self) -> WriteOriginKind {
        match (
            self.write_origin_mask & WriteOrigin::Normal.mask() != 0,
            self.write_origin_mask & WriteOrigin::Cached.mask() != 0,
        ) {
            (false, false) => WriteOriginKind::Unknown,
            (true, false) => WriteOriginKind::NormalOnly,
            (false, true) => WriteOriginKind::CachedOnly,
            (true, true) => WriteOriginKind::Mixed,
        }
    }

    pub(crate) fn data_range(&self) -> Option<(u64, u64)> {
        let len = self.data.len();
        if len == 0 {
            return None;
        }

        Some((self.offset, self.offset + len))
    }

    pub(crate) fn can_coalesce_cached_writable(&self) -> bool {
        matches!(self.state, SliceStatus::Writable)
            && self.slice_id.is_none()
            && self.uploaded == 0
            && self.dispatched_end == 0
            && self.block_done == 0
            && self.in_flight == 0
            && !self.upload_task_active
            && !self.recent_pending_accounted
            && self.writeback_persisted_bytes == 0
            && !self.writeback_record_sealing
            && !self.writeback_record_sealed
            && self.err.is_none()
            && self.frozen_epoch == 0
            && !self.meta_write_started
            && self.freeze_reason.is_none()
            && self.auto_freeze_trigger.is_none()
            && self.data.len() > 0
            && self.data.contiguous_written_len() == self.data.len()
            && matches!(self.write_origin_kind(), WriteOriginKind::CachedOnly)
    }

    pub(crate) fn write_range_has_written_overlap(&self, offset: u64, len: usize) -> bool {
        let end = offset.saturating_add(len as u64);
        if offset >= end {
            return false;
        }

        let slice_start = self.offset;
        let slice_end = self.offset.saturating_add(self.data.len());
        let overlap_start = offset.max(slice_start);
        let overlap_end = end.min(slice_end);
        if overlap_start >= overlap_end {
            return false;
        }
        self.data
            .has_written_overlap(overlap_start - slice_start, overlap_end - overlap_start)
    }

    pub(crate) fn can_overlay_read(&self) -> bool {
        match self.state {
            SliceStatus::Writable
            | SliceStatus::Readonly
            | SliceStatus::Uploaded
            | SliceStatus::Failed => true,
            // Commit-before-upload exposes metadata before object upload
            // completion; overlay only needs to cover that upload gap.
            SliceStatus::Committed => !self.upload_complete(),
        }
    }

    pub fn has_idle_block(&self) -> bool {
        let size = self.data.block_size();
        // Use dispatched_end as the frontier — blocks below this are either
        // uploaded or in-flight.
        let pending_end = (self.dispatched_end as u64 * size as u64).max(self.uploaded);

        let ready_len = self.upload_ready_len();
        let remaining = ready_len.saturating_sub(pending_end);

        if matches!(
            self.state,
            SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
        ) {
            remaining > 0
        } else {
            remaining >= size as u64
        }
    }

    pub fn idx_need_upload(&self) -> (usize, usize) {
        let size = self.data.block_size() as u64;
        // Start from dispatched_end (not uploaded) — pipeline allows dispatching
        // new blocks while earlier ones are still in-flight.
        let start = self.dispatched_end;
        let ready_len = self.upload_ready_len();
        let end = if matches!(
            self.state,
            SliceStatus::Readonly | SliceStatus::Failed | SliceStatus::Committed
        ) {
            if ready_len == 0 {
                0
            } else {
                ready_len.div_ceil(size) as usize
            }
        } else {
            (ready_len / size) as usize
        };

        (start, end)
    }

    pub(crate) fn upload_ready_len(&self) -> u64 {
        if matches!(self.state, SliceStatus::Writable) {
            self.data.contiguous_written_len()
        } else {
            self.data.len()
        }
    }

    pub(crate) fn upload_complete(&self) -> bool {
        self.in_flight == 0 && !self.has_idle_block()
    }
}

pub(crate) struct ChunkState {
    /// ID of the chunk.
    pub(crate) chunk_id: u64,
    pub(crate) slices: VecDeque<Arc<ParkingMutex<SliceState>>>,
    /// Committed slices kept for a grace period so that overlay_dirty can
    /// still serve their data after commit_chunk marks them Committed.
    pub(crate) recently_committed: VecDeque<Arc<ParkingMutex<SliceState>>>,
    pub(crate) commit_started: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct CachedCoalesceCandidate {
    pub(crate) index: usize,
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl ChunkState {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            chunk_id: id,
            slices: VecDeque::new(),
            recently_committed: VecDeque::new(),
            commit_started: false,
        }
    }
}
