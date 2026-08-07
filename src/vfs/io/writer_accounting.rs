//! Writeback pending-upload accounting.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): tracks
//! recently-committed-but-not-uploaded bytes, stage/commit latency and
//! backpressure counters. Pure code motion — no behavior changes.

use crate::vfs::io::writer::{AutoFreezeTrigger, SliceFreezeReason};
use crate::vfs::io::writer_upload::WriteOriginKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub(crate) struct RecentPendingUploadState {
    pub(crate) bytes: AtomicU64,
    pub(crate) soft_sleep_ops: AtomicU64,
    pub(crate) soft_sleep_us: AtomicU64,
    pub(crate) hard_wait_ops: AtomicU64,
    pub(crate) hard_wait_us: AtomicU64,
    pub(crate) buffer_soft_sleep_ops: AtomicU64,
    pub(crate) buffer_soft_sleep_us: AtomicU64,
    pub(crate) buffer_moderate_sleep_ops: AtomicU64,
    pub(crate) buffer_moderate_sleep_us: AtomicU64,
    pub(crate) buffer_hard_sleep_ops: AtomicU64,
    pub(crate) buffer_hard_sleep_us: AtomicU64,
    pub(crate) stage_inflight_bytes: Arc<AtomicU64>,
    pub(crate) remote_upload_inflight_bytes: Arc<AtomicU64>,
    pub(crate) stage_ops: AtomicU64,
    pub(crate) stage_bytes: AtomicU64,
    pub(crate) stage_us: AtomicU64,
    pub(crate) stage_failures: AtomicU64,
    pub(crate) commit_before_stage_ops: AtomicU64,
    pub(crate) commit_wait_upload_ops: AtomicU64,
    pub(crate) commit_wait_upload_us: AtomicU64,
    pub(crate) commit_wait_upload_size_ops: AtomicU64,
    pub(crate) commit_wait_upload_size_us: AtomicU64,
    pub(crate) commit_wait_upload_max_unflushed_ops: AtomicU64,
    pub(crate) commit_wait_upload_max_unflushed_us: AtomicU64,
    pub(crate) commit_wait_upload_explicit_flush_ops: AtomicU64,
    pub(crate) commit_wait_upload_explicit_flush_us: AtomicU64,
    pub(crate) commit_wait_upload_auto_ops: AtomicU64,
    pub(crate) commit_wait_upload_auto_us: AtomicU64,
    pub(crate) commit_wait_upload_commit_age_ops: AtomicU64,
    pub(crate) commit_wait_upload_commit_age_us: AtomicU64,
    pub(crate) commit_wait_upload_unknown_reason_ops: AtomicU64,
    pub(crate) commit_wait_upload_unknown_reason_us: AtomicU64,
    pub(crate) commit_wait_upload_normal_only_ops: AtomicU64,
    pub(crate) commit_wait_upload_normal_only_us: AtomicU64,
    pub(crate) commit_wait_upload_cached_only_ops: AtomicU64,
    pub(crate) commit_wait_upload_cached_only_us: AtomicU64,
    pub(crate) commit_wait_upload_mixed_origin_ops: AtomicU64,
    pub(crate) commit_wait_upload_mixed_origin_us: AtomicU64,
    pub(crate) commit_wait_upload_unknown_origin_ops: AtomicU64,
    pub(crate) commit_wait_upload_unknown_origin_us: AtomicU64,
    pub(crate) commit_wait_retry_ops: AtomicU64,
    pub(crate) commit_wait_retry_us: AtomicU64,
    pub(crate) flush_wait_ops: AtomicU64,
    pub(crate) flush_wait_us: AtomicU64,
    pub(crate) flush_wait_slices: AtomicU64,
    pub(crate) flush_fragmentation_ops: AtomicU64,
    pub(crate) flush_fragmentation_slices: AtomicU64,
    pub(crate) flush_fragmentation_bytes: AtomicU64,
    pub(crate) flush_fragmentation_cached_sub_block_slices: AtomicU64,
    pub(crate) flush_fragmentation_cached_sub_block_bytes: AtomicU64,
    pub(crate) flush_fragmentation_full_block_slices: AtomicU64,
    pub(crate) flush_fragmentation_full_block_bytes: AtomicU64,
    pub(crate) slice_create_ops: AtomicU64,
    pub(crate) slice_reuse_ops: AtomicU64,
    pub(crate) slice_reject_older_unique_ops: AtomicU64,
    pub(crate) slice_reject_dispatched_prefix_ops: AtomicU64,
    pub(crate) freeze_size_ops: AtomicU64,
    pub(crate) freeze_size_bytes: AtomicU64,
    pub(crate) freeze_max_unflushed_ops: AtomicU64,
    pub(crate) freeze_max_unflushed_bytes: AtomicU64,
    pub(crate) freeze_explicit_flush_ops: AtomicU64,
    pub(crate) freeze_explicit_flush_bytes: AtomicU64,
    pub(crate) freeze_auto_ops: AtomicU64,
    pub(crate) freeze_auto_bytes: AtomicU64,
    pub(crate) freeze_commit_age_ops: AtomicU64,
    pub(crate) freeze_commit_age_bytes: AtomicU64,
    pub(crate) upload_batch_ops: AtomicU64,
    pub(crate) upload_batch_bytes: AtomicU64,
    pub(crate) upload_batch_blocks: AtomicU64,
    pub(crate) upload_batch_single_block_ops: AtomicU64,
    pub(crate) upload_batch_multi_block_ops: AtomicU64,
    pub(crate) upload_partial_tail_ops: AtomicU64,
    pub(crate) upload_partial_tail_size_ops: AtomicU64,
    pub(crate) upload_partial_tail_max_unflushed_ops: AtomicU64,
    pub(crate) upload_partial_tail_explicit_flush_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_ops: AtomicU64,
    pub(crate) upload_partial_tail_normal_only_ops: AtomicU64,
    pub(crate) upload_partial_tail_cached_only_ops: AtomicU64,
    pub(crate) upload_partial_tail_mixed_origin_ops: AtomicU64,
    pub(crate) upload_partial_tail_unknown_origin_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_age_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_idle_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_pressure_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_too_many_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_buffer_high_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_flush_duration_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_unknown_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_normal_only_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_cached_only_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_mixed_origin_ops: AtomicU64,
    pub(crate) upload_partial_tail_auto_unknown_origin_ops: AtomicU64,
    pub(crate) upload_partial_tail_commit_age_ops: AtomicU64,
    pub(crate) notify: Notify,
}

pub(crate) struct InflightBytesGuard {
    pub(crate) counter: Arc<AtomicU64>,
    pub(crate) bytes: u64,
}

impl Drop for InflightBytesGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl RecentPendingUploadState {
    pub(crate) fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            soft_sleep_ops: AtomicU64::new(0),
            soft_sleep_us: AtomicU64::new(0),
            hard_wait_ops: AtomicU64::new(0),
            hard_wait_us: AtomicU64::new(0),
            buffer_soft_sleep_ops: AtomicU64::new(0),
            buffer_soft_sleep_us: AtomicU64::new(0),
            buffer_moderate_sleep_ops: AtomicU64::new(0),
            buffer_moderate_sleep_us: AtomicU64::new(0),
            buffer_hard_sleep_ops: AtomicU64::new(0),
            buffer_hard_sleep_us: AtomicU64::new(0),
            stage_inflight_bytes: Arc::new(AtomicU64::new(0)),
            remote_upload_inflight_bytes: Arc::new(AtomicU64::new(0)),
            stage_ops: AtomicU64::new(0),
            stage_bytes: AtomicU64::new(0),
            stage_us: AtomicU64::new(0),
            stage_failures: AtomicU64::new(0),
            commit_before_stage_ops: AtomicU64::new(0),
            commit_wait_upload_ops: AtomicU64::new(0),
            commit_wait_upload_us: AtomicU64::new(0),
            commit_wait_upload_size_ops: AtomicU64::new(0),
            commit_wait_upload_size_us: AtomicU64::new(0),
            commit_wait_upload_max_unflushed_ops: AtomicU64::new(0),
            commit_wait_upload_max_unflushed_us: AtomicU64::new(0),
            commit_wait_upload_explicit_flush_ops: AtomicU64::new(0),
            commit_wait_upload_explicit_flush_us: AtomicU64::new(0),
            commit_wait_upload_auto_ops: AtomicU64::new(0),
            commit_wait_upload_auto_us: AtomicU64::new(0),
            commit_wait_upload_commit_age_ops: AtomicU64::new(0),
            commit_wait_upload_commit_age_us: AtomicU64::new(0),
            commit_wait_upload_unknown_reason_ops: AtomicU64::new(0),
            commit_wait_upload_unknown_reason_us: AtomicU64::new(0),
            commit_wait_upload_normal_only_ops: AtomicU64::new(0),
            commit_wait_upload_normal_only_us: AtomicU64::new(0),
            commit_wait_upload_cached_only_ops: AtomicU64::new(0),
            commit_wait_upload_cached_only_us: AtomicU64::new(0),
            commit_wait_upload_mixed_origin_ops: AtomicU64::new(0),
            commit_wait_upload_mixed_origin_us: AtomicU64::new(0),
            commit_wait_upload_unknown_origin_ops: AtomicU64::new(0),
            commit_wait_upload_unknown_origin_us: AtomicU64::new(0),
            commit_wait_retry_ops: AtomicU64::new(0),
            commit_wait_retry_us: AtomicU64::new(0),
            flush_wait_ops: AtomicU64::new(0),
            flush_wait_us: AtomicU64::new(0),
            flush_wait_slices: AtomicU64::new(0),
            flush_fragmentation_ops: AtomicU64::new(0),
            flush_fragmentation_slices: AtomicU64::new(0),
            flush_fragmentation_bytes: AtomicU64::new(0),
            flush_fragmentation_cached_sub_block_slices: AtomicU64::new(0),
            flush_fragmentation_cached_sub_block_bytes: AtomicU64::new(0),
            flush_fragmentation_full_block_slices: AtomicU64::new(0),
            flush_fragmentation_full_block_bytes: AtomicU64::new(0),
            slice_create_ops: AtomicU64::new(0),
            slice_reuse_ops: AtomicU64::new(0),
            slice_reject_older_unique_ops: AtomicU64::new(0),
            slice_reject_dispatched_prefix_ops: AtomicU64::new(0),
            freeze_size_ops: AtomicU64::new(0),
            freeze_size_bytes: AtomicU64::new(0),
            freeze_max_unflushed_ops: AtomicU64::new(0),
            freeze_max_unflushed_bytes: AtomicU64::new(0),
            freeze_explicit_flush_ops: AtomicU64::new(0),
            freeze_explicit_flush_bytes: AtomicU64::new(0),
            freeze_auto_ops: AtomicU64::new(0),
            freeze_auto_bytes: AtomicU64::new(0),
            freeze_commit_age_ops: AtomicU64::new(0),
            freeze_commit_age_bytes: AtomicU64::new(0),
            upload_batch_ops: AtomicU64::new(0),
            upload_batch_bytes: AtomicU64::new(0),
            upload_batch_blocks: AtomicU64::new(0),
            upload_batch_single_block_ops: AtomicU64::new(0),
            upload_batch_multi_block_ops: AtomicU64::new(0),
            upload_partial_tail_ops: AtomicU64::new(0),
            upload_partial_tail_size_ops: AtomicU64::new(0),
            upload_partial_tail_max_unflushed_ops: AtomicU64::new(0),
            upload_partial_tail_explicit_flush_ops: AtomicU64::new(0),
            upload_partial_tail_auto_ops: AtomicU64::new(0),
            upload_partial_tail_normal_only_ops: AtomicU64::new(0),
            upload_partial_tail_cached_only_ops: AtomicU64::new(0),
            upload_partial_tail_mixed_origin_ops: AtomicU64::new(0),
            upload_partial_tail_unknown_origin_ops: AtomicU64::new(0),
            upload_partial_tail_auto_age_ops: AtomicU64::new(0),
            upload_partial_tail_auto_idle_ops: AtomicU64::new(0),
            upload_partial_tail_auto_pressure_ops: AtomicU64::new(0),
            upload_partial_tail_auto_too_many_ops: AtomicU64::new(0),
            upload_partial_tail_auto_buffer_high_ops: AtomicU64::new(0),
            upload_partial_tail_auto_flush_duration_ops: AtomicU64::new(0),
            upload_partial_tail_auto_unknown_ops: AtomicU64::new(0),
            upload_partial_tail_auto_normal_only_ops: AtomicU64::new(0),
            upload_partial_tail_auto_cached_only_ops: AtomicU64::new(0),
            upload_partial_tail_auto_mixed_origin_ops: AtomicU64::new(0),
            upload_partial_tail_auto_unknown_origin_ops: AtomicU64::new(0),
            upload_partial_tail_commit_age_ops: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub(crate) fn record_soft_sleep(&self, duration: Duration) {
        self.soft_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.soft_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_hard_wait(&self, duration: Duration) {
        self.hard_wait_ops.fetch_add(1, Ordering::Relaxed);
        self.hard_wait_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_buffer_soft_sleep(&self, duration: Duration) {
        self.buffer_soft_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.buffer_soft_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_buffer_moderate_sleep(&self, duration: Duration) {
        self.buffer_moderate_sleep_ops
            .fetch_add(1, Ordering::Relaxed);
        self.buffer_moderate_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_buffer_hard_sleep(&self, duration: Duration) {
        self.buffer_hard_sleep_ops.fetch_add(1, Ordering::Relaxed);
        self.buffer_hard_sleep_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_stage_start(&self, bytes: u64) -> Instant {
        self.stage_inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
        Instant::now()
    }

    pub(crate) fn record_stage_finish(&self, start: Instant, bytes: u64, success: bool) {
        self.stage_inflight_bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.stage_ops.fetch_add(1, Ordering::Relaxed);
        self.stage_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.stage_us.fetch_add(
            start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if !success {
            self.stage_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn track_remote_upload_inflight(&self, bytes: u64) -> InflightBytesGuard {
        self.remote_upload_inflight_bytes
            .fetch_add(bytes, Ordering::AcqRel);
        InflightBytesGuard {
            counter: self.remote_upload_inflight_bytes.clone(),
            bytes,
        }
    }

    pub(crate) fn record_commit_before_stage(&self) {
        self.commit_before_stage_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_commit_wait_upload(
        &self,
        duration: Duration,
        reason: Option<SliceFreezeReason>,
        origin: WriteOriginKind,
    ) {
        let elapsed_us = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.commit_wait_upload_ops.fetch_add(1, Ordering::Relaxed);
        self.commit_wait_upload_us
            .fetch_add(elapsed_us, Ordering::Relaxed);

        let (reason_ops, reason_us) = match reason {
            Some(SliceFreezeReason::SizeOrChunkEnd) => (
                &self.commit_wait_upload_size_ops,
                &self.commit_wait_upload_size_us,
            ),
            Some(SliceFreezeReason::MaxUnflushed) => (
                &self.commit_wait_upload_max_unflushed_ops,
                &self.commit_wait_upload_max_unflushed_us,
            ),
            Some(SliceFreezeReason::ExplicitFlush) => (
                &self.commit_wait_upload_explicit_flush_ops,
                &self.commit_wait_upload_explicit_flush_us,
            ),
            Some(SliceFreezeReason::Auto) => (
                &self.commit_wait_upload_auto_ops,
                &self.commit_wait_upload_auto_us,
            ),
            Some(SliceFreezeReason::CommitAgeSafety) => (
                &self.commit_wait_upload_commit_age_ops,
                &self.commit_wait_upload_commit_age_us,
            ),
            None => (
                &self.commit_wait_upload_unknown_reason_ops,
                &self.commit_wait_upload_unknown_reason_us,
            ),
        };
        reason_ops.fetch_add(1, Ordering::Relaxed);
        reason_us.fetch_add(elapsed_us, Ordering::Relaxed);

        let (origin_ops, origin_us) = match origin {
            WriteOriginKind::NormalOnly => (
                &self.commit_wait_upload_normal_only_ops,
                &self.commit_wait_upload_normal_only_us,
            ),
            WriteOriginKind::CachedOnly => (
                &self.commit_wait_upload_cached_only_ops,
                &self.commit_wait_upload_cached_only_us,
            ),
            WriteOriginKind::Mixed => (
                &self.commit_wait_upload_mixed_origin_ops,
                &self.commit_wait_upload_mixed_origin_us,
            ),
            WriteOriginKind::Unknown => (
                &self.commit_wait_upload_unknown_origin_ops,
                &self.commit_wait_upload_unknown_origin_us,
            ),
        };
        origin_ops.fetch_add(1, Ordering::Relaxed);
        origin_us.fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_commit_wait_retry(&self, duration: Duration) {
        self.commit_wait_retry_ops.fetch_add(1, Ordering::Relaxed);
        self.commit_wait_retry_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_flush_wait(&self, duration: Duration, slices: u64) {
        self.flush_wait_ops.fetch_add(1, Ordering::Relaxed);
        self.flush_wait_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        self.flush_wait_slices.fetch_add(slices, Ordering::Relaxed);
    }

    pub(crate) fn record_flush_fragmentation(
        &self,
        slices: u64,
        bytes: u64,
        cached_sub_block_slices: u64,
        cached_sub_block_bytes: u64,
        full_block_slices: u64,
        full_block_bytes: u64,
    ) {
        self.flush_fragmentation_ops.fetch_add(1, Ordering::Relaxed);
        self.flush_fragmentation_slices
            .fetch_add(slices, Ordering::Relaxed);
        self.flush_fragmentation_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.flush_fragmentation_cached_sub_block_slices
            .fetch_add(cached_sub_block_slices, Ordering::Relaxed);
        self.flush_fragmentation_cached_sub_block_bytes
            .fetch_add(cached_sub_block_bytes, Ordering::Relaxed);
        self.flush_fragmentation_full_block_slices
            .fetch_add(full_block_slices, Ordering::Relaxed);
        self.flush_fragmentation_full_block_bytes
            .fetch_add(full_block_bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_slice_create(&self) {
        self.slice_create_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_slice_reuse(&self) {
        self.slice_reuse_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_slice_reject_older_unique(&self) {
        self.slice_reject_older_unique_ops
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_slice_reject_dispatched_prefix(&self) {
        self.slice_reject_dispatched_prefix_ops
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_freeze(&self, reason: SliceFreezeReason, bytes: u64) {
        let (ops, total_bytes) = match reason {
            SliceFreezeReason::SizeOrChunkEnd => (&self.freeze_size_ops, &self.freeze_size_bytes),
            SliceFreezeReason::MaxUnflushed => (
                &self.freeze_max_unflushed_ops,
                &self.freeze_max_unflushed_bytes,
            ),
            SliceFreezeReason::ExplicitFlush => (
                &self.freeze_explicit_flush_ops,
                &self.freeze_explicit_flush_bytes,
            ),
            SliceFreezeReason::Auto => (&self.freeze_auto_ops, &self.freeze_auto_bytes),
            SliceFreezeReason::CommitAgeSafety => {
                (&self.freeze_commit_age_ops, &self.freeze_commit_age_bytes)
            }
        };
        ops.fetch_add(1, Ordering::Relaxed);
        total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_upload_batch(
        &self,
        bytes: u64,
        blocks: u64,
        partial_tail: bool,
        partial_tail_reason: Option<SliceFreezeReason>,
        partial_tail_auto_trigger: Option<AutoFreezeTrigger>,
        partial_tail_origin: WriteOriginKind,
    ) {
        self.upload_batch_ops.fetch_add(1, Ordering::Relaxed);
        self.upload_batch_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.upload_batch_blocks
            .fetch_add(blocks, Ordering::Relaxed);
        if blocks <= 1 {
            self.upload_batch_single_block_ops
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.upload_batch_multi_block_ops
                .fetch_add(1, Ordering::Relaxed);
        }
        if partial_tail {
            self.upload_partial_tail_ops.fetch_add(1, Ordering::Relaxed);
            let origin_counter = match partial_tail_origin {
                WriteOriginKind::NormalOnly => &self.upload_partial_tail_normal_only_ops,
                WriteOriginKind::CachedOnly => &self.upload_partial_tail_cached_only_ops,
                WriteOriginKind::Mixed => &self.upload_partial_tail_mixed_origin_ops,
                WriteOriginKind::Unknown => &self.upload_partial_tail_unknown_origin_ops,
            };
            origin_counter.fetch_add(1, Ordering::Relaxed);
            if let Some(reason) = partial_tail_reason {
                let counter = match reason {
                    SliceFreezeReason::SizeOrChunkEnd => &self.upload_partial_tail_size_ops,
                    SliceFreezeReason::MaxUnflushed => &self.upload_partial_tail_max_unflushed_ops,
                    SliceFreezeReason::ExplicitFlush => {
                        &self.upload_partial_tail_explicit_flush_ops
                    }
                    SliceFreezeReason::Auto => &self.upload_partial_tail_auto_ops,
                    SliceFreezeReason::CommitAgeSafety => &self.upload_partial_tail_commit_age_ops,
                };
                counter.fetch_add(1, Ordering::Relaxed);
                if matches!(reason, SliceFreezeReason::Auto) {
                    let auto_counter = match partial_tail_auto_trigger {
                        Some(AutoFreezeTrigger::Age) => &self.upload_partial_tail_auto_age_ops,
                        Some(AutoFreezeTrigger::Idle) => &self.upload_partial_tail_auto_idle_ops,
                        Some(AutoFreezeTrigger::Pressure) => {
                            &self.upload_partial_tail_auto_pressure_ops
                        }
                        Some(AutoFreezeTrigger::TooMany) => {
                            &self.upload_partial_tail_auto_too_many_ops
                        }
                        Some(AutoFreezeTrigger::BufferHigh) => {
                            &self.upload_partial_tail_auto_buffer_high_ops
                        }
                        Some(AutoFreezeTrigger::FlushDuration) => {
                            &self.upload_partial_tail_auto_flush_duration_ops
                        }
                        None => &self.upload_partial_tail_auto_unknown_ops,
                    };
                    auto_counter.fetch_add(1, Ordering::Relaxed);
                    let auto_origin_counter = match partial_tail_origin {
                        WriteOriginKind::NormalOnly => {
                            &self.upload_partial_tail_auto_normal_only_ops
                        }
                        WriteOriginKind::CachedOnly => {
                            &self.upload_partial_tail_auto_cached_only_ops
                        }
                        WriteOriginKind::Mixed => &self.upload_partial_tail_auto_mixed_origin_ops,
                        WriteOriginKind::Unknown => {
                            &self.upload_partial_tail_auto_unknown_origin_ops
                        }
                    };
                    auto_origin_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
