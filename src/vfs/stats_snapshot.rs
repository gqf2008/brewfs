//! Point-in-time snapshot of the VFS stats counters.
//!
//! Split out of `stats.rs` (part of the writer/stats decomposition): the
//! snapshot struct, its derived metrics and the `ratio` helper are pure data
//! transformations with no behavior changes.

/// Point-in-time copy of the counters exposed through `.stats`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FsStatsSnapshot {
    pub uptime_seconds: u64,
    pub fuse_read_ops: u64,
    pub fuse_read_bytes: u64,
    pub fuse_read_lat_us: u64,
    pub fuse_write_ops: u64,
    pub fuse_write_bytes: u64,
    pub fuse_write_lat_us: u64,
    pub fuse_lookup_ops: u64,
    pub fuse_lookup_lat_us: u64,
    pub fuse_getattr_ops: u64,
    pub fuse_getattr_lat_us: u64,
    pub fuse_open_ops: u64,
    pub fuse_create_ops: u64,
    pub fuse_unlink_ops: u64,
    pub fuse_readdir_ops: u64,
    pub fuse_flush_ops: u64,
    pub fuse_flush_lat_us: u64,
    pub meta_ops: u64,
    pub meta_lat_us: u64,
    pub meta_txn_ops: u64,
    pub meta_txn_lat_us: u64,
    pub vfs_create_total_ops: u64,
    pub vfs_create_total_lat_us: u64,
    pub vfs_create_meta_ops: u64,
    pub vfs_create_meta_lat_us: u64,
    pub vfs_unlink_total_ops: u64,
    pub vfs_unlink_total_lat_us: u64,
    pub vfs_unlink_lookup_ops: u64,
    pub vfs_unlink_lookup_lat_us: u64,
    pub vfs_unlink_stat_ops: u64,
    pub vfs_unlink_stat_lat_us: u64,
    pub vfs_unlink_meta_ops: u64,
    pub vfs_unlink_meta_lat_us: u64,
    pub vfs_unlink_recent_ops: u64,
    pub vfs_unlink_recent_lat_us: u64,
    pub vfs_setattr_recent_remove_ops: u64,
    pub vfs_setattr_recent_remove_lat_us: u64,
    pub vfs_setattr_recent_get_mut_ops: u64,
    pub vfs_setattr_recent_get_mut_lat_us: u64,
    pub vfs_read_dirty_probe_ops: u64,
    pub vfs_read_dirty_probe_lat_us: u64,
    pub vfs_read_handle_ops: u64,
    pub vfs_read_handle_lat_us: u64,
    pub vfs_read_overlay_ops: u64,
    pub vfs_read_overlay_lat_us: u64,
    pub s3_get_ops: u64,
    pub s3_get_bytes: u64,
    pub s3_get_lat_us: u64,
    pub s3_put_ops: u64,
    pub s3_put_bytes: u64,
    pub s3_put_lat_us: u64,
    pub s3_put_prepare_lat_us: u64,
    pub s3_put_cache_lat_us: u64,
    pub s3_del_ops: u64,
    pub buf_dirty_bytes: u64,
    pub buf_read_bytes: u64,
    pub writeback_live_dirty_bytes: u64,
    pub writeback_live_slices: u64,
    pub writeback_live_normal_only_bytes: u64,
    pub writeback_live_normal_only_slices: u64,
    pub writeback_live_cached_only_bytes: u64,
    pub writeback_live_cached_only_slices: u64,
    pub writeback_live_mixed_origin_bytes: u64,
    pub writeback_live_mixed_origin_slices: u64,
    pub writeback_live_unknown_origin_bytes: u64,
    pub writeback_live_unknown_origin_slices: u64,
    pub writeback_recent_pending_upload_bytes: u64,
    pub writeback_recent_pending_upload_slices: u64,
    pub writeback_recent_uploaded_bytes: u64,
    pub writeback_recent_uploaded_slices: u64,
    pub writeback_backpressure_soft_sleep_ops: u64,
    pub writeback_backpressure_soft_sleep_us: u64,
    pub writeback_backpressure_hard_wait_ops: u64,
    pub writeback_backpressure_hard_wait_us: u64,
    pub writeback_buffer_soft_sleep_ops: u64,
    pub writeback_buffer_soft_sleep_us: u64,
    pub writeback_buffer_moderate_sleep_ops: u64,
    pub writeback_buffer_moderate_sleep_us: u64,
    pub writeback_buffer_hard_sleep_ops: u64,
    pub writeback_buffer_hard_sleep_us: u64,
    pub writeback_stage_inflight_bytes: u64,
    pub writeback_remote_upload_inflight_bytes: u64,
    pub writeback_stage_ops: u64,
    pub writeback_stage_bytes: u64,
    pub writeback_stage_lat_us: u64,
    pub writeback_stage_failures: u64,
    pub writeback_commit_before_stage_ops: u64,
    pub writeback_commit_wait_upload_ops: u64,
    pub writeback_commit_wait_upload_us: u64,
    pub writeback_commit_wait_upload_size_ops: u64,
    pub writeback_commit_wait_upload_size_us: u64,
    pub writeback_commit_wait_upload_max_unflushed_ops: u64,
    pub writeback_commit_wait_upload_max_unflushed_us: u64,
    pub writeback_commit_wait_upload_explicit_flush_ops: u64,
    pub writeback_commit_wait_upload_explicit_flush_us: u64,
    pub writeback_commit_wait_upload_auto_ops: u64,
    pub writeback_commit_wait_upload_auto_us: u64,
    pub writeback_commit_wait_upload_commit_age_ops: u64,
    pub writeback_commit_wait_upload_commit_age_us: u64,
    pub writeback_commit_wait_upload_unknown_reason_ops: u64,
    pub writeback_commit_wait_upload_unknown_reason_us: u64,
    pub writeback_commit_wait_upload_normal_only_ops: u64,
    pub writeback_commit_wait_upload_normal_only_us: u64,
    pub writeback_commit_wait_upload_cached_only_ops: u64,
    pub writeback_commit_wait_upload_cached_only_us: u64,
    pub writeback_commit_wait_upload_mixed_origin_ops: u64,
    pub writeback_commit_wait_upload_mixed_origin_us: u64,
    pub writeback_commit_wait_upload_unknown_origin_ops: u64,
    pub writeback_commit_wait_upload_unknown_origin_us: u64,
    pub writeback_commit_wait_retry_ops: u64,
    pub writeback_commit_wait_retry_us: u64,
    pub writeback_flush_wait_ops: u64,
    pub writeback_flush_wait_us: u64,
    pub writeback_flush_wait_slices: u64,
    pub writeback_flush_fragmentation_ops: u64,
    pub writeback_flush_fragmentation_slices: u64,
    pub writeback_flush_fragmentation_bytes: u64,
    pub writeback_flush_fragmentation_cached_sub_block_slices: u64,
    pub writeback_flush_fragmentation_cached_sub_block_bytes: u64,
    pub writeback_flush_fragmentation_full_block_slices: u64,
    pub writeback_flush_fragmentation_full_block_bytes: u64,
    pub writeback_slice_create_ops: u64,
    pub writeback_slice_reuse_ops: u64,
    pub writeback_slice_reject_older_unique_ops: u64,
    pub writeback_slice_reject_dispatched_prefix_ops: u64,
    pub writeback_freeze_size_ops: u64,
    pub writeback_freeze_size_bytes: u64,
    pub writeback_freeze_max_unflushed_ops: u64,
    pub writeback_freeze_max_unflushed_bytes: u64,
    pub writeback_freeze_explicit_flush_ops: u64,
    pub writeback_freeze_explicit_flush_bytes: u64,
    pub writeback_freeze_auto_ops: u64,
    pub writeback_freeze_auto_bytes: u64,
    pub writeback_freeze_commit_age_ops: u64,
    pub writeback_freeze_commit_age_bytes: u64,
    pub writeback_upload_batch_ops: u64,
    pub writeback_upload_batch_bytes: u64,
    pub writeback_upload_batch_blocks: u64,
    pub writeback_upload_batch_single_block_ops: u64,
    pub writeback_upload_batch_multi_block_ops: u64,
    pub writeback_upload_partial_tail_ops: u64,
    pub writeback_upload_partial_tail_size_ops: u64,
    pub writeback_upload_partial_tail_max_unflushed_ops: u64,
    pub writeback_upload_partial_tail_explicit_flush_ops: u64,
    pub writeback_upload_partial_tail_auto_ops: u64,
    pub writeback_upload_partial_tail_normal_only_ops: u64,
    pub writeback_upload_partial_tail_cached_only_ops: u64,
    pub writeback_upload_partial_tail_mixed_origin_ops: u64,
    pub writeback_upload_partial_tail_unknown_origin_ops: u64,
    pub writeback_upload_partial_tail_auto_age_ops: u64,
    pub writeback_upload_partial_tail_auto_idle_ops: u64,
    pub writeback_upload_partial_tail_auto_pressure_ops: u64,
    pub writeback_upload_partial_tail_auto_too_many_ops: u64,
    pub writeback_upload_partial_tail_auto_buffer_high_ops: u64,
    pub writeback_upload_partial_tail_auto_flush_duration_ops: u64,
    pub writeback_upload_partial_tail_auto_unknown_ops: u64,
    pub writeback_upload_partial_tail_auto_normal_only_ops: u64,
    pub writeback_upload_partial_tail_auto_cached_only_ops: u64,
    pub writeback_upload_partial_tail_auto_mixed_origin_ops: u64,
    pub writeback_upload_partial_tail_auto_unknown_origin_ops: u64,
    pub writeback_upload_partial_tail_commit_age_ops: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub read_block_cache_hits: u64,
    pub read_page_cache_hits: u64,
    pub read_page_cache_misses: u64,
    pub read_range_gets: u64,
    pub read_full_gets: u64,
    pub read_piggyback_full: u64,
    pub read_background_prefetches: u64,
    pub read_background_prefetch_dropped: u64,
    pub meta_stat_cache_hit: u64,
    pub meta_stat_cache_miss: u64,
    pub meta_stat_fresh_store_hit: u64,
    pub meta_lookup_cache_hit: u64,
    pub meta_lookup_cache_miss: u64,
    pub meta_get_slices_cache_hit: u64,
    pub meta_get_slices_cache_miss: u64,
    pub meta_open_fresh_stat: u64,
    pub meta_open_file_cache_hit: u64,
    pub meta_open_file_cache_miss: u64,
    pub meta_lookup_attr_fused_hit: u64,
    pub meta_lookup_attr_fused_miss: u64,
    pub meta_lookup_attr_fused_error: u64,
}

impl FsStatsSnapshot {
    pub fn cache_requests(&self) -> u64 {
        self.cache_hits + self.cache_misses
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        ratio(self.cache_hits, self.cache_requests())
    }

    pub fn avg_fuse_read_lat_us(&self) -> f64 {
        ratio(self.fuse_read_lat_us, self.fuse_read_ops)
    }

    pub fn avg_fuse_write_lat_us(&self) -> f64 {
        ratio(self.fuse_write_lat_us, self.fuse_write_ops)
    }

    pub fn avg_fuse_flush_lat_us(&self) -> f64 {
        ratio(self.fuse_flush_lat_us, self.fuse_flush_ops)
    }

    pub fn avg_s3_get_lat_us(&self) -> f64 {
        ratio(self.s3_get_lat_us, self.s3_get_ops)
    }

    pub fn avg_s3_put_lat_us(&self) -> f64 {
        ratio(self.s3_put_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_prepare_lat_us(&self) -> f64 {
        ratio(self.s3_put_prepare_lat_us, self.s3_put_ops)
    }

    pub fn avg_s3_put_cache_lat_us(&self) -> f64 {
        ratio(self.s3_put_cache_lat_us, self.s3_put_ops)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}
