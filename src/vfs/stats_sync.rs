//! Counter-sync helpers for `FsStats`.
//!
//! Split out of `stats.rs` (part of the writer/stats decomposition): these
//! methods copy subsystem counters into atomics. Pure code motion — no
//! behavior changes.

use crate::vfs::stats::{FsStats, ORD};

impl FsStats {
    pub fn sync_cache_counters(&self, hits: u64, misses: u64) {
        self.cache_hits.store(hits, ORD);
        self.cache_misses.store(misses, ORD);
    }

    pub fn sync_buffer_bytes(&self, dirty_bytes: u64, read_bytes: u64) {
        self.buf_dirty_bytes.store(dirty_bytes, ORD);
        self.buf_read_bytes.store(read_bytes, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_object_store_metrics(
        &self,
        get_ops: u64,
        get_bytes: u64,
        get_lat_us: u64,
        put_ops: u64,
        put_bytes: u64,
        put_lat_us: u64,
        put_prepare_lat_us: u64,
        put_cache_lat_us: u64,
        del_ops: u64,
    ) {
        self.s3_get_ops.store(get_ops, ORD);
        self.s3_get_bytes.store(get_bytes, ORD);
        self.s3_get_lat_us.store(get_lat_us, ORD);
        self.s3_put_ops.store(put_ops, ORD);
        self.s3_put_bytes.store(put_bytes, ORD);
        self.s3_put_lat_us.store(put_lat_us, ORD);
        self.s3_put_prepare_lat_us.store(put_prepare_lat_us, ORD);
        self.s3_put_cache_lat_us.store(put_cache_lat_us, ORD);
        self.s3_del_ops.store(del_ops, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_read_strategy_metrics(
        &self,
        block_cache_hits: u64,
        page_cache_hits: u64,
        page_cache_misses: u64,
        range_gets: u64,
        full_gets: u64,
        piggyback_full: u64,
        background_prefetches: u64,
        background_prefetch_dropped: u64,
    ) {
        self.read_block_cache_hits.store(block_cache_hits, ORD);
        self.read_page_cache_hits.store(page_cache_hits, ORD);
        self.read_page_cache_misses.store(page_cache_misses, ORD);
        self.read_range_gets.store(range_gets, ORD);
        self.read_full_gets.store(full_gets, ORD);
        self.read_piggyback_full.store(piggyback_full, ORD);
        self.read_background_prefetches
            .store(background_prefetches, ORD);
        self.read_background_prefetch_dropped
            .store(background_prefetch_dropped, ORD);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sync_meta_client_metrics(
        &self,
        stat_cache_hit: u64,
        stat_cache_miss: u64,
        stat_fresh_store_hit: u64,
        lookup_cache_hit: u64,
        lookup_cache_miss: u64,
        get_slices_cache_hit: u64,
        get_slices_cache_miss: u64,
        open_fresh_stat: u64,
        open_file_cache_hit: u64,
        open_file_cache_miss: u64,
        lookup_attr_fused_hit: u64,
        lookup_attr_fused_miss: u64,
        lookup_attr_fused_error: u64,
    ) {
        self.meta_stat_cache_hit.store(stat_cache_hit, ORD);
        self.meta_stat_cache_miss.store(stat_cache_miss, ORD);
        self.meta_stat_fresh_store_hit
            .store(stat_fresh_store_hit, ORD);
        self.meta_lookup_cache_hit.store(lookup_cache_hit, ORD);
        self.meta_lookup_cache_miss.store(lookup_cache_miss, ORD);
        self.meta_get_slices_cache_hit
            .store(get_slices_cache_hit, ORD);
        self.meta_get_slices_cache_miss
            .store(get_slices_cache_miss, ORD);
        self.meta_open_fresh_stat.store(open_fresh_stat, ORD);
        self.meta_open_file_cache_hit
            .store(open_file_cache_hit, ORD);
        self.meta_open_file_cache_miss
            .store(open_file_cache_miss, ORD);
        self.meta_lookup_attr_fused_hit
            .store(lookup_attr_fused_hit, ORD);
        self.meta_lookup_attr_fused_miss
            .store(lookup_attr_fused_miss, ORD);
        self.meta_lookup_attr_fused_error
            .store(lookup_attr_fused_error, ORD);
    }
}
