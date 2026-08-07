//! RAII timing guards for the VFS stats collector.
//!
//! Split out of `stats.rs` (part of the writer/stats decomposition): these
//! guards are self-contained and have no behavior changes.

use crate::vfs::stats::FsStats;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

/// RAII guard for timing an operation and recording latency + count.
/// Records stats on drop, so it can be used as `let _timer = OpTimer::new(...)`.
pub struct OpTimer<'a> {
    start: Instant,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> OpTimer<'a> {
    pub fn new(ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: Instant::now(),
            ops_counter,
            lat_counter,
        }
    }

    /// Finish timing and record the operation (consumes self).
    pub fn finish(self) {
        // Drop impl handles the actual recording.
    }
}

impl<'a> Drop for OpTimer<'a> {
    fn drop(&mut self) {
        FsStats::record_duration(self.ops_counter, self.lat_counter, self.start.elapsed());
    }
}

/// Optional timer for diagnostic hot-path stats. Disabled timers avoid
/// `Instant::now()` so production hot paths only pay a cheap branch.
pub struct MaybeOpTimer<'a> {
    start: Option<Instant>,
    ops_counter: &'a AtomicU64,
    lat_counter: &'a AtomicU64,
}

impl<'a> MaybeOpTimer<'a> {
    pub fn new(enabled: bool, ops_counter: &'a AtomicU64, lat_counter: &'a AtomicU64) -> Self {
        Self {
            start: enabled.then(Instant::now),
            ops_counter,
            lat_counter,
        }
    }
}

impl<'a> Drop for MaybeOpTimer<'a> {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            FsStats::record_duration(self.ops_counter, self.lat_counter, start.elapsed());
        }
    }
}
