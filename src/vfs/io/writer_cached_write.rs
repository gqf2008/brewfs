//! Cached-write range/segment bookkeeping and debug helpers.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): these pure
//! helpers manage the sparse cached-write watermark and debug window used by
//! the write path. No behavior changes.

use crate::utils::NumCastExt;
use std::fmt::Write as FmtWrite;
use std::sync::LazyLock;

static DEBUG_CACHED_WRITE_OFFSET: LazyLock<Option<u64>> = LazyLock::new(|| {
    std::env::var("BREWFS_DEBUG_CACHED_WRITE_OFFSET")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CachedWriteRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) max_unique: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CachedWriteSegment {
    pub(crate) file_offset: u64,
    pub(crate) buf_offset: usize,
    pub(crate) len: usize,
}

pub(crate) fn debug_cached_write_offset() -> Option<u64> {
    *DEBUG_CACHED_WRITE_OFFSET
}

pub(crate) fn cached_write_probe_index(offset: u64, len: usize, probe: u64) -> Option<usize> {
    let end = offset.checked_add(len as u64)?;
    (offset <= probe && probe < end).then_some((probe - offset).as_usize())
}

pub(crate) fn cached_write_probe_segments(
    segments: &[CachedWriteSegment],
    probe: u64,
) -> Vec<CachedWriteSegment> {
    segments
        .iter()
        .copied()
        .filter(|segment| {
            segment.file_offset <= probe && probe < segment.file_offset + segment.len as u64
        })
        .collect()
}

pub(crate) fn cached_write_debug_window(buf: &[u8], index: usize) -> String {
    let start = index.saturating_sub(16);
    let end = (index + 17).min(buf.len());
    let mut out = String::new();
    for (pos, byte) in buf[start..end].iter().enumerate() {
        if pos > 0 {
            out.push(' ');
        }
        if start + pos == index {
            out.push('[');
            let _ = write!(&mut out, "{byte:02x}");
            out.push(']');
        } else {
            let _ = write!(&mut out, "{byte:02x}");
        }
    }
    out
}

pub(crate) fn push_sparse_cached_segment_if_materialized(
    out: &mut Vec<CachedWriteSegment>,
    file_offset: u64,
    buf_offset: usize,
    buf: &[u8],
) {
    if buf.iter().any(|byte| *byte != 0) {
        out.push(CachedWriteSegment {
            file_offset,
            buf_offset,
            len: buf.len(),
        });
    }
}

pub(crate) fn record_cached_write_watermark(
    ranges: &mut Vec<CachedWriteRange>,
    start: u64,
    end: u64,
    unique: u64,
) {
    if unique == 0 || start >= end {
        return;
    }

    let mut boundaries = vec![start, end];
    for range in ranges.iter() {
        if range.end <= start || range.start >= end {
            continue;
        }
        boundaries.push(range.start.max(start));
        boundaries.push(range.end.min(end));
    }
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut next = Vec::with_capacity(ranges.len() + boundaries.len());
    for range in ranges.iter().copied() {
        if range.end <= start || range.start >= end {
            next.push(range);
            continue;
        }
        if range.start < start {
            next.push(CachedWriteRange {
                start: range.start,
                end: start,
                max_unique: range.max_unique,
            });
        }
        if range.end > end {
            next.push(CachedWriteRange {
                start: end,
                end: range.end,
                max_unique: range.max_unique,
            });
        }
    }

    for window in boundaries.windows(2) {
        let seg_start = window[0];
        let seg_end = window[1];
        if seg_start >= seg_end {
            continue;
        }
        let mut max_unique = unique;
        for range in ranges.iter() {
            if range.start < seg_end && seg_start < range.end {
                max_unique = max_unique.max(range.max_unique);
            }
        }
        next.push(CachedWriteRange {
            start: seg_start,
            end: seg_end,
            max_unique,
        });
    }

    next.sort_by_key(|range| (range.start, range.end));
    let mut merged: Vec<CachedWriteRange> = Vec::with_capacity(next.len());
    for range in next {
        if range.start >= range.end {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && last.end == range.start
            && last.max_unique == range.max_unique
        {
            last.end = range.end;
            continue;
        }
        merged.push(range);
    }
    *ranges = merged;
}
