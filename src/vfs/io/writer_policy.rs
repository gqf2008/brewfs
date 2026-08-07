//! Write-path policy helpers: tuning constants, env overrides, writeback
//! backpressure decisions, and retry classification.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition) — this
//! module is pure policy (no I/O) and has no behavior changes.

use crate::meta::store::MetaError;
use crate::vfs::cache::config::WriteBackMode;
use rand::RngCore;
use std::fmt::Display;
use std::time::Duration;

pub(crate) const FLUSH_DURATION: Duration = Duration::from_secs(5);
pub(crate) const COMMIT_WAIT_SLICE: Duration = Duration::from_millis(100);
pub(crate) const FLUSH_WAIT: Duration = Duration::from_secs(3);
pub(crate) const FLUSH_DEADLINE: Duration = Duration::from_secs(300);
pub(crate) const TRUNCATE_FLUSH_DEADLINE: Duration = Duration::from_secs(10);
/// Shorter deadline for close-triggered flushes.  FUSE already calls flush()
/// before close(), so close() only needs to drain residual in-flight work.
pub(crate) const CLOSE_FLUSH_DEADLINE: Duration = Duration::from_secs(5);
/// Maximum time commit_chunk will wait for a single slice's upload before
/// marking it failed.  Prevents indefinite hangs on stalled S3 connections.
pub(crate) const COMMIT_UPLOAD_MAX_WAIT: Duration = Duration::from_secs(180);
pub(crate) const UPLOAD_MAX_RETRIES: u64 = 5;
pub(crate) const COMMIT_RETRY_BASE_MS: u64 = 20;
pub(crate) const COMMIT_RETRY_MAX_MS: u64 = 2000;
pub(crate) const COMMIT_META_MAX_RETRIES: u32 = 15;
// High-throughput foreground writes can race with background freeze/flush for
// several scheduler turns; keep this below "spin forever", but high enough not
// to surface an internal slice handoff as EIO.
pub(crate) const WRITE_SLICE_MAX_RETRIES: u32 = 1024;
/// Maximum age of a Writable slice before auto_flush freezes it and starts
/// background upload, regardless of idle time.  For S3 backends, a longer
/// threshold aggregates more data per slice, reducing small-object PUT
/// amplification.  fsync/close still force-seal immediately.
/// NOTE: This is the fallback; prefer config.auto_flush_max_age when available.
pub(crate) const AUTO_FLUSH_MAX_AGE: Duration = Duration::from_millis(2000);

pub(crate) const MAX_UNFLUSHED_SLICES: usize = 3;
pub(crate) const MAX_SLICES_THRESHOLD: usize = 800;
pub(crate) const WRITE_MAX_WAIT: Duration = Duration::from_secs(30);
pub(crate) const WRITEBACK_WRITE_MAX_WAIT: Duration = Duration::from_secs(300);
pub(crate) const CACHED_SUB_BLOCK_IDLE_GRACE: Duration = Duration::from_secs(3);
pub(crate) const CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE: Duration = Duration::from_secs(1);
pub(crate) const CACHED_SUB_BLOCK_AUTO_FREEZE_MIN_AGE: Duration = Duration::from_secs(10);
pub(crate) const WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP: Duration = Duration::from_millis(1);
pub(crate) const WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP: Duration = Duration::from_millis(6);
pub(crate) const WRITEBACK_HARD_BACKPRESSURE_MAX_SLEEP: Duration = Duration::from_millis(6);
/// Minimum number of bytes a Writable slice must hold before `should_freeze`
/// returns true on a size basis.  32 MiB gives 8 blocks per upload batch,
/// maximizing pipeline parallelism while keeping flush latency reasonable.
/// fsync/close bypass this threshold and force-seal regardless of size.
/// NOTE: This is the fallback; prefer config.freeze_min_bytes when available.
pub(crate) const SHOULD_FREEZE_MIN_BYTES: u64 = 8 * 1024 * 1024;

pub(crate) static CACHED_SUB_BLOCK_IDLE_GRACE_CONFIG: std::sync::LazyLock<Duration> =
    std::sync::LazyLock::new(|| {
        env_duration_ms(
            "BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS",
            CACHED_SUB_BLOCK_IDLE_GRACE,
        )
    });

pub(crate) static CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_CONFIG: std::sync::LazyLock<Duration> =
    std::sync::LazyLock::new(|| {
        env_duration_ms(
            "BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS",
            CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE,
        )
    });

pub(crate) fn env_duration_ms(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

pub(crate) fn cached_sub_block_idle_grace() -> Duration {
    *CACHED_SUB_BLOCK_IDLE_GRACE_CONFIG
}

pub(crate) fn cached_sub_block_too_many_min_age() -> Duration {
    *CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_CONFIG
}

pub(crate) enum WritebackBackpressureDecision {
    Allow,
    SoftSleep(Duration),
    Wait,
}

pub(crate) fn decide_writeback_backpressure(
    pending: u64,
    incoming: u64,
    soft_limit: u64,
    hard_limit: u64,
) -> WritebackBackpressureDecision {
    if soft_limit == 0 {
        return WritebackBackpressureDecision::Allow;
    }

    let projected = pending.saturating_add(incoming);
    if projected <= soft_limit {
        return WritebackBackpressureDecision::Allow;
    }

    if hard_limit > soft_limit && projected <= hard_limit {
        let over_soft = projected.saturating_sub(soft_limit);
        let soft_range = hard_limit - soft_limit;
        let sleep_span_ms = (WRITEBACK_SOFT_BACKPRESSURE_MAX_SLEEP
            - WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP)
            .as_millis() as u64;
        let extra_ms =
            ((over_soft as u128) * (sleep_span_ms as u128) / (soft_range as u128)) as u64;
        return WritebackBackpressureDecision::SoftSleep(
            WRITEBACK_SOFT_BACKPRESSURE_MIN_SLEEP + Duration::from_millis(extra_ms),
        );
    }

    WritebackBackpressureDecision::Wait
}

pub(crate) fn write_buffer_max_wait(mode: WriteBackMode) -> Duration {
    match mode {
        WriteBackMode::CommitBeforeUpload => WRITEBACK_WRITE_MAX_WAIT,
        WriteBackMode::UploadBeforeCommit => WRITE_MAX_WAIT,
    }
}

pub(crate) fn truncate_flush_deadline() -> Duration {
    std::env::var("BREWFS_TRUNCATE_FLUSH_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or(TRUNCATE_FLUSH_DEADLINE)
}

pub(crate) fn commit_retry_backoff(failures: u32) -> Duration {
    let exp = failures.saturating_sub(1).min(16);
    let step = COMMIT_RETRY_BASE_MS.checked_shl(exp).unwrap_or(u64::MAX);
    let base = step.min(COMMIT_RETRY_MAX_MS);
    // Scale jitter with base delay to spread retry bursts at higher backoff levels.
    let jitter_span = (base / 10).max(20);
    let jitter = rand::rng().next_u64() % (jitter_span.saturating_add(1));
    Duration::from_millis(base.saturating_add(jitter))
}

pub(crate) fn looks_retryable_backend_error(err: &impl Display) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    [
        "deadlock",
        "database is locked",
        "database is busy",
        "serialization",
        "retry",
        "timeout",
        "timed out",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(crate) fn should_retry_meta_write(err: &MetaError) -> bool {
    match err {
        MetaError::ContinueRetry(_) | MetaError::MaxRetriesExceeded => true,
        MetaError::Database(err) => looks_retryable_backend_error(err),
        MetaError::Io(err) => matches!(
            err.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionReset
        ),
        _ => false,
    }
}
