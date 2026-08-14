//! Metadata-less object-store filesystem.
//!
//! Mounts an S3-compatible bucket (Aliyun OSS, MinIO, ...) directly as a
//! filesystem: file paths are encoded into object keys and the bucket itself
//! is the single source of truth. There is **no local metadata database**, so
//! any number of machines can mount the same bucket and see the same tree —
//! exactly what `ossfs`/s3fs do. The trade-off is weak consistency (no locks,
//! no atomic rename): it is meant for "cloud drive" usage where machines do
//! not concurrently edit the same file.
//!
//! Layout (s3fs-style):
//! - `/docs/report.txt` -> object key `docs/report.txt`
//! - directory `/docs` -> implicit via prefix, plus a zero-byte marker
//!   object `docs/` so empty directories survive listing.
//!
//! This module is cross-platform; the platform mount adapters live in
//! [`crate::ossfs::winfsp`] (Windows only) and [`crate::ossfs::fuse`] (macOS/Linux).

pub mod admin;

#[cfg(not(windows))]
pub mod fuse;
#[cfg(windows)]
pub mod winfsp;

use anyhow::{Context as _, Result};
use aws_config::credential_process::CredentialProcessProvider;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, StorageClass};
use aws_sdk_s3::{Client, config::BehaviorVersion};
use aws_smithy_runtime_api::client::interceptors::{
    Intercept, context::BeforeDeserializationInterceptorContextRef,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// S3-compatible object store configuration.
#[derive(Debug, Clone)]
pub struct OssConfig {
    pub bucket: String,
    pub region: String,
    /// Custom endpoint URL (Aliyun OSS, MinIO, ...). None = AWS.
    pub endpoint: Option<String>,
    /// Force path-style addressing (MinIO, Aliyun access points usually need
    /// virtual-hosted style, so default false).
    pub force_path_style: bool,
    /// Optional namespace prefix under the bucket (e.g. `ossfs/`). All keys
    /// are stored under it. Must be empty or end with `/`.
    pub prefix: String,
    /// Optional in-flight S3 request cap. `None` (or `Some(0)`) uses the
    /// default [`MAX_CONCURRENT_S3_REQUESTS`]; explicit values let high-RTT
    /// or low-memory mounts tune the bound (0/None = default, never disable).
    pub max_concurrent_requests: Option<usize>,
    /// Cap on directory-enumeration (`ListObjects`) rate in calls/second
    /// (mirrors a readdir soft-limit). `None`/`Some(0)` disables it.
    pub list_rate_limit: Option<f64>,
    /// Mount the filesystem read-only: reject write / mkdir / delete / rename.
    pub read_only: bool,
    /// POSIX ownership / permission defaults applied to every object by the
    /// FUSE adapters. Objects are metadata-less, so these are mount-level
    /// defaults (like aliyun/ossfs `uid` / `gid` / `dir_mode` / `file_mode`).
    /// `uid`/`gid` of 0 mean "use the mounting user".
    pub uid: u32,
    pub gid: u32,
    pub dir_mode: u32,
    pub file_mode: u32,
    /// Open the FUSE mount to all users (mirrors aliyun/ossfs
    /// `allow_other`). FUSE-only.
    pub allow_other: bool,
    /// Additional permission mask applied on top of `dir_mode`/`file_mode`
    /// (mirrors aliyun/ossfs `umask`). `0` applies no extra mask.
    pub umask: u32,
    /// Allow directory renames. Directory rename is implemented as a
    /// recursive copy + delete; disabling it avoids unbounded tree copies
    /// (mirrors aliyun/ossfs `allow_rename_dir`).
    pub allow_rename_dir: bool,
    /// Maximum number of objects copied by a single directory rename.
    /// `None` (or `Some(0)`) means unlimited. Mirrors aliyun/ossfs
    /// `rename_dir_limit`.
    pub rename_dir_limit: Option<u64>,
    /// Cap on aggregate bytes of in-flight object writes (whole-object PUT
    /// and multipart uploads). `None` (or `Some(0)`) means unlimited.
    /// Rounded up to 1 MiB units internally. Mirrors aliyun/ossfs
    /// `total_mem_limit` (the write-upload portion).
    pub max_upload_bytes: Option<usize>,
    /// Sequential-read prefetch window in bytes. When set, consecutive
    /// reads (next offset == previous end) fetch this many bytes ahead and
    /// cache them so the following small reads are served locally (mirrors
    /// aliyun/ossfs `prefetch_chunk_size`). `None`/`Some(0)` disables it.
    pub read_ahead_bytes: Option<usize>,
    /// Ignore FUSE fsync requests instead of flushing the whole-file write
    /// buffer on every sync (mirrors aliyun/ossfs `ignore_fsync`). The write
    /// is still pushed on flush/release.
    pub ignore_fsync: bool,
    /// Cap on aggregate dirty whole-file write-buffer bytes held by the
    /// mount adapters. `None`/`Some(0)` disables the cap. Rounded up to 1 MiB
    /// units. Mirrors aliyun/ossfs `total_mem_limit` (the dirty-buffer part).
    pub max_dirty_bytes: Option<usize>,
    /// External `credential_process` command (mirrors aliyun/ossfs). The
    /// command is executed on credential refresh and must emit the standard
    /// AWS credential-process JSON. Takes precedence over env/profile creds.
    pub credential_process: Option<String>,
    /// Socket connect timeout in seconds (mirrors aliyun/ossfs
    /// `connect_timeout`). `None` keeps the SDK default.
    pub connect_timeout_secs: Option<u64>,
    /// Read timeout in seconds (mirrors aliyun/ossfs `readwrite_timeout`).
    /// `None` keeps the SDK default.
    pub readwrite_timeout_secs: Option<u64>,
    /// Additional retry attempts after the initial request (mirrors
    /// aliyun/ossfs `retries`). `None` keeps the SDK default.
    pub retries: Option<u32>,
    /// Verify each uploaded object's integrity against the `x-oss-hash-crc64ecma`
    /// header returned by OSS (mirrors aliyun/ossfs `enable_crc64`). The
    /// CRC64-ECMA-182 checksum is computed locally on every object / multipart
    /// write and compared to the value OSS reports back after the upload.
    pub verify_crc64: bool,
    /// Set the `Content-MD5` header on single PUT and each multipart part
    /// (mirrors aliyun/ossfs `enable_content_md5`).
    pub content_md5: bool,
    /// Skip legacy `_$folder$` directory-marker objects in listings
    /// (mirrors aliyun/ossfs `notsup_compat_dir`).
    pub notsup_compat_dir: bool,
    /// Storage class applied to newly written objects (mirrors aliyun/ossfs
    /// `storage_class`). Common values: `Standard`, `IA`, `Archive` (OSS) or
    /// `STANDARD` / `STANDARD_IA` / `GLACIER` (S3). `None` keeps the bucket
    /// default.
    pub storage_class: Option<String>,
    /// Multipart upload part size in bytes (mirrors aliyun/ossfs
    /// `multipart_size`). `None` uses [`MULTIPART_PART_SIZE`]; values below
    /// 5 MiB are clamped up to the S3 minimum.
    pub multipart_size: Option<usize>,
    /// Number of concurrent part uploads within one multipart write (mirrors
    /// aliyun/ossfs `parallel_count`). `None` uses
    /// [`MULTIPART_UPLOAD_CONCURRENCY`].
    pub multipart_concurrency: Option<usize>,
    /// Local disk cache directory for object-range blocks. When set, read
    /// ranges that are not served from the in-memory read-ahead cache are
    /// written here and reused on later reads (even across remounts).
    /// Mirrors aliyun/ossfs disk cache.
    pub disk_cache_dir: Option<PathBuf>,
    /// Upper bound on disk-cache bytes; evicts oldest blocks when exceeded.
    /// `Some(0)` disables the disk cache. Rounded up to 1 MiB units.
    /// Total process memory budget for read/write buffers. When set, it
    /// overrides the read-cache / upload / dirty budgets with a fixed
    /// 2:1:1 split (read cache : upload : dirty). Mirrors aliyun/ossfs
    /// `total_mem_limit`. `Some(0)` disables the override.
    pub total_mem_limit: Option<usize>,
    /// Fraction of [`Self::total_mem_limit`] reserved for the in-memory read
    /// cache (aliyun/ossfs `rw_ratio` semantics). Valid range `(0, 1)`.
    /// The remaining memory is split equally between upload and dirty buffers.
    pub total_mem_read_ratio: f64,
    /// Upper bound on the in-memory read-ahead cache in bytes. `None`
    /// uses the default [`READ_CACHE_MAX_BYTES`].
    pub read_cache_max_bytes: Option<usize>,
    pub disk_cache_max_bytes: usize,
    /// Disk-cache block size in bytes. `Some(0)` / `None` uses
    /// [`DISK_CACHE_BLOCK_SIZE`].
    pub disk_cache_block_size: Option<usize>,
    /// Keep at least this many bytes free on the disk cache's filesystem.
    /// When a cache write would drop free space below this floor the block
    /// is skipped (mirrors aliyun/ossfs `ensure_diskfree`).
    pub disk_cache_reserve_diskfree: u64,
    /// Keep at least this fraction of the cache filesystem free. Combined
    /// with [`Self::disk_cache_reserve_diskfree`] via `max` (mirrors
    /// aliyun/ossfs `free_space_ratio`).
    pub disk_cache_free_space_ratio: Option<f64>,
    /// Number of consecutive blocks to prefetch in the background after a
    /// sequential disk-cache read. `0` disables prefetch.
    pub disk_cache_prefetch_blocks: usize,
    /// Maximum concurrent background disk-cache prefetch tasks.
    /// `Some(0)` / `None` uses [`DISK_CACHE_PREFETCH_CONCURRENCY`].
    pub disk_cache_prefetch_concurrency: usize,
    /// Verify object ETag with a HEAD before serving disk-cache blocks.
    /// Detects remote changes made by other writers.
    pub disk_cache_verify_etag: bool,
    /// ETag re-check TTL in seconds (default 10).
    pub disk_cache_etag_ttl_secs: u64,
    /// Negative-stat cache TTL in seconds (default 5).
    pub negative_cache_ttl_secs: u64,
    /// Maximum negative-stat cache entries (default 4096).
    pub negative_cache_max_entries: usize,
    /// Positive-stat cache TTL in seconds (default 3).
    pub stat_cache_ttl_secs: u64,
    /// Maximum positive-stat cache entries (default 4096).
    pub stat_cache_max_entries: usize,
}

/// POSIX ownership / permission defaults applied to every object by the FUSE
/// adapters. See [`OssConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MountAttr {
    pub uid: u32,
    pub gid: u32,
    pub dir_mode: u32,
    pub file_mode: u32,
    pub umask: u32,
}

impl Default for MountAttr {
    fn default() -> Self {
        Self {
            uid: 0,
            gid: 0,
            dir_mode: 0o755,
            file_mode: 0o644,
            umask: 0,
        }
    }
}

/// Resolve a configured owner id: `0` means "use the mounting user".
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn effective_owner(configured: u32, current: u32) -> u32 {
    if configured == 0 { current } else { configured }
}

/// Resolve the FUSE permission bits for an object: directory vs file.
#[cfg_attr(windows, allow(dead_code))]
pub(crate) fn effective_mode(is_dir: bool, dir_mode: u32, file_mode: u32, umask: u32) -> u16 {
    let base = if is_dir { dir_mode } else { file_mode };
    (base & !umask) as u16
}

/// Compute the CRC-64/ECMA-182 checksum used by Aliyun OSS
/// (`x-oss-hash-crc64ecma`). The reflected polynomial is `0xC96C5795D7870F42`,
/// init `0xFFFF_FFFF_FFFF_FFFF`, xorout `0xFFFF_FFFF_FFFF_FFFF`, identical to
/// CRC-64/XZ and to aliyun/ossfs's PhotonLibOS `crc64ecma`.
pub(crate) fn crc64ecma(data: &[u8]) -> u64 {
    const POLY: u64 = 0xC96C_5795_D787_0F42;
    let mut crc: u64 = 0xFFFF_FFFF_FFFF_FFFF;
    for &b in data {
        crc ^= b as u64;
        for _ in 0..8 {
            crc = (crc >> 1) ^ ((crc & 1).wrapping_neg() & POLY);
        }
    }
    crc ^ 0xFFFF_FFFF_FFFF_FFFF
}

/// Incremental CRC-64/ECMA-182 hasher, so a multipart upload can be verified
/// while streaming a file from disk (see [`ObjectFs::write_from_file`]).
struct Crc64Ecma {
    crc: u64,
}

impl Crc64Ecma {
    fn new() -> Self {
        Self {
            crc: 0xFFFF_FFFF_FFFF_FFFF,
        }
    }

    fn update(&mut self, data: &[u8]) {
        const POLY: u64 = 0xC96C_5795_D787_0F42;
        for &b in data {
            self.crc ^= b as u64;
            for _ in 0..8 {
                self.crc = (self.crc >> 1) ^ ((self.crc & 1).wrapping_neg() & POLY);
            }
        }
    }

    fn finalize(self) -> u64 {
        self.crc ^ 0xFFFF_FFFF_FFFF_FFFF
    }
}

/// Captures the `x-oss-hash-crc64ecma` response header before the SDK
/// deserializes the output, so the write path can compare it to the locally
/// computed checksum after the call returns.
#[derive(Debug)]
struct Crc64ResponseCapture {
    slot: Arc<Mutex<Option<u64>>>,
}

impl Intercept for Crc64ResponseCapture {
    fn name(&self) -> &'static str {
        "Crc64ResponseCapture"
    }

    fn read_before_deserialization(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
        _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
    ) -> std::result::Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let value = context
            .response()
            .headers()
            .get("x-oss-hash-crc64ecma")
            .and_then(|v| v.parse::<u64>().ok());
        *self.slot.lock().unwrap() = value;
        Ok(())
    }
}

/// Verify that `expected` matches the CRC64 value returned by OSS. The header
/// is captured as a side effect of [`Crc64ResponseCapture`] on the operation
/// that just completed.
fn check_crc64_response(
    slot: Arc<Mutex<Option<u64>>>,
    expected: u64,
    metrics: &Metrics,
) -> Result<()> {
    let actual = slot.lock().unwrap().take();
    match actual {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => {
            metrics.crc64_mismatches.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!(
                "crc64 mismatch: expected {expected}, got {actual} from x-oss-hash-crc64ecma"
            )
        }
        None => anyhow::bail!("x-oss-hash-crc64ecma header missing from upload response"),
    }
}

/// Base64-encoded MD5 of `data`, for the S3 `Content-MD5` header
/// (aliyun/ossfs `enable_content_md5`).
fn content_md5(data: &[u8]) -> String {
    use base64::Engine as _;
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

impl OssConfig {
    pub fn normalize(mut self) -> Self {
        if !self.prefix.is_empty() && !self.prefix.ends_with('/') {
            self.prefix.push('/');
        }
        self
    }
}

/// A directory entry returned by [`ObjectFs::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime_secs: i64,
}

/// Object-store-backed filesystem handle (no local metadata).
/// How long a `stat` result is cached locally. Explorer issues several
/// sequential attribute queries (get_file_info / get_security_by_name / open)
/// per click, and each used to cost an S3 round trip (10ms warm, 200-800ms
/// cold). A short cache absorbs the repeats while keeping remote changes
/// visible within a few seconds, consistent with the 1s WinFsp attr TTL.
const STAT_TTL: Duration = Duration::from_secs(3);
/// Upper bound on cached stat entries; the cache is cleared when exceeded.
const MAX_STAT_ENTRIES: usize = 4096;
/// TTL for the negative-stat cache (paths known to not exist). Avoids
/// repeated remote HEAD/probe round trips when callers repeatedly stat
/// missing paths.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);
/// Upper bound on negative-cache entries; cleared when exceeded so memory
/// stays bounded (mirrors [`MAX_STAT_ENTRIES`]).
const MAX_NEGATIVE_ENTRIES: usize = 4096;
/// Upper bound on in-flight S3 requests issued by one mount. Bounds peak
/// memory (every list/head materializes full results) and remote pressure
/// during I/O storms such as `find /` recursing into the mounted network
/// drive; without it the process can OOM-abort (0xc0000409).
const MAX_CONCURRENT_S3_REQUESTS: usize = 32;
/// Above this size, `write` uploads via S3 multipart (bounded-concurrency
/// parts) instead of a single PUT. A single PUT is capped at 5 GiB by OSS/S3
/// and is more sensitive to timeouts / retries on large objects.
const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;
/// Part size for multipart uploads (>= 5 MiB required by AWS; Aliyun OSS
/// allows >= 100 KiB, so 8 MiB is safe for both).
const MULTIPART_PART_SIZE: u64 = 8 * 1024 * 1024;
/// Concurrent in-flight part uploads within a single multipart write. Each
/// part also takes a global request-limit permit, so global in-flight stays
/// bounded.
const MULTIPART_UPLOAD_CONCURRENCY: usize = 4;
/// Unit size for the in-flight write-byte budget. The semaphore counts
/// whole MiB units, so the bound is accurate to within one MiB while keeping
/// permit counts small.
const UPLOAD_BUDGET_UNIT: usize = 1 << 20;
/// Upper bound on bytes held by the read-ahead cache. Keeps prefetch from
/// turning a sequential scan into an OOM (the same failure class the global
/// request limiter already guards against).
const READ_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Upper bound on read-ahead cache entries.
const READ_CACHE_MAX_ENTRIES: usize = 256;
/// Upper bound on tracked sequential-read hints; cleared when exceeded.
const MAX_READ_SEQ_ENTRIES: usize = 4096;
/// Unit size for the dirty-buffer budget (whole MiB permits).
const DIRTY_BUDGET_UNIT: usize = 1 << 20;
/// Block size for the disk cache. Reads are fetched and stored in these
/// fixed-size chunks, mirroring aliyun/ossfs cache_block_size.
const DISK_CACHE_BLOCK_SIZE: u64 = 4 * 1024 * 1024;
/// Version for the disk-cache on-disk format (block checksum layout).
const DISK_CACHE_META_VERSION: u64 = 2;
/// Default maximum number of concurrent disk-cache prefetch tasks.
const DISK_CACHE_PREFETCH_CONCURRENCY: usize = 4;
/// How long an object ETag check is considered fresh before re-HEADing.
const ETAG_CHECK_TTL: Duration = Duration::from_secs(10);
/// Unit size for the disk-cache byte budget (whole MiB permits).
const DISK_CACHE_BUDGET_UNIT: usize = 1 << 20;

/// `(total_capacity_bytes, available_bytes)` for the filesystem containing
/// `dir`. Best-effort: returns `None` when the OS query is unavailable, in
/// which case free-space protection is skipped.
#[cfg(windows)]
fn disk_space(dir: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    let wide: Vec<u16> = dir
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut available = 0u64;
    let mut total = 0u64;
    let mut free = 0u64;
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            &mut total,
            &mut free,
        )
    };
    if ok == 0 {
        None
    } else {
        Some((total, available))
    }
}

#[cfg(not(windows))]
fn disk_space(dir: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    let frsize = stat.f_frsize.max(1) as u64;
    Some((
        (stat.f_blocks as u64).saturating_mul(frsize),
        (stat.f_bavail as u64).saturating_mul(frsize),
    ))
}

/// Effective free-space floor: `max(reserve_bytes, ratio * total_bytes)`.
fn min_free_bytes(reserve: u64, ratio: Option<f64>, total: u64) -> u64 {
    let ratio_bytes = ratio
        .map(|r| (total as f64 * r.clamp(0.0, 0.99)) as u64)
        .unwrap_or(0);
    reserve.max(ratio_bytes)
}

/// Map `retries` (aliyun/ossfs semantics: additional attempts after the first)
/// to the AWS SDK `max_attempts` (total attempts: initial + retries).
fn s3_max_attempts(retries: u32) -> u32 {
    retries.saturating_add(1).max(1)
}

/// Resolve [`OssConfig::max_upload_bytes`] into a number of MiB permits.
/// Returns `None` when the budget is disabled. Values above what a single
/// acquire call can represent are clamped.
/// Derive effective memory budgets. When `total_mem_limit` is set it wins: the
/// read cache takes `total_mem_read_ratio`, and the rest splits equally between
/// upload bytes and dirty bytes. Otherwise the individual options win.
fn effective_memory_budgets(
    total_mem_limit: Option<usize>,
    total_mem_read_ratio: f64,
    max_upload_bytes: Option<usize>,
    max_dirty_bytes: Option<usize>,
    read_cache_max_bytes: Option<usize>,
) -> (Option<usize>, Option<usize>, usize) {
    match total_mem_limit {
        Some(total) if total > 0 => {
            let ratio = total_mem_read_ratio.clamp(0.01, 0.99);
            let read = ((total as f64) * ratio) as usize;
            let rest = total.saturating_sub(read);
            (Some(rest / 2), Some(rest / 2), read)
        }
        _ => (
            max_upload_bytes,
            max_dirty_bytes,
            read_cache_max_bytes.unwrap_or(READ_CACHE_MAX_BYTES),
        ),
    }
}
fn upload_budget_units(max_bytes: Option<usize>) -> Option<usize> {
    let bytes = max_bytes?;
    if bytes == 0 {
        return None;
    }
    let units = bytes.div_ceil(UPLOAD_BUDGET_UNIT).min(u32::MAX as usize);
    Some(units.max(1))
}

/// Shared, bounded budget for whole-file dirty write buffers held by the
/// WinFsp/FUSE adapters. The budget is a semaphore of whole-MiB permits; each
/// open write handle keeps permits for its high-water buffer size and releases
/// them when the handle is closed.
#[derive(Clone)]
pub struct DirtyBudget {
    sem: Arc<Semaphore>,
    unit: usize,
    max_units: usize,
}

impl DirtyBudget {
    pub fn new(max_bytes: usize) -> Option<Self> {
        if max_bytes == 0 {
            return None;
        }
        let unit = DIRTY_BUDGET_UNIT;
        let max_units = max_bytes.div_ceil(unit).min(u32::MAX as usize).max(1);
        Some(Self {
            sem: Arc::new(Semaphore::new(max_units)),
            unit,
            max_units,
        })
    }

    pub fn unit(&self) -> usize {
        self.unit
    }

    pub fn max_units(&self) -> usize {
        self.max_units
    }

    /// Acquire `units` MiB permits. Returns an RAII permit that releases them
    /// on drop. `units == 0` returns an empty permit.
    pub async fn acquire_units(&self, units: usize) -> Result<DirtyPermit> {
        if units == 0 {
            return Ok(DirtyPermit { _permit: None });
        }
        let permit = self
            .sem
            .clone()
            .acquire_many_owned(units as u32)
            .await
            .map_err(|_| anyhow::anyhow!("dirty buffer budget closed"))?;
        Ok(DirtyPermit {
            _permit: Some(permit),
        })
    }
}

/// RAII permit returned by [`DirtyBudget::acquire_units`]. Dropping it
/// releases the reserved MiB permits back to the budget.
pub struct DirtyPermit {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Token-bucket rate limiter for directory enumerations. Bounds how
/// fast a recursive scan (`find /`) can drive `ListObjects` calls while a
/// single normal directory read is served immediately (burst capacity).
struct TokenBucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: f64) -> Self {
        let burst = rate.max(1.0);
        Self {
            rate,
            burst,
            tokens: burst,
            last: Instant::now(),
        }
    }

    /// Refill and reserve one token. `None` means a token is available now;
    /// `Some(dur)` is how long to wait before retrying.
    fn reserve(&mut self, now: Instant) -> Option<Duration> {
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            None
        } else {
            Some(Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
        }
    }
}

/// One cached read-ahead window for a path.
struct ReadCacheEntry {
    start: u64,
    data: Vec<u8>,
    last_used: Instant,
}

/// Block-oriented on-disk cache for object ranges. Blocks are keyed by
/// FNV-1a hash of the object key plus block index; each block file starts
/// with the raw key so a hash collision is detected and treated as a miss.
#[derive(Debug)]
struct DiskCache {
    dir: PathBuf,
    max_bytes: u64,
    block_size: u64,
    used: AtomicU64,
    /// In-process LRU order of cached blocks: `(key, block)` with the most
    /// recently used entry at the back. Populated lazily on read/write; a
    /// cold-start eviction falls back to mtime.
    order: Mutex<VecDeque<(String, u64)>>,
    /// Free-space floor on the cache filesystem; writes are skipped below it.
    min_free_bytes: u64,
}

impl DiskCache {
    fn new(
        dir: PathBuf,
        max_bytes: usize,
        block_size: usize,
        reserve_diskfree: u64,
        free_space_ratio: Option<f64>,
    ) -> Result<Self> {
        let max_bytes =
            max_bytes.div_ceil(DISK_CACHE_BUDGET_UNIT) as u64 * DISK_CACHE_BUDGET_UNIT as u64;
        std::fs::create_dir_all(&dir).context("create disk cache dir")?;
        let min_free_bytes = {
            let (total, _avail) = disk_space(&dir).unwrap_or((0, 0));
            min_free_bytes(reserve_diskfree, free_space_ratio, total)
        };
        let block_size = Self::load_or_init_block_size(&dir, block_size)?;
        let cache = Self {
            dir,
            max_bytes,
            block_size,
            used: AtomicU64::new(0),
            order: Mutex::new(VecDeque::new()),
            min_free_bytes,
        };
        cache.load_order();
        if cache.order.lock().unwrap().is_empty() {
            cache.rebuild_order_from_mtime();
        }
        cache.rescan_used();
        Ok(cache)
    }

    fn load_or_init_block_size(dir: &Path, requested: usize) -> Result<u64> {
        let requested = requested.max(1) as u64;
        let meta = dir.join("cache.meta");
        let existing = std::fs::read_to_string(&meta).ok().and_then(|raw| {
            let mut version = None;
            let mut block_size = None;
            for line in raw.lines() {
                if let Some(v) = line.strip_prefix("version=") {
                    version = v.parse::<u64>().ok();
                } else if let Some(v) = line.strip_prefix("block_size=") {
                    block_size = v.parse::<u64>().ok();
                }
            }
            Some((version, block_size))
        });

        let reuse = matches!(
            existing,
            Some((Some(DISK_CACHE_META_VERSION), Some(block))) if block == requested
        );
        if !reuse {
            Self::clear_blocks(dir);
            std::fs::write(
                &meta,
                format!("version={DISK_CACHE_META_VERSION}\nblock_size={requested}\n"),
            )?;
        }
        Ok(requested)
    }

    fn clear_blocks(dir: &Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let ext = path.extension().and_then(|e| e.to_str());
                if ext == Some("blk")
                    || ext == Some("etag")
                    || path.file_name().and_then(|n| n.to_str()) == Some("lru.order")
                {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
    }

    fn etag_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.etag", fnv1a64(key)))
    }

    fn read_etag(&self, key: &str) -> Option<String> {
        std::fs::read_to_string(self.etag_path(key)).ok()
    }

    fn store_etag(&self, key: &str, etag: &str) {
        let _ = std::fs::write(self.etag_path(key), etag);
    }

    fn order_path(&self) -> PathBuf {
        self.dir.join("lru.order")
    }

    fn load_order(&self) {
        let Ok(raw) = std::fs::read_to_string(self.order_path()) else {
            return;
        };
        let mut order = self.order.lock().unwrap();
        for line in raw.lines() {
            let Some((block, hex)) = line.split_once(' ') else {
                continue;
            };
            let Ok(block) = block.parse::<u64>() else {
                continue;
            };
            let Some(key) = hex_decode(hex) else {
                continue;
            };
            order.push_back((key, block));
        }
    }

    fn save_order(&self) {
        let order = self.order.lock().unwrap();
        let mut out = String::new();
        for (key, block) in order.iter() {
            out.push_str(&format!("{block} {}\n", hex_encode(key)));
        }
        let _ = std::fs::write(self.order_path(), out);
    }

    fn path_for(&self, key: &str, block: u64) -> PathBuf {
        self.dir.join(format!("{}-{:08x}.blk", fnv1a64(key), block))
    }

    fn read_block(&self, key: &str, block: u64) -> Option<Vec<u8>> {
        let path = self.path_for(key, block);
        let raw = std::fs::read(&path).ok()?;
        if raw.len() < 4 + 8 {
            return None;
        }
        let klen = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
        if raw.len() < 4 + klen + 8 || &raw[4..4 + klen] != key.as_bytes() {
            let _ = std::fs::remove_file(&path);
            return None;
        }
        let crc = u64::from_le_bytes(raw[4 + klen..4 + klen + 8].try_into().unwrap());
        let data = raw[4 + klen + 8..].to_vec();
        if crc64ecma(&data) != crc {
            let _ = std::fs::remove_file(&path);
            return None;
        }
        self.touch(key, block);
        Some(data)
    }

    fn write_block(&self, key: &str, block: u64, data: &[u8]) -> Result<()> {
        let header_len = (4 + key.len() + 8 + data.len()) as u64;
        if self.min_free_bytes > 0
            && let Some((_, avail)) = disk_space(&self.dir)
            && avail.saturating_sub(header_len) < self.min_free_bytes
        {
            // Refusing to cache keeps the disk above the free-space floor.
            // The read still succeeds; it is just not persisted locally.
            return Ok(());
        }
        let mut header = Vec::with_capacity(4 + key.len() + 8 + data.len());
        header.extend_from_slice(&(key.len() as u32).to_le_bytes());
        header.extend_from_slice(key.as_bytes());
        let crc = crc64ecma(data);
        header.extend_from_slice(&crc.to_le_bytes());
        header.extend_from_slice(data);
        let final_path = self.path_for(key, block);
        let tmp = self
            .dir
            .join(format!(".tmp-{:x}", fnv1a64(key).wrapping_add(block)));
        std::fs::write(&tmp, &header).context("write disk cache block")?;
        std::fs::rename(&tmp, &final_path).context("commit disk cache block")?;
        self.touch(key, block);
        let bytes = header.len() as u64;
        self.used.fetch_add(bytes, Ordering::Relaxed);
        if self.used.load(Ordering::Relaxed) > self.max_bytes {
            self.evict();
        }
        self.save_order();
        Ok(())
    }

    fn touch(&self, key: &str, block: u64) {
        let mut order = self.order.lock().unwrap();
        if let Some(pos) = order.iter().position(|(k, b)| k == key && *b == block) {
            if let Some(entry) = order.remove(pos) {
                order.push_back(entry);
            }
        } else {
            order.push_back((key.to_string(), block));
        }
    }

    fn evict(&self) {
        let mut used = self.used.load(Ordering::Relaxed);
        while used > self.max_bytes {
            let (key, block) = {
                let mut order = self.order.lock().unwrap();
                let Some(entry) = order.pop_front() else {
                    break;
                };
                entry
            };
            let path = self.path_for(&key, block);
            if let Ok(meta) = std::fs::metadata(&path) {
                let len = meta.len();
                if std::fs::remove_file(&path).is_ok() {
                    used = used.saturating_sub(len);
                }
            }
        }
        self.used.store(used, Ordering::Relaxed);

        // Cold start (or stale order) fallback: if the in-memory LRU did not
        // bring us under budget, fall back to oldest-mtime eviction.
        if self.used.load(Ordering::Relaxed) > self.max_bytes {
            self.evict_by_mtime();
        }
    }

    fn evict_by_mtime(&self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        let mut files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "blk").unwrap_or(false))
            .filter_map(|e| {
                let meta = e.metadata().ok()?;
                Some((e.path(), meta.modified().ok()?, meta.len()))
            })
            .collect();
        files.sort_by(|a, b| b.1.cmp(&a.1)); // newest first
        let mut used = self.used.load(Ordering::Relaxed);
        for (path, _mtime, len) in files.iter().rev() {
            if used <= self.max_bytes {
                break;
            }
            if std::fs::remove_file(path).is_ok() {
                used = used.saturating_sub(*len);
            }
        }
        self.used.store(used, Ordering::Relaxed);
    }
    fn rebuild_order_from_mtime(&self) {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().and_then(|x| x.to_str()) != Some("blk") {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let stem = name.strip_suffix(".blk").unwrap_or(name);
                let Some(block) = stem
                    .rsplit_once('-')
                    .and_then(|(_, h)| u64::from_str_radix(h, 16).ok())
                else {
                    continue;
                };
                let Ok(raw) = std::fs::read(&path) else {
                    continue;
                };
                if raw.len() < 4 {
                    continue;
                }
                let klen = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
                if raw.len() < 4 + klen {
                    continue;
                }
                let key = String::from_utf8_lossy(&raw[4..4 + klen]).to_string();
                let mtime = e
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                files.push((mtime, key, block));
            }
        }
        files.sort_by_key(|(mtime, _, _)| *mtime);
        let mut order = self.order.lock().unwrap();
        for (_, key, block) in files {
            order.push_back((key, block));
        }
    }

    fn rescan_used(&self) {
        let mut used = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) != Some("blk") {
                    continue;
                }
                if let Ok(meta) = e.metadata() {
                    used += meta.len();
                }
            }
        }
        self.used.store(used, Ordering::Relaxed);
    }

    fn invalidate(&self, key: &str) {
        let _ = std::fs::remove_file(self.etag_path(key));
        self.order.lock().unwrap().retain(|(k, _)| k != key);
        self.save_order();
        let prefix = format!("{}-", fnv1a64(key));
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) != Some("blk") {
                    continue;
                }
                let path = e.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with(&prefix) {
                    if let Ok(meta) = e.metadata() {
                        let len = meta.len();
                        if std::fs::remove_file(&path).is_ok() {
                            self.used.fetch_sub(len, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    fn clear(&self) {
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if entry.path().extension().and_then(|e| e.to_str()) == Some("etag") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                if e.path().extension().and_then(|x| x.to_str()) != Some("blk") {
                    continue;
                }
                let _ = std::fs::remove_file(e.path());
            }
        }
        self.order.lock().unwrap().clear();
        self.save_order();
        self.used.store(0, Ordering::Relaxed);
    }
}

impl Drop for DiskCache {
    fn drop(&mut self) {
        self.save_order();
    }
}

/// FNV-1a 64-bit hash used for disk-cache block file names.
fn hex_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn hex_decode(s: &str) -> Option<String> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    String::from_utf8(out).ok()
}

fn fnv1a64(s: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Bounded read-ahead cache shared by all paths in one mount.
#[derive(Default)]
struct ReadCache {
    entries: HashMap<String, ReadCacheEntry>,
    bytes: usize,
}

/// Monotonic operation counters exposed by [`ObjectFs::metrics`].
#[derive(Default)]
pub struct Metrics {
    reads: AtomicU64,
    writes: AtomicU64,
    s3_gets: AtomicU64,
    s3_heads: AtomicU64,
    s3_stat_heads: AtomicU64,
    stat_cache_hits: AtomicU64,
    stat_positive_cache_hits: AtomicU64,
    stat_negative_cache_hits: AtomicU64,
    s3_etag_heads: AtomicU64,
    s3_lists: AtomicU64,
    s3_puts: AtomicU64,
    s3_errors: AtomicU64,
    s3_get_errors: AtomicU64,
    s3_list_errors: AtomicU64,
    s3_put_errors: AtomicU64,
    s3_delete_errors: AtomicU64,
    s3_multipart_errors: AtomicU64,
    upload_bytes_total: AtomicU64,
    download_bytes_total: AtomicU64,
    read_cache_hits: AtomicU64,
    read_cache_misses: AtomicU64,
    disk_cache_hits: AtomicU64,
    disk_cache_misses: AtomicU64,
    prefetch_started: AtomicU64,
    prefetch_skipped: AtomicU64,
    prefetch_failed: AtomicU64,
    list_throttled: AtomicU64,
    crc64_mismatches: AtomicU64,
}

/// Point-in-time snapshot of [`Metrics`] counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub s3_gets: u64,
    pub s3_heads: u64,
    pub s3_stat_heads: u64,
    pub stat_cache_hits: u64,
    pub stat_positive_cache_hits: u64,
    pub stat_negative_cache_hits: u64,
    pub s3_etag_heads: u64,
    pub s3_lists: u64,
    pub s3_puts: u64,
    pub s3_errors: u64,
    pub s3_get_errors: u64,
    pub s3_list_errors: u64,
    pub s3_put_errors: u64,
    pub s3_delete_errors: u64,
    pub s3_multipart_errors: u64,
    pub upload_bytes_total: u64,
    pub download_bytes_total: u64,
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub disk_cache_hits: u64,
    pub disk_cache_misses: u64,
    pub prefetch_started: u64,
    pub prefetch_inflight: usize,
    pub prefetch_skipped: u64,
    pub prefetch_failed: u64,
    pub list_throttled: u64,
    pub crc64_mismatches: u64,
}

impl Metrics {
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            s3_gets: self.s3_gets.load(Ordering::Relaxed),
            s3_heads: self.s3_heads.load(Ordering::Relaxed),
            s3_stat_heads: self.s3_stat_heads.load(Ordering::Relaxed),
            stat_cache_hits: self.stat_cache_hits.load(Ordering::Relaxed),
            stat_positive_cache_hits: self.stat_positive_cache_hits.load(Ordering::Relaxed),
            stat_negative_cache_hits: self.stat_negative_cache_hits.load(Ordering::Relaxed),
            s3_etag_heads: self.s3_etag_heads.load(Ordering::Relaxed),
            s3_lists: self.s3_lists.load(Ordering::Relaxed),
            s3_puts: self.s3_puts.load(Ordering::Relaxed),
            s3_errors: self.s3_errors.load(Ordering::Relaxed),
            s3_get_errors: self.s3_get_errors.load(Ordering::Relaxed),
            s3_list_errors: self.s3_list_errors.load(Ordering::Relaxed),
            s3_put_errors: self.s3_put_errors.load(Ordering::Relaxed),
            s3_delete_errors: self.s3_delete_errors.load(Ordering::Relaxed),
            s3_multipart_errors: self.s3_multipart_errors.load(Ordering::Relaxed),
            upload_bytes_total: self.upload_bytes_total.load(Ordering::Relaxed),
            download_bytes_total: self.download_bytes_total.load(Ordering::Relaxed),
            read_cache_hits: self.read_cache_hits.load(Ordering::Relaxed),
            read_cache_misses: self.read_cache_misses.load(Ordering::Relaxed),
            disk_cache_hits: self.disk_cache_hits.load(Ordering::Relaxed),
            disk_cache_misses: self.disk_cache_misses.load(Ordering::Relaxed),
            prefetch_started: self.prefetch_started.load(Ordering::Relaxed),
            prefetch_inflight: 0,
            prefetch_skipped: self.prefetch_skipped.load(Ordering::Relaxed),
            prefetch_failed: self.prefetch_failed.load(Ordering::Relaxed),
            list_throttled: self.list_throttled.load(Ordering::Relaxed),
            crc64_mismatches: self.crc64_mismatches.load(Ordering::Relaxed),
        }
    }
}

/// A streaming multipart upload handle. Bytes are fed via [`StreamingUpload::write`]
/// and uploaded as [`MULTIPART_PART_SIZE`] parts in the background (bounded by
/// `part_sem`), so upload overlaps with the local write and the process never
/// holds the whole object in memory.
pub struct StreamingUpload {
    client: Client,
    bucket: String,
    key: String,
    upload_id: String,
    next_part: i32,
    parts: Vec<(i32, String)>,
    pending: Vec<u8>,
    hasher: Crc64Ecma,
    verify_crc64: bool,
    content_md5: bool,
    part_sem: Arc<Semaphore>,
    limiter: Arc<Semaphore>,
    tasks: tokio::task::JoinSet<anyhow::Result<(i32, String)>>,
    metrics: Arc<Metrics>,
}

impl StreamingUpload {
    /// Feed `data` into the upload. Buffers until a full part is ready, then
    /// uploads it in the background.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.hasher.update(data);
        self.pending.extend_from_slice(data);
        while self.pending.len() >= MULTIPART_PART_SIZE as usize {
            let chunk: Vec<u8> = self.pending.drain(..MULTIPART_PART_SIZE as usize).collect();
            self.upload_part(chunk).await?;
        }
        Ok(())
    }

    async fn upload_part(&mut self, chunk: Vec<u8>) -> Result<()> {
        let part_no = self.next_part;
        self.next_part += 1;
        let slot = self
            .part_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("multipart concurrency closed"))?;
        let md5 = self.content_md5.then(|| content_md5(&chunk));
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let limiter = Arc::clone(&self.limiter);
        self.tasks.spawn(async move {
            let _permit = limiter
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("s3 request limiter closed"))?;
            let mut part = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_no)
                .body(ByteStream::from(chunk));
            if let Some(md5) = md5 {
                part = part.content_md5(md5);
            }
            let resp = part.send().await.context("s3 upload part")?;
            let etag = resp.e_tag().unwrap_or_default().to_string();
            drop(slot);
            Ok((part_no, etag))
        });
        Ok(())
    }

    /// Flush the final partial part, await all parts, and complete the upload.
    pub async fn finish(mut self) -> Result<()> {
        if !self.pending.is_empty() {
            let chunk = std::mem::take(&mut self.pending);
            self.upload_part(chunk).await?;
        }
        let mut upload_error = None;
        while let Some(joined) = self.tasks.join_next().await {
            match joined {
                Ok(Ok((part_no, etag))) => self.parts.push((part_no, etag)),
                Ok(Err(e)) => upload_error = Some(e),
                Err(e) => upload_error = Some(anyhow::anyhow!("multipart task panicked: {e}")),
            }
        }
        if let Some(e) = upload_error {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .await;
            return Err(e);
        }
        self.parts.sort_by_key(|p| p.0);
        let expected_crc = self.verify_crc64.then(|| self.hasher.finalize());
        let crc_slot = Arc::new(Mutex::new(None));
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(
                        self.parts
                            .into_iter()
                            .map(|(n, etag)| {
                                CompletedPart::builder().part_number(n).e_tag(etag).build()
                            })
                            .collect(),
                    ))
                    .build(),
            )
            .customize();
        if expected_crc.is_some() {
            complete = complete.interceptor(Crc64ResponseCapture {
                slot: Arc::clone(&crc_slot),
            });
        }
        if let Err(e) = complete.send().await {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .send()
                .await;
            return Err(e).context("s3 complete multipart upload");
        }
        if let Some(expected) = expected_crc {
            check_crc64_response(crc_slot, expected, &self.metrics)?;
        }
        Ok(())
    }
}

pub struct ObjectFs {
    client: Client,
    bucket: String,
    prefix: String,
    /// Short-TTL attribute cache: path -> (cached_at, entry).
    stats: Mutex<HashMap<String, (Instant, DirEntry)>>,
    /// Negative stat cache: path -> cached_at (path known missing).
    negative: Mutex<HashMap<String, Instant>>,
    /// Bounds in-flight S3 requests (see [`MAX_CONCURRENT_S3_REQUESTS`]).
    limiter: Arc<Semaphore>,
    /// Optional directory-enumeration rate limiter (see [OssConfig::list_rate_limit]).
    list_rate: Option<Mutex<TokenBucket>>,
    /// Read-only mount: reject all mutations.
    read_only: bool,
    /// Open the FUSE mount to all users (see [OssConfig::allow_other]).
    allow_other: bool,
    /// POSIX ownership / permission defaults for the FUSE adapters.
    mount_attr: MountAttr,
    /// Whether directory renames are allowed at all.
    allow_rename_dir: bool,
    /// Max object count for one directory rename; `None` = unlimited.
    rename_dir_limit: Option<u64>,
    /// In-flight write-byte budget (MiB-unit semaphore); `None` = unlimited.
    upload_budget: Option<Arc<Semaphore>>,
    /// Total MiB units available when [`Self::upload_budget`] is set.
    upload_budget_units: usize,
    /// Sequential-read prefetch window; 0 disables read-ahead.
    read_ahead_window: usize,
    /// Bounded read-ahead cache.
    read_cache: Mutex<ReadCache>,
    /// Upper bound on in-memory read-ahead cache bytes.
    read_cache_max_bytes: usize,
    /// Optional block-oriented on-disk read cache.
    disk_cache: Option<Arc<DiskCache>>,
    /// Background prefetch depth for sequential disk-cache reads.
    disk_cache_prefetch_blocks: usize,
    /// In-flight prefetch dedup: `(key, block)` currently being prefetched.
    prefetch_inflight: Arc<Mutex<HashSet<(String, u64)>>>,
    /// Caps concurrent background prefetch tasks.
    prefetch_sem: Arc<Semaphore>,
    /// Verify object ETag with HEAD before serving disk-cache blocks.
    disk_cache_verify_etag: bool,
    /// path -> last successful ETag check (short TTL, see [`ETAG_CHECK_TTL`]).
    etag_checked: Mutex<HashMap<String, Instant>>,
    /// TTL for cached ETag checks.
    etag_ttl: Duration,
    negative_ttl: Duration,
    stat_ttl: Duration,
    negative_max_entries: usize,
    stat_max_entries: usize,

    /// Prefetch dedup skips and failures are tracked inside [`Metrics`].
    /// path -> end offset of its previous read (sequential-read hint).
    read_seq: Mutex<HashMap<String, u64>>,
    /// Whether FUSE fsync should be a no-op (whole-file buffered writes).
    ignore_fsync: bool,
    /// Verify uploaded objects via x-oss-hash-crc64ecma (see [OssConfig::verify_crc64]).
    verify_crc64: bool,
    /// Storage class for newly written objects (see [OssConfig::storage_class]).
    storage_class: Option<StorageClass>,
    /// Set Content-MD5 on uploads (see [OssConfig::content_md5]).
    content_md5: bool,
    /// Skip legacy `_$folder$` directory markers (see [OssConfig::notsup_compat_dir]).
    notsup_compat_dir: bool,
    /// Multipart upload part size in bytes.
    multipart_part_size: usize,
    /// Concurrent in-flight part uploads within a single multipart write.
    multipart_concurrency: usize,
    /// Monotonic operation counters.
    metrics: Arc<Metrics>,
    /// Dirty-buffer budget for the adapters; None when unlimited.
    dirty_budget: Option<DirtyBudget>,
}

impl ObjectFs {
    /// Build the S3 client from environment credentials
    /// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or the shared config
    /// file), which is how the desktop tray app spawns mounts.
    pub async fn connect(config: OssConfig) -> Result<Self> {
        let config = config.normalize();
        let loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region.clone()))
            .load()
            .await;
        let mut builder = aws_sdk_s3::config::Builder::from(&loader);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        if config.force_path_style {
            builder = builder.force_path_style(true);
        }
        if let Some(command) = &config.credential_process {
            builder = builder.credentials_provider(CredentialProcessProvider::new(command.clone()));
        }
        if config.connect_timeout_secs.is_some() || config.readwrite_timeout_secs.is_some() {
            let mut timeout = aws_smithy_types::timeout::TimeoutConfig::builder();
            if let Some(secs) = config.connect_timeout_secs {
                timeout = timeout.connect_timeout(Duration::from_secs(secs));
            }
            if let Some(secs) = config.readwrite_timeout_secs {
                timeout = timeout.read_timeout(Duration::from_secs(secs));
            }
            let existing = loader
                .timeout_config()
                .cloned()
                .map(aws_smithy_types::timeout::TimeoutConfig::into_builder)
                .unwrap_or_else(aws_smithy_types::timeout::TimeoutConfig::builder);
            builder = builder.timeout_config(timeout.take_unset_from(existing).build());
        }
        if let Some(retries) = config.retries {
            builder = builder.retry_config(
                aws_smithy_types::retry::RetryConfig::standard()
                    .with_max_attempts(s3_max_attempts(retries)),
            );
        }
        let client = Client::from_conf(builder.build());
        let (max_upload_bytes, max_dirty_bytes, read_cache_max_bytes) = effective_memory_budgets(
            config.total_mem_limit,
            config.total_mem_read_ratio,
            config.max_upload_bytes,
            config.max_dirty_bytes,
            config.read_cache_max_bytes,
        );
        let upload_budget_units = upload_budget_units(max_upload_bytes);
        let read_ahead_window = config.read_ahead_bytes.unwrap_or(0);
        let ignore_fsync = config.ignore_fsync;
        let dirty_budget = DirtyBudget::new(max_dirty_bytes.unwrap_or(0));
        let disk_cache = match &config.disk_cache_dir {
            Some(dir) if config.disk_cache_max_bytes > 0 => Some(Arc::new(DiskCache::new(
                dir.clone(),
                config.disk_cache_max_bytes,
                config
                    .disk_cache_block_size
                    .unwrap_or(DISK_CACHE_BLOCK_SIZE as usize),
                config.disk_cache_reserve_diskfree,
                config.disk_cache_free_space_ratio,
            )?)),
            _ => None,
        };
        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(effective_max_concurrent_requests(
                config.max_concurrent_requests,
            ))),
            list_rate: config
                .list_rate_limit
                .filter(|r| *r > 0.0)
                .map(|r| Mutex::new(TokenBucket::new(r))),
            read_only: config.read_only,
            allow_other: config.allow_other,
            mount_attr: MountAttr {
                uid: config.uid,
                gid: config.gid,
                dir_mode: config.dir_mode,
                file_mode: config.file_mode,
                umask: config.umask,
            },
            allow_rename_dir: config.allow_rename_dir,
            rename_dir_limit: config.rename_dir_limit,
            upload_budget: upload_budget_units.map(|units| Arc::new(Semaphore::new(units))),
            upload_budget_units: upload_budget_units.unwrap_or(0),
            read_ahead_window,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes,
            disk_cache,
            disk_cache_prefetch_blocks: config.disk_cache_prefetch_blocks,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(
                config.disk_cache_prefetch_concurrency.max(1),
            )),
            disk_cache_verify_etag: config.disk_cache_verify_etag,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: Duration::from_secs(config.disk_cache_etag_ttl_secs.max(1)),
            negative_ttl: Duration::from_secs(config.negative_cache_ttl_secs.max(1)),
            stat_ttl: Duration::from_secs(config.stat_cache_ttl_secs.max(1)),
            negative_max_entries: config.negative_cache_max_entries.max(1),
            stat_max_entries: config.stat_cache_max_entries.max(1),
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync,
            verify_crc64: config.verify_crc64,
            storage_class: config.storage_class.map(|s| StorageClass::from(s.as_str())),
            content_md5: config.content_md5,
            notsup_compat_dir: config.notsup_compat_dir,
            multipart_part_size: config
                .multipart_size
                .unwrap_or(MULTIPART_PART_SIZE as usize)
                .max(5 * 1024 * 1024),
            multipart_concurrency: config
                .multipart_concurrency
                .unwrap_or(MULTIPART_UPLOAD_CONCURRENCY)
                .max(1),
            metrics: Arc::new(Metrics::default()),
            dirty_budget,
        })
    }

    /// Read-only state of this mount.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Whether FUSE fsync should be ignored (whole-file buffered write model).
    pub fn ignore_fsync(&self) -> bool {
        self.ignore_fsync
    }

    /// Shared dirty-buffer budget for the adapters, when configured.
    /// Snapshot of monotonic operation counters (for an admin/metrics endpoint).
    pub fn metrics(&self) -> MetricsSnapshot {
        let mut snapshot = self.metrics.snapshot();
        snapshot.prefetch_inflight = self.prefetch_inflight.lock().unwrap().len();
        snapshot
    }

    pub fn dirty_budget(&self) -> Option<DirtyBudget> {
        self.dirty_budget.clone()
    }

    /// POSIX ownership / permission defaults applied by the FUSE adapters.
    pub fn mount_attr(&self) -> MountAttr {
        self.mount_attr
    }

    /// Whether the FUSE mount is opened to all users (`allow_other`).
    pub fn allow_other(&self) -> bool {
        self.allow_other
    }

    /// Acquire the in-flight write-byte budget for `data_len` bytes.
    /// Returns a permit that must be held for the whole upload.
    async fn acquire_upload_budget(
        &self,
        data_len: usize,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>> {
        let Some(sem) = &self.upload_budget else {
            return Ok(None);
        };
        let units = data_len.div_ceil(UPLOAD_BUDGET_UNIT);
        if units > self.upload_budget_units {
            anyhow::bail!(
                "write of {data_len} bytes exceeds max-upload-bytes budget ({})",
                self.upload_budget_units.saturating_mul(UPLOAD_BUDGET_UNIT)
            );
        }
        if units == 0 {
            return Ok(None);
        }
        Ok(Some(
            sem.clone()
                .acquire_many_owned(units as u32)
                .await
                .map_err(|_| anyhow::anyhow!("upload budget closed"))?,
        ))
    }

    /// Reject mutations when mounted read-only.
    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            anyhow::bail!("filesystem is mounted read-only");
        }
        Ok(())
    }

    /// Acquire a permit bounding in-flight S3 operations. Every public
    /// S3-facing method takes exactly one permit for its whole body (mkdir
    /// takes one indirectly via write); internal `*_impl` helpers never
    /// acquire, so cross-calls cannot deadlock even when the pool is
    /// saturated. Keep this invariant: public methods acquire, `*_impl`
    /// helpers never do.
    async fn acquire(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        self.limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("s3 request limiter closed"))
    }

    /// Full object key for a normalized POSIX path (see module docs).
    pub fn key_for(&self, path: &str) -> String {
        let rel = rel_key(path);
        if rel.is_empty() {
            self.prefix.trim_end_matches('/').to_string()
        } else {
            format!("{}{}", self.prefix, rel)
        }
    }

    /// S3 list prefix for the children of `dir` (always ends with `/`).
    fn list_prefix(&self, dir: &str) -> String {
        if dir == "/" {
            self.prefix.clone()
        } else {
            format!("{}{}/", self.prefix, rel_key(dir))
        }
    }

    /// List the immediate children of `dir`.
    pub async fn list(&self, dir: &str) -> Result<Vec<DirEntry>> {
        self.acquire_list_permit().await;
        let _permit = self.acquire().await?;
        self.list_impl(dir).await
    }

    /// Await a token from the directory-enumeration rate limiter, if set.
    async fn acquire_list_permit(&self) {
        let Some(rate) = &self.list_rate else { return };
        loop {
            let wait = rate.lock().unwrap().reserve(Instant::now());
            let Some(wait) = wait else { return };
            self.metrics.list_throttled.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(wait).await;
        }
    }

    async fn list_impl(&self, dir: &str) -> Result<Vec<DirEntry>> {
        self.metrics.s3_lists.fetch_add(1, Ordering::Relaxed);
        let prefix = self.list_prefix(dir);
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .delimiter("/");
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                    self.metrics.s3_list_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e).context("s3 list");
                }
            };
            for cp in resp.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    let name = p
                        .strip_prefix(&prefix)
                        .unwrap_or(p)
                        .trim_end_matches('/')
                        .to_string();
                    if !name.is_empty() {
                        out.push(DirEntry {
                            name,
                            is_dir: true,
                            size: 0,
                            mtime_secs: 0,
                        });
                    }
                }
            }
            for obj in resp.contents() {
                let Some(key) = obj.key() else { continue };
                // The directory marker (key == list prefix) is the dir itself.
                if key == prefix {
                    continue;
                }
                let Some(name) = key.strip_prefix(&prefix) else {
                    continue;
                };
                if name.is_empty() || name.ends_with('/') {
                    continue;
                }
                if self.notsup_compat_dir && name.ends_with("_$folder$") {
                    continue;
                }
                out.push(DirEntry {
                    name: name.to_string(),
                    is_dir: false,
                    size: obj.size().unwrap_or(0).max(0) as u64,
                    mtime_secs: obj.last_modified().map(|d| d.secs()).unwrap_or(0),
                });
            }
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(out)
    }

    /// Stat a path. Returns `None` when the path does not exist.
    ///
    /// Results are cached for [`STAT_TTL`] so the repeated attribute queries
    /// Explorer makes on a click (get_file_info / get_security_by_name / open)
    /// do not each pay an S3 round trip.
    pub async fn stat(&self, path: &str) -> Result<Option<DirEntry>> {
        {
            let cache = self.stats.lock().unwrap();
            if let Some((at, entry)) = cache.get(path) {
                if at.elapsed() < self.stat_ttl {
                    self.metrics.stat_cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .stat_positive_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(entry.clone()));
                }
            }
        }
        if self.negative_hit(path) {
            self.metrics.stat_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .stat_negative_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let _permit = self.acquire().await?;
        let result = self.stat_uncached_impl(path).await?;
        if let Some(entry) = &result {
            self.cache_insert(path, entry.clone());
        } else {
            self.negative_insert(path);
        }
        Ok(result)
    }

    fn negative_hit(&self, path: &str) -> bool {
        matches!(
            self.negative.lock().unwrap().get(path),
            Some(at) if at.elapsed() < self.negative_ttl
        )
    }

    /// Record `path` as missing (bounded; evicts the oldest entry when full).
    /// positive cache).
    fn negative_insert(&self, path: &str) {
        let mut cache = self.negative.lock().unwrap();
        if cache.len() >= self.negative_max_entries && !cache.contains_key(path) {
            let oldest = cache
                .iter()
                .min_by_key(|(_, at)| *at)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                cache.remove(&k);
            }
        }
        cache.insert(path.to_string(), Instant::now());
    }

    fn cache_insert(&self, path: &str, entry: DirEntry) {
        let mut cache = self.stats.lock().unwrap();
        if cache.len() >= self.stat_max_entries && !cache.contains_key(path) {
            let oldest = cache
                .iter()
                .min_by_key(|(_, (at, _))| *at)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                cache.remove(&k);
            }
        }
        cache.insert(path.to_string(), (Instant::now(), entry));
    }

    /// Drop any cached attribute (positive or negative) for `path`, called
    /// after local mutations.
    fn invalidate_stat(&self, path: &str) {
        self.stats.lock().unwrap().remove(path);
        self.negative.lock().unwrap().remove(path);
    }

    /// The actual S3 lookup behind [`Self::stat`] (HEAD, then directory-marker
    /// HEAD, then prefix probe as a last resort). Caller must hold a limiter
    /// permit.
    async fn stat_uncached_impl(&self, path: &str) -> Result<Option<DirEntry>> {
        self.metrics.s3_heads.fetch_add(1, Ordering::Relaxed);
        self.metrics.s3_stat_heads.fetch_add(1, Ordering::Relaxed);
        if path == "/" {
            return Ok(Some(DirEntry {
                name: String::new(),
                is_dir: true,
                size: 0,
                mtime_secs: 0,
            }));
        }
        let key = self.key_for(path);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(resp) => {
                let is_dir = path.ends_with('/') || key.ends_with('/');
                Ok(Some(DirEntry {
                    name: basename(path),
                    is_dir,
                    size: resp.content_length().unwrap_or(0).max(0) as u64,
                    mtime_secs: resp.last_modified().map(|d| d.secs()).unwrap_or(0),
                }))
            }
            Err(e) if is_s3_not_found(&e) => {
                // A directory marker lives at `path + "/"`; check it before
                // falling back to a prefix scan.
                if !key.ends_with('/') {
                    let marker_key = format!("{key}/");
                    match self
                        .client
                        .head_object()
                        .bucket(&self.bucket)
                        .key(&marker_key)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            return Ok(Some(DirEntry {
                                name: basename(path),
                                is_dir: true,
                                size: resp.content_length().unwrap_or(0).max(0) as u64,
                                mtime_secs: resp.last_modified().map(|d| d.secs()).unwrap_or(0),
                            }));
                        }
                        Err(e2) if is_s3_not_found(&e2) => {}
                        Err(e2) => return Err(e2).context("s3 head marker"),
                    }
                }
                // Implied directory (children exist under the prefix).
                // Probe with max_keys=1 instead of materializing a full
                // listing: stat storms on missing paths otherwise allocate a
                // whole directory just to learn "has children".
                if !path.ends_with('/') && self.has_children_impl(path).await? {
                    return Ok(Some(DirEntry {
                        name: basename(path),
                        is_dir: true,
                        size: 0,
                        mtime_secs: 0,
                    }));
                }
                Ok(None)
            }
            Err(e) => Err(e).context("s3 head"),
        }
    }

    /// Cheap existence probe: does `dir` have any child object? Uses
    /// `max_keys = 1` so a missing implied directory costs one tiny request
    /// instead of a full listing. Caller must hold a limiter permit.
    async fn has_children_impl(&self, dir: &str) -> Result<bool> {
        let prefix = self.list_prefix(dir);
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .max_keys(1)
            .send()
            .await
            .context("s3 probe children")?;
        Ok(!resp.contents().is_empty())
    }

    /// Read `len` bytes starting at `offset`. Returns fewer bytes near EOF,
    /// empty when `offset` is at/behind EOF.
    ///
    /// When [`Self::read_ahead_window`] is enabled and the read continues the
    /// previous read (`offset == last_end`), the object is fetched in
    /// window-sized chunks and cached so subsequent sequential small reads do
    /// not pay an S3 round trip each.
    pub async fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        if len == 0 {
            return Ok(Vec::new());
        }
        let window = self.read_ahead_window;
        let key = self.key_for(path);

        if window > 0
            && let Some(data) = self.read_cache_hit(path, offset, len)
        {
            self.metrics.read_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.note_read_end(path, offset.saturating_add(data.len() as u64));
            return Ok(data);
        }

        if window > 0 {
            self.metrics
                .read_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        let sequential = if window > 0 {
            let seq = self.read_seq.lock().unwrap();
            seq.get(path) == Some(&offset)
        } else {
            false
        };

        // Warm the sequential hint so the next contiguous read can trigger
        // prefetch even if this one is served directly.
        if window > 0 {
            self.note_read_end(path, offset.saturating_add(len as u64));
        }

        let fetch_len = read_fetch_len(len, window, sequential);

        let _permit = self.acquire().await?;
        let data = if self.disk_cache.is_some() {
            self.read_range_disk(&key, offset, fetch_len, sequential)
                .await?
        } else {
            self.read_range_uncached(&key, offset, fetch_len).await?
        };

        if window > 0 && fetch_len > len {
            self.insert_read_cache(path, offset, data.clone());
        }
        if window > 0 {
            self.note_read_end(path, offset.saturating_add(data.len() as u64));
        }

        Ok(data[..len.min(data.len())].to_vec())
    }

    /// Read `fetch_len` bytes at `offset`, sourcing each `DISK_CACHE_BLOCK_SIZE`
    /// block from the on-disk cache when present and otherwise fetching and
    /// storing it. Returns at most `fetch_len` bytes (fewer near EOF).
    async fn read_range_disk(
        &self,
        key: &str,
        offset: u64,
        fetch_len: usize,
        prefetch_next: bool,
    ) -> Result<Vec<u8>> {
        if self.disk_cache_verify_etag {
            self.verify_disk_cache_etag(key).await;
        }
        let cache = self.disk_cache.as_ref().expect("disk cache enabled");
        let block_size = cache.block_size;
        let end = offset.saturating_add(fetch_len as u64);
        let first_block = offset / block_size;
        let last_block = end.saturating_sub(1) / block_size;
        let mut out = Vec::with_capacity(fetch_len);
        let mut pos = offset;
        let mut eof = false;

        for block in first_block..=last_block {
            let block_start = block * block_size;
            let within = (pos - block_start) as usize;
            let want = (end - pos).min(block_size - within as u64) as usize;

            if let Some(block_data) = cache.read_block(key, block) {
                self.metrics.disk_cache_hits.fetch_add(1, Ordering::Relaxed);
                if within >= block_data.len() {
                    eof = true;
                    break;
                }
                let take = want.min(block_data.len() - within);
                out.extend_from_slice(&block_data[within..within + take]);
                pos += take as u64;
                if block_data.len() < block_size as usize || take < want {
                    eof = true;
                    break;
                }
                continue;
            }

            self.metrics
                .disk_cache_misses
                .fetch_add(1, Ordering::Relaxed);
            let fetched = self
                .read_range_uncached(key, block_start, block_size as usize)
                .await?;
            if fetched.is_empty() {
                eof = true;
                break;
            }
            let _ = cache.write_block(key, block, &fetched);

            if within >= fetched.len() {
                eof = true;
                break;
            }
            let take = want.min(fetched.len() - within);
            out.extend_from_slice(&fetched[within..within + take]);
            pos += take as u64;
            if fetched.len() < block_size as usize || take < want {
                eof = true;
                break;
            }
        }

        if prefetch_next && !eof && self.disk_cache_prefetch_blocks > 0 {
            self.metrics
                .prefetch_started
                .fetch_add(1, Ordering::Relaxed);
            let cache = Arc::clone(cache);
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let limiter = Arc::clone(&self.limiter);
            let key = key.to_string();
            let inflight = Arc::clone(&self.prefetch_inflight);
            let prefetch_sem = Arc::clone(&self.prefetch_sem);
            let metrics = Arc::clone(&self.metrics);
            let first_next = last_block + 1;
            let count = self.disk_cache_prefetch_blocks;
            tokio::spawn(async move {
                let Ok(_prefetch_guard) = prefetch_sem.acquire_owned().await else {
                    return;
                };
                for block in first_next..first_next + count as u64 {
                    {
                        let mut set = inflight.lock().unwrap();
                        if !set.insert((key.clone(), block)) {
                            metrics.prefetch_skipped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                    let Ok(_permit) = limiter.clone().acquire_owned().await else {
                        return;
                    };
                    let start = block * cache.block_size;
                    let range = format!("bytes={}-{}", start, start + cache.block_size - 1);
                    let Ok(resp) = client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .range(&range)
                        .send()
                        .await
                    else {
                        metrics.prefetch_failed.fetch_add(1, Ordering::Relaxed);
                        inflight.lock().unwrap().remove(&(key.clone(), block));
                        return;
                    };
                    if let Ok(body) = resp.body.collect().await {
                        let bytes = body.to_vec();
                        if bytes.is_empty() {
                            inflight.lock().unwrap().remove(&(key.clone(), block));
                            return;
                        }
                        let _ = cache.write_block(&key, block, &bytes);
                    } else {
                        metrics.prefetch_failed.fetch_add(1, Ordering::Relaxed);
                        inflight.lock().unwrap().remove(&(key.clone(), block));
                    }
                    inflight.lock().unwrap().remove(&(key.clone(), block));
                }
            });
        }

        Ok(out)
    }

    async fn verify_disk_cache_etag(&self, key: &str) {
        self.metrics.s3_heads.fetch_add(1, Ordering::Relaxed);
        self.metrics.s3_etag_heads.fetch_add(1, Ordering::Relaxed);
        {
            let checked = self.etag_checked.lock().unwrap();
            if let Some(at) = checked.get(key) {
                if at.elapsed() < self.etag_ttl {
                    return;
                }
            }
        }
        let cache = self.disk_cache.as_ref().expect("disk cache enabled");
        let Ok(resp) = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        else {
            return;
        };
        let Some(etag) = resp.e_tag().map(str::to_string) else {
            return;
        };
        if etag.is_empty() {
            return;
        }
        if cache.read_etag(key).as_deref() != Some(etag.as_str()) {
            cache.invalidate(key);
            cache.store_etag(key, &etag);
            self.etag_checked
                .lock()
                .unwrap()
                .insert(key.to_string(), Instant::now());
        }
    }

    /// Actual S3 GET for `len` bytes at `offset`. Caller holds a limiter
    /// permit.
    async fn read_range_uncached(&self, key: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.metrics.s3_gets.fetch_add(1, Ordering::Relaxed);
        let end = offset.saturating_add(len as u64);
        let range = if offset == 0 && len == usize::MAX {
            "bytes=0-".to_string()
        } else {
            format!("bytes={}-{}", offset, end.saturating_sub(1))
        };
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(&range)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) if is_s3_invalid_range(&e) => return Ok(Vec::new()),
            Err(e) => {
                self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                self.metrics.s3_get_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e).context("s3 get");
            }
        };
        let bytes = resp.body.collect().await.context("s3 get body")?.to_vec();
        self.metrics
            .download_bytes_total
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    /// Return a fully-cached window slice when `[offset, offset+len)` is
    /// entirely inside it.
    fn read_cache_hit(&self, path: &str, offset: u64, len: usize) -> Option<Vec<u8>> {
        let mut cache = self.read_cache.lock().unwrap();
        let entry = cache.entries.get_mut(path)?;
        let end = entry.start.checked_add(entry.data.len() as u64)?;
        if offset < entry.start || offset.saturating_add(len as u64) > end {
            return None;
        }
        let start = (offset - entry.start) as usize;
        entry.last_used = Instant::now();
        Some(entry.data[start..start + len].to_vec())
    }

    /// Insert a read-ahead window into the bounded cache, evicting arbitrary
    /// entries when the byte/entry budgets are exceeded.
    fn insert_read_cache(&self, path: &str, start: u64, data: Vec<u8>) {
        if data.is_empty() || data.len() > self.read_cache_max_bytes {
            return;
        }
        let mut cache = self.read_cache.lock().unwrap();
        if let Some(old) = cache.entries.remove(path) {
            cache.bytes = cache.bytes.saturating_sub(old.data.len());
        }
        while cache.bytes + data.len() > self.read_cache_max_bytes
            || cache.entries.len() >= READ_CACHE_MAX_ENTRIES
        {
            let key = cache
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            let Some(key) = key else { break };
            if let Some(evicted) = cache.entries.remove(&key) {
                cache.bytes = cache.bytes.saturating_sub(evicted.data.len());
            }
        }
        cache.bytes += data.len();
        cache.entries.insert(
            path.to_string(),
            ReadCacheEntry {
                start,
                data,
                last_used: Instant::now(),
            },
        );
    }

    /// Track the end of the most recent read for sequential-prefetch
    /// detection. The hint map is bounded like the stat/negative caches.
    fn note_read_end(&self, path: &str, end: u64) {
        let mut seq = self.read_seq.lock().unwrap();
        if seq.len() >= MAX_READ_SEQ_ENTRIES {
            seq.clear();
        }
        seq.insert(path.to_string(), end);
    }

    /// Drop cached read-ahead data for one path after a local mutation.
    fn invalidate_read_cache(&self, path: &str) {
        if let Some(cache) = &self.disk_cache {
            cache.invalidate(&self.key_for(path));
        }
        let mut cache = self.read_cache.lock().unwrap();
        if let Some(old) = cache.entries.remove(path) {
            cache.bytes = cache.bytes.saturating_sub(old.data.len());
        }
        self.read_seq.lock().unwrap().remove(path);
    }

    /// Drop all cached read-ahead data (used by recursive delete/rename).
    fn clear_read_cache(&self) {
        if let Some(cache) = &self.disk_cache {
            cache.clear();
        }
        let mut cache = self.read_cache.lock().unwrap();
        cache.entries.clear();
        cache.bytes = 0;
        self.read_seq.lock().unwrap().clear();
    }

    /// Overwrite an object with `data` (whole-object write). Large objects
    /// are uploaded via S3 multipart so they are not limited by the single-PUT
    /// object-size cap and can be retried per part.
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        self.metrics.s3_puts.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .upload_bytes_total
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.ensure_writable()?;
        self.invalidate_stat(path);
        self.invalidate_read_cache(path);
        let _budget = self.acquire_upload_budget(data.len()).await?;
        if data.len() as u64 > MULTIPART_THRESHOLD {
            self.write_multipart(path, data).await
        } else {
            let _permit = self.acquire().await?;
            self.put_whole_object(path, data).await
        }
    }

    async fn put_whole_object(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.key_for(path);
        let expected_crc = self.verify_crc64.then(|| crc64ecma(data));
        let crc_slot = Arc::new(Mutex::new(None));

        let mut put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data.to_vec()));
        if let Some(sc) = &self.storage_class {
            put = put.storage_class(sc.clone());
        }
        if self.content_md5 {
            put = put.content_md5(content_md5(data));
        }
        let mut put = put.customize();
        if expected_crc.is_some() {
            put = put.interceptor(Crc64ResponseCapture {
                slot: Arc::clone(&crc_slot),
            });
        }
        if let Err(e) = put.send().await {
            self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            self.metrics.s3_put_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e).context("s3 put");
        }

        if let Some(expected) = expected_crc {
            check_crc64_response(crc_slot, expected, &self.metrics)?;
        }
        Ok(())
    }

    /// Multipart upload: initiate -> upload parts (bounded concurrency, one
    /// global permit per part) -> complete. Any failure aborts the upload so
    /// no unfinished multipart upload is left behind on the bucket.
    async fn write_multipart(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.key_for(path);
        let expected_crc = self.verify_crc64.then(|| crc64ecma(data));
        let crc_slot = Arc::new(Mutex::new(None));

        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key);
        if let Some(sc) = &self.storage_class {
            create = create.storage_class(sc.clone());
        }
        let create = create
            .send()
            .await
            .inspect_err(|_| {
                self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                self.metrics
                    .s3_multipart_errors
                    .fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 create multipart upload")?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("s3 create multipart upload returned no upload id"))?
            .to_string();

        let local = Arc::new(Semaphore::new(self.multipart_concurrency));
        let mut handles = tokio::task::JoinSet::new();
        let mut part_number = 1i32;
        let mut offset = 0usize;

        while offset < data.len() {
            let end = (offset + self.multipart_part_size).min(data.len());
            // Wait for a local slot so at most self.multipart_concurrency
            // part chunks are materialized in memory at once.
            let slot = local
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("multipart upload concurrency closed"))?;
            let chunk = data[offset..end].to_vec();
            let part_md5 = self.content_md5.then(|| content_md5(&chunk));
            let part_no = part_number;
            let upload_id = upload_id.clone();
            let key = key.clone();
            let bucket = self.bucket.clone();
            let client = self.client.clone();
            let limiter = Arc::clone(&self.limiter);
            handles.spawn(async move {
                // Bound in-flight part uploads against the global limit too.
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("s3 request limiter closed"))?;
                let mut part = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_no)
                    .body(ByteStream::from(chunk));
                if let Some(md5) = part_md5 {
                    part = part.content_md5(md5);
                }
                let resp = part.send().await.context("s3 upload part")?;
                let etag = resp.e_tag().unwrap_or_default().to_string();
                drop(slot);
                Ok::<(i32, String), anyhow::Error>((part_no, etag))
            });
            part_number += 1;
            offset = end;
        }

        let mut parts = Vec::new();
        let mut upload_error = None;
        while let Some(joined) = handles.join_next().await {
            match joined {
                Ok(Ok((part_no, etag))) => {
                    parts.push(
                        CompletedPart::builder()
                            .part_number(part_no)
                            .e_tag(etag)
                            .build(),
                    );
                }
                Ok(Err(e)) => upload_error = Some(e),
                Err(e) => {
                    upload_error = Some(anyhow::anyhow!("multipart upload task panicked: {e}"))
                }
            }
        }

        if let Some(e) = upload_error {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        parts.sort_by_key(|p| p.part_number);
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .customize();
        if expected_crc.is_some() {
            complete = complete.interceptor(Crc64ResponseCapture {
                slot: Arc::clone(&crc_slot),
            });
        }
        if let Err(e) = complete.send().await {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e).context("s3 complete multipart upload");
        }
        if let Some(expected) = expected_crc {
            check_crc64_response(crc_slot, expected, &self.metrics)?;
        }
        Ok(())
    }

    /// Overwrite an object from a local file, streaming large files through
    /// multipart so the process never holds the whole object in memory. Used
    /// by the WinFsp adapter once a write buffer spills to disk.
    pub async fn write_from_file(&self, path: &str, src: &Path) -> Result<()> {
        let size = std::fs::metadata(src).context("stat spool file")?.len();
        self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        self.metrics.s3_puts.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .upload_bytes_total
            .fetch_add(size, Ordering::Relaxed);
        self.ensure_writable()?;
        self.invalidate_stat(path);
        self.invalidate_read_cache(path);
        let _budget = self.acquire_upload_budget(size as usize).await?;
        if size > MULTIPART_THRESHOLD {
            self.write_multipart_from_file(path, src, size).await
        } else {
            let data = tokio::fs::read(src).await.context("read spool file")?;
            let _permit = self.acquire().await?;
            self.put_whole_object(path, &data).await
        }
    }

    /// Multipart upload reading part chunks directly from `src`, bounded by
    /// [`Self::multipart_concurrency`] so memory stays at a few part sizes.
    async fn write_multipart_from_file(&self, path: &str, src: &Path, size: u64) -> Result<()> {
        let key = self.key_for(path);
        let crc_slot = Arc::new(Mutex::new(None));
        let mut hasher = self.verify_crc64.then(Crc64Ecma::new);

        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key);
        if let Some(sc) = &self.storage_class {
            create = create.storage_class(sc.clone());
        }
        let create = create
            .send()
            .await
            .inspect_err(|_| {
                self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                self.metrics
                    .s3_multipart_errors
                    .fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 create multipart upload")?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("s3 create multipart upload returned no upload id"))?
            .to_string();

        let local = Arc::new(Semaphore::new(self.multipart_concurrency));
        let mut handles = tokio::task::JoinSet::new();
        let mut file = tokio::fs::File::open(src)
            .await
            .context("open spool file")?;
        let part_size = self.multipart_part_size as u64;
        let mut remaining = size;
        let mut part_number = 1i32;

        while remaining > 0 {
            let chunk_len = part_size.min(remaining) as usize;
            let slot = local
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("multipart upload concurrency closed"))?;
            let mut chunk = vec![0u8; chunk_len];
            tokio::io::AsyncReadExt::read_exact(&mut file, &mut chunk)
                .await
                .context("read spool file chunk")?;
            if let Some(h) = &mut hasher {
                h.update(&chunk);
            }
            let part_md5 = self.content_md5.then(|| content_md5(&chunk));
            let part_no = part_number;
            let upload_id = upload_id.clone();
            let key = key.clone();
            let bucket = self.bucket.clone();
            let client = self.client.clone();
            let limiter = Arc::clone(&self.limiter);
            handles.spawn(async move {
                let _permit = limiter
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("s3 request limiter closed"))?;
                let mut part = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_no)
                    .body(ByteStream::from(chunk));
                if let Some(md5) = part_md5 {
                    part = part.content_md5(md5);
                }
                let resp = part.send().await.context("s3 upload part")?;
                let etag = resp.e_tag().unwrap_or_default().to_string();
                drop(slot);
                Ok::<(i32, String), anyhow::Error>((part_no, etag))
            });
            part_number += 1;
            remaining -= chunk_len as u64;
        }

        let mut parts = Vec::new();
        let mut upload_error = None;
        while let Some(joined) = handles.join_next().await {
            match joined {
                Ok(Ok((part_no, etag))) => {
                    parts.push(
                        CompletedPart::builder()
                            .part_number(part_no)
                            .e_tag(etag)
                            .build(),
                    );
                }
                Ok(Err(e)) => upload_error = Some(e),
                Err(e) => {
                    upload_error = Some(anyhow::anyhow!("multipart upload task panicked: {e}"))
                }
            }
        }

        if let Some(e) = upload_error {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        parts.sort_by_key(|p| p.part_number);
        let expected_crc = hasher.map(Crc64Ecma::finalize);
        let mut complete = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .customize();
        if expected_crc.is_some() {
            complete = complete.interceptor(Crc64ResponseCapture {
                slot: Arc::clone(&crc_slot),
            });
        }
        if let Err(e) = complete.send().await {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e).context("s3 complete multipart upload");
        }
        if let Some(expected) = expected_crc {
            check_crc64_response(crc_slot, expected, &self.metrics)?;
        }
        Ok(())
    }

    /// Begin a streaming multipart upload for `path`. Bytes are fed via
    /// [`StreamingUpload::write`] and uploaded as parts in the background, so
    /// upload overlaps with the local write. Call [`StreamingUpload::finish`]
    /// on close.
    pub async fn begin_streaming_upload(&self, path: &str) -> Result<StreamingUpload> {
        let key = self.key_for(path);
        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key);
        if let Some(sc) = &self.storage_class {
            create = create.storage_class(sc.clone());
        }
        let create = create
            .send()
            .await
            .inspect_err(|_| {
                self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                self.metrics
                    .s3_multipart_errors
                    .fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 create multipart upload")?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("s3 create multipart upload returned no upload id"))?
            .to_string();
        Ok(StreamingUpload {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key,
            upload_id,
            next_part: 1,
            parts: Vec::new(),
            pending: Vec::new(),
            hasher: Crc64Ecma::new(),
            verify_crc64: self.verify_crc64,
            content_md5: self.content_md5,
            part_sem: Arc::new(Semaphore::new(self.multipart_concurrency.max(1))),
            limiter: Arc::clone(&self.limiter),
            tasks: tokio::task::JoinSet::new(),
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Create an empty directory marker object.
    pub async fn mkdir(&self, path: &str) -> Result<()> {
        self.invalidate_stat(path);
        let dir = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };
        self.write(&dir, &[]).await
    }

    /// Delete a single object.
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.ensure_writable()?;
        let _permit = self.acquire().await?;
        self.delete_impl(path).await
    }

    async fn delete_impl(&self, path: &str) -> Result<()> {
        self.invalidate_stat(path);
        self.invalidate_read_cache(path);
        let key = self.key_for(path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .inspect_err(|_| {
                self.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                self.metrics
                    .s3_delete_errors
                    .fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 delete")?;
        Ok(())
    }

    /// Recursively delete a directory tree (objects under the dir prefix).
    pub async fn delete_dir_recursive(&self, dir: &str) -> Result<()> {
        self.ensure_writable()?;
        let _permit = self.acquire().await?;
        self.delete_dir_recursive_impl(dir).await
    }

    async fn delete_dir_recursive_impl(&self, dir: &str) -> Result<()> {
        self.invalidate_stat(dir);
        self.clear_read_cache();
        let prefix = self.list_prefix(dir);
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = req.send().await.context("s3 list for delete")?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await
                        .context("s3 delete object")?;
                }
            }
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        // Remove the marker itself (it is included in the prefix listing).
        let marker_path = if dir.ends_with('/') {
            dir.to_string()
        } else {
            format!("{dir}/")
        };
        let marker = self.key_for(&marker_path);
        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&marker)
            .send()
            .await;
        Ok(())
    }

    /// Rename a file or directory. Directories are copied recursively; the
    /// operation is intentionally non-atomic (object storage semantics).
    pub async fn rename(&self, old: &str, new: &str) -> Result<()> {
        self.ensure_writable()?;
        let _permit = self.acquire().await?;
        self.rename_impl(old, new).await
    }

    async fn rename_impl(&self, old: &str, new: &str) -> Result<()> {
        self.invalidate_stat(old);
        self.invalidate_stat(new);
        self.clear_read_cache();
        let old_key = self.key_for(old);
        let new_key = self.key_for(new);
        let source = format!("{}/{}", self.bucket, old_key);

        // Determine directory-ness from S3 instead of assuming a trailing
        // slash: WinFsp/FUSE rename paths for directories arrive without a
        // trailing slash.
        let is_dir = self
            .stat_uncached_impl(old)
            .await?
            .map(|e| e.is_dir)
            .unwrap_or(false);

        if is_dir {
            if !self.allow_rename_dir {
                anyhow::bail!("directory rename is disabled");
            }
            if let Some(limit) = self.rename_dir_limit {
                let count = self.count_tree_entries(&old_key, limit).await?;
                if count > limit {
                    anyhow::bail!(
                        "directory {old} has {count} entries, exceeding rename-dir-limit {limit}"
                    );
                }
            }
            // Directory: copy the marker + every child recursively.
            self.copy_tree(&old_key, &new_key).await?;
            self.delete_dir_recursive_impl(old).await
        } else {
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .key(&new_key)
                .copy_source(&source)
                .send()
                .await
                .context("s3 copy")?;
            self.delete_impl(old).await
        }
    }

    /// Count objects under the `old_key` directory prefix, failing as soon as
    /// the count exceeds `limit` so an oversized rename is rejected before any
    /// copy work starts.
    async fn count_tree_entries(&self, old_key: &str, limit: u64) -> Result<u64> {
        let prefix = dir_object_prefix(old_key);
        let mut count = 0u64;
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = req.send().await.context("s3 list for rename count")?;
            count += resp.contents().len() as u64;
            if count > limit {
                anyhow::bail!(
                    "directory exceeds rename-dir-limit {limit} ({count} entries so far)"
                );
            }
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(count)
    }

    async fn copy_tree(&self, old_key: &str, new_key: &str) -> Result<()> {
        let prefix = dir_object_prefix(old_key);
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix);
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = req.send().await.context("s3 list for rename")?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let suffix = key.strip_prefix(&prefix).unwrap_or(key);
                    let dst = format!("{}/{suffix}", new_key.trim_end_matches('/'));
                    self.client
                        .copy_object()
                        .bucket(&self.bucket)
                        .key(&dst)
                        .copy_source(format!("{}/{}", self.bucket, key))
                        .send()
                        .await
                        .context("s3 copy")?;
                }
            }
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        // Copy the dir marker.
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(format!("{}/", new_key.trim_end_matches('/')))
            .copy_source(format!(
                "{}/{}/",
                self.bucket,
                old_key.trim_end_matches('/')
            ))
            .send()
            .await
            .context("s3 copy marker")?;
        Ok(())
    }
}

/// True when an AWS SDK error is a 404 (used to distinguish missing objects).
fn is_s3_not_found(
    e: &aws_sdk_s3::error::SdkError<impl std::fmt::Debug + std::fmt::Display>,
) -> bool {
    match e {
        aws_sdk_s3::error::SdkError::ServiceError(err) => err.raw().status().as_u16() == 404,
        _ => false,
    }
}

/// True when an AWS SDK error is an out-of-range read (416 InvalidRange).
/// Reads at/behind EOF are treated as "return 0 bytes", so this is not an
/// error.
fn is_s3_invalid_range(
    e: &aws_sdk_s3::error::SdkError<impl std::fmt::Debug + std::fmt::Display>,
) -> bool {
    match e {
        aws_sdk_s3::error::SdkError::ServiceError(err) => {
            let status = err.raw().status().as_u16();
            if status == 416 {
                return true;
            }
            // Some S3-compatible services return 400 with a body code.
            if status == 400 {
                let body = String::from_utf8_lossy(err.raw().body().bytes().unwrap_or_default());
                return body.contains("InvalidRange");
            }
            false
        }
        _ => false,
    }
}

/// Size of the S3 GET for one read, factoring in the read-ahead window and
/// whether this read continues the previous one. Extracted so the prefetch
/// decision is unit-testable.
fn read_fetch_len(len: usize, window: usize, sequential: bool) -> usize {
    if window > 0 && (len as u64) < window as u64 && sequential {
        window
    } else {
        len
    }
}

/// Normalize an object key so its directory prefix is `<base>/`, never
/// `<base>//`. Used by directory rename copy/count paths.
fn dir_object_prefix(key: &str) -> String {
    let base = key.trim_end_matches('/');
    format!("{base}/")
}

/// Strip the leading slash from a normalized path; `"/"` -> `""`.
pub fn rel_key(path: &str) -> String {
    path.trim().trim_start_matches('/').to_string()
}

/// Last path component of a normalized POSIX path. `/` stays `/`.
pub fn basename(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        None => trimmed.to_string(),
        Some(0) => trimmed[1..].to_string(),
        Some(idx) => trimmed[idx + 1..].to_string(),
    }
}

/// Parent of a normalized POSIX path. `/` stays `/`.
pub fn parent_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(idx) => trimmed[..idx].to_string(),
    }
}

/// Minimal S3 request timeout (avoid hanging mounts on unreachable buckets).
pub fn request_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Effective in-flight S3 request cap: explicit config wins, `None`/`0`
/// fall back to the default bound (0 never means "unlimited", which would
/// reintroduce the unbounded-concurrency OOM this limiter prevents).
fn effective_max_concurrent_requests(configured: Option<usize>) -> usize {
    configured
        .filter(|&n| n > 0)
        .unwrap_or(MAX_CONCURRENT_S3_REQUESTS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_key_maps_paths() {
        assert_eq!(rel_key("/"), "");
        assert_eq!(rel_key("/a"), "a");
        assert_eq!(rel_key("/a/b.txt"), "a/b.txt");
        assert_eq!(rel_key("/a/"), "a/");
        assert_eq!(rel_key("//a//b"), "a//b");
    }

    #[test]
    fn basename_and_parent() {
        assert_eq!(basename("/a/b.txt"), "b.txt");
        assert_eq!(basename("/a/"), "a");
        assert_eq!(basename("/"), "/");
        assert_eq!(parent_path("/a/b"), "/a");
        assert_eq!(parent_path("/a"), "/");
        assert_eq!(parent_path("/"), "/");
    }

    #[test]
    fn key_for_applies_prefix() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: "ossfs/".into(),
        };
        assert_eq!(fs.key_for("/docs/a.txt"), "ossfs/docs/a.txt");
        assert_eq!(fs.key_for("/docs/"), "ossfs/docs/");
        assert_eq!(fs.key_for("/"), "ossfs");

        let fs2 = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        assert_eq!(fs2.key_for("/docs/a.txt"), "docs/a.txt");
        assert_eq!(fs2.list_prefix("/docs"), "docs/");
        assert_eq!(fs2.list_prefix("/"), "");
    }

    #[test]
    fn effective_owner_and_mode_resolve_defaults() {
        assert_eq!(effective_owner(0, 1000), 1000);
        assert_eq!(effective_owner(42, 1000), 42);
        assert_eq!(effective_mode(true, 0o755, 0o644, 0), 0o755);
        assert_eq!(effective_mode(false, 0o755, 0o644, 0), 0o644);
        assert_eq!(effective_mode(false, 0o755, 0o644, 0o022), 0o644);
        assert_eq!(effective_mode(true, 0o777, 0o666, 0o022), 0o755);
    }

    #[tokio::test]
    async fn read_only_rejects_all_mutations() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: true,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        assert!(fs.ensure_writable().is_err());
        assert!(fs.write("/a", b"x").await.is_err());
        assert!(fs.mkdir("/d").await.is_err());
        assert!(fs.delete("/a").await.is_err());
        assert!(fs.delete_dir_recursive("/d").await.is_err());
        assert!(fs.rename("/a", "/b").await.is_err());
    }

    #[tokio::test]
    async fn upload_budget_rejects_object_larger_than_limit() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: Some(Arc::new(Semaphore::new(1))),
            upload_budget_units: 1,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        let data = vec![0u8; 2 * UPLOAD_BUDGET_UNIT];
        let err = fs.write("/large.bin", &data).await.unwrap_err();
        assert!(err.to_string().contains("max-upload-bytes budget"));
    }

    #[test]
    fn read_fetch_len_prefetches_only_on_sequential_reads() {
        assert_eq!(read_fetch_len(4096, 8 * 1024, false), 4096);
        assert_eq!(read_fetch_len(4096, 8 * 1024, true), 8 * 1024);
        assert_eq!(read_fetch_len(8 * 1024, 8 * 1024, true), 8 * 1024);
        assert_eq!(read_fetch_len(4096, 0, true), 4096);
    }

    #[test]
    fn read_cache_hit_and_invalidation() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 1024,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        fs.insert_read_cache("/a", 0, (0..1024u32).map(|v| v as u8).collect::<Vec<_>>());
        assert_eq!(fs.read_cache_hit("/a", 10, 4), Some(vec![10, 11, 12, 13]));
        assert_eq!(fs.read_cache_hit("/a", 2048, 4), None);
        fs.invalidate_read_cache("/a");
        assert_eq!(fs.read_cache_hit("/a", 10, 4), None);
    }

    #[test]
    fn dirty_budget_rounds_up_and_disables_on_zero() {
        assert!(DirtyBudget::new(0).is_none());
        let budget = DirtyBudget::new(DIRTY_BUDGET_UNIT + 1).unwrap();
        assert_eq!(budget.max_units(), 2);
    }

    #[test]
    fn total_mem_limit_derives_budgets() {
        assert_eq!(
            effective_memory_budgets(Some(64 * 1024 * 1024), 0.5, None, None, None),
            (
                Some(16 * 1024 * 1024),
                Some(16 * 1024 * 1024),
                32 * 1024 * 1024
            )
        );
        assert_eq!(
            effective_memory_budgets(None, 0.5, Some(5), Some(7), Some(9)),
            (Some(5), Some(7), 9)
        );
        assert_eq!(
            effective_memory_budgets(None, 0.5, None, None, None),
            (None, None, READ_CACHE_MAX_BYTES)
        );
    }

    #[test]
    fn s3_max_attempts_maps_retries_to_total_attempts() {
        assert_eq!(s3_max_attempts(0), 1);
        assert_eq!(s3_max_attempts(1), 2);
        assert_eq!(s3_max_attempts(2), 3);
        assert_eq!(s3_max_attempts(u32::MAX), u32::MAX);
    }

    #[test]
    fn min_free_bytes_uses_max_of_reserve_and_ratio() {
        assert_eq!(min_free_bytes(0, None, 1000), 0);
        assert_eq!(min_free_bytes(100, None, 1000), 100);
        assert_eq!(min_free_bytes(0, Some(0.1), 1000), 100);
        assert_eq!(min_free_bytes(50, Some(0.1), 1000), 100);
        assert_eq!(min_free_bytes(200, Some(0.1), 1000), 200);
    }

    #[test]
    fn disk_cache_skips_write_below_free_space_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = DiskCache::new(
            dir.path().to_path_buf(),
            4 * 1024 * 1024,
            4 * 1024 * 1024,
            u64::MAX,
            None,
        )
        .expect("cache");
        cache.write_block("k", 0, b"data").expect("write");

        let blocks = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "blk").unwrap_or(false))
            .count();
        assert_eq!(
            blocks, 0,
            "write must be skipped below the free-space floor"
        );
    }

    #[test]
    fn disk_cache_lru_evicts_least_recently_used() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = DiskCache::new(
            dir.path().to_path_buf(),
            5 * 1024 * 1024,
            DISK_CACHE_BLOCK_SIZE as usize,
            0,
            None,
        )
        .expect("cache");
        let two_mib = vec![0xA5u8; 2 * 1024 * 1024];

        cache.write_block("k", 0, &two_mib).expect("write A");
        cache.write_block("k", 1, &two_mib).expect("write B");
        cache.read_block("k", 0); // touch A
        cache
            .write_block("k", 2, &two_mib)
            .expect("write C triggers evict");

        assert!(cache.path_for("k", 0).exists(), "A must survive");
        assert!(cache.path_for("k", 2).exists(), "C must survive");
        assert!(!cache.path_for("k", 1).exists(), "B must be evicted as LRU");
    }

    #[test]
    fn disk_cache_lru_order_persists_across_remount() {
        let dir = tempfile::tempdir().expect("tempdir");
        let two_mib = vec![0xB5u8; 2 * 1024 * 1024];

        {
            let cache = DiskCache::new(
                dir.path().to_path_buf(),
                5 * 1024 * 1024,
                DISK_CACHE_BLOCK_SIZE as usize,
                0,
                None,
            )
            .expect("cache");
            cache.write_block("k", 0, &two_mib).expect("write A");
            cache.write_block("k", 1, &two_mib).expect("write B");
            cache.read_block("k", 0); // touch A
        }

        let cache = DiskCache::new(
            dir.path().to_path_buf(),
            5 * 1024 * 1024,
            DISK_CACHE_BLOCK_SIZE as usize,
            0,
            None,
        )
        .expect("reopen");
        cache
            .write_block("k", 2, &two_mib)
            .expect("write C triggers evict");
        assert!(
            cache.path_for("k", 0).exists(),
            "A must survive across remount"
        );
        assert!(cache.path_for("k", 2).exists(), "C must survive");
        assert!(!cache.path_for("k", 1).exists(), "B must be evicted as LRU");
    }

    #[test]
    fn disk_cache_block_size_mismatch_rebuilds() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let cache = DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                2 * 1024 * 1024,
                0,
                None,
            )
            .expect("cache");
            assert_eq!(cache.block_size, 2 * 1024 * 1024);
        }
        let cache = DiskCache::new(
            dir.path().to_path_buf(),
            64 * 1024 * 1024,
            1 * 1024 * 1024,
            0,
            None,
        )
        .expect("reopen");
        assert_eq!(cache.block_size, 1 * 1024 * 1024);
    }

    #[test]
    fn disk_cache_detects_corrupt_block() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = DiskCache::new(
            dir.path().to_path_buf(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            0,
            None,
        )
        .expect("cache");
        cache
            .write_block("k", 0, &vec![0x5Au8; 1024])
            .expect("write");

        let path = cache.path_for("k", 0);
        let mut raw = std::fs::read(&path).expect("read block");
        let n = raw.len();
        raw[n - 1] ^= 0xFF;
        std::fs::write(&path, raw).expect("corrupt block");

        assert_eq!(cache.read_block("k", 0), None);
        assert!(!path.exists(), "corrupt block should be removed");
    }

    #[test]
    fn token_bucket_limits_burst_and_refills() {
        let mut b = TokenBucket::new(10.0);
        let t0 = Instant::now();
        for _ in 0..10 {
            assert!(b.reserve(t0).is_none(), "burst allows 10 immediate tokens");
        }
        assert!(b.reserve(t0).is_some(), "11th token must wait");
        let t1 = t0 + Duration::from_secs_f64(0.2);
        assert!(b.reserve(t1).is_none());
        assert!(b.reserve(t1).is_none());
        assert!(b.reserve(t1).is_some());
    }

    #[tokio::test]
    async fn dirty_budget_acquire_and_drop_releases_permits() {
        let budget = DirtyBudget::new(2 * DIRTY_BUDGET_UNIT).unwrap();
        assert_eq!(budget.max_units(), 2);
        let permit = budget.acquire_units(2).await.unwrap();
        assert_eq!(budget.sem.available_permits(), 0);
        drop(permit);
        assert_eq!(budget.sem.available_permits(), 2);
    }

    #[test]
    fn config_normalizes_prefix() {
        let cfg = OssConfig {
            bucket: "b".into(),
            region: "cn-shanghai".into(),
            endpoint: None,
            force_path_style: false,
            prefix: "ossfs".into(),
            max_concurrent_requests: None,
            list_rate_limit: None,
            read_only: false,
            uid: 0,
            gid: 0,
            dir_mode: 0o755,
            file_mode: 0o644,
            allow_other: false,
            umask: 0,
            allow_rename_dir: true,
            rename_dir_limit: Some(2_000_000),
            max_upload_bytes: None,
            read_ahead_bytes: None,
            ignore_fsync: true,
            max_dirty_bytes: None,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            connect_timeout_secs: None,
            readwrite_timeout_secs: None,
            retries: None,
            multipart_size: None,
            multipart_concurrency: None,
            disk_cache_reserve_diskfree: 0,
            disk_cache_free_space_ratio: None,
            disk_cache_dir: None,
            disk_cache_max_bytes: 0,
            disk_cache_block_size: None,
            disk_cache_prefetch_blocks: 1,
            disk_cache_prefetch_concurrency: 4,
            disk_cache_verify_etag: false,
            disk_cache_etag_ttl_secs: 10,
            negative_cache_ttl_secs: 5,
            negative_cache_max_entries: 4096,
            stat_cache_ttl_secs: 3,
            stat_cache_max_entries: 4096,
            total_mem_limit: None,
            total_mem_read_ratio: 0.5,
            read_cache_max_bytes: None,
            credential_process: None,
        }
        .normalize();
        assert_eq!(cfg.prefix, "ossfs/");
        let _ = request_timeout();
    }

    #[tokio::test]
    async fn stat_returns_cached_entry_without_s3() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        let entry = DirEntry {
            name: "a.txt".into(),
            is_dir: false,
            size: 5,
            mtime_secs: 1,
        };
        // Seed the cache: stat() must return this without touching S3 (the
        // unconfigured client would error if it did).
        fs.stats
            .lock()
            .unwrap()
            .insert("/a.txt".into(), (Instant::now(), entry.clone()));
        let got = fs.stat("/a.txt").await.expect("cached stat");
        assert_eq!(got, Some(entry));
    }

    #[tokio::test]
    async fn stat_misses_cache_and_caches_result() {
        // A missing object returns None and does not cache a hit (stat only
        // caches Some). The unconfigured client returns an error for the
        // HEAD, which surfaces as Err rather than None; this is fine as long
        // as it does not panic. Here we only assert the plumbing: after
        // seeding a stale (expired) entry, stat must not return it and must
        // not leave the cache holding the stale entry past a successful call.
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        let old = DirEntry {
            name: "a.txt".into(),
            is_dir: false,
            size: 5,
            mtime_secs: 1,
        };
        // Expired entry (cached 1 hour ago).
        fs.stats.lock().unwrap().insert(
            "/a.txt".into(),
            (Instant::now() - Duration::from_secs(3600), old),
        );
        // stat will try S3 and fail (unconfigured client) -> Err, but the
        // expired entry must be ignored, not returned.
        assert!(fs.stat("/a.txt").await.is_err());
    }

    #[test]
    fn stat_cache_invalidate_removes_entry() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        let entry = DirEntry {
            name: "a.txt".into(),
            is_dir: false,
            size: 5,
            mtime_secs: 1,
        };
        fs.stats
            .lock()
            .unwrap()
            .insert("/a.txt".into(), (Instant::now(), entry));
        fs.invalidate_stat("/a.txt");
        assert!(!fs.stats.lock().unwrap().contains_key("/a.txt"));
        fs.invalidate_stat("/never-cached"); // must not panic
    }

    #[test]
    fn stat_cache_evicts_all_when_over_bound() {
        let fs = ObjectFs {
            client: Client::from_conf(aws_sdk_s3::config::Config::builder().build()),
            bucket: "b".into(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_S3_REQUESTS)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: None,
            prefix: String::new(),
        };
        let entry = DirEntry {
            name: "f".into(),
            is_dir: false,
            size: 1,
            mtime_secs: 1,
        };
        // Fill the cache to the bound through the real insertion helper.
        for i in 0..MAX_STAT_ENTRIES {
            fs.cache_insert(&format!("/f{i}"), entry.clone());
        }
        assert_eq!(fs.stats.lock().unwrap().len(), MAX_STAT_ENTRIES);
        // One more insert hits the bound branch in cache_insert (clear +
        // keep only the new entry), exactly what stat() would do.
        fs.cache_insert("/overflow", entry.clone());
        let cache = fs.stats.lock().unwrap();
        assert_eq!(cache.len(), MAX_STAT_ENTRIES);
        assert!(!cache.contains_key("/f0"));
        assert!(cache.contains_key("/overflow"));
    }

    #[test]
    fn max_concurrent_requests_default_and_override() {
        assert_eq!(
            effective_max_concurrent_requests(None),
            MAX_CONCURRENT_S3_REQUESTS
        );
        assert_eq!(
            effective_max_concurrent_requests(Some(0)),
            MAX_CONCURRENT_S3_REQUESTS
        );
        assert_eq!(effective_max_concurrent_requests(Some(4)), 4);
    }
}

#[cfg(test)]
mod s3_mock_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Minimal in-process S3 mock: counts concurrent in-flight requests,
    /// records request targets + bodies, and serves canned responses so the
    /// AWS SDK can round-trip (ListBucketResult and multipart uploads).
    #[derive(Clone, Debug)]
    pub(crate) struct MockRequest {
        pub(crate) method: String,
        pub(crate) target: String,
        pub(crate) body: Vec<u8>,
        pub(crate) storage_class: Option<String>,
        pub(crate) content_md5: Option<String>,
    }

    pub(crate) struct MockS3 {
        pub(crate) active: Arc<AtomicUsize>,
        pub(crate) max_concurrent: Arc<AtomicUsize>,
        pub(crate) requests: Arc<Mutex<Vec<String>>>,
        pub(crate) recorded: Arc<Mutex<Vec<MockRequest>>>,
        pub(crate) delay: Duration,
        pub(crate) entries: Arc<Mutex<Vec<(String, bool)>>>,
        pub(crate) objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        pub(crate) get_count: Arc<AtomicUsize>,
        pub(crate) head_count: Arc<AtomicUsize>,
        pub(crate) crc64: Mutex<u64>,
        pub(crate) head_etag: Mutex<String>,
    }

    impl MockS3 {
        pub(crate) fn set_object(&self, key: &str, data: Vec<u8>) {
            self.objects.lock().unwrap().insert(key.to_string(), data);
        }

        fn set_head_etag(&self, v: &str) {
            *self.head_etag.lock().unwrap() = v.to_string();
        }

        fn set_crc64(&self, v: u64) {
            *self.crc64.lock().unwrap() = v;
        }

        pub(crate) async fn start(
            entries: Vec<(String, bool)>,
            delay: Duration,
        ) -> (Arc<Self>, u16) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let mock = Arc::new(MockS3 {
                active: Arc::new(AtomicUsize::new(0)),
                max_concurrent: Arc::new(AtomicUsize::new(0)),
                requests: Arc::new(Mutex::new(Vec::new())),
                recorded: Arc::new(Mutex::new(Vec::new())),
                delay,
                objects: Arc::new(Mutex::new(HashMap::new())),
                get_count: Arc::new(AtomicUsize::new(0)),
                head_count: Arc::new(AtomicUsize::new(0)),
                entries: Arc::new(Mutex::new(entries)),
                crc64: Mutex::new(0),
                head_etag: Mutex::new("mock-etag".to_string()),
            });
            let server = Arc::clone(&mock);
            tokio::spawn(async move {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(ok) => ok,
                        Err(_) => break,
                    };
                    let mock = Arc::clone(&server);
                    tokio::spawn(async move {
                        handle_conn(stream, mock).await;
                    });
                }
            });
            (mock, port)
        }
    }

    async fn handle_conn(mut stream: TcpStream, mock: Arc<MockS3>) {
        // Read the full HTTP request: headers first, then the Content-Length
        // body (PUT upload-part / complete-multipart carry a payload).
        let mut buf = Vec::new();
        let mut tmp = [0u8; 8192];
        let mut header_end = None;
        let mut content_length = 0usize;
        while header_end.is_none() {
            let n = match stream.read(&mut tmp).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = Some(pos + 4);
            }
        }
        let head = String::from_utf8_lossy(&buf[..header_end.unwrap()]);
        let mut range_header: Option<String> = None;
        let mut storage_class_header: Option<String> = None;
        let mut content_md5_header: Option<String> = None;
        for line in head.lines() {
            let lower = line.to_ascii_lowercase();
            if let Some(v) = lower.strip_prefix("range:") {
                range_header = Some(v.trim().to_string());
            }
            if let Some(v) = lower.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            if lower.strip_prefix("x-amz-storage-class:").is_some() {
                storage_class_header = line.split_once(':').map(|(_, v)| v.trim().to_string());
            }
            if lower.strip_prefix("content-md5:").is_some() {
                content_md5_header = line.split_once(':').map(|(_, v)| v.trim().to_string());
            }
        }
        let mut parts = head.lines().next().unwrap_or("").split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let target = parts.next().unwrap_or("").to_string();
        let query = target.split('?').nth(1).unwrap_or("").to_lowercase();

        // Read the remaining body bytes.
        let total = header_end.unwrap() + content_length;
        while buf.len() < total {
            let n = match stream.read(&mut tmp).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            buf.extend_from_slice(&tmp[..n]);
        }
        let body = if total <= buf.len() {
            buf[header_end.unwrap()..total].to_vec()
        } else {
            Vec::new()
        };

        let in_flight = mock.active.fetch_add(1, Ordering::SeqCst) + 1;
        let mut cur = mock.max_concurrent.load(Ordering::SeqCst);
        while in_flight > cur {
            match mock.max_concurrent.compare_exchange(
                cur,
                in_flight,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        mock.requests.lock().unwrap().push(target.clone());
        mock.recorded.lock().unwrap().push(MockRequest {
            method: method.clone(),
            target: target.clone(),
            body,
            storage_class: storage_class_header.clone(),
            content_md5: content_md5_header.clone(),
        });

        tokio::time::sleep(mock.delay).await;
        // 模拟的服务端处理（sleep）到此结束，先释放并发槽位再写响应。
        // 若等写完响应再 -1，客户端读到响应即释放限流 permit 放入新请求，
        // 新请求的 +1 会与这里滞后的 -1 重叠，把并发峰值虚记高一档（ flaky ）。
        mock.active.fetch_sub(1, Ordering::SeqCst);

        let mut get_body: Option<Vec<u8>> = None;
        let response = if query.contains("list-type=2") {
            let entries = mock.entries.lock().unwrap().clone();
            let body = list_xml(&entries);
            http_response(200, "application/xml", Some(&format!("{body}")))
        } else if method == "GET" {
            mock.get_count.fetch_add(1, Ordering::SeqCst);
            let path = target.split('?').next().unwrap_or(&target);
            let key = path
                .trim_start_matches('/')
                .split_once('/')
                .map(|(_, k)| k.to_string())
                .unwrap_or_default();
            let objects = mock.objects.lock().unwrap();
            match objects.get(&key) {
                Some(object) => {
                    let len = object.len();
                    let (start, end) = match &range_header {
                        Some(range) => {
                            let range = range.trim().strip_prefix("bytes=").unwrap_or(range);
                            match range.split_once('-') {
                                Some((start, end)) => {
                                    let start = start.parse::<usize>().unwrap_or(0).min(len);
                                    let end = end
                                        .parse::<usize>()
                                        .ok()
                                        .map(|e| e + 1)
                                        .unwrap_or(len)
                                        .min(len)
                                        .max(start);
                                    (start, end)
                                }
                                None => (0, len),
                            }
                        }
                        None => (0, len),
                    };
                    get_body = Some(object[start..end].to_vec());
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        end - start
                    )
                }
                None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string(),
            }
        } else if query.contains("uploads") && !query.contains("uploadid") {
            // InitiateMultipartUpload: POST /key?uploads
            let body = initiate_multipart_xml();
            http_response(200, "application/xml", Some(&body))
        } else if query.contains("uploadid") && query.contains("partnumber") {
            // UploadPart: PUT /key?partNumber=N&uploadId=...
            format!(
                "HTTP/1.1 200 OK\r\nETag: \"etag-{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "mock"
            )
        } else if query.contains("uploadid") && method == "DELETE" {
            // AbortMultipartUpload
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
        } else if query.contains("uploadid") && method == "POST" {
            // CompleteMultipartUpload
            let crc = *mock.crc64.lock().unwrap();
            let body = complete_multipart_xml();
            format!(
                "HTTP/1.1 200 OK\r\nx-oss-hash-crc64ecma: {crc}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
        } else if method == "HEAD" {
            mock.head_count.fetch_add(1, Ordering::SeqCst);
            let path = target.split('?').next().unwrap_or(&target);
            let key = path
                .trim_start_matches('/')
                .split_once('/')
                .map(|(_, k)| k.to_string())
                .unwrap_or_default();
            let objects = mock.objects.lock().unwrap();
            if let Some(obj) = objects.get(&key) {
                let etag = mock.head_etag.lock().unwrap().clone();
                format!(
                    "HTTP/1.1 200 OK\r\nETag: \"{etag}\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    obj.len()
                )
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            }
        } else {
            // Plain PutObject / DeleteObject / ...
            let crc = *mock.crc64.lock().unwrap();
            format!(
                "HTTP/1.1 200 OK\r\nx-oss-hash-crc64ecma: {crc}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
        };
        let _ = stream.write_all(response.as_bytes()).await;
        if let Some(body) = get_body {
            let _ = stream.write_all(&body).await;
        }
        let _ = stream.shutdown().await;
    }

    fn http_response(status: u16, content_type: &str, body: Option<&String>) -> String {
        let body = body.cloned().unwrap_or_default();
        format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn initiate_multipart_xml() -> String {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>bucket</Bucket><Key>key</Key><UploadId>mock-upload-1</UploadId></InitiateMultipartUploadResult>"
            .to_string()
    }

    fn complete_multipart_xml() -> String {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Location>http://127.0.0.1/bucket/key</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>&quot;mock&quot;</ETag></CompleteMultipartUploadResult>"
            .to_string()
    }

    fn list_xml(entries: &[(String, bool)]) -> String {
        let mut body = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
        );
        body.push_str("<Name>bucket</Name><Prefix></Prefix><KeyCount>");
        body.push_str(&entries.len().to_string());
        body.push_str("</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>");
        for (key, is_dir) in entries {
            if *is_dir {
                body.push_str(&format!(
                    "<CommonPrefixes><Prefix>{key}</Prefix></CommonPrefixes>"
                ));
            } else {
                body.push_str(&format!(
                    "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;mock&quot;</ETag><Size>5</Size><StorageClass>STANDARD</StorageClass></Contents>"
                ));
            }
        }
        body.push_str("</ListBucketResult>");
        body
    }

    pub(crate) fn test_fs(port: u16, limit: usize) -> ObjectFs {
        test_fs_with_budget(port, limit, None)
    }

    pub(crate) fn test_fs_with_budget(
        port: u16,
        limit: usize,
        max_dirty_bytes: Option<usize>,
    ) -> ObjectFs {
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .endpoint_url(format!("http://127.0.0.1:{port}"))
                .force_path_style(true)
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ak", "sk", None, None, "test",
                ))
                .behavior_version(BehaviorVersion::latest())
                .build(),
        );
        ObjectFs {
            client,
            bucket: "b".into(),
            prefix: String::new(),
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(limit)),
            read_only: false,
            allow_other: false,
            list_rate: None,
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
            read_ahead_window: 0,
            read_cache: Mutex::new(ReadCache::default()),
            read_cache_max_bytes: READ_CACHE_MAX_BYTES,
            disk_cache: None,
            disk_cache_prefetch_blocks: 1,
            prefetch_inflight: Arc::new(Mutex::new(HashSet::new())),
            prefetch_sem: Arc::new(Semaphore::new(DISK_CACHE_PREFETCH_CONCURRENCY)),
            disk_cache_verify_etag: false,
            etag_checked: Mutex::new(HashMap::new()),
            etag_ttl: ETAG_CHECK_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
            stat_ttl: STAT_TTL,
            negative_max_entries: MAX_NEGATIVE_ENTRIES,
            stat_max_entries: MAX_STAT_ENTRIES,
            read_seq: Mutex::new(HashMap::new()),
            ignore_fsync: true,
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            multipart_part_size: MULTIPART_PART_SIZE as usize,
            multipart_concurrency: MULTIPART_UPLOAD_CONCURRENCY,
            metrics: Arc::new(Metrics::default()),
            dirty_budget: DirtyBudget::new(max_dirty_bytes.unwrap_or(0)),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rename_dir_disabled_rejects_directory_rename() {
        let entries = vec![("dir/a.txt".to_string(), false)];
        let (mock, port) = MockS3::start(entries, Duration::from_millis(0)).await;
        let mut fs = test_fs(port, 8);
        fs.allow_rename_dir = false;
        let err = fs.rename("/dir", "/newdir").await.unwrap_err();
        assert!(err.to_string().contains("directory rename is disabled"));
        drop(mock);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rename_dir_limit_exceeded_rejects_before_copy() {
        let entries = vec![
            ("dir/a.txt".to_string(), false),
            ("dir/b.txt".to_string(), false),
        ];
        let (mock, port) = MockS3::start(entries, Duration::from_millis(0)).await;
        let mut fs = test_fs(port, 8);
        fs.rename_dir_limit = Some(1);
        let err = fs.rename("/dir", "/newdir").await.unwrap_err();
        assert!(err.to_string().contains("rename-dir-limit"));
        drop(mock);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn list_concurrency_is_bounded_by_limiter() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(30)).await;
        let fs = Arc::new(test_fs(port, 2));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let fs = Arc::clone(&fs);
            handles.push(tokio::spawn(async move { fs.list("/").await }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_ok(), "list failed");
        }
        let max = mock.max_concurrent.load(Ordering::SeqCst);
        assert!(
            max <= 2,
            "limiter not honored: observed {max} concurrent S3 requests with limit 2"
        );
        assert!(
            max >= 2,
            "test is vacuous: never saw concurrency ({max}), mock/delay broken?"
        );
    }

    #[tokio::test]
    async fn list_rate_limit_throttles_recursive_walk() {
        let (mock, port) =
            MockS3::start(vec![("docs/a.txt".into(), false)], Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.list_rate = Some(Mutex::new(TokenBucket::new(2.0)));

        // Burst capacity is 2; the 3rd list must wait and therefore count as
        // throttled. The limiter delays but never drops, so all 3 requests
        // still reach the mock.
        for _ in 0..3 {
            fs.list("/docs").await.expect("list");
        }
        let throttled = fs.metrics().list_throttled;
        assert!(
            throttled >= 1,
            "expected at least one throttled list, got {throttled}"
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn stat_probes_implied_dir_with_max_keys_1() {
        let (mock, port) = MockS3::start(
            vec![("implied/f.txt".into(), false)],
            Duration::from_millis(1),
        )
        .await;
        let fs = test_fs(port, 32);
        let got = fs.stat("/implied").await.expect("stat");
        let entry = got.expect("implied directory should exist via probe");
        assert!(entry.is_dir, "probe must report an implied directory");
        let reqs = mock.requests.lock().unwrap();
        let list_reqs: Vec<&String> = reqs
            .iter()
            .filter(|t| t.to_lowercase().contains("list-type=2"))
            .collect();
        assert!(!list_reqs.is_empty(), "expected a probe LIST request");
        assert!(
            list_reqs
                .iter()
                .any(|t| t.to_lowercase().contains("max-keys=1")),
            "probe must use max_keys=1, got: {list_reqs:?}"
        );
    }

    #[tokio::test]
    async fn stat_missing_path_returns_none_via_probe() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);
        let got = fs.stat("/nope").await.expect("stat");
        assert!(got.is_none(), "missing path must be None");
        assert!(!mock.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_parses_common_prefixes_and_objects() {
        let (mock, port) = MockS3::start(
            vec![("docs/sub/".into(), true), ("docs/a.txt".into(), false)],
            Duration::from_millis(1),
        )
        .await;
        let fs = test_fs(port, 32);
        let entries = fs.list("/docs").await.expect("list");
        let names: Vec<(String, bool)> =
            entries.iter().map(|e| (e.name.clone(), e.is_dir)).collect();
        assert_eq!(
            names,
            vec![("sub".to_string(), true), ("a.txt".to_string(), false)]
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_skips_legacy_folder_markers_when_enabled() {
        let (mock, port) = MockS3::start(
            vec![
                ("docs/sub_$folder$".into(), false),
                ("docs/a.txt".into(), false),
            ],
            Duration::from_millis(1),
        )
        .await;
        let mut fs = test_fs(port, 32);
        fs.notsup_compat_dir = true;
        let entries = fs.list("/docs").await.expect("list");
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["a.txt".to_string()]);
    }

    #[tokio::test]
    async fn list_keeps_legacy_folder_markers_when_disabled() {
        let (mock, port) = MockS3::start(
            vec![
                ("docs/sub_$folder$".into(), false),
                ("docs/a.txt".into(), false),
            ],
            Duration::from_millis(1),
        )
        .await;
        let fs = test_fs(port, 32);
        let entries = fs.list("/docs").await.expect("list");
        let names: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
        assert_eq!(names, vec!["sub_$folder$".to_string(), "a.txt".to_string()]);
    }

    #[tokio::test]
    async fn write_small_object_uses_single_put() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);
        let data = vec![0x5Au8; 1024];
        fs.write("/small.bin", &data).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1, "small write must be a single request");
        assert_eq!(recorded[0].method, "PUT");
        let q = recorded[0].target.to_lowercase();
        assert!(!q.contains("uploads"), "must not initiate multipart");
        assert!(!q.contains("uploadid"), "must not touch multipart");
        assert!(!q.contains("partnumber"), "must not upload parts");
        assert_eq!(recorded[0].body, data, "whole object must be the PUT body");
    }

    #[tokio::test]
    async fn write_small_object_sets_storage_class() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.storage_class = Some(StorageClass::from("Standard"));

        fs.write("/small-sc.bin", &[1u8, 2, 3])
            .await
            .expect("write");

        let recorded = mock.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].method, "PUT");
        assert_eq!(
            recorded[0].storage_class.as_deref(),
            Some("Standard"),
            "PUT must carry the requested x-amz-storage-class header"
        );
    }

    #[tokio::test]
    async fn write_small_object_sets_content_md5() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.content_md5 = true;

        let data = vec![0x5Au8; 1024];
        fs.write("/small-md5.bin", &data).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].content_md5.as_deref(),
            Some(content_md5(&data).as_str()),
            "single PUT must carry the base64 Content-MD5 header"
        );
    }

    #[tokio::test]
    async fn write_small_object_verifies_crc64() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.verify_crc64 = true;

        let data = vec![0x5Au8; 1024];
        let expected = crc64ecma(&data);
        mock.set_crc64(expected);

        fs.write("/small-crc.bin", &data).await.expect("write");
    }

    #[tokio::test]
    async fn write_small_object_rejects_crc64_mismatch() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.verify_crc64 = true;

        let data = vec![0x5Au8; 1024];
        mock.set_crc64(crc64ecma(&data).wrapping_add(1));

        let err = fs.write("/small-bad.bin", &data).await.unwrap_err();
        assert!(
            err.to_string().contains("crc64 mismatch"),
            "expected crc64 mismatch error, got: {err}"
        );
    }

    #[tokio::test]
    async fn write_large_object_uses_multipart_and_reassembles() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);

        // 20 MiB > MULTIPART_THRESHOLD (16 MiB), byte pattern varies so the
        // reassembled order can be verified.
        let data: Vec<u8> = (0..20 * 1024 * 1024usize)
            .map(|i| (i % 256) as u8)
            .collect();
        fs.write("/large.bin", &data).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        let lc = |t: &str| t.to_lowercase();

        let creates = recorded
            .iter()
            .filter(|r| {
                r.method == "POST"
                    && lc(&r.target).contains("uploads")
                    && !lc(&r.target).contains("uploadid")
            })
            .count();
        assert_eq!(creates, 1, "exactly one initiate-multipart");

        let aborts = recorded
            .iter()
            .filter(|r| r.method == "DELETE" && lc(&r.target).contains("uploadid"))
            .count();
        assert_eq!(aborts, 0, "no abort on a successful upload");

        let completes = recorded
            .iter()
            .filter(|r| {
                r.method == "POST"
                    && lc(&r.target).contains("uploadid")
                    && !lc(&r.target).contains("partnumber")
            })
            .count();
        assert_eq!(completes, 1, "exactly one complete-multipart");

        let mut parts: Vec<(i32, Vec<u8>)> = recorded
            .iter()
            .filter(|r| r.method == "PUT" && lc(&r.target).contains("partnumber"))
            .map(|r| {
                let query = r.target.split('?').nth(1).unwrap_or("");
                let part_no = query
                    .split('&')
                    .find_map(|kv| {
                        let (k, v) = kv.split_once('=')?;
                        k.eq_ignore_ascii_case("partnumber")
                            .then(|| v.parse::<i32>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                (part_no, r.body.clone())
            })
            .collect();
        parts.sort_by_key(|(n, _)| *n);
        assert!(
            parts.len() >= 2,
            "expected multiple multipart parts, got {}",
            parts.len()
        );

        let reassembled: Vec<u8> = parts.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(
            reassembled, data,
            "multipart parts must reassemble to the original bytes in order"
        );
    }

    #[tokio::test]
    async fn write_from_file_streams_multipart_without_whole_buffer() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("large.bin");
        let data: Vec<u8> = (0..20 * 1024 * 1024usize)
            .map(|i| (i % 256) as u8)
            .collect();
        std::fs::write(&src, &data).expect("write spool");

        fs.write_from_file("/large.bin", &src).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        let lc = |t: &str| t.to_lowercase();
        let creates = recorded
            .iter()
            .filter(|r| {
                r.method == "POST"
                    && lc(&r.target).contains("uploads")
                    && !lc(&r.target).contains("uploadid")
            })
            .count();
        assert_eq!(creates, 1, "exactly one initiate-multipart");

        let mut parts: Vec<(i32, Vec<u8>)> = recorded
            .iter()
            .filter(|r| r.method == "PUT" && lc(&r.target).contains("partnumber"))
            .map(|r| {
                let query = r.target.split('?').nth(1).unwrap_or("");
                let part_no = query
                    .split('&')
                    .find_map(|kv| {
                        let (k, v) = kv.split_once('=')?;
                        k.eq_ignore_ascii_case("partnumber")
                            .then(|| v.parse::<i32>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                (part_no, r.body.clone())
            })
            .collect();
        parts.sort_by_key(|(n, _)| *n);
        let reassembled: Vec<u8> = parts.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(
            reassembled, data,
            "multipart parts must reassemble in order"
        );
    }

    #[tokio::test]
    async fn write_from_file_small_uses_single_put() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);

        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("small.bin");
        let data = vec![0x5Au8; 1024];
        std::fs::write(&src, &data).expect("write spool");

        fs.write_from_file("/small.bin", &src).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1, "small file must be a single PUT");
        assert_eq!(recorded[0].method, "PUT");
        assert!(!recorded[0].target.to_lowercase().contains("uploads"));
        assert_eq!(recorded[0].body, data);
    }

    #[tokio::test]
    async fn streaming_upload_reassembles() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);

        let mut up = fs
            .begin_streaming_upload("/large.bin")
            .await
            .expect("begin");
        let data: Vec<u8> = (0..20 * 1024 * 1024usize)
            .map(|i| (i % 256) as u8)
            .collect();
        up.write(&data).await.expect("write");
        up.finish().await.expect("finish");

        let recorded = mock.recorded.lock().unwrap();
        let lc = |t: &str| t.to_lowercase();
        let creates = recorded
            .iter()
            .filter(|r| {
                r.method == "POST"
                    && lc(&r.target).contains("uploads")
                    && !lc(&r.target).contains("uploadid")
            })
            .count();
        assert_eq!(creates, 1, "exactly one initiate-multipart");

        let mut parts: Vec<(i32, Vec<u8>)> = recorded
            .iter()
            .filter(|r| r.method == "PUT" && lc(&r.target).contains("partnumber"))
            .map(|r| {
                let query = r.target.split('?').nth(1).unwrap_or("");
                let part_no = query
                    .split('&')
                    .find_map(|kv| {
                        let (k, v) = kv.split_once('=')?;
                        k.eq_ignore_ascii_case("partnumber")
                            .then(|| v.parse::<i32>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                (part_no, r.body.clone())
            })
            .collect();
        parts.sort_by_key(|(n, _)| *n);
        let reassembled: Vec<u8> = parts.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(
            reassembled, data,
            "streaming parts must reassemble in order"
        );
    }

    #[tokio::test]
    async fn write_large_object_honors_custom_part_size() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.multipart_part_size = 6 * 1024 * 1024;

        let data: Vec<u8> = vec![0xAAu8; 20 * 1024 * 1024];
        fs.write("/large-custom.bin", &data).await.expect("write");

        let recorded = mock.recorded.lock().unwrap();
        let part_count = recorded
            .iter()
            .filter(|r| r.method == "PUT" && r.target.to_lowercase().contains("partnumber"))
            .count();
        assert_eq!(
            part_count, 4,
            "20 MiB with a 6 MiB part size must upload 4 parts"
        );
    }

    #[tokio::test]
    async fn write_large_object_verifies_crc64() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.verify_crc64 = true;

        let data: Vec<u8> = (0..20 * 1024 * 1024usize)
            .map(|i| (i % 256) as u8)
            .collect();
        mock.set_crc64(crc64ecma(&data));

        fs.write("/large-crc.bin", &data).await.expect("write");
    }

    #[tokio::test]
    async fn disk_cache_serves_repeat_reads_without_s3() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                DISK_CACHE_BLOCK_SIZE as usize,
                0,
                None,
            )
            .expect("cache"),
        ));

        let data: Vec<u8> = (0..5 * 1024 * 1024usize).map(|i| (i % 251) as u8).collect();
        mock.set_object("big.bin", data.clone());

        let off = 4 * 1024 * 1024 - 8;
        let first = fs.read_range("/big.bin", off, 16).await.expect("read");
        assert_eq!(first, data[off as usize..off as usize + 16]);
        assert_eq!(mock.get_count.load(Ordering::SeqCst), 2); // crosses a block boundary

        let second = fs.read_range("/big.bin", off, 16).await.expect("read");
        assert_eq!(second, data[off as usize..off as usize + 16]);
        assert_eq!(
            mock.get_count.load(Ordering::SeqCst),
            2,
            "second read must hit disk cache"
        );
    }

    #[tokio::test]
    async fn disk_cache_prefetches_next_block_on_sequential_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.read_ahead_window = 8 * 1024 * 1024;
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                0,
                None,
            )
            .expect("cache"),
        ));

        let data: Vec<u8> = (0..13 * 1024 * 1024usize)
            .map(|i| (i % 256) as u8)
            .collect();
        mock.set_object("seq.bin", data);
        fs.read_range("/seq.bin", 0, 1024).await.expect("read");
        fs.read_range("/seq.bin", 1024, 1024).await.expect("read");
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            fs.disk_cache
                .as_ref()
                .unwrap()
                .read_block("seq.bin", 2)
                .is_some(),
            "sequential read should prefetch the next block"
        );
    }

    #[tokio::test]
    async fn disk_cache_etag_verification_invalidates_on_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                0,
                None,
            )
            .expect("cache"),
        ));
        fs.disk_cache_verify_etag = true;

        let data: Vec<u8> = (0..5 * 1024 * 1024usize).map(|i| (i % 256) as u8).collect();
        mock.set_object("e.bin", data);
        fs.read_range("/e.bin", 0, 1024).await.expect("read");
        fs.read_range("/e.bin", 0, 1024).await.expect("hit");
        assert_eq!(mock.get_count.load(Ordering::SeqCst), 1);

        fs.etag_checked.lock().unwrap().clear();
        mock.set_head_etag("changed");
        fs.read_range("/e.bin", 0, 1024).await.expect("refetch");
        assert_eq!(mock.get_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn disk_cache_etag_ttl_skips_repeated_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                0,
                None,
            )
            .expect("cache"),
        ));
        fs.disk_cache_verify_etag = true;

        let data: Vec<u8> = (0..5 * 1024 * 1024usize).map(|i| (i % 256) as u8).collect();
        mock.set_object("t.bin", data);
        fs.read_range("/t.bin", 0, 1024).await.expect("read");
        fs.read_range("/t.bin", 0, 1024).await.expect("hit");
        assert_eq!(mock.head_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disk_cache_invalidated_by_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                DISK_CACHE_BLOCK_SIZE as usize,
                0,
                None,
            )
            .expect("cache"),
        ));

        let data: Vec<u8> = (0..1024usize).map(|i| i as u8).collect();
        mock.set_object("small.bin", data.clone());
        fs.read_range("/small.bin", 0, 1024).await.expect("read");
        assert_eq!(mock.get_count.load(Ordering::SeqCst), 1);

        fs.write("/small.bin", &data).await.expect("write");
        fs.read_range("/small.bin", 0, 1024).await.expect("read");
        assert_eq!(
            mock.get_count.load(Ordering::SeqCst),
            2,
            "write must invalidate the cached block"
        );
    }

    #[tokio::test]
    async fn metrics_count_reads_writes_and_caches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = test_fs(port, 32);
        fs.disk_cache = Some(Arc::new(
            DiskCache::new(
                dir.path().to_path_buf(),
                64 * 1024 * 1024,
                DISK_CACHE_BLOCK_SIZE as usize,
                0,
                None,
            )
            .expect("cache"),
        ));

        let data = vec![0xAAu8; 1024];
        mock.set_object("a.bin", data);
        fs.read_range("/a.bin", 0, 1024).await.expect("read");
        fs.read_range("/a.bin", 0, 1024).await.expect("read");
        fs.write("/a.bin", &[1, 2, 3]).await.expect("write");

        let m = fs.metrics();
        assert_eq!(m.reads, 2);
        assert_eq!(m.disk_cache_hits, 1);
        assert_eq!(m.writes, 1);
        assert_eq!(m.s3_gets, 1);
        assert_eq!(m.s3_puts, 1);
        assert_eq!(m.upload_bytes_total, 3);
        assert_eq!(m.download_bytes_total, 1024);
    }

    #[tokio::test]
    async fn s3_errors_increment_on_get_failure() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);
        let err = fs.read_range("/missing.bin", 0, 16).await.unwrap_err();
        assert!(err.to_string().contains("s3 get"));
        assert_eq!(fs.metrics().s3_get_errors, 1);
    }

    #[tokio::test]
    async fn stat_missing_path_is_negatively_cached() {
        let (mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = test_fs(port, 32);

        assert!(fs.stat("/nope").await.expect("stat").is_none());
        let before = mock.requests.lock().unwrap().len();

        // Within NEGATIVE_CACHE_TTL, a second stat of the same missing path
        // must not issue any remote HEAD/probe requests.
        assert!(fs.stat("/nope").await.expect("stat").is_none());
        let after = mock.requests.lock().unwrap().len();
        assert_eq!(before, after, "second stat must hit the negative cache");
    }

    #[test]
    fn crc64ecma_matches_known_vectors() {
        assert_eq!(crc64ecma(b"123456789"), 0x995DC9BBDF1939FA);
        assert_eq!(crc64ecma(b"a"), 0x330284772E652B05);
    }
}

/// Test-only S3 mock shared by the platform adapter test modules
/// (`ossfs::winfsp` on Windows, `ossfs::fuse` on macOS/Linux).
#[cfg(test)]
pub(crate) use s3_mock_tests::{MockS3, test_fs, test_fs_with_budget};
