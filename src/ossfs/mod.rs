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

#[cfg(not(windows))]
pub mod fuse;
#[cfg(windows)]
pub mod winfsp;

use anyhow::{Context as _, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{Client, config::BehaviorVersion};
use std::collections::HashMap;
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
}

/// POSIX ownership / permission defaults applied to every object by the FUSE
/// adapters. See [`OssConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MountAttr {
    pub uid: u32,
    pub gid: u32,
    pub dir_mode: u32,
    pub file_mode: u32,
}

impl Default for MountAttr {
    fn default() -> Self {
        Self {
            uid: 0,
            gid: 0,
            dir_mode: 0o755,
            file_mode: 0o644,
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
pub(crate) fn effective_mode(is_dir: bool, dir_mode: u32, file_mode: u32) -> u16 {
    if is_dir {
        dir_mode as u16
    } else {
        file_mode as u16
    }
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

/// Resolve [`OssConfig::max_upload_bytes`] into a number of MiB permits.
/// Returns `None` when the budget is disabled. Values above what a single
/// acquire call can represent are clamped.
fn upload_budget_units(max_bytes: Option<usize>) -> Option<usize> {
    let bytes = max_bytes?;
    if bytes == 0 {
        return None;
    }
    let units = bytes.div_ceil(UPLOAD_BUDGET_UNIT).min(u32::MAX as usize);
    Some(units.max(1))
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
    /// Read-only mount: reject all mutations.
    read_only: bool,
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
        let client = Client::from_conf(builder.build());
        let upload_budget_units = upload_budget_units(config.max_upload_bytes);
        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
            stats: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
            limiter: Arc::new(Semaphore::new(effective_max_concurrent_requests(
                config.max_concurrent_requests,
            ))),
            read_only: config.read_only,
            mount_attr: MountAttr {
                uid: config.uid,
                gid: config.gid,
                dir_mode: config.dir_mode,
                file_mode: config.file_mode,
            },
            allow_rename_dir: config.allow_rename_dir,
            rename_dir_limit: config.rename_dir_limit,
            upload_budget: upload_budget_units.map(|units| Arc::new(Semaphore::new(units))),
            upload_budget_units: upload_budget_units.unwrap_or(0),
        })
    }

    /// Read-only state of this mount.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// POSIX ownership / permission defaults applied by the FUSE adapters.
    pub fn mount_attr(&self) -> MountAttr {
        self.mount_attr
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
        let _permit = self.acquire().await?;
        self.list_impl(dir).await
    }

    async fn list_impl(&self, dir: &str) -> Result<Vec<DirEntry>> {
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
            let resp = req.send().await.context("s3 list")?;
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
                if at.elapsed() < STAT_TTL {
                    return Ok(Some(entry.clone()));
                }
            }
        }
        if self.negative_hit(path) {
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

    /// Whether `path` is currently recorded as missing (and still fresh).
    fn negative_hit(&self, path: &str) -> bool {
        matches!(
            self.negative.lock().unwrap().get(path),
            Some(at) if at.elapsed() < NEGATIVE_CACHE_TTL
        )
    }

    /// Record `path` as missing (bounded, same clear-on-overflow policy as the
    /// positive cache).
    fn negative_insert(&self, path: &str) {
        let mut cache = self.negative.lock().unwrap();
        if cache.len() >= MAX_NEGATIVE_ENTRIES {
            cache.clear();
        }
        cache.insert(path.to_string(), Instant::now());
    }

    /// Insert a stat result into the cache, clearing the whole cache when
    /// over the bound so memory stays bounded. Kept separate from `stat` so
    /// the bound logic is unit-testable without S3.
    fn cache_insert(&self, path: &str, entry: DirEntry) {
        let mut cache = self.stats.lock().unwrap();
        if cache.len() >= MAX_STAT_ENTRIES {
            cache.clear();
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
    pub async fn read_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let _permit = self.acquire().await?;
        let key = self.key_for(path);
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
            .key(&key)
            .range(&range)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) if is_s3_invalid_range(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e).context("s3 get"),
        };
        let body = resp.body.collect().await.context("s3 get body")?;
        Ok(body.to_vec())
    }

    /// Overwrite an object with `data` (whole-object write). Large objects
    /// are uploaded via S3 multipart so they are not limited by the single-PUT
    /// object-size cap and can be retried per part.
    pub async fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        self.ensure_writable()?;
        self.invalidate_stat(path);
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
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .context("s3 put")?;
        Ok(())
    }

    /// Multipart upload: initiate -> upload parts (bounded concurrency, one
    /// global permit per part) -> complete. Any failure aborts the upload so
    /// no unfinished multipart upload is left behind on the bucket.
    async fn write_multipart(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.key_for(path);

        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .context("s3 create multipart upload")?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("s3 create multipart upload returned no upload id"))?
            .to_string();

        let local = Arc::new(Semaphore::new(MULTIPART_UPLOAD_CONCURRENCY));
        let mut handles = tokio::task::JoinSet::new();
        let mut part_number = 1i32;
        let mut offset = 0usize;

        while offset < data.len() {
            let end = (offset + MULTIPART_PART_SIZE as usize).min(data.len());
            // Wait for a local slot so at most MULTIPART_UPLOAD_CONCURRENCY
            // part chunks are materialized in memory at once.
            let slot = local
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("multipart upload concurrency closed"))?;
            let chunk = data[offset..end].to_vec();
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
                let resp = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_no)
                    .body(ByteStream::from(chunk))
                    .send()
                    .await
                    .context("s3 upload part")?;
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
        if let Err(e) = self
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
            .send()
            .await
        {
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
        Ok(())
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
        let key = self.key_for(path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
        assert_eq!(effective_mode(true, 0o755, 0o644), 0o755);
        assert_eq!(effective_mode(false, 0o755, 0o644), 0o644);
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: Some(Arc::new(Semaphore::new(1))),
            upload_budget_units: 1,
            prefix: String::new(),
        };
        let data = vec![0u8; 2 * UPLOAD_BUDGET_UNIT];
        let err = fs.write("/large.bin", &data).await.unwrap_err();
        assert!(err.to_string().contains("max-upload-bytes budget"));
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
            read_only: false,
            uid: 0,
            gid: 0,
            dir_mode: 0o755,
            file_mode: 0o644,
            allow_rename_dir: true,
            rename_dir_limit: Some(2_000_000),
            max_upload_bytes: None,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
        assert_eq!(cache.len(), 1);
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
    struct MockRequest {
        method: String,
        target: String,
        body: Vec<u8>,
    }

    struct MockS3 {
        active: Arc<AtomicUsize>,
        max_concurrent: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<String>>>,
        recorded: Arc<Mutex<Vec<MockRequest>>>,
        delay: Duration,
        entries: Arc<Mutex<Vec<(String, bool)>>>,
    }

    impl MockS3 {
        async fn start(entries: Vec<(String, bool)>, delay: Duration) -> (Arc<Self>, u16) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let mock = Arc::new(MockS3 {
                active: Arc::new(AtomicUsize::new(0)),
                max_concurrent: Arc::new(AtomicUsize::new(0)),
                requests: Arc::new(Mutex::new(Vec::new())),
                recorded: Arc::new(Mutex::new(Vec::new())),
                delay,
                entries: Arc::new(Mutex::new(entries)),
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
        for line in head.lines() {
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
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
        });

        tokio::time::sleep(mock.delay).await;

        let response = if query.contains("list-type=2") {
            let entries = mock.entries.lock().unwrap().clone();
            let body = list_xml(&entries);
            http_response(200, "application/xml", Some(&format!("{body}")))
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
            http_response(200, "application/xml", Some(&complete_multipart_xml()))
        } else if method == "HEAD" {
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
        } else {
            // Plain PutObject / DeleteObject / ...
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
        };
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        mock.active.fetch_sub(1, Ordering::SeqCst);
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

    fn test_fs(port: u16, limit: usize) -> ObjectFs {
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
            mount_attr: MountAttr::default(),
            allow_rename_dir: true,
            rename_dir_limit: None,
            upload_budget: None,
            upload_budget_units: 0,
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
}
