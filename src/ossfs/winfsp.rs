//! Windows WinFsp mount adapter for the metadata-less object filesystem.
//!
//! Bridges WinFsp IRP callbacks to [`ObjectFs`](super::ObjectFs). Writes are
//! buffered in memory and flushed as a whole-object `PutObject` on
//! close/flush — the same "cloud drive" semantics as ossfs/s3fs.

use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::future::Future;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tracing::{debug, info, warn};

use winfsp::filesystem::{
    AsyncFileSystemContext, DirBuffer, DirInfo, DirMarker, FileInfo, FileSecurity,
    FileSystemContext, OpenFileInfo, VolumeInfo, WideNameInfo,
};
use winfsp::host::{FileSystemHost, FileSystemParams, VolumeParams};
use winfsp::notify::{Notifier, NotifyInfo, NotifyingFileSystemContext};
use winfsp::{FspError, U16CStr};

use super::{DirEntry, DirtyBudget, DirtyPermit, ObjectFs, StreamingUpload, spool_file_path};

const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;
const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;

const WIN32_FILE_NOT_FOUND: i32 = 2;
const WIN32_ACCESS_DENIED: i32 = 5;
const WIN32_INVALID_PARAMETER: i32 = 87;
const WIN32_ALREADY_EXISTS: i32 = 183;

/// Above this size a write handle spills its buffer to a temp file so a large
/// file copy cannot exhaust process memory.
const WRITE_SPOOL_THRESHOLD: usize = 8 * 1024 * 1024;

// Periodic directory refresh: when the OS has an active directory watch
// (Explorer window open), WinFsp calls our notifier every REFRESH_INTERVAL_MS
// so changes made by other machines appear without a manual F5. When nothing
// is watching, FspFileSystemNotifyBegin fails and no S3 listing happens.
const REFRESH_INTERVAL_MS: u32 = 10_000;
/// Upper bound on the number of directories the periodic change-notification
/// pass refreshes (root always included; oldest non-root evicted on overflow).
const MAX_TRACKED_DIRS: usize = 64;
/// Total-entry budget for the persisted notify snapshots. Browsing a huge
/// tree (e.g. `find /` recursion into the mounted network drive) otherwise
/// keeps full per-directory listings alive in `snapshots`, growing memory
/// without bound and OOM-aborting the process (0xc0000409).
const MAX_SNAPSHOT_ENTRIES: usize = 50_000;

// Win32 change-notification constants (fileapi.h).
const FILE_NOTIFY_CHANGE_FILE_NAME: u32 = 0x0000_0001;
const FILE_NOTIFY_CHANGE_DIR_NAME: u32 = 0x0000_0002;
const FILE_NOTIFY_CHANGE_SIZE: u32 = 0x0000_0008;
const FILE_NOTIFY_CHANGE_LAST_WRITE: u32 = 0x0000_0010;
const FILE_ACTION_ADDED: u32 = 1;
const FILE_ACTION_REMOVED: u32 = 2;
const FILE_ACTION_MODIFIED: u32 = 3;

const UNIX_TO_FILETIME_EPOCH_SECS: i64 = 11_644_473_600;

/// Convert a Unix timestamp (seconds) to Windows FILETIME (100ns since 1601).
fn unix_to_filetime(secs: i64) -> u64 {
    if secs <= 0 {
        return 0;
    }
    ((secs as i128 + UNIX_TO_FILETIME_EPOCH_SECS as i128) * 10_000_000) as u64
}

fn win_path_to_posix(name: &U16CStr) -> String {
    let s = name.to_string_lossy();
    if s.is_empty() {
        return "/".to_string();
    }
    let trimmed = s.trim_start_matches('\\');
    let replaced = trimmed.replace('\\', "/");
    if replaced.starts_with('/') {
        replaced
    } else {
        format!("/{replaced}")
    }
}

fn parent_posix(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(idx) => trimmed[..idx].to_string(),
    }
}

fn file_info_from(entry: &DirEntry, index: u64) -> FileInfo {
    let mut fi = FileInfo::default();
    fi.file_attributes = if entry.is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_ARCHIVE
    };
    fi.file_size = entry.size;
    fi.allocation_size = entry.size;
    fi.creation_time = unix_to_filetime(entry.mtime_secs);
    fi.last_access_time = unix_to_filetime(entry.mtime_secs);
    fi.last_write_time = unix_to_filetime(entry.mtime_secs);
    fi.change_time = unix_to_filetime(entry.mtime_secs);
    fi.index_number = index;
    fi.hard_links = 1;
    fi
}

fn wildcard_match(pattern: &str, name: &str) -> bool {
    fn inner(p: &[char], n: &[char]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some('*'), _) => inner(&p[1..], n) || (!n.is_empty() && inner(p, &n[1..])),
            (Some('?'), Some(_)) => inner(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => inner(&p[1..], &n[1..]),
            _ => false,
        }
    }
    inner(
        &pattern.chars().collect::<Vec<_>>(),
        &name.chars().collect::<Vec<_>>(),
    )
}

/// Per-open-file state. Writes are buffered whole-file; reads go straight to
/// the object store unless the file is open for write.
pub struct OssFileContext {
    /// POSIX path. `Mutex` so the `rename` callback (which only receives an
    /// immutable context) can retarget an open handle to its new path;
    /// otherwise a dirty handle would flush to the deleted old key and
    /// resurrect the object (#46).
    path: Mutex<String>,
    is_dir: bool,
    /// Whole-file write buffer; `Some` when the handle was opened for write.
    /// Content is loaded **lazily**: `loaded` stays false until the first
    /// operation that needs the original bytes (first write / truncate),
    /// so simply opening a file for write (e.g. preview/thumbnail handlers)
    /// no longer downloads the whole object.
    write_buf: Mutex<Option<Vec<u8>>>,
    loaded: AtomicBool,
    dirty: AtomicBool,
    delete_on_close: AtomicBool,
    dir_buffer: DirBuffer,
    /// High-water MiB units reserved from [`OssMountContext::dirty_budget`].
    budget_units: AtomicUsize,
    /// RAII permits for every reservation made by this handle.
    budget_permits: Mutex<Vec<DirtyPermit>>,
    /// When a write buffer grows beyond [`WRITE_SPOOL_THRESHOLD`], it is
    /// spilled to this temp file and subsequent writes append there.
    spool_path: Mutex<Option<PathBuf>>,
    /// Logical size of the spooled file (total bytes written so far).
    spool_size: AtomicU64,
    /// Streaming multipart upload for large files (write-while-upload).
    stream: tokio::sync::Mutex<Option<StreamingUpload>>,
    /// Set when a streaming multipart completion failed. `upload_dirty` then
    /// refuses to fall back to the whole-buffer PUT: the buffer was emptied
    /// into the stream, so that PUT would upload an empty object over the
    /// previous content.
    stream_failed: AtomicBool,
    /// Logical size reported while a streaming upload is in flight.
    logical_size: AtomicU64,
}

impl OssFileContext {
    fn index(&self) -> u64 {
        // Stable-ish per-path index derived from the string.
        self.path
            .lock()
            .unwrap()
            .as_bytes()
            .iter()
            .fold(0x9E37_79B9u64, |acc, b| {
                acc.wrapping_mul(31).wrapping_add(*b as u64)
            })
    }
}

/// Per-directory last-seen listing plus the recently-browsed directories the
/// periodic change-notification pass refreshes. Root is always tracked.
struct RefreshState {
    /// POSIX dir path -> (name -> (is_dir, size, mtime)) last seen by the
    /// change-notification diff.
    snapshots: HashMap<String, HashMap<String, (bool, u64, i64)>>,
    /// Directories whose baseline snapshot has been seeded at least once.
    /// Separate from the snapshot itself: an empty snapshot can be a valid
    /// baseline (empty directory), which must not be mistaken for "never
    /// listed".
    seeded: HashSet<String>,
    /// Recently-browsed directories to refresh, most recent last. Root is
    /// always present and never evicted; bounded by MAX_TRACKED_DIRS.
    dirs: Vec<String>,
    /// Total-entry budget for `snapshots` (see [`MAX_SNAPSHOT_ENTRIES`]);
    /// kept as a field so tests can shrink it.
    snapshot_budget: usize,
}

impl RefreshState {
    fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
            seeded: HashSet::new(),
            dirs: vec!["/".to_string()],
            snapshot_budget: MAX_SNAPSHOT_ENTRIES,
        }
    }

    /// Number of entries currently persisted across all snapshots.
    fn snapshot_entries(&self) -> usize {
        self.snapshots.values().map(|snap| snap.len()).sum()
    }

    /// Enforce the total-entry budget: evict the largest non-root snapshot
    /// until the total fits. Root is always kept so the volume-root watch
    /// keeps a baseline; an evicted directory simply re-seeds on the next
    /// notify pass (no change events fire for it in the meantime).
    fn enforce_snapshot_budget(&mut self) {
        while self.snapshot_entries() > self.snapshot_budget {
            let victim = self
                .snapshots
                .iter()
                .filter(|(dir, _)| dir.as_str() != "/")
                .max_by_key(|(_, snap)| snap.len())
                .map(|(dir, _)| dir.clone());
            let Some(victim) = victim else { break };
            if self.snapshots.remove(&victim).is_some() {
                self.seeded.remove(&victim);
            }
        }
    }

    /// Persist a directory listing as the notify-change baseline. Directories
    /// whose listing alone exceeds the budget are **not** persisted (storing
    /// one would blow the bound; the next pass re-seeds it, so no spurious
    /// change events fire). Afterwards the total budget is enforced.
    fn store_snapshot(&mut self, dir: &str, entries: &[DirEntry]) {
        if entries.len() > self.snapshot_budget {
            self.snapshots.remove(dir);
            self.seeded.remove(dir);
            return;
        }
        let snap: HashMap<String, (bool, u64, i64)> = entries
            .iter()
            .map(|e| (e.name.clone(), (e.is_dir, e.size, e.mtime_secs)))
            .collect();
        self.snapshots.insert(dir.to_string(), snap);
        self.seeded.insert(dir.to_string());
        self.enforce_snapshot_budget();
    }

    /// Mark `dir` as recently browsed (move to the most-recent position,
    /// evicting the oldest non-root entry when over the bound).
    fn record_browsed(&mut self, dir: &str) {
        if let Some(pos) = self.dirs.iter().position(|d| d == dir) {
            if pos + 1 != self.dirs.len() {
                let d = self.dirs.remove(pos);
                self.dirs.push(d);
            }
        } else if self.dirs.len() < MAX_TRACKED_DIRS {
            self.dirs.push(dir.to_string());
        } else if let Some(oldest) = self.dirs.get(1).cloned() {
            self.dirs.remove(1);
            self.snapshots.remove(&oldest);
            self.seeded.remove(&oldest);
            self.dirs.push(dir.to_string());
        }
    }
}

pub struct OssMountContext {
    fs: Arc<ObjectFs>,
    rt: Handle,
    mount_point: PathBuf,
    /// Per-directory last-seen listings + recently-browsed dirs used by the
    /// periodic change-notification diff.
    refresh: Mutex<RefreshState>,
    /// Optional mount-wide dirty-buffer budget.
    dirty_budget: Option<DirtyBudget>,
}

impl OssMountContext {
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future,
    {
        self.rt.block_on(fut)
    }

    /// Reserve dirty-buffer budget for `bytes`, if the mount configured one.
    /// Tracks the handle's high-water mark so a file that later shrinks does
    /// not need to release and reacquire permits.
    async fn reserve_dirty(&self, context: &OssFileContext, bytes: usize) -> winfsp::Result<()> {
        let Some(budget) = &self.dirty_budget else {
            return Ok(());
        };
        let new_units = bytes.div_ceil(budget.unit());
        if new_units > budget.max_units() {
            return Err(FspError::from(IoError::other(format!(
                "dirty buffer {bytes} bytes exceeds max-dirty-bytes budget"
            ))));
        }
        let current = context.budget_units.load(Ordering::Acquire);
        if new_units <= current {
            return Ok(());
        }
        let permit = budget
            .acquire_units(new_units - current)
            .await
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        context.budget_permits.lock().unwrap().push(permit);
        context.budget_units.store(new_units, Ordering::Release);
        Ok(())
    }

    /// Upload the handle's dirty content, streaming from the spool file when
    /// one was created so large files are never held whole in memory.
    ///
    /// On success the handle is no longer dirty: WinFsp fires both `flush`
    /// and `cleanup` when a modified handle closes, and a second upload would
    /// be wrong — for the stream/spool branches the buffers are reset here,
    /// so a repeat call would PUT an empty object over the one just written.
    async fn upload_dirty(&self, ctx: &OssFileContext) -> winfsp::Result<()> {
        if !ctx.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        if ctx.stream_failed.load(Ordering::Acquire) {
            return Err(FspError::from(IoError::other(
                "streaming upload previously failed; refusing to overwrite the object with partial data",
            )));
        }
        if let Some(up) = ctx.stream.lock().await.take() {
            if let Err(e) = up.finish().await {
                // The buffer was emptied into the stream, so a later retry
                // through the buffer path would PUT an empty object over the
                // previous content; remember that and refuse.
                ctx.stream_failed.store(true, Ordering::Release);
                return Err(FspError::from(IoError::other(e.to_string())));
            }
            // The upload is complete; drop the read-back spool (#47).
            if let Some(path) = ctx.spool_path.lock().unwrap().take() {
                let _ = std::fs::remove_file(&path);
            }
            ctx.spool_size.store(0, Ordering::Release);
            ctx.logical_size.store(0, Ordering::Release);
            *ctx.write_buf.lock().unwrap() = Some(Vec::new());
            ctx.loaded.store(false, Ordering::Release);
            ctx.dirty.store(false, Ordering::Release);
            return Ok(());
        }
        let spool = ctx.spool_path.lock().unwrap().clone();
        if let Some(path) = spool {
            self.fs
                .write_from_file(&*ctx.path.lock().unwrap(), &path)
                .await
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            let _ = std::fs::remove_file(&path);
            ctx.spool_path.lock().unwrap().take();
            ctx.spool_size.store(0, Ordering::Release);
            // The object is now authoritative; drop the stale in-memory
            // prefix so a later read on this handle re-fetches from S3.
            *ctx.write_buf.lock().unwrap() = Some(Vec::new());
            ctx.loaded.store(false, Ordering::Release);
            ctx.dirty.store(false, Ordering::Release);
            return Ok(());
        }
        let data = ctx.write_buf.lock().unwrap().clone();
        if let Some(data) = data {
            self.fs
                .write(&*ctx.path.lock().unwrap(), &data)
                .await
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        }
        // Small-buffer path keeps the buffer (later reads serve from it), so
        // only the flag needs clearing.
        ctx.dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Remember that the user browsed `dir` and seed its baseline snapshot
    /// with the listing just returned, so the periodic diff only reports
    /// changes made after this point.
    fn record_browsed(&self, dir: &str, entries: &[DirEntry]) {
        let mut state = self.refresh.lock().unwrap();
        state.record_browsed(dir);
        state.store_snapshot(dir, entries);
    }
}

/// Emit a single WinFsp change notification.
///
/// WinFsp requires the name to be **root-absolute** (`\dir\file`): names
/// without a leading backslash are treated as relative to a previous absolute
/// name in the same notify buffer and are silently dropped when none exists
/// (see FspVolumeNotifyWork in winfsp/src/sys/volume.c). `posix` is a POSIX
/// path relative to the filesystem root; it is converted to the Windows form.
fn notify_change(notifier: &Notifier, posix: &str, action: u32, filter: u32) {
    let mut info = NotifyInfo::<1024>::default();
    info.filter = filter;
    info.action = action;
    let win = format!("\\{}", posix.trim_start_matches('/').replace('/', "\\"));
    if info.set_name(win.as_str()).is_ok() {
        // `set_name` counts the trailing NUL in `Size`, but the WinFsp FSD
        // rejects names containing a NUL (FspFileNameIsValid), silently
        // dropping the notification. Shrink `Size` to the NUL-free name
        // length, exactly like the .NET `NotifyInfoInternal.SetFileNameBuf`.
        let chars = win.encode_utf16().count() as u16;
        let header = std::mem::size_of::<NotifyInfo<0>>() as u16;
        unsafe {
            // SAFETY: NotifyInfo is #[repr(C)] with `size: u16` at offset 0.
            let size_ptr = (&mut info as *mut NotifyInfo<1024>).cast::<u16>();
            std::ptr::write_volatile(size_ptr, header + chars * 2);
        }
        notifier.notify(&info);
    }
}

/// Join a POSIX directory path and entry name into a normalized POSIX path.
fn join_posix(dir: &str, name: &str) -> String {
    if dir == "/" {
        format!("/{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// Periodic change detection: every REFRESH_INTERVAL_MS (only when the OS
/// holds an active directory watch) list the bucket root and every
/// recently-browsed directory, diff each against its last-seen snapshot and
/// publish ADDED/REMOVED/MODIFIED events with root-absolute names. The FSD
/// routes each event to the matching watch (a subdirectory watch receives the
/// notification for changes under it), so open Explorer windows refresh
/// without a manual F5. When no window is watching, FspFileSystemNotifyBegin
/// fails and no S3 listing happens.
impl NotifyingFileSystemContext<()> for OssMountContext {
    fn should_notify(&self) -> Option<()> {
        debug!("[notify] should_notify called");
        Some(())
    }

    fn notify(&self, _context: (), notifier: &Notifier) {
        let dirs: Vec<String> = {
            let state = self.refresh.lock().unwrap();
            state.dirs.clone()
        };
        for dir in dirs {
            self.refresh_dir(notifier, &dir);
        }
    }
}

impl OssMountContext {
    fn refresh_dir(&self, notifier: &Notifier, dir: &str) {
        let current = match self.block_on(self.fs.list(dir)) {
            Ok(entries) => entries,
            Err(e) => {
                debug!(dir, error = ?e, "[notify] list failed");
                return;
            }
        };
        let mut state = self.refresh.lock().unwrap();
        // No baseline yet (the directory was never listed) -> just seed it.
        // A watch can exist on a directory that has not been enumerated yet;
        // reporting everything as ADDED would be wrong. Note: an *empty*
        // snapshot is a valid baseline (empty directory), not a missing one.
        if !state.seeded.contains(dir) {
            debug!(dir, count = current.len(), "[notify] seeding baseline");
            state.store_snapshot(dir, &current);
            return;
        }
        let snap = state.snapshots.entry(dir.to_string()).or_default();
        debug!(dir, count = current.len(), "[notify] diff");
        let mut seen = HashSet::with_capacity(current.len());
        for entry in &current {
            seen.insert(entry.name.clone());
            let sig = (entry.is_dir, entry.size, entry.mtime_secs);
            match snap.get(&entry.name) {
                Some(prev) if *prev != sig => {
                    let path = join_posix(dir, &entry.name);
                    debug!("[notify] MODIFIED {path}");
                    let filter = if entry.is_dir {
                        FILE_NOTIFY_CHANGE_DIR_NAME
                    } else {
                        FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE
                    };
                    notify_change(notifier, &path, FILE_ACTION_MODIFIED, filter);
                }
                None => {
                    let path = join_posix(dir, &entry.name);
                    debug!("[notify] ADDED {path}");
                    let filter = if entry.is_dir {
                        FILE_NOTIFY_CHANGE_DIR_NAME
                    } else {
                        FILE_NOTIFY_CHANGE_FILE_NAME
                    };
                    notify_change(notifier, &path, FILE_ACTION_ADDED, filter);
                }
                _ => {}
            }
        }
        let removed: Vec<(String, bool)> = snap
            .iter()
            .filter(|(k, _)| !seen.contains(*k))
            .map(|(k, v)| (k.clone(), v.0))
            .collect();
        for (name, was_dir) in removed {
            let path = join_posix(dir, &name);
            debug!("[notify] REMOVED {path}");
            let filter = if was_dir {
                FILE_NOTIFY_CHANGE_DIR_NAME
            } else {
                FILE_NOTIFY_CHANGE_FILE_NAME
            };
            notify_change(notifier, &path, FILE_ACTION_REMOVED, filter);
            snap.remove(&name);
        }
        // Persist the new baseline through the same budget-enforcing path as
        // seeding: a directory that grows past the budget mid-session is not
        // kept (re-seeded next pass) and the total-entry budget is never
        // exceeded even after diffs. (`snap`'s borrow ends after the removed
        // loop above; NLL releases it before this new mutable borrow.)
        state.store_snapshot(dir, &current);
    }
}

/// Mount the object filesystem at `mount_point` via WinFsp. Blocks until
/// Ctrl+C or the process receives a termination signal.
pub async fn mount_oss_winfsp(fs: Arc<ObjectFs>, mount_point: &Path) -> anyhow::Result<()> {
    ensure_winfsp_dll_discoverable();
    winfsp::winfsp_init()
        .map_err(|e| anyhow::anyhow!("WinFsp is not installed or could not be loaded: {e}"))?;

    // Verify the bucket is reachable and the prefix lists cleanly BEFORE
    // mounting. Without this, a misconfigured endpoint (e.g. an Aliyun OSS
    // access-point URL that the SDK cannot address) mounts a volume whose
    // every operation fails with a generic I/O error.
    match fs.list("/").await {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "ossmount: S3 连通性检查失败，拒绝挂载。请检查 endpoint/bucket/密钥配置：{e:?}"
            );
            anyhow::bail!("S3 connectivity check failed: {e:?}");
        }
    }

    let rt = Handle::current();
    let read_only = fs.read_only();
    let dirty_budget = fs.dirty_budget();
    let context = OssMountContext {
        fs,
        rt,
        mount_point: mount_point.to_path_buf(),
        refresh: Mutex::new(RefreshState::new()),
        dirty_budget,
    };
    let params = FileSystemParams::default_params(build_volume_params(read_only));
    let mut host = FileSystemHost::new_with_timer_async::<(), REFRESH_INTERVAL_MS>(params, context)
        .map_err(|e| anyhow::anyhow!("failed to create WinFsp filesystem host: {e}"))?;

    host.mount(mount_point)
        .map_err(|e| anyhow::anyhow!("failed to mount at {}: {e}", mount_point.display()))?;
    if let Err(e) = host.start() {
        host.unmount();
        return Err(anyhow::anyhow!("failed to start WinFsp dispatcher: {e}"));
    }

    info!(mount_point = %mount_point.display(), "ossfs-oss mounted via WinFsp");
    println!("mounted at {}", mount_point.display());
    write_runtime_record(mount_point);

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("unmounting...");
        }
    }

    host.stop();
    host.unmount();
    remove_runtime_record();
    Ok(())
}

fn build_volume_params(read_only: bool) -> VolumeParams {
    let mut vp = VolumeParams::new();
    vp.read_only_volume(read_only);
    vp.sector_size(512)
        .sectors_per_allocation_unit(8)
        .max_component_length(255)
        .filesystem_name("OSSFS-OSS")
        .case_sensitive_search(true)
        .case_preserved_names(true)
        .unicode_on_disk(true)
        .persistent_acls(false)
        .reparse_points(false)
        .post_cleanup_when_modified_only(true)
        .flush_and_purge_on_cleanup(true)
        .pass_query_directory_pattern(true)
        .file_info_timeout(1000)
        .dir_info_timeout(1000);
    vp
}

impl FileSystemContext for OssMountContext {
    type FileContext = OssFileContext;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        let posix = win_path_to_posix(file_name);
        let entry = self
            .block_on(self.fs.stat(&posix))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        let entry = entry
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: if entry.is_dir {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_ARCHIVE
                    | if self.fs.read_only() {
                        FILE_ATTRIBUTE_READONLY
                    } else {
                        0
                    }
            },
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        granted_access: u32,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let posix = win_path_to_posix(file_name);
        let entry = self
            .block_on(self.fs.stat(&posix))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        let entry = entry
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        let is_dir = entry.is_dir;
        if create_options & FILE_DIRECTORY_FILE != 0 && !is_dir {
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32)); // STATUS_FILE_IS_A_DIRECTORY
        }
        if create_options & FILE_NON_DIRECTORY_FILE != 0 && is_dir {
            return Err(FspError::NTSTATUS(0xC000_0103u32 as i32)); // STATUS_NOT_A_DIRECTORY
        }

        let write = granted_access & 0x2 != 0 || granted_access & 0x4000_0000 != 0;
        if write && self.fs.read_only() {
            return Err(FspError::NTSTATUS(WIN32_ACCESS_DENIED));
        }
        let write_buf = if is_dir {
            None
        } else if write {
            // Lazy: the existing content is fetched on the first write or
            // truncate that needs it (see write_async), so opening a file
            // for write never downloads the whole object.
            Some(Vec::new())
        } else {
            None
        };
        *file_info.as_mut() = file_info_from(&entry, file_index(&posix));
        Ok(OssFileContext {
            path: Mutex::new(posix),
            is_dir,
            write_buf: Mutex::new(write_buf),
            loaded: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
            delete_on_close: AtomicBool::new(false),
            dir_buffer: DirBuffer::new(),
            budget_units: AtomicUsize::new(0),
            budget_permits: Mutex::new(Vec::new()),
            spool_path: Mutex::new(None),
            spool_size: AtomicU64::new(0),
            stream: tokio::sync::Mutex::new(None),
            stream_failed: AtomicBool::new(false),
            logical_size: AtomicU64::new(0),
        })
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: u32,
        _file_attributes: u32,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let posix = win_path_to_posix(file_name);
        let is_dir = create_options & FILE_DIRECTORY_FILE != 0;
        if self.fs.read_only() {
            return Err(FspError::NTSTATUS(WIN32_ACCESS_DENIED));
        }
        if is_dir {
            self.block_on(self.fs.mkdir(&posix))
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        }
        let entry = DirEntry {
            name: posix.clone(),
            is_dir,
            size: 0,
            mtime_secs: 0,
        };
        let write_buf = if is_dir { None } else { Some(Vec::new()) };
        *file_info.as_mut() = file_info_from(&entry, file_index(&posix));
        Ok(OssFileContext {
            path: Mutex::new(posix),
            is_dir,
            write_buf: Mutex::new(write_buf),
            loaded: AtomicBool::new(true),
            dirty: AtomicBool::new(false),
            delete_on_close: AtomicBool::new(false),
            dir_buffer: DirBuffer::new(),
            budget_units: AtomicUsize::new(0),
            budget_permits: Mutex::new(Vec::new()),
            spool_path: Mutex::new(None),
            spool_size: AtomicU64::new(0),
            stream: tokio::sync::Mutex::new(None),
            stream_failed: AtomicBool::new(false),
            logical_size: AtomicU64::new(0),
        })
    }

    fn cleanup(&self, context: &Self::FileContext, _file_name: Option<&U16CStr>, flags: u32) {
        let delete_requested = context.delete_on_close.load(Ordering::Acquire)
            || winfsp::constants::FspCleanupFlags::FspCleanupDelete.is_flagged(flags);
        if delete_requested {
            let path = context.path.lock().unwrap().clone();
            let is_dir = context.is_dir;
            let fs = Arc::clone(&self.fs);
            let result = self.block_on({
                let path = path.clone();
                async move {
                    if is_dir {
                        fs.delete_dir_recursive(&path).await
                    } else {
                        fs.delete(&path).await
                    }
                }
            });
            match result {
                Ok(()) => debug!(path = log_path(&path), "ossfs cleanup deleted"),
                Err(e) => warn!(path = log_path(&path), error = ?e, "ossfs cleanup delete failed"),
            }
            return;
        }
        if context.dirty.load(Ordering::Acquire)
            && let Err(e) = self.block_on(self.upload_dirty(context))
        {
            warn!(path = log_path(&*context.path.lock().unwrap()), error = ?e, "ossfs cleanup flush failed");
        }
    }

    fn close(&self, _context: Self::FileContext) {}

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        _file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let Some(ctx) = context else { return Ok(()) };
        self.block_on(self.upload_dirty(ctx))
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let logical_size = context.logical_size.load(Ordering::Acquire);
        if logical_size > 0 {
            *file_info = file_info_from(
                &DirEntry {
                    name: context.path.lock().unwrap().clone(),
                    is_dir: context.is_dir,
                    size: logical_size,
                    mtime_secs: 0,
                },
                context.index(),
            );
            return Ok(());
        }
        if context.spool_path.lock().unwrap().is_some() {
            let size = context.spool_size.load(Ordering::Acquire);
            *file_info = file_info_from(
                &DirEntry {
                    name: context.path.lock().unwrap().clone(),
                    is_dir: context.is_dir,
                    size,
                    mtime_secs: 0,
                },
                context.index(),
            );
            return Ok(());
        }
        if let Some(buf) = context.write_buf.lock().unwrap().as_ref()
            && context.loaded.load(Ordering::Acquire)
        {
            *file_info = file_info_from(
                &DirEntry {
                    name: context.path.lock().unwrap().clone(),
                    is_dir: context.is_dir,
                    size: buf.len() as u64,
                    mtime_secs: 0,
                },
                context.index(),
            );
            return Ok(());
        }
        let entry = self
            .block_on(self.fs.stat(&*context.path.lock().unwrap()))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        *file_info = file_info_from(&entry, context.index());
        Ok(())
    }

    fn overwrite(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _replace_file_attributes: bool,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if context.is_dir {
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32));
        }
        // #47: an in-flight streaming upload must be aborted or the bytes
        // written after the overwrite would append to the old stream and the
        // object would end up containing pre-overwrite content.
        {
            let mut stream_guard = self.block_on(async { context.stream.lock().await });
            if let Some(up) = stream_guard.take() {
                self.block_on(up.abort());
            }
        }
        if let Some(path) = context.spool_path.lock().unwrap().take() {
            let _ = std::fs::remove_file(&path);
            context.spool_size.store(0, Ordering::Release);
        }
        context.logical_size.store(0, Ordering::Release);
        if let Some(buf) = context.write_buf.lock().unwrap().as_mut() {
            buf.clear();
        }
        // The (now empty) buffer is the authoritative content; no S3 fetch.
        context.loaded.store(true, Ordering::Release);
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.lock().unwrap().clone(),
            is_dir: false,
            size: 0,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(())
    }

    fn rename(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_exists: bool,
    ) -> winfsp::Result<()> {
        self.block_on(self.rename_async(context, file_name, new_file_name, replace_if_exists))
    }

    /// Async core of [`Self::rename`] (kept separate so tests can drive it
    /// without a WinFsp dispatcher thread to block on).
    async fn rename_async(
        &self,
        context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_exists: bool,
    ) -> winfsp::Result<()> {
        let old = win_path_to_posix(file_name);
        let new = win_path_to_posix(new_file_name);
        let fs = Arc::clone(&self.fs);
        let new_for_upload = new.clone();
        fs.rename(&old, &new_for_upload, replace_if_exists)
            .await
            .map_err(|e| {
                let io = IoError::other(e.to_string());
                if e.to_string().contains("target already exists") {
                    FspError::from(IoError::from_raw_os_error(WIN32_ALREADY_EXISTS))
                } else {
                    FspError::from(io)
                }
            })?;
        // Retarget this handle to the new path so a later flush writes the
        // new key instead of resurrecting the deleted old object (#46).
        *context.path.lock().unwrap() = new;
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        _last_access_time: u64,
        _last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // Object storage has no settable timestamps; nothing to do. Prefer
        // the in-memory size for a loaded write handle (matches get_file_info
        // and the FUSE adapter's effective_attr).
        let buf_size = if context.logical_size.load(Ordering::Acquire) > 0 {
            Some(context.logical_size.load(Ordering::Acquire))
        } else if context.spool_path.lock().unwrap().is_some() {
            Some(context.spool_size.load(Ordering::Acquire))
        } else {
            let guard = context.write_buf.lock().unwrap();
            match guard.as_ref() {
                Some(buf) if context.loaded.load(Ordering::Acquire) => Some(buf.len() as u64),
                _ => None,
            }
        };
        let entry = if let Some(size) = buf_size {
            DirEntry {
                name: context.path.lock().unwrap().clone(),
                is_dir: context.is_dir,
                size,
                mtime_secs: 0,
            }
        } else {
            self.block_on(self.fs.stat(&*context.path.lock().unwrap()))
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?
                .unwrap_or(DirEntry {
                    name: context.path.lock().unwrap().clone(),
                    is_dir: context.is_dir,
                    size: 0,
                    mtime_secs: 0,
                })
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(())
    }

    fn set_delete(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        delete_file: bool,
    ) -> winfsp::Result<()> {
        context
            .delete_on_close
            .store(delete_file, Ordering::Release);
        Ok(())
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        new_size: u64,
        _set_allocation_size: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if context.is_dir {
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32));
        }

        if new_size == 0 {
            // Truncate to zero: abort any in-flight stream (bytes written
            // after the truncate would otherwise append to it), discard the
            // spool and clear the buffer (#47).
            {
                let mut stream_guard = self.block_on(async { context.stream.lock().await });
                if let Some(up) = stream_guard.take() {
                    self.block_on(up.abort());
                }
            }
            if let Some(path) = context.spool_path.lock().unwrap().take() {
                let _ = std::fs::remove_file(&path);
                context.spool_size.store(0, Ordering::Release);
            }
            context.logical_size.store(0, Ordering::Release);
            let mut guard = context.write_buf.lock().unwrap();
            if let Some(buf) = guard.as_mut() {
                buf.clear();
            }
            context.loaded.store(true, Ordering::Release);
        } else {
            // Pre-allocation and SetEndOfFile only change the logical size;
            // the actual bytes are streamed/buffered by write_async. Never
            // materialize the file here (that was the 20GB OOM source).
            context.logical_size.store(new_size, Ordering::Release);
        }
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.lock().unwrap().clone(),
            is_dir: false,
            size: new_size,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(())
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> winfsp::Result<()> {
        out_volume_info.total_size = 1 << 50;
        out_volume_info.free_size = 1 << 50;
        out_volume_info.set_volume_label("OSSFS-OSS");
        Ok(())
    }
}

impl AsyncFileSystemContext for OssMountContext {
    fn spawn_task(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.rt.spawn(future);
    }

    async fn read_async(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        if buffer.is_empty() {
            return Ok(0);
        }
        {
            let spool = context.spool_path.lock().unwrap().clone();
            if let Some(path) = spool {
                let mut file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                tokio::io::AsyncSeekExt::seek(&mut file, std::io::SeekFrom::Start(offset))
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                let n = tokio::io::AsyncReadExt::read(&mut file, buffer)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                return Ok(n as u32);
            }
        }
        {
            let guard = context.write_buf.lock().unwrap();
            if let Some(buf) = guard.as_ref() {
                // Only serve from the buffer once the original content has
                // been loaded; before that the object is unmodified, so read
                // straight from S3.
                if context.loaded.load(Ordering::Acquire) {
                    let start = offset.min(buf.len() as u64) as usize;
                    let n = (buf.len() - start).min(buffer.len());
                    buffer[..n].copy_from_slice(&buf[start..start + n]);
                    return Ok(n as u32);
                }
            }
        }
        let read_path = context.path.lock().unwrap().clone();
        match self.fs.read_range(&read_path, offset, buffer.len()).await {
            Ok(data) => {
                let n = data.len().min(buffer.len());
                buffer[..n].copy_from_slice(&data[..n]);
                Ok(n as u32)
            }
            Err(e) => {
                eprintln!(
                    "ossfs read_range err path={} offset={} len={}: {e:?}",
                    *context.path.lock().unwrap(),
                    offset,
                    buffer.len()
                );
                Err(FspError::from(IoError::other(e.to_string())))
            }
        }
    }

    async fn write_async(
        &self,
        context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        write_to_eof: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<u32> {
        if buffer.is_empty() {
            return Ok(0);
        }

        // Streaming multipart already active: feed it directly (upload
        // overlaps with the local write).
        {
            let mut stream_guard = context.stream.lock().await;
            if let Some(up) = stream_guard.as_mut() {
                // #47: the streaming upload is append-only. A write anchored
                // anywhere but the current end (or past it) would silently
                // corrupt the object — reject it explicitly.
                let base = if write_to_eof {
                    context.logical_size.load(Ordering::Acquire)
                } else {
                    offset
                };
                let cur = context.logical_size.load(Ordering::Acquire);
                if base != cur {
                    return Err(FspError::from(IoError::other(format!(
                        "streaming write at offset {base} while current size is {cur}"
                    ))));
                }
                // Keep the read-back spool in sync with the stream (#47).
                let spool = context.spool_path.lock().unwrap().clone();
                if let Some(path) = spool {
                    let mut f = tokio::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .await
                        .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                    tokio::io::AsyncWriteExt::write_all(&mut f, buffer)
                        .await
                        .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                }
                context
                    .spool_size
                    .fetch_add(buffer.len() as u64, Ordering::Release);
                up.write(buffer)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                let end = base.saturating_add(buffer.len() as u64);
                let cur = context.logical_size.load(Ordering::Acquire);
                if end > cur {
                    context.logical_size.store(end, Ordering::Release);
                }
                context.dirty.store(true, Ordering::Release);
                let entry = DirEntry {
                    name: context.path.lock().unwrap().clone(),
                    is_dir: false,
                    size: context.logical_size.load(Ordering::Acquire),
                    mtime_secs: 0,
                };
                *file_info = file_info_from(&entry, context.index());
                return Ok(buffer.len() as u32);
            }
        }

        // Lazy load the original content (for overwrite handles). Reserve the
        // dirty-buffer budget from the stat'd size BEFORE downloading: the
        // download itself allocates the whole object, so a post-hoc reserve
        // cannot stop an oversized object from exhausting process memory.
        // Only meaningful when the mount has a budget configured (without one
        // the stat would be dead work).
        if !context.loaded.load(Ordering::Acquire) {
            if self.dirty_budget.is_some() {
                let stat_path = context.path.lock().unwrap().clone();
                let remote_size = self
                    .fs
                    .stat(&stat_path)
                    .await
                    .ok()
                    .flatten()
                    .map(|e| e.size as usize)
                    .unwrap_or(0);
                self.reserve_dirty(context, remote_size).await?;
            }
            let lazy_path = context.path.lock().unwrap().clone();
            let data = self
                .fs
                .read_range(&lazy_path, 0, usize::MAX)
                .await
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            // The object may have grown since stat; top up the reservation.
            if self.dirty_budget.is_some() {
                self.reserve_dirty(context, data.len()).await?;
            }
            let mut guard = context.write_buf.lock().unwrap();
            let Some(buf) = guard.as_mut() else {
                return Err(FspError::from(IoError::from_raw_os_error(
                    WIN32_ACCESS_DENIED,
                )));
            };
            if !context.loaded.load(Ordering::Acquire) {
                *buf = data;
                context.loaded.store(true, Ordering::Release);
            }
        }

        let cur_size = context
            .write_buf
            .lock()
            .unwrap()
            .as_ref()
            .map(|b| b.len() as u64)
            .unwrap_or(0);
        let effective = if write_to_eof { cur_size } else { offset };
        let new_size = (effective as usize).saturating_add(buffer.len());

        // Switch to streaming multipart once the buffer would exceed the
        // in-memory threshold.
        if new_size > WRITE_SPOOL_THRESHOLD {
            self.reserve_dirty(context, new_size).await?;
            let existing = context.write_buf.lock().unwrap().clone();
            let stream_path = context.path.lock().unwrap().clone();
            let mut up = self
                .fs
                .begin_streaming_upload(&stream_path)
                .await
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            if let Some(existing) = &existing
                && !existing.is_empty()
            {
                up.write(existing)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            }
            up.write(buffer)
                .await
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            // #47: spill everything written so far to a temp file so reads on
            // this handle see the bytes (multipart parts are invisible until
            // the upload completes). read_async serves from the spool.
            let spool = spool_file_path();
            {
                let mut f = tokio::fs::File::create(&spool)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                if let Some(existing) = &existing {
                    tokio::io::AsyncWriteExt::write_all(&mut f, existing)
                        .await
                        .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
                }
                tokio::io::AsyncWriteExt::write_all(&mut f, buffer)
                    .await
                    .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            }
            *context.spool_path.lock().unwrap() = Some(spool);
            context.spool_size.store(new_size as u64, Ordering::Release);
            *context.write_buf.lock().unwrap() = Some(Vec::new());
            context.loaded.store(true, Ordering::Release);
            *context.stream.lock().await = Some(up);
            let sz = context
                .logical_size
                .load(Ordering::Acquire)
                .max(new_size as u64);
            context.logical_size.store(sz, Ordering::Release);
            context.dirty.store(true, Ordering::Release);
            let entry = DirEntry {
                name: context.path.lock().unwrap().clone(),
                is_dir: false,
                size: new_size as u64,
                mtime_secs: 0,
            };
            *file_info = file_info_from(&entry, context.index());
            return Ok(buffer.len() as u32);
        }

        self.reserve_dirty(context, new_size).await?;
        {
            let mut guard = context.write_buf.lock().unwrap();
            let Some(buf) = guard.as_mut() else {
                return Err(FspError::from(IoError::from_raw_os_error(
                    WIN32_ACCESS_DENIED,
                )));
            };
            let start = if write_to_eof {
                buf.len()
            } else {
                offset as usize
            };
            if start + buffer.len() > buf.len() {
                buf.resize(start + buffer.len(), 0);
            }
            buf[start..start + buffer.len()].copy_from_slice(buffer);
        }
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.lock().unwrap().clone(),
            is_dir: false,
            size: new_size as u64,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(buffer.len() as u32)
    }

    async fn read_directory_async(
        &self,
        context: &Self::FileContext,
        pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        let dir_path = context.path.lock().unwrap().clone();
        let entries = self.fs.list(&dir_path).await.map_err(|e| {
            eprintln!("ossmount: 列目录失败 {}: {e:?}", dir_path);
            FspError::from(IoError::other(e.to_string()))
        })?;

        // Remember this directory and its listing so the periodic
        // change-notification pass can diff it and refresh open views.
        self.record_browsed(&*context.path.lock().unwrap(), &entries);

        let is_root = *context.path.lock().unwrap() == "/";
        let pat = pattern.map(|p| p.to_string_lossy());

        // Resume from the marker entry if present. Entries are streamed
        // straight into the WinFsp buffer (no second full Vec is built), so
        // a huge directory costs one listing allocation instead of two.
        let start = match marker.inner() {
            Some(name) => {
                let name = String::from_utf16_lossy(name);
                entries
                    .iter()
                    .position(|e| e.name == name)
                    .map(|i| i + 1)
                    .unwrap_or(0)
            }
            None => 0,
        };

        let matches = |name: &str| pat.as_deref().is_none_or(|p| wildcard_match(p, name));

        // Fetch "." / ".." attributes BEFORE acquiring the DirBuffer lock:
        // these stat calls wait on the S3 limiter and do network I/O, which
        // must not happen while holding the kernel-side directory buffer
        // lock. Only the first page (start == 0) emits them, matching the
        // original listing where the dots preceded every real entry.
        let mut dots: Vec<(String, DirEntry)> = Vec::new();
        if !is_root && start == 0 {
            let dot_path = context.path.lock().unwrap().clone();
            if matches(".")
                && let Ok(Some(dot)) = self.fs.stat(&dot_path).await
            {
                dots.push((".".to_string(), dot));
            }
            let parent = parent_posix(&dot_path);
            if matches("..")
                && let Ok(Some(dotdot)) = self.fs.stat(&parent).await
            {
                dots.push(("..".to_string(), dotdot));
            }
        }

        let lock = context
            .dir_buffer
            .acquire(marker.is_none(), Some(buffer.len() as u32))?;

        for (name, dot) in dots {
            let mut di = DirInfo::<255>::new();
            if di.set_name(&name).is_ok() {
                *di.file_info_mut() = file_info_from(&dot, file_index(&name));
                lock.write(&mut di)?;
            }
        }

        for entry in entries.iter().skip(start) {
            if !matches(&entry.name) {
                continue;
            }
            let mut di = DirInfo::<255>::new();
            if let Err(e) = di.set_name(&entry.name) {
                debug!(name = %entry.name, error = ?e, "ossfs readdir entry name too long");
                continue;
            }
            *di.file_info_mut() = file_info_from(entry, file_index(&entry.name));
            lock.write(&mut di)?;
        }
        drop(lock);

        Ok(context.dir_buffer.read(marker, buffer))
    }
}

fn file_index(path: &str) -> u64 {
    path.as_bytes().iter().fold(0x9E37_79B9u64, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(*b as u64)
    })
}

fn log_path(path: &str) -> &str {
    path
}

/// Runtime record the desktop tray app uses to list and stop `ossmount`
/// instances. Kept in `%TEMP%\ossfs-oss` so it never mixes with the OSSFS
/// control-plane registry (`%TEMP%\ossfs`).
fn runtime_record_path(pid: u32) -> PathBuf {
    std::env::temp_dir()
        .join("ossfs-oss")
        .join(format!("{pid}.json"))
}

fn write_runtime_record(mount_point: &Path) {
    let dir = std::env::temp_dir().join("ossfs-oss");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        warn!(error = ?e, "ossfs failed to create runtime record dir");
        return;
    }
    let record = serde_json::json!({
        "pid": std::process::id(),
        "mount_point": mount_point.display().to_string(),
        "socket_path": "",
        "started_at": chrono::Utc::now().to_rfc3339(),
    });
    let data = serde_json::to_vec_pretty(&record).unwrap_or_default();
    if let Err(e) = std::fs::write(runtime_record_path(std::process::id()), data) {
        warn!(error = ?e, "ossfs failed to write runtime record");
    }
}

fn remove_runtime_record() {
    let _ = std::fs::remove_file(runtime_record_path(std::process::id()));
}

unsafe extern "system" {
    #[link(name = "kernel32")]
    fn SetDllDirectoryW(lp_path_name: *const u16) -> i32;
}

fn ensure_winfsp_dll_discoverable() {
    let candidates = [
        r"C:\Program Files (x86)\WinFsp\bin",
        r"C:\Program Files\WinFsp\bin",
    ];
    for dir in candidates {
        if Path::new(dir).join("winfsp-x64.dll").exists() {
            let wide: Vec<u16> = dir.encode_utf16().chain(std::iter::once(0)).collect();
            // SAFETY: SetDllDirectoryW points at a valid NUL-terminated wide
            // string kept alive for the duration of the call.
            unsafe {
                SetDllDirectoryW(wide.as_ptr());
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ossfs::{MockS3, test_fs_with_budget};
    use std::time::Duration;

    /// NUL-terminated U16CStr for callback tests (leaked, tests only).
    fn w(s: &str) -> &'static U16CStr {
        let mut units: Vec<u16> = s.encode_utf16().collect();
        units.push(0);
        let leaked: &'static mut [u16] = Box::leak(units.into_boxed_slice());
        U16CStr::from_slice(leaked).unwrap()
    }

    fn entry(name: &str) -> DirEntry {
        DirEntry {
            name: name.to_string(),
            is_dir: false,
            size: 0,
            mtime_secs: 0,
        }
    }

    #[test]
    fn snapshot_budget_evicts_largest_non_root() {
        let mut state = RefreshState::new();
        state.snapshot_budget = 100;
        state.store_snapshot("/", &[entry("root.txt")]);

        let big: Vec<DirEntry> = (0..60).map(|i| entry(&format!("f{i}"))).collect();
        let mid: Vec<DirEntry> = (0..40).map(|i| entry(&format!("g{i}"))).collect();
        let small: Vec<DirEntry> = (0..30).map(|i| entry(&format!("h{i}"))).collect();
        state.store_snapshot("/big", &big);
        state.store_snapshot("/mid", &mid);
        state.store_snapshot("/small", &small);

        // 60 + 40 + 30 = 130 > 100 budget -> the largest (60) is evicted.
        assert!(state.snapshot_entries() <= 100, "budget exceeded");
        assert!(!state.snapshots.contains_key("/big"));
        assert!(!state.seeded.contains("/big"));
        // Root baseline is always kept.
        assert!(state.snapshots.contains_key("/"));
        assert!(state.snapshots.contains_key("/mid"));
        assert!(state.snapshots.contains_key("/small"));
    }

    #[test]
    fn snapshot_skips_directory_larger_than_budget() {
        let mut state = RefreshState::new();
        state.snapshot_budget = 10;
        let huge: Vec<DirEntry> = (0..11).map(|i| entry(&format!("f{i}"))).collect();
        state.store_snapshot("/huge", &huge);
        assert!(!state.snapshots.contains_key("/huge"));
        assert!(!state.seeded.contains("/huge"));
    }

    #[test]
    fn refresh_growth_evicts_largest_when_total_exceeds_budget() {
        let mut state = RefreshState::new();
        state.snapshot_budget = 100;
        let a60: Vec<DirEntry> = (0..60).map(|i| entry(&format!("a{i}"))).collect();
        let b40: Vec<DirEntry> = (0..40).map(|i| entry(&format!("b{i}"))).collect();
        state.store_snapshot("/a", &a60);
        state.store_snapshot("/b", &b40);
        assert_eq!(state.snapshot_entries(), 100);
        // /b grows to 50 -> total 110 > 100 -> largest non-root (/a, 60)
        // is evicted, mirroring the refresh_dir diff path (which now goes
        // through store_snapshot).
        let b50: Vec<DirEntry> = (0..50).map(|i| entry(&format!("b{i}"))).collect();
        state.store_snapshot("/b", &b50);
        assert!(
            state.snapshot_entries() <= 100,
            "budget exceeded after growth"
        );
        assert!(!state.snapshots.contains_key("/a"));
        assert!(state.snapshots.contains_key("/b"));
    }

    #[test]
    fn refresh_growth_past_single_dir_cap_drops_snapshot() {
        let mut state = RefreshState::new();
        state.snapshot_budget = 20;
        let small: Vec<DirEntry> = (0..5).map(|i| entry(&format!("a{i}"))).collect();
        state.store_snapshot("/d", &small);
        assert!(state.snapshots.contains_key("/d"));
        // The directory grows past the whole budget mid-session: the
        // baseline must be dropped (re-seed next pass), not persisted.
        let big: Vec<DirEntry> = (0..30).map(|i| entry(&format!("b{i}"))).collect();
        state.store_snapshot("/d", &big);
        assert!(!state.snapshots.contains_key("/d"));
        assert!(!state.seeded.contains("/d"));
        assert!(state.snapshot_entries() <= 20);
    }

    #[test]
    fn record_browsed_keeps_dirs_bounded() {
        let mut state = RefreshState::new();
        for i in 0..MAX_TRACKED_DIRS {
            let dir = format!("/d{i}");
            state.record_browsed(&dir);
            state.store_snapshot(&dir, &[entry(&format!("f{i}"))]);
        }
        assert!(state.dirs.len() <= MAX_TRACKED_DIRS);
        // Oldest non-root dirs are evicted from both the refresh list and
        // the snapshot map as new dirs are browsed.
        assert!(!state.dirs.contains(&"/d0".to_string()));
        assert!(!state.snapshots.contains_key("/d0"));
        assert!(state.dirs.contains(&"/".to_string()));
    }

    // -------------------------------------------------------------------
    // Large-file write / flush regression tests (in-process S3 mock)
    // -------------------------------------------------------------------

    /// Mount context for adapter-level upload tests, mirroring what
    /// `mount_oss_winfsp` wires up (including the dirty budget).
    fn test_mount(fs: ObjectFs) -> (Arc<ObjectFs>, OssMountContext) {
        let fs = Arc::new(fs);
        let ctx = OssMountContext {
            fs: Arc::clone(&fs),
            rt: Handle::current(),
            mount_point: PathBuf::from("Z:"),
            refresh: Mutex::new(RefreshState::new()),
            dirty_budget: fs.dirty_budget(),
        };
        (fs, ctx)
    }

    /// Leaked file handle. `DirBuffer`'s `Drop` calls
    /// `FspFileSystemDeleteDirectoryBuffer`, a delay-loaded `winfsp-x64.dll`
    /// import: dropping it on a machine without WinFsp installed raises
    /// 0xC06D007E (MOD_NOT_FOUND) and aborts the whole test binary. Leaking
    /// the handle keeps that drop out of the test (tiny allocation per test;
    /// same approach as the existing `w()` helper).
    fn test_file_with(path: &str, loaded: bool) -> &'static OssFileContext {
        Box::leak(Box::new(OssFileContext {
            path: Mutex::new(path.to_string()),
            is_dir: false,
            write_buf: Mutex::new(Some(Vec::new())),
            loaded: AtomicBool::new(loaded),
            dirty: AtomicBool::new(false),
            delete_on_close: AtomicBool::new(false),
            dir_buffer: DirBuffer::new(),
            budget_units: AtomicUsize::new(0),
            budget_permits: Mutex::new(Vec::new()),
            spool_path: Mutex::new(None),
            spool_size: AtomicU64::new(0),
            stream: tokio::sync::Mutex::new(None),
            stream_failed: AtomicBool::new(false),
            logical_size: AtomicU64::new(0),
        }))
    }

    fn test_file(path: &str) -> &'static OssFileContext {
        test_file_with(path, true)
    }

    /// Whole-object PUTs. The AWS SDK appends `?ln=<Operation>` to every
    /// request and multipart parts carry `partNumber`/`uploadId` (camelCase),
    /// so classify by the lowercase query rather than by the presence of `?`.
    fn plain_put_count(mock: &MockS3) -> usize {
        mock.recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.method == "PUT" && {
                    let q = r.target.to_lowercase();
                    !q.contains("partnumber") && !q.contains("uploadid")
                }
            })
            .count()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_then_cleanup_uploads_small_buffer_exactly_once() {
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/f");
        let mut fi = FileInfo::default();
        let data = b"hello ossfs".to_vec();
        let written = ctx
            .write_async(file, &data, 0, false, false, &mut fi)
            .await
            .expect("write");
        assert_eq!(written as usize, data.len());
        assert!(file.dirty.load(Ordering::Acquire));

        // WinFsp fires both `flush` (FlushFileBuffers) and `cleanup` when a
        // modified handle closes; the second must be a no-op. Regression:
        // upload_dirty never cleared dirty, so cleanup re-uploaded the whole
        // buffer (and, after a finished stream, PUT an empty object over it).
        ctx.upload_dirty(file).await.expect("flush");
        assert_eq!(plain_put_count(&mock), 1, "flush uploads once");
        assert!(
            !file.dirty.load(Ordering::Acquire),
            "dirty must be cleared after a successful flush"
        );
        ctx.upload_dirty(file).await.expect("cleanup");
        assert_eq!(
            plain_put_count(&mock),
            1,
            "cleanup must not re-upload after a successful flush"
        );

        let recorded = mock.recorded.lock().unwrap();
        let put = recorded
            .iter()
            .find(|r| {
                r.method == "PUT" && {
                    let q = r.target.to_lowercase();
                    !q.contains("partnumber") && !q.contains("uploadid")
                }
            })
            .expect("one plain PUT");
        assert_eq!(put.body, data, "uploaded body matches the written data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_then_cleanup_after_streaming_does_not_put_empty_object() {
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/big");
        let mut fi = FileInfo::default();
        // Above WRITE_SPOOL_THRESHOLD the handle switches to streaming multipart.
        let big = vec![0xABu8; WRITE_SPOOL_THRESHOLD + 1024 * 1024];
        let written = ctx
            .write_async(file, &big, 0, false, false, &mut fi)
            .await
            .expect("write");
        assert_eq!(written as usize, big.len());
        eprintln!("[testdbg] write_async done");

        ctx.upload_dirty(file).await.expect("flush");
        assert!(
            !file.dirty.load(Ordering::Acquire),
            "dirty must be cleared after the multipart finishes"
        );
        let after_flush = mock.recorded.lock().unwrap().len();
        ctx.upload_dirty(file).await.expect("cleanup");
        assert_eq!(
            mock.recorded.lock().unwrap().len(),
            after_flush,
            "cleanup after a finished stream must not upload anything \
             (a repeat would PUT an empty object over the completed multipart)"
        );
        // The object was delivered via multipart completion, never as an
        // empty whole-object PUT. (`uploadId` arrives camelCase from the SDK.)
        // NOTE: scope the guard — `plain_put_count` locks the same std Mutex,
        // which is not reentrant; holding the guard across that call
        // deadlocks the test (this exact bug hung the first Windows CI run).
        {
            let recorded = mock.recorded.lock().unwrap();
            assert!(
                recorded
                    .iter()
                    .any(|r| r.method == "POST" && r.target.to_lowercase().contains("uploadid")),
                "multipart upload must be completed"
            );
        }
        assert_eq!(
            plain_put_count(&mock),
            0,
            "streamed file must never be PUT as a whole object"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overwrite_lazy_load_rejects_oversized_object_before_download() {
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        // 10 MiB existing object under a 1 MiB dirty budget: the lazy-load
        // download must be rejected up front, not after allocating 10 MiB.
        mock.set_object("f", vec![0u8; 10 * 1024 * 1024]);
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, Some(1 << 20)));
        let file = test_file_with("/f", false); // overwrite handle, not yet loaded
        let mut fi = FileInfo::default();
        assert!(
            ctx.write_async(file, b"x", 0, false, false, &mut fi)
                .await
                .is_err(),
            "oversized lazy-load must fail instead of downloading the object"
        );
        assert_eq!(
            mock.get_count.load(Ordering::SeqCst),
            0,
            "oversized lazy-load must not download the object"
        );
    }

    #[tokio::test]
    async fn rename_retargets_handle_path() {
        // #46: after a rename the open handle must flush to the new key —
        // otherwise a dirty handle resurrects the deleted old object.
        let (_mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/a");
        ctx.rename(&file, w("\\a"), w("\\b"), true).expect("rename");
        assert_eq!(
            *file.path.lock().unwrap(),
            "/b",
            "handle must be retargeted to the new path"
        );
    }

    /// Data larger than WRITE_SPOOL_THRESHOLD with a distinguishable byte
    /// pattern for read-back verification.
    fn big_data() -> Vec<u8> {
        (0..(WRITE_SPOOL_THRESHOLD + 1024 * 1024))
            .map(|i| (i % 251) as u8)
            .collect()
    }

    #[tokio::test]
    async fn streaming_write_out_of_order_rejected() {
        // #47: once streaming, a write anchored anywhere but the current end
        // must fail instead of silently corrupting the object.
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/big");
        let mut fi = FileInfo::default();
        let big = big_data();
        let n = ctx
            .write_async(file, &big, 0, false, false, &mut fi)
            .await
            .expect("first write");
        assert_eq!(n as usize, big.len());

        ctx.write_async(file, b"xx", 0, false, false, &mut fi)
            .await
            .expect_err("out-of-order write must fail");
        assert_eq!(
            mock.recorded.lock().unwrap().len(),
            2,
            "only initiate + first part upload; the rejected write must not upload"
        );
    }

    #[tokio::test]
    async fn truncate_zero_aborts_inflight_stream() {
        // #47: truncate-to-zero must abort the in-flight stream or bytes
        // written after it would append to the old upload.
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/big");
        let mut fi = FileInfo::default();
        ctx.write_async(file, &big_data(), 0, false, false, &mut fi)
            .await
            .expect("write");

        ctx.set_file_size(file, 0, false, &mut fi)
            .expect("truncate to zero");
        ctx.upload_dirty(file).await.expect("flush");

        let recorded = mock.recorded.lock().unwrap();
        assert!(
            recorded
                .iter()
                .any(|r| r.method == "DELETE" && r.target.to_lowercase().contains("uploadid")),
            "in-flight stream must be aborted on truncate-to-zero"
        );
        assert!(
            !recorded
                .iter()
                .any(|r| r.method == "POST" && r.target.to_lowercase().contains("uploadid")),
            "no multipart completion after truncate"
        );
    }

    #[tokio::test]
    async fn streaming_handle_reads_back_spooled_bytes() {
        // #47: while a multipart upload is in flight the parts are invisible
        // to reads — the handle must serve the bytes written so far from its
        // read-back spool instead of reporting EOF.
        let (mock, port) = MockS3::start(vec![], Duration::ZERO).await;
        let (_fs, ctx) = test_mount(test_fs_with_budget(port, 32, None));
        let file = test_file("/big");
        let mut fi = FileInfo::default();
        let big = big_data();
        ctx.write_async(file, &big, 0, false, false, &mut fi)
            .await
            .expect("write");

        let mut head = vec![0u8; 4096];
        let n = ctx.read_async(file, &mut head, 0).await.expect("read");
        assert_eq!(n as usize, 4096);
        assert_eq!(&head[..], &big[..4096], "head read-back must match");

        let mut mid = vec![0u8; 4096];
        let n2 = ctx.read_async(file, &mut mid, 1024).await.expect("read");
        assert_eq!(
            &mid[..],
            &big[1024..1024 + 4096],
            "mid read-back must match"
        );
    }
}
