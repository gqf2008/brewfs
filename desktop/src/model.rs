//! Data model for the OSSFS tray app: saved mount profiles and live
//! `ossmount` instance records read from the runtime registry.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::secrets::{Credentials, SecretStore};

// ---------------------------------------------------------------------------
// Saved mount profiles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Profile {
    pub name: String,
    /// "oss" = metadata-less OSS direct mount (multi-machine cloud drive,
    /// weak consistency). The only supported mode.
    pub mode: String,
    pub drive: String,
    pub s3_bucket: String,
    pub s3_endpoint: String,
    pub s3_region: String,
    pub s3_force_path_style: bool,
    pub s3_disable_payload_checksum: bool,
    /// Optional object-key namespace for OSS direct mode (e.g. "myns/").
    pub prefix: String,
    /// In-memory credential fields. On disk they are only written when the
    /// OS secure store is unavailable (warned plaintext fallback); after a
    /// successful [`load_profiles`] they are repopulated from the store.
    pub access_key: String,
    pub secret_key: String,
    /// Key of this profile's credentials inside the OS secure store
    /// (Windows Credential Manager / macOS Keychain, service `ossfs-tray`).
    /// Absent = legacy plaintext profile not yet migrated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
}

/// Default mount point for a fresh config. Windows uses drive letters and the
/// UI picks the first free one; macOS/Linux use a directory path.
#[cfg(windows)]
pub fn default_drive() -> String {
    String::new()
}

/// Default mount point for a fresh config (see `default_drive`).
#[cfg(not(windows))]
pub fn default_drive() -> String {
    "/Volumes/ossfs".to_string()
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            name: "新建配置".to_string(),
            mode: "oss".to_string(),
            drive: default_drive(),
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_force_path_style: false,
            s3_disable_payload_checksum: true,
            prefix: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            secret_ref: None,
        }
    }
}

impl Profile {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("配置名称不能为空".into());
        }
        let drive = self.drive.trim();
        if drive.is_empty() {
            return Err("请填写挂载点（例如 Z: 或 /Volumes/ossfs）".into());
        }
        // Windows uses drive letters (`Z:`); macOS/Linux use a directory path
        // (e.g. `/Volumes/ossfs`).
        if !drive.starts_with('/') {
            let bytes = drive.as_bytes();
            let ok = bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
            if !ok {
                return Err(format!(
                    "挂载点格式不正确：{drive}（Windows 用盘符如 Z:，macOS/Linux 用目录如 /Volumes/ossfs）"
                ));
            }
        }
        // OSS direct mount only.
        if self.s3_bucket.trim().is_empty() {
            return Err("请填写 Bucket".into());
        }
        if self.s3_endpoint.trim().is_empty() {
            return Err("请填写 Endpoint".into());
        }
        if self.access_key.trim().is_empty() || self.secret_key.trim().is_empty() {
            return Err("请填写 AccessKey / SecretKey".into());
        }
        Ok(())
    }

    /// Serialize this profile into the same JSON shape `ossmount --config`
    /// accepts. Only the shared mount options are emitted; tray-only fields
    /// (`name` / `drive` / `mode` / payload-checksum toggle) are omitted.
    pub fn to_ossmount_config(&self) -> String {
        serde_json::to_string_pretty(&serde_json::json!({
            "mount_point": self.drive,
            "bucket": self.s3_bucket,
            "endpoint": self.s3_endpoint,
            "region": self.s3_region,
            "force-path-style": self.s3_force_path_style,
            "prefix": self.prefix,
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
        }))
        .unwrap_or_default()
    }

    /// Build a tray profile from an `ossmount --config` JSON document.
    /// Tray-only fields are filled with sensible defaults.
    pub fn from_ossmount_config(json: &str) -> Result<Profile, String> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|e| format!("invalid JSON: {e}"))?;
        let Some(obj) = value.as_object() else {
            return Err("config must be a JSON object".into());
        };
        let get_str = |k: &str| {
            obj.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        let drive = get_str("mount_point");
        let drive = if drive.is_empty() {
            default_drive()
        } else {
            drive
        };
        Ok(Profile {
            name: "导入配置".into(),
            mode: "oss".into(),
            drive,
            s3_bucket: get_str("bucket"),
            s3_endpoint: get_str("endpoint"),
            s3_region: get_str("region"),
            s3_force_path_style: obj
                .get("force-path-style")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            s3_disable_payload_checksum: true,
            prefix: get_str("prefix"),
            access_key: get_str("access_key_id"),
            secret_key: get_str("secret_access_key"),
            secret_ref: None,
        })
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProfilesFile {
    #[serde(default)]
    pub profiles: Vec<Profile>,
}

pub fn profiles_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ossfs-tray")
        .join("profiles.json")
}

/// Result of loading the profiles file.
#[derive(Debug)]
pub struct LoadProfilesResult {
    pub file: ProfilesFile,
    /// User-visible, non-fatal problems (corrupt file recovered, secure
    /// store unavailable, credentials missing from the store, ...).
    pub warnings: Vec<String>,
}

/// Result of saving the profiles file.
#[derive(Debug, Default)]
pub struct SaveProfilesResult {
    /// User-visible, non-fatal problems (warned plaintext fallback).
    pub warnings: Vec<String>,
}

pub fn load_profiles() -> LoadProfilesResult {
    load_profiles_from(crate::secrets::system_store(), &profiles_path())
}

pub fn save_profiles(file: &ProfilesFile) -> io::Result<SaveProfilesResult> {
    save_profiles_to(crate::secrets::system_store(), &profiles_path(), file)
}

/// Load profiles from `path`, resolving credentials from the secure store
/// and migrating legacy plaintext credentials into it.
pub fn load_profiles_from(store: Option<&dyn SecretStore>, path: &Path) -> LoadProfilesResult {
    let mut warnings = Vec::new();
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return LoadProfilesResult {
                file: ProfilesFile::default(),
                warnings,
            };
        }
        Err(e) => {
            warnings.push(format!("无法读取配置文件 {}: {e}", path.display()));
            return LoadProfilesResult {
                file: ProfilesFile::default(),
                warnings,
            };
        }
    };
    let mut file: ProfilesFile = match serde_json::from_slice(&raw) {
        Ok(file) => file,
        Err(e) => {
            // Never silently reset: report the corruption and keep the
            // original bytes as a timestamped backup next to the file.
            match backup_corrupt_file(path) {
                Some(backup) => warnings.push(format!(
                    "配置文件已损坏（{e}），已重置为空配置；原文件备份在 {}，请手动找回需要的配置",
                    backup.display()
                )),
                None => warnings.push(format!(
                    "配置文件已损坏（{e}），已重置为空配置；原文件保留在 {}（无法自动备份，请手动处理）",
                    path.display()
                )),
            }
            return LoadProfilesResult {
                file: ProfilesFile::default(),
                warnings,
            };
        }
    };

    let mut migrated = false;
    for p in &mut file.profiles {
        if p.secret_ref.as_deref() == Some("") {
            p.secret_ref = None;
        }
        match p.secret_ref.as_deref() {
            Some(secret_ref) => match store {
                None => warnings.push(format!(
                    "系统安全存储不可用，配置「{}」的 AccessKey/SecretKey 以明文保存在 {}",
                    p.name,
                    path.display()
                )),
                Some(store) => match store.get(secret_ref) {
                    Ok(Some(creds)) => {
                        p.access_key = creds.access_key;
                        p.secret_key = creds.secret_key;
                    }
                    Ok(None) => warnings.push(format!(
                        "配置「{}」的凭据不在系统安全存储中（可能已被清除），请重新填写 AccessKey/SecretKey",
                        p.name
                    )),
                    Err(e) => warnings.push(format!(
                        "读取配置「{}」的凭据失败（{e}），如文件内无明文凭据则需重新填写",
                        p.name
                    )),
                },
            },
            None if !p.secret_key.is_empty() || !p.access_key.is_empty() => {
                // Legacy plaintext profile: migrate into the secure store
                // now; the sanitized file is persisted right after.
                match store {
                    Some(store) => {
                        let secret_ref = format!("profile-{}", new_secret_ref_suffix());
                        let creds = Credentials {
                            access_key: p.access_key.clone(),
                            secret_key: p.secret_key.clone(),
                        };
                        match store.put(&secret_ref, &creds) {
                            Ok(()) => {
                                p.secret_ref = Some(secret_ref);
                                migrated = true;
                            }
                            Err(e) => warnings.push(format!(
                                "迁移配置「{}」的密钥到系统安全存储失败（{e}），将继续以明文保存",
                                p.name
                            )),
                        }
                    }
                    None => warnings.push(format!(
                        "系统安全存储不可用，配置「{}」的 AccessKey/SecretKey 将以明文保存在 {}",
                        p.name,
                        path.display()
                    )),
                }
            }
            None => {}
        }
    }

    // A migration happened: persist the sanitized file so the plaintext
    // credentials actually leave the disk (store already holds them).
    if migrated {
        match save_profiles_to(store, path, &file) {
            Ok(res) => warnings.extend(res.warnings),
            Err(e) => warnings.push(format!(
                "移除配置文件中的明文密钥失败（{e}），明文暂仍保留在 {}",
                path.display()
            )),
        }
    }

    LoadProfilesResult { file, warnings }
}

/// Persist `file` to `path` atomically (temp file + rename), moving the
/// credentials of every profile into the secure store first so the JSON on
/// disk contains no plaintext secrets (unless the store is unavailable —
/// in that case the plaintext fallback is kept and a warning returned).
pub fn save_profiles_to(
    store: Option<&dyn SecretStore>,
    path: &Path,
    file: &ProfilesFile,
) -> io::Result<SaveProfilesResult> {
    let mut warnings = Vec::new();

    let old_refs: HashSet<String> = fs::read(path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<ProfilesFile>(&raw).ok())
        .map(|old| {
            old.profiles
                .iter()
                .filter_map(|p| p.secret_ref.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut sanitized = ProfilesFile {
        profiles: Vec::with_capacity(file.profiles.len()),
    };
    let mut new_refs: HashSet<String> = HashSet::new();
    for p in &file.profiles {
        let mut q = p.clone();
        if q.secret_key.is_empty() && q.access_key.is_empty() {
            sanitized.profiles.push(q);
            continue;
        }
        if q.secret_ref.as_deref().map_or(true, |r| r.is_empty()) {
            q.secret_ref = Some(format!("profile-{}", new_secret_ref_suffix()));
        }
        let secret_ref = q.secret_ref.clone().unwrap_or_default();
        let mut protected = false;
        match store {
            Some(store) => {
                let creds = Credentials {
                    access_key: q.access_key.clone(),
                    secret_key: q.secret_key.clone(),
                };
                match store.put(&secret_ref, &creds) {
                    Ok(()) => protected = true,
                    Err(e) => warnings.push(format!(
                        "写入配置「{}」的密钥到系统安全存储失败（{e}），AccessKey/SecretKey 将以明文保存在 {}",
                        q.name,
                        path.display()
                    )),
                }
            }
            None => warnings.push(format!(
                "系统安全存储不可用，配置「{}」的 AccessKey/SecretKey 将以明文保存在 {}",
                q.name,
                path.display()
            )),
        }
        if protected {
            q.access_key.clear();
            q.secret_key.clear();
        }
        new_refs.insert(secret_ref);
        sanitized.profiles.push(q);
    }

    write_profiles_atomic(path, &sanitized)?;

    // Best-effort cleanup of secrets whose profiles no longer exist.
    if let Some(store) = store {
        for orphan in old_refs.difference(&new_refs) {
            let _ = store.delete(orphan);
        }
    }

    Ok(SaveProfilesResult { warnings })
}

/// Atomically replace `path` with the JSON of `file`: write a sibling temp
/// file, fsync it, then rename over the target. A crash mid-write can never
/// truncate an existing profiles.json.
fn write_profiles_atomic(path: &Path, file: &ProfilesFile) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(file).map_err(io::Error::other)?;
    let tmp = sibling_temp_path(path);
    let write = || -> io::Result<()> {
        use std::io::Write;
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
        Ok(())
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    // The file may contain credential fallbacks; restrict it to the current
    // user. (%APPDATA% is already per-user on Windows.)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    }
    match fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn sibling_temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map_or_else(
        || std::ffi::OsString::from("profiles.json"),
        std::ffi::OsStr::to_owned,
    );
    name.push(format!(".tmp-{}", std::process::id()));
    path.with_file_name(name)
}

/// Rename a corrupt profiles file aside as `<name>.corrupt-<unix-ts>`.
/// Returns `None` when the original could not be moved (it then stays in
/// place for manual inspection).
fn backup_corrupt_file(path: &Path) -> Option<PathBuf> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut name = path.file_name()?.to_owned();
    name.push(format!(".corrupt-{ts}"));
    let backup = path.with_file_name(name);
    fs::rename(path, &backup).ok()?;
    Some(backup)
}

/// Generate an opaque per-process-unique secret-store key suffix
/// (timestamp + pid + monotonic counter).
fn new_secret_ref_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{nanos:x}-{n:x}-{:x}", std::process::id())
}

// ---------------------------------------------------------------------------
// Live mounts (runtime registry mirror)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceRecord {
    pub pid: u32,
    pub mount_point: String,
    #[allow(dead_code)]
    pub socket_path: String,
    #[allow(dead_code)]
    pub started_at: String,
}

/// Directory where `ossmount` records its instances.
pub fn oss_records_dir() -> PathBuf {
    std::env::temp_dir().join("ossfs-oss")
}

fn read_records_raw(dir: &Path) -> Vec<InstanceRecord> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = fs::read(&path) else { continue };
        let Ok(record) = serde_json::from_slice::<InstanceRecord>(&raw) else {
            continue;
        };
        out.push(record);
    }
    out
}

/// A live mount merged with the profile that owns its drive letter, if any.
#[derive(Debug, Clone)]
pub struct MountStatus {
    pub drive: String,
    pub backend: String,
    pub detail: String,
    pub pid: u32,
    pub alive: bool,
    /// True when this is a metadata-less `ossmount` instance; unmount = terminate
    /// the process.
    pub is_oss: bool,
}

/// Read the runtime registry and produce mount status rows.
///
/// Stale records (dead pids) are still returned with `alive == false` so the
/// UI can show them, but they are not counted as live mounts.
pub fn read_mounts(profiles: &[Profile]) -> Vec<MountStatus> {
    let mut out: Vec<MountStatus> = Vec::new();
    for record in read_records_raw(&oss_records_dir()) {
        let drive = normalize_mount_point(&record.mount_point);
        let profile = profiles
            .iter()
            .find(|p| normalize_mount_point(&p.drive) == drive);
        let detail = profile
            .map(|p| format!("{} / {}", p.s3_bucket, p.s3_endpoint.trim_end_matches('/')))
            .unwrap_or_else(|| "OSS 直挂（无元数据）".to_string());
        let alive = crate::winutil::pid_alive(record.pid)
            && crate::winutil::pid_is_mount_process(record.pid);
        out.push(MountStatus {
            drive,
            backend: "oss".to_string(),
            detail,
            pid: record.pid,
            alive,
            is_oss: true,
        });
    }
    out.sort_by_key(|m| std::cmp::Reverse(m.alive));
    // A drive may have multiple runtime records (e.g. stale records from a
    // duplicate-mount race or crashed processes); show at most one row per
    // drive, preferring the live one (alive sorts first).
    let mut seen = std::collections::HashSet::new();
    out.retain(|m| seen.insert(m.drive.clone()));
    out
}

/// Normalize a mount point string: `Z:` stays `Z:`, `Z:\` becomes `Z:`,
/// `C:\mnt\x` stays as-is.
pub fn normalize_mount_point(s: &str) -> String {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return format!("{}:", bytes[0] as char);
    }
    s.to_string()
}

/// Best-effort cleanup of stale runtime records whose owning process is gone.
pub fn prune_stale_records() {
    prune_records_in(&oss_records_dir());
}

fn prune_records_in(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = fs::read(&path) else { continue };
        let Ok(record) = serde_json::from_slice::<InstanceRecord>(&raw) else {
            continue;
        };
        if !crate::winutil::pid_alive(record.pid)
            || !crate::winutil::pid_is_mount_process(record.pid)
        {
            let _ = fs::remove_file(&path);
        }
    }
}

// ---------------------------------------------------------------------------
// Spawning ossmount
// ---------------------------------------------------------------------------

/// Locate the `ossmount` binary: same directory as the tray executable, then
/// `OSSMOUNT_EXE`, then PATH.
pub fn find_ossmount() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("OSSMOUNT_EXE") {
        let p = PathBuf::from(explicit);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for sibling in ["ossmount.exe", "ossmount"] {
            let p = exe.parent()?.join(sibling);
            if p.is_file() {
                return Some(p);
            }
        }
    }
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            for candidate in ["ossmount.exe", "ossmount"] {
                let p = dir.join(candidate);
                if p.is_file() {
                    return Some(p);
                }
            }
        }
    }
    None
}

/// Spawn `ossmount mount --bucket ... <drive>` (metadata-less OSS direct
/// mount) and return (child pid, log path).
pub fn spawn_oss_mount(ossmount: &Path, profile: &Profile) -> std::io::Result<(u32, PathBuf)> {
    let app_dir = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ossfs-tray");
    let log_dir = app_dir.join("logs");
    fs::create_dir_all(&log_dir)?;

    let safe_name: String = profile
        .name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let log_path = log_dir.join(format!("{safe_name}-oss.log"));
    let log_file = fs::File::create(&log_path)?;

    let mut cmd = Command::new(ossmount);
    cmd.arg("mount")
        .arg("--bucket")
        .arg(profile.s3_bucket.trim())
        .arg("--endpoint")
        .arg(profile.s3_endpoint.trim());
    if !profile.s3_region.trim().is_empty() {
        cmd.arg("--region").arg(profile.s3_region.trim());
    }
    if !profile.prefix.trim().is_empty() {
        cmd.arg("--prefix").arg(profile.prefix.trim());
    }
    if profile.s3_force_path_style {
        cmd.arg("--force-path-style");
    }
    cmd.arg(profile.drive.trim());
    if !profile.access_key.is_empty() {
        cmd.env("AWS_ACCESS_KEY_ID", &profile.access_key)
            .env("AWS_SECRET_ACCESS_KEY", &profile.secret_key);
    }
    cmd.stdout(log_file.try_clone()?).stderr(log_file);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000 /* CREATE_NO_WINDOW */);
    }
    let child = cmd.spawn()?;
    Ok((child.id(), log_path))
}

/// Read the tail of a mount log file for error reporting.
pub fn read_log_tail(path: &Path, max_bytes: usize) -> String {
    let Ok(meta) = fs::metadata(path) else {
        return String::new();
    };
    let len = meta.len() as usize;
    let skip = len.saturating_sub(max_bytes);
    let Ok(file) = fs::File::open(path) else {
        return String::new();
    };
    use std::io::{Read, Seek, SeekFrom};
    let mut reader = std::io::BufReader::new(file);
    let _ = reader.seek(SeekFrom::Start(skip as u64));
    let mut buf = String::new();
    let _ = reader.read_to_string(&mut buf);
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::memory::MemoryStore;

    fn oss_profile() -> Profile {
        Profile {
            name: "OSS".into(),
            mode: "oss".into(),
            drive: "Z:".into(),
            s3_bucket: "my-bucket".into(),
            s3_endpoint: "https://s3.example.com".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            ..Profile::default()
        }
    }

    fn temp_profiles_path(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ossfs-tray-model-test-{tag}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("profiles.json")
    }

    fn credentialed_profile() -> Profile {
        Profile {
            name: "OSS".into(),
            mode: "oss".into(),
            drive: "Z:".into(),
            s3_bucket: "my-bucket".into(),
            s3_endpoint: "https://s3.example.com".into(),
            access_key: "AKIA-test-123".into(),
            secret_key: "topsecret-456".into(),
            ..Profile::default()
        }
    }

    // ------------------------------------------------------------------
    // #56 secure credential storage + atomic profiles.json
    // ------------------------------------------------------------------

    #[test]
    fn save_moves_secrets_out_of_profiles_json() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("save-sanitize");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile()],
        };
        let res = save_profiles_to(Some(&store), &path, &file).unwrap();
        assert!(res.warnings.is_empty(), "{:?}", res.warnings);

        let raw = String::from_utf8(fs::read(&path).unwrap()).unwrap();
        assert!(
            !raw.contains("topsecret-456"),
            "plaintext SecretKey must not be written to profiles.json: {raw}"
        );
        assert!(
            !raw.contains("AKIA-test-123"),
            "plaintext AccessKey must not be written to profiles.json: {raw}"
        );
        assert!(raw.contains("secret_ref"), "store key must be kept: {raw}");
        // In memory the profile keeps its credentials for the running app.
        assert_eq!(file.profiles[0].secret_key, "topsecret-456");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_resolves_credentials_from_store() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("load-from-store");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile()],
        };
        save_profiles_to(Some(&store), &path, &file).unwrap();

        let out = load_profiles_from(Some(&store), &path);
        assert!(out.warnings.is_empty(), "{:?}", out.warnings);
        assert_eq!(out.file.profiles.len(), 1);
        let p = &out.file.profiles[0];
        assert_eq!(p.access_key, "AKIA-test-123");
        assert_eq!(p.secret_key, "topsecret-456");
        assert!(p.secret_ref.is_some());
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn legacy_plaintext_migrates_into_store_and_sanitizes_file() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("legacy-migrate");
        let legacy = serde_json::json!({
            "profiles": [{
                "name": "legacy",
                "mode": "oss",
                "drive": "Z:",
                "s3_bucket": "b",
                "s3_endpoint": "https://e",
                "s3_region": "",
                "s3_force_path_style": false,
                "s3_disable_payload_checksum": true,
                "prefix": "",
                "access_key": "AKIA-legacy",
                "secret_key": "legacy-secret"
            }]
        });
        fs::write(&path, legacy.to_string()).unwrap();

        let out = load_profiles_from(Some(&store), &path);
        assert!(out.warnings.is_empty(), "{:?}", out.warnings);
        let p = &out.file.profiles[0];
        // Repopulated in memory for the running app...
        assert_eq!(p.access_key, "AKIA-legacy");
        assert_eq!(p.secret_key, "legacy-secret");
        // ...stored in the secure store...
        let secret_ref = p.secret_ref.clone().expect("migrated ref");
        assert!(store.contains(&secret_ref));
        // ...and removed from the on-disk JSON right away.
        let raw = String::from_utf8(fs::read(&path).unwrap()).unwrap();
        assert!(!raw.contains("legacy-secret"), "{raw}");
        assert!(!raw.contains("AKIA-legacy"), "{raw}");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn corrupt_profiles_file_is_backed_up_and_reported() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("corrupt-backup");
        let garbage = "{\"profiles\": [ truncated";
        fs::write(&path, garbage).unwrap();

        let out = load_profiles_from(Some(&store), &path);
        assert!(out.file.profiles.is_empty());
        assert_eq!(out.warnings.len(), 1, "{:?}", out.warnings);
        assert!(out.warnings[0].contains("损坏"), "{:?}", out.warnings);

        let dir = path.parent().unwrap();
        let backups: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("profiles.json.corrupt-")
            })
            .collect();
        assert_eq!(backups.len(), 1, "exactly one timestamped backup");
        assert_eq!(
            fs::read_to_string(backups[0].path()).unwrap(),
            garbage,
            "backup must keep the original bytes"
        );
        assert!(!path.exists(), "corrupt file must be moved aside");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn unavailable_store_keeps_plaintext_with_warning() {
        let path = temp_profiles_path("no-store-fallback");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile()],
        };
        let res = save_profiles_to(None, &path, &file).unwrap();
        assert!(
            !res.warnings.is_empty(),
            "plaintext fallback must be warned"
        );
        let raw = String::from_utf8(fs::read(&path).unwrap()).unwrap();
        assert!(raw.contains("topsecret-456"), "{raw}");

        let out = load_profiles_from(None, &path);
        assert_eq!(out.file.profiles[0].secret_key, "topsecret-456");
        assert!(!out.warnings.is_empty(), "loading must warn too");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn save_is_atomic_and_leaves_no_temp_files() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("atomic");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile()],
        };
        save_profiles_to(Some(&store), &path, &file).unwrap();
        // Save again over the existing file: the rename-replace path.
        save_profiles_to(Some(&store), &path, &file).unwrap();

        let dir = path.parent().unwrap();
        let names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["profiles.json".to_string()],
            "no temp files may survive the atomic write: {names:?}"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn removed_profile_secret_is_deleted_from_store() {
        let store = MemoryStore::new();
        let path = temp_profiles_path("orphan-cleanup");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile(), oss_profile()],
        };
        save_profiles_to(Some(&store), &path, &file).unwrap();
        assert_eq!(store.len(), 2);

        let mut reloaded = load_profiles_from(Some(&store), &path).file;
        reloaded.profiles.pop();
        save_profiles_to(Some(&store), &path, &reloaded).unwrap();
        assert_eq!(store.len(), 1, "orphaned secret must be deleted");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn saved_file_is_user_read_write_only() {
        use std::os::unix::fs::PermissionsExt;
        let store = MemoryStore::new();
        let path = temp_profiles_path("perms");
        let file = ProfilesFile {
            profiles: vec![credentialed_profile()],
        };
        save_profiles_to(Some(&store), &path, &file).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "profiles.json must be 0600");
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn secret_ref_suffixes_are_unique() {
        let a = new_secret_ref_suffix();
        let b = new_secret_ref_suffix();
        assert_ne!(a, b);
    }

    #[cfg(not(windows))]
    #[test]
    fn default_drive_is_volume_path() {
        // Regression: on macOS/Linux a fresh "添加配置" must come with a
        // mount point, otherwise saving silently fails validation while the
        // dialog stays open.
        let d = default_drive();
        assert!(!d.is_empty());
        assert!(d.starts_with('/'));
    }

    #[test]
    fn validate_oss_ok() {
        assert!(oss_profile().validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_drive() {
        let mut p = oss_profile();
        p.drive = "Z".into();
        assert!(p.validate().is_err());
        p.drive = "ZZ:".into();
        assert!(p.validate().is_err());
        p.drive = "1:".into();
        assert!(p.validate().is_err());
    }

    #[test]
    fn validate_requires_oss_fields() {
        let mut p = oss_profile();
        p.s3_bucket.clear();
        assert!(p.validate().is_err()); // no bucket
        p.s3_bucket = "b".into();
        p.s3_endpoint.clear();
        assert!(p.validate().is_err()); // no endpoint
        p.s3_endpoint = "https://example.com".into();
        p.access_key.clear();
        assert!(p.validate().is_err()); // no keys
        p.access_key = "ak".into();
        p.secret_key = "sk".into();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn normalize_drive_letters() {
        assert_eq!(normalize_mount_point("Z:"), "Z:");
        assert_eq!(normalize_mount_point("  z:  "), "z:");
        assert_eq!(normalize_mount_point("C:\\"), "C:");
        assert_eq!(normalize_mount_point("C:\\mnt\\x"), "C:");
        assert_eq!(normalize_mount_point("/mnt/x"), "/mnt/x");
    }

    #[test]
    fn read_records_skips_non_json_and_bad_files() {
        let dir = std::env::temp_dir().join(format!("ossfs-tray-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("101.json"),
            "{\"pid\":101,\"mount_point\":\"Z:\",\"socket_path\":\"x\",\"started_at\":\"t\"}",
        )
        .unwrap();
        std::fs::write(dir.join("not-json.json"), "garbage").unwrap();
        std::fs::write(dir.join("readme.txt"), "hello").unwrap();
        let records = read_records_raw(&dir);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].pid, 101);
        assert_eq!(records[0].mount_point, "Z:");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_profile_is_oss_only() {
        // The GUI default must be OSS direct mount (recommended); metadata
        // mode is an explicit choice.
        let p = Profile::default();
        assert_eq!(p.mode, "oss");
        assert!(
            p.validate().is_err(),
            "default OSS profile still needs fields"
        );
    }

    #[test]
    fn validate_oss_mode_requires_s3_fields() {
        let mut p = Profile {
            mode: "oss".into(),
            drive: "F:".into(),
            s3_bucket: String::new(),
            s3_endpoint: String::new(),
            ..Profile::default()
        };
        assert!(p.validate().is_err()); // no bucket
        p.s3_bucket = "b".into();
        assert!(p.validate().is_err()); // no endpoint
        p.s3_endpoint = "https://s3.example.com".into();
        assert!(p.validate().is_err()); // no keys
        p.access_key = "ak".into();
        p.secret_key = "sk".into();
        assert!(p.validate().is_ok());
        // prefix is optional in OSS mode
        p.prefix = "myns/".into();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn profile_roundtrips_through_ossmount_config() {
        let p = oss_profile();
        let json = p.to_ossmount_config();
        let back = Profile::from_ossmount_config(&json).expect("import");
        assert_eq!(back.drive, p.drive);
        assert_eq!(back.s3_bucket, p.s3_bucket);
        assert_eq!(back.s3_endpoint, p.s3_endpoint);
        assert_eq!(back.s3_region, p.s3_region);
        assert_eq!(back.s3_force_path_style, p.s3_force_path_style);
        assert_eq!(back.prefix, p.prefix);
        assert_eq!(back.access_key, p.access_key);
        assert_eq!(back.secret_key, p.secret_key);
        assert_eq!(back.mode, "oss");
        assert_eq!(back.name, "导入配置");
    }

    #[test]
    fn profile_import_rejects_non_object_config() {
        assert!(Profile::from_ossmount_config("[1,2]").is_err());
    }

    #[test]
    fn profile_import_falls_back_to_default_drive_when_mount_point_missing() {
        let json = r#"{"bucket":"b","endpoint":"https://e","access_key_id":"ak","secret_access_key":"sk"}"#;
        let p = Profile::from_ossmount_config(json).expect("import");
        assert_eq!(p.drive, default_drive());
        assert_eq!(p.s3_bucket, "b");
    }

    #[test]
    fn profile_json_roundtrip_preserves_mode_and_prefix() {
        let p = Profile {
            mode: "oss".into(),
            prefix: "myns/".into(),
            drive: "F:".into(),
            s3_bucket: "b".into(),
            s3_endpoint: "https://s3.example.com".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            ..Profile::default()
        };
        let data = serde_json::to_vec(&p).unwrap();
        let back: Profile = serde_json::from_slice(&data).unwrap();
        assert_eq!(back.mode, "oss");
        assert_eq!(back.prefix, "myns/");
    }

    #[test]
    fn read_mounts_scans_oss_records_dir() {
        let oss_dir = oss_records_dir();
        let _ = fs::create_dir_all(&oss_dir);
        let fake_pid = 4_200_000 + std::process::id() % 100_000;
        let path = oss_dir.join(format!("{fake_pid}.json"));
        let record = serde_json::json!({
            "pid": fake_pid,
            "mount_point": "H:",
            "socket_path": "",
            "started_at": "2026-01-01T00:00:00Z",
        });
        fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        let mounts = read_mounts(&[]);
        let _ = fs::remove_file(&path);
        let m = mounts.iter().find(|m| m.pid == fake_pid);
        assert!(m.is_some(), "ossmount record should be picked up");
        assert!(m.unwrap().is_oss);
        assert_eq!(m.unwrap().backend, "oss");
    }

    #[test]
    fn profile_json_roundtrip() {
        let file = ProfilesFile {
            profiles: vec![oss_profile()],
        };
        let data = serde_json::to_vec(&file).unwrap();
        let back: ProfilesFile = serde_json::from_slice(&data).unwrap();
        assert_eq!(back.profiles.len(), 1);
        assert_eq!(back.profiles[0].name, "OSS");
        assert_eq!(back.profiles[0].drive, "Z:");
        assert_eq!(back.profiles[0].mode, "oss");
    }
}
