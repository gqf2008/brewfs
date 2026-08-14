//! Data model for the OSSFS tray app: saved mount profiles and live
//! `ossmount` instance records read from the runtime registry.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

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
    pub access_key: String,
    pub secret_key: String,
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
    /// An empty region is omitted so `ossmount` keeps its documented
    /// `us-east-1` default instead of expanding `--region ""` (#61).
    pub fn to_ossmount_config(&self) -> String {
        let mut obj = serde_json::json!({
            "mount_point": self.drive,
            "bucket": self.s3_bucket,
            "endpoint": self.s3_endpoint,
            "force-path-style": self.s3_force_path_style,
            "prefix": self.prefix,
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
        });
        if !self.s3_region.trim().is_empty() {
            obj["region"] = serde_json::json!(self.s3_region);
        }
        serde_json::to_string_pretty(&obj).unwrap_or_default()
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

pub fn load_profiles() -> ProfilesFile {
    let path = profiles_path();
    match fs::read(&path) {
        Ok(data) => serde_json::from_slice(&data).unwrap_or_default(),
        Err(_) => ProfilesFile::default(),
    }
}

pub fn save_profiles(file: &ProfilesFile) -> std::io::Result<()> {
    let path = profiles_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
    fs::write(path, data)
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

/// When a mount log grows past this size it is rotated down to
/// [`MOUNT_LOG_KEEP_BYTES`] before the next append, so a crash-looping mount
/// cannot grow the file without bound (#61).
const MOUNT_LOG_ROTATE_BYTES: u64 = 1024 * 1024;
/// Portion of the log kept when rotating.
const MOUNT_LOG_KEEP_BYTES: u64 = 256 * 1024;

/// Open the per-profile mount log for appending, rotating it first when it
/// exceeds [`MOUNT_LOG_ROTATE_BYTES`]. Appending (instead of truncating)
/// preserves the previous crash's output so a restart loop stays
/// diagnosable (#61).
fn open_mount_log(path: &Path) -> std::io::Result<fs::File> {
    if fs::metadata(path).map(|m| m.len()).unwrap_or(0) > MOUNT_LOG_ROTATE_BYTES {
        rotate_log_tail(path, MOUNT_LOG_KEEP_BYTES as usize);
    }
    fs::OpenOptions::new().create(true).append(true).open(path)
}

/// Keep only the last `keep` bytes of `path` (best effort).
fn rotate_log_tail(path: &Path, keep: usize) {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = fs::File::open(path) else {
        return;
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if len <= keep as u64 {
        return;
    }
    if file.seek(SeekFrom::Start(len - keep as u64)).is_err() {
        return;
    }
    let mut tail = Vec::new();
    if file.read_to_end(&mut tail).is_err() {
        return;
    }
    let tmp = path.with_extension("log.tmp");
    let _ = fs::write(&tmp, &tail).and_then(|()| fs::rename(&tmp, path));
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
    // Append (with rotation) so a restart loop keeps earlier crash output
    // instead of truncating it away (#61).
    let log_file = open_mount_log(&log_path)?;

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
///
/// The window starts at an arbitrary byte offset, so it may begin in the
/// middle of a multi-byte UTF-8 sequence (common with Chinese error text);
/// the leading partial sequence is skipped and anything still undecodable is
/// replaced lossily instead of failing the whole read with `InvalidData` —
/// which used to leave the user with only a generic "挂载失败" (#61).
pub fn read_log_tail(path: &Path, max_bytes: usize) -> String {
    let Ok(meta) = fs::metadata(path) else {
        return String::new();
    };
    let len = meta.len() as usize;
    let skip = len.saturating_sub(max_bytes);
    let Ok(mut file) = fs::File::open(path) else {
        return String::new();
    };
    use std::io::Read;
    let mut buf = Vec::new();
    if skip > 0 {
        use std::io::Seek;
        if file.seek(std::io::SeekFrom::Start(skip as u64)).is_err() {
            return String::new();
        }
        // Drop continuation bytes of a multi-byte UTF-8 char the window
        // landed inside (at most 3 per char) and keep from the next char
        // boundary, so the lossy decode does not start with a replacement
        // character. The boundary byte itself is kept.
        let mut probe = [0u8; 1];
        for _ in 0..3 {
            if file.read(&mut probe).is_err() {
                return String::new();
            }
            if probe[0] & 0xC0 != 0x80 {
                buf.extend_from_slice(&probe);
                break;
            }
        }
    }
    let _ = file.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn export_omits_empty_region() {
        // Regression (#61): exporting a profile without a region used to emit
        // `"region": ""`, which ossmount expands to `--region ""` and thereby
        // overrides its documented `us-east-1` default.
        let mut p = oss_profile();
        p.s3_region = String::new();
        let json: serde_json::Value = serde_json::from_str(&p.to_ossmount_config()).expect("json");
        assert!(json.get("region").is_none(), "got: {json}");

        p.s3_region = "cn-shanghai".into();
        let json: serde_json::Value = serde_json::from_str(&p.to_ossmount_config()).expect("json");
        assert_eq!(json["region"].as_str(), Some("cn-shanghai"));
    }

    #[test]
    fn read_log_tail_decodes_partial_utf8_window() {
        // Regression (#61): a window starting mid multi-byte character used
        // to fail read_to_string with InvalidData and leave the user with an
        // empty (generic) failure message.
        let dir = std::env::temp_dir().join(format!("ossfs-tray-test-tail-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mount.log");
        // 100 ASCII bytes, then a long CJK message (3 bytes/char): the
        // 2048-byte window starts at byte 5252, i.e. byte 1 of a 3-byte
        // character.
        let head = "x".repeat(100);
        let tail = "挂载失败：无法解析的响应".repeat(200);
        fs::write(&path, format!("{head}{tail}")).unwrap();

        let got = read_log_tail(&path, 2048);
        assert!(
            got.contains("无法解析的响应"),
            "tail must decode lossily instead of failing; got {got:?}"
        );
        assert!(
            !got.contains('\u{FFFD}'),
            "boundary skip must avoid a leading replacement char; got {got:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_mount_log_appends_and_rotates() {
        // Regression (#61): each spawn truncated the previous crash log with
        // File::create, making a crash loop undiagnosable.
        let dir = std::env::temp_dir().join(format!("ossfs-tray-test-log-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m-oss.log");

        {
            let mut f = open_mount_log(&path).expect("open");
            use std::io::Write;
            f.write_all(b"first crash\n").unwrap();
        }
        {
            let mut f = open_mount_log(&path).expect("open");
            use std::io::Write;
            f.write_all(b"second crash\n").unwrap();
        }
        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "first crash\nsecond crash\n", "must append");

        // Past the rotation threshold the log shrinks to its tail.
        let big = "y".repeat(MOUNT_LOG_ROTATE_BYTES as usize + 100);
        fs::write(&path, &big).unwrap();
        open_mount_log(&path).expect("open after rotate");
        let len = fs::metadata(&path).unwrap().len();
        assert!(
            len <= MOUNT_LOG_ROTATE_BYTES as u64,
            "oversized log must be rotated down, got {len}"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
