//! `ossmount` — mount an S3-compatible bucket (Aliyun OSS, MinIO, ...) as a
//! local filesystem with **no local metadata database**.
//!
//! The bucket is the single source of truth: paths are encoded into object
//! keys, so any number of machines can mount the same bucket and see the same
//! tree. Consistency is weak (no locks / no atomic rename) — it is a "cloud
//! drive", not a multi-writer POSIX filesystem.
//!
//! Credentials come from the environment (`AWS_ACCESS_KEY_ID`,
//! `AWS_SECRET_ACCESS_KEY`), matching how the OSSFS tray app spawns mounts.
//!
//! Platform mount adapters:
//! - Windows: WinFsp 2.x (`mount_oss_winfsp`)
//! - macOS: FUSE via macFUSE (`mount_oss_fuse`); Linux: FUSE via libfuse
//!
//! The `MOUNT_POINT` is a drive letter (`Z:`) on Windows and a directory
//! (e.g. `/Volumes/ossfs`) on macOS/Linux.

use anyhow::Context as _;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use tracing_subscriber::EnvFilter;

use ossfs::trash::SystemTrashConfig;
use ossfs::{ObjectFs, OssConfig, TrashRefreshMode};

fn usage_text() -> String {
    "usage: ossmount [mount] [--config PATH] --bucket BUCKET [--endpoint URL] [--region REGION] [--version]\n\
                 [--prefix PREFIX] [--force-path-style] [--refresh-secs N]\n\
                 [--read-only] [--uid N] [--gid N] [--dir-mode M] [--file-mode M]\n\
                 [--allow-other] [--umask M]\n\
                 [--no-rename-dir] [--rename-dir-limit N] [--max-upload-bytes N]\n\
                 [--max-concurrent-requests N] [--list-rate-limit R]\n\
                 [--read-ahead-bytes N] [--no-ignore-fsync] [--no-verify-crc64]\n\
                 [--storage-class SC] [--multipart-size N] [--multipart-concurrency N]\n\
                 [--content-md5] [--connect-timeout N] [--readwrite-timeout N] [--retries N]\n\
                 (timeouts default 10 / 60 seconds; 0 = default; retries default 1, 0 = no retry)\n\
                 (readwrite-timeout bounds one whole S3 request incl. its retries —\n\
                  a slow-but-flowing upload/download is also cut at this budget)\n\
                 [--notsup-compat-dir]\n\
                 [--disk-cache-reserve-diskfree N] [--disk-cache-free-space-ratio R]\n\
                 [--max-dirty-bytes N] [--credential-process CMD]\n\
                 [--disk-cache-dir PATH] [--disk-cache-max-bytes N] [--disk-cache-block-size N] [--disk-cache-prefetch-blocks N] [--disk-cache-prefetch-concurrency N] [--disk-cache-verify-etag] [--disk-cache-etag-ttl N] [--negative-cache-ttl N] [--negative-cache-max-entries N] [--stat-cache-ttl N] [--stat-cache-max-entries N]\n\
                 [--metrics-listen ADDR]\n\
                 [--log-dir PATH] [--log-level LEVEL] [--metrics-log-interval N]\n\
                 [--total-mem-limit N] [--total-mem-read-ratio R] [--read-cache-max-bytes N]\n\
                 [--trash-dir NAME] [--trash-refresh-mode lazy|eager] [--no-trash]\n\
                 [--system-trash-dir NAME] [--system-trash-uids N[,N...]] [--no-system-trash]\n\
                 MOUNT_POINT\n\
         --refresh-secs N:  periodic directory refresh interval in seconds\n\
                           (FUSE; 0 disables. Windows WinFsp fixed at 10s)\n\
         --trash-dir NAME:  trash (soft delete) directory, default ON as .trash\n\
                           (deleted objects stay until GC: default 30-day retention,\n\
                           --trash-retention-days N to override);\n\
                           --no-trash restores immediate permanent delete\n\
         --trash-refresh-mode lazy|eager:  trash refresh policy, default lazy\n\
                           (eager refreshes tombstones before every list/stat,\n\
                           shrinking the remote-deletion visibility window)\n\
         --system-trash-dir NAME:  system recycle bin virtual view (issue #80)\n\
                           Windows/Linux: default ON with trash (dir $Recycle.Bin);\n\
                           macOS: default OFF — this flag enables it (dir .Trashes);\n\
                           NAME overrides the directory name on any platform\n\
         --system-trash-uids N[,N...]:  macOS only — render only these uid dirs\n\
                           under .Trashes (default: the mounting user's uid)\n\
         --no-system-trash:  disable the system recycle bin view (any platform)\n\
         env:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n\
         --config PATH:  JSON config file; keys are long option names (CLI\n\
                          args override file values). access_key_id /\n\
                          secret_access_key keys set the AWS env creds.\n\
subcommands:\n\
  mount [options] MOUNT_POINT (default when first arg is not a subcommand)\n\
  trash-list [--json] [--trash-dir PATH] (connection args below)\n\
  trash-restore <path> [--date YYYY-MM-DD] [--trash-dir PATH]\n\
  trash-clean [--before YYYY-MM-DD] [--dry-run] [--trash-dir PATH]\n\
                  (--before only tightens the default retention window:\n\
                   dates later than today-N retention days are ignored)\n\
  trash options: --trash-dir PATH --trash-retention-days N\n\
                  --trash-refresh-interval-secs N --trash-refresh-mode lazy|eager\n\
                  --trash-gc-interval-secs N --no-trash (mount only)\n\
  trash commands share the connection args (--bucket/--endpoint/--region/...)\n\
  and ignore mount-only keys coming from a --config file; passing them on\n\
  the command line is an error"
        .to_string()
}

fn usage() -> ! {
    eprint!("{}", usage_text());
    std::process::exit(2);
}

/// Print usage to stdout and exit 0 (used by `--help` / `-h`).
fn usage_ok() -> ! {
    print!("{}", usage_text());
    std::process::exit(0);
}

/// Parse a POSIX permission mode: accepts octal (`755` / `0o755`) or
/// decimal. Octal is the expected input for `--dir-mode` / `--file-mode`.
fn parse_mode(s: &str) -> Option<u32> {
    let s = s.trim();
    if let Some(oct) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        return u32::from_str_radix(oct, 8).ok();
    }
    if !s.is_empty() && s.chars().all(|c| ('0'..='7').contains(&c)) {
        return u32::from_str_radix(s, 8).ok();
    }
    s.parse().ok()
}

/// Mount options accepted as `--config` keys. Kept in sync with the CLI long
/// option names (minus `--config` and `--version`, which are not mount options).
const KNOWN_CONFIG_KEYS: &[&str] = &[
    "bucket",
    "endpoint",
    "region",
    "prefix",
    "force-path-style",
    "refresh-secs",
    "read-only",
    "uid",
    "gid",
    "dir-mode",
    "file-mode",
    "allow-other",
    "umask",
    "no-rename-dir",
    "rename-dir-limit",
    "max-upload-bytes",
    "max-concurrent-requests",
    "list-rate-limit",
    "read-ahead-bytes",
    "no-ignore-fsync",
    "max-dirty-bytes",
    "credential-process",
    "no-verify-crc64",
    "content-md5",
    "storage-class",
    "multipart-size",
    "multipart-concurrency",
    "connect-timeout",
    "readwrite-timeout",
    "retries",
    "notsup-compat-dir",
    "disk-cache-dir",
    "disk-cache-max-bytes",
    "disk-cache-block-size",
    "disk-cache-prefetch-blocks",
    "disk-cache-prefetch-concurrency",
    "disk-cache-verify-etag",
    "disk-cache-etag-ttl",
    "disk-cache-reserve-diskfree",
    "disk-cache-free-space-ratio",
    "negative-cache-ttl",
    "negative-cache-max-entries",
    "stat-cache-ttl",
    "stat-cache-max-entries",
    "total-mem-limit",
    "total-mem-read-ratio",
    "read-cache-max-bytes",
    "metrics-listen",
    "log-dir",
    "log-level",
    "metrics-log-interval",
    "trash-dir",
    "trash-retention-days",
    "trash-refresh-interval-secs",
    "trash-refresh-mode",
    "trash-gc-interval-secs",
    "no-trash",
    "system-trash-dir",
    "system-trash-uids",
    "no-system-trash",
];

/// Expand a JSON config file into CLI arguments. Each top-level key maps
/// to a `--key` option: `true` emits a bare switch flag, `false` skips it,
/// and other values are emitted as `--key value`. The credential keys
/// `access_key_id` / `secret_access_key` are applied to the environment.
fn expand_config_file(path: &str) -> Result<Vec<String>, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read config file {path}: {e}"))?;
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("invalid config JSON in {path}: {e}"))?;
    let Some(obj) = value.as_object() else {
        return Err(format!("config file {path} must contain a JSON object"));
    };
    let mut args = Vec::new();
    for (key, val) in obj {
        if key == "access_key_id" || key == "secret_access_key" {
            let Some(s) = val.as_str() else {
                return Err(format!("config key `{key}` must be a string"));
            };
            if !s.is_empty() {
                let var = if key == "access_key_id" {
                    "AWS_ACCESS_KEY_ID"
                } else {
                    "AWS_SECRET_ACCESS_KEY"
                };
                // SAFETY: config expansion runs during arg parsing, before any threads spawn.
                unsafe { std::env::set_var(var, s) };
            }
            continue;
        }
        if key == "mount_point" {
            let Some(s) = val.as_str() else {
                return Err("config key `mount_point` must be a string".to_string());
            };
            args.push(s.to_string());
            continue;
        }
        let normalized = key.replace('_', "-");
        if !KNOWN_CONFIG_KEYS.contains(&normalized.as_str()) {
            return Err(format!("unknown config key `{key}`"));
        }
        match val {
            serde_json::Value::Bool(true) => args.push(format!("--{normalized}")),
            serde_json::Value::Bool(false) => {}
            serde_json::Value::String(s) => {
                args.push(format!("--{normalized}"));
                args.push(s.clone());
            }
            serde_json::Value::Number(n) => {
                args.push(format!("--{normalized}"));
                args.push(n.to_string());
            }
            _ => {
                return Err(format!(
                    "config key `{key}` must be a string, number, or boolean"
                ));
            }
        }
    }
    Ok(args)
}

fn parse_args_from(
    raw_args: Vec<String>,
) -> (
    OssConfig,
    PathBuf,
    u64,
    Option<String>,
    Option<PathBuf>,
    Option<String>,
    u64,
) {
    let mut raw = raw_args;
    let mut bucket = String::new();
    let mut endpoint: Option<String> = None;
    let mut region = "us-east-1".to_string();
    let mut prefix = String::new();
    let mut force_path_style = false;
    let mut refresh_secs: u64 = 10;
    let mut read_only = false;
    let mut uid: u32 = 0;
    let mut gid: u32 = 0;
    let mut dir_mode: u32 = 0o755;
    let mut file_mode: u32 = 0o644;
    let mut allow_other = false;
    let mut umask: u32 = 0;
    let mut allow_rename_dir = true;
    let mut rename_dir_limit: Option<u64> = Some(2_000_000);
    let mut max_concurrent_requests: Option<usize> = None;
    let mut list_rate_limit: Option<f64> = None;
    let mut max_upload_bytes: Option<usize> = None;
    let mut read_ahead_bytes: Option<usize> = Some(8 * 1024 * 1024);
    let mut ignore_fsync = true;
    let mut max_dirty_bytes: Option<usize> = None;
    let mut credential_process: Option<String> = None;
    let mut disk_cache_dir: Option<PathBuf> = None;
    let mut total_mem_limit: Option<usize> = None;
    let mut total_mem_read_ratio: f64 = 0.5;
    let mut read_cache_max_bytes: Option<usize> = None;
    let mut disk_cache_max_bytes: usize = 0;
    let mut disk_cache_block_size: Option<usize> = None;
    let mut disk_cache_prefetch_blocks: usize = 1;
    let mut disk_cache_prefetch_concurrency: usize = 4;
    let mut disk_cache_verify_etag = false;
    let mut disk_cache_etag_ttl_secs: u64 = 10;
    let mut negative_cache_ttl_secs: u64 = 5;
    let mut negative_cache_max_entries: usize = 4096;
    let mut stat_cache_ttl_secs: u64 = 3;
    let mut stat_cache_max_entries: usize = 4096;
    let mut log_dir: Option<PathBuf> = None;
    let mut metrics_log_interval: u64 = 0;
    let mut log_level: Option<String> = None;
    let mut metrics_listen: Option<String> = None;
    let mut verify_crc64 = true;
    let mut storage_class: Option<String> = None;
    let mut content_md5 = false;
    let mut notsup_compat_dir = false;
    let mut connect_timeout_secs: Option<u64> = None;
    let mut readwrite_timeout_secs: Option<u64> = None;
    let mut retries: Option<u32> = None;
    let mut multipart_size: Option<usize> = None;
    let mut multipart_concurrency: Option<usize> = None;
    let mut disk_cache_reserve_diskfree: u64 = 0;
    let mut disk_cache_free_space_ratio: Option<f64> = None;
    let mut mount_point: Option<PathBuf> = None;
    let mut trash_dir: Option<String> = None;
    let mut trash_retention_days: Option<u32> = None;
    let mut trash_refresh_interval_secs: Option<u64> = None;
    let mut trash_refresh_mode: Option<TrashRefreshMode> = None;
    let mut trash_gc_interval_secs: Option<u64> = None;
    let mut no_trash = false;
    let mut system_trash_dir: Option<String> = None;
    let mut system_trash_uids: Vec<u32> = Vec::new();
    let mut no_system_trash = false;

    if raw.first().map(String::as_str) == Some("mount") {
        raw.remove(0);
    }
    // Expand --config/-c JSON files first; CLI args are appended after so
    // they override file values.
    let mut args: Vec<String> = Vec::new();
    let mut cli_args: Vec<String> = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == "--config" || raw[i] == "-c" {
            let path = raw.get(i + 1).cloned().unwrap_or_else(|| usage());
            match expand_config_file(&path) {
                Ok(expanded) => args.extend(expanded),
                Err(e) => {
                    eprintln!("ossmount: {e}");
                    std::process::exit(2);
                }
            }
            i += 2;
        } else {
            cli_args.push(raw[i].clone());
            i += 1;
        }
    }
    args.extend(cli_args);
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => usage_ok(),
            "--version" => {
                println!(
                    "{} {} ({} {} dirty={} build={})",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                    env!("OSSFS_GIT_COMMIT_SHORT"),
                    env!("OSSFS_GIT_BRANCH"),
                    env!("OSSFS_GIT_DIRTY"),
                    env!("OSSFS_BUILD_TIMESTAMP"),
                );
                std::process::exit(0);
            }
            "--bucket" => bucket = iter.next().unwrap_or_else(|| usage()),
            "--endpoint" => endpoint = Some(iter.next().unwrap_or_else(|| usage())),
            "--region" => region = iter.next().unwrap_or_else(|| usage()),
            "--prefix" => prefix = iter.next().unwrap_or_else(|| usage()),
            "--force-path-style" => force_path_style = true,
            "--read-only" => read_only = true,
            "--uid" => {
                uid = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--gid" => {
                gid = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--dir-mode" => {
                dir_mode = iter
                    .next()
                    .and_then(|v| parse_mode(&v))
                    .unwrap_or_else(|| usage());
            }
            "--file-mode" => {
                file_mode = iter
                    .next()
                    .and_then(|v| parse_mode(&v))
                    .unwrap_or_else(|| usage());
            }
            "--allow-other" => allow_other = true,
            "--umask" => {
                umask = iter
                    .next()
                    .and_then(|v| parse_mode(&v))
                    .unwrap_or_else(|| usage());
            }
            "--no-rename-dir" => allow_rename_dir = false,
            "--max-concurrent-requests" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                max_concurrent_requests = if v == 0 { None } else { Some(v) };
            }
            "--list-rate-limit" => {
                let v: f64 = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                list_rate_limit = if v > 0.0 { Some(v) } else { None };
            }
            "--rename-dir-limit" => {
                let v: u64 = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                rename_dir_limit = if v == 0 { None } else { Some(v) };
            }
            "--max-upload-bytes" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                max_upload_bytes = if v == 0 { None } else { Some(v) };
            }
            "--read-ahead-bytes" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                read_ahead_bytes = if v == 0 { None } else { Some(v) };
            }
            "--no-ignore-fsync" => ignore_fsync = false,
            "--max-dirty-bytes" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                max_dirty_bytes = if v == 0 { None } else { Some(v) };
            }
            "--no-verify-crc64" => verify_crc64 = false,
            "--content-md5" => content_md5 = true,
            "--notsup-compat-dir" => notsup_compat_dir = true,
            "--storage-class" => storage_class = Some(iter.next().unwrap_or_else(|| usage())),
            "--multipart-size" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                multipart_size = if v == 0 { None } else { Some(v) };
            }
            "--multipart-concurrency" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                multipart_concurrency = if v == 0 { None } else { Some(v) };
            }
            "--disk-cache-dir" => {
                disk_cache_dir = Some(PathBuf::from(iter.next().unwrap_or_else(|| usage())))
            }
            "--disk-cache-max-bytes" => {
                disk_cache_max_bytes = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--disk-cache-block-size" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                disk_cache_block_size = if v == 0 { None } else { Some(v) };
            }
            "--disk-cache-prefetch-blocks" => {
                disk_cache_prefetch_blocks = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--disk-cache-prefetch-concurrency" => {
                disk_cache_prefetch_concurrency = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--disk-cache-verify-etag" => disk_cache_verify_etag = true,
            "--disk-cache-reserve-diskfree" => {
                disk_cache_reserve_diskfree = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--disk-cache-free-space-ratio" => {
                let v: f64 = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                disk_cache_free_space_ratio = if v == 0.0 {
                    None
                } else if v > 0.0 && v < 1.0 {
                    Some(v)
                } else {
                    usage();
                };
            }
            "--disk-cache-etag-ttl" => {
                disk_cache_etag_ttl_secs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--negative-cache-ttl" => {
                negative_cache_ttl_secs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--negative-cache-max-entries" => {
                negative_cache_max_entries = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--stat-cache-ttl" => {
                stat_cache_ttl_secs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--stat-cache-max-entries" => {
                stat_cache_max_entries = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--metrics-listen" => metrics_listen = Some(iter.next().unwrap_or_else(|| usage())),
            "--log-dir" => log_dir = Some(PathBuf::from(iter.next().unwrap_or_else(|| usage()))),
            "--log-level" => log_level = Some(iter.next().unwrap_or_else(|| usage())),
            "--metrics-log-interval" => {
                metrics_log_interval = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--total-mem-limit" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                total_mem_limit = if v == 0 { None } else { Some(v) };
            }
            "--total-mem-read-ratio" => {
                total_mem_read_ratio = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .filter(|r| *r > 0.0 && *r < 1.0)
                    .unwrap_or_else(|| usage());
            }
            "--read-cache-max-bytes" => {
                let v: usize = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                read_cache_max_bytes = if v == 0 { None } else { Some(v) };
            }
            "--credential-process" => {
                credential_process = Some(iter.next().unwrap_or_else(|| usage()))
            }
            "--connect-timeout" => {
                let v: u64 = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                connect_timeout_secs = if v > 0 { Some(v) } else { None };
            }
            "--readwrite-timeout" => {
                let v: u64 = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                readwrite_timeout_secs = if v > 0 { Some(v) } else { None };
            }
            "--retries" => {
                retries = Some(
                    iter.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--refresh-secs" => {
                refresh_secs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--no-trash" => no_trash = true,
            "--no-system-trash" => no_system_trash = true,
            "--system-trash-dir" => {
                let v = iter.next().unwrap_or_else(|| usage());
                // 单段名校验(与 build_trash_state 一致)
                if v.is_empty() || v.contains('/') || v == "." || v == ".." {
                    usage();
                }
                system_trash_dir = Some(v);
            }
            "--system-trash-uids" => {
                let v = iter.next().unwrap_or_else(|| usage());
                system_trash_uids = v
                    .split(',')
                    .map(|s| s.trim().parse::<u32>().map_err(|_| ()))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap_or_else(|_| usage());
            }
            "--trash-dir" => {
                let v = iter.next().unwrap_or_else(|| usage());
                // 单段名校验(与 build_trash_state 一致):含 '/'、'.'、'..'、空 → usage()
                if v.is_empty() || v.contains('/') || v == "." || v == ".." {
                    usage();
                }
                trash_dir = Some(v);
            }
            "--trash-refresh-mode" => {
                let v = iter.next().unwrap_or_else(|| usage());
                trash_refresh_mode = Some(match v.as_str() {
                    "lazy" => TrashRefreshMode::Lazy,
                    "eager" => TrashRefreshMode::Eager,
                    _ => usage(),
                });
            }
            "--trash-retention-days" => {
                trash_retention_days = Some(
                    iter.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--trash-refresh-interval-secs" => {
                trash_refresh_interval_secs = Some(
                    iter.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--trash-gc-interval-secs" => {
                trash_gc_interval_secs = Some(
                    iter.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            other if other.starts_with("--") => {
                eprintln!("ossmount: unknown option: {other}");
                usage();
            }
            other => mount_point = Some(PathBuf::from(other)),
        }
    }
    let mount_point = mount_point.unwrap_or_else(|| usage());
    if bucket.is_empty() {
        usage();
    }
    (
        OssConfig {
            bucket,
            region,
            endpoint,
            force_path_style,
            prefix,
            max_concurrent_requests,
            list_rate_limit,
            read_only,
            uid,
            gid,
            dir_mode,
            file_mode,
            allow_other,
            umask,
            allow_rename_dir,
            rename_dir_limit,
            max_upload_bytes,
            read_ahead_bytes,
            ignore_fsync,
            max_dirty_bytes,
            credential_process,
            disk_cache_dir,
            disk_cache_max_bytes,
            disk_cache_block_size,
            disk_cache_reserve_diskfree,
            disk_cache_free_space_ratio,
            disk_cache_prefetch_blocks,
            disk_cache_prefetch_concurrency,
            disk_cache_verify_etag,
            disk_cache_etag_ttl_secs,
            negative_cache_ttl_secs,
            negative_cache_max_entries,
            stat_cache_ttl_secs,
            stat_cache_max_entries,
            total_mem_limit,
            total_mem_read_ratio,
            read_cache_max_bytes,
            verify_crc64,
            storage_class,
            content_md5,
            notsup_compat_dir,
            connect_timeout_secs,
            readwrite_timeout_secs,
            retries,
            multipart_size,
            multipart_concurrency,
            // CLI 默认开启回收站(D5);--no-trash 优先级最高。
            trash_dir: if no_trash {
                None
            } else {
                Some(trash_dir.unwrap_or_else(|| ".trash".to_string()))
            },
            trash_retention_days,
            trash_refresh_interval_secs,
            trash_refresh_mode,
            trash_gc_interval_secs,
            // 系统回收站视图(裁决 R1):Windows/Linux 默认随 trash 开启,
            // macOS 默认关(仅 --system-trash-dir 显式开启);--no-system-trash
            // 全平台显式关闭;--no-trash(硬删除)时视图无墓碑可渲染,一并关闭。
            system_trash: if no_system_trash || no_trash {
                None
            } else if cfg!(target_os = "macos") {
                system_trash_dir.map(|d| SystemTrashConfig {
                    dir_name: Some(d),
                    macos_uid_dirs: system_trash_uids,
                })
            } else {
                Some(SystemTrashConfig {
                    dir_name: system_trash_dir,
                    macos_uid_dirs: system_trash_uids,
                })
            },
        },
        mount_point,
        refresh_secs,
        metrics_listen,
        log_dir,
        log_level,
        metrics_log_interval,
    )
}

// ---------- 单元 4:回收站管理命令(trash-list / trash-restore / trash-clean) ----------

/// trash 子命令(规格 4.2)。全字段 pub 测试可断言。
#[derive(Debug, Clone, PartialEq, Eq)]
enum TrashCommand {
    List {
        json: bool,
    },
    Restore {
        path: String,
        date: Option<chrono::NaiveDate>,
    },
    Clean {
        before: Option<chrono::NaiveDate>,
        dry_run: bool,
    },
}

/// --before / --date 严格 YYYY-MM-DD(零填充,如 2026-06-01);失败返回
/// None(调用处 usage())。round-trip 校验把 chrono 宽容接受的
/// "2026-6-1" 也判为非法 —— 非法日期必须 usage(),绝不静默当作未提供
/// (M1:静默退化 = 全量扫描 + 可能命中错误日期的墓碑/错误清理范围)。
fn parse_trash_date(s: &str) -> Option<chrono::NaiveDate> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    (d.to_string() == s).then_some(d)
}

/// 连接/trash 选项里带值的标志:拆分 trash 子命令参数时必须连同值一起
/// 归连接参数,否则值会被误判为 trash-restore 的 path。与
/// [`parse_connection_args`] 的接受集保持同步 —— 新增带值连接选项时
/// 必须同步更新本表。
const TRASH_CONN_VALUE_FLAGS: &[&str] = &[
    "-c",
    "--config",
    "--bucket",
    "--endpoint",
    "--region",
    "--prefix",
    "--max-concurrent-requests",
    "--list-rate-limit",
    "--credential-process",
    "--connect-timeout",
    "--readwrite-timeout",
    "--retries",
    "--trash-dir",
    "--trash-retention-days",
    "--trash-refresh-interval-secs",
    "--trash-refresh-mode",
    "--trash-gc-interval-secs",
    "--log-level",
];

/// 把 trash 子命令的原始参数(raw 去掉子命令名)拆成
/// (连接参数, trash 命令参数)。trash 命令参数 = path(裸 positional,
/// 仅 trash-restore)/--date <值>/--before <值>/--dry-run/--json;其余
/// 全部归连接参数(含带值标志的值,表见 [`TRASH_CONN_VALUE_FLAGS`])。
/// 没有这层拆分,两个解析器会把对方的参数当非法输入拒掉 —— trash
/// 子命令整体不可用(实测 `trash-restore docs/a.txt --bucket b` 报
/// "unexpected argument: docs/a.txt"),命令日期校验的失败场景也根本
/// 无法到达。日期值已由 parse_trash_command 校验为 YYYY-MM-DD,绝不
/// 形似选项,跳过安全。
fn split_trash_command_args(raw: &[String]) -> (Vec<String>, Vec<String>) {
    let mut conn = Vec::new();
    let mut trash_args = Vec::new();
    let mut iter = raw.iter().skip(1); // 子命令名
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--date" | "--before" => {
                trash_args.push(arg.clone());
                if let Some(v) = iter.next() {
                    trash_args.push(v.clone());
                }
            }
            "--dry-run" | "--json" => trash_args.push(arg.clone()),
            other if other.starts_with("--") => {
                conn.push(arg.clone());
                if TRASH_CONN_VALUE_FLAGS.contains(&other)
                    && let Some(v) = iter.next()
                {
                    conn.push(v.clone());
                }
            }
            other => trash_args.push(other.to_string()), // path(裸 positional)
        }
    }
    (conn, trash_args)
}

/// 解析 trash 子命令参数(已由 [`split_trash_command_args`] 剥离连接
/// 参数,只含 path/--date/--before/--dry-run/--json)。非法参数(未知
/// 选项 / 多余 positional / 坏日期)→ usage() 退出。
fn parse_trash_command(raw: &[String]) -> TrashCommand {
    let rest = &raw[1..];
    match raw.first().map(String::as_str).unwrap_or("") {
        "trash-list" => {
            let mut json = false;
            for a in rest {
                match a.as_str() {
                    "--json" => json = true,
                    _ => usage(),
                }
            }
            TrashCommand::List { json }
        }
        "trash-restore" => {
            let mut path: Option<String> = None;
            let mut date: Option<chrono::NaiveDate> = None;
            let mut i = 0;
            while i < rest.len() {
                match rest[i].as_str() {
                    "--date" => {
                        let v = rest.get(i + 1).unwrap_or_else(|| usage());
                        // 非法日期(非 YYYY-MM-DD / 越界)→ usage() 退出,
                        // 绝不静默当作未提供(M1:静默退化 = 全量扫描 + 可能
                        // 命中错误日期的墓碑)。
                        let Some(d) = parse_trash_date(v) else {
                            usage();
                        };
                        date = Some(d);
                        i += 2;
                    }
                    other if other.starts_with("--") => usage(),
                    other => {
                        if path.is_some() {
                            usage(); // 多余 positional
                        }
                        path = Some(other.to_string());
                        i += 1;
                    }
                }
            }
            TrashCommand::Restore {
                path: path.unwrap_or_else(|| usage()),
                date,
            }
        }
        "trash-clean" => {
            let mut before: Option<chrono::NaiveDate> = None;
            let mut dry_run = false;
            let mut i = 0;
            while i < rest.len() {
                match rest[i].as_str() {
                    "--before" => {
                        let v = rest.get(i + 1).unwrap_or_else(|| usage());
                        // 非法日期 → usage() 退出,绝不静默当作未提供(M1:
                        // 静默退化 = 按默认 30 天 cutoff 执行,用户意图被
                        // 替换为另一行为)。
                        let Some(b) = parse_trash_date(v) else {
                            usage();
                        };
                        before = Some(b);
                        i += 2;
                    }
                    "--dry-run" => {
                        dry_run = true;
                        i += 1;
                    }
                    other if other.starts_with("--") => usage(),
                    _ => usage(),
                }
            }
            TrashCommand::Clean { before, dry_run }
        }
        _ => usage(),
    }
}

/// 挂载专用、管理命令不消费的键。命令行显式传入 → 报错;config 文件
/// 带入 → 忽略(同一份 mount config 可复用于管理命令,规格 4.2)。
const MOUNT_ONLY_SWITCH_KEYS: &[&str] = &[
    "read-only",
    "allow-other",
    "no-rename-dir",
    "no-ignore-fsync",
    "no-verify-crc64",
    "content-md5",
    "notsup-compat-dir",
    "disk-cache-verify-etag",
    "no-system-trash",
];
const MOUNT_ONLY_VALUE_KEYS: &[&str] = &[
    "refresh-secs",
    "uid",
    "gid",
    "dir-mode",
    "file-mode",
    "umask",
    "rename-dir-limit",
    "max-upload-bytes",
    "read-ahead-bytes",
    "max-dirty-bytes",
    "storage-class",
    "multipart-size",
    "multipart-concurrency",
    "disk-cache-dir",
    "disk-cache-max-bytes",
    "disk-cache-block-size",
    "disk-cache-prefetch-blocks",
    "disk-cache-prefetch-concurrency",
    "disk-cache-etag-ttl",
    "disk-cache-reserve-diskfree",
    "disk-cache-free-space-ratio",
    "negative-cache-ttl",
    "negative-cache-max-entries",
    "stat-cache-ttl",
    "stat-cache-max-entries",
    "total-mem-limit",
    "total-mem-read-ratio",
    "read-cache-max-bytes",
    "metrics-listen",
    "log-dir",
    "metrics-log-interval",
    "system-trash-dir",
    "system-trash-uids",
];

/// 管理命令的连接参数解析(规格 4.2 连接复用方案):从 CLI/config 解析
/// 连接子集(bucket/endpoint/region/prefix/force-path-style/
/// credential-process/connect-timeout/readwrite-timeout/retries/
/// max-concurrent-requests/list-rate-limit)+ trash 参数 + log-level。
/// 返回 (OssConfig, log_level)。trash 命令强制 read_only=false
/// (回收站运维必然写操作)。
fn parse_connection_args(
    iter: &mut impl Iterator<Item = String>,
) -> anyhow::Result<(OssConfig, Option<String>)> {
    let raw: Vec<String> = iter.collect();
    // 与 parse_args 相同的 --config 展开;config 来源标记 from_config=true
    // (其挂载专用键被忽略),CLI 来源显式传入挂载专用键 → 报错。
    let mut args: Vec<(String, bool)> = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == "--config" || raw[i] == "-c" {
            let path = raw.get(i + 1).cloned().unwrap_or_else(|| usage());
            match expand_config_file(&path) {
                Ok(expanded) => args.extend(expanded.into_iter().map(|a| (a, true))),
                Err(e) => {
                    eprintln!("ossmount: {e}");
                    std::process::exit(2);
                }
            }
            i += 2;
        } else {
            args.push((raw[i].clone(), false));
            i += 1;
        }
    }
    let mut bucket = String::new();
    let mut endpoint: Option<String> = None;
    let mut region = "us-east-1".to_string();
    let mut prefix = String::new();
    let mut force_path_style = false;
    let mut max_concurrent_requests: Option<usize> = None;
    let mut list_rate_limit: Option<f64> = None;
    let mut credential_process: Option<String> = None;
    let mut connect_timeout_secs: Option<u64> = None;
    let mut readwrite_timeout_secs: Option<u64> = None;
    let mut retries: Option<u32> = None;
    let mut trash_dir: Option<String> = None;
    let mut trash_retention_days: Option<u32> = None;
    let mut trash_refresh_interval_secs: Option<u64> = None;
    let mut trash_refresh_mode: Option<TrashRefreshMode> = None;
    let mut trash_gc_interval_secs: Option<u64> = None;
    let mut log_level: Option<String> = None;
    let mut iter = args.into_iter().peekable();
    while let Some((arg, from_config)) = iter.next() {
        if let Some(rest) = arg.strip_prefix("--") {
            if MOUNT_ONLY_SWITCH_KEYS.contains(&rest) {
                if from_config {
                    continue; // config 挂载专用键忽略
                }
                anyhow::bail!("option `--{rest}` is mount-only and not valid for trash commands");
            }
            if MOUNT_ONLY_VALUE_KEYS.contains(&rest) {
                if from_config {
                    iter.next(); // 跳过其值
                    continue;
                }
                anyhow::bail!("option `--{rest}` is mount-only and not valid for trash commands");
            }
        }
        match arg.as_str() {
            "--bucket" => bucket = iter.next().unwrap_or_else(|| usage()).0,
            "--endpoint" => endpoint = Some(iter.next().unwrap_or_else(|| usage()).0),
            "--region" => region = iter.next().unwrap_or_else(|| usage()).0,
            "--prefix" => prefix = iter.next().unwrap_or_else(|| usage()).0,
            "--force-path-style" => force_path_style = true,
            "--max-concurrent-requests" => {
                let v: usize = iter
                    .next()
                    .map(|(v, _)| v)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                max_concurrent_requests = if v == 0 { None } else { Some(v) };
            }
            "--list-rate-limit" => {
                let v: f64 = iter
                    .next()
                    .map(|(v, _)| v)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                list_rate_limit = if v > 0.0 { Some(v) } else { None };
            }
            "--credential-process" => {
                credential_process = Some(iter.next().unwrap_or_else(|| usage()).0)
            }
            "--connect-timeout" => {
                let v: u64 = iter
                    .next()
                    .map(|(v, _)| v)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                connect_timeout_secs = if v > 0 { Some(v) } else { None };
            }
            "--readwrite-timeout" => {
                let v: u64 = iter
                    .next()
                    .map(|(v, _)| v)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
                readwrite_timeout_secs = if v > 0 { Some(v) } else { None };
            }
            "--retries" => {
                retries = Some(
                    iter.next()
                        .map(|(v, _)| v)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--trash-dir" => {
                let v = iter.next().unwrap_or_else(|| usage()).0;
                // 单段名校验(与 build_trash_state 一致)
                if v.is_empty() || v.contains('/') || v == "." || v == ".." {
                    usage();
                }
                trash_dir = Some(v);
            }
            "--trash-retention-days" => {
                trash_retention_days = Some(
                    iter.next()
                        .map(|(v, _)| v)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--trash-refresh-interval-secs" => {
                trash_refresh_interval_secs = Some(
                    iter.next()
                        .map(|(v, _)| v)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--trash-refresh-mode" => {
                let v = iter.next().unwrap_or_else(|| usage()).0;
                trash_refresh_mode = Some(match v.as_str() {
                    "lazy" => TrashRefreshMode::Lazy,
                    "eager" => TrashRefreshMode::Eager,
                    _ => usage(),
                });
            }
            "--trash-gc-interval-secs" => {
                trash_gc_interval_secs = Some(
                    iter.next()
                        .map(|(v, _)| v)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--log-level" => log_level = Some(iter.next().unwrap_or_else(|| usage()).0),
            "--no-trash" => {
                anyhow::bail!(
                    "--no-trash conflicts with trash management commands (trash must be enabled)"
                );
            }
            other if other.starts_with("--") => {
                anyhow::bail!("ossmount: unknown option: {other}");
            }
            other => {
                anyhow::bail!("ossmount: unexpected argument: {other}");
            }
        }
    }
    if bucket.is_empty() {
        anyhow::bail!("ossmount: missing --bucket");
    }
    Ok((
        OssConfig {
            bucket,
            region,
            endpoint,
            force_path_style,
            prefix,
            max_concurrent_requests,
            list_rate_limit,
            read_only: false, // 管理命令必然写操作
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
            credential_process,
            disk_cache_dir: None,
            disk_cache_max_bytes: 0,
            disk_cache_block_size: None,
            disk_cache_reserve_diskfree: 0,
            disk_cache_free_space_ratio: None,
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
            verify_crc64: false,
            storage_class: None,
            content_md5: false,
            notsup_compat_dir: false,
            connect_timeout_secs,
            readwrite_timeout_secs,
            retries,
            multipart_size: None,
            multipart_concurrency: None,
            // 管理命令必须有回收站(CLI 默认 Some(".trash"));--no-trash 已拦截
            trash_dir: Some(trash_dir.unwrap_or_else(|| ".trash".to_string())),
            trash_retention_days,
            trash_refresh_interval_secs,
            trash_refresh_mode,
            trash_gc_interval_secs,
            // 系统回收站视图是挂载专属(挂载时渲染);管理命令不消费
            system_trash: None,
        },
        log_level,
    ))
}

/// 执行 trash 子命令(独立进程,复用 ObjectFs::connect):tracing 初始化
/// (复用 log-level,默认 info,stderr)→ 连接解析 → connect →
/// 分发(规格 4.2)。解析错误 exit 2(与 usage 一致);restore 未恢复
/// exit 1(0 恢复 / 1 未恢复)。
async fn run_trash_command(cmd: TrashCommand, conn_args: Vec<String>) -> anyhow::Result<()> {
    let (cfg, log_level) = parse_connection_args(&mut conn_args.into_iter()).unwrap_or_else(|e| {
        eprintln!("ossmount: {e:#}");
        std::process::exit(2);
    });
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level.as_deref().unwrap_or("info")));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let fs = Arc::new(ObjectFs::connect(cfg).await?);
    match cmd {
        TrashCommand::List { json } => {
            fs.trash_list(|page| {
                for e in page {
                    if json {
                        println!("{}", serde_json::to_string(&e)?);
                    } else {
                        println!(
                            "{}\t{}\t{}\t{}",
                            e.deleted_date,
                            e.path,
                            e.etag.as_deref().unwrap_or("-"),
                            e.size.map_or_else(|| "-".to_string(), |s| s.to_string())
                        );
                    }
                }
                Ok(())
            })
            .await?;
        }
        TrashCommand::Restore { path, date } => match fs.trash_restore(&path, date).await? {
            ossfs::trash::RestoreOutcome::Restored {
                etag_mismatch,
                multiple_versions,
            } => {
                if etag_mismatch {
                    eprintln!("警告:内容已被其他端修改,恢复的是当前内容");
                }
                if multiple_versions {
                    // L6:同名多日期墓碑只清了最旧一条,key 可能仍被较新
                    // 墓碑隐藏 —— 必须提示,否则用户以为已恢复。
                    eprintln!(
                        "警告:存在 {path} 的多个版本墓碑,仅清除了最旧一条;请用 --date 指定恢复特定版本"
                    );
                }
                println!("已恢复 {path}");
            }
            ossfs::trash::RestoreOutcome::OriginalGone => {
                eprintln!("原对象不存在,无法恢复(墓碑已清除)");
                std::process::exit(1);
            }
            ossfs::trash::RestoreOutcome::NoTombstone => {
                eprintln!("未找到 {path} 的回收站墓碑");
                std::process::exit(1);
            }
        },
        TrashCommand::Clean { before, dry_run } => {
            let report = fs
                .trash_gc(ossfs::trash::GcOptions { before, dry_run })
                .await?;
            println!(
                "files_removed={} files_tombstone_only={} files_skipped_etag={} \
                 dirs_removed={} objects_deleted={} tombstones_deleted={}{}",
                report.files_removed,
                report.files_tombstone_only,
                report.files_skipped_etag,
                report.dirs_removed,
                report.objects_deleted,
                report.tombstones_deleted,
                if dry_run { " (dry-run,未删除)" } else { "" }
            );
        }
    }
    Ok(())
}

/// 周期 GC 任务:先立即 trash_gc(default)(挂载时触发),再 interval
/// 循环(interval.tick() 消费首次立即 tick);每次失败仅 warn 不退出
/// (规格 4.2,与刷新循环同生命周期)。
async fn run_trash_gc_periodic(fs: Arc<ObjectFs>, interval_secs: u64) {
    if let Err(e) = fs.trash_gc(Default::default()).await {
        tracing::warn!(error = %e, "trash gc failed at mount; will retry next cycle");
    }
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // 挂载时已 GC 一次,消费首个立即 tick
    loop {
        interval.tick().await;
        if let Err(e) = fs.trash_gc(Default::default()).await {
            tracing::warn!(error = %e, "trash gc failed; will retry next cycle");
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raw: Vec<String> = env::args().skip(1).collect();
    // 子命令分发(规格 4.2,parse_args 调用之前):trash-* 独立进程,
    // 复用 ObjectFs::connect 的连接参数构造方式。
    match raw.first().map(String::as_str) {
        Some("trash-list") | Some("trash-restore") | Some("trash-clean") => {
            // 子命令参数与连接参数可任意交错(spec 4.2:trash commands
            // share the connection args);先拆分,各解析器只见自己的参数。
            let (conn_args, trash_args) = split_trash_command_args(&raw);
            let mut cmd_raw = vec![raw[0].clone()]; // parse_trash_command 期望首参是子命令名
            cmd_raw.extend(trash_args);
            let cmd = parse_trash_command(&cmd_raw);
            return run_trash_command(cmd, conn_args).await;
        }
        _ => {}
    }
    let (cfg, mount_point, refresh_secs, metrics_listen, log_dir, log_level, metrics_log_interval) =
        parse_args_from(raw);
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level.as_deref().unwrap_or("info")));
    if let Some(dir) = log_dir {
        std::fs::create_dir_all(&dir).context("create log dir")?;
        let appender = tracing_appender::rolling::daily(&dir, "ossmount.log");
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(appender)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    let fs = Arc::new(ObjectFs::connect(cfg).await?);
    // 周期 GC:挂载时立即 trash_gc(default) 一次,再按
    // trash_gc_interval_secs 循环(规格 4.2;trash 关闭或 interval=0 不启动;
    // 只读挂载的 GC 早退在 trash_gc 入口,spawn 本身无妨)。
    if let Some(interval) = fs.trash_gc_interval_secs()
        && interval > 0
    {
        let gc_fs = Arc::clone(&fs);
        tokio::spawn(async move {
            run_trash_gc_periodic(gc_fs, interval).await;
        });
    }
    if metrics_log_interval > 0 {
        let metrics_fs = Arc::clone(&fs);
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(metrics_log_interval));
            ticker.tick().await; // skip the immediate tick
            loop {
                ticker.tick().await;
                let m = metrics_fs.metrics();
                tracing::info!(
                    reads = m.reads,
                    writes = m.writes,
                    s3_gets = m.s3_gets,
                    s3_heads = m.s3_heads,
                    s3_stat_heads = m.s3_stat_heads,
                    stat_cache_hits = m.stat_cache_hits,
                    stat_positive_cache_hits = m.stat_positive_cache_hits,
                    stat_negative_cache_hits = m.stat_negative_cache_hits,
                    s3_etag_heads = m.s3_etag_heads,
                    s3_lists = m.s3_lists,
                    s3_puts = m.s3_puts,
                    s3_errors = m.s3_errors,
                    s3_get_errors = m.s3_get_errors,
                    s3_list_errors = m.s3_list_errors,
                    s3_put_errors = m.s3_put_errors,
                    s3_delete_errors = m.s3_delete_errors,
                    s3_multipart_errors = m.s3_multipart_errors,
                    read_cache_hits = m.read_cache_hits,
                    read_cache_misses = m.read_cache_misses,
                    disk_cache_hits = m.disk_cache_hits,
                    disk_cache_misses = m.disk_cache_misses,
                    prefetch_started = m.prefetch_started,
                    prefetch_inflight = m.prefetch_inflight,
                    prefetch_skipped = m.prefetch_skipped,
                    prefetch_failed = m.prefetch_failed,
                    list_throttled = m.list_throttled,
                    crc64_mismatches = m.crc64_mismatches,
                    upload_bytes_total = m.upload_bytes_total,
                    download_bytes_total = m.download_bytes_total,
                    trash_index_entries = m.trash_index_entries,
                    trash_gc_etag_skips = m.trash_gc_etag_skips,
                    "ossfs metrics snapshot"
                );
            }
        });
    }
    if let Some(addr) = metrics_listen {
        let metrics_fs = Arc::clone(&fs);
        tokio::spawn(async move {
            if let Err(e) = ossfs::admin::serve_metrics(&addr, metrics_fs).await {
                eprintln!("metrics server {addr} failed: {e:#}");
            }
        });
    }

    #[cfg(windows)]
    {
        // WinFsp uses a fixed 10s notify interval (see REFRESH_INTERVAL_MS).
        let _ = refresh_secs;
        ossfs::winfsp::mount_oss_winfsp(fs, &mount_point).await
    }
    #[cfg(all(not(windows), feature = "fuse"))]
    {
        ossfs::fuse::mount_oss_fuse(fs, &mount_point, refresh_secs).await
    }
    #[cfg(all(not(windows), not(feature = "fuse")))]
    {
        let _ = (fs, &mount_point, refresh_secs);
        anyhow::bail!("this build was compiled without the FUSE adapter (feature \"fuse\")")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KNOWN_CONFIG_KEYS, TrashCommand, TrashRefreshMode, expand_config_file, parse_args_from,
        parse_connection_args, parse_mode, parse_trash_command, split_trash_command_args,
    };

    #[test]
    fn parses_trash_refresh_mode_flag() {
        // --trash-refresh-mode eager → OssConfig.trash_refresh_mode = Some(Eager)
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--trash-refresh-mode".to_string(),
            "eager".to_string(),
            "Z:".to_string(),
        ]);
        assert_eq!(
            cfg.trash_refresh_mode,
            Some(TrashRefreshMode::Eager),
            "--trash-refresh-mode eager 必须映射到 Some(Eager)"
        );
        // lazy 显式值同样可解析(默认值形态)
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--trash-refresh-mode".to_string(),
            "lazy".to_string(),
            "Z:".to_string(),
        ]);
        assert_eq!(cfg.trash_refresh_mode, Some(TrashRefreshMode::Lazy));
    }

    #[test]
    fn config_file_expands_flags_and_skips_false_switches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cfg.json");
        std::fs::write(
            &path,
            r#"{"bucket":"b","read_only":true,"force_path_style":false,"max-concurrent-requests":64}"#,
        )
        .unwrap();
        let args = expand_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(
            args.len(),
            5,
            "one bool switch skipped, three keys expanded"
        );
        let has = |f: &str| args.iter().any(|a| a == f);
        assert!(has("--bucket"));
        assert!(has("--read-only"));
        assert!(has("--max-concurrent-requests"));
        assert!(!has("--force-path-style"));
        for (flag, val) in [("--bucket", "b"), ("--max-concurrent-requests", "64")] {
            let i = args.iter().position(|a| a == flag).expect("flag present");
            assert_eq!(args[i + 1], val, "value must follow its flag");
        }
    }

    #[test]
    fn config_file_rejects_non_object_and_nested_values() {
        let dir = tempfile::tempdir().expect("tempdir");

        let arr = dir.path().join("arr.json");
        std::fs::write(&arr, "[1,2]").unwrap();
        let err = expand_config_file(arr.to_str().unwrap()).unwrap_err();
        assert!(err.contains("must contain a JSON object"), "got: {err}");

        let nested = dir.path().join("nested.json");
        std::fs::write(&nested, r#"{"bucket":{"name":"b"}}"#).unwrap();
        let err = expand_config_file(nested.to_str().unwrap()).unwrap_err();
        assert!(
            err.contains("must be a string, number, or boolean"),
            "got: {err}"
        );

        let unknown = dir.path().join("unknown.json");
        std::fs::write(&unknown, r#"{"bucket":"b","buckte":true}"#).unwrap();
        let err = expand_config_file(unknown.to_str().unwrap()).unwrap_err();
        assert!(err.contains("unknown config key `buckte`"), "got: {err}");
    }

    #[test]
    fn example_config_keys_are_all_known() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ossfs.example.json");
        let raw = std::fs::read_to_string(&path).expect("example file");
        let value: serde_json::Value = serde_json::from_str(&raw).expect("valid JSON");
        let obj = value.as_object().expect("object");
        for key in obj.keys() {
            if key == "access_key_id" || key == "secret_access_key" || key == "mount_point" {
                continue;
            }
            let normalized = key.replace('_', "-");
            assert!(
                KNOWN_CONFIG_KEYS.contains(&normalized.as_str()),
                "ossfs.example.json key `{key}` is not a known option"
            );
        }
    }

    #[test]
    fn config_file_expands_mount_point_as_positional() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cfg.json");
        std::fs::write(&path, r#"{"bucket":"b","mount_point":"Z:"}"#).unwrap();
        let args = expand_config_file(path.to_str().unwrap()).unwrap();
        assert!(
            args.iter().any(|a| a == "Z:"),
            "mount_point must be a bare positional"
        );
        assert!(
            !args.iter().any(|a| a.starts_with("--mount")),
            "mount_point must not emit a --mount-point flag"
        );
    }

    #[test]
    fn config_file_ignores_empty_credentials() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cfg.json");
        std::fs::write(
            &path,
            r#"{"bucket":"b","access_key_id":"","secret_access_key":""}"#,
        )
        .unwrap();
        let args = expand_config_file(path.to_str().unwrap()).unwrap();
        assert!(args.iter().any(|a| a == "--bucket"));
        assert!(
            !args
                .iter()
                .any(|a| a.contains("access_key") || a.contains("secret_access"))
        );
    }

    #[test]
    fn parses_octal_and_decimal_modes() {
        assert_eq!(parse_mode("755"), Some(0o755));
        assert_eq!(parse_mode("0o755"), Some(0o755));
        assert_eq!(parse_mode("0O644"), Some(0o644));
        assert_eq!(parse_mode("0644"), Some(0o644));
        assert_eq!(parse_mode("644"), Some(0o644));
        assert_eq!(parse_mode("493"), Some(493)); // '9' forces decimal
        assert_eq!(parse_mode(""), None);
        assert_eq!(parse_mode("abc"), None);
    }

    // ---------- 单元 4:trash 子命令解析 ----------

    #[test]
    fn trash_subcommand_parse() {
        // trash-restore <path> [--date YYYY-MM-DD]
        let cmd = parse_trash_command(&[
            "trash-restore".into(),
            "docs/a.txt".into(),
            "--date".into(),
            "2026-07-01".into(),
        ]);
        assert_eq!(
            cmd,
            TrashCommand::Restore {
                path: "docs/a.txt".into(),
                date: Some(chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap()),
            }
        );
        // trash-restore 无 --date
        let cmd = parse_trash_command(&["trash-restore".into(), "docs/a.txt".into()]);
        assert_eq!(
            cmd,
            TrashCommand::Restore {
                path: "docs/a.txt".into(),
                date: None,
            }
        );
        // trash-clean --before --dry-run
        let cmd = parse_trash_command(&[
            "trash-clean".into(),
            "--before".into(),
            "2026-06-01".into(),
            "--dry-run".into(),
        ]);
        assert_eq!(
            cmd,
            TrashCommand::Clean {
                before: Some(chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()),
                dry_run: true,
            }
        );
        // trash-list --json
        let cmd = parse_trash_command(&["trash-list".into(), "--json".into()]);
        assert_eq!(cmd, TrashCommand::List { json: true });
        let cmd = parse_trash_command(&["trash-list".into()]);
        assert_eq!(cmd, TrashCommand::List { json: false });
    }

    #[test]
    fn trash_command_split_conn_and_own_args() {
        // trash-restore:path(任意位置)+ --date 值归命令参数,连接/trash
        // 选项(含值)归连接参数
        let raw: Vec<String> = [
            "trash-restore",
            "docs/a.txt",
            "--date",
            "2026-07-01",
            "--bucket",
            "b",
            "--trash-dir",
            ".trash",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (conn, trash_args) = split_trash_command_args(&raw);
        assert_eq!(
            conn,
            ["--bucket", "b", "--trash-dir", ".trash"],
            "连接/trash 选项必须保留在连接侧"
        );
        assert_eq!(
            trash_args,
            ["docs/a.txt", "--date", "2026-07-01"],
            "path 与 --date 值必须归命令参数"
        );
        // 顺序任意:--date 在前、path 在后;--config 带路径值不被误判为 path
        let raw2: Vec<String> = [
            "trash-restore",
            "--date",
            "2026-07-01",
            "docs/a.txt",
            "--config",
            "/tmp/cfg.json",
            "--bucket",
            "b",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (conn2, trash_args2) = split_trash_command_args(&raw2);
        assert_eq!(trash_args2, ["--date", "2026-07-01", "docs/a.txt"]);
        assert_eq!(
            conn2,
            ["--config", "/tmp/cfg.json", "--bucket", "b"],
            "--config 的路径值必须留在连接侧"
        );
        // trash-clean:--before 值 + --dry-run 归命令参数
        let raw3: Vec<String> = [
            "trash-clean",
            "--before",
            "2026-06-01",
            "--dry-run",
            "--bucket",
            "b",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (conn3, trash_args3) = split_trash_command_args(&raw3);
        assert_eq!(conn3, ["--bucket", "b"]);
        assert_eq!(trash_args3, ["--before", "2026-06-01", "--dry-run"]);
        // trash-list:--json 归命令参数
        let raw4: Vec<String> = ["trash-list", "--json", "--bucket", "b"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (conn4, trash_args4) = split_trash_command_args(&raw4);
        assert_eq!(conn4, ["--bucket", "b"]);
        assert_eq!(trash_args4, ["--json"]);
        // 未知标志归连接侧,由 parse_connection_args 报错(其后的裸参数
        // 会落到命令侧,但未知标志本身必报错,不会静默通过)
        let raw5: Vec<String> = ["trash-clean", "--bogus", "x", "--bucket", "b"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (conn5, trash_args5) = split_trash_command_args(&raw5);
        assert_eq!(conn5, ["--bogus", "--bucket", "b"]);
        assert_eq!(trash_args5, ["x"]);
        // 裸 positional 只允许一个(restore 的 path);多余交给命令解析报错
        let raw6: Vec<String> = ["trash-restore", "a.txt", "b.txt"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (_conn6, trash_args6) = split_trash_command_args(&raw6);
        assert_eq!(trash_args6, ["a.txt", "b.txt"]);
    }

    #[test]
    fn trash_connection_args_parse() {
        // 连接子集 + trash 参数 + log-level
        let args = vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--endpoint".to_string(),
            "http://127.0.0.1:9000".to_string(),
            "--prefix".to_string(),
            "ossfs".to_string(),
            "--trash-dir".to_string(),
            ".trash".to_string(),
            "--trash-retention-days".to_string(),
            "7".to_string(),
            "--trash-refresh-mode".to_string(),
            "eager".to_string(),
            "--trash-gc-interval-secs".to_string(),
            "3600".to_string(),
            "--log-level".to_string(),
            "debug".to_string(),
        ];
        let (cfg, log_level) = parse_connection_args(&mut args.into_iter()).unwrap();
        assert_eq!(cfg.bucket, "b");
        assert_eq!(cfg.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(cfg.prefix, "ossfs");
        assert_eq!(cfg.trash_dir.as_deref(), Some(".trash"));
        assert_eq!(cfg.trash_retention_days, Some(7));
        assert_eq!(cfg.trash_refresh_mode, Some(TrashRefreshMode::Eager));
        assert_eq!(cfg.trash_gc_interval_secs, Some(3600));
        assert_eq!(log_level.as_deref(), Some("debug"));
        // 默认:trash_dir 默认 Some(".trash")(管理命令需回收站开启)
        let (cfg, _) =
            parse_connection_args(&mut ["--bucket".to_string(), "b".to_string()].into_iter())
                .unwrap();
        assert_eq!(cfg.trash_dir.as_deref(), Some(".trash"));
        // --no-trash 与 trash 命令冲突 → Err
        let err = parse_connection_args(
            &mut [
                "--bucket".to_string(),
                "b".to_string(),
                "--no-trash".to_string(),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("--no-trash"), "got: {err}");
        // CLI 显式挂载专用参数 → Err
        let err = parse_connection_args(
            &mut [
                "--bucket".to_string(),
                "b".to_string(),
                "--uid".to_string(),
                "1000".to_string(),
            ]
            .into_iter(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mount-only"), "got: {err}");
        // config 文件带入的挂载专用键 → 忽略(同一份 mount config 可复用)
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cfg.json");
        std::fs::write(
            &path,
            r#"{"bucket":"b","uid":1000,"read_only":true,"trash_gc_interval_secs":7200}"#,
        )
        .unwrap();
        let (cfg, _) = parse_connection_args(
            &mut ["--config".to_string(), path.to_str().unwrap().to_string()].into_iter(),
        )
        .unwrap();
        assert_eq!(cfg.bucket, "b");
        assert_eq!(cfg.uid, 0, "config 挂载专用键被忽略");
        assert!(!cfg.read_only, "config 挂载专用键被忽略");
        assert_eq!(cfg.trash_gc_interval_secs, Some(7200), "trash 键保留");
    }
    #[test]
    fn parses_system_trash_flags() {
        // --system-trash-dir NAME:开启 + 覆盖目录名(全平台)
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--system-trash-dir".to_string(),
            "CustomBin".to_string(),
            "Z:".to_string(),
        ]);
        let sys = cfg
            .system_trash
            .expect("--system-trash-dir 必须开启系统视图");
        assert_eq!(sys.dir_name.as_deref(), Some("CustomBin"));

        // --system-trash-uids 逗号分隔(全平台接受,macOS 消费;macOS 下
        // 必须与 --system-trash-dir 同传 —— uids 不构成显式开启)
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--system-trash-dir".to_string(),
            "CustomBin".to_string(),
            "--system-trash-uids".to_string(),
            "501,502".to_string(),
            "Z:".to_string(),
        ]);
        let sys = cfg
            .system_trash
            .expect("--system-trash-dir 必须开启系统视图");
        assert_eq!(
            sys.macos_uid_dirs,
            vec![501, 502],
            "--system-trash-uids 必须逗号分隔解析"
        );
        assert_eq!(sys.dir_name.as_deref(), Some("CustomBin"));

        // --no-system-trash:全平台显式关闭(--no-trash 一并关闭视图)
        for extra in [
            vec!["--no-system-trash".to_string()],
            vec!["--no-trash".to_string()],
        ] {
            let mut args = vec!["--bucket".to_string(), "b".to_string()];
            args.extend(extra.clone());
            args.push("Z:".to_string());
            let (cfg, _, _, _, _, _, _) = parse_args_from(args);
            assert!(
                cfg.system_trash.is_none(),
                "{extra:?} 必须关闭系统回收站视图"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn system_trash_default_off_on_macos() {
        // 裁决 R1:macOS 默认关闭(需显式 --system-trash-dir)
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "Z:".to_string(),
        ]);
        assert!(cfg.system_trash.is_none(), "macOS 默认关闭系统回收站视图");
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "--system-trash-dir".to_string(),
            ".Trashes".to_string(),
            "Z:".to_string(),
        ]);
        assert!(
            cfg.system_trash.is_some(),
            "macOS --system-trash-dir 显式开启"
        );
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn system_trash_default_on_non_macos() {
        // 裁决 R1:Windows/Linux 跟随 trash_dir 默认开启
        let (cfg, _, _, _, _, _, _) = parse_args_from(vec![
            "--bucket".to_string(),
            "b".to_string(),
            "Z:".to_string(),
        ]);
        let sys = cfg
            .system_trash
            .expect("Windows/Linux 默认开启系统回收站视图");
        assert_eq!(
            sys.dir_name, None,
            "默认目录名由 build_trash_state 按平台注入"
        );
        assert!(sys.macos_uid_dirs.is_empty());
    }

    #[test]
    fn config_file_expands_system_trash_keys() {
        // system-trash-* 是合法 config 键(expand_config_file 归一化下划线)
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cfg.json");
        std::fs::write(
            &path,
            r#"{"bucket":"b","system-trash-dir":"CustomBin","system-trash-uids":"501","no-system-trash":true}"#,
        )
        .unwrap();
        let args = expand_config_file(path.to_str().unwrap()).unwrap();
        let has = |f: &str| args.iter().any(|a| a == f);
        assert!(has("--system-trash-dir"));
        assert!(has("--system-trash-uids"));
        assert!(has("--no-system-trash"));
        let i = args.iter().position(|a| a == "--system-trash-dir").unwrap();
        assert_eq!(args[i + 1], "CustomBin");
    }
}

#[cfg(all(test, target_os = "windows"))]
mod stack_tests {
    /// The WinFsp host threads that run ossmount's synchronous callbacks
    /// (`get_file_info` / `open` / `get_security_by_name` / ...) are created
    /// with the process default stack size (`SizeOfStackReserve` from the PE
    /// header, `CreateThread` with `dwStackSize = 0`). Those callbacks drive
    /// AWS SDK futures via `Handle::block_on`; the hyper/rustls async stack
    /// can exhaust the linker default 1 MiB reserve under heavy concurrent
    /// I/O and abort the process with 0xc0000409 (FAST_FAIL_FATAL_APP_EXIT).
    /// `build.rs` widens the reserve; this test pins that property so a
    /// future linker change cannot silently shrink it again.
    #[test]
    fn pe_stack_reserve_is_widened() {
        const MIN_RESERVE: u64 = 8 * 1024 * 1024;

        let exe = std::env::current_exe().expect("current_exe");
        let bytes = std::fs::read(&exe).expect("read executable");
        assert!(bytes.starts_with(b"MZ"), "not a PE image");

        let e_lfanew = u32::from_le_bytes(bytes[0x3C..0x40].try_into().unwrap()) as usize;
        let pe = e_lfanew + 4;
        assert_eq!(&bytes[e_lfanew..pe], b"PE\0\0", "missing PE signature");

        let magic = u16::from_le_bytes(bytes[pe + 20..pe + 22].try_into().unwrap());
        assert_eq!(magic, 0x20B, "expected PE32+ (x64) image");

        // PE32+ optional header: SizeOfStackReserve is a u64 at offset 0x48.
        let reserve_off = pe + 20 + 0x48;
        let reserve = u64::from_le_bytes(bytes[reserve_off..reserve_off + 8].try_into().unwrap());
        assert!(
            reserve >= MIN_RESERVE,
            "SizeOfStackReserve {reserve:#x} < {MIN_RESERVE:#x}; build.rs stack widening missing?"
        );
    }
}
