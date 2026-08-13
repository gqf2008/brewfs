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

use ossfs::{ObjectFs, OssConfig};

fn usage() -> ! {
    eprintln!(
        "usage: ossmount [mount] [--config PATH] --bucket BUCKET [--endpoint URL] [--region REGION] [--version]\n\
                 [--prefix PREFIX] [--force-path-style] [--refresh-secs N]\n\
                 [--read-only] [--uid N] [--gid N] [--dir-mode M] [--file-mode M]\n\
                 [--allow-other] [--umask M]\n\
                 [--no-rename-dir] [--rename-dir-limit N] [--max-upload-bytes N]\n\
                 [--max-concurrent-requests N] [--list-rate-limit R]\n\
                 [--read-ahead-bytes N] [--no-ignore-fsync] [--no-verify-crc64]\n\
                 [--storage-class SC] [--multipart-size N] [--multipart-concurrency N]\n\
                 [--content-md5] [--connect-timeout N] [--readwrite-timeout N] [--retries N]\n\
                 [--notsup-compat-dir]\n\
                 [--disk-cache-reserve-diskfree N] [--disk-cache-free-space-ratio R]\n\
                 [--max-dirty-bytes N] [--credential-process CMD]\n\
                 [--disk-cache-dir PATH] [--disk-cache-max-bytes N] [--disk-cache-block-size N] [--disk-cache-prefetch-blocks N] [--disk-cache-prefetch-concurrency N] [--disk-cache-verify-etag] [--disk-cache-etag-ttl N] [--negative-cache-ttl N] [--negative-cache-max-entries N] [--stat-cache-ttl N] [--stat-cache-max-entries N]\n\
                 [--metrics-listen ADDR]\n\
                 [--log-dir PATH] [--log-level LEVEL] [--metrics-log-interval N]\n\
                 [--total-mem-limit N] [--total-mem-read-ratio R] [--read-cache-max-bytes N]\n\
                 MOUNT_POINT\n\
         --refresh-secs N:  periodic directory refresh interval in seconds\n\
                           (FUSE; 0 disables. Windows WinFsp fixed at 10s)\n\
         env:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY\n\
         --config PATH:  JSON config file; keys are long option names (CLI\n\
                          args override file values). access_key_id /\n\
                          secret_access_key keys set the AWS env creds."
    );
    std::process::exit(2);
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

fn parse_args() -> (
    OssConfig,
    PathBuf,
    u64,
    Option<String>,
    Option<PathBuf>,
    Option<String>,
    u64,
) {
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

    let mut raw: Vec<String> = env::args().skip(1).collect();
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
        },
        mount_point,
        refresh_secs,
        metrics_listen,
        log_dir,
        log_level,
        metrics_log_interval,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cfg, mount_point, refresh_secs, metrics_listen, log_dir, log_level, metrics_log_interval) =
        parse_args();
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
    #[cfg(not(windows))]
    {
        ossfs::fuse::mount_oss_fuse(fs, &mount_point, refresh_secs).await
    }
}

#[cfg(test)]
mod tests {
    use super::{KNOWN_CONFIG_KEYS, expand_config_file, parse_mode};

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
