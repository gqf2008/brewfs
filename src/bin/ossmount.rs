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
        "usage: ossmount [mount] --bucket BUCKET [--endpoint URL] [--region REGION]\n\
                 [--prefix PREFIX] [--force-path-style] [--refresh-secs N]\n\
                 [--read-only] [--uid N] [--gid N] [--dir-mode M] [--file-mode M]\n\
                 [--no-rename-dir] [--rename-dir-limit N] [--max-upload-bytes N]\n\
                 [--read-ahead-bytes N] [--no-ignore-fsync] [--no-verify-crc64]\n\
                 [--max-dirty-bytes N] [--credential-process CMD]\n\
                 [--disk-cache-dir PATH] [--disk-cache-max-bytes N] [--disk-cache-block-size N] [--disk-cache-prefetch-blocks N] [--disk-cache-prefetch-concurrency N] [--disk-cache-verify-etag] [--disk-cache-etag-ttl N]\n\
                 [--metrics-listen ADDR]\n\
                 [--log-dir PATH] [--log-level LEVEL] [--metrics-log-interval N]\n\
                 [--total-mem-limit N] [--total-mem-read-ratio R] [--read-cache-max-bytes N]\n\
                 MOUNT_POINT\n\
         --refresh-secs N:  periodic directory refresh interval in seconds\n\
                           (FUSE; 0 disables. Windows WinFsp fixed at 10s)\n\
         env:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
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
    let mut allow_rename_dir = true;
    let mut rename_dir_limit: Option<u64> = Some(2_000_000);
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
    let mut log_dir: Option<PathBuf> = None;
    let mut metrics_log_interval: u64 = 0;
    let mut log_level: Option<String> = None;
    let mut metrics_listen: Option<String> = None;
    let mut verify_crc64 = true;
    let mut mount_point: Option<PathBuf> = None;

    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("mount") {
        args.remove(0);
    }
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
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
            "--no-rename-dir" => allow_rename_dir = false,
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
            "--disk-cache-etag-ttl" => {
                disk_cache_etag_ttl_secs = iter
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
            "--refresh-secs" => {
                refresh_secs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            other if other.starts_with("--") => usage(),
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
            max_concurrent_requests: None,
            read_only,
            uid,
            gid,
            dir_mode,
            file_mode,
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
            disk_cache_prefetch_blocks,
            disk_cache_prefetch_concurrency,
            disk_cache_verify_etag,
            disk_cache_etag_ttl_secs,
            total_mem_limit,
            total_mem_read_ratio,
            read_cache_max_bytes,
            verify_crc64,
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
    use super::parse_mode;

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
