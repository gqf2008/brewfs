//! Minimal Prometheus-style metrics endpoint for `ossmount`.
//!
//! `serve_metrics` binds a TCP listener and serves `GET /metrics` with the
//! monotonic counters exposed by [`ObjectFs::metrics`]. It intentionally
//! avoids any external HTTP dependency: the request grammar is tiny and the
//! endpoint is meant for local observability only.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::{MetricsSnapshot, ObjectFs};

/// Serve `GET /metrics` on `addr` until the process exits. The returned future
/// never completes normally; bind/accept errors are returned to the caller.
pub async fn serve_metrics(addr: &str, fs: Arc<ObjectFs>) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind metrics listener {addr}"))?;

    loop {
        let (mut stream, _peer) = listener
            .accept()
            .await
            .context("accept metrics connection")?;
        let fs = Arc::clone(&fs);
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let Ok(n) = stream.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let response = if request.starts_with("GET /metrics") || request.starts_with("GET /") {
                let body = format_prometheus(&fs.metrics());
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            };
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

fn format_prometheus(s: &MetricsSnapshot) -> String {
    let mut out = String::with_capacity(256);
    for (name, value) in [
        ("ossfs_reads_total", s.reads),
        ("ossfs_writes_total", s.writes),
        ("ossfs_s3_gets_total", s.s3_gets),
        ("ossfs_s3_heads_total", s.s3_heads),
        ("ossfs_s3_lists_total", s.s3_lists),
        ("ossfs_s3_puts_total", s.s3_puts),
        ("ossfs_s3_errors_total", s.s3_errors),
        ("ossfs_s3_get_errors_total", s.s3_get_errors),
        ("ossfs_s3_list_errors_total", s.s3_list_errors),
        ("ossfs_s3_put_errors_total", s.s3_put_errors),
        ("ossfs_s3_delete_errors_total", s.s3_delete_errors),
        ("ossfs_s3_multipart_errors_total", s.s3_multipart_errors),
        ("ossfs_upload_bytes_total", s.upload_bytes_total),
        ("ossfs_download_bytes_total", s.download_bytes_total),
        ("ossfs_read_cache_hits_total", s.read_cache_hits),
        ("ossfs_read_cache_misses_total", s.read_cache_misses),
        ("ossfs_disk_cache_hits_total", s.disk_cache_hits),
        ("ossfs_disk_cache_misses_total", s.disk_cache_misses),
        ("ossfs_prefetch_started_total", s.prefetch_started),
        ("ossfs_prefetch_inflight", s.prefetch_inflight as u64),
        ("ossfs_prefetch_skipped_total", s.prefetch_skipped),
        ("ossfs_prefetch_failed_total", s.prefetch_failed),
        ("ossfs_crc64_mismatches_total", s.crc64_mismatches),
    ] {
        out.push_str(&format!("{name} {value}\n"));
    }
    let avg_upload = if s.s3_puts > 0 {
        s.upload_bytes_total as f64 / s.s3_puts as f64
    } else {
        0.0
    };
    let avg_download = if s.s3_gets > 0 {
        s.download_bytes_total as f64 / s.s3_gets as f64
    } else {
        0.0
    };
    out.push_str(&format!("ossfs_avg_upload_bytes {avg_upload:.2}\n"));
    out.push_str(&format!("ossfs_avg_download_bytes {avg_download:.2}\n"));
    let read_total = s.read_cache_hits + s.read_cache_misses;
    let read_hit_ratio = if read_total > 0 {
        s.read_cache_hits as f64 / read_total as f64
    } else {
        0.0
    };
    let disk_total = s.disk_cache_hits + s.disk_cache_misses;
    let disk_hit_ratio = if disk_total > 0 {
        s.disk_cache_hits as f64 / disk_total as f64
    } else {
        0.0
    };
    out.push_str(&format!("ossfs_read_cache_hit_ratio {read_hit_ratio:.4}\n"));
    out.push_str(&format!("ossfs_disk_cache_hit_ratio {disk_hit_ratio:.4}\n"));
    out
}

#[cfg(test)]
mod tests {
    use super::format_prometheus;
    use crate::ossfs::MetricsSnapshot;

    #[test]
    fn formats_prometheus_lines() {
        let body = format_prometheus(&MetricsSnapshot {
            reads: 1,
            writes: 2,
            s3_gets: 3,
            s3_heads: 4,
            s3_lists: 4,
            s3_puts: 5,
            s3_errors: 9,
            s3_get_errors: 1,
            s3_list_errors: 2,
            s3_put_errors: 3,
            s3_delete_errors: 4,
            s3_multipart_errors: 5,
            upload_bytes_total: 123,
            download_bytes_total: 456,
            read_cache_hits: 6,
            read_cache_misses: 14,
            disk_cache_hits: 7,
            disk_cache_misses: 13,
            prefetch_started: 10,
            prefetch_inflight: 3,
            prefetch_skipped: 11,
            prefetch_failed: 12,
            crc64_mismatches: 8,
        });
        assert!(body.contains("ossfs_reads_total 1\n"));
        assert!(body.contains("ossfs_crc64_mismatches_total 8\n"));
        assert!(body.contains("ossfs_avg_upload_bytes 24.60\n"));
        assert!(body.contains("ossfs_avg_download_bytes 152.00\n"));
        assert!(body.contains("ossfs_read_cache_hit_ratio 0.3000\n"));
        assert!(body.contains("ossfs_disk_cache_hit_ratio 0.3500\n"));
    }
}
