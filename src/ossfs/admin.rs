//! Minimal Prometheus-style metrics endpoint for `ossmount`.
//!
//! `serve_metrics` binds a TCP listener and serves `GET /metrics` with the
//! monotonic counters exposed by [`ObjectFs::metrics`]. It intentionally
//! avoids any external HTTP dependency: the request grammar is tiny and the
//! endpoint is meant for local observability only.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::{MetricsSnapshot, ObjectFs};

/// How long a metrics connection may sit without sending a request before it
/// is dropped. Bounds the per-connection task count: an idle client (open
/// socket, no bytes) otherwise pins a task forever (#60).
const METRICS_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Backoff after an `accept` error so a persistent error (EMFILE, a broken
/// listener) cannot busy-loop the accept task (#60).
const METRICS_ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Serve `GET /metrics` on `addr` until the process exits. Bind errors are
/// returned to the caller; transient `accept` errors are logged and retried
/// so a single bad accept cannot take the metrics endpoint down forever.
pub async fn serve_metrics(addr: &str, fs: Arc<ObjectFs>) -> Result<()> {
    serve_metrics_with_read_timeout(addr, fs, METRICS_READ_TIMEOUT).await
}

/// [`serve_metrics`] with an explicit read timeout (test seam).
pub async fn serve_metrics_with_read_timeout(
    addr: &str,
    fs: Arc<ObjectFs>,
    read_timeout: Duration,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind metrics listener {addr}"))?;

    loop {
        let (mut stream, _peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                // One failed accept (EMFILE, connection aborted during accept,
                // ...) must not terminate the endpoint permanently.
                tracing::warn!(error = %e, "metrics accept failed; retrying");
                tokio::time::sleep(METRICS_ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let fs = Arc::clone(&fs);
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            // Drop connections that connect but never send a request: the
            // task (and its buffers) is bounded by the read timeout.
            let read = tokio::time::timeout(read_timeout, stream.read(&mut buf)).await;
            let Ok(Ok(n)) = read else {
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
            let _ = tokio::time::timeout(read_timeout, stream.write_all(response.as_bytes())).await;
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
        ("ossfs_s3_stat_heads_total", s.s3_stat_heads),
        ("ossfs_stat_cache_hits_total", s.stat_cache_hits),
        (
            "ossfs_stat_positive_cache_hits_total",
            s.stat_positive_cache_hits,
        ),
        (
            "ossfs_stat_negative_cache_hits_total",
            s.stat_negative_cache_hits,
        ),
        ("ossfs_s3_etag_heads_total", s.s3_etag_heads),
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
        ("ossfs_list_throttled_total", s.list_throttled),
        ("ossfs_crc64_mismatches_total", s.crc64_mismatches),
        ("ossfs_trash_index_entries", s.trash_index_entries as u64),
        ("ossfs_trash_gc_etag_skips_total", s.trash_gc_etag_skips),
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
    let stat_total = s.stat_cache_hits + s.s3_stat_heads;
    let stat_hit_ratio = if stat_total > 0 {
        s.stat_cache_hits as f64 / stat_total as f64
    } else {
        0.0
    };
    out.push_str(&format!("ossfs_stat_cache_hit_ratio {stat_hit_ratio:.4}\n"));
    let neg_total = s.stat_positive_cache_hits + s.stat_negative_cache_hits;
    let neg_hit_ratio = if neg_total > 0 {
        s.stat_negative_cache_hits as f64 / neg_total as f64
    } else {
        0.0
    };
    out.push_str(&format!(
        "ossfs_negative_cache_hit_ratio {neg_hit_ratio:.4}\n"
    ));
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
    use std::time::Duration;

    use super::*;
    use crate::ossfs::{MockS3, test_fs};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// Connect with retries until the freshly spawned service has bound its
    /// listener.
    async fn connect_with_retry(addr: &str) -> TcpStream {
        for _ in 0..100 {
            if let Ok(stream) = TcpStream::connect(addr).await {
                return stream;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("metrics service did not come up");
    }

    #[tokio::test]
    async fn idle_connection_is_dropped_and_service_survives() {
        // Regression (#60): an idle connection pinned a per-connection task
        // forever, and any single accept error killed the endpoint.
        let (_mock, port) = MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let fs = Arc::new(test_fs(port, 8));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let addr = format!("127.0.0.1:{}", addr.port());
        let service_addr = addr.clone();
        let service = tokio::spawn(async move {
            serve_metrics_with_read_timeout(&service_addr, fs, Duration::from_millis(200)).await
        });

        // An idle connection must be dropped by the read timeout.
        let mut idle = connect_with_retry(&addr).await;
        let mut scratch = [0u8; 16];
        let closed = tokio::time::timeout(Duration::from_secs(2), idle.read(&mut scratch)).await;
        match closed {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(n)) => panic!("expected the idle connection to be closed, read {n} bytes"),
            Err(_) => panic!("idle connection was not dropped within the timeout"),
        }

        // The endpoint still serves a real request afterwards.
        let mut client = connect_with_retry(&addr).await;
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut buf))
            .await
            .expect("read response")
            .expect("read response");
        let text = String::from_utf8_lossy(&buf);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("ossfs_reads_total"), "got: {text}");
        service.abort();
    }

    #[test]
    fn formats_prometheus_lines() {
        let body = format_prometheus(&MetricsSnapshot {
            reads: 1,
            writes: 2,
            s3_gets: 3,
            s3_heads: 4,
            s3_stat_heads: 5,
            stat_cache_hits: 7,
            stat_positive_cache_hits: 8,
            stat_negative_cache_hits: 9,
            s3_etag_heads: 6,
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
            list_throttled: 13,
            crc64_mismatches: 8,
            trash_tombstones_written: 0,
            trash_index_entries: 42,
            trash_refresh_incrementals: 0,
            trash_refresh_rebuilds: 0,
            trash_refresh_errors: 0,
            trash_start_after_ignored: 0,
            trash_bootstrap_failures: 0,
            trash_gc_etag_skips: 7,
        });
        assert!(body.contains("ossfs_reads_total 1\n"));
        assert!(body.contains("ossfs_crc64_mismatches_total 8\n"));
        assert!(
            body.contains("ossfs_trash_index_entries 42\n"),
            "gauge 无 _total 后缀:got {body}"
        );
        assert!(
            body.contains("ossfs_trash_gc_etag_skips_total 7\n"),
            "counter 带 _total 后缀:got {body}"
        );
        assert!(body.contains("ossfs_avg_upload_bytes 24.60\n"));
        assert!(body.contains("ossfs_avg_download_bytes 152.00\n"));
        assert!(body.contains("ossfs_stat_cache_hit_ratio 0.5833\n"));
        assert!(body.contains("ossfs_negative_cache_hit_ratio 0.5294\n"));
        assert!(body.contains("ossfs_read_cache_hit_ratio 0.3000\n"));
        assert!(body.contains("ossfs_disk_cache_hit_ratio 0.3500\n"));
    }
}
