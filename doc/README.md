# OSSFS Documentation

OSSFS mounts an S3-compatible bucket as a local filesystem with no local
metadata database (s3fs-style layout).

## Components

- `src/ossfs/` — the object-store filesystem: `ObjectFs` (list / stat / read /
  write / rename / delete against S3), plus the mount adapters:
  - `src/ossfs/winfsp.rs` — Windows WinFsp adapter (`ossmount` on Windows)
  - `src/ossfs/fuse.rs` — macOS / Linux FUSE adapter (FUSE-T / macFUSE / libfuse)
- `src/bin/ossmount.rs` — the `ossmount` CLI
- `desktop/` — `ossfs-tray` system-tray manager

## Operational notes

- **No local metadata database**: every directory enumeration / stat is a
  remote S3 request. Avoid full-disk scans over the mounted drive.
- **Concurrency & memory bounds**: `ObjectFs` caps in-flight S3 requests
  (`MAX_CONCURRENT_S3_REQUESTS`, default 32, configurable via
  `OssConfig::max_concurrent_requests`), probes implied directories with
  `max_keys=1`, and bounds the notify snapshot cache. This prevents an I/O
  storm (e.g. `find /` recursing into the drive) from exhausting memory and
  aborting the process (0xc0000409).
- **WinFsp thread stacks**: the PE image reserves a 16 MiB thread stack so the
  deep AWS-SDK async stack cannot overflow WinFsp callback threads.
- Runtime records: `ossmount` writes per-instance JSON under `%TEMP%\brewfs-oss`.

## Known limitations

- Weak consistency (no locks / no atomic rename); suitable for single-writer or
  multi-machine "cloud drive" usage.
- `generic/075` (xfstests) and LTP `iogen01` remain excluded from default test
  profiles due to buffered-FUSE page-cache races.
