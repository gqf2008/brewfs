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

## Design notes

- **Metadata-less**: every directory enumeration / stat is a remote S3 request.
  Layout is s3fs-style: `/docs/report.txt` → object key `docs/report.txt`;
  directories are implicit via prefix, with a zero-byte marker object so empty
  directories survive listing.
- **Concurrency & memory bounds**: `ObjectFs` caps in-flight S3 requests
  (`MAX_CONCURRENT_S3_REQUESTS`, default 32, configurable via
  `OssConfig::max_concurrent_requests`), probes implied directories with
  `max_keys=1`, and bounds the notify snapshot cache. This prevents an I/O
  storm (e.g. `find /` recursing into the drive) from exhausting memory and
  aborting the process (0xc0000409).
- **WinFsp thread stacks**: the PE image reserves a 16 MiB thread stack so the
  deep AWS-SDK async stack cannot overflow WinFsp callback threads.
- **Writes**: whole-file buffered; pushed to the object store on close/flush.

## Operational notes

- Avoid full-disk scans over the mounted drive — every operation is a remote
  round trip.
- Runtime records: `ossmount` writes per-instance JSON under
  `%TEMP%\ossfs-oss` (used by the tray to list/stop mounts).

## Known POSIX / FUSE limitations

- `generic/075` (xfstests) and LTP `iogen01` remain excluded from default test
  profiles: buffered FUSE mmap after truncate/extend can expose stale
  page-cache data, and the tiny-overlap direct-I/O profile has a
  split-write/page-cache coherency race. Full direct I/O is not a substitute
  (mmap returns `ENODEV`).
- Special file types are **not supported**. The FUSE adapter only produces
  `Directory` / `RegularFile` attributes (`rdev` is always 0) and `mknod`
  rejects non-regular modes with `EPERM`; `DirEntry` carries no type or
  `rdev` field. FIFOs, sockets, and char/block devices cannot be created or
  represented. (Whether to persist them as object markers like the WinFsp
  side is an open decision — see the tracking issue.)
- Symlinks (`symlink`/`readlink`), hard links (`link`), extended attributes
  (`setxattr`/`getxattr`/`listxattr`/`removexattr`), `lseek` (`SEEK_DATA` /
  `SEEK_HOLE`) and `fallocate` are not implemented; the vendored fuser
  defaults answer `ENOSYS` for all of them (e.g. `ln -s` fails with `ENOSYS`,
  not `EPERM`/`ENOTSUP`).
- POSIX file locks (`setlk`/`getlk`) are not implemented and answer `ENOSYS`;
  `flush` ignores `lock_owner`. Local `flock`/`fcntl` locking still works
  inside one machine via the kernel's page-cache-level locking; there is no
  cross-machine lock coordination.
- `rmdir` recursively deletes the whole prefix under the directory (matching
  the WinFsp adapter's cleanup semantics) instead of failing with
  `ENOTEMPTY` on a non-empty directory — this is what makes `rm -rf` and
  Finder deletion work, but plain `rmdir` on a non-empty directory also
  succeeds.
