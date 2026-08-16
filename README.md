# OSSFS

<div align="center">
  <p><strong>OSSFS — mount an S3-compatible bucket as a local network drive.</strong></p>
  <p>
    <a href="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml"><img src="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/gqf2008/ossfs/releases"><img src="https://img.shields.io/github/v/release/gqf2008/ossfs" alt="Release" /></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" /></a>
  </p>
</div>

OSSFS mounts an S3-compatible bucket (Aliyun OSS, MinIO, AWS S3, ...) as a
local filesystem with **no local metadata database**. Paths are encoded
directly into object keys, so any number of machines can mount the same bucket
and see the same tree — a multi-machine "cloud drive".

> OSSFS is a standalone project for the OSS network-drive use case. It has no
> metadata backend — no Redis / SQLx / etcd / TiKV, no chunk cache, no
> compaction, no control plane — the bucket is the only source of truth.

## Features

- **Metadata-less**: the bucket is the single source of truth — no local DB,
  no sync, works from any machine.
- **Windows**: mounts as a drive letter (`F:`) via WinFsp.
- **macOS**: mounts as `/Volumes/ossfs` via FUSE-T (no kernel extension) or
  macFUSE.
- **Linux**: mounts as a directory via libfuse.
- **System tray** (`ossfs-tray`): add / mount / unmount profiles, auto-restart,
  open in Explorer.
- **Whole-file buffered writes**: writes are buffered and pushed to the object
  store on close/flush (s3fs-style).

## Install

### From a release

Download the installer / DMG from the
[Releases](https://github.com/gqf2008/ossfs/releases) page:

- **Windows**: `OSSFS-Setup-<version>.exe` (installs `ossfs-tray` +
  `ossmount`, bundles WinFsp).
- **macOS**: `OSSFS-<version>.dmg` (FUSE-T is installed on first mount if
  missing).

### From source

```bash
# Windows
cargo build --release -p ossfs --bin ossmount --no-default-features --features fuse-winfsp
cargo build --release -p ossfs-tray

# macOS / Linux (needs FUSE-T / macFUSE / libfuse headers)
cargo build --release -p ossfs --bin ossmount
```

## Quick Start

```bash
# Aliyun OSS
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
ossmount mount --bucket my-bucket \
  --endpoint https://oss-cn-shanghai.aliyuncs.com \
  --region cn-shanghai F:

# MinIO (path-style)
ossmount mount --bucket my-bucket \
  --endpoint http://127.0.0.1:9000 --region us-east-1 \
  --force-path-style F:

# macOS / Linux
ossmount mount --bucket my-bucket \
  --endpoint https://oss-cn-shanghai.aliyuncs.com --region cn-shanghai \
  /Volumes/ossfs
```

Or use `ossfs-tray`: *Add config* → fill in name / drive / bucket / endpoint /
region / access key → *Save* → *Mount*. Run `ossmount --version` to print the
version, git commit, branch, dirty flag, and build timestamp.

## Configuration

`ossmount mount` options:

| Option | Meaning |
|---|---|
| `--config PATH` | JSON config file (keys are long option names; CLI args override file values; `access_key_id`/`secret_access_key` set AWS env creds) |
| `--bucket` | Bucket name (required) |
| `--endpoint` | S3-compatible endpoint URL (required) |
| `--region` | Region (default `us-east-1`) |
| `--prefix` | Optional object-key namespace (e.g. `myns/`); keep consistent across machines |
| `--force-path-style` | Use path-style addressing (MinIO / self-hosted S3) |
| `--refresh-secs N` | Periodic directory refresh interval (FUSE; 0 disables; WinFsp fixed at 10s) |
| `--read-only` | Reject all write/mkdir/delete/rename at mount level |
| `--uid N` | Owner uid shown on every object (0 = mounting user) |
| `--gid N` | Owner gid shown on every object (0 = mounting user) |
| `--dir-mode M` | Directory permission bits, octal (default `755`) |
| `--file-mode M` | File permission bits, octal (default `644`) |
| `--allow-other` | Open the FUSE mount to all users (macOS/Linux only) |
| `--umask M` | Extra permission mask applied on top of dir/file-mode, octal (default `0`) |
| `--no-rename-dir` | Disable recursive directory rename |
| `--rename-dir-limit N` | Max objects copied by one directory rename (default `2000000`, `0` = unlimited) |
| `--max-concurrent-requests N` | Cap on in-flight S3 requests (default `32`, `0` = default) |
| `--list-rate-limit R` | Directory-enumeration (ListObjects) rate cap, calls/sec (default `0` = unlimited) |
| `--max-upload-bytes N` | Cap aggregate in-flight write bytes (`0` = unlimited) |
| `--read-ahead-bytes N` | Sequential-read prefetch window, bytes (default `8388608`, `0` = off) |
| `--no-ignore-fsync` | Disable the default fsync ignore (flush whole-file buffer on FUSE fsync) |
| `--max-dirty-bytes N` | Cap aggregate dirty whole-file write buffers (`0` = unlimited) |
| `--credential-process CMD` | External credential process (standard AWS credential_process JSON) |
| `--connect-timeout N` | Socket connect timeout in seconds (default `10`, `0` = default) |
| `--readwrite-timeout N` | Read timeout in seconds, bounds each S3 request incl. its upload body (default `600`, `0` = default) |
| `--retries N` | Additional retry attempts after the initial request (default: SDK default 3 attempts; `0` = no retry) |
| `--no-verify-crc64` | Disable write-path CRC64-ECMA integrity verification (default on) |
| `--content-md5` | Set Content-MD5 on uploads (cross-S3-compatible integrity fallback) |
| `--notsup-compat-dir` | Skip legacy `_$folder$` directory-marker objects in listings |
| `--storage-class SC` | Storage class for newly written objects (e.g. `Standard`/`IA`/`Archive` or `STANDARD`/`GLACIER`) |
| `--multipart-size N` | Multipart part size, bytes (default `8388608`, clamped to `5242880` minimum; raising it requires co-raising `--readwrite-timeout` — each part must upload within the read timeout) |
| `--multipart-concurrency N` | Concurrent part uploads per multipart write (default `4`) |
| `--disk-cache-dir PATH` | Local disk cache directory for object-range blocks |
| `--disk-cache-max-bytes N` | Disk cache byte budget; evicts LRU blocks when exceeded |
| `--disk-cache-block-size N` | Disk-cache block size, bytes (default `4194304`, `0` = default) |
| `--disk-cache-prefetch-blocks N` | Sequential read background prefetch depth (default `1`, `0` = off) |
| `--disk-cache-prefetch-concurrency N` | Max concurrent disk-cache prefetch tasks (default `4`) |
| `--disk-cache-verify-etag` | Verify object ETag with a HEAD before serving disk-cache blocks |
| `--disk-cache-etag-ttl N` | ETag re-check TTL in seconds (default `10`) |
| `--disk-cache-reserve-diskfree N` | Keep at least this many bytes free on the cache filesystem |
| `--disk-cache-free-space-ratio R` | Keep at least this fraction `(0,1)` of the cache filesystem free |
| `--total-mem-limit N` | Total read/write buffer budget; derives upload/dirty/read-cache limits |
| `--total-mem-read-ratio R` | Fraction of `--total-mem-limit` reserved for read cache, `(0,1)` (default `0.5`) |
| `--read-cache-max-bytes N` | In-memory read-ahead cache cap, bytes (default `67108864`) |
| `--stat-cache-ttl N` | Positive stat cache TTL in seconds (default `3`) |
| `--stat-cache-max-entries N` | Max positive stat cache entries (default `4096`) |
| `--negative-cache-ttl N` | Negative stat cache TTL in seconds (default `5`) |
| `--negative-cache-max-entries N` | Max negative stat cache entries (default `4096`) |
| `--metrics-listen ADDR` | Serve Prometheus `/metrics` on `ADDR` |
| `--metrics-log-interval N` | Emit a metrics snapshot to the log every N seconds (`0` = off) |
| `--log-dir PATH` | Write daily-rotating `ossmount.log` to PATH |
| `--log-level LEVEL` | Default tracing filter (info/debug/warn); overridable by `RUST_LOG` |

See `ossfs.example.json` in the repo root for a full template (keys are long option names; boolean switches use their switch name, e.g. `no-verify-crc64`). Example:

```json
{
  "mount_point": "Z:",
  "bucket": "my-bucket",
  "endpoint": "https://oss-cn-shanghai.aliyuncs.com",
  "region": "cn-shanghai",
  "read_only": false,
  "max-concurrent-requests": 64,
  "access_key_id": "AK",
  "secret_access_key": "SK"
}
```

FUSE directory reads use `readdirplus`, so each directory entry also returns its
attributes without extra stat round trips.

`--config` key reference (type / default; the authoritative list is `ossfs.example.json`):

- `mount_point`: string (mount-point positional, required)
- `bucket`: string (required)
- `endpoint`: string
- `region`: string (`us-east-1`)
- `prefix`: string
- `access_key_id` / `secret_access_key`: string (empty does not override env)

- `uid` / `gid`: number (`0` = current user)
- `dir-mode` / `file-mode` / `umask`: octal string (`0755` / `0644` / `0`)
- Boolean switches (`true` enables, `false` skips): `force-path-style`, `read-only`, `allow-other`, `no-rename-dir`, `no-ignore-fsync`, `no-verify-crc64`, `content-md5`, `notsup-compat-dir`, `disk-cache-verify-etag`
- `rename-dir-limit` / `max-upload-bytes` / `max-dirty-bytes` / `max-concurrent-requests` / `read-ahead-bytes` / `multipart-size` / `multipart-concurrency`: number
- `list-rate-limit`: number, calls/sec (`0` = unlimited)
- `storage-class` / `credential-process`: string
- `connect-timeout` / `readwrite-timeout`: number (`0` = default `10` / `600`); `retries`: number (`0` = no retry)
- Request timeouts cannot be disabled — a request that may hang forever can wedge the write path (frozen copies, silent upload loss). To approximate the old unbounded behavior, set a very large value (e.g. `86400`)

- Caches: `stat-cache-ttl` (`3`), `stat-cache-max-entries` (`4096`), `negative-cache-ttl` (`5`), `negative-cache-max-entries` (`4096`), `read-cache-max-bytes` (`67108864`), `total-mem-limit` (`0`), `total-mem-read-ratio` (`0.5`)
- Disk cache: `disk-cache-dir`, `disk-cache-max-bytes`, `disk-cache-block-size`, `disk-cache-prefetch-blocks`, `disk-cache-prefetch-concurrency`, `disk-cache-etag-ttl`, `disk-cache-reserve-diskfree`, `disk-cache-free-space-ratio`
- Logging/metrics: `log-dir`, `log-level`, `metrics-listen`, `metrics-log-interval`

Credentials come from the environment (`AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`) or the AWS shared config. The tray injects them into
the `ossmount` process it spawns.

## Consistency model

Weak consistency — no locks, no atomic rename. Files are written whole-file on
close/flush. This is a **cloud drive**, not a multi-writer POSIX filesystem;
do not use it as a database backend or for concurrent editors on the same file.

## System recycle bin

A **virtual** recycle-bin view at the mount root (issue #80): entries are
synthesized from the trash tombstone index, with **no local metadata database
and zero data copies** — soft-deleting into the bin only writes one tombstone
object (the original object never moves). The view shares the same tombstone
set as the CLI trash commands (`trash-list` / `trash-restore` / `trash-clean`).

| Platform | View | Default | Notes |
|---|---|---|---|
| Windows | `$Recycle.Bin` | **ON** with trash | Explorer's delete protocol is intercepted in the `ObjectFs` layer: the `$R` name is recorded in the tombstone and the `$I` metadata file is captured byte-faithfully (up to 4 KiB, stored in the tombstone body — never a real bucket object). No shell-side integration needed. |
| macOS | `.Trashes` | **OFF** — enable with `--system-trash-dir` | Finder's volume trash only activates with **macFUSE** and the `local` mount option (OSSFS appends it automatically when the view is on). With **FUSE-T** the volume mounts as an NFS network volume: **Finder trash is unavailable and deletes take effect immediately** (a warning is logged at mount). `.Trashes` / `.Trashes/<uid>` are presented with mode `0700`. |
| Linux | `$Recycle.Bin` | **ON** with trash | The view is browsable in any file manager; there is no desktop-shell delete integration (the view itself still works for rename/delete — it is a normal directory). |

CLI switches (also in `ossmount --help`):

- `--system-trash-dir NAME` — enable the view; `NAME` overrides the directory
  name on any platform (defaults: `$Recycle.Bin` on Windows/Linux,
  `.Trashes` on macOS).
- `--system-trash-uids N[,N...]` — macOS only: render only these uid
  directories under `.Trashes` (default: the mounting user's uid).
- `--no-system-trash` — disable the view on any platform.

Known limitations:

- The view renders from the tombstone index, so a deletion made on another
  machine can take up to one refresh cycle (~30 s; `--trash-refresh-mode eager`
  polls before every list/stat) to appear or disappear.
- A directory shows at most one entry per name: same-named tombstones from
  different directories collapse into a single view entry, and reading or
  restoring it resolves the **newest** tombstone (with a warning).
- Only the newest version of a re-deleted path is shown in the view; older
  versions remain accessible via `trash-restore --date`.
- Real objects under the system prefix (e.g. Finder's `.DS_Store` under
  `.Trashes/<uid>/`) stay visible and are **never** touched when emptying the
  view — only tombstone-backed entries are permanently deleted.
- Paths deeper than `$Recycle.Bin/<sid>/<name>` are not intercepted; real
  bucket data at those keys is shown as-is.
- Moving a file into `.Trashes` from a terminal also soft-deletes it — the
  same behavior as real macOS.

## Operational notes

- Every directory enumeration / stat is a **remote S3 request**. Avoid
  full-disk scans (`find /`) over the mounted drive.
- `ObjectFs` bounds in-flight S3 requests (default 32) and memory so an I/O
  storm cannot OOM-abort the process; the WinFsp image reserves a 16 MiB
  thread stack. See [doc/README.md](doc/README.md).

## Development

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --lib --bins --tests
cargo clippy --workspace
```

See [AGENTS.md](AGENTS.md) for the full contribution guide and
[doc/README.md](doc/README.md) for design / limitations.

## License

MIT — see [LICENSE](LICENSE).
