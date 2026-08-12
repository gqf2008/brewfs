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

> This project was forked from [brewfs](https://github.com/brewfs/brewfs) and
> slimmed down to the OSS network-drive use case. All metadata-backend code
> (Redis / SQLx / etcd / TiKV, chunk cache, compaction, control plane) has been
> removed.

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
region / access key → *Save* → *Mount*.

## Configuration

`ossmount mount` options:

| Option | Meaning |
|---|---|
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
| `--no-rename-dir` | Disable recursive directory rename |
| `--rename-dir-limit N` | Max objects copied by one directory rename (default `2000000`, `0` = unlimited) |
| `--max-upload-bytes N` | Cap aggregate in-flight write bytes (`0` = unlimited) |

Credentials come from the environment (`AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`) or the AWS shared config. The tray injects them into
the `ossmount` process it spawns.

## Consistency model

Weak consistency — no locks, no atomic rename. Files are written whole-file on
close/flush. This is a **cloud drive**, not a multi-writer POSIX filesystem;
do not use it as a database backend or for concurrent editors on the same file.

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
cargo test --workspace --lib --bins
cargo clippy --workspace
```

See [AGENTS.md](AGENTS.md) for the full contribution guide and
[doc/README.md](doc/README.md) for design / limitations.

## License

MIT — see [LICENSE](LICENSE).
