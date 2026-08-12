<div align="center">
  <img src="doc/assets/brewfs.png" alt="OSSFS" width="366" height="167" />
  <p><strong>OSSFS: mount an S3-compatible bucket as a local network drive.</strong></p>

  <p>
    <a href="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml"><img src="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/gqf2008/ossfs/releases"><img src="https://img.shields.io/github/v/release/gqf2008/ossfs" alt="Release" /></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" /></a>
  </p>
  <p>
    <a href="#quick-start">Install</a> ·
    <a href="doc/README.md">Documentation</a>
  </p>
</div>

OSSFS is a **metadata-less cloud drive**: it mounts an S3-compatible bucket
(Aliyun OSS, MinIO, AWS S3, ...) as a local filesystem with **no local metadata
database**. Paths are encoded directly into object keys, so any number of
machines can mount the same bucket and see the same tree.

- Windows: WinFsp, mounts as a drive letter (e.g. `F:`)
- macOS: FUSE-T / macFUSE, mounts as `/Volumes/ossfs`
- Linux: libfuse, mounts as a directory

## Quick Start

```bash
# Windows (installer) or:
cargo build --release -p ossfs --bin ossmount --no-default-features --features fuse-winfsp
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  target/release/ossmount mount --bucket BUCKET --endpoint https://oss-cn-shanghai.aliyuncs.com --region cn-shanghai F:
```

Or use the `ossfs-tray` desktop app to add / mount / unmount profiles from the
system tray.

## Consistency model

Weak consistency: no locks, no atomic rename. The bucket is the single source
of truth — this is a "cloud drive", not a multi-writer POSIX filesystem. Writes
are buffered whole-file and pushed to the object store on close/flush.

## Documentation

See [doc/README.md](doc/README.md) for design details, known limitations and
performance notes.
