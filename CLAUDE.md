# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

OSSFS mounts an S3-compatible bucket (Aliyun OSS, MinIO, AWS S3, ...) as a
local network drive with **no local metadata database** — paths are encoded
into object keys. Correctness, POSIX behavior, and repeatable performance
evidence are first-class requirements.

`AGENTS.md` is the authoritative contributor guide (dev discipline, local CI
gate, OSSFS-specific guardrails, known POSIX/FUSE limitations). Read it before
making behavior or performance changes.

## Build, test, lint

Rust 1.85+, edition 2024. `default-members = ["."]`, so bare `cargo build` /
`cargo test` build the root `ossfs` crate — pass `--workspace` (as CI does) to
also cover `ossfs-tray`. CI sets `CARGO_INCREMENTAL=0
CARGO_PROFILE_DEV_DEBUG=0`.

```bash
cargo fmt --all --check
cargo check --workspace
cargo build --workspace
cargo test --workspace --lib --bins
cargo clippy --workspace
```

Windows (OSS mount + tray) — mirror the CI `windows` job:

```bash
cargo check -p ossfs --no-default-features --features fuse-winfsp
cargo test -p ossfs --no-default-features --features fuse-winfsp --lib --bins --tests
cargo clippy -p ossfs --no-default-features --features fuse-winfsp --lib --bins
cargo check -p ossfs-tray --all-targets
cargo clippy -p ossfs-tray --all-targets
cargo test -p ossfs-tray
```

macOS — mirror the CI `macos` job (`cargo check --workspace` /
`cargo clippy --workspace` cover the `#[cfg(target_os = "macos")]` surface;
no FUSE-T needed to build).

## Key files

- `src/ossfs/mod.rs` — `ObjectFs`: S3 list/stat/read/write/delete, concurrency
  limiter, `max_keys=1` probes, notify snapshot budget
- `src/ossfs/trash.rs` — trash (soft-delete) tombstones, index, GC, and the
  system recycle bin virtual view (`SystemTrash` / `RecycleNameIndex` / system
  entry synthesize; the `ObjectFs` hooks live in `mod.rs`)
- `src/ossfs/winfsp.rs` — Windows WinFsp adapter (sync callbacks via
  `Handle::block_on`; keep thread stacks large)
- `src/ossfs/fuse.rs` — macOS/Linux FUSE adapter
- `src/bin/ossmount.rs` — the `ossmount` CLI
- `desktop/` — `ossfs-tray` (Slint), spawns `ossmount`; no dependency on the
  `ossfs` library crate

## Caveats

- Metadata-less: every directory enumeration / stat is a remote S3 request.
  Never reintroduce a local metadata database.
- The S3 concurrency/memory bounds and the 16 MiB WinFsp thread stack are
  deliberate anti-OOM / anti-stack-overflow measures — keep them.
- The system recycle bin (issue #80) is a **virtual view** synthesized from
  the tombstone index — it must never gain its own metadata database or copy
  object data (soft delete writes one tombstone; `$I` capture bytes live in
  the tombstone body, never as bucket objects).
