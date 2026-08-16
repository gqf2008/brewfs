# OSSFS Agent Guide

OSSFS mounts an S3-compatible bucket (Aliyun OSS, MinIO, AWS S3, ...) as a
local network drive with **no local metadata database**. Paths are encoded into
object keys; the bucket is the single source of truth.

Correctness, POSIX behavior, and repeatable performance evidence are
first-class requirements.

## Repository Map

- `src/ossfs/` — the object-store filesystem: `ObjectFs` (list / stat / read /
  write / rename / delete against S3), plus mount adapters:
  - `src/ossfs/winfsp.rs` — Windows WinFsp adapter
  - `src/ossfs/fuse.rs` — macOS (FUSE-T / macFUSE) / Linux (libfuse) adapter
- `src/bin/ossmount.rs` — the `ossmount` CLI (`ossmount mount --bucket ... F:`)
- `desktop/` — `ossfs-tray` (Slint system-tray manager that spawns `ossmount`)
- `desktop/installer/` — WiX Windows installer; `scripts/package_macos.sh` —
  macOS DMG packaging

## Development Discipline

- Read the relevant module and nearby tests before editing.
- Keep changes small and tied to one hypothesis.
- Use `rg`/`rg --files` for search.
- For Rust behavior changes, prefer TDD: write or identify a failing test,
  prove it fails, then make the smallest implementation change.
- Do not revert user changes or unrelated worktree changes.

## Local CI Gate For Accepted Code

Every accepted code change must pass the relevant local CI gate before commit.
At minimum, run the Rust job commands from `.github/workflows/ci.yml`:

```bash
cargo fmt --all --check
cargo check --workspace
cargo build --workspace
cargo test --workspace --lib --bins --tests
cargo clippy --workspace
git diff --check
```

Windows-specific (the OSS mount path) — mirror the CI `windows` job:

```bash
cargo check -p ossfs --no-default-features --features fuse-winfsp
cargo test -p ossfs --no-default-features --features fuse-winfsp --lib --bins --tests
cargo clippy -p ossfs --no-default-features --features fuse-winfsp --lib --bins
cargo check -p ossfs-tray --all-targets
cargo clippy -p ossfs-tray --all-targets
cargo test -p ossfs-tray
```

macOS-specific — mirror the CI `macos` job (compiles/lints the
`#[cfg(target_os = "macos")]` surface; no FUSE-T install needed to build):

```bash
cargo check --workspace
cargo clippy --workspace
cargo test --workspace --lib --bins --tests -- --test-threads=1
```

## OSSFS-Specific Guardrails

- **Metadata-less by design**: every directory enumeration / stat is a remote
  S3 request. Do not "optimize" by adding a local metadata database — that is
  the exact thing this project deliberately removed.
- **Concurrency & memory bounds**: `ObjectFs` caps in-flight S3 requests
  (`MAX_CONCURRENT_S3_REQUESTS`, default 32; `OssConfig::max_concurrent_requests`
  overrides), probes implied directories with `max_keys=1`, and bounds the
  WinFsp notify snapshot cache. Do not regress these — an I/O storm (e.g.
  `find /` recursing into the drive) must never OOM-abort the process
  (0xc0000409).
- **WinFsp thread stacks**: `build.rs` reserves a 16 MiB thread stack in the PE
  image so the deep AWS-SDK async stack cannot overflow WinFsp callback
  threads. Keep it.
- **Remote-I/O cost**: before changing read/write semantics, check
  `doc/README.md` and keep the whole-file-buffered write + close/flush push
  model.

## System Recycle Bin (issue #80)

A virtual recycle-bin view at the mount root, synthesized from the trash
tombstone index — **no new metadata database, zero data copies** (soft delete
writes one tombstone; the original object never moves). Shares the tombstone
set with `trash-list` / `trash-restore` / `trash-clean`.

| Platform | View | Default | Notes |
|---|---|---|---|
| Windows | `$Recycle.Bin` | ON with trash | Explorer's delete protocol intercepted in the `ObjectFs` layer: `$R` name recorded in the tombstone, `$I` captured byte-faithfully (≤ 4 KiB) into the tombstone body — never a real object (R8) |
| macOS | `.Trashes` | OFF — explicit `--system-trash-dir` | Finder volume trash needs macFUSE + the `local` mount option (OSSFS appends it automatically when the view is on, R12). **FUSE-T mounts as an NFS network volume: Finder trash unavailable, deletes take effect immediately** (mount-time warning) |
| Linux | `$Recycle.Bin` | ON with trash | View browsable in any file manager; no desktop-shell delete integration |

CLI switches: `--system-trash-dir NAME` (enable + override dir name),
`--system-trash-uids N[,N...]` (macOS uid filter, empty = mounting user,
R17), `--no-system-trash` (disable).

Guardrails:

- Recognition, rendering, interception and permanent delete all live in the
  `ObjectFs` layer (`trash.rs` + `mod.rs` hooks) — the FUSE/WinFsp adapters
  pass through relative view paths unchanged (R13). The only macOS-specific
  fuse.rs changes are the `local` mount option / FUSE-T warning
  (`build_config`) and the `.Trashes` mode `0700` override (getattr).
- `list`/`stat` on the system view must stay within the P1/P3 budgets: warm
  path zero extra requests, entry stat ≤ 1 body GET (stat cache absorbs it).
- Emptying the view permanently deletes only tombstone-backed entries; real
  objects under the system prefix (e.g. `.Trashes/<uid>/.DS_Store`) stay
  visible and are never touched.
- macOS `.Trashes/<uid>/` real objects (`.DS_Store`) fall back to the normal
  stat/read path when no tombstone resolves — never bail on them (unit 5).

## Known POSIX And FUSE Limitations

- Keep `generic/075` (xfstests) and LTP `iogen01` excluded from default test
  profiles: buffered FUSE mmap after truncate/extend can expose stale
  page-cache data, and the tiny-overlap direct-I/O profile has a
  split-write/page-cache coherency race. See `doc/README.md`.
- Special file types are **not supported**: the FUSE adapter only produces
  `Directory` / `RegularFile` attributes (`rdev` is always 0) and `mknod`
  rejects non-regular modes with `EPERM`. Symlinks, hard links, xattrs,
  `lseek`, `fallocate` and POSIX locks (`setlk`/`getlk`) answer `ENOSYS`
  (fuser defaults); `rmdir` recursively deletes the prefix. See
  `doc/README.md` "Known POSIX / FUSE limitations".

## Artifact Hygiene

- Preserve accepted performance artifacts referenced from `doc/`.
- Check disk before long builds: `df -h` and `du -sh target`.
