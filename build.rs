use std::env;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn brewfs_git_env() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());

    emit_git_rerun_hints(&manifest_dir);

    let commit =
        git_output(&manifest_dir, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let short_commit = git_output(&manifest_dir, &["rev-parse", "--short=12", "HEAD"])
        .unwrap_or_else(|| {
            commit
                .chars()
                .take(12)
                .collect::<String>()
                .if_empty("unknown")
        });
    let branch = git_output(&manifest_dir, &["branch", "--show-current"]).unwrap_or_else(|| {
        git_output(&manifest_dir, &["rev-parse", "--abbrev-ref", "HEAD"])
            .unwrap_or_else(|| "unknown".into())
    });
    let dirty = git_output(&manifest_dir, &["status", "--porcelain"])
        .map(|status| if status.is_empty() { "false" } else { "true" })
        .unwrap_or("unknown");
    let build_timestamp = build_timestamp(&manifest_dir);

    println!("cargo:rustc-env=BREWFS_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=BREWFS_GIT_COMMIT_SHORT={short_commit}");
    println!("cargo:rustc-env=BREWFS_GIT_BRANCH={branch}");
    println!("cargo:rustc-env=BREWFS_GIT_DIRTY={dirty}");
    println!("cargo:rustc-env=BREWFS_BUILD_TIMESTAMP={build_timestamp}");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}

fn git_output(cwd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn emit_git_rerun_hints(manifest_dir: &str) {
    if let Some(head_path) = git_path(manifest_dir, "HEAD") {
        println!("cargo:rerun-if-changed={head_path}");
    }

    if let Some(ref_name) = git_output(manifest_dir, &["symbolic-ref", "-q", "HEAD"])
        && let Some(ref_path) = git_path(manifest_dir, &ref_name)
    {
        println!("cargo:rerun-if-changed={ref_path}");
    }

    if let Some(index_path) = git_path(manifest_dir, "index") {
        println!("cargo:rerun-if-changed={index_path}");
    }
}

fn git_path(manifest_dir: &str, path: &str) -> Option<String> {
    git_output(manifest_dir, &["rev-parse", "--git-path", path])
}

fn build_timestamp(manifest_dir: &str) -> String {
    if let Ok(epoch) = env::var("SOURCE_DATE_EPOCH") {
        return format!("unix:{epoch}");
    }

    if let Some(commit_epoch) = git_output(manifest_dir, &["show", "-s", "--format=%ct", "HEAD"]) {
        return format!("unix:{commit_epoch}");
    }

    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format!("unix:{seconds}")
}

trait EmptyFallback {
    fn if_empty(self, fallback: &str) -> String;
}

impl EmptyFallback for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

// Windows-only: emit DELAYLOAD linker flags for the WinFsp user-mode library.
// winfsp-sys ships a built-in import library, so no WinFsp SDK is required to
// compile; only the final binary needs WinFsp installed at runtime.
#[cfg(windows)]
fn winfsp_delayload() {
    winfsp::build::winfsp_link_delayload();
}

/// Windows: widen the thread stack reserve of the final PE image.
///
/// WinFsp creates its host/worker threads with the process default stack
/// size (`CreateThread` with `dwStackSize = 0`), i.e. the PE
/// `SizeOfStackReserve` of the executable. ossmount drives AWS SDK futures
/// (hyper + rustls) through `Handle::block_on` on those threads; the deep
/// async stack can exhaust the linker default 1 MiB reserve under heavy
/// concurrent I/O, and Rust then aborts the whole process with
/// 0xc0000409 (FAST_FAIL_FATAL_APP_EXIT). Reserve 16 MiB (commit 1 MiB)
/// so every WinFsp callback thread inherits a safe stack.
#[cfg(windows)]
fn widen_thread_stack() {
    match env::var("CARGO_CFG_TARGET_ENV").as_deref() {
        Ok("msvc") => println!("cargo:rustc-link-arg=/STACK:16777216,1048576"),
        Ok("gnu") => println!("cargo:rustc-link-arg=-Wl,--stack,16777216"),
        _ => {}
    }
}

fn main() {
    brewfs_git_env();
    #[cfg(windows)]
    winfsp_delayload();
    #[cfg(windows)]
    widen_thread_stack();
}
