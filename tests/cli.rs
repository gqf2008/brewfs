//! Integration tests for the `ossmount` CLI. These run the real binary so a
//! config/CLI regression (e.g. an option rejecting `0`) is caught before it
//! ships.

#[test]
fn example_config_parses_without_error() {
    let exe = env!("CARGO_BIN_EXE_ossmount");
    let manifest = env!("CARGO_MANIFEST_DIR");
    let output = std::process::Command::new(exe)
        .args([
            "mount",
            "--config",
            &format!("{manifest}/ossfs.example.json"),
            "--version",
        ])
        .output()
        .expect("run ossmount");
    assert!(
        output.status.success(),
        "ossmount must parse ossfs.example.json and print --version:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("ossfs"),
        "unexpected --version output: {stdout}"
    );
}
