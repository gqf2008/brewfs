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

/// M1 回归:非法 --date / --before(非 YYYY-MM-DD 或日期越界)必须
/// usage() 退出(exit 2 + stderr 打印 usage),绝不静默当作未提供 ——
/// 静默退化的代价是 50 万墓碑全量扫描或错误清理范围。断言 stderr 含
/// usage 文本以区分「usage 退出」与「其他 exit 2 路径」。
#[test]
fn trash_command_rejects_invalid_dates_with_usage() {
    let exe = env!("CARGO_BIN_EXE_ossmount");
    let cases: [&[&str]; 4] = [
        &["trash-restore", "docs/a.txt", "--date", "2026-13-99"],
        &["trash-restore", "docs/a.txt", "--date", "2026-6-1"],
        &["trash-clean", "--before", "garbage"],
        &["trash-clean", "--before", "2026-13-01"],
    ];
    for args in cases {
        let output = std::process::Command::new(exe)
            .args(args)
            .output()
            .expect("run ossmount");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(2),
            "非法日期必须 usage() 退出: args={args:?} stderr={stderr}"
        );
        assert!(
            stderr.contains("usage: ossmount"),
            "非法日期必须打印 usage 文本: args={args:?} stderr={stderr}"
        );
    }
}
