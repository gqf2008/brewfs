fn main() {
    slint_build::compile("ui/ossfs_tray.slint").expect("failed to compile Slint UI");

    // Windows: embed the application icon into the .exe so Explorer, the
    // taskbar and Alt-Tab all show the OSSFS icon. macOS uses the .icns in
    // the app bundle (and the runtime window icon set by Slint).
    #[cfg(windows)]
    {
        println!("cargo:rerun-if-changed=assets/ossfs.ico");
        embed_resource::compile("app.rc", embed_resource::NONE);
    }
}
