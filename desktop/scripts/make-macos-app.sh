#!/usr/bin/env bash
# Build a proper macOS .app bundle for OSSFS so the Dock/taskbar shows the
# OSSFS icon (blue cloud-download) instead of the generic tan "Unix
# executable" icon. Requires a successful `cargo build --release -p ossfs-tray`.
#
# Usage:  bash desktop/scripts/make-macos-app.sh
# Output: target/release/OSSFS.app
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
APP="$ROOT/target/release/OSSFS.app"
CONTENTS="$APP/Contents"
MACOS="$CONTENTS/MacOS"
RESOURCES="$CONTENTS/Resources"

BIN="$ROOT/target/release/ossfs-tray"
if [ ! -x "$BIN" ]; then
  echo "ossfs-tray binary not found; run first:"
  echo "  cargo build --release -p ossfs-tray"
  exit 1
fi

rm -rf "$APP"
mkdir -p "$MACOS" "$RESOURCES"

cp "$BIN" "$MACOS/ossfs-tray"

# 与 release 打包（scripts/package_macos.sh）保持一致：版本号来自
# `cargo metadata`（单一受校验来源，不再硬编码），BundleID/最低系统版本
# 对齐 release 的 ai.ossfs.tray / 12.0，避免 dev .app 与发布产物混淆。
VERSION="${VERSION:-"$(cargo metadata --no-deps --format-version 1 --manifest-path "$ROOT/Cargo.toml" | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="ossfs"))')"}"
BUNDLE_ID="${BUNDLE_ID:-ai.ossfs.tray}"

# The tray app locates ossmount next to itself; bundle it inside the .app so
# OSS direct mounts work from a double-clicked app.
OSSMNT="$ROOT/target/release/ossmount"
if [ ! -x "$OSSMNT" ]; then
  echo "ossmount binary not found; run first:"
  echo "  cargo build --release -p ossfs --bin ossmount"
  exit 1
fi
cp "$OSSMNT" "$MACOS/ossmount"
chmod +x "$MACOS/ossmount"
cp "$ROOT/desktop/assets/ossfs.icns" "$RESOURCES/ossfs.icns"

cat > "$CONTENTS/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>OSSFS</string>
  <key>CFBundleDisplayName</key><string>OSSFS</string>
  <key>CFBundleIdentifier</key><string>${BUNDLE_ID}</string>
  <key>CFBundleVersion</key><string>${VERSION}</string>
  <key>CFBundleShortVersionString</key><string>${VERSION}</string>
  <key>CFBundleExecutable</key><string>ossfs-tray</string>
  <key>CFBundleIconFile</key><string>ossfs</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

chmod +x "$MACOS/ossfs-tray"
echo "Built $APP"
echo "Launch with: open $APP"
