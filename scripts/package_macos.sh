#!/usr/bin/env bash
# Package OSSFS for macOS: build release binaries, assemble a signed .app,
# create a Developer ID-signed DMG, and notarize + staple it.
#
# Requirements:
#   - macOS with Xcode command line tools (codesign, hdiutil, xcrun notarytool)
#   - Developer ID Application identity in the login keychain
#   - A FUSE provider for linking ossmount (pkg-config "fuse"):
#       * FUSE-T (recommended, kext-free) — local prefix pointed to by
#         FUSE_T_PREFIX (default: ~/ossfs-deps/fuse-t). See the comment near
#         the FUSE_T_PREFIX config below for how to prepare it after
#         `brew install --cask fuse-t`.
#       * macFUSE — local extracted copy pointed to by
#         MACFUSE_PREFIX (default: ~/ossfs-deps/macfuse-5.3.3)
#     Set FUSE_BACKEND=fuse-t|macfuse to force one; default is auto (fuse-t
#     wins when its prefix is present).
#   - Notarization credentials: set APPLE_ID, APPLE_TEAM_ID, APPLE_PASSWORD or
#     create ~/Documents/Apple Certificates/{app-specific-passwd.txt,team-id.txt}
#
# Usage:
#   bash scripts/package_macos.sh [--skip-notarize] [--skip-sign]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# ---- config ----
IDENTITY="${IDENTITY:-Developer ID Application: qingfeng gao (XFXU84HVK3)}"
BUNDLE_ID="${BUNDLE_ID:-ai.ossfs.tray}"
# 版本号来自 `cargo metadata`（单一受校验来源），避免对 Cargo.toml 做
# "取第一个 version =" 的脆弱文本提取（与 release-desktop.yml 同一来源）。
VERSION="${VERSION:-"$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="ossfs"))')"}"
APP_NAME="OSSFS"
MACFUSE_PREFIX="${MACFUSE_PREFIX:-$HOME/ossfs-deps/macfuse-5.3.3}"
# FUSE-T prefix layout: $FUSE_T_PREFIX/{include/fuse,lib,lib/pkgconfig}.
# Prepare it after `brew install --cask fuse-t` (installs to /usr/local) with:
#   mkdir -p ~/ossfs-deps/fuse-t/lib/pkgconfig ~/ossfs-deps/fuse-t/include
#   cp -R /usr/local/include/fuse ~/ossfs-deps/fuse-t/include/fuse
#   cp /usr/local/lib/libfuse-t-*.dylib ~/ossfs-deps/fuse-t/lib/
#   ln -s libfuse-t-*.dylib ~/ossfs-deps/fuse-t/lib/libfuse-t.dylib
#   cat > ~/ossfs-deps/fuse-t/lib/pkgconfig/fuse.pc <<'EOF'
#   prefix=$HOME/ossfs-deps/fuse-t
#   exec_prefix=${prefix}
#   libdir=${prefix}/lib
#   includedir=${prefix}/include/fuse
#   Name: fuse
#   Description: FUSE-T libfuse2-compatible shim
#   Version: 2.9.9
#   Libs: -L${libdir} -Wl,-rpath,${libdir} -lfuse-t
#   Cflags: -I${includedir}
#   EOF
FUSE_T_PREFIX="${FUSE_T_PREFIX:-$HOME/ossfs-deps/fuse-t}"
FUSE_BACKEND="${FUSE_BACKEND:-auto}"
if [[ "$FUSE_BACKEND" == "auto" && -f "$FUSE_T_PREFIX/lib/pkgconfig/fuse.pc" ]]; then
  FUSE_BACKEND=fuse-t
fi
case "$FUSE_BACKEND" in
  fuse-t)
    if [[ ! -f "$FUSE_T_PREFIX/lib/pkgconfig/fuse.pc" ]]; then
      echo "FUSE-T prefix not found: $FUSE_T_PREFIX (see instructions above)" >&2
      exit 1
    fi
    PKG_CONFIG_PATH="${FUSE_T_PREFIX}/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    ;;
  macfuse)
    PKG_CONFIG_PATH="${MACFUSE_PREFIX}/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    ;;
  *)
    echo "unknown FUSE_BACKEND: $FUSE_BACKEND (use fuse-t or macfuse)" >&2
    exit 2
    ;;
esac
export PKG_CONFIG_PATH

CERT_DIR="${CERT_DIR:-$HOME/Documents/Apple Certificates}"
APPLE_ID="${APPLE_ID:-$(cat "$CERT_DIR/apple-id.txt" 2>/dev/null || true)}"
APPLE_TEAM_ID="${APPLE_TEAM_ID:-$(cat "$CERT_DIR/team-id.txt" 2>/dev/null || true)}"
APPLE_PASSWORD="${APPLE_PASSWORD:-$(cat "$CERT_DIR/app-specific-passwd.txt" 2>/dev/null || true)}"

SKIP_NOTARIZE=0
SKIP_SIGN=0
for arg in "$@"; do
  case "$arg" in
    --skip-notarize) SKIP_NOTARIZE=1 ;;
    --skip-sign) SKIP_SIGN=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

ARCH="$(uname -m)"
STAGE="dist/macos/staging"
APP="$STAGE/$APP_NAME.app"
DMG="dist/macos/OSSFS-${VERSION}-macos-${ARCH}.dmg"
ENTITLEMENTS="dist/macos/entitlements.plist"

# ---- 0. ensure plist templates ----
mkdir -p dist/macos
if [[ ! -f dist/macos/Info.plist ]]; then
cat > dist/macos/Info.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>zh_CN</string>
    <key>CFBundleExecutable</key>
    <string>ossfs-tray</string>
    <key>CFBundleIdentifier</key>
    <string>ai.ossfs.tray</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>OSSFS</string>
    <key>CFBundleDisplayName</key>
    <string>OSSFS</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.2</string>
    <key>CFBundleVersion</key>
    <string>0.1.2</string>
    <key>CFBundleIconFile</key>
    <string>ossfs</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>LSApplicationCategoryType</key>
    <string>public.app-category.utilities</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSHumanReadableCopyright</key>
    <string>Copyright © 2026 rk8s-dev team. MIT License.</string>
</dict>
</plist>
PLIST
fi
if [[ ! -f dist/macos/entitlements.plist ]]; then
cat > dist/macos/entitlements.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.disable-library-validation</key>
    <true/>
</dict>
</plist>
PLIST
fi

# ---- 1. build ----
echo "==> Building ossmount (release)"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo build --release --bin ossmount
echo "==> Building ossfs-tray"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --release -p ossfs-tray

# ---- 2. assemble .app ----
echo "==> Assembling $APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp dist/macos/Info.plist "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleIdentifier $BUNDLE_ID" "$APP/Contents/Info.plist" 2>/dev/null || true
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$APP/Contents/Info.plist" 2>/dev/null || true
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$APP/Contents/Info.plist" 2>/dev/null || true
cp target/release/ossfs-tray "$APP/Contents/MacOS/"
cp target/release/ossmount "$APP/Contents/MacOS/"
if [[ "$FUSE_BACKEND" == "fuse-t" ]]; then
  # The build links libfuse-t via the build prefix's rpath; the distributed
  # app must look in FUSE-T's actual install location instead.
  install_name_tool -change @rpath/libfuse-t.dylib /usr/local/lib/libfuse-t.dylib \
    "$APP/Contents/MacOS/ossmount" 2>/dev/null || true
fi
# Prefer the repo's ossfs.icns (same source as the Windows .ico) so the
# macOS app icon always matches the Windows version; fall back to generating
# one from ossfs.png if it is missing.
if [[ -f desktop/assets/ossfs.icns ]]; then
  echo "==> Using desktop/assets/ossfs.icns"
  cp desktop/assets/ossfs.icns "$APP/Contents/Resources/ossfs.icns"
elif [[ ! -f "$APP/Contents/Resources/ossfs.icns" ]]; then
  echo "==> Generating icns"
  ICONSET="dist/macos/iconset.iconset"
  rm -rf "$ICONSET"
  mkdir -p "$ICONSET"
  for s in 16 32 64 128 256 512; do
    sips -z "$s" "$s" desktop/assets/ossfs.png --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  done
  for s in 32 64 128 256 512 1024; do
    h=$((s / 2))
    sips -z "$s" "$s" desktop/assets/ossfs.png --out "$ICONSET/icon_${h}x${h}@2x.png" >/dev/null
  done
  iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/ossfs.icns"
fi
chmod +x "$APP/Contents/MacOS/"*

# ---- 3. sign ----
if [[ "$SKIP_SIGN" == "1" ]]; then
  echo "==> Skipping codesign (--skip-sign); app/DMG will be unsigned"
  SKIP_NOTARIZE=1
else
  echo "==> Signing with $IDENTITY"
  # CI 用临时钥匙串签名：build 可能耗时较长，签名前再 unlock 一次并显式指定
  # --keychain，避免钥匙串自动上锁后 codesign 弹密码框挂起。
  KEYCHAIN_ARGS=()
  if [[ -n "${CODESIGN_KEYCHAIN:-}" ]]; then
    security unlock-keychain -p "${CODESIGN_KEYCHAIN_PASSWORD:-}" "$CODESIGN_KEYCHAIN"
    KEYCHAIN_ARGS=(--keychain "$CODESIGN_KEYCHAIN")
  fi
  codesign --force --options runtime --timestamp \
    --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" "${KEYCHAIN_ARGS[@]}" \
    "$APP/Contents/MacOS/ossmount"
  codesign --force --options runtime --timestamp \
    --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" "${KEYCHAIN_ARGS[@]}" \
    "$APP/Contents/MacOS/ossfs-tray"
  codesign --force --options runtime --timestamp \
    --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" "${KEYCHAIN_ARGS[@]}" "$APP"
  codesign --verify --deep --strict --verbose=2 "$APP"
fi

# ---- 4. notarize the app (zip) and staple ----
if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  if [[ -z "$APPLE_ID" || -z "$APPLE_PASSWORD" || -z "$APPLE_TEAM_ID" ]]; then
    echo "!! Notarization credentials missing (APPLE_ID / APPLE_PASSWORD / APPLE_TEAM_ID)." >&2
    echo "   App is signed but NOT notarized." >&2
    SKIP_NOTARIZE=1
  fi
fi
if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  echo "==> Notarizing app"
  ZIP="dist/macos/${APP_NAME}-app.zip"
  ditto -c -k --keepParent "$APP" "$ZIP"
  xcrun notarytool submit "$ZIP" \
    --apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$APPLE_TEAM_ID" \
    --wait
  xcrun stapler staple "$APP"
  spctl --assess --type execute --verbose=4 "$APP" || true
fi

# ---- 5. create DMG ----
# Bundle macFUSE installer + license alongside OSSFS.app when present
# (non-commercial redistribution is allowed under macFUSE's BSD-style
# license; see dist/macos/macfuse/License.rtf, condition 4).
DMG_ROOT="dist/macos/dmg-root"
rm -rf "$DMG_ROOT"
mkdir -p "$DMG_ROOT"
ditto "$APP" "$DMG_ROOT/$APP_NAME.app"
if [[ "$FUSE_BACKEND" == "fuse-t" ]]; then
  # Bundle the FUSE-T installer when the workflow provides it (brew cask
  # cache), so a freshly installed OSSFS is usable without a second download.
  # FUSE-T is free for non-commercial use; bundling with commercial software
  # needs a commercial license from the FUSE-T authors (License.txt is
  # shipped alongside, see https://github.com/macos-fuse-t/fuse-t).
  if [[ -n "${FUSE_T_PKG:-}" && -f "$FUSE_T_PKG" ]]; then
    cp "$FUSE_T_PKG" "$DMG_ROOT/fuse-t-macos-installer.pkg"
    cat > "$DMG_ROOT/FUSE-T-License.txt" <<'LICE'
FUSE-T 二进制分发许可（摘要，完整条款见 https://github.com/macos-fuse-t/fuse-t/blob/main/License.txt）：
- 非商业使用免费（BSD 风格条件）
- 捆绑商业软件需向 FUSE-T 作者获取商业许可
- 内置 LIBFUSE 库为 LGPL（fork 自 osxfuse/fuse）
OSSFS（MIT）在本 DMG 中捆绑 FUSE-T 安装包供免费分发。
LICE
    echo "==> Bundled FUSE-T installer: $FUSE_T_PKG"
  else
    # No installer available (local builds): ship install instructions.
    cat > "$DMG_ROOT/安装FUSE-T（免内核扩展）.txt" <<'EOF'
本包使用 FUSE-T 作为 macOS 挂载后端（无需内核扩展、无需修改系统安全策略）。
安装 FUSE-T（任选其一）：
  1) Homebrew:  brew install --cask fuse-t
  2) 官网下载:   https://www.fuse-t.org/  （安装 fuse-t-macos-installer-*.pkg）

安装后即可挂载 OSS 直挂盘。若系统提示"Network Volumes"访问权限，
请在 系统设置 → 隐私与安全性 → 文件与文件夹 → 网络卷宗 中允许。
EOF
  fi
elif [[ -d dist/macos/macfuse ]]; then
  cp dist/macos/macfuse/* "$DMG_ROOT/" 2>/dev/null || true
fi
# Standard drag-to-install layout: the "Applications" alias lets the user
# drop the app onto /Applications from Finder.
ln -s /Applications "$DMG_ROOT/Applications"
echo "==> Creating DMG"
hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_ROOT" -ov -format UDZO -fs HFS+ "$DMG"
if [[ "$SKIP_SIGN" == "0" ]]; then
  codesign --force --sign "$IDENTITY" "${KEYCHAIN_ARGS[@]}" "$DMG"
fi

if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  echo "==> Notarizing DMG"
  xcrun notarytool submit "$DMG" \
    --apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$APPLE_TEAM_ID" \
    --wait
  xcrun stapler staple "$DMG"
  spctl --assess --type open --context context:primary-signature --verbose=4 "$DMG" || true
fi

echo "==> Done: $DMG"
shasum -a 256 "$DMG"
