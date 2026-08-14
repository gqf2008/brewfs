OSSFS 托盘（Windows）
======================

把阿里云 OSS / S3 兼容 bucket 直接挂载成 Windows 盘符（多机共享网盘，
无本地元数据）。

- ossfs-tray.exe   系统托盘管理界面（配置、挂载/卸载）
- ossmount.exe      底层挂载进程（WinFsp 后端）

使用前请先安装 WinFsp（本安装包会自动安装）。macOS/Linux 请直接使用
ossmount：macOS 推荐 FUSE-T（免内核扩展，DMG 版会在首次挂载时自动安装），
也可用 macFUSE；Linux 需要 libfuse。

仓库：https://github.com/ossfs/ossfs