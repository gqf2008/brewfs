OSSFS 托盘（Windows）
======================

把阿里云 OSS / S3 兼容 bucket 直接挂载成 Windows 盘符（多机共享网盘，
无本地元数据）。

- ossfs-tray.exe   系统托盘管理界面（配置、挂载/卸载）
- ossmount.exe      底层挂载进程（WinFsp 后端）
- LICENSES\         第三方组件许可归属（当前为 WinFsp 的 GPLv3 许可文本）

使用前请先安装 WinFsp（本安装包会自动安装）。macOS/Linux 请直接使用
ossmount：macOS 推荐 FUSE-T（免内核扩展，DMG 版会在首次挂载时自动安装），
也可用 macFUSE；Linux 需要 libfuse。

仓库：https://github.com/gqf2008/ossfs

第三方组件
----------

本安装包在未检测到 WinFsp 内核服务时，会自动链式安装随包再分发的
WinFsp 官方安装器（未经修改的 winfsp-2.1.25156.msi）；卸载 OSSFS 时
保留 WinFsp。

WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos.
WinFsp 以 GPLv3（附 FLOSS 特别例外）许可发布；完整许可文本与源代码
可得性说明见安装目录下的 LICENSES\WinFsp-License.txt 与 LICENSES\README.txt，
源代码仓库：https://github.com/winfsp/winfsp

凭据存储说明
------------

托盘应用保存的 S3 AccessKey/SecretKey 存放在当前用户的系统安全凭据
存储（Windows 凭据管理器，服务名 `ossfs-tray`）中；profiles.json 仅保留
非敏感配置与凭据引用。挂载时凭据经环境变量传给 ossmount 子进程，
同用户的其他进程可见（操作系统层面的已知权衡）。
