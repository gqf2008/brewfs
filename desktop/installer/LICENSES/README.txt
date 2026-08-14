第三方组件许可归属（Third-party licenses）
==========================================

本目录随 OSSFS 安装包一起再分发的第三方组件的许可文本。

WinFsp
------
- 组件：WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos
- 再分发形式：未经修改的官方安装器 MSI（winfsp-2.1.25156.msi），由 OSSFS
  安装程序（WiX Burn bundle）在目标机未安装 WinFsp 内核服务时链式安装；
  卸载 OSSFS 时保留（共享系统组件）。
- 许可：GPLv3，附 FLOSS 特别例外（允许再分发未修改的官方安装器二进制，
  前提是被分发的软件本身满足自由软件/开源定义，并保留版权声明与仓库链接）。
  完整许可文本见同目录 `WinFsp-License.txt`。
- 源代码可得性：https://github.com/winfsp/winfsp
  （对应版本 tag 可在该仓库 Releases 中找到；如无法获取，可联系
  OSSFS 维护者免费提供对应完整源码副本。）
- 官方发布页：https://github.com/winfsp/winfsp/releases
