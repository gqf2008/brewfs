# OSSFS

<div align="center">
  <p><strong>OSSFS — 把 S3 兼容存储桶挂载为本地网络盘。</strong></p>
  <p>
    <a href="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml"><img src="https://github.com/gqf2008/ossfs/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/gqf2008/ossfs/releases"><img src="https://img.shields.io/github/v/release/gqf2008/ossfs" alt="Release" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" /></a>
  </p>
</div>

OSSFS 把 S3 兼容存储桶（阿里云 OSS、MinIO、AWS S3 等）直接挂载为本地文件系统，**无本地元数据库**。路径直接编码为对象键，任意多台机器挂同一桶看到同一棵树——多机"云盘"。

> OSSFS 是面向纯 OSS 网盘场景的独立项目，无元数据后端——没有 Redis / SQLx / etcd / TiKV、块缓存、压缩、控制面；桶是唯一数据源。

## 特性

- **无元数据**：桶是唯一数据源——无本地库、无需同步、任何机器可用。
- **Windows**：经 WinFsp 挂为盘符（如 `F:`）。
- **macOS**：经 FUSE-T（无需内核扩展）或 macFUSE 挂为 `/Volumes/ossfs`。
- **Linux**：经 libfuse 挂为目录。
- **托盘**（`ossfs-tray`）：添加/挂载/卸载配置、自动重启、资源管理器打开。
- **整文件缓冲写入**：写入缓冲，close/flush 时推送到对象存储（s3fs 风格）。

## 安装

### 发行版

从 [Releases](https://github.com/gqf2008/ossfs/releases) 下载：

- **Windows**：`OSSFS-Setup-<version>.exe`（安装 `ossfs-tray` + `ossmount`，内置 WinFsp）。
- **macOS**：`OSSFS-<version>.dmg`（首次挂载时若缺 FUSE-T 会自动安装）。

### 源码构建

```bash
# Windows
cargo build --release -p ossfs --bin ossmount --no-default-features --features fuse-winfsp
cargo build --release -p ossfs-tray

# macOS / Linux（需要 FUSE-T / macFUSE / libfuse 头文件）
cargo build --release -p ossfs --bin ossmount
```

## 快速开始

```bash
# 阿里云 OSS
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
ossmount mount --bucket my-bucket \
  --endpoint https://oss-cn-shanghai.aliyuncs.com \
  --region cn-shanghai F:

# MinIO（path-style）
ossmount mount --bucket my-bucket \
  --endpoint http://127.0.0.1:9000 --region us-east-1 \
  --force-path-style F:

# macOS / Linux
ossmount mount --bucket my-bucket \
  --endpoint https://oss-cn-shanghai.aliyuncs.com --region cn-shanghai \
  /Volumes/ossfs
```

或用 `ossfs-tray`：添加配置 → 填名称/盘符/Bucket/Endpoint/Region/密钥 → 保存 → 挂载。

运行 `ossmount --version` 可查看版本号、git 提交、分支、是否 dirty 与构建时间戳。

## 配置

`ossmount mount` 参数：

| 参数 | 含义 |
|---|---|
| `--config PATH` | JSON 配置文件（键为长选项名，CLI 参数覆盖文件；`access_key_id`/`secret_access_key` 写入 AWS 环境变量） |
| `--bucket` | 桶名（必填） |
| `--endpoint` | S3 兼容端点 URL（必填） |
| `--region` | 区域（默认 `us-east-1`） |
| `--prefix` | 可选对象键命名空间（如 `myns/`）；多机需一致 |
| `--force-path-style` | path-style 寻址（MinIO/自建 S3 需要） |
| `--refresh-secs N` | 目录定时刷新间隔（FUSE；0 关闭；WinFsp 固定 10s） |
| `--read-only` | 挂载级只读，拒绝写入/建目录/删除/重命名 |
| `--uid N` | 所有对象显示的属主 uid（0 = 当前挂载用户） |
| `--gid N` | 所有对象显示的属组 gid（0 = 当前挂载用户） |
| `--dir-mode M` | 目录权限位，八进制（默认 `755`） |
| `--file-mode M` | 文件权限位，八进制（默认 `644`） |
| `--allow-other` | FUSE 挂载对所有用户开放（仅 macOS/Linux） |
| `--umask M` | 在 dir/file-mode 之上额外施加的权限掩码，八进制（默认 `0`） |
| `--no-rename-dir` | 禁用目录递归重命名 |
| `--rename-dir-limit N` | 单次目录重命名最多拷贝的对象数（默认 `2000000`，`0` = 不限制） |
| `--max-concurrent-requests N` | 同时在途的 S3 请求上限（默认 `32`，`0` = 默认） |
| `--list-rate-limit R` | 目录枚举（ListObjects）速率上限，次/秒（默认 `0` = 不限） |
| `--max-upload-bytes N` | 限制同时在途的写上传字节数（`0` = 不限制） |
| `--read-ahead-bytes N` | 顺序读预取窗口字节数（默认 `8388608`，`0` = 关闭） |
| `--no-ignore-fsync` | 关闭默认的 fsync 忽略（FUSE fsync 时立即整文件 flush） |
| `--max-dirty-bytes N` | 限制聚合的整文件写缓冲脏字节数（`0` = 不限制） |
| `--credential-process CMD` | 外部凭据进程（标准 AWS credential_process JSON） |
| `--connect-timeout N` | 套接字连接超时（秒，默认 `10`，`0` = 默认值） |
| `--readwrite-timeout N` | 读超时（秒），约束单个 S3 请求含上传体的总时长（默认 `600`，`0` = 默认值） |
| `--retries N` | 首次请求后的额外重试次数（默认 SDK 默认 3 次尝试；`0` = 不重试） |
| `--no-verify-crc64` | 关闭写路径 CRC64-ECMA 完整性校验（默认开启） |
| `--content-md5` | 上传时设置 Content-MD5（跨 S3 兼容的完整性兜底） |
| `--notsup-compat-dir` | 目录列举时跳过 `_$folder$` 旧目录标记对象 |
| `--storage-class SC` | 新写对象的存储类型（如 `Standard`/`IA`/`Archive` 或 `STANDARD`/`GLACIER`） |
| `--multipart-size N` | 分片上传每片大小（默认 `8388608`，最小钳制 `5242880`；调大需同步调大 `--readwrite-timeout` —— 每片必须在读超时内传完） |
| `--multipart-concurrency N` | 单次分片上传的并发片数（默认 `4`） |
| `--disk-cache-dir PATH` | 对象区间本地磁盘缓存目录 |
| `--disk-cache-max-bytes N` | 磁盘缓存字节上限；超出后 LRU 逐出 |
| `--disk-cache-block-size N` | 磁盘缓存块大小（默认 `4194304`，`0` = 默认） |
| `--disk-cache-prefetch-blocks N` | 顺序读后台预取深度（默认 `1`，`0` = 关闭） |
| `--disk-cache-prefetch-concurrency N` | 磁盘缓存预取任务最大并发（默认 `4`） |
| `--disk-cache-verify-etag` | 服务磁盘缓存块前用 HEAD 校验对象 ETag |
| `--disk-cache-etag-ttl N` | ETag 复检 TTL（秒，默认 `10`） |
| `--disk-cache-reserve-diskfree N` | 磁盘缓存所在盘至少保留的空闲字节数 |
| `--disk-cache-free-space-ratio R` | 磁盘缓存所在盘至少保留的空闲比例 `(0,1)` |
| `--total-mem-limit N` | 总读写缓冲预算，自动派生上传/脏/读缓存上限 |
| `--total-mem-read-ratio R` | `--total-mem-limit` 中读缓存占比 `(0,1)`（默认 `0.5`） |
| `--read-cache-max-bytes N` | 内存预读缓存上限（默认 `67108864`） |
| `--stat-cache-ttl N` | 正向 stat 缓存 TTL（秒，默认 `3`） |
| `--stat-cache-max-entries N` | 正向 stat 缓存最大条目（默认 `4096`） |
| `--negative-cache-ttl N` | 负缓存 TTL（秒，默认 `5`） |
| `--negative-cache-max-entries N` | 负缓存最大条目（默认 `4096`） |
| `--metrics-listen ADDR` | 在 `ADDR` 提供 Prometheus `/metrics` |
| `--metrics-log-interval N` | 每 N 秒输出一次指标快照日志（`0` = 关闭） |
| `--log-dir PATH` | 写入按天滚动的 `ossmount.log` |
| `--log-level LEVEL` | 默认日志过滤级别（info/debug/warn）；可被 `RUST_LOG` 覆盖 |

完整模板见仓库根目录 `ossfs.example.json`（键为长选项名；布尔开关用其开关名，如 `no-verify-crc64`）。示例：

```json
{
  "mount_point": "Z:",
  "bucket": "my-bucket",
  "endpoint": "https://oss-cn-shanghai.aliyuncs.com",
  "region": "cn-shanghai",
  "read_only": false,
  "max-concurrent-requests": 64,
  "access_key_id": "AK",
  "secret_access_key": "SK"
}
```

FUSE 目录读取使用 `readdirplus`，每个目录项同时返回属性，无需额外 stat 往返。

`--config` 键速查（类型 / 默认；完整与权威以 `ossfs.example.json` 为准）：

- `mount_point`：字符串（挂载点位置参数，必填）
- `bucket`：字符串（必填）
- `endpoint`：字符串
- `region`：字符串（`us-east-1`）
- `prefix`：字符串
- `access_key_id` / `secret_access_key`：字符串（空值不覆盖环境变量）

- `uid` / `gid`：数字（`0` = 当前用户）
- `dir-mode` / `file-mode` / `umask`：八进制字符串（`0755` / `0644` / `0`）
- 布尔开关（`true` 开启，`false` 跳过）：`force-path-style`、`read-only`、`allow-other`、`no-rename-dir`、`no-ignore-fsync`、`no-verify-crc64`、`content-md5`、`notsup-compat-dir`、`disk-cache-verify-etag`
- `rename-dir-limit` / `max-upload-bytes` / `max-dirty-bytes` / `max-concurrent-requests` / `read-ahead-bytes` / `multipart-size` / `multipart-concurrency`：数字
- `list-rate-limit`：数字，次/秒（`0` = 不限）
- `storage-class` / `credential-process`：字符串
- `connect-timeout` / `readwrite-timeout`：数字（`0` = 默认值 `10` / `600`）；`retries`：数字（`0` = 不重试）
- 请求超时**不可禁用**——允许永久挂起的请求会冻结写入路径（复制冻结、上传静默丢失）；需近似旧的无限等待行为时可设一个足够大的值（如 `86400`）

- 缓存：`stat-cache-ttl`（`3`）、`stat-cache-max-entries`（`4096`）、`negative-cache-ttl`（`5`）、`negative-cache-max-entries`（`4096`）、`read-cache-max-bytes`（`67108864`）、`total-mem-limit`（`0`）、`total-mem-read-ratio`（`0.5`）
- 磁盘缓存：`disk-cache-dir`、`disk-cache-max-bytes`、`disk-cache-block-size`、`disk-cache-prefetch-blocks`、`disk-cache-prefetch-concurrency`、`disk-cache-etag-ttl`、`disk-cache-reserve-diskfree`、`disk-cache-free-space-ratio`
- 日志/指标：`log-dir`、`log-level`、`metrics-listen`、`metrics-log-interval`



凭据来自环境变量（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）或 AWS 共享配置；托盘会把密钥注入其拉起的 `ossmount` 进程。

## 一致性

弱一致：无锁、无原子 rename；文件在 close/flush 时整文件写入。这是"云盘"，不是多写者 POSIX 文件系统——不要用作数据库后端或多人并发编辑同一文件。

## 系统回收站

挂载根下的**虚拟**回收站视图（issue #80）：条目由回收站墓碑索引合成，**无本地元数据库、零数据复制**——软删进回收站只写一条墓碑对象（原对象从未移动）。视图与 CLI 回收站命令（`trash-list` / `trash-restore` / `trash-clean`）共享同一墓碑集。

| 平台 | 视图 | 默认 | 说明 |
|---|---|---|---|
| Windows | `$Recycle.Bin` | 随回收站**默认开启** | Explorer 删除协议在 ObjectFs 层拦截：`$R` 名记录进墓碑，`$I` 元数据文件按字节捕获（上限 4 KiB，存墓碑 body——**不落真实桶对象**）。无需任何 shell 侧集成。 |
| macOS | `.Trashes` | **默认关闭**——需 `--system-trash-dir` 显式开启 | Finder 卷级废纸篓仅在 **macFUSE** + `local` 挂载选项下激活（视图开启时 OSSFS 自动追加）。**FUSE-T** 挂载为 NFS 网络卷：**Finder 废纸篓不可用，删除立即生效**（挂载时告警）。`.Trashes` / `.Trashes/<uid>` 以 mode `0700` 呈现。 |
| Linux | `$Recycle.Bin` | 随回收站**默认开启** | 视图可在任意文件管理器浏览；无桌面 shell 删除集成（视图本身 rename/delete 照常可用——就是普通目录视图）。 |

CLI 开关（同 `ossmount --help`）：

- `--system-trash-dir NAME` — 开启视图；`NAME` 覆盖任意平台的目录名（默认：Windows/Linux `$Recycle.Bin`，macOS `.Trashes`）。
- `--system-trash-uids N[,N...]` — 仅 macOS：只渲染 `.Trashes` 下这些 uid 目录（默认：挂载用户 uid）。
- `--no-system-trash` — 任意平台显式关闭视图。

已知限制：

- 视图由墓碑索引渲染，远端删除最长需一个刷新周期（约 30s；`--trash-refresh-mode eager` 每次 list/stat 前轮询）才出现/消失。
- 目录下每个名字最多一个条目：不同目录的同名墓碑合并为单个视图条目；读取/还原取**最新**墓碑（带告警）。
- 重复删除同一路径时，视图只显示最新版本；旧版本仍可经 `trash-restore --date` 访问。
- 系统前缀下的真实对象（如 `.Trashes/<uid>/.DS_Store`）保持可见，清空视图时**绝不触碰**——只有墓碑支撑的条目被永久删除。
- 深于 `$Recycle.Bin/<sid>/<name>` 的路径不拦截；这些 key 上的真实桶数据原样可见。
- 终端手动 mv 进 `.Trashes` 同样是软删——与真实 macOS 行为一致。
- **Windows 侧目前为代码级验证**:拦截语义由单测与 WinFsp 构建门禁保障;Explorer 真实协议行为(探测序列、`$I`/`$R` 交互)仍需真实 Windows 挂载 + ProcMon 抓包确认,首次 Windows 发布前必做。

## 运维注意

- 每个目录枚举/stat 都是一次**远程 S3 请求**；避免对挂载盘做全盘扫描（`find /`）。
- `ObjectFs` 限制在途 S3 请求（默认 32）与内存，防止 I/O 风暴 OOM 崩溃；WinFsp 镜像保留 16 MiB 线程栈。详见 [doc/README.md](doc/README.md)。

## 开发

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --lib --bins --tests
cargo clippy --workspace
```

贡献指南见 [AGENTS.md](AGENTS.md)；设计与限制见 [doc/README.md](doc/README.md)。

## License

MIT — 见 [LICENSE](LICENSE)。
