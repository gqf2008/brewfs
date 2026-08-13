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

> 本项目由 [brewfs](https://github.com/brewfs/brewfs) 分叉而来，精简为纯 OSS 网盘场景；已删除全部元数据后端代码（Redis / SQLx / etcd / TiKV、块缓存、压缩、控制面）。

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

运行 `ossmount --version` 可查看版本号。

## 配置

`ossmount mount` 参数：

| 参数 | 含义 |
|---|---|
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
| `--no-rename-dir` | 禁用目录递归重命名 |
| `--rename-dir-limit N` | 单次目录重命名最多拷贝的对象数（默认 `2000000`，`0` = 不限制） |
| `--max-upload-bytes N` | 限制同时在途的写上传字节数（`0` = 不限制） |
| `--read-ahead-bytes N` | 顺序读预取窗口字节数（默认 `8388608`，`0` = 关闭） |
| `--no-ignore-fsync` | 关闭默认的 fsync 忽略（FUSE fsync 时立即整文件 flush） |
| `--max-dirty-bytes N` | 限制聚合的整文件写缓冲脏字节数（`0` = 不限制） |
| `--credential-process CMD` | 外部凭据进程（标准 AWS credential_process JSON） |
| `--no-verify-crc64` | 关闭写路径 CRC64-ECMA 完整性校验（默认开启） |
| `--content-md5` | 上传时设置 Content-MD5（跨 S3 兼容的完整性兜底） |
| `--storage-class SC` | 新写对象的存储类型（如 `Standard`/`IA`/`Archive` 或 `STANDARD`/`GLACIER`） |
| `--multipart-size N` | 分片上传每片大小（默认 `8388608`，最小钳制 `5242880`） |
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

FUSE 目录读取使用 `readdirplus`，每个目录项同时返回属性，无需额外 stat 往返。

凭据来自环境变量（`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）或 AWS 共享配置；托盘会把密钥注入其拉起的 `ossmount` 进程。

## 一致性

弱一致：无锁、无原子 rename；文件在 close/flush 时整文件写入。这是"云盘"，不是多写者 POSIX 文件系统——不要用作数据库后端或多人并发编辑同一文件。

## 运维注意

- 每个目录枚举/stat 都是一次**远程 S3 请求**；避免对挂载盘做全盘扫描（`find /`）。
- `ObjectFs` 限制在途 S3 请求（默认 32）与内存，防止 I/O 风暴 OOM 崩溃；WinFsp 镜像保留 16 MiB 线程栈。详见 [doc/README.md](doc/README.md)。

## 开发

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --lib --bins
cargo clippy --workspace
```

贡献指南见 [AGENTS.md](AGENTS.md)；设计与限制见 [doc/README.md](doc/README.md)。

## License

MIT — 见 [LICENSE](LICENSE)。
