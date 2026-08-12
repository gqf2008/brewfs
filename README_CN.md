# OSSFS

把 S3 兼容存储桶（阿里云 OSS、MinIO、AWS S3 等）直接挂载为本地网络盘，**无本地元数据库**。路径直接编码为对象键，任意多台机器挂同一桶看到同一棵树。

- Windows：WinFsp，挂为盘符（如 `F:`）
- macOS：FUSE-T / macFUSE，挂为 `/Volumes/ossfs`
- Linux：libfuse，挂为目录

## 快速开始

```bash
cargo build --release -p ossfs --bin ossmount --no-default-features --features fuse-winfsp
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  target/release/ossmount mount --bucket BUCKET --endpoint https://oss-cn-shanghai.aliyuncs.com --region cn-shanghai F:
```

或使用 `ossfs-tray` 托盘程序管理挂载。

## 一致性

弱一致：无锁、无原子 rename。桶是唯一数据源——这是"云盘"，不是多写者 POSIX 文件系统。写入按整文件缓冲，close/flush 时推送到对象存储。

详见 [doc/README.md](doc/README.md)。
