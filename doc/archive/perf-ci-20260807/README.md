# perf-ci 分支性能补丁归档（2026-08-07）

## 来源

`codex/perf-ci` 分支（git worktree `brewfs-wt-perf-ci`）上的 29 个性能/重构提交，
2026-08-07 当天完成，push 时 CI 全绿（perf-check 工作流），**从未开 PR、未合入任何分支**。
2026-08-14 处置：归档至此，worktree 与分支已清理。

## 处置理由

该分支基于 `a104efc`（2026-08-07 的 main，BrewFS 全功能版）。此后 `10caf6f`
（OSSFS 独立化）删除了 `src/vfs`、`src/chunk`、`src/meta`、`src/fuse`、
`src/cadapter` 全部模块——29 个提交的**目标代码已不存在**，无法 cherry-pick 合并。

## 内容

29 个 `format-patch` 文件（按提交顺序编号，`git am` 可应用）：

- **perf(fuse)**（2）：readdirplus 批量取子节点属性；per-inode POSIX 锁计数
- **perf(chunk/cache)**（8）：磁盘 cache LRU 索引化（去 atime+目录扫描）；
  insert_hot 去强制 run_pending_tasks；read_at_into 零拷贝；压缩有界并发；LRU 审查修复
- **perf(cadapter)**（2）：S3 DeleteObjects 批量删除；合并重复 multipart 实现
- **refactor(vfs/io, vfs/stats)**（9）：writer_policy/writer_upload/writeback 记账、
  stats 计时/快照/渲染/同步提取
- **fix 杂项**（2）：batch_stat 指标记录；POSIX 锁计数顺序
- **ci/chore**（6）：perf-check 矩阵、死代码清理、macOS 测试门

## 可移植项备忘（OSSFS 时代）

| 提交 | 内容 | OSSFS 现状 |
|---|---|---|
| `759f2cc` | S3 DeleteObjects 批量删除 | `ObjectFs::delete_dir_recursive` 仍逐个 delete（`src/ossfs/mod.rs`），可借鉴实现批量 |
| `187c017` | readdirplus 批量取属性 | OSSFS fuse.rs 已实现 readdirplus，无需移植 |
| `9d5ecbf` | macOS 本地测试门 | 参考价值（OSSFS 本地测试门） |

## 应用方法

```bash
git am doc/archive/perf-ci-20260807/*.patch
```
