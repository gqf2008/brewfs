# BrewFS 独立元数据服务架构设计

- 状态：草案（设计评审中）
- 关联 issue：gqf2008/brewfs#16（设计）、#17（API 契约）、#18（服务端）、#19（客户端 RPC）、#20（失效广播）、#21（独立部署/HA）
- 范围：本文档只做设计与决策记录，不包含实现代码

## 1. 背景与动机

### 1.1 现状

当前 BrewFS 的元数据访问是**进程内 trait 调用**：

```
FUSE 请求
   ↓
VFS ──► MetaLayer trait
         └─► MetaClient（缓存 / 失效 / 批量预取 / session / 锁）
              └─► MetaStore trait
                   ├─► RedisMetaStore
                   ├─► DatabaseMetaStore（SQLite / PostgreSQL）
                   ├─► EtcdMetaStore
                   └─► TiKvMetaStore
```

- `MetaStore`（`src/meta/store.rs`）是后端抽象边界，定义了 100+ 方法，其中约 63 个方法有默认 `NotImplemented`。
- `MetaClient`（`src/meta/client/mod.rs`）在客户端侧实现 inode/path/children/slice 缓存、失效、批量预取、session、锁。
- 每个挂载进程都直连后端数据库，各自维护一套 key schema、Lua/事务脚本与缓存失效逻辑。
- 仓库已有两类"服务化"雏形，但都不是元数据服务：
  - `src/control/`：Unix socket control plane（RunGc / GetInfo / GetJob 等管理命令）。
  - `src/console/`：axum HTTP server（web console / CSI 辅助）。

### 1.2 问题

1. **逻辑复制**：缓存、失效、锁、session 逻辑在每个 mount 进程各实现一份，改一处需要所有挂载点升级。
2. **后端替换成本高**：客户端与后端 schema/脚本耦合，替换元数据后端需要改多个客户端。
3. **多挂载点一致性弱**：跨客户端失效目前只有 etcd watch（Redis/TiKV/DB 缺失），close-to-open 语义依赖后端能力，不一致。
4. **安全边界缺失**：客户端需要直接访问数据库的凭据与端口，攻击面大。
5. **独立运维困难**：无法对元数据服务单独扩容、升级、监控、限流。

### 1.3 目标

把 `MetaClient + MetaStore` 语义封装为**独立的元数据服务**：

- FUSE 客户端通过服务 API 访问元数据，不再直连后端数据库。
- 客户端缓存**保留**在客户端，避免元数据 RPC 成为性能瓶颈。
- 后端替换、失效广播、锁、session、运维全部收敛到服务端。
- 直连模式保留为默认与回退路径，服务化逐步灰度。

### 1.4 非目标

- 不做数据面（对象存储读写仍走客户端/块缓存）。
- 不改变 chunk/block/slice 数据布局与对象 key 规则。
- 不承诺与 JuiceFS 元数据协议兼容。
- 第一阶段不做跨机房/广域元数据联邦。

## 2. 总体架构

```
                    ┌───────────────────────────────┐
                    │   BrewFS Meta Service (daemon) │
                    │  ┌───────────────────────────┐ │
                    │  │ MetaServiceHandler        │ │
                    │  │  - 语义校验 / 权限 / 限流   │ │
                    │  └──────────┬────────────────┘ │
                    │             │                  │
                    │  ┌──────────▼────────────────┐ │
                    │  │ MetaClient (服务端实例)     │ │
                    │  │  - 缓存 / 失效 / session    │ │
                    │  │  - 锁 / watch 事件源        │ │
                    │  └──────────┬────────────────┘ │
                    │             │                  │
                    │  ┌──────────▼────────────────┐ │
                    │  │ MetaStore trait           │ │
                    │  │  Redis/TiKV/etcd/SQL      │ │
                    │  └───────────────────────────┘ │
                    └──────────────┬────────────────┘
                                   │ gRPC / HTTP
          ┌────────────┬───────────┼───────────┬────────────┐
          ▼            ▼           ▼           ▼            ▼
       mount A      mount B     brewfs CLI   console     CSI
       (FUSE)      (FUSE)      (info/gc)    (web)      (k8s)
```

- 每个 mount 进程内保留 `MetaClient` 的**客户端缓存层**，但 `store` 换成 RPC 客户端。
- 服务端进程内复用现有 `MetaStore` 后端实现，因此后端支持面不缩水。
- 管理命令（info/gc/status）与数据面（读写）可走同一服务。

## 3. 服务边界

### 3.1 进入服务（元数据语义）

| 类别 | 方法（草案） | 说明 |
|---|---|---|
| 基础查询 | `stat` / `stat_fresh` / `lookup` / `lookup_with_attr` | 支持批量 `batch_stat` |
| 目录 | `readdir` / `opendir` / `mkdir` / `rmdir` | readdir 返回稳定排序 |
| 命名空间 | `create_file` / `unlink` / `rename` / `link` / `symlink` | 原子语义由后端保证 |
| 属性 | `set_attr` / `set_file_size` / `truncate` / `fallocate` | 含 mtime/ctime 语义 |
| 数据映射 | `get_slices` / `append_slice` / `invalidate_chunk_slices` | slice 列表带版本 |
| 锁 | `get_flock` / `set_flock` / `get_plock` / `set_plock` | 绑定 session/owner |
| 会话 | `start_session` / `shutdown_session` / heartbeat | 崩溃回收锁 |
| 标识 | `next_id`（inode/slice） | 服务端原子分配 |
| 维护 | `stat_fs` / GC 账本 / `list_chunk_ids` | 服务端统一 |

### 3.2 留在客户端（不进服务）

- 对象块读写（`BlockStore` → S3/RustFS/MinIO）。
- 块/page 缓存、预取、写回 staging、dirty overlay。
- FUSE 请求编排、handle 生命周期、split-write barrier。
- 客户端元数据缓存（TTL/失效由服务端事件驱动）。

### 3.3 边界原则

- 客户端**不感知**后端 key schema、Lua 脚本、表结构。
- 服务端**不感知** FUSE 请求、内核页缓存、块缓存。
- 一次 RPC 的粒度 = 一次元数据语义操作（可带批量），不做"文件系统级"巨型调用。

## 4. API 契约

### 4.1 传输选型：gRPC（首选）

| 维度 | gRPC | HTTP/JSON |
|---|---|---|
| 性能 | 二进制 protobuf、流式、多路复用 | JSON 编解码开销大 |
| 流式 | 原生 server-streaming（适合失效通知） | SSE 需额外实现 |
| 生态 | tonic 已在本仓库依赖中（workspace） | axum 已有，但缺流式广播 |
| 错误模型 | 状态码 + 结构化 details | 需自定错误 schema |

结论：**方法调用与失效通知都用 gRPC**（tonic）。`console` 保持 axum 不动，通过服务端聚合 API 暴露给 web，不直接连后端。

### 4.2 方法集（草案，详见 #17）

- `service MetaService`：
  - 查询：`Stat`、`BatchStat`、`Lookup`、`LookupWithAttr`、`Readdir`
  - 变更：`CreateFile`、`Mkdir`、`Unlink`、`Rmdir`、`Rename`、`Link`、`Symlink`、`SetAttr`
  - 数据映射：`GetSlices`、`AppendSlice`、`InvalidateChunkSlices`
  - 锁：`GetFlock`、`SetFlock`、`GetPlock`、`SetPlock`
  - 会话：`StartSession`、`ShutdownSession`、`Heartbeat`
  - 维护：`NextId`、`StatFs`、`ListChunkIds`
- `service MetaWatch`（server-streaming）：`WatchEvents`（变更通知，见 §5.2）。
- 消息字段以 `FileAttr` / `DirEntry` / `SliceDesc` 等现有结构为基线，保持语义不变。

### 4.3 错误码映射

| 服务错误 | MetaError | VfsError / errno |
|---|---|---|
| NOT_FOUND | NotFound | ENOENT |
| NOT_DIRECTORY | NotDirectory | ENOTDIR |
| NOT_EMPTY | DirectoryNotEmpty | ENOTEMPTY |
| EXISTS | AlreadyExists | EEXIST |
| PERMISSION_DENIED | PermissionDenied | EACCES |
| NOT_SUPPORTED | NotImplemented / NotSupported | ENOTSUP / EOPNOTSUPP |
| CONFLICT / RETRY | ContinueRetry | 内部重试，不直接暴露 |
| INTERNAL | Internal | EIO |

- 契约评审时以 `src/vfs/error.rs` 与 `src/fs.rs` 的现有映射为准，确保双向一致。

### 4.4 幂等与重试

- 查询天然幂等；变更操作需可重放（如 `AppendSlice` 依赖 chunk version CAS，见 `doc/architecture/redis-version-cas.md`）。
- 客户端对 `CONFLICT` / 网络错误按现有 `backoff` 语义重试；服务端保持幂等键（session id + request id）可选实现。
- 超时参数可配（默认对齐现有 S3 客户端：connect 5s / read 30s / op 120s 为参考值）。

## 5. 缓存归属与失效协议

### 5.1 缓存归属：客户端保留，服务端只做事件源

理由：

1. metaperf 中 `stat` 达 72 万 ops/s 量级（README 性能表），任何把缓存移到服务端 + 网络往返的方案都会击穿性能。
2. 客户端缓存已实现 inode/path/children/slice/open-file 多层，语义成熟（`src/meta/client/cache.rs`）。
3. 服务化后**失效协议**由服务端统一发布，客户端订阅，等价于把"每客户端各自 watch 后端"收敛为"客户端 watch 服务"。

保留的客户端缓存项：

- inode attr cache（TTL 可配，默认 1s 级别，与 `fuse_cache_ttl` 对齐）
- path → inode 缓存与 path trie
- children（readdir）缓存 + batch prefetch
- chunk → slice list 缓存（必须带版本 token，见 gqf2008/brewfs#22）
- open-file cache（服务端 close 事件驱动刷新）

### 5.2 失效协议

- 服务端在每次成功变更后（rename/unlink/truncate/compact/append_slice/setattr）生成事件：
  `{kind, ino, path, chunk_index, version, seq}`。
- 通过 `MetaWatch.WatchEvents` server-streaming 广播给所有订阅客户端。
- 客户端把事件翻译为现有 `MetaClient::invalidate_*` / `invalidate_chunk_slices` 调用。
- 订阅断线：客户端收到 stream 断开后**降级为 TTL 过期模式**（缓存按自身 TTL 过期）并重连；重连成功后按服务端 `seq` 回放或整体失效重建（安全侧选择整体失效）。
- 与直连模式关系：直连模式目前只有 etcd watch；服务化后 Redis/TiKV/SQL 也能获得统一失效，这是服务化的核心收益之一。

### 5.3 close-to-open 一致性

- `open`（fresh stat）在服务化模式下仍走 `StatFresh`（绕过 attr 缓存），保证 close 后可见。
- `get_slices` 带版本校验（fork #22 落地后成为前置条件）。
- 弱一致缓存开关（`open_file_cache`）语义保持与直连一致，由 mount 配置控制。

## 6. HA 与运维

### 6.1 部署形态

- **阶段 B**：`brewfs meta serve --meta-url redis://... --listen 127.0.0.1:7001`，独立进程，单实例。
- **阶段 C**：多实例，客户端连虚拟地址/负载均衡。

### 6.2 多实例与选主

- 后端是 Redis/TiKV/etcd/SQL，本身是共享存储；多服务实例对同一后端做读写需要**单一写入者**约束，避免缓存/锁语义分叉。
- 选主方案（评审候选）：
  a. **etcd lease 选主**：服务实例抢租约，租约持有者处理变更；备实例只读/拒绝写。依赖已有 etcd 组件。
  b. **复用后端全局锁**：用 `MetaStore::get_global_lock`（已有分布式锁语义）做 leader 选举。
  c. 独立 raft（如 openraft）引入新依赖，成本高，不建议第一阶段。
- 目标 RPO=0（后端即持久层，主实例不落本地状态）；RTO 目标 ≤ 30s（租约过期 + 切换），具体以评审为准。
- 失效广播由 leader 独占发布，避免事件乱序。

### 6.3 健康检查与监控

- gRPC health 协议（`grpc.health.v1.Health`）暴露存活/就绪。
- 指标复用 `src/vfs/stats.rs` 模式：请求数/延迟/错误/缓存命中/失效事件数，暴露 Prometheus 文本格式。
- `brewfs info/status` 增加服务拓扑（实例、leader、后端、会话数）。

### 6.4 安全

- TLS：gRPC TLS（tonic-rustls，依赖已在 workspace）。
- 认证：第一阶段 token（`--meta-service-token`），后续可接 mTLS。
- 客户端不再持有后端数据库凭据——这是独立服务的直接收益。

### 6.5 配置面

- 收敛散落的 `BREWFS_*` 环境变量到 `meta-service` 配置节（YAML + CLI flag），`brewfs info` 可展示。
- 兼容直连模式：客户端配置 `meta.backend = "direct" | "service"`。

## 7. 迁移路径

分三阶段，每阶段独立可交付、可回退：

| 阶段 | 内容 | 交付物 | 回退 |
|---|---|---|---|
| A | 进程内服务化：同二进制启动 meta daemon，客户端 RPC 直连本机服务（不独立部署） | #17 契约 + #18 服务端 + #19 客户端 RPC | 切回直连，零改动 |
| B | 独立进程：`brewfs meta serve` 独立运行，多 mount 连同一服务 | #20 失效广播 + 运维文档 | 客户端指向直连或本机服务 |
| C | 多副本 HA：leader 选举 + 健康检查 + TLS | #21 | 退回单实例 |

- 每阶段验收都要求：直连与 RPC 双模式行为等价（同一测试集双跑）、`run_redis_pjdfstest.sh` 通过、无 metaperf 回归。
- 默认仍走直连模式，直到 B/C 阶段稳定后再考虑翻转默认。

## 8. 与现有组件的关系

| 组件 | 关系 |
|---|---|
| `MetaStore` | 服务端复用它，不修改 trait 语义；能力矩阵（gqf2008/brewfs#14）先行 |
| `MetaClient` | 客户端缓存层保留；新增 RPC store 实现替代 `MetaClient.store()` |
| `src/control/` | 管理命令可继续走 control plane，也可经 meta service 转发；不强制合并 |
| `src/console/` | 保持 axum；通过 meta service 聚合数据，不再直连后端 |
| `src/ossfs/` | 无元数据服务（metadata-less），不受影响 |

## 9. 开放问题（评审必答）

1. 选主是否必须？单实例 + 后端 HA（Redis Sentinel / TiKV / etcd 本身高可用）能否满足第一版？
2. 失效广播与"客户端直连 watch 后端"相比，多一跳是否可接受（事件延迟目标：≤ 100ms 到达）？
3. 变更请求是否全部经 leader？读请求能否走任意实例（后端强一致时）？
4. `batch_stat` / `readdir` 批量语义是否进入 v1 契约？
5. 客户端缓存 TTL 默认值是否沿用直连模式（1s）？服务化后是否需要更短以换取一致窗口？
6. RPO=0 是否可承诺（后端即持久层）？`uncommitted slice` 恢复流程如何走服务端？
7. 是否需要版本化契约（protobuf 向后兼容策略）？

## 10. 落地计划（issue 映射）

| Issue | 内容 | 依赖 |
|---|---|---|
| gqf2008/brewfs#16 | 本文档（设计评审） | — |
| gqf2008/brewfs#17 | API 契约（proto + 错误映射） | #16 |
| gqf2008/brewfs#18 | 服务端（进程内可运行） | #16 #17 |
| gqf2008/brewfs#19 | 客户端 RPC 化（双模式） | #16 #17 #18 |
| gqf2008/brewfs#20 | 失效广播与多客户端一致性 | #16–#19 |
| gqf2008/brewfs#21 | 独立部署与 HA | #16–#20 |

## 11. 验收标准（本文档）

- [ ] 覆盖本文件第 3–9 节的 7 个设计要点（边界 / 契约 / 缓存 / 失效 / HA / 迁移 / 开放问题）
- [ ] 经至少一次独立 review，开放问题有结论或明确责任人
- [ ] 落地 issue 清单与依赖关系已确认（§10）
- [ ] 纯设计文档，不包含实现代码
