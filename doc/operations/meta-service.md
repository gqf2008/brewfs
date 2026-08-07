# Standalone Metadata Service

独立元数据服务（`brewfs meta serve`）把元数据访问从"每个挂载进程直连数据库"
收敛为独立的 gRPC 服务（设计见 `doc/architecture/meta-service.md`，落地 issue
gqf2008/brewfs#16–#21）。客户端用 `RpcMetaStore` 连服务，不再持有数据库凭据。

## 快速开始

```bash
# 1) 启动独立元数据服务（后端用 SQLite 文件）
brewfs meta-serve \
  --meta-url sqlite:///tmp/brewfs/meta-service.db \
  --listen 127.0.0.1:7001

# 2) 客户端挂载（RPC 模式）——需要 CLI 支持（#19 起 RpcMetaStore 可用）
#    brewfs mount --meta-service-endpoint http://127.0.0.1:7001 ...
```

参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--meta-url` | `sqlite:///tmp/brewfs/meta-service.db` | 元数据后端（sqlite/redis/etcd/tikv/postgres） |
| `--listen` | `127.0.0.1:7001` | gRPC 监听地址 |
| `--token` | 无 | Bearer token 认证（v1 无 TLS） |
| `--leader-ttl-secs` | 30 | Leader 租约 TTL（秒） |

## Leader 与健康检查

- 服务启动后通过后端全局锁（`MetaServiceLeader`）竞争 leader 租约，并周期性续约。
- gRPC Health（`grpc.health.v1.Health`）：
  - `brewfs.meta.v1.MetaService`：进程存活即 `SERVING`；
  - 空 service：仅 leader 为 `SERVING`，standby 为 `NOT_SERVING`。
- 负载均衡器应把客户端流量路由到空 service 为 `SERVING` 的实例；租约过期即摘除。
- v1 限制：客户端不做内置 failover（连固定地址或 LB）；TLS 未实现，内网使用或配合网络层加密。

## 认证

```bash
brewfs meta-serve --meta-url redis://... --listen 127.0.0.1:7001 --token s3cret
```

服务端要求每个请求带 `authorization: Bearer s3cret`。客户端侧在
`RpcMetaStore`/`MetaClient` 接线时通过 `with_token` 注入 metadata
（当前 CLI 挂载尚未暴露该参数，属后续接线）。

## Compose 示例

`docker/compose-xfstests/docker-compose.meta-service.yml` 提供一个
Redis + 独立元数据服务的最小拓扑（供本地验证，非生产形态）：

```bash
docker compose -f docker/compose-xfstests/docker-compose.meta-service.yml up
```

## 运维注意事项

- 元数据服务是无状态的（除可重建的内存缓存），后端即持久层：RPO=0 由后端事务保证；
- 升级流程：滚动替换服务实例即可，客户端缓存按 TTL 兜底；
- 查看服务日志中的 `metadata service starting` / `leader lease refreshed` 确认选主状态；
- 监控：gRPC Health + 服务端 tracing（`RUST_LOG=debug` 可见事件与请求）。
