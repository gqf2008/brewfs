# OSSFS 软删除回收站(.trash 墓碑)设计稿

> 状态:设计稿,供评审。定稿后再转正式英文文档。
> 形态:墓碑(引用)+ 软删除(原对象不搬不删)。删除只写一个小墓碑对象,
> 原对象留在原地,由端上过滤隐藏。恢复 = 删墓碑,真正清除 = GC 删墓碑 + 删原对象。

## 0. 决策记录

| # | 决策 | 理由 |
|---|------|------|
| D1 | **墓碑方案,非 CopyObject 复制方案** | 删除零数据复制、零存储翻倍;墓碑是 KB 级小对象,恢复是元数据级操作,原子性比 copy+delete 强(墓碑写成功即删除提交点) |
| D2 | **不用布隆过滤器,用 HashSet + 前缀索引** | 布隆解决"集合大到放不进内存",墓碑集合真实规模(几万条)就是几 MB HashSet;标准布隆**不可删除**,与"恢复"直接冲突;误报方向是"把活文件藏起来",不可接受 |
| D3 | **同步源 = `.trash/` 前缀本身** | 墓碑持久化在 S3,任何端拉一遍即得全量被删集合,不需要额外协议;多端同步退化为"多久拉一次" |
| D4 | **本端写墓碑即时更新本地索引;远端靠后台周期刷新** | 本端删除立即隐藏(不等刷新);远端接受秒级~分钟级窗口,回收站场景可容忍 |
| D5 | **默认开启(CLI 默认 `--trash-dir .trash --trash-retention-days 30`),`--no-trash` 显式关闭** | 回收站的意义就是防误删,默认关等于没防(用户拍板);空间回收靠 GC 兜底;行为偏差写入 README 已知限制 |

## 1. 语义声明(默认行为即偏差,除非 `--no-trash`;必须文档化)

1. `unlink` / `rmdir` **不再释放空间**,原对象保留至 GC 清理(POSIX 偏差;`--no-trash` 恢复原语义)。
2. **隐藏是本端客户端幻觉**:OSS 控制台、其他 S3 客户端、未同步墓碑的挂载机,都能看到"已删除"的原对象。
3. **恢复不保证"删除时内容"**:墓碑只记 key + etag 校验信息。删除后其他端覆盖同名 key,恢复出来是新内容;恢复命令通过 etag 校验告知用户。
4. **删除生效有延迟窗口**:远端删除到本端感知 ≤ 刷新周期 + OSS 最终一致(通常秒级)。
5. 目录 GC 用 mtime 启发式判定"前缀下对象是否晚于墓碑日期",不保证完美(见 §7)。

## 2. 墓碑 key 格式与内容

```
<命名空间prefix>.trash/<YYYY-MM-DD>/<原key>        # 文件墓碑
<命名空间prefix>.trash/<YYYY-MM-DD>/<原key>/       # 目录墓碑(尾斜杠)
```

- 日期为删除时的 UTC 日期,按天分区,GC 按分区整区清理。
- 文件墓碑 body = 小 JSON:`{"etag": "...", "size": 123, "is_dir": false}`。
  etag/size 来自删除时的 HEAD(比原来的 DELETE 多 1 次 HEAD + 1 次 PUT 小对象,共 2 次小请求)。
- 目录墓碑 body = `{"is_dir": true}`(隐式目录无原对象,不需要 etag)。
- 原 key 为根(空)禁止删除,复用现有守卫 `is_root_path`(mod.rs:3224)。
- `.trash` 前缀自身对挂载视图**完全隐藏**(见 §4),挂载盘内不可创建、不可见;外部客户端往 `.trash/` 下写东西不会被索引记录(过滤时整前缀跳过)。
- 与 `prefix`(命名空间)共存:墓碑放 `self.prefix` 之下,与数据同命名空间,过滤用 `trash_prefix = format!("{prefix}.trash/")` 判断。

## 3. 索引数据结构(替代布隆过滤器)

```rust
/// 被删 key 索引。精确命中(文件墓碑)或前缀覆盖(目录墓碑)。
struct TombstoneIndex {
    files: HashSet<String>,   // 被删的精确 key
    dirs: Vec<String>,        // 被删的目录 key(前缀,尾斜杠),排序后二分
}
impl TombstoneIndex {
    fn is_covered(&self, key: &str) -> bool;   // files 命中 或 dirs 中存在 key 的前缀
    fn insert(&mut self, key: &str, is_dir: bool);
    fn remove(&mut self, key: &str, is_dir: bool);
    fn rebuild(&mut self, tombstones: impl Iterator<Item = (String, bool)>);
}
```

- 查询量:list 一页 1000 key × HashSet/二分,纳秒级;过滤**零额外远程请求**(数据来自本地索引)。
- 线程模型:`Arc<RwLock<TombstoneIndex>>` 挂在 `ObjectFs` 上(与 `stats` 缓存同模式);读路径是 list/stat 主路径,写路径是刷新任务 + 本端删除。
- 规模守卫:`metrics.trash_index_entries` 计数,超过 `TRASH_INDEX_ALERT_THRESHOLD`(默认 500_000,阈值独立 commit 并带断言,见 `RULE_阈值变更规范`)记 warn——规模异常必有信号;正常规模由 GC 的 retention 兜底。

## 4. 过滤挂点(两处,双平台覆盖)

FUSE `readdir`/`lookup` 与 WinFsp `read_directory_async`/`get_security_by_name`/`get_file_info` 全部收敛到 `ObjectFs::list` / `ObjectFs::stat`,只改这两处:

1. **`list_impl`(mod.rs:2094)**,在 push 每个 common_prefix / object 之前:
   - `key.starts_with(trash_prefix)` → 跳过(隐藏 `.trash` 自身,含根目录下的 `.trash/` common_prefix);
   - `index.is_covered(key)` → 跳过(被删)。
   - 目录前缀也会以 common_prefix 形式出现:被目录墓碑覆盖的 common_prefix 一并跳过(对 common_prefix 判断 `is_covered(prefix)`)。

2. **`stat_uncached_impl`(mod.rs:2240)**,在 `path == "/"` 守卫之后、发 HEAD **之前**:
   - `key.starts_with(trash_prefix)` → 返回 None;
   - `index.is_covered(key)` → 返回 None。
   - 收益:被删路径的 stat **零远程请求**(head 都不发),比现在多一次 HEAD 成本更低。
   - 注意 `stat` 结果缓存(TTL):墓碑恢复/新墓碑要 invalidate 相关缓存(复用 `invalidate_stat`)。

派生挂点(自动获得过滤,无需改动):WinFsp notify 刷新 `refresh_dir`(winfsp.rs:686)、FUSE 周期刷新、`has_children_impl` 探测路径(stat 入口已拦)。

## 5. 删除流程(统一收敛到 ObjectFs,adapter 不改语义)

现有入口:`delete`(mod.rs:3222)、`delete_dir_recursive`(mod.rs:3256);调用方:FUSE `unlink`/`rmdir`(fuse.rs:829/864)、WinFsp cleanup delete(winfsp.rs:1060,含 `delete_on_close` 路径)。

Trash 开启时,入口替换为:

```
unlink(path):
  1. HEAD 原对象 → etag/size(目录不需要)
  2. PUT 墓碑 .trash/<date>/<key>  (提交点)
  3. 本地索引即时 insert + invalidate_stat(path)
  4. 原对象不删
  失败语义:PUT 墓碑失败 → 删除报错,文件还在(提交点前无任何副作用)

rmdir(dir):
  1. PUT 目录墓碑 .trash/<date>/<dirkey>/
  2. 索引 insert(dir, is_dir=true) + invalidate_stat(dir)
  3. 前缀下所有对象不删,由前缀覆盖隐藏
```

Trash 关闭时走原逻辑(默认行为不变,现有 delete/DeleteObjects 测试全绿)。

## 6. 多端同步(刷新调度)

- **挂载启动**:后台任务全量 list `.trash/`(分页)构建索引。
- **本端写入**:§5 即时更新索引,不等刷新。
- **远端变更**:周期增量拉取——
  - 游标:`ListObjectsV2 start-after=last_seen_key` 只拉新增墓碑(追加语义,墓碑创建后不可变);
  - 周期性全量重建(默认每 10 分钟)兜底"被恢复/被 GC 移除的墓碑"——索引只增不减的问题,全量重建解决;
  - 周期默认 30s(`trash_refresh_interval_secs`),两个周期都是阈值,独立 commit 带断言。
- **强一致档位**(可配置 `trash_refresh_mode = eager`):每次 list/stat 前先增量刷一遍 `.trash`,窗口缩到 OSS 最终一致量级,代价是枚举类请求的远程成本翻倍。默认 `lazy`。

## 7. GC(过期清理)

- 规则:墓碑日期早于 `trash_retention_days`(**默认 30**)才可清;触发 = 挂载时 + 每 `trash_gc_interval_secs`(默认 24h)+ 命令 `ossmount trash-clean`。
- **文件墓碑**:HEAD 原对象 →
  - 不存在(外部已删)→ 删墓碑;
  - 存在且 etag 与墓碑一致 → DELETE 原对象 → 删墓碑;
  - 存在且 etag 不一致(活数据)→ **跳过**,记 metrics(`trash_gc_etag_skips`),留给人工。
- **目录墓碑**:先按 mtime 启发式判定前缀下对象:`last_modified < 墓碑日期` 的才批量删(`DeleteObjects` 复用 `MAX_DELETE_OBJECTS_PER_REQUEST`),晚于墓碑日期的对象视为新数据保留;然后删墓碑。文档声明该启发式的边界。
- **顺序约定(多端竞态)**:先删原对象、后删墓碑。另一端在 GC 删除原对象后、删墓碑前发起恢复 → 恢复时 HEAD 原对象 404,报"原对象已不存在"并清墓碑,不留空引用。
- GC 全程持本地索引写锁更新;GC 期间的远端刷新按游标继续(墓碑删除不影响 start-after 游标方向)。

## 8. 恢复

- 机制:删除墓碑(DELETE)+ 索引 remove + invalidate_stat → 原对象立即复活。
- 入口(V1 为命令式,不做盘内可见):
  - `ossmount trash-list`(分页列出墓碑:日期 / 原路径 / etag / size);
  - `ossmount trash-restore <path>`:HEAD 原对象校验 —— 404 → "原对象不存在,无法恢复"(已 GC);etag 不一致 → 警告"内容已被其他端修改"后默认仍恢复;
  - `ossmount trash-clean [--before <date>]`。
- V2 候选:盘内只读 `.trash` 视图(显示墓碑,rename 出盘 = 恢复),本期不做。
- 交互坑(文档化):在被删路径上**新建**同名文件 = 覆盖语义 —— PUT 前先清该路径墓碑,旧内容随覆盖丢失,符合"用户主动创建"直觉。

## 9. 配置与 CLI

`OssConfig` 新增(扁平,风格同现有字段):

```rust
pub trash_dir: Option<String>,            // Some(".trash") 即开启,None 关闭;CLI 默认 Some(".trash")
pub trash_retention_days: Option<u32>,    // 默认 30
pub trash_refresh_interval_secs: Option<u64>, // 默认 30
pub trash_refresh_mode: Option<TrashRefreshMode>, // lazy(默认) | eager
pub trash_gc_interval_secs: Option<u64>,  // 默认 86400
```

CLI:默认开启 —— `ossmount mount ...` 等效 `--trash-dir .trash --trash-retention-days 30`;
`--no-trash` 显式关闭(恢复直接永久删除);
管理命令:`trash-list / trash-restore / trash-clean`(复用现有连接参数)。

## 10. 测试计划

- **单元**:`TombstoneIndex`(精确/前缀覆盖/remove/rebuild);墓碑 key 编解码(特殊字符、长路径、日期分区、与 `prefix` 组合);恢复 etag 校验分支。
- **集成(mock S3,复用现有 mock 基础设施)**:
  - unlink 写墓碑 + 原对象保留;list 过滤被删 key;stat 被删路径零请求返回 None(断言 `metrics.s3_heads` 不增);
  - rmdir 写目录墓碑 + 前缀覆盖隐藏 + 子文件 stat 返回 None;
  - 同名重建清墓碑;rename 目标/源被墓碑覆盖的语义;
  - GC:etag 一致删、不一致跳过、原对象 404 删墓碑、目录 mtime 启发式;
  - 多端模拟:两个独立索引实例,一端写墓碑,另一端按游标刷新后过滤生效;
  - 恢复:删墓碑后立即可见;404/etag 不一致分支。
- **性能守卫(阈值带断言)**:开启 trash 后,普通 list/stat 的远程请求数与关闭时一致(`s3_lists`/`s3_heads` 不增)——"过滤零额外远程成本"可执行化。
- **回归**:`--no-trash` 下现有 delete / delete_dir_recursive 测试(含 OSS Content-MD5)全绿,确认显式关闭后行为与当前版本完全一致。

## 11. 里程碑拆分(批次化,每个单元独立 PR + 本地 CI 门禁)

| 单元 | 内容 | 验收 |
|------|------|------|
| 1 | `TombstoneIndex` + 两处过滤挂点 + 配置字段(默认开,`--no-trash` 可关) | 默认开启下过滤生效,`--no-trash` 后零行为变化;性能守卫断言 |
| 2 | 软删除写墓碑(unlink/rmdir)+ 本端即时索引 | 删除流程测试 + 现有回归全绿 |
| 3 | 刷新调度(全量 + start-after 增量 + 周期重建) | 多端模拟测试 |
| 4 | `trash-list / trash-restore / trash-clean` + GC | 恢复/GC 分支测试 |
| 5 | README 已知偏差声明 + metrics + 阈值断言收尾 | 文档与指标一致 |

## 12. 明确不做的(本期)

- 盘内可见 `.trash`(V2 候选);
- 墓碑内容快照(恢复"删除时内容"要复制数据,违背 D1;由 bucket 版本控制兜底);
- 服务端过滤(S3 ListObjects 无 tag/条件过滤参数,只能端上过滤);
- 与版本控制互操作(二者并存:版本控制保内容,墓碑管交互,不互相依赖)。
