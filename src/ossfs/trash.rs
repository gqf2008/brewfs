//! 回收站(soft delete / trash)索引与墓碑编解码。
//!
//! 形态:删除只写一个小墓碑对象到 `<trash_prefix><YYYY-MM-DD>/<原key>`,
//! 原对象留在原地,由挂载端 [`crate::ossfs::ObjectFs::hidden_key`] 过滤隐藏。
//! 恢复 = 删墓碑;真正清除 = GC(单元 4)。多端同步 = 周期拉取 `.trash/`
//! 前缀重建/增量索引(单元 3)。
//!
//! metadata-less 原则:墓碑本身就是唯一状态源,本模块不引入本地元数据库。

use crate::ossfs::{
    DeleteObjectsContentMd5, DirEntry, MAX_DELETE_OBJECTS_PER_REQUEST, ObjectFs, TrashRefreshMode,
    basename, is_s3_not_found, next_page_token,
};
use anyhow::{Context as _, Result};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// eager 档的最小轮询间隔:每次 list/stat 前的增量拉取节流,防枚举类请求
/// 远程成本翻倍放大(规格 C5 阈值,独立 commit 落地 + 断言;变更必须独立
/// commit 写明新旧值与理由)。
pub const TRASH_EAGER_MIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// 墓碑 key 结构:`<date>` 为删除时 UTC 日期分区;original_key 为完整原对象 key
/// (含命名空间 prefix,与 `key_for()` / `obj.key()` 零转换);is_dir 由 original_key
/// 尾斜杠推导(目录墓碑 key 以 '/' 结尾)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneKey {
    pub date: chrono::NaiveDate, // "YYYY-MM-DD"(UTC),decode 时经 parse_from_str 校验
    pub original_key: String,    // 完整原对象 key(目录含尾 '/')
    pub is_dir: bool,
}

/// 编码:`{trash_prefix}{date}/{original_key}`;is_dir 且 original_key 未以 '/'
/// 结尾时补尾斜杠(幂等:已带则不重复追加 —— 防 "docs//" 双斜杠)。
pub fn encode_tombstone_key(
    trash_prefix: &str,
    date: chrono::NaiveDate,
    original_key: &str,
    is_dir: bool,
) -> String {
    let mut key = format!("{trash_prefix}{date}/{original_key}");
    if is_dir && !key.ends_with('/') {
        key.push('/');
    }
    key
}

/// 解码。失败情形返回 None:非 trash 前缀、缺 '/'、date 非法
/// (`NaiveDate::parse_from_str("%Y-%m-%d")` 校验)、original_key 为空
/// (裸日期分区 / 外部客户端写入的垃圾对象)。
pub fn decode_tombstone_key(trash_prefix: &str, key: &str) -> Option<TombstoneKey> {
    let rest = key.strip_prefix(trash_prefix)?;
    let (date_str, original) = rest.split_once('/')?;
    if original.is_empty() {
        return None;
    }
    let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;
    Some(TombstoneKey {
        date,
        original_key: original.to_string(),
        is_dir: original.ends_with('/'),
    })
}

/// 删除时 UTC 日期分区;参数化 now 便于测试跨日边界(东八区 8/17 07:59 = UTC 8/16)。
pub fn date_partition_utc(now: std::time::SystemTime) -> chrono::NaiveDate {
    let dt: chrono::DateTime<chrono::Utc> = now.into();
    dt.date_naive()
}

/// is_covered 复杂度断言插桩(test-only):比较次数计数。thread_local 保证
/// 并行测试互不污染;cfg(test) 下编译,release 构建零开销。
#[cfg(test)]
thread_local! {
    static COUNT_IS_COVERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static COUNT_VALUE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// dirs(升序)中精确二分查找 prefix;每轮比较计数一次(裁决 #5 测试断言
/// 复杂度上界用)。非 test 构建下闭包只做比较,零额外开销。
/// 裁决 R4:dirs 携带 date(系统回收站视图冷路径按索引 date 反查墓碑 key),
/// 二分键仍是 `.0`(比较计数插桩保持)。
fn dirs_binary_search_covered(dirs: &[(String, chrono::NaiveDate)], prefix: &str) -> bool {
    dirs.binary_search_by(|(d, _)| {
        #[cfg(test)]
        {
            if COUNT_IS_COVERED.with(|c| c.get()) {
                COUNT_VALUE.with(|c| c.set(c.get() + 1));
            }
        }
        d.as_str().cmp(prefix)
    })
    .is_ok()
}

/// 被删 key 索引。精确命中(文件墓碑)或前缀覆盖(目录墓碑)。
/// files 用 HashMap(精确匹配,date = 最新墓碑日期),dirs 用排序 Vec
/// (前缀二分)—— 不用布隆过滤器的理由见设计稿 D2:布隆不可删除(与恢复
/// 冲突),且误报方向是藏起活文件。
/// 裁决 R4:date 随墓碑 key 免费可得(解码时已有,原被丢弃),一并索引 ——
/// 系统回收站视图冷路径(body 反查)与渲染需要 date,避免二次解码。内存
/// 增量 ≈ 16-24B/条(500k 条 ≈ 10MB,不含 size/etag/recycle_name)。
#[derive(Debug, Default)]
pub struct TombstoneIndex {
    /// 被删精确 key(文件,含命名空间前缀,无尾斜杠)→ 最新墓碑日期
    pub files: HashMap<String, chrono::NaiveDate>,
    /// 被删目录前缀(一律以 '/' 结尾,含命名空间前缀),按 key 升序、无
    /// 重复(key 去重保留最新日期)
    pub dirs: Vec<(String, chrono::NaiveDate)>,
}

impl TombstoneIndex {
    /// key 是否被覆盖:files 精确命中,或 dirs 中存在 key 的前缀。
    /// 关键正确性细节:目录形态双探测 —— key 不以 '/' 结尾时必须再探测 key+"/"。
    /// (stat("/docs") 得 key "docs",目录墓碑存 "docs/";只对 key 前缀匹配会漏,
    /// 经 marker HEAD 把已删目录复活。)
    pub fn is_covered(&self, key: &str) -> bool {
        if self.files.contains_key(key) {
            return true;
        }
        if self.dirs.is_empty() {
            return false;
        }
        // 目录形态双探测:key 不以 '/' 结尾时必须再探测 key+"/"(stat("/docs")
        // 得 key "docs",目录墓碑存 "docs/";只对 key 前缀匹配会漏,经 marker
        // HEAD 把已删目录复活)。
        //
        // 算法(裁决 #5):对 dir_key 的每级路径前缀精确二分 —— dirs 中能覆盖
        // dir_key 的墓碑 D 必等于某级路径前缀(含 dir_key 自身),逐级二分
        // O(路径深度 × log n),取代「插入点 + 线性回扫」的最坏 O(n)(1000 个
        // 共享公共前缀的墓碑会让回扫扫过全部条目才确认未覆盖)。500k 条目 ×
        // 深度 5 ≈ 5×19 次比较,纳秒级;复杂度承诺由
        // is_covered_comparisons_bounded_by_depth_log 断言执行化。
        //
        // 分配纪律(裁决 #12):浅层前缀是 key 的切片,零分配;仅当 key 不以
        // '/' 结尾且浅层全部未命中时才分配 key+"/" 探测完整 dir_key ——
        // 常规命中(文件直接位于被删目录下)路径零分配。
        for (i, &b) in key.as_bytes().iter().enumerate() {
            if b == b'/' && dirs_binary_search_covered(&self.dirs, &key[..=i]) {
                return true;
            }
        }
        if !key.ends_with('/') {
            let full = format!("{key}/");
            if dirs_binary_search_covered(&self.dirs, &full) {
                return true;
            }
        }
        false
    }

    /// 插入墓碑。is_dir=true 归一化补尾斜杠并保 dirs 升序;重复插入幂等,
    /// date 取最新(同名多日期墓碑只留一条,裁决 R7 —— 系统视图冷路径按
    /// 索引 date 反查墓碑 key,必须指向最新墓碑)。
    pub fn insert(&mut self, key: &str, is_dir: bool, date: chrono::NaiveDate) {
        if is_dir {
            let dir = if key.ends_with('/') {
                key.to_string()
            } else {
                format!("{key}/")
            };
            match self.dirs.binary_search_by(|(k, _)| k.as_str().cmp(&dir)) {
                Ok(pos) => {
                    if date > self.dirs[pos].1 {
                        self.dirs[pos].1 = date;
                    }
                }
                Err(pos) => self.dirs.insert(pos, (dir, date)),
            }
        } else {
            self.files
                .entry(key.to_string())
                .and_modify(|d| {
                    if date > *d {
                        *d = date;
                    }
                })
                .or_insert(date);
        }
    }

    /// 移除墓碑。不存在 no-op。
    pub fn remove(&mut self, key: &str, is_dir: bool) {
        if is_dir {
            let dir = if key.ends_with('/') {
                key.to_string()
            } else {
                format!("{key}/")
            };
            if let Ok(pos) = self.dirs.binary_search_by(|(k, _)| k.as_str().cmp(&dir)) {
                self.dirs.remove(pos);
            }
        } else {
            self.files.remove(key);
        }
    }

    /// 整体替换;dirs 按 key sort + 去重(保留最新 date);files 由 HashMap
    /// 天然按 key 去重(同名多日期墓碑只留最新 date)。
    pub fn rebuild(&mut self, tombstones: impl Iterator<Item = (String, bool, chrono::NaiveDate)>) {
        let mut files = HashMap::new();
        let mut dirs: Vec<(String, chrono::NaiveDate)> = Vec::new();
        for (key, is_dir, date) in tombstones {
            if is_dir {
                let dir = if key.ends_with('/') {
                    key
                } else {
                    format!("{key}/")
                };
                dirs.push((dir, date));
            } else {
                files
                    .entry(key)
                    .and_modify(|d: &mut chrono::NaiveDate| {
                        if date > *d {
                            *d = date;
                        }
                    })
                    .or_insert(date);
            }
        }
        dirs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut deduped: Vec<(String, chrono::NaiveDate)> = Vec::with_capacity(dirs.len());
        for (k, date) in dirs {
            match deduped.last_mut() {
                Some(last) if last.0 == k => {
                    if date > last.1 {
                        last.1 = date;
                    }
                }
                _ => deduped.push((k, date)),
            }
        }
        self.files = files;
        self.dirs = deduped;
    }
}

// ---------- 单元 1:系统回收站虚拟视图(识别/渲染/放行) ----------

/// 系统回收站配置(裁决 R1)。`OssConfig.system_trash: None` = 关闭。
/// 平台默认与默认开/关在 CLI(ossmount)与 build_trash_state 按
/// `cfg!(target_os = "macos")` 注入:Windows/Linux 默认随 trash 开启,
/// macOS 默认关闭(需显式 --system-trash-dir,采纳 A3 的保守默认)。
#[derive(Debug, Clone)]
pub struct SystemTrashConfig {
    /// 目录名覆盖(平台默认:"$Recycle.Bin" / ".Trashes")
    pub dir_name: Option<String>,
    /// macOS:渲染哪些 uid 段;空 = 当前挂载用户 uid
    pub macos_uid_dirs: Vec<u32>,
}

/// 平台形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SystemTrashPlatform {
    /// 条目成对:$R(内容)+$I(元数据,捕获字节)
    WindowsRecycleBin,
    /// 条目 = 原名(无 $R/$I)
    MacOsTrashes,
}

/// 系统回收站虚拟视图,挂在 TrashState.system(None = 不渲染)。
#[derive(Debug, Clone)]
pub(crate) struct SystemTrash {
    /// "$Recycle.Bin" | ".Trashes"(挂载根下可见目录名)
    pub dir_name: String,
    pub platform: SystemTrashPlatform,
    /// macOS 渲染/拦截范围;空 = 当前挂载用户 uid(裁决 R17)
    pub macos_uid_dirs: Vec<u32>,
}

/// 路径落在系统回收站内的哪一层(纯结构识别,同步,零远程)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SystemTrashMatch {
    /// 目录层:level 0 = 根("$Recycle.Bin" / ".Trashes"),1 = SID/uid 段
    Dir { level: usize },
    /// 条目层(第 2 段):Windows = $R/$I 名;macOS = 条目名
    Entry { entry_name: String },
}

/// $I 捕获字节上限(裁决 R8):Explorer 的 $I 头文件 ≤ 数百字节,4KiB 覆盖
/// 全部已知变体(含长路径 8B 长度字段);超出截断(数据仍可用,仅可能丢
/// 尾部 padding)。winfsp.rs 捕获缓冲与 set_recycle_i 落 body 共用同一
/// 常量(截断在落 body 时强制执行,防缓冲上限漂移)。验证见
/// set_recycle_i_truncates_over_4k。
pub(crate) const MAX_RECYCLE_I_BYTES: usize = 4 * 1024;

/// Windows $R 名 ↔ 墓碑的反向索引(裁决 R3:① 本地软删写入;② 增量/全量
/// 重建 diff 读 body 填充;③ 渲染/读取未命中按需 GET 兜底)。派生缓存,
/// 非独立事实源;与墓碑同生命周期(remove_tombstone_maps 收尾)。
/// 字段 pub(crate):单元 2(soft_delete_via_system 写入)/3/4 消费。
#[derive(Debug, Default)]
pub(crate) struct RecycleNameIndex {
    /// recycle_name -> 墓碑 key(".trash/<date>/<orig>",带前缀)
    pub(crate) by_name: HashMap<String, String>,
    /// original_key(带前缀) -> recycle_name(最新,裁决 R7)
    pub(crate) by_key: HashMap<String, String>,
}

/// 回收站运行状态:墓碑前缀 + 本地索引 + 多端同步调度字段。挂在
/// `ObjectFs.trash` 上(`Option<Arc<TrashState>>`,None = 回收站关闭,硬删除)。
/// 锁纪律:调用方不得跨 await 持有 index 锁;读锁只应在 is_covered 内瞬时
/// 持有;增量/重建遍历期间不持写锁(离线构建 + 短写锁整体换入)。
#[derive(Debug)]
pub(crate) struct TrashState {
    /// 墓碑前缀,如 "ossfs/.trash/"(含命名空间,尾斜杠)
    pub prefix: String,
    /// 本地索引(files + dirs)
    pub index: RwLock<TombstoneIndex>,
    /// lazy(默认)| eager —— eager 档每次 list/stat 前先增量刷一遍 .trash
    pub(crate) mode: TrashRefreshMode,
    /// 增量拉取周期(refresh_loop 的循环节拍;normalize 默认 30s)
    pub(crate) refresh_interval: Duration,
    /// 全量重建周期(兜底被恢复/被 GC 移除的墓碑;默认 600s)
    pub(crate) rebuild_interval: Duration,
    /// 后台 GC 周期(挂载时立即 GC 一次后按此周期循环;normalize 默认 24h)
    pub(crate) gc_interval: Duration,
    /// 保留期天数(`--trash-retention-days`;trash_gc 的 cutoff 消费,
    /// build_trash_state 从 normalized config 传入,默认 30)
    pub(crate) retention_days: u32,
    /// 索引代际(L4):trash_gc(非 dry-run)完成后 fetch_add —— 并发中的
    /// refresh/rebuild 在 apply 前检测代际变化,整体丢弃陈旧快照(否则
    /// 会把 GC 刚删的墓碑重插回索引,隐藏至下轮全量重建)。
    pub(crate) generation: AtomicU64,
    /// 增量游标 = 最后见过的墓碑 key(ListObjectsV2 start-after 参数)
    pub(crate) cursor: Mutex<Option<String>>,
    /// 上次全量重建时刻(含 bootstrap);未 bootstrap 前构造为「早已过期」,
    /// 首次 refresh_once 直接全量重建(挂载 bootstrap 失败自愈路径)。
    pub(crate) last_full_rebuild: Mutex<Instant>,
    /// start-after 自动探测:true=store 遵守 start-after;探测到被忽略
    /// → false,此后增量退化为全量(每轮全量,insert 幂等,正确性不损)。
    pub(crate) start_after_supported: AtomicBool,
    /// eager 节流:距上次 < TRASH_EAGER_MIN_POLL_INTERVAL 跳过本轮
    pub(crate) last_eager_poll: Mutex<Instant>,
    /// 增量/重建互斥:swap(true) 抢锁,失败即跳过(天然限 1 —— eager 挂点
    /// 不 acquire limiter permit,靠它防并发放大)。
    pub(crate) poll_inflight: AtomicBool,
    /// gauge:索引条目数(files+dirs)。统一经 [`Self::store_index_entries`]
    /// store(含超阈值告警,裁决 #6/#9),`ObjectFs::metrics()` 注入
    /// snapshot(prefetch_inflight 先例,§0.3)。
    pub index_entries: AtomicU64,
    /// 系统回收站虚拟视图(None = 不渲染)。配置/平台注入见
    /// build_trash_state(mod.rs);测试直接置字段定制。
    pub(crate) system: Option<SystemTrash>,
    /// Windows $R 名 ↔ 墓碑的反向索引(裁决 R3)
    pub(crate) recycle_names: RwLock<RecycleNameIndex>,
    /// Windows:见过的 SID 段(裁决 R14,list("$Recycle.Bin") 渲染);
    /// macOS 不使用
    pub(crate) seen_sids: RwLock<HashSet<String>>,
}

/// 分页全量拉取 .trash 全部 key 并离线构建新索引(不换入、不失效缓存)。
/// **单一全量列表逻辑**(裁决 #11:消除 bootstrap/rebuild 三处全量逻辑
/// 并存;`ObjectFs::rebuild_trash_index` 与 [`TrashState::full_rebuild`]
/// 共用,各自只做换入/游标/diff 等后续处理)。返回 (新索引, 最后 key)。
/// 调用方负责持 limiter permit;列表期间不持 index 锁,读路径继续用
/// 旧索引。
pub(crate) async fn fetch_all_tombstones(
    fs: &ObjectFs,
    trash: &TrashState,
) -> Result<(TombstoneIndex, Option<String>)> {
    let prefix = trash.prefix.clone();
    let mut index = TombstoneIndex::default();
    let mut last_key: Option<String> = None;
    list_trash_keys(fs, None, None, |page| {
        for key in page {
            if let Some(t) = decode_tombstone_key(&prefix, &key) {
                index.insert(&t.original_key, t.is_dir, t.date);
            }
            last_key = Some(key);
        }
        Ok(())
    })
    .await?;
    Ok((index, last_key))
}

impl TrashState {
    /// 索引规模是否超告警阈值(裁决 #6):超过 TRASH_INDEX_ALERT_THRESHOLD
    /// 仅告警不换行为 —— 不换入/清缓存会让已删文件可见性偏离远端(正确性
    /// 优先),告警让 full_rebuild 的 diff 内存尖峰可观测,缓解手段是
    /// GC/trash-clean。纯函数化便于断言(阈值规范:新阈值落地带验证)。
    fn index_size_alert(len: usize) -> bool {
        len > crate::ossfs::TRASH_INDEX_ALERT_THRESHOLD
    }

    /// gauge 统一落点(裁决 #6/#9):store + 超阈值告警。所有索引变更
    /// (软删/增量/重建/清墓碑)经此更新,告警随任何增长路径触发。
    pub(crate) fn store_index_entries(&self, len: usize) {
        self.index_entries.store(len as u64, Ordering::Relaxed);
        if Self::index_size_alert(len) {
            tracing::warn!(
                index_entries = len,
                threshold = crate::ossfs::TRASH_INDEX_ALERT_THRESHOLD,
                "trash index above alert threshold; diff 重建内存尖峰可见,请评估 trash-clean/GC"
            );
        }
    }

    /// 构造(含调度字段默认值)。refresh_interval/mode 由 connect 读
    /// normalized config 传入;rebuild_interval/gc_interval/retention_days
    /// 传常量或 config(保留期 H1:--trash-retention-days 的消费点)。
    /// 测试经 `new` 或直接改 pub(crate) 字段定制(eager 档、重建周期强制)。
    pub(crate) fn new(
        prefix: String,
        mode: TrashRefreshMode,
        refresh_interval: Duration,
        rebuild_interval: Duration,
        gc_interval: Duration,
        retention_days: u32,
    ) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            prefix,
            index: RwLock::new(TombstoneIndex::default()),
            mode,
            refresh_interval,
            rebuild_interval,
            gc_interval,
            retention_days,
            generation: AtomicU64::new(0),
            cursor: Mutex::new(None),
            // 未 bootstrap 前视为「全量早已过期」:首次 refresh_once 直接全量
            last_full_rebuild: Mutex::new(now.checked_sub(rebuild_interval).unwrap_or(now)),
            start_after_supported: AtomicBool::new(true),
            last_eager_poll: Mutex::new(
                now.checked_sub(TRASH_EAGER_MIN_POLL_INTERVAL)
                    .unwrap_or(now),
            ),
            poll_inflight: AtomicBool::new(false),
            index_entries: AtomicU64::new(0),
            system: None,
            recycle_names: RwLock::new(RecycleNameIndex::default()),
            seen_sids: RwLock::new(HashSet::new()),
        })
    }

    /// 一轮调度:距上次全量 >= rebuild_interval → full_rebuild,否则
    /// poll_incremental。测试与挂载 refresh_loop 共用(经
    /// [`ObjectFs::trash_refresh_once`] 转发)。全量重建只经此入口
    /// (后台循环);eager 挂点直接调 poll_incremental,不触全量分支
    /// (裁决 #1)。
    /// 入口统一 poll_inflight 互斥(裁决 #7):周期循环与 eager 挂点共用
    /// 一把锁 —— 被占用(swap=true)即本轮跳过,防两轮全量/增量并发
    /// (双倍 S3 成本);RAII 保证 await 取消后互斥位复位。
    pub(crate) async fn refresh_once(&self, fs: &ObjectFs) -> Result<()> {
        if self.poll_inflight.swap(true, Ordering::SeqCst) {
            return Ok(()); // 已在跑(失败即跳过,天然限 1)
        }
        let _guard = InflightGuard(&self.poll_inflight);
        if self.full_rebuild_due() {
            self.full_rebuild(fs).await
        } else {
            self.poll_incremental(fs).await
        }
    }

    /// 全量重建是否到期(距上次全量 >= rebuild_interval)。
    fn full_rebuild_due(&self) -> bool {
        let last = self.last_full_rebuild.lock().unwrap();
        last.elapsed() >= self.rebuild_interval
    }

    /// 应用新增墓碑:短写锁批量 insert + 锁外缓存失效 + gauge(锁不跨
    /// await;insert 幂等,同名多日期墓碑只留最新一条)。
    /// 代际校验(L4):调用方传入开始拉取时捕获的 generation_snapshot,若
    /// 期间 trash_gc 已完成(代际推进)则本轮快照已陈旧 —— 其中可能含
    /// GC 刚删掉的墓碑,整体丢弃返回 false(游标不推进,下轮重试),绝不
    /// 把已删墓碑重插回索引。
    /// F7(裁决 R3②):Windows 平台对 diff 新增墓碑按需 GET body 填充
    /// recycle_names —— 请求数 = 新增数,天然有界;兑现「远端软删后本端
    /// 首次 list 回收站暖路径零逐个 GET」。远端 I/O 全部在锁外完成、仅
    /// 收集(不 mutate),提交前二次代际校验 —— GET 期间 GC 并发完成则
    /// 整体丢弃(任何状态未变更)。macOS 恒零额外请求(basename 扫描
    /// 不需要反向索引,裁决 P1)。与 poll_incremental 的刷新路径约定
    /// 一致:不 acquire limiter permit(eager 挂点调用方已持,双 acquire
    /// 违背 #55 纪律;刷新并发由 poll_inflight 天然限 1)。
    async fn apply_added(
        &self,
        fs: &ObjectFs,
        added: &[(String, bool, chrono::NaiveDate)],
        gen_snapshot: u64,
    ) -> Result<bool> {
        if self.generation.load(Ordering::SeqCst) != gen_snapshot {
            return Ok(false);
        }
        if added.is_empty() {
            return Ok(true);
        }
        // F7:新增墓碑的 recycle_names 填充候选(仅 Windows;锁外 GET)。
        let mut name_fills: Vec<(String, String, String)> = Vec::new();
        if self
            .system
            .as_ref()
            .is_some_and(|s| s.platform == SystemTrashPlatform::WindowsRecycleBin)
        {
            for (key, is_dir, date) in added {
                let tomb_key = encode_tombstone_key(&self.prefix, *date, key, *is_dir);
                if let Some(body) = self.read_tombstone(fs, &tomb_key).await?
                    && let Some(name) = &body.recycle_name
                {
                    name_fills.push((tomb_key, name.clone(), key.clone()));
                }
            }
            // GET 期间 GC 并发完成 → 陈旧快照整体丢弃(任何状态未变更)
            if self.generation.load(Ordering::SeqCst) != gen_snapshot {
                return Ok(false);
            }
        }
        {
            let mut idx = self.index.write().unwrap();
            // 裁决 #13:锁内一次性整体换入 —— dirs 批量 extend + 按 key 排序
            // + 去重(保留最新 date,裁决 R4),取代逐条 binary_search+insert
            // 的 O(m×n) memmove(数十万 dirs + 上千新增时写锁持续毫秒到秒级,
            // 期间全部 list/stat 读锁被阻塞)。mem::take 零克隆换出原 Vec,
            // 排序期间读侧被写锁遮挡;files 由 HashMap 批量 entry(O(m))。
            let mut dirs = std::mem::take(&mut idx.dirs);
            dirs.extend(
                added
                    .iter()
                    .filter(|(_, is_dir, _)| *is_dir)
                    .map(|(k, _, date)| {
                        if k.ends_with('/') {
                            (k.clone(), *date)
                        } else {
                            (format!("{k}/"), *date)
                        }
                    }),
            );
            dirs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let mut deduped: Vec<(String, chrono::NaiveDate)> = Vec::with_capacity(dirs.len());
            for (k, date) in dirs {
                match deduped.last_mut() {
                    Some(last) if last.0 == k => {
                        if date > last.1 {
                            last.1 = date;
                        }
                    }
                    _ => deduped.push((k, date)),
                }
            }
            idx.dirs = deduped;
            for (key, is_dir, date) in added {
                if !*is_dir {
                    idx.files
                        .entry(key.clone())
                        .and_modify(|d| {
                            if *date > *d {
                                *d = *date;
                            }
                        })
                        .or_insert(*date);
                }
            }
            self.store_index_entries(idx.len());
        }
        // 提交点后:recycle_names 与墓碑同生命周期(裁决 R3 ②;仅插入
        // 未被覆盖的 key —— 本地软删的映射优先,裁决 R7 最新优先)。
        for (tomb_key, name, key) in name_fills {
            let mut names = self.recycle_names.write().unwrap();
            if !names.by_key.contains_key(&key) {
                names.by_name.insert(name.clone(), tomb_key);
                names.by_key.insert(key, name);
            }
        }
        for (key, is_dir, _) in added {
            invalidate_key(fs, key, *is_dir);
        }
        Ok(true)
    }

    /// 增量刷新,两阶段(裁决 #2):
    /// 1) 当前 UTC 日期分区始终完整扫描(prefix = trash_prefix + today,
    ///    无 start-after)—— 同日新墓碑的 key 字典序可能 ≤ 游标,游标
    ///    增量永远追不上,分区全量扫描是唯一不漏的保证(分区 = 单日墓碑,
    ///    成本有界);
    /// 2) 其余分区走 start-after 游标增量(跳过今天分区,避免双扫)。
    /// start-after 被忽略的探测:返回 key 含 ≤ 游标者即当轮转全量重建,
    /// trash_start_after_ignored+1;此后退化为「今天分区扫描 + 受
    /// last_full_rebuild 周期节流的全量兜底」(裁决 #1)。成功时游标推进
    /// 到最后 key,trash_refresh_incrementals+1。
    pub(crate) async fn poll_incremental(&self, fs: &ObjectFs) -> Result<()> {
        // 代际捕获(L4):本轮 S3 快照的基准 —— apply 前代际变化(GC 并发
        // 完成)则整轮丢弃。
        let gen_snapshot = self.generation.load(Ordering::SeqCst);
        let today = date_partition_utc(SystemTime::now());
        let today_prefix = format!("{}{}/", self.prefix, today);
        let prefix = self.prefix.clone();
        let mut added: Vec<(String, bool, chrono::NaiveDate)> = Vec::new();
        let mut last_key_phase1: Option<String> = None;
        // 阶段一:今天分区完整扫描(游标无关)
        list_trash_keys(fs, None, Some(&today_prefix), |page| {
            for key in page {
                if let Some(t) = decode_tombstone_key(&prefix, &key) {
                    added.push((t.original_key, t.is_dir, t.date));
                }
                last_key_phase1 = Some(key);
            }
            Ok(())
        })
        .await?;

        if !self.start_after_supported.load(Ordering::SeqCst) {
            // 已探测到 store 不支持 start-after → 其余分区只能靠全量兜底,
            // 且受 last_full_rebuild 周期节流(裁决 #1):eager 挂点每分钟
            // 不再数百次全量,最坏每 rebuild_interval 一次(正确性不损,
            // insert 幂等)。今天分区新增始终应用(游标无关,已由阶段一
            // 保证同日删除 ≤1s 内可见)。
            // 今天分区新增始终应用(游标无关,已由阶段一保证同日删除 ≤1s
            // 内可见);代际变化(GC 并发)时丢弃本轮,下轮重试。
            if self.apply_added(fs, &added, gen_snapshot).await? {
                if self.full_rebuild_due() {
                    self.full_rebuild(fs).await?;
                } else {
                    fs.metrics
                        .trash_refresh_incrementals
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            return Ok(());
        }

        let cursor = self.cursor.lock().unwrap().clone();
        let mut last_key_phase2: Option<String> = None;
        let mut ignored = false;
        // 阶段二:游标增量扫其余分区(跳过今天分区 —— 阶段一已完整覆盖)
        list_trash_keys(fs, cursor.as_deref(), None, |page| {
            for key in page {
                // 探测必须先于分区跳过:返回任何 ≤ 游标的 key(含今天分区
                // 新墓碑 —— 符合规范的 store 在 start-after=游标下不会
                // 返回它们)即证明 store 忽略 start-after
                if !ignored
                    && let Some(c) = &cursor
                    && key.as_str() <= c.as_str()
                {
                    ignored = true;
                }
                if key.starts_with(&today_prefix) {
                    continue; // 今天分区已由阶段一完整覆盖
                }
                if let Some(t) = decode_tombstone_key(&prefix, &key) {
                    added.push((t.original_key, t.is_dir, t.date));
                }
                last_key_phase2 = Some(key);
            }
            Ok(())
        })
        .await?;

        if ignored {
            // store 忽略 start-after → 当轮转全量重建(游标不可信)
            self.start_after_supported.store(false, Ordering::SeqCst);
            fs.metrics
                .trash_start_after_ignored
                .fetch_add(1, Ordering::Relaxed);
            return self.full_rebuild(fs).await;
        }

        // 代际校验(L4):GC 并发完成 → 丢弃本轮(游标不推进,下轮从旧
        // 游标重扫,避免把 GC 刚删的墓碑重插回索引)。
        if !self.apply_added(fs, &added, gen_snapshot).await? {
            return Ok(());
        }
        // 游标推进:阶段二有非今天分区的最后 key 用它,否则用阶段一最后
        // key(今天分区尽头)—— 保证后续阶段二不重复扫描今天分区。
        let new_cursor = last_key_phase2.or(last_key_phase1);
        if let Some(k) = new_cursor {
            *self.cursor.lock().unwrap() = Some(k);
        }
        fs.metrics
            .trash_refresh_incrementals
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 全量 list + diff(旧索引 entries() vs 远端):新增 insert + 缓存失效、
    /// 移除 remove + 缓存失效(否则 stats 正向缓存 ≤3s TTL 内旧文件仍可见);
    /// 单轮 diff > [`FULL_REBUILD_DIFF_CLEAR_THRESHOLD`] 条 → 整体清空
    /// stats/negative 缓存(防瞬时 stat 风暴);cursor=最后 key;
    /// trash_refresh_rebuilds+1。
    pub(crate) async fn full_rebuild(&self, fs: &ObjectFs) -> Result<()> {
        // 代际捕获(L4):快照基准 —— 列表期间 GC 完成则整体丢弃,
        // last_full_rebuild 不推进,下轮重试。
        let gen_snapshot = self.generation.load(Ordering::SeqCst);
        let prev = self.index.read().unwrap().entries();
        // 全量列表逻辑统一走 fetch_all_tombstones(裁决 #11)
        let (index, last_key) = fetch_all_tombstones(fs, self).await?;
        if self.generation.load(Ordering::SeqCst) != gen_snapshot {
            // GC 并发完成:本快照可能含刚被删的墓碑,丢弃(L4);不推进
            // last_full_rebuild —— 下轮 refresh 重试。
            return Ok(());
        }
        // diff(裁决 #6 内存尖峰守卫):prev 与 new 各自 sort_unstable(原地、
        // 不分配)后归并扫描 —— 省两个 HashSet(500k 条目时每 HashSet 约
        // 16B/桶 + 哈希运算,归并扫描峰值约减半)。语义与 HashSet 差集
        // 等价:removed = prev - new(被恢复/被 GC 移除),added = new - prev。
        // 裁决 R4:entries 带 date —— 同名同形态跨日期条目在归并中自然产生
        // removed+added 对(缓存失效重复一次无害;索引以 new 整体换入)。
        let mut prev_sorted = prev;
        prev_sorted.sort_unstable();
        let mut new_sorted = index.entries();
        new_sorted.sort_unstable();
        let mut removed: Vec<(String, bool, chrono::NaiveDate)> = Vec::new();
        let mut added: Vec<(String, bool, chrono::NaiveDate)> = Vec::new();
        let (mut i, mut j) = (0usize, 0usize);
        while i < prev_sorted.len() && j < new_sorted.len() {
            match prev_sorted[i].cmp(&new_sorted[j]) {
                CmpOrdering::Equal => {
                    i += 1;
                    j += 1;
                }
                CmpOrdering::Less => {
                    removed.push(prev_sorted[i].clone());
                    i += 1;
                }
                CmpOrdering::Greater => {
                    added.push(new_sorted[j].clone());
                    j += 1;
                }
            }
        }
        removed.extend(prev_sorted.into_iter().skip(i));
        added.extend(new_sorted.into_iter().skip(j));
        let diff_total = removed.len() + added.len();
        // F7(裁决 R3②):新增墓碑读 body 填充 recycle_names(仅 Windows,
        // 请求数 = 新增数,天然有界;兑现远端软删后本端首次 list 回收站
        // 暖路径零逐个 GET)。与刷新路径约定一致不 acquire permit(见
        // apply_added 注释)。新增墓碑读 body 之前已过代际校验(第 661 行),
        // 填充失败仅 warn 级影响(下次冷路径兜底),不 fail 重建。
        if self
            .system
            .as_ref()
            .is_some_and(|s| s.platform == SystemTrashPlatform::WindowsRecycleBin)
        {
            for (key, is_dir, date) in &added {
                let tomb_key = encode_tombstone_key(&self.prefix, *date, key, *is_dir);
                if let Ok(Some(body)) = self.read_tombstone(fs, &tomb_key).await
                    && let Some(name) = &body.recycle_name
                {
                    let mut names = self.recycle_names.write().unwrap();
                    if !names.by_key.contains_key(key) {
                        names.by_name.insert(name.clone(), tomb_key);
                        names.by_key.insert(key.clone(), name.clone());
                    }
                }
            }
        }
        // 短写锁整体换入:diff 计算期间不持锁
        *self.index.write().unwrap() = index;
        self.store_index_entries(self.index.read().unwrap().len());
        *self.cursor.lock().unwrap() = last_key;
        *self.last_full_rebuild.lock().unwrap() = Instant::now();
        // F9(medium):removed 列表走 remove_tombstone_maps —— 远端
        // restore/GC 移除的墓碑在 by_name/by_key 不得残留(修复前整体
        // 换入索引时映射残留:视图条目消失但同名 $R 再软删前 resolve
        // 命中陈旧映射,下游 404 自愈;内存泄漏)。须在索引换入之后
        // (still_covered 以新索引判定:同名多日期墓碑留新时映射仍有效)。
        for (key, is_dir, date) in &removed {
            let tomb_key = encode_tombstone_key(&self.prefix, *date, key, *is_dir);
            self.remove_tombstone_maps(&[tomb_key], key);
        }
        if diff_total > FULL_REBUILD_DIFF_CLEAR_THRESHOLD {
            fs.stats.lock().unwrap().clear();
            fs.negative.lock().unwrap().clear();
        } else {
            for (key, is_dir, _) in removed.iter().chain(added.iter()) {
                invalidate_key(fs, key, *is_dir);
            }
        }
        fs.metrics
            .trash_refresh_rebuilds
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// eager 入口(list/stat 挂点):节流(距上次 <
    /// TRASH_EAGER_MIN_POLL_INTERVAL 跳过)+ poll_inflight 互斥 + 错误仅
    /// warn(不 fail 上层 list/stat);**只做增量**(直接调 poll_incremental,
    /// 剥离 refresh_once 的 rebuild_due 全量重建分支 —— 全量重建只留
    /// 后台 refresh_loop,裁决 #1:热路径不得全量重建);降级时受
    /// last_full_rebuild 周期节流;不 acquire limiter permit —— 调用方
    /// (list/stat)已持 permit,再等第二把会在饱和池死锁;靠 poll_inflight
    /// 天然限 1。
    pub(crate) async fn poll_incremental_eager(&self, fs: &ObjectFs) {
        if self.mode != TrashRefreshMode::Eager {
            return; // lazy 零开销
        }
        // 节流:1s 内最多一次增量拉取(连续 list/stat 只放大一份工作)
        {
            let mut last = self.last_eager_poll.lock().unwrap();
            if last.elapsed() < TRASH_EAGER_MIN_POLL_INTERVAL {
                return;
            }
            *last = Instant::now();
        }
        // 互斥:上一次增量/重建未结束 → 本轮跳过
        if self.poll_inflight.swap(true, Ordering::SeqCst) {
            return;
        }
        // RAII:中途被取消(drop 上层 list/stat future)也复位互斥位,
        // 避免永久卡死后续 eager 轮询
        let _guard = InflightGuard(&self.poll_inflight);
        let result = self.poll_incremental(fs).await;
        if let Err(e) = result {
            fs.metrics
                .trash_refresh_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %e, "trash eager refresh failed; will retry");
        }
    }
}

// ---------- 单元 1:系统回收站路径识别(同步,零远程) ----------

impl TrashState {
    /// 纯字符串判定:path 命中系统前缀 0/1/2 层(Dir 或 Entry)。边界:
    /// ".Trashesx" 不误伤;与 ".trash" 前缀互不干扰;trash 关闭时恒 false。
    pub(crate) fn is_system_trash_path(&self, path: &str) -> bool {
        self.match_system_trash(path).is_some()
    }

    /// 路径位于系统回收站目录名下但 match 未命中(范围外 uid / 非数字
    /// uid 段 / >2 层深层路径)。delete 的硬删除分派用(F16):此类路径
    /// 不得软删 —— 墓碑会以 basename 渲染进范围内 uid 视图(跨 uid
    /// 数据可见,与裁决 R17「范围外按普通路径、不产生无视图对应的
    /// 墓碑」冲突)。纯字符串,零远程;恰为根目录名本身(level 0)返回
    /// false(另有分支处理)。
    pub(crate) fn is_system_trash_named_path(&self, path: &str) -> bool {
        let Some(sys) = &self.system else {
            return false;
        };
        let Some((first, _)) = path.trim_matches('/').split_once('/') else {
            return false;
        };
        first == sys.dir_name
    }

    /// 结构识别(同步零远程):段切分后精确匹配 dir_name + 固定段数。
    /// "$Recycle.Bin/<SID>/<name>" 第 3 段为条目名;更深(>2 层)不拦截、
    /// 原样可见(风险:桶中真实用户数据,裁决:深层文件不隐藏)。第二段
    /// Windows 任意接受(SID 只是视图分区,跨段共享同一墓碑集,裁决 R14);
    /// macOS 先经 uid 范围过滤(裁决 R17:仅 macos_uid_dirs,空 = 当前
    /// 用户 uid;非数字 uid 不拦截),范围外返回 None 走普通路径。
    pub(crate) fn match_system_trash(&self, path: &str) -> Option<SystemTrashMatch> {
        let sys = self.system.as_ref()?;
        let dir_name = sys.dir_name.as_str();
        let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
        match segments.as_slice() {
            [d] if *d == dir_name => Some(SystemTrashMatch::Dir { level: 0 }),
            [d, uid] if *d == dir_name && uid_in_scope(sys, uid) => {
                Some(SystemTrashMatch::Dir { level: 1 })
            }
            [d, uid, name] if *d == dir_name && uid_in_scope(sys, uid) => {
                Some(SystemTrashMatch::Entry {
                    entry_name: (*name).to_string(),
                })
            }
            _ => None,
        }
    }

    /// Windows:mkdir/rename 拦截时记录 SID 段(裁决 R14)。仅
    /// WindowsRecycleBin 平台记录;mkdir 命中 SID 目录(level 1)、rename
    /// 软删命中条目(Entry)都记录第 2 段。macOS 不使用。幂等(集合去重)。
    pub(crate) fn record_seen_sid(&self, path: &str) {
        let Some(sys) = &self.system else { return };
        if sys.platform != SystemTrashPlatform::WindowsRecycleBin {
            return;
        }
        let sid = matches!(
            self.match_system_trash(path),
            Some(SystemTrashMatch::Dir { level: 1 }) | Some(SystemTrashMatch::Entry { .. })
        )
        .then(|| path.trim_matches('/').split('/').nth(1))
        .flatten();
        if let Some(sid) = sid
            && !sid.is_empty()
        {
            self.seen_sids.write().unwrap().insert(sid.to_string());
        }
    }

    /// 软删(rename 目标在回收站条目层,单元 2):判定源形态后分派
    /// soft_delete_file_impl / soft_delete_dir_impl,带 recycle_name
    /// (Windows 恒写 $R 名,Explorer 还原按此 rename;macOS 仅 Finder
    /// 改名时写,与 basename 一致时 None,渲染回退 basename)。
    /// 幂等:索引已覆盖 → Ok(零远程)。**调用方已持 limiter permit**
    /// (rename 拦截持有,#55 纪律:内部不再 acquire)。
    pub(crate) async fn soft_delete_via_system(
        &self,
        fs: &ObjectFs,
        old: &str,
        entry_name: &str,
    ) -> Result<()> {
        let key = fs.key_for(old);
        // 幂等:索引已覆盖 → Ok(零远程)。is_covered 双形态(文件精确 +
        // 目录前缀),隐藏条目在视图不可见,rename 不应到达。
        if self
            .index
            .read()
            .unwrap()
            .is_covered(key.trim_end_matches('/'))
        {
            return Ok(());
        }
        // 源形态判定:文件 vs 目录(目录无 marker,HEAD 404 不可判 —— 目录
        // 墓碑形如 ".trash/<date>/<key>/",故必须 stat 一次;命中 3s stat
        // 缓存时零额外请求)。
        let stat = fs.stat_uncached_impl(old).await?;
        let Some(entry) = stat else {
            anyhow::bail!("rename: source not found: {old}");
        };
        let recycle_name = match self.system.as_ref().expect("system view").platform {
            SystemTrashPlatform::WindowsRecycleBin => Some(entry_name.to_string()),
            SystemTrashPlatform::MacOsTrashes => {
                (entry_name != basename(&key)).then(|| entry_name.to_string())
            }
        };
        if entry.is_dir {
            self.soft_delete_dir_impl(fs, old, recycle_name).await
        } else {
            self.soft_delete_file_impl(fs, old, recycle_name).await
        }
    }

    /// 条目名 → (原 key, 墓碑 key, is_dir)。反查顺序(裁决 R3):
    /// ① by_name 命中 → 墓碑 key → decode(零远程);
    /// ② macOS:索引内 basename 扫描(同名多日期/重名取最新,裁决 R7;
    ///    重名歧义 warn;零远程);
    /// ③ Windows 冷路径(cold_scan=true):对 by_key 未覆盖的索引条目按需
    ///    GET 墓碑 body 扫描填充(请求数 ≤ 未命中条目数,天然有界)。
    /// **调用方已持 limiter permit**(冷路径 GET 走 read_tombstone);
    /// read_range 的系统分支在调用前 acquire。pub(crate):单元 2 的
    /// WithinRecycle no-op 源存在性校验(mod.rs,F14)以 cold_scan=false
    /// 零远程调用。
    pub(crate) async fn resolve_entry(
        &self,
        fs: &ObjectFs,
        entry_name: &str,
        cold_scan: bool,
    ) -> Result<Option<ResolvedEntry>> {
        let Some(sys) = &self.system else {
            return Ok(None);
        };
        // ① by_name 命中:零远程
        if let Some(tomb_key) = self
            .recycle_names
            .read()
            .unwrap()
            .by_name
            .get(entry_name)
            .cloned()
        {
            if let Some(t) = decode_tombstone_key(&self.prefix, &tomb_key) {
                return Ok(Some(ResolvedEntry {
                    original_key: t.original_key,
                    tomb_key,
                    is_dir: t.is_dir,
                }));
            }
        }
        // 索引快照(锁不跨 await)
        let entries: Vec<(String, chrono::NaiveDate, bool)> = {
            let index = self.index.read().unwrap();
            let mut v: Vec<(String, chrono::NaiveDate, bool)> = index
                .files
                .iter()
                .map(|(k, d)| (k.clone(), *d, false))
                .collect();
            v.extend(index.dirs.iter().map(|(k, d)| (k.clone(), *d, true)));
            v
        };
        match sys.platform {
            SystemTrashPlatform::MacOsTrashes => {
                // ② basename 扫描:同名多日期/不同原 key 同名都取最新(裁决 R7)
                let mut best: Option<ResolvedEntry> = None;
                let mut best_date: Option<chrono::NaiveDate> = None;
                let mut candidates: usize = 0;
                for (key, date, is_dir) in &entries {
                    if basename(key) == entry_name {
                        candidates += 1;
                        if best_date.is_none_or(|bd| *date > bd) {
                            best = Some(ResolvedEntry {
                                original_key: key.clone(),
                                tomb_key: encode_tombstone_key(&self.prefix, *date, key, *is_dir),
                                is_dir: *is_dir,
                            });
                            best_date = Some(*date);
                        }
                    }
                }
                if candidates > 1 {
                    tracing::warn!(
                        entry_name,
                        "trash system view: 同名条目存在多个墓碑,取最新(date),还原/读取可能命中重名文件"
                    );
                }
                Ok(best)
            }
            SystemTrashPlatform::WindowsRecycleBin => {
                if !cold_scan {
                    return Ok(None); // stat 路径:未知条目按不存在(P3 零远程)
                }
                // ③ 冷路径:按需 GET body 扫描填充(裁决 R3 ③)
                let _permit = fs.acquire().await?;
                let mut found: Option<ResolvedEntry> = None;
                for (key, date, is_dir) in &entries {
                    let tomb_key = encode_tombstone_key(&self.prefix, *date, key, *is_dir);
                    let Some(body) = self.read_tombstone(fs, &tomb_key).await? else {
                        continue; // 墓碑并发删除:跳过
                    };
                    let Some(name) = body.recycle_name else {
                        continue; // 非系统视图墓碑(普通软删):不填充
                    };
                    let mut names = self.recycle_names.write().unwrap();
                    if !names.by_key.contains_key(key) {
                        names.by_name.insert(name.clone(), tomb_key.clone());
                        names.by_key.insert(key.clone(), name.clone());
                    }
                    drop(names);
                    if name == entry_name {
                        found = Some(ResolvedEntry {
                            original_key: key.clone(),
                            tomb_key,
                            is_dir: *is_dir,
                        });
                        break;
                    }
                }
                Ok(found)
            }
        }
    }

    /// 条目 → 原 key(带命名空间前缀)。Windows:by_name 命中零远程;未命中
    /// 按需 GET 墓碑 body 扫描填充(裁决 R3 ③)。macOS:索引内 basename 扫描
    /// (同名多版本取最新,裁决 R7;重名歧义 warn)。None = 反查失败。
    /// **调用方已持 limiter permit**(冷路径 GET);read_range 在调用前 acquire。
    pub(crate) async fn resolve_entry_original(
        &self,
        fs: &ObjectFs,
        entry_name: &str,
    ) -> Result<Option<String>> {
        Ok(self
            .resolve_entry(fs, entry_name, true)
            .await?
            .map(|r| r.original_key))
    }

    /// 目录层列表合成(裁决 R9:目录条目为叶)。Dir level 0 → Windows 渲染
    /// seen_sids(裁决 R14)/macOS 渲染 macos_uid_dirs(空 = 当前用户 uid,
    /// 裁决 R17);level 1 → 遍历索引,条目名 = by_key 命中则用之,否则
    /// basename(original_key);Windows 额外成对生成 $I 条目(裁决 R8,
    /// $R 名换 $I 前缀,size = 合成长度)。Windows 冷路径对 by_key 未覆盖
    /// 条目 GET body 填充(裁决 R3 ③,GET 数 ≤ 未命中条目数,天然有界);
    /// 暖路径零 GET(P1 守卫)。**调用方(list_impl)已持 limiter permit**。
    pub(crate) async fn synthesize_dir_entries(
        &self,
        fs: &ObjectFs,
        dir: &str,
    ) -> Result<Vec<DirEntry>> {
        let Some(sys) = &self.system else {
            return Ok(Vec::new());
        };
        match self.match_system_trash(dir) {
            Some(SystemTrashMatch::Dir { level: 0 }) => {
                let mut out: Vec<DirEntry> = Vec::new();
                match sys.platform {
                    SystemTrashPlatform::WindowsRecycleBin => {
                        // 裁决 R14:seen_sids ∪ 当前用户 SID(尽力)。当前用户
                        // SID 获取(GetTokenInformation)[待验证],非 Windows
                        // 平台降级为仅 seen_sids(重启后根目录只显示新记录
                        // 的 SID,条目仍可直接访问 SID 路径)。
                        for sid in self.seen_sids.read().unwrap().iter() {
                            out.push(DirEntry {
                                name: sid.clone(),
                                is_dir: true,
                                size: 0,
                                mtime_secs: 0,
                            });
                        }
                    }
                    SystemTrashPlatform::MacOsTrashes => {
                        let uids: Vec<u32> = if sys.macos_uid_dirs.is_empty() {
                            vec![current_uid()]
                        } else {
                            sys.macos_uid_dirs.clone()
                        };
                        for uid in uids {
                            out.push(DirEntry {
                                name: uid.to_string(),
                                is_dir: true,
                                size: 0,
                                mtime_secs: 0,
                            });
                        }
                    }
                }
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            }
            Some(SystemTrashMatch::Dir { level: 1 }) => {
                // 索引快照(锁不跨 await;锁纪律见模块头)
                let entries: Vec<(String, chrono::NaiveDate, bool)> = {
                    let index = self.index.read().unwrap();
                    let mut v: Vec<(String, chrono::NaiveDate, bool)> = index
                        .files
                        .iter()
                        .map(|(k, d)| (k.clone(), *d, false))
                        .collect();
                    v.extend(index.dirs.iter().map(|(k, d)| (k.clone(), *d, true)));
                    v
                };
                // F16:macOS 渲染兜底 —— 泄漏墓碑(原 key 落在
                // ".Trashes/" 前缀下且 uid 段 ≠ 渲染层)不显示;正常数据
                // 路径(原 key 在 .Trashes/ 外)不受影响。Windows 不做过滤
                // (跨 SID 共享同一墓碑集,裁决 R14/规格 §4.3 测试 5)。
                let mac_filter: Option<(String, String)> = match sys.platform {
                    SystemTrashPlatform::MacOsTrashes => {
                        let segments: Vec<&str> = dir.trim_matches('/').split('/').collect();
                        (segments.len() >= 2).then(|| {
                            (
                                format!("{}{}/", fs.prefix, segments[0]),
                                format!("{}{}/{}/", fs.prefix, segments[0], segments[1]),
                            )
                        })
                    }
                    SystemTrashPlatform::WindowsRecycleBin => None,
                };
                // 第一遍:条目名 = by_key 命中则用之,否则 basename(裁决
                // R7 最新优先);Windows 对 by_key 未覆盖条目标记冷路径候选。
                let mut pending: Vec<(String, String, chrono::NaiveDate, bool)> =
                    Vec::with_capacity(entries.len());
                let mut cold: Vec<(String, String, bool)> = Vec::new();
                for (key, date, is_dir) in &entries {
                    if let Some((trash_space, rendered)) = &mac_filter
                        && key.starts_with(trash_space)
                        && !key.starts_with(rendered)
                    {
                        continue; // F16:泄漏到其他 uid 段的墓碑不渲染
                    }
                    let by_key = self.recycle_names.read().unwrap().by_key.get(key).cloned();
                    let name = by_key.clone().unwrap_or_else(|| basename(key));
                    if sys.platform == SystemTrashPlatform::WindowsRecycleBin && by_key.is_none() {
                        cold.push((
                            key.clone(),
                            encode_tombstone_key(&self.prefix, *date, key, *is_dir),
                            *is_dir,
                        ));
                    }
                    pending.push((key.clone(), name, *date, *is_dir));
                }
                // Windows 冷路径:GET body 填充(请求数 ≤ 未命中条目数,
                // P1 守卫);recycle_name None 的墓碑保持 basename 渲染。
                // macOS 恒零额外请求(裁决 P1)。
                if sys.platform == SystemTrashPlatform::WindowsRecycleBin && !cold.is_empty() {
                    for (key, tomb_key, is_dir) in &cold {
                        let Some(body) = self.read_tombstone(fs, tomb_key).await? else {
                            continue; // 墓碑并发删除:保持 basename 渲染
                        };
                        let Some(name) = &body.recycle_name else {
                            continue; // 非系统视图墓碑(普通软删)
                        };
                        let mut names = self.recycle_names.write().unwrap();
                        if !names.by_key.contains_key(key) {
                            names.by_name.insert(name.clone(), tomb_key.clone());
                            names.by_key.insert(key.clone(), name.clone());
                        }
                        drop(names);
                        if let Some(e) = pending
                            .iter_mut()
                            .find(|(k, _, _, d)| k == key && *d == *is_dir)
                        {
                            e.1 = name.clone();
                        }
                    }
                }
                let mut out: Vec<DirEntry> = pending
                    .into_iter()
                    .map(|(_, name, _, is_dir)| DirEntry {
                        name,
                        is_dir,
                        size: 0,
                        mtime_secs: 0,
                    })
                    .collect();
                // Windows 额外成对生成 $I 条目(裁决 R8):$R 名换 $I 前缀,
                // size = 合成长度(确定性,零远程)。目录无 $I。
                if sys.platform == SystemTrashPlatform::WindowsRecycleBin {
                    let pairs: Vec<(String, usize)> = out
                        .iter()
                        .filter(|e| !e.is_dir && e.name.starts_with("$R"))
                        .map(|e| {
                            (
                                format!("$I{}", &e.name[2..]),
                                synthesized_i_len(&self.i_path(fs, &e.name)),
                            )
                        })
                        .collect();
                    for (name, size) in pairs {
                        out.push(DirEntry {
                            name,
                            is_dir: false,
                            size: size as u64,
                            mtime_secs: 0,
                        });
                    }
                }
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// $I 路径字段(挂载盘符 + original_key 视图形态;`/` → `\`)。由条目名
    /// 反查原 key(by_name),未命中退化为条目名本身(仅影响合成长度与字节
    /// 内容;捕获为主时无影响)。
    fn i_path(&self, fs: &ObjectFs, entry_name: &str) -> String {
        let orig = self
            .recycle_names
            .read()
            .unwrap()
            .by_name
            .get(entry_name)
            .and_then(|tk| decode_tombstone_key(&self.prefix, tk))
            .map(|t| t.original_key);
        match orig {
            Some(orig) => i_view_path(fs, &orig),
            None => format!("{}\\{}", SYNTHESIZED_I_DRIVE, entry_name.replace('/', "\\")),
        }
    }

    /// 系统前缀内 stat(裁决 P3):Dir 层合成目录条目(零远程);Entry 层
    /// Windows $I → size = recycle_i 长度(无捕获字节时合成长度);$R/macOS
    /// 条目 → 按需 GET 墓碑 body 取 size(≤1 次,裁决 P3);目录条目零远程
    /// (size 恒 0)。by_name 未命中的条目按不存在处理(cold_scan=false,
    /// 未知条目 stat 零远程 —— P3 的 ≤1 次 GET 只覆盖「已解析」条目)。
    /// **调用方(stat_uncached_impl)已持 limiter permit**。
    pub(crate) async fn synthesize_stat(
        &self,
        fs: &ObjectFs,
        path: &str,
    ) -> Result<Option<DirEntry>> {
        let Some(sys) = &self.system else {
            return Ok(None);
        };
        match self.match_system_trash(path) {
            Some(SystemTrashMatch::Dir { .. }) => Ok(Some(DirEntry {
                name: basename(path),
                is_dir: true,
                size: 0,
                mtime_secs: 0,
            })),
            Some(SystemTrashMatch::Entry { entry_name }) => {
                // Windows $I:size = 捕获字节长度,缺失时合成长度(裁决 R8)。
                // 反查 $R 同名条目(by_name 键是 $R 名),GET ≤1(裁决 P3)。
                if sys.platform == SystemTrashPlatform::WindowsRecycleBin && is_i_entry(&entry_name)
                {
                    let r_name = format!("$R{}", &entry_name[2..]);
                    let Some(resolved) = self.resolve_entry(fs, &r_name, false).await? else {
                        return Ok(None);
                    };
                    let Some(body) = self.read_tombstone(fs, &resolved.tomb_key).await? else {
                        return Ok(None);
                    };
                    let size = body.recycle_i.map(|b| b.len() as u64).unwrap_or_else(|| {
                        synthesized_i_len(&i_view_path(fs, &resolved.original_key)) as u64
                    });
                    return Ok(Some(DirEntry {
                        name: entry_name,
                        is_dir: false,
                        size,
                        mtime_secs: 0,
                    }));
                }
                // $R / macOS 条目:解析(≤1 body GET 由 read_tombstone 承担)
                let Some(resolved) = self.resolve_entry(fs, &entry_name, false).await? else {
                    return Ok(None);
                };
                if resolved.is_dir {
                    return Ok(Some(DirEntry {
                        name: entry_name,
                        is_dir: true,
                        size: 0,
                        mtime_secs: 0,
                    }));
                }
                let Some(body) = self.read_tombstone(fs, &resolved.tomb_key).await? else {
                    return Ok(None);
                };
                Ok(Some(DirEntry {
                    name: entry_name,
                    is_dir: false,
                    size: body.size.unwrap_or(0),
                    mtime_secs: 0,
                }))
            }
            None => Ok(None),
        }
    }

    /// Windows $I 合成(裁决 R8 回退,仅无捕获字节时用):8B 头(0x01)+
    /// 8B size(LE,删除时 HEAD 的 content_length)+ 8B FILETIME(LE,删除
    /// 日期 UTC 午夜)+ 4B 路径字符数(LE)+ UTF-16LE 路径(挂载盘符 +
    /// original_key 视图形态)。长路径变体(8B 长度字段)触发条件
    /// [待验证](捕获为主,风险低)。**调用方已持 limiter permit**
    /// (read_range 系统分支);GET 数 ≤1(反查墓碑 body)。
    pub(crate) async fn synthesize_i_file(
        &self,
        fs: &ObjectFs,
        entry_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        // $I 名 → $R 名(反向索引键;单一推导点 i_to_r_name)
        let Some(r_name) = i_to_r_name(entry_name) else {
            return Ok(None);
        };
        let Some(resolved) = self.resolve_entry(fs, &r_name, true).await? else {
            return Ok(None);
        };
        let Some(body) = self.read_tombstone(fs, &resolved.tomb_key).await? else {
            return Ok(None);
        };
        // 捕获字节优先(裁决 R8:捕获保真,含盘符原始路径)
        if let Some(captured) = &body.recycle_i {
            return Ok(Some(captured.clone()));
        }
        let Some(date) = decode_tombstone_key(&self.prefix, &resolved.tomb_key).map(|t| t.date)
        else {
            return Ok(None);
        };
        // 合成:8B 头(0x01)+ 8B size + 8B FILETIME + 4B 字符数 + UTF-16LE 路径
        let path = i_view_path(fs, &resolved.original_key);
        let size = body.size.unwrap_or(0);
        let filetime = filenum_100ns(date);
        // F11:4B 长度字段按 UTF-16 单元数计($I 格式规格;修复前
        // chars().count() —— emoji 等非 BMP 字符少算,依赖长度字段的
        // 第三方回收站查看器解析截断/错位)。
        let utf16_units = path.encode_utf16().count();
        let mut out = Vec::with_capacity(28 + 2 * utf16_units);
        out.extend_from_slice(&[0x01, 0, 0, 0, 0, 0, 0, 0]); // 8B 头
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&filetime.to_le_bytes());
        out.extend_from_slice(&(utf16_units as u32).to_le_bytes());
        for u in path.encode_utf16() {
            out.extend_from_slice(&u.to_le_bytes());
        }
        Ok(Some(out))
    }

    /// $I 名 → 对应 $R 墓碑是否已在 by_name 反向索引(单元 4 捕获窗口
    /// 判定,裁决 R11 ②:「$I 形态且对应 $R 墓碑存在」的零远程检查)。
    /// 非 $I 形态恒 false;索引未覆盖(如跨客户端软删未刷新)返回 false,
    /// 调用方退化为普通写路径(真实对象)。
    pub(crate) fn i_entry_has_r_tombstone(&self, entry_name: &str) -> bool {
        let Some(r_name) = i_to_r_name(entry_name) else {
            return false;
        };
        self.recycle_names
            .read()
            .unwrap()
            .by_name
            .contains_key(&r_name)
    }

    /// $I 捕获落 body(单元 4,裁决 R8 捕获为主):`entry_name` 为 $I 名,
    /// 反查对应 $R 墓碑 → GET body → 设 recycle_i(>MAX_RECYCLE_I_BYTES
    /// 截断)→ 条件 PUT(if-match 墓碑 etag,F8:窗口内墓碑被并发删除
    /// → 412 → 丢弃字节不复活幽灵)。update 式写:整 body 重写,etag/size
    /// 字段原样保留(serde 往返,保 etag/size)。未命中(捕获丢失,如 $R
    /// 在另一客户端软删而本端索引未刷新)→ warn + no-op —— 不可因 $I
    /// 缺失拒绝 restore(Agent 2 risk 7)。幂等:重入再次捕获覆盖。
    /// 调用方不持 limiter permit(resolve_entry 冷路径内部自持;此处再
    /// 持一个覆盖 read_tombstone + write_tombstone,顺序 acquire 无死锁)。
    pub(crate) async fn set_recycle_i(
        &self,
        fs: &ObjectFs,
        entry_name: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let Some(r_name) = i_to_r_name(entry_name) else {
            return Ok(()); // 非 $I 形态:防御性 no-op(调用方已限定)
        };
        let Some(resolved) = self.resolve_entry(fs, &r_name, true).await? else {
            tracing::warn!(
                entry_name,
                "recycle bin $I capture: 对应 $R 墓碑未解析,recycle_i 不落 body(不阻塞 restore)"
            );
            return Ok(());
        };
        let _permit = fs.acquire().await?;
        let (maybe_body, etag) = self
            .read_tombstone_with_etag(fs, &resolved.tomb_key)
            .await?;
        let Some(mut body) = maybe_body else {
            tracing::warn!(
                tomb_key = %resolved.tomb_key,
                "recycle bin $I capture: 墓碑并发删除,丢弃捕获字节"
            );
            return Ok(());
        };
        body.recycle_i = Some(bytes.into_iter().take(MAX_RECYCLE_I_BYTES).collect());
        // F8:条件写(if-match 墓碑 etag)—— GET→PUT 窗口内墓碑被并发
        // restore/永久删/清空删除 → 412(或部分存储以 404 表达)→ 丢弃
        // 捕获字节,不复活幽灵墓碑。修复前无条件 PUT 会把已删墓碑重新
        // 写回,条目以幽灵形态残留(stat 可合成但 read/restore 404)。
        match self
            .write_tombstone_if_match(fs, &resolved.tomb_key, &body, etag.as_deref())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) if is_conditional_write_failed(&e) => {
                tracing::warn!(
                    tomb_key = %resolved.tomb_key,
                    "recycle bin $I capture: 墓碑在捕获窗口内被删除(条件写失败),丢弃捕获字节,不复活幽灵墓碑"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// 墓碑删除的统一收尾:recycle_names 清理(by_name 中指向已删墓碑键
    /// 的条目;原 key 已无存活墓碑时移除 by_key 映射 —— 多日期墓碑清旧
    /// 留新时映射仍有效,裁决 R7)。seen_sids 不在此清理(条目与 SID 无
    /// 关联,空 SID 目录残留无害,文档化)。挂进 clear_tombstones_both_forms
    /// 收尾与 gc_partition_files/dirs、trash_restore 的删除路径,保证映射
    /// 与索引同生命周期。by_key 的目录键带尾斜杠(insert_recycle_names
    /// 以 dir_key 写入),调用方传裸形态时双形态都移除(单元 3 清墓碑
    /// 传 file_key = trim 后,否则目录映射残留成幽灵)。
    fn remove_tombstone_maps(&self, tombstone_keys: &[String], original_key: &str) {
        let mut names = self.recycle_names.write().unwrap();
        if !tombstone_keys.is_empty() {
            names.by_name.retain(|_, tk| !tombstone_keys.contains(tk));
        }
        let still_covered = self
            .index
            .read()
            .unwrap()
            .is_covered(original_key.trim_end_matches('/'));
        if !still_covered {
            names.by_key.remove(original_key);
            if !original_key.ends_with('/') {
                names.by_key.remove(&format!("{original_key}/"));
            }
        }
    }
}

/// $I 合成的挂载盘符占位(裁决 R8 回退;WinFsp 适配器单元 4 以真实捕获
/// 字节为主,合成仅兜底 —— 盘符不影响 $I 结构字节断言)。
const SYNTHESIZED_I_DRIVE: &str = "C:";

/// $I 路径字段:挂载盘符 + original_key 视图形态(`/` → `\`)。
fn i_view_path(fs: &ObjectFs, original_key: &str) -> String {
    let rel = original_key
        .strip_prefix(fs.prefix.as_str())
        .unwrap_or(original_key);
    format!("{}\\{}", SYNTHESIZED_I_DRIVE, rel.replace('/', "\\"))
}

/// 墓碑原 key(带命名空间前缀)→ 挂载视图路径(目录形态去尾斜杠;
/// delete_dir_recursive_impl 以视图路径取 S3 前缀)。单元 3 永久删用。
fn key_view_path(fs: &ObjectFs, key: &str) -> String {
    let rel = key.strip_prefix(fs.prefix.as_str()).unwrap_or(key);
    format!("/{}", rel.trim_end_matches('/'))
}

/// $I 合成长度(8B 头 + 8B size + 8B FILETIME + 4B 字符数 + UTF-16LE 路径)。
/// 供目录列表的 $I 条目 size(确定性,零远程)。F11:按 UTF-16 单元计数
/// (与 synthesize_i_file 的长度字段一致;$I 格式按 UTF-16 单元计)。
fn synthesized_i_len(path: &str) -> usize {
    8 + 8 + 8 + 4 + 2 * path.encode_utf16().count()
}

/// $I 名称判定:`$I` + 8 位十六进制(单元 4 捕获窗口;单元 1 合成/转发
/// 用)。"$I" 前缀 + 前 8 位 hex 即视为 $I 形态。
pub(crate) fn is_i_entry(name: &str) -> bool {
    name.len() >= 10
        && name.starts_with("$I")
        && name.as_bytes()[2..10].iter().all(u8::is_ascii_hexdigit)
}

/// $I 名 → 对应 $R 名(8 位 hex 后缀互换,裁决 R8 捕获窗口的反查键)。
/// 非 $I 形态返回 None。synthesize_i_file / i_entry_has_r_tombstone /
/// set_recycle_i 共用(单一推导点)。
fn i_to_r_name(name: &str) -> Option<String> {
    if is_i_entry(name) {
        Some(format!("$R{}", &name[2..]))
    } else {
        None
    }
}

/// SDK 错误是否为 412 PreconditionFailed 或 404(if-match 条件写失败
/// —— set_recycle_i 的 F8「无墓碑不复活」判定;S3/OSS 对条件 PUT 的
/// 失配响应为 412,部分兼容存储以 404 表达)。anyhow 链内查找(write_
/// tombstone_if_match 以 context 包装)。消费链(set_recycle_i)仅
/// Windows(winfsp.rs)可达,非 Windows 构建为死代码 —— 与既有
/// MAX_RECYCLE_I_BYTES / set_recycle_i 告警同源,按裁决 F17 口径允许。
#[cfg_attr(not(windows), allow(dead_code))]
fn is_conditional_write_failed(e: &anyhow::Error) -> bool {
    let Some(sdk) = e.downcast_ref::<aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::put_object::PutObjectError,
    >>() else {
        return false;
    };
    match sdk {
        aws_sdk_s3::error::SdkError::ServiceError(err) => {
            let status = err.raw().status().as_u16();
            status == 412 || status == 404
        }
        _ => false,
    }
}

/// 删除日期 → FILETIME(100ns 步进,1601-01-01 起;UTC 午夜)。$I 合成的
/// FILETIME 字段(裁决 R8 回退;捕获为主时不用)。
fn filenum_100ns(date: chrono::NaiveDate) -> u64 {
    let unix = date
        .and_hms_opt(0, 0, 0)
        .map(|d| d.and_utc().timestamp())
        .unwrap_or(0);
    ((unix as u128 + 11_644_473_600u128) * 10_000_000u128) as u64
}

/// 当前挂载用户 uid(macOS 视图渲染;空 macos_uid_dirs = 当前 uid,裁决
/// R17)。Windows 平台不渲染 MacOsTrashes 视图(平台形态由 cfg 决定),
/// 兜底 0。
#[cfg(not(windows))]
fn current_uid() -> u32 {
    // SAFETY:geteuid 无参数、无副作用、无失败。
    unsafe { libc::geteuid() }
}

#[cfg(windows)]
fn current_uid() -> u32 {
    0
}

/// 裁决 R17:macOS `.Trashes/<uid>` 段的渲染/拦截范围判定。Windows SID
/// 段任意接受(非数字也不拦截 —— SID 形态本身非数字);macOS 仅
/// macos_uid_dirs(空 = 当前挂载用户 uid)内命中,非数字 uid 不拦截。
fn uid_in_scope(sys: &SystemTrash, uid: &str) -> bool {
    if sys.platform != SystemTrashPlatform::MacOsTrashes {
        return true;
    }
    let Ok(uid) = uid.parse::<u32>() else {
        return false;
    };
    if sys.macos_uid_dirs.is_empty() {
        uid == current_uid()
    } else {
        sys.macos_uid_dirs.contains(&uid)
    }
}

/// 条目解析结果:原 key + 墓碑 key + 目录形态(全部带命名空间前缀)。
/// pub(crate):resolve_entry 被 mod.rs(F14)以 cold_scan=false 调用。
#[derive(Debug, Clone)]
pub(crate) struct ResolvedEntry {
    original_key: String,
    tomb_key: String,
    is_dir: bool,
}

/// poll_inflight 的 RAII 复位(防 await 取消后互斥位永久置位)。
struct InflightGuard<'a>(&'a AtomicBool);

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// 全量重建 diff 超此条数 → 整体清空 stats/negative 缓存,防瞬时 stat
/// 风暴(规格 §3.2 full_rebuild)。内部阈值,随主 commit 落地。
const FULL_REBUILD_DIFF_CLEAR_THRESHOLD: usize = 1000;

/// 墓碑 key(含命名空间前缀)对应的缓存失效:key → 挂载视图路径 →
/// invalidate_trash_cached(目录额外扫掉 stats/negative 后代);目录另按
/// 裸形态 invalidate(stat("/docs") 与 stat("/docs/") 是不同缓存键)。
fn invalidate_key(fs: &ObjectFs, key: &str, is_dir: bool) {
    let rel = key.strip_prefix(fs.prefix.as_str()).unwrap_or(key);
    let path = if rel.is_empty() {
        "/".to_string()
    } else {
        format!("/{rel}")
    };
    fs.invalidate_trash_cached(&path, is_dir);
    if is_dir {
        fs.invalidate_stat(path.trim_end_matches('/'));
    }
}

/// 墓碑 body(serde 往返;serde_json 已是直接依赖)。serde 默认忽略未知
/// 字段 → 前向兼容;新增字段一律 `#[serde(default, skip_serializing_if = ...)]`
/// —— 旧版本客户端读新墓碑不炸、旧墓碑读入新字段默认 None。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneBody {
    /// 文件墓碑:HEAD 原样 e_tag(OSS 带引号大写 / S3 小写;恢复比较忽略大小写)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// HEAD content_length
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// 目录墓碑 = {"is_dir":true}
    pub is_dir: bool,
    /// 回收站条目名(裁决 R2):Windows = Explorer 的 $R 名(必须记录,
    /// Explorer 还原按自己生成的 $R 名 rename);macOS = Finder 改名后的
    /// 条目名(与原名一致时 None,渲染回退 basename)。反向索引键。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recycle_name: Option<String>,
    /// 捕获的 $I 文件原始字节(裁决 R8,含盘符原始路径),上限 4KiB;不真实
    /// 落桶(软删零数据复制)。stat/read($I) 由此服务;缺失时读时合成回退。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recycle_i: Option<Vec<u8>>,
}

// ---------- 单元 4:管理命令与 GC 类型 ----------

/// trash-list 输出单元;--json 时逐条 NDJSON(全 pub —— ossmount 是
/// 独立 bin crate,凡被 ossmount.rs 消费的项必须 pub,契约 C10)。
/// deleted_date 经自定义 serde 序列化为 "YYYY-MM-DD" 字符串
/// (chrono 未开 serde feature,不为此改依赖)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashEntry {
    /// 删除时 UTC 日期分区(YYYY-MM-DD),墓碑 key 第 2 段
    #[serde(serialize_with = "ser_naive_date", deserialize_with = "de_naive_date")]
    pub deleted_date: chrono::NaiveDate,
    /// 原路径(挂载视图相对路径,无前导 '/');目录带尾 '/'
    pub path: String,
    /// 文件墓碑才有(墓碑 body 里删除时 HEAD 的 etag);目录 None
    pub etag: Option<String>,
    /// 文件墓碑才有(删除时 HEAD 的 content_length);目录 None
    pub size: Option<u64>,
    pub is_dir: bool,
}

fn ser_naive_date<S: serde::Serializer>(
    d: &chrono::NaiveDate,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(&d.to_string())
}

fn de_naive_date<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<chrono::NaiveDate, D::Error> {
    let s = String::deserialize(d)?;
    chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(serde::de::Error::custom)
}

/// trash-restore 三分支结果(契约 C10,全 pub)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    /// 已恢复(墓碑已删,原对象立即复活);etag_mismatch=true 表示
    /// 墓碑 etag 与当前原对象不一致,恢复的是当前内容(已警告);
    /// multiple_versions=true 表示同名存在多个日期墓碑,仅清除了最旧
    /// 一条 —— 调用方(CLI)应提示用户用 --date 指定版本(L6)。
    Restored {
        etag_mismatch: bool,
        multiple_versions: bool,
    },
    /// 墓碑存在但原对象 HEAD 404(已 GC / 其他端删):按 §7 顺序约定
    /// 清墓碑,不留空引用
    OriginalGone,
    /// 文件/目录两形都未找到墓碑
    NoTombstone,
}

/// GC 报告(trash-clean 输出;契约 C10,全 pub)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcReport {
    /// etag 一致 → 原对象已删
    pub files_removed: u64,
    /// 原对象 404 → 仅清墓碑
    pub files_tombstone_only: u64,
    /// etag 不一致 → 跳过(活数据),记 metrics trash_gc_etag_skips
    pub files_skipped_etag: u64,
    /// 目录墓碑已处理
    pub dirs_removed: u64,
    /// 目录 mtime 启发式批删的原对象数
    pub objects_deleted: u64,
    /// 删除的墓碑对象数(文件+目录,允许 DeleteObjects 批删)
    pub tombstones_deleted: u64,
}

/// GC 选项(契约 C10,全 pub)。
#[derive(Debug, Clone, Copy, Default)]
pub struct GcOptions {
    /// 只处理日期分区严格早于该日期的;None = today - 保留期(严格早于,
    /// 边界日不清)。**--before 只收紧不放松**:晚于默认保留期的 --before
    /// 无效 —— cutoff = min(before, today - retention_days)(L7 文档口径;
    /// 按字段字面语义实现会破坏 retention 对近期墓碑的保护)。
    pub before: Option<chrono::NaiveDate>,
    /// 只报告不删除(trash-clean --dry-run;判定照做,删除动作跳过)
    pub dry_run: bool,
}

/// etag 比较:忽略大小写与首尾引号(OSS 大写带引号 / S3 小写,墓碑存
/// HEAD 原样;恢复/GC 校验统一,规格 2.4 风险 7)。
fn etag_eq(a: &str, b: &str) -> bool {
    a.trim_matches('"')
        .eq_ignore_ascii_case(b.trim_matches('"'))
}

impl TombstoneIndex {
    /// 仅 files 精确命中(不含 dirs 前缀覆盖)—— 软删幂等门控专用
    /// (soft_delete_file:索引已覆盖 → 幂等 Ok,零远程;重建入口的双形态
    /// 门控见 [`Self::clear_tombstones_if_covered`])。
    pub fn is_file_covered(&self, key: &str) -> bool {
        self.files.contains_key(key)
    }

    /// 全量枚举(files+dirs,带 date),供 full_rebuild diff(单元 3)与
    /// 系统回收站视图渲染(单元 1)。
    pub fn entries(&self) -> Vec<(String, bool, chrono::NaiveDate)> {
        let mut out = Vec::with_capacity(self.files.len() + self.dirs.len());
        out.extend(self.files.iter().map(|(k, d)| (k.clone(), false, *d)));
        out.extend(self.dirs.iter().map(|(k, d)| (k.clone(), true, *d)));
        out
    }

    /// 条目总数(files + dirs),gauge 与规模告警用。
    pub fn len(&self) -> usize {
        self.files.len() + self.dirs.len()
    }

    /// 是否为空(clippy len_without_is_empty)。
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.dirs.is_empty()
    }
}

impl TrashState {
    /// 文件软删(trash 开启时 [`ObjectFs::delete`] 的替代)。步骤:
    /// key = fs.key_for(path);索引已覆盖 → 幂等 Ok(零远程);
    /// HEAD 原对象:404 → 幂等 Ok(不写墓碑,防隐藏不存在之物);
    /// 成功取 e_tag/content_length → 写墓碑(提交点)→ 索引 insert +
    /// invalidate_stat + invalidate_read_cache + trash_tombstones_written+1。
    /// 任一非 404 错误 → Err(删除失败,文件还在,提交点前零副作用)。
    /// 全程持一个 limiter permit(镜像 delete:并发上限不可回归)。
    /// `recycle_name`:系统回收站软删(单元 2)的视图条目名(Windows = $R
    /// 名;macOS = Finder 改名后的条目名,与原名一致时 None);普通软删
    /// 传 None,零行为变化。
    pub(crate) async fn soft_delete_file(
        &self,
        fs: &ObjectFs,
        path: &str,
        recycle_name: Option<String>,
    ) -> Result<()> {
        let _permit = fs.acquire().await?;
        self.soft_delete_file_impl(fs, path, recycle_name).await
    }

    /// 无 permit 变体:调用方已持 limiter permit(单元 2 rename 拦截镜像
    /// clear_target_tombstones 的 #55 纪律 —— 饱和池二次 acquire 死锁)。
    /// 语义与 [`Self::soft_delete_file`] 相同。
    async fn soft_delete_file_impl(
        &self,
        fs: &ObjectFs,
        path: &str,
        recycle_name: Option<String>,
    ) -> Result<()> {
        let key = fs.key_for(path);
        if self.index.read().unwrap().is_file_covered(&key) {
            return Ok(()); // 已隐藏:stale handle 二次删除,幂等,零远程
        }
        fs.metrics.s3_heads.fetch_add(1, Ordering::Relaxed);
        fs.metrics.s3_stat_heads.fetch_add(1, Ordering::Relaxed);
        let head = fs
            .client
            .head_object()
            .bucket(&fs.bucket)
            .key(&key)
            .send()
            .await;
        let (etag, size) = match head {
            Ok(resp) => (
                resp.e_tag().map(|s| s.to_string()),
                resp.content_length().map(|l| l.max(0) as u64),
            ),
            Err(e) if is_s3_not_found(&e) => return Ok(()), // 原对象不存在 → 幂等删除
            Err(e) => {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e).context("s3 head for soft delete");
            }
        };
        let tomb_key = encode_tombstone_key(
            &self.prefix,
            date_partition_utc(SystemTime::now()),
            &key,
            false,
        );
        self.write_tombstone(
            fs,
            &tomb_key,
            &TombstoneBody {
                etag,
                size,
                is_dir: false,
                recycle_name: recycle_name.clone(),
                recycle_i: None,
            },
        )
        .await?;
        // 提交点后:索引 + 缓存失效 + 计数(put 失败则以上全部不发生)
        {
            let mut idx = self.index.write().unwrap();
            idx.insert(&key, false, date_partition_utc(SystemTime::now()));
            self.store_index_entries(idx.len());
        }
        // 提交点后:recycle_names 与墓碑同生命周期(裁决 R3 ① —— 本地软删
        // 写入;仅系统视图软删带 recycle_name)。
        self.insert_recycle_names(&tomb_key, &key, &recycle_name);
        fs.invalidate_stat(path);
        fs.invalidate_read_cache(path);
        fs.metrics
            .trash_tombstones_written
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 目录软删(trash 开启时 [`ObjectFs::delete_dir_recursive`] 的替代)。
    /// 无需 HEAD/枚举(隐式目录也无 marker,统一写墓碑);不枚举、不
    /// DeleteObjects —— 前缀覆盖隐藏整个子树。步骤:
    /// dir_key 带尾斜杠 → 已覆盖 → 幂等 Ok;写墓碑 {is_dir:true} →
    /// 索引 insert + invalidate_stat + clear_read_cache(镜像
    /// delete_dir_recursive_impl 的缓存清理)。
    /// `recycle_name` 语义同 [`Self::soft_delete_file`]。
    pub(crate) async fn soft_delete_dir(
        &self,
        fs: &ObjectFs,
        dir: &str,
        recycle_name: Option<String>,
    ) -> Result<()> {
        let _permit = fs.acquire().await?;
        self.soft_delete_dir_impl(fs, dir, recycle_name).await
    }

    /// 无 permit 变体:调用方已持 limiter permit(单元 2 rename 拦截)。
    async fn soft_delete_dir_impl(
        &self,
        fs: &ObjectFs,
        dir: &str,
        recycle_name: Option<String>,
    ) -> Result<()> {
        let dir_key = format!("{}/", fs.key_for(dir).trim_end_matches('/'));
        if self.index.read().unwrap().is_covered(&dir_key) {
            return Ok(()); // 幂等,零远程
        }
        let tomb_key = encode_tombstone_key(
            &self.prefix,
            date_partition_utc(SystemTime::now()),
            &dir_key,
            true,
        );
        self.write_tombstone(
            fs,
            &tomb_key,
            &TombstoneBody {
                etag: None,
                size: None,
                is_dir: true,
                recycle_name: recycle_name.clone(),
                recycle_i: None,
            },
        )
        .await?;
        {
            let mut idx = self.index.write().unwrap();
            idx.insert(&dir_key, true, date_partition_utc(SystemTime::now()));
            self.store_index_entries(idx.len());
        }
        // 提交点后:recycle_names 与墓碑同生命周期(裁决 R3 ①)
        self.insert_recycle_names(&tomb_key, &dir_key, &recycle_name);
        // 双形态缓存失效(裁决 #8):invalidate_trash_cached 覆盖裸形态
        // "/d" 与 "/d/" 尾斜杠形态及后代前缀(stat("/d") 与 stat("/d/")
        // 是不同缓存键 —— 只失效裸形态会让 "/d/" 正值条目存活至 TTL,
        // 期间 stat("/d/") 短暂返回存在);clear_read_cache 保持子树
        // read-ahead 清理(镜像 delete_dir_recursive_impl)。
        fs.invalidate_trash_cached(dir, true);
        fs.invalidate_stat(dir.trim_end_matches('/'));
        fs.clear_read_cache();
        fs.metrics
            .trash_tombstones_written
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 系统回收站软删的反向索引写入(裁决 R3 ①):by_name →
    /// 墓碑 key、by_key → recycle_name(最新,裁决 R7)。同名 $R 重复 →
    /// by_name 覆盖为最新并 warn(不静默错删,测试 8 断言)。仅
    /// recycle_name Some(系统视图软删)时写入;提交点后调用。
    fn insert_recycle_names(
        &self,
        tomb_key: &str,
        original_key: &str,
        recycle_name: &Option<String>,
    ) {
        let Some(name) = recycle_name else {
            return;
        };
        let mut names = self.recycle_names.write().unwrap();
        if let Some(prev) = names.by_name.insert(name.clone(), tomb_key.to_string())
            && prev != tomb_key
        {
            tracing::warn!(
                recycle_name = %name,
                "trash system view: recycle name 已存在,by_name 覆盖为最新墓碑(裁决 R7)"
            );
        }
        names.by_key.insert(original_key.to_string(), name.clone());
    }

    /// 还原(rename 源在回收站条目层,单元 2):
    /// 1. 解析 old → Entry{entry_name};$I 形态 → Err(元数据条目不可还原)。
    /// 2. resolve_entry_original → None → Err(NotFound)。暖路径(by_name
    ///    命中)零远程;Windows 冷路径内部 acquire 兜底。
    /// 3. key_for(new) == original_key(还原到原位置,数据从未移动)→
    ///    文件先 HEAD 原对象:404 → 清墓碑返回 OriginalGone(对齐
    ///    trash_restore 三分支,不留空引用);存在 → 清墓碑 → Restored。
    ///    零 copy(P4 守卫)。清墓碑 = clear_target_tombstones(裁决 R7:
    ///    同名多日期全部版本清除)。
    /// 4. new != original_key(拖到任意位置):镜像 rename_impl 拷贝机件
    ///    (文件 copy_object/multipart;目录 copy_tree + allow_rename_dir/
    ///    rename_dir_limit 守卫)→ copy 成功后清源墓碑 + 目标墓碑(覆盖
    ///    语义,只清目标自身双形态不清祖先 —— 不复活已删目录,§2.4)。
    /// 5. 原对象 404 → 清墓碑,返回 OriginalGone(不销毁可能存活的数据)。
    ///
    /// 返回 RestoreOutcome(对齐 trash_restore 三分支);rename 调用方丢弃
    /// 详情(map 为 Ok(()) —— 还原失败已含语义,成功与否即 Outcome 之外
    /// 的信息)。
    pub(crate) async fn restore_via_system(
        &self,
        fs: &ObjectFs,
        old: &str,
        new: &str,
    ) -> Result<RestoreOutcome> {
        // 1. 解析 old → Entry{entry_name};$I 形态 → Err(NotFound 语义)
        let Some(SystemTrashMatch::Entry { entry_name }) = self.match_system_trash(old) else {
            anyhow::bail!("restore: source is not a system recycle bin entry");
        };
        if is_i_entry(&entry_name) {
            anyhow::bail!("restore: cannot restore a $I metadata entry");
        }
        // 2. resolve_entry_original → None → Err(NotFound)
        let Some(original_key) = self.resolve_entry_original(fs, &entry_name).await? else {
            anyhow::bail!("restore: recycle bin entry not found: {entry_name}");
        };
        let is_dir = original_key.ends_with('/');
        let new_key = fs.key_for(new);
        let same_place = new_key.trim_end_matches('/') == original_key.trim_end_matches('/');

        // F2(high):还原目标与原 key 的子树关系校验(镜像 rename_impl 的
        // EINVAL 检查 —— restore_via_system 此前缺失,审查确认的遗漏):
        // new 落在原 key 子树内 → copy_tree 自拷贝,ListObjectsV2 续页
        // 持续包含新拷贝 → 无限生成对象;还原到祖先 → 拷贝对象落入源
        // 前缀,同样错乱。同 key 还原(same_place)不受影响(前缀检查带
        // '/' 边界,相等不可能命中)。
        let new_bare = new_key.trim_end_matches('/');
        let orig_bare = original_key.trim_end_matches('/');
        if new_bare != orig_bare
            && (new_bare.starts_with(&format!("{orig_bare}/"))
                || orig_bare.starts_with(&format!("{new_bare}/")))
        {
            anyhow::bail!("restore: cannot restore a path into its own subtree");
        }
        // F6(medium):目标各级祖先被墓碑覆盖 → bail(规格 §2.4:还原进
        // 已删目录会失败;修复前清墓碑/拷贝静默成功但结果被祖先墓碑
        // 隐藏 —— 条目从视图与回收站双消失)。零远程(本地索引判定)。
        if self.is_ancestor_covered(&new_key) {
            anyhow::bail!("restore: target is inside a directory that is in the trash");
        }

        // 还原主体(HEAD/copy/清墓碑):持一个 permit(镜像 trash_restore
        // 入口;resolve_entry_original 的冷路径 acquire 已先行释放)。
        let _permit = fs.acquire().await?;
        // 文件:HEAD 原对象校验(404 → OriginalGone 仅清墓碑)。目录无原
        // 对象(隐式目录无 marker),跳过。
        let mut size: u64 = 0;
        if !is_dir {
            let head = fs
                .client
                .head_object()
                .bucket(&fs.bucket)
                .key(&original_key)
                .send()
                .await;
            match head {
                Err(e) if is_s3_not_found(&e) => {
                    self.clear_target_tombstones(fs, &original_key).await?;
                    fs.invalidate_trash_cached(old, false);
                    fs.invalidate_stat(new.trim_end_matches('/'));
                    return Ok(RestoreOutcome::OriginalGone);
                }
                Err(e) => {
                    fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e).context("s3 head for system restore");
                }
                Ok(resp) => {
                    fs.metrics.s3_heads.fetch_add(1, Ordering::Relaxed);
                    fs.metrics.s3_stat_heads.fetch_add(1, Ordering::Relaxed);
                    size = resp.content_length().map(|l| l.max(0) as u64).unwrap_or(0);
                }
            }
        }
        if same_place {
            // 3. 还原到原位置:数据从未移动,零 copy(P4 守卫)
            self.clear_target_tombstones(fs, &original_key).await?;
            fs.invalidate_trash_cached(old, is_dir);
            fs.invalidate_stat(new.trim_end_matches('/'));
            return Ok(RestoreOutcome::Restored {
                etag_mismatch: false,
                multiple_versions: false,
            });
        }
        // 4. 拖到任意位置:镜像 rename_impl 拷贝机件
        let bare_original = original_key.trim_end_matches('/');
        if is_dir {
            if !fs.allow_rename_dir {
                anyhow::bail!("directory rename is disabled");
            }
            if let Some(limit) = fs.rename_dir_limit {
                let count = fs.count_tree_entries(bare_original, limit).await?;
                if count > limit {
                    anyhow::bail!(
                        "directory {old} has {count} entries, exceeding rename-dir-limit {limit}"
                    );
                }
            }
            fs.copy_tree(bare_original, &new_key).await?;
        } else if size >= crate::ossfs::MULTIPART_COPY_THRESHOLD {
            fs.multipart_copy_object(&original_key, &new_key, size)
                .await?;
        } else {
            let mut copy = fs
                .client
                .copy_object()
                .bucket(&fs.bucket)
                .key(&new_key)
                .copy_source(crate::ossfs::s3_copy_source(&fs.bucket, &original_key));
            if let Some(sc) = &fs.storage_class {
                copy = copy.storage_class(sc.clone());
            }
            copy.send().await.context("s3 copy")?;
        }
        // 5. copy 成功 → 清源墓碑(提交点);目标被墓碑覆盖 → 覆盖语义
        // (clear_target_tombstones(new) 只清 new 自身双形态,不清祖先)。
        self.clear_target_tombstones(fs, &original_key).await?;
        self.clear_target_tombstones(fs, &new_key).await?;
        fs.invalidate_trash_cached(old, is_dir);
        fs.invalidate_stat(new.trim_end_matches('/'));
        Ok(RestoreOutcome::Restored {
            etag_mismatch: false,
            multiple_versions: false,
        })
    }

    // ---------- 单元 3:回收站内删除 = 永久删 ----------

    /// 回收站内单条永久删(裁决 R6 顺序:先原对象后墓碑):
    ///
    ///    1. 结构解析 → Entry{entry_name}。
    ///    2. $I 形态(F5):对应 $R 墓碑已解析 → no-op(捕获字节随 $R 的
    ///       永久删一并清除,测试 2);未解析 → 回退删除同键真实对象。
    ///    3. resolve_entry(暖路径零远程;Windows 冷路径内部 acquire 兜底)
    ///       → None → F4 回退删除视图路径真实对象(macOS .DS_Store 等)。
    ///    4. 文件:GET 墓碑 body 取 etag → HEAD 原 key:一致或 404 →
    ///       DELETE 原对象;不一致 → warn + 跳过(原对象孤儿化,不销毁
    ///       可能存活的数据)。目录:F1 先重查墓碑 body,已消失 → 仅清
    ///       墓碑;仍在 → delete_dir_recursive_impl(无条件递归删)。
    ///    5. clear_tombstones_both_forms(清全部版本,裁决 R7)+ 视图条目
    ///       缓存失效。
    ///
    /// 全程持一个 limiter permit(镜像 delete 的并发上限不可回归;
    /// resolve_entry 冷路径的 acquire 在返回前已释放)。
    pub(crate) async fn permanent_delete_entry(&self, fs: &ObjectFs, path: &str) -> Result<()> {
        // 1. 结构解析;$I 形态 no-op(捕获字节随对应 $R 的永久删一并清除,
        //    零远程,测试 2 断言)
        let Some(SystemTrashMatch::Entry { entry_name }) = self.match_system_trash(path) else {
            anyhow::bail!("permanent delete: not a system recycle bin entry: {path}");
        };
        // 2. $I 形态:F5 —— 对应 $R 墓碑已解析 → no-op(捕获字节随 $R
        // 永久删一并清除,零远程);未解析(历史遗留/捕获丢失的真实 $I
        // 对象)→ 回退删除同键真实对象(修复前恒 no-op,幽灵条目不可删)。
        if is_i_entry(&entry_name) {
            let r_name = i_to_r_name(&entry_name).expect("is_i_entry ⇒ i_to_r_name");
            if self.resolve_entry(fs, &r_name, true).await?.is_some() {
                return Ok(());
            }
            fs.invalidate_stat(path);
            fs.invalidate_read_cache(path);
            let _permit = fs.acquire().await?;
            self.delete_object(fs, &fs.key_for(path)).await?;
            return Ok(());
        }
        // 3. 条目 → 原 key + 墓碑 key + 目录形态;未解析(桶中真实对象 ——
        // macOS .DS_Store、Windows 历史遗留真实条目)→ F4 回退删除视图
        // 路径真实对象(修复前 bail → Finder 清空废纸篓 EIO;stat/read
        // 已有真实对象回退,删除侧补齐对称)。DELETE 幂等:无真实对象
        // 即 no-op 成功。
        let Some(resolved) = self.resolve_entry(fs, &entry_name, true).await? else {
            fs.invalidate_stat(path);
            fs.invalidate_read_cache(path);
            let _permit = fs.acquire().await?;
            self.delete_object(fs, &fs.key_for(path)).await?;
            return Ok(());
        };
        let _permit = fs.acquire().await?;
        // 3. 先原对象后墓碑(裁决 R6)
        if resolved.is_dir {
            // F1(high):目录永久删前重查墓碑 body 仍在 —— 多端交错下索引
            // 可能陈旧(他端已 restore/GC/永久删),修复前无条件递归删会
            // 连带删除墓碑消失后他端写入的新数据。已消失 → 仅清墓碑,
            // 绝不动无法核验的原对象(镜像 permanent_delete_file 的 L5
            // 口径,目录路径此前无对等校验,属实现不对称缺陷)。
            let Some(_) = self.read_tombstone(fs, &resolved.tomb_key).await? else {
                self.clear_tombstones_both_forms(fs, &resolved.original_key)
                    .await?;
                fs.invalidate_trash_cached(path, true);
                fs.invalidate_read_cache(path);
                return Ok(());
            };
            let view = key_view_path(fs, &resolved.original_key);
            fs.delete_dir_recursive_impl(&view).await?;
        } else {
            self.permanent_delete_file(fs, &resolved.original_key, &resolved.tomb_key)
                .await?;
        }
        // 4. 清全部版本墓碑(裁决 R7)+ 索引/反向索引收尾 + 视图条目缓存
        // 失效(目录额外清后代;read 缓存镜像 soft_delete)
        self.clear_tombstones_both_forms(fs, &resolved.original_key)
            .await?;
        fs.invalidate_trash_cached(path, resolved.is_dir);
        fs.invalidate_read_cache(path);
        Ok(())
    }

    /// 文件条目永久删主体(调用方已持 permit;单条与 purge_all 复用):
    /// GET 墓碑 body 取 etag → HEAD 原 key → etag 一致或 HEAD 404 →
    /// DELETE 原对象;不一致 → warn + 跳过原对象(原对象孤儿化,不销毁
    /// 可能存活的数据,裁决 R6)。墓碑 body 404(并发删除,无法校验)→
    /// 跳过原对象(L5 口径:绝不动无法核验的原对象)。
    async fn permanent_delete_file(
        &self,
        fs: &ObjectFs,
        original_key: &str,
        tomb_key: &str,
    ) -> Result<()> {
        let head = self.head_original(fs, original_key).await?;
        match head {
            // 原对象不存在(已 GC/其他端删):DELETE 幂等兜底
            None => self.delete_object(fs, original_key).await,
            Some(current_etag) => {
                let mismatched = match self.read_tombstone(fs, tomb_key).await? {
                    None => {
                        // 墓碑并发删除:无法校验 etag,不动原对象
                        tracing::warn!(
                            original_key,
                            "trash system view: 永久删时墓碑已不存在,跳过原对象删除"
                        );
                        true
                    }
                    Some(body) => match (body.etag.as_deref(), current_etag.as_deref()) {
                        (Some(a), Some(b)) => !etag_eq(a, b),
                        (Some(_), None) => true, // 墓碑有 etag、当前无 → 视为不一致
                        _ => false,
                    },
                };
                if mismatched {
                    tracing::warn!(
                        original_key,
                        "trash system view: 原对象 etag 与墓碑不一致,跳过原对象删除(孤儿化)"
                    );
                    return Ok(());
                }
                self.delete_object(fs, original_key).await
            }
        }
    }

    /// 清空整个系统回收站(delete_dir_recursive 命中 Dir{level:0}):
    /// 阶段一逐条删原对象(文件:HEAD etag 校验 + DELETE;目录:F1 重查
    /// 墓碑 body 后递归删),阶段二分区级单次扫描 + 批删全部墓碑,阶段三
    /// 索引/反向索引/缓存收尾。只清"有墓碑的条目"对应的原对象+墓碑,
    /// 不触碰桶中真实(非墓碑)对象(风险 6 口径)。
    /// 幂等:索引空 → 零远程;条目删除后重入安全。
    /// F12(非阻塞改进):permit 短持(阶段一每条一个、阶段二三一个)——
    /// 修复前全程独占一个并发位且每条目一次 clear_tombstones_both_forms
    /// (O(条目数×分区数) 请求,大回收站清空 = 千万级请求持续数小时);
    /// 阶段二为 O(分区数) 列表 + DeleteObjects 批删。
    pub(crate) async fn purge_all(&self, fs: &ObjectFs) -> Result<()> {
        // 快照:索引 entries()(date = 最新墓碑日期,裁决 R7)。空 → 零远程。
        let entries = self.index.read().unwrap().entries();
        if entries.is_empty() {
            return Ok(());
        }
        // 阶段一:逐条删原对象(permit 短持 —— 每条一个)。
        for (original_key, is_dir, date) in &entries {
            let _permit = fs.acquire().await?;
            if *is_dir {
                // F1(high):删除前重查墓碑 body(索引快照可能陈旧 —— 他端
                // 已 restore/GC/永久删)。已消失 → 仅清墓碑,不动原树。
                let tomb_key = encode_tombstone_key(&self.prefix, *date, original_key, true);
                if self.read_tombstone(fs, &tomb_key).await?.is_none() {
                    continue;
                }
                let view = key_view_path(fs, original_key);
                fs.delete_dir_recursive_impl(&view).await?;
            } else {
                // 该 key 最新墓碑的 etag(索引 date = 最新,裁决 R7)
                let tomb_key = encode_tombstone_key(&self.prefix, *date, original_key, false);
                self.permanent_delete_file(fs, original_key, &tomb_key)
                    .await?;
            }
        }
        // 阶段二:分区级单次扫描收集全部待删墓碑(文件/目录双形态一并
        // 收集,按墓碑 original_key 裸形态判定集合成员;不触碰其他原 key
        // 的墓碑)。请求 = O(分区数) 列表 + 批删。
        let purge_set: HashSet<&str> = entries
            .iter()
            .map(|(k, _, _)| k.trim_end_matches('/'))
            .collect();
        let _permit = fs.acquire().await?;
        let mut doomed: Vec<String> = Vec::new();
        for date in Self::list_partitions_desc(fs, &self.prefix).await? {
            let partition_prefix = format!("{}{}/", self.prefix, date);
            list_trash_keys(fs, None, Some(&partition_prefix), |page| {
                for k in page {
                    if let Some(t) = decode_tombstone_key(&self.prefix, &k)
                        && purge_set.contains(t.original_key.trim_end_matches('/'))
                    {
                        doomed.push(k);
                    }
                }
                Ok(())
            })
            .await?;
        }
        for chunk in doomed.chunks(MAX_DELETE_OBJECTS_PER_REQUEST) {
            self.delete_keys_batch(fs, chunk).await?;
        }
        // 阶段三:索引整体移除 + 反向索引清理 + 缓存失效。
        {
            let mut idx = self.index.write().unwrap();
            for (key, is_dir, _) in &entries {
                idx.remove(key, *is_dir);
            }
            self.store_index_entries(idx.len());
        }
        {
            // recycle_names 与墓碑同生命周期:by_name 中指向已删墓碑键
            // 的条目移除;by_key 按快照条目移除(文件/目录双形态;并发
            // 新建条目不在快照内,映射保留 —— 冷路径兜底自愈)。
            let mut names = self.recycle_names.write().unwrap();
            names.by_name.retain(|_, tk| !doomed.contains(tk));
            for (key, _, _) in &entries {
                let bare = key.trim_end_matches('/');
                names.by_key.remove(bare);
                names.by_key.remove(&format!("{bare}/"));
            }
        }
        for (key, is_dir, _) in &entries {
            invalidate_key(fs, key, *is_dir);
        }
        fs.clear_read_cache();
        Ok(())
    }

    /// 重建入口统一清墓碑挂点(write / write_from_file / mkdir):门控 =
    /// 裸 key 的 is_covered(文件精确命中 或 目录前缀覆盖 —— 跨形态同名
    /// 重建 F1 的双形态判定),覆盖则清文件+目录双形态墓碑。未覆盖 →
    /// 零远程请求(性能守卫)。入口级:调用链无外层 permit
    /// (write/write_from_file/mkdir 的 permit 在 put_whole_object 内部),
    /// 此处持一个 permit 完成列表+删除。
    pub(crate) async fn clear_tombstones_if_covered(
        &self,
        fs: &ObjectFs,
        path: &str,
    ) -> Result<()> {
        let key = fs.key_for(path);
        let bare = key.trim_end_matches('/');
        if !self.index.read().unwrap().is_covered(bare) {
            return Ok(());
        }
        let _permit = fs.acquire().await?;
        self.clear_tombstones_both_forms(fs, &key).await?;
        fs.invalidate_stat(path);
        Ok(())
    }

    /// rename 目标清墓碑(调用方已持 limiter permit,内部不 acquire ——
    /// 饱和池阻塞第二次 acquire 会死锁,#55 纪律)。门控与清双形态语义
    /// 同 [`Self::clear_tombstones_if_covered`](F1:rmdir /e 后 rename
    /// 文件到 /e 的目录墓碑、unlink /e 后 rename 目录到 /e 的文件墓碑
    /// 均被清)。未覆盖 → 零远程请求。
    pub(crate) async fn clear_target_tombstones(&self, fs: &ObjectFs, key: &str) -> Result<()> {
        let bare = key.trim_end_matches('/');
        if !self.index.read().unwrap().is_covered(bare) {
            return Ok(());
        }
        self.clear_tombstones_both_forms(fs, key).await
    }

    /// 目标路径各级祖先(不含自身)是否被墓碑覆盖(F6)。零远程 —— 本地
    /// 索引判定(读锁瞬时持有,不跨 await)。裸形态 is_covered(文件精确
    /// + 目录前缀双形态)。
    fn is_ancestor_covered(&self, key: &str) -> bool {
        let bare = key.trim_end_matches('/');
        let idx = self.index.read().unwrap();
        let mut rest = bare;
        while let Some(pos) = rest.rfind('/') {
            rest = &rest[..pos];
            if idx.is_covered(rest) {
                return true;
            }
        }
        false
    }

    /// 清双形态墓碑(跨形态同名重建统一扫描,F1 裁决):文件形态(key 精确、
    /// is_dir=false)与目录形态(key+"/"、is_dir=true)跨全部分区一并清除,
    /// 索引两种形态条目一并移除(不存在时 remove 为 no-op —— 祖先墓碑
    /// 覆盖下的子路径创建不清祖先墓碑,V1 语义不变)。
    /// 请求数 = 1 次分区枚举 + 2×分区数次精确前缀探测(双形态,常量因子,
    /// 裁决 #10 的 O(分区数) 有界不回归)。任一 DELETE 失败 → Err。
    /// 锁纪律与旧 clear_tombstones_matching 相同:列表/删除期间不持
    /// index 锁,仅收尾时短写锁。不 acquire permit:入口(_if_covered
    /// 变体)或调用方(rename 的外层 permit)负责。
    async fn clear_tombstones_both_forms(&self, fs: &ObjectFs, key: &str) -> Result<()> {
        let file_key = key.trim_end_matches('/');
        let dir_key = format!("{file_key}/");
        let prefix = self.prefix.clone();
        let mut to_delete: Vec<String> = Vec::new();
        let partitions = Self::list_partitions_desc(fs, &prefix).await?;
        for date in &partitions {
            // 每分区两个精确前缀探测:文件形态与目录形态(各 0-2 键,
            // 均经 decode 精确过滤 —— 目录形态探测不会误收文件墓碑)
            for probe_key in [file_key, dir_key.as_str()] {
                let probe = format!("{prefix}{date}/{probe_key}");
                list_trash_keys(fs, None, Some(&probe), |page| {
                    for k in page {
                        if let Some(t) = decode_tombstone_key(&prefix, &k)
                            && t.is_dir == (probe_key.ends_with('/'))
                            && t.original_key == probe_key
                        {
                            to_delete.push(k);
                        }
                    }
                    Ok(())
                })
                .await?;
            }
        }
        for k in &to_delete {
            fs.client
                .delete_object()
                .bucket(&fs.bucket)
                .key(k)
                .send()
                .await
                .inspect_err(|_| {
                    fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                })
                .inspect_err(|_| {
                    fs.metrics.s3_delete_errors.fetch_add(1, Ordering::Relaxed);
                })
                .context("s3 delete tombstone")?;
        }
        // 扫描成功 = 远端已无对应形态墓碑(命中已删,或外部客户端先删 →
        // 幽灵):无条件移除两种形态索引条目并同步 gauge —— 扫描无命中
        // 即幽灵,立即解除隐藏(裁决 #4;不存在时 remove 为 no-op)。
        {
            let mut idx = self.index.write().unwrap();
            idx.remove(file_key, false);
            idx.remove(&dir_key, true);
            self.store_index_entries(idx.len());
        }
        // 统一收尾:recycle_names 与墓碑同生命周期(单元 1;幽灵场景
        // tombstone_keys 为空 —— by_name 中指向已删墓碑键的条目随下次
        // 反查 404 自然失效,by_key 映射按「已无存活墓碑」移除)。
        self.remove_tombstone_maps(&to_delete, file_key);
        Ok(())
    }

    /// 枚举 .trash 下全部日期分区(common prefixes,delimiter 扫描,
    /// 分页续 token 复用 next_page_token 护栏),返回降序(最新在前)
    /// 的 "YYYY-MM-DD" 字符串。1+ 次 list 请求(分区数 >1000 才多页,
    /// 现实中远低于此)。s3_lists 计数与 list_impl 对齐(每页 +1)。
    /// 不 acquire permit:调用方(clear 路径)已持。
    async fn list_partitions_desc(fs: &ObjectFs, trash_prefix: &str) -> Result<Vec<String>> {
        let mut token: Option<String> = None;
        let mut parts: Vec<String> = Vec::new();
        loop {
            fs.metrics.s3_lists.fetch_add(1, Ordering::Relaxed);
            let mut req = fs
                .client
                .list_objects_v2()
                .bucket(&fs.bucket)
                .prefix(trash_prefix)
                .delimiter("/");
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                    fs.metrics.s3_list_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e).context("s3 list trash partitions");
                }
            };
            for cp in resp.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    let date = p.strip_prefix(trash_prefix).unwrap_or(p);
                    if !date.is_empty() {
                        parts.push(date.trim_end_matches('/').to_string());
                    }
                }
            }
            match next_page_token(&resp)? {
                Some(tok) => token = Some(tok),
                None => break,
            }
        }
        parts.sort_unstable_by(|a, b| b.cmp(a)); // 降序:今天分区最先
        parts.dedup();
        Ok(parts)
    }

    /// PUT 墓碑小对象:serde_json body + content_type("application/json");
    /// content_md5 配置开启时显式加头(镜像 put_whole_object,防 OSS 拒收 ——
    /// #74 教训);不传 storage_class(墓碑走默认存储类,免 Archive 类墓碑
    /// 冻结 GC/恢复读)。错误 → s3_errors/s3_put_errors 计数。
    async fn write_tombstone(
        &self,
        fs: &ObjectFs,
        tomb_key: &str,
        body: &TombstoneBody,
    ) -> Result<()> {
        let json = serde_json::to_vec(body).context("serialize tombstone body")?;
        let mut put = fs
            .client
            .put_object()
            .bucket(&fs.bucket)
            .key(tomb_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(json.clone()))
            .content_type("application/json");
        if fs.content_md5 {
            put = put.content_md5(crate::ossfs::content_md5(&json));
        }
        if let Err(e) = put.send().await {
            fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            fs.metrics.s3_put_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e).context("s3 put tombstone");
        }
        Ok(())
    }

    // ---------- 单元 4:管理命令与 GC ----------

    /// 找原 key 的全部墓碑:date Some → 快速路径直查 `.trash/<date>/<key>`
    /// 与 `.trash/<date>/<key>/` 两形(精确前缀探测,各 0-1 键);date
    /// None → 全量分页扫描(管理命令成本,规格 4.4 风险 5 文档化)。
    /// 返回全部命中按 (date, is_dir) 升序(最旧在前);外部杂项 key 经
    /// decode 校验跳过。多命中即 L6 场景(同名多日期墓碑 / 文件目录
    /// 双形):restore 只清最旧一条并标记 multiple_versions。
    pub(crate) async fn find_tombstone(
        &self,
        fs: &ObjectFs,
        key: &str,
        date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<(chrono::NaiveDate, bool)>> {
        let prefix = self.prefix.clone();
        let file_key = key.trim_end_matches('/');
        let dir_key = format!("{file_key}/");
        let mut hits: Vec<(chrono::NaiveDate, bool)> = Vec::new();
        if let Some(d) = date {
            for probe in [file_key, dir_key.as_str()] {
                let p = format!("{prefix}{d}/{probe}");
                list_trash_keys(fs, None, Some(&p), |page| {
                    for k in page {
                        if let Some(t) = decode_tombstone_key(&prefix, &k)
                            && t.original_key == probe
                        {
                            hits.push((t.date, t.is_dir));
                        }
                    }
                    Ok(())
                })
                .await?;
            }
            hits.sort(); // (date, is_dir) 升序,最旧在前
            return Ok(hits);
        }
        list_trash_keys(fs, None, None, |page| {
            for k in page {
                if let Some(t) = decode_tombstone_key(&prefix, &k)
                    && (t.original_key == file_key || t.original_key == dir_key)
                {
                    hits.push((t.date, t.is_dir));
                }
            }
            Ok(())
        })
        .await?;
        hits.sort(); // 最旧在前:restore 无 --date 恢复最旧一条(L6)
        Ok(hits)
    }

    /// 恢复:删墓碑 + 索引 remove + 缓存失效 → 原对象立即复活。
    /// 文件墓碑先 HEAD 原对象校验(三分支):404 → 清墓碑(§7 顺序约定)
    /// 报 OriginalGone,不留空引用;etag 不一致 → warn「内容已被其他端
    /// 修改」后默认仍恢复(etag_mismatch=true,恢复的是当前内容);
    /// 一致 → Restored{false}。目录墓碑无需 HEAD,删墓碑即恢复。
    /// 未命中 → NoTombstone。
    pub(crate) async fn trash_restore(
        &self,
        fs: &ObjectFs,
        path: &str,
        date: Option<chrono::NaiveDate>,
    ) -> Result<RestoreOutcome> {
        let key = fs.key_for(path);
        let hits = self.find_tombstone(fs, &key, date).await?;
        let Some(&(d, is_dir)) = hits.first() else {
            return Ok(RestoreOutcome::NoTombstone);
        };
        // L6:同名多日期墓碑(或文件/目录双形)时只清最旧一条 —— 其余
        // 仍隐藏 key,调用方(CLI)据此提示用户用 --date 指定版本。
        let multiple_versions = hits.len() > 1;
        // L2:入口持一个 permit 覆盖 find_tombstone 之后的 HEAD/GET/DELETE
        // (head_original/read_tombstone 的「调用方已持 permit」契约;
        // delete_tombstone 不再内部 acquire —— 饱和池二次 acquire 死锁,
        // #55 纪律)。find_tombstone 的全量列表不占 permit(管理命令成本,
        // 规格 4.4 风险 5),此处获取恰好覆盖其余请求。
        let _permit = fs.acquire().await?;
        // 墓碑 key 按 encode 同规则拼接:目录形态补尾斜杠(幂等)。
        let original_key = if is_dir {
            format!("{}/", key.trim_end_matches('/'))
        } else {
            key.clone()
        };
        let tomb_key = encode_tombstone_key(&self.prefix, d, &original_key, is_dir);
        let outcome = if is_dir {
            // 目录:删墓碑即恢复(无原对象,不做 HEAD)
            self.delete_tombstone(fs, &tomb_key).await?;
            RestoreOutcome::Restored {
                etag_mismatch: false,
                multiple_versions,
            }
        } else {
            // 文件:HEAD 原对象校验(三分支)
            let head = self.head_original(fs, &key).await?;
            match head {
                None => {
                    // 原对象不存在(已 GC / 其他端删):清墓碑,不留空引用(§7)
                    self.delete_tombstone(fs, &tomb_key).await?;
                    RestoreOutcome::OriginalGone
                }
                Some(current_etag) => {
                    // 墓碑 body 里删除时的 etag 与当前 etag 比较(忽略
                    // 大小写与引号:OSS 大写带引号 / S3 小写,规格 2.4 风险 7)
                    let mismatched = match self.read_tombstone(fs, &tomb_key).await? {
                        // 墓碑已被并发恢复/GC 删除(L5):key 本已恢复
                        None => false,
                        Some(body) => match (body.etag.as_deref(), current_etag.as_deref()) {
                            (Some(a), Some(b)) => !etag_eq(a, b),
                            (Some(_), None) => true, // 墓碑有 etag、当前无 → 视为不一致
                            _ => false,
                        },
                    };
                    if mismatched {
                        tracing::warn!(
                            path = %path,
                            "trash restore: 内容已被其他端修改,恢复的是当前内容"
                        );
                    }
                    self.delete_tombstone(fs, &tomb_key).await?;
                    RestoreOutcome::Restored {
                        etag_mismatch: mismatched,
                        multiple_versions,
                    }
                }
            }
        };
        // 提交点后:索引 remove + 缓存失效(双形态,镜像 soft_delete_dir)
        {
            let mut idx = self.index.write().unwrap();
            idx.remove(&key, is_dir);
            self.store_index_entries(idx.len());
        }
        fs.invalidate_trash_cached(path, is_dir);
        fs.invalidate_stat(path.trim_end_matches('/'));
        // 统一收尾:recycle_names 与墓碑同生命周期(单元 1)
        self.remove_tombstone_maps(&[tomb_key], &key);
        Ok(outcome)
    }

    /// trash-list:分页列出墓碑(日期/原路径/etag/size)。文件墓碑 GET
    /// body 取 etag/size(管理命令成本,一次性操作);目录墓碑 None。
    /// 页回调流式输出,50 万条不整体驻留(规格 4.2)。分页骨架与
    /// list_trash_keys 相同,但页内需 async GET body,同步 on_page 无法
    /// 承载,故独立实现。
    pub(crate) async fn trash_list(
        &self,
        fs: &ObjectFs,
        mut on_page: impl FnMut(Vec<TrashEntry>) -> Result<()>,
    ) -> Result<()> {
        let prefix = self.prefix.clone();
        let mut token: Option<String> = None;
        loop {
            fs.metrics.s3_lists.fetch_add(1, Ordering::Relaxed);
            let mut req = fs
                .client
                .list_objects_v2()
                .bucket(&fs.bucket)
                .prefix(&prefix);
            if let Some(tok) = token.as_deref() {
                req = req.continuation_token(tok);
            }
            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                    fs.metrics.s3_list_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e).context("s3 list trash");
                }
            };
            let mut entries = Vec::new();
            for key in resp
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
            {
                let Some(t) = decode_tombstone_key(&prefix, &key) else {
                    continue; // 外部客户端垃圾对象跳过
                };
                let (etag, size) = if t.is_dir {
                    (None, None)
                } else {
                    let Some(body) = self.read_tombstone(fs, &key).await? else {
                        continue; // 墓碑已被并发删除:跳过该条目
                    };
                    (body.etag, body.size)
                };
                // 挂载视图相对路径(剥命名空间 prefix;目录保留尾斜杠)
                let rel = t
                    .original_key
                    .strip_prefix(&fs.prefix)
                    .unwrap_or(&t.original_key);
                entries.push(TrashEntry {
                    deleted_date: t.date,
                    path: rel.to_string(),
                    etag,
                    size,
                    is_dir: t.is_dir,
                });
            }
            on_page(entries)?;
            match next_page_token(&resp)? {
                Some(tok) => token = Some(tok),
                None => break,
            }
        }
        Ok(())
    }

    /// GC 过期清理。cutoff = min(opts.before 或今天, today - 保留期)
    /// (规格 4.2:--before 只收紧不放松,绝不清今天以内的分区);按分区
    /// 只处理 date < cutoff 的分区(未来日期墓碑天然跳过,时钟偏快保护);
    /// 每分区先文件后目录(gc_partition_files → gc_partition_dirs ——
    /// 顺序保证文件 etag 判定优先于目录 mtime 启发式,偏斜场景下
    /// "活数据"不被目录启发式误删,规格 4.4 风险 9);处理完的墓碑从
    /// 索引 remove;dry_run 判定照做(HEAD/GET/list)、删除动作全跳过。
    pub(crate) async fn trash_gc(&self, fs: &ObjectFs, opts: GcOptions) -> Result<GcReport> {
        // 只读挂载绝不动桶(M2):后台周期 GC 与任何直接调用都早退零动作
        // —— 只读"绝不改桶"语义;管理命令 trash-clean 强制 read_only=false,
        // 不受影响。
        if fs.read_only() {
            return Ok(GcReport::default());
        }
        let today = date_partition_utc(SystemTime::now());
        let retention_start = today
            .checked_sub_days(chrono::Days::new(self.retention_days as u64))
            .unwrap_or(today);
        let cutoff = opts.before.unwrap_or(today).min(retention_start);
        let mut report = GcReport::default();
        let partitions = Self::list_partitions_desc(fs, &self.prefix).await?;
        for date_str in partitions {
            let Ok(date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") else {
                continue; // 外部垃圾分区
            };
            if date >= cutoff {
                continue; // 严格早于 cutoff 才处理
            }
            self.gc_partition_files(fs, date, opts.dry_run, &mut report)
                .await?;
            self.gc_partition_dirs(fs, date, opts.dry_run, &mut report)
                .await?;
        }
        if !opts.dry_run {
            // L4 代际推进:GC 的索引变更已完成 —— 并发中的 refresh/rebuild
            // 凭此丢弃其陈旧快照(apply 前检测),防止把已删墓碑重插回索引。
            // dry-run 零状态变更,不推进。
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
        Ok(report)
    }

    /// 单分区文件墓碑 GC。每墓碑:GET body 取删除时 etag → HEAD 原对象:
    /// 404 → files_tombstone_only++、墓碑入批删;etag 一致 →
    /// files_removed++,先 DELETE 原对象(单对象)再墓碑入批删;etag
    /// 不一致 → files_skipped_etag++ + trash_gc_etag_skips,不动(活数据
    /// 留给人工)。批删列表每 MAX_DELETE_OBJECTS_PER_REQUEST 条
    /// DeleteObjects 一次(复用 DeleteObjectsContentMd5 interceptor,
    /// #74 防 OSS 400 InvalidDigest)。每墓碑持一个 limiter permit
    /// (并发上限不可回归);顺序约定 §7:先删原对象后删墓碑(多端竞态)。
    async fn gc_partition_files(
        &self,
        fs: &ObjectFs,
        date: chrono::NaiveDate,
        dry_run: bool,
        report: &mut GcReport,
    ) -> Result<()> {
        let prefix = self.prefix.clone();
        let partition_prefix = format!("{prefix}{date}/");
        let mut files: Vec<String> = Vec::new();
        list_trash_keys(fs, None, Some(&partition_prefix), |page| {
            for k in page {
                if let Some(t) = decode_tombstone_key(&prefix, &k)
                    && !t.is_dir
                {
                    files.push(k);
                }
            }
            Ok(())
        })
        .await?;
        let mut tomb_keys: Vec<String> = Vec::new(); // 待批删墓碑
        let mut removed: Vec<(String, bool)> = Vec::new(); // 索引 remove(key, is_dir)
        for tomb_key in files {
            let _permit = fs.acquire().await?;
            let Some(body) = self.read_tombstone(fs, &tomb_key).await? else {
                // 墓碑已被并发 restore/GC 删除(L5):绝不动原对象 ——
                // 旧实现把 404 当「无 etag」按 matched 处理,恢复成功
                // 瞬间原对象又被 GC 永久删除;跳过即下轮 S3 列表自然
                // 不再出现,自愈。
                continue;
            };
            let file_key = tomb_key
                .strip_prefix(&partition_prefix)
                .unwrap_or(&tomb_key);
            match self.head_original(fs, file_key).await? {
                None => {
                    // 原对象不存在(外部已删):仅清墓碑,不留空引用
                    report.files_tombstone_only += 1;
                    removed.push((file_key.to_string(), false));
                    tomb_keys.push(tomb_key);
                }
                Some(current_etag) => {
                    let matched = match (body.etag.as_deref(), current_etag.as_deref()) {
                        (Some(a), Some(b)) => etag_eq(a, b),
                        (Some(_), None) => false,
                        _ => true, // 墓碑无 etag(旧版/异常)或当前无 → 视为一致
                    };
                    if matched {
                        report.files_removed += 1;
                        removed.push((file_key.to_string(), false));
                        if !dry_run {
                            // 顺序约定 §7:先删原对象,后删墓碑
                            self.delete_object(fs, file_key).await?;
                        }
                        tomb_keys.push(tomb_key);
                    } else {
                        // 活数据:跳过,记 metrics,留给人工
                        report.files_skipped_etag += 1;
                        fs.metrics
                            .trash_gc_etag_skips
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        if !tomb_keys.is_empty() {
            if !dry_run {
                for chunk in tomb_keys.chunks(MAX_DELETE_OBJECTS_PER_REQUEST) {
                    self.delete_keys_batch(fs, chunk).await?;
                }
            }
            report.tombstones_deleted += tomb_keys.len() as u64;
        }
        if !dry_run && !removed.is_empty() {
            {
                let mut idx = self.index.write().unwrap();
                for (key, is_dir) in &removed {
                    idx.remove(key, *is_dir);
                }
                self.store_index_entries(idx.len());
            }
            for (key, is_dir) in &removed {
                invalidate_key(fs, key, *is_dir);
            }
            // 统一收尾:recycle_names 与墓碑同生命周期(单元 1)
            for (tomb_key, file_key) in tomb_keys.iter().zip(removed.iter()) {
                self.remove_tombstone_maps(std::slice::from_ref(tomb_key), &file_key.0);
            }
        }
        Ok(())
    }

    /// 单分区目录墓碑 GC(mtime 启发式):以原目录 key(含尾 '/')为前缀
    /// ListObjectsV2,收集 last_modified < date 00:00 UTC 的对象 → 按
    /// MAX_DELETE_OBJECTS_PER_REQUEST 分块 DeleteObjects(interceptor
    /// 同文件路径);last_modified >= date 视为墓碑日期后的新数据保留。
    /// 删原对象后删目录墓碑(顺序约定 §7);dirs_removed++/
    /// objects_deleted+=n。边界声明:删除机本地时钟超前时墓碑日期晚于
    /// 真实删除,之后合法重写对象 last_modified < 墓碑日期会被误删
    /// (规格 4.4 风险 1,缓解:只处理 date < cutoff 分区、严格用服务器
    /// LastModified 对比)。
    async fn gc_partition_dirs(
        &self,
        fs: &ObjectFs,
        date: chrono::NaiveDate,
        dry_run: bool,
        report: &mut GcReport,
    ) -> Result<()> {
        let prefix = self.prefix.clone();
        let partition_prefix = format!("{prefix}{date}/");
        let mut dirs: Vec<String> = Vec::new();
        list_trash_keys(fs, None, Some(&partition_prefix), |page| {
            for k in page {
                if let Some(t) = decode_tombstone_key(&prefix, &k)
                    && t.is_dir
                {
                    dirs.push(t.original_key);
                }
            }
            Ok(())
        })
        .await?;
        for dir_key in dirs {
            let _permit = fs.acquire().await?;
            // mtime 启发式:last_modified < 墓碑日 00:00 UTC 的对象才删
            let cutoff = date.and_hms_opt(0, 0, 0).unwrap().and_utc();
            let mut doomed: Vec<String> = Vec::new();
            let mut token: Option<String> = None;
            loop {
                fs.metrics.s3_lists.fetch_add(1, Ordering::Relaxed);
                let mut req = fs
                    .client
                    .list_objects_v2()
                    .bucket(&fs.bucket)
                    .prefix(&dir_key);
                if let Some(tok) = token.as_deref() {
                    req = req.continuation_token(tok);
                }
                let resp = match req.send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                        fs.metrics.s3_list_errors.fetch_add(1, Ordering::Relaxed);
                        return Err(e).context("s3 list dir prefix in trash gc");
                    }
                };
                for obj in resp.contents() {
                    let (Some(k), Some(lm)) = (obj.key(), obj.last_modified()) else {
                        continue;
                    };
                    if lm.secs() < cutoff.timestamp() {
                        doomed.push(k.to_string());
                    }
                }
                match next_page_token(&resp)? {
                    Some(tok) => token = Some(tok),
                    None => break,
                }
            }
            // 顺序约定 §7:先删原对象(批删),后删目录墓碑
            if !dry_run && !doomed.is_empty() {
                for chunk in doomed.chunks(MAX_DELETE_OBJECTS_PER_REQUEST) {
                    self.delete_keys_batch(fs, chunk).await?;
                }
            }
            let tomb_key = encode_tombstone_key(&prefix, date, &dir_key, true);
            if !dry_run {
                self.delete_keys_batch(fs, std::slice::from_ref(&tomb_key))
                    .await?;
            }
            report.objects_deleted += doomed.len() as u64;
            report.dirs_removed += 1;
            report.tombstones_deleted += 1;
            if !dry_run {
                // L1:dry-run 不改变任何状态 —— 索引 remove 与缓存失效
                // 同样门控(dry-run 契约:判定照做、删除不落、状态不变)。
                {
                    let mut idx = self.index.write().unwrap();
                    idx.remove(&dir_key, true);
                    self.store_index_entries(idx.len());
                }
                invalidate_key(fs, &dir_key, true);
                // 统一收尾:recycle_names 与墓碑同生命周期(单元 1)
                self.remove_tombstone_maps(&[tomb_key], &dir_key);
            }
        }
        Ok(())
    }

    /// HEAD 原对象:404 → None(原对象不存在);成功 → Some(当前 etag,
    /// 可能为 None)。错误计数与 soft_delete_file 对齐。
    /// 调用方已持 limiter permit。
    async fn head_original(&self, fs: &ObjectFs, key: &str) -> Result<Option<Option<String>>> {
        let resp = fs
            .client
            .head_object()
            .bucket(&fs.bucket)
            .key(key)
            .send()
            .await;
        match resp {
            Ok(r) => Ok(Some(r.e_tag().map(|s| s.to_string()))),
            Err(e) if is_s3_not_found(&e) => Ok(None),
            Err(e) => {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                Err(e).context("s3 head in trash gc")
            }
        }
    }

    /// 单对象 DELETE(GC 删原对象;顺序约定 §7 的第一步)。
    /// 调用方已持 limiter permit。
    async fn delete_object(&self, fs: &ObjectFs, key: &str) -> Result<()> {
        fs.client
            .delete_object()
            .bucket(&fs.bucket)
            .key(key)
            .send()
            .await
            .inspect_err(|_| {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                fs.metrics.s3_delete_errors.fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 delete original in trash gc")?;
        Ok(())
    }

    /// DELETE 单个墓碑对象(restore 用)。**调用方已持 limiter permit**
    /// (trash_restore 入口持一个覆盖 HEAD/GET/DELETE 全程;内部不再
    /// acquire —— 饱和池二次 acquire 死锁,#55 纪律)。
    async fn delete_tombstone(&self, fs: &ObjectFs, tomb_key: &str) -> Result<()> {
        fs.client
            .delete_object()
            .bucket(&fs.bucket)
            .key(tomb_key)
            .send()
            .await
            .inspect_err(|_| {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            })
            .inspect_err(|_| {
                fs.metrics.s3_delete_errors.fetch_add(1, Ordering::Relaxed);
            })
            .context("s3 delete tombstone")?;
        Ok(())
    }

    /// GET 墓碑 body 解析 TombstoneBody。404 → None(墓碑已被并发删除:
    /// GC 调用方跳过该墓碑绝不动原对象[L5],restore 调用方按「本已
    /// 恢复」处理,list 调用方跳过该条目);调用方已持 limiter permit。
    async fn read_tombstone(&self, fs: &ObjectFs, tomb_key: &str) -> Result<Option<TombstoneBody>> {
        Ok(self.read_tombstone_with_etag(fs, tomb_key).await?.0)
    }

    /// GET 墓碑 body + 响应 e_tag(条件写用 —— set_recycle_i 的 F8
    /// 「无墓碑不复活」判定)。语义同 [`Self::read_tombstone`];调用方
    /// 已持 limiter permit。
    async fn read_tombstone_with_etag(
        &self,
        fs: &ObjectFs,
        tomb_key: &str,
    ) -> Result<(Option<TombstoneBody>, Option<String>)> {
        let resp = match fs
            .client
            .get_object()
            .bucket(&fs.bucket)
            .key(tomb_key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) if is_s3_not_found(&e) => return Ok((None, None)),
            Err(e) => {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e).context("s3 get tombstone");
            }
        };
        let etag = resp.e_tag().map(|s| s.to_string());
        let bytes = resp
            .body
            .collect()
            .await
            .context("read tombstone body")?
            .into_bytes();
        Ok((
            Some(serde_json::from_slice(&bytes).context("parse tombstone body")?),
            etag,
        ))
    }

    /// 条件写墓碑(if-match 墓碑 etag;F8「无墓碑不复活」)。etag 失配 /
    /// 对象缺失 → 存储返回 412,调用方按「墓碑已被并发删除」丢弃捕获
    /// 字节(不复活幽灵墓碑)。错误计数与 [`Self::write_tombstone`] 对齐。
    /// 调用方已持 limiter permit。消费链(set_recycle_i)仅 Windows
    /// (winfsp.rs)可达,非 Windows 构建为死代码 —— 与既有
    /// MAX_RECYCLE_I_BYTES / set_recycle_i 告警同源,按裁决 F17 口径允许。
    #[cfg_attr(not(windows), allow(dead_code))]
    async fn write_tombstone_if_match(
        &self,
        fs: &ObjectFs,
        tomb_key: &str,
        body: &TombstoneBody,
        etag: Option<&str>,
    ) -> Result<()> {
        let json = serde_json::to_vec(body).context("serialize tombstone body")?;
        let mut put = fs
            .client
            .put_object()
            .bucket(&fs.bucket)
            .key(tomb_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(json.clone()))
            .content_type("application/json");
        if let Some(e) = etag {
            put = put.if_match(e);
        }
        if fs.content_md5 {
            put = put.content_md5(crate::ossfs::content_md5(&json));
        }
        if let Err(e) = put.send().await {
            fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
            fs.metrics.s3_put_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e).context("s3 put tombstone (if-match)");
        }
        Ok(())
    }

    /// DeleteObjects 批删(≤1000 键/请求,复用 DeleteObjectsContentMd5
    /// interceptor —— OSS 缺 Content-MD5 报 400 InvalidDigest,#74;
    /// trash 路径不得绕过)。镜像 delete_dir_recursive_impl 的批删
    /// 形状(含部分失败检查)。调用方已持 limiter permit。
    async fn delete_keys_batch(&self, fs: &ObjectFs, keys: &[String]) -> Result<()> {
        let objects = keys
            .iter()
            .map(|k| ObjectIdentifier::builder().key(k).build())
            .collect::<Result<Vec<_>, _>>()
            .context("build delete object identifiers")?;
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .context("build batch delete request")?;
        let resp = fs
            .client
            .delete_objects()
            .bucket(&fs.bucket)
            .delete(delete)
            .customize()
            .interceptor(DeleteObjectsContentMd5)
            .send()
            .await
            .context("s3 batch delete in trash gc")?;
        let failed = resp.errors();
        if !failed.is_empty() {
            let sample: Vec<&str> = failed.iter().filter_map(|e| e.key()).take(5).collect();
            anyhow::bail!(
                "s3 batch delete failed for {} of {} keys (e.g. {:?})",
                failed.len(),
                keys.len(),
                sample
            );
        }
        Ok(())
    }
}

/// 以 trash_prefix 分页枚举墓碑对象 key。start_after 传 Some 时携带
/// ListObjectsV2 start-after 参数(单元 3);None 为从头全量。prefix 传
/// Some 时覆盖默认 trash_prefix(如「当前 UTC 日期分区」完整扫描、
/// 清墓碑的逐分区精确探测)。
/// 分页间续 token 处理复用 next_page_token 的 truncated 护栏(#60)。
/// 不 acquire limiter permit:调用方决定(rebuild 全程持一个 permit,
/// eager 挂点靠 poll_inflight 互斥,均见各调用点注释)。
/// s3_lists 计数与 list_impl 对齐(每页 +1)。
///
/// **不施加 list_rate(规格 §7.1 待验证项,裁决 #10 结论)**:list_rate
/// 是用户目录枚举节流(list_impl 路径),施加到 trash 刷新会让墓碑可见
/// 性依赖用户枚举负载(同一桶大目录枚举可把 30s 可见性 SLA 任意恶化);
/// trash 路径已有自身并发约束(全量持 permit、eager 靠 poll_inflight
/// 天然限 1);且清墓碑已按分区扫描,请求数 O(分区数) 有界,无线性
/// 放大可节流。
pub(crate) async fn list_trash_keys(
    fs: &ObjectFs,
    start_after: Option<&str>,
    prefix: Option<&str>,
    mut on_page: impl FnMut(Vec<String>) -> Result<()>,
) -> Result<()> {
    let Some(trash) = &fs.trash else {
        return Ok(());
    };
    let trash_prefix = prefix
        .map(|p| p.to_string())
        .unwrap_or_else(|| trash.prefix.clone());
    let mut token: Option<String> = None;
    loop {
        fs.metrics.s3_lists.fetch_add(1, Ordering::Relaxed);
        let mut req = fs
            .client
            .list_objects_v2()
            .bucket(&fs.bucket)
            .prefix(&trash_prefix);
        if let Some(sa) = start_after {
            req = req.start_after(sa);
        }
        if let Some(tok) = token.as_deref() {
            req = req.continuation_token(tok);
        }
        let resp = match req.send().await {
            Ok(resp) => resp,
            Err(e) => {
                fs.metrics.s3_errors.fetch_add(1, Ordering::Relaxed);
                fs.metrics.s3_list_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e).context("s3 list trash");
            }
        };
        let page: Vec<String> = resp
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_string))
            .collect();
        on_page(page)?;
        match next_page_token(&resp)? {
            Some(tok) => token = Some(tok),
            None => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(ymd: (i32, u32, u32)) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2).unwrap()
    }

    fn index_with_dirs(dirs: &[&str]) -> TombstoneIndex {
        let mut idx = TombstoneIndex::default();
        for dir in dirs {
            idx.insert(dir, true, d((2026, 8, 16)));
        }
        idx
    }

    /// 单元 1:系统回收站测试统一构造(直接置 pub(crate) 字段,不走
    /// build_trash_state —— 平台/目录名/uid 范围由测试显式定制)。
    fn state_with_system(sys: SystemTrash) -> Arc<TrashState> {
        let mut state = TrashState::new(
            ".trash/".to_string(),
            TrashRefreshMode::Lazy,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(86400),
            crate::ossfs::TRASH_RETENTION_DAYS,
        );
        Arc::get_mut(&mut state)
            .expect("freshly created arc is uniquely owned")
            .system = Some(sys);
        state
    }

    fn win_state() -> Arc<TrashState> {
        state_with_system(SystemTrash {
            dir_name: "$Recycle.Bin".into(),
            platform: SystemTrashPlatform::WindowsRecycleBin,
            macos_uid_dirs: vec![],
        })
    }

    fn mac_state() -> Arc<TrashState> {
        state_with_system(SystemTrash {
            dir_name: ".Trashes".into(),
            platform: SystemTrashPlatform::MacOsTrashes,
            macos_uid_dirs: vec![501],
        })
    }

    #[test]
    fn match_system_trash_matrix() {
        // Windows 形态:0/1/2 层命中,>2 层不拦截(桶中真实用户数据原样可见)
        let s = win_state();
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin"),
            Some(SystemTrashMatch::Dir { level: 0 })
        );
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/S-1-5-21-1"),
            Some(SystemTrashMatch::Dir { level: 1 })
        );
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/S-1-5-21-1/$R4de00001a.txt"),
            Some(SystemTrashMatch::Entry {
                entry_name: "$R4de00001a.txt".into()
            })
        );
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/S-1-5-21-1/$R4de00001a.txt/sub"),
            None,
            ">2 层不拦截(深层文件原样可见)"
        );
        // 尾斜杠形态与根/空路径
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/"),
            Some(SystemTrashMatch::Dir { level: 0 })
        );
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/S-1/"),
            Some(SystemTrashMatch::Dir { level: 1 })
        );
        assert_eq!(s.match_system_trash("/"), None);
        assert_eq!(s.match_system_trash(""), None);
        // 前缀边界不误伤
        assert_eq!(s.match_system_trash("/$Recycle.Binx"), None, "前缀边界");
        assert_eq!(s.match_system_trash("/x$Recycle.Bin"), None);
        assert_eq!(
            s.match_system_trash("/.trash/2026-08-16/a.txt"),
            None,
            "与 .trash 前缀互不干扰"
        );
        assert_eq!(s.match_system_trash("/docs/a.txt"), None);
        // macOS 形态同构
        let m = mac_state();
        assert_eq!(
            m.match_system_trash("/.Trashes"),
            Some(SystemTrashMatch::Dir { level: 0 })
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/501"),
            Some(SystemTrashMatch::Dir { level: 1 })
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/501/a.txt"),
            Some(SystemTrashMatch::Entry {
                entry_name: "a.txt".into()
            })
        );
        assert_eq!(m.match_system_trash("/.Trashesx/501/a.txt"), None);
        assert_eq!(m.match_system_trash("/.Trashes/501/a/b"), None);
        // 目录名覆盖生效
        let custom = state_with_system(SystemTrash {
            dir_name: "CustomBin".into(),
            platform: SystemTrashPlatform::WindowsRecycleBin,
            macos_uid_dirs: vec![],
        });
        assert_eq!(
            custom.match_system_trash("/CustomBin/S-1"),
            Some(SystemTrashMatch::Dir { level: 1 })
        );
        assert_eq!(custom.match_system_trash("/$Recycle.Bin"), None);
        // trash 关闭(system=None)恒 None
        let closed = TrashState::new(
            ".trash/".to_string(),
            TrashRefreshMode::Lazy,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(86400),
            crate::ossfs::TRASH_RETENTION_DAYS,
        );
        assert_eq!(closed.match_system_trash("/$Recycle.Bin"), None);
        assert_eq!(closed.match_system_trash("/.Trashes/501"), None);
        assert!(!closed.is_system_trash_path("/$Recycle.Bin"));
        // is_system_trash_path = match 命中
        assert!(s.is_system_trash_path("/$Recycle.Bin"));
        assert!(s.is_system_trash_path("/$Recycle.Bin/S-1"));
        assert!(s.is_system_trash_path("/$Recycle.Bin/S-1/$R1.txt"));
        assert!(!s.is_system_trash_path("/$Recycle.Bin/S-1/$R1.txt/x"));
    }

    #[test]
    fn macos_match_filters_uid_scope() {
        // 裁决 R17:macOS 渲染/拦截限 macos_uid_dirs(空 = 当前用户 uid),
        // 范围外按普通路径(返回 None,走 S3 404 自然失败);非数字 uid 不
        // 拦截;Windows SID 段任意接受(不受 uid 过滤)。
        let m = mac_state(); // macos_uid_dirs = [501]
        assert_eq!(
            m.match_system_trash("/.Trashes/501/a.txt"),
            Some(SystemTrashMatch::Entry {
                entry_name: "a.txt".into()
            })
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/501"),
            Some(SystemTrashMatch::Dir { level: 1 })
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/999"),
            None,
            "范围外 uid 不拦截"
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/999/a.txt"),
            None,
            "范围外 uid 不拦截"
        );
        assert_eq!(
            m.match_system_trash("/.Trashes/x/a.txt"),
            None,
            "非数字 uid 不拦截"
        );
        assert_eq!(
            m.match_system_trash("/.Trashes"),
            Some(SystemTrashMatch::Dir { level: 0 }),
            "根层不过滤"
        );
        // Windows:SID 段非数字,不受 uid 过滤影响
        let s = win_state();
        assert_eq!(
            s.match_system_trash("/$Recycle.Bin/S-1-5-21-1/$R1.txt"),
            Some(SystemTrashMatch::Entry {
                entry_name: "$R1.txt".into()
            })
        );
    }

    #[test]
    fn record_seen_sid_windows_only() {
        // 裁决 R14:Windows mkdir/rename 时记录 SID 段;macOS 不使用
        let s = win_state();
        s.record_seen_sid("/$Recycle.Bin/S-1-5-21-1");
        s.record_seen_sid("/$Recycle.Bin/S-1-5-21-2/");
        s.record_seen_sid("/$Recycle.Bin"); // level 0 不记录
        s.record_seen_sid("/docs"); // 范围外不记录
        let sids = s.seen_sids.read().unwrap();
        assert_eq!(sids.len(), 2);
        assert!(sids.contains("S-1-5-21-1"));
        assert!(sids.contains("S-1-5-21-2"));
        drop(sids);
        let m = mac_state();
        m.record_seen_sid("/.Trashes/501");
        assert!(
            m.seen_sids.read().unwrap().is_empty(),
            "macOS 不使用 seen_sids"
        );
    }

    #[test]
    fn is_covered_matrix() {
        // 文件精确命中
        let mut idx = TombstoneIndex::default();
        idx.insert("docs/a.txt", false, d((2026, 8, 16)));
        assert!(idx.is_covered("docs/a.txt"));
        assert!(!idx.is_covered("docs/b.txt"));

        // 目录前缀覆盖
        let idx = index_with_dirs(&["docs/"]);
        assert!(idx.is_covered("docs/a.txt"));
        assert!(idx.is_covered("docs/sub/b.txt"));
        // 嵌套
        let idx = index_with_dirs(&["a/"]);
        assert!(idx.is_covered("a/b/c.txt"));
        // 非覆盖:前缀边界不误伤
        let idx = index_with_dirs(&["docs/"]);
        assert!(!idx.is_covered("docsx/a.txt"));
        let idx = index_with_dirs(&["d/"]);
        assert!(!idx.is_covered("docs/a.txt"));
        // 目录双形态:"docs"(无尾斜杠)与 "docs/" 都被覆盖 ——
        // 回归 stat 复活 bug:stat("/docs") 得 key "docs",只对 key 前缀
        // 匹配会漏,经 marker HEAD 把已删目录复活。
        let idx = index_with_dirs(&["docs/"]);
        assert!(idx.is_covered("docs"));
        assert!(idx.is_covered("docs/"));
        // marker 形态(目录墓碑 key 本身)
        assert!(idx.is_covered("docs/"));
        // 空集合恒 false
        let idx = TombstoneIndex::default();
        assert!(!idx.is_covered("docs/a.txt"));
        assert!(!idx.is_covered("docs"));
        assert!(!idx.is_covered(""));
    }

    #[test]
    fn is_covered_comparisons_bounded_by_depth_log() {
        // 裁决 #5 复杂度承诺可执行化:dirs ~1000 的对抗数据(全部共享超长
        // 公共前缀、查询无覆盖 —— 旧线性回扫需扫过全部 1000 条才确认未覆盖),
        // is_covered 比较次数必须 ≤ 深度×log2(n) 上界且 ≥ 每级二分下界;
        // 回归到 O(n) 回扫(比较量 ≈ n 或 ≈ 单次二分)时本断言红。
        // 计数来自生产搜索闭包(thread_local 插桩,cfg(test) 下编译,
        // release 零开销),而非镜像实现 —— 生产路径本身被断言。
        let mut dirs = Vec::new();
        for i in 0..1000 {
            dirs.push((format!("common/prefix/shared/dir{i:04}/"), d((2026, 8, 16))));
        }
        let idx = TombstoneIndex {
            files: HashMap::new(),
            dirs,
        };
        // 查询落在公共前缀之下但无精确覆盖(最坏回扫场景)
        let key = "common/prefix/shared/dir1000/x.txt";
        let depth = key.matches('/').count(); // 4 级路径前缀
        COUNT_IS_COVERED.with(|c| c.set(true));
        assert!(!idx.is_covered(key));
        COUNT_IS_COVERED.with(|c| c.set(false));
        let comps = COUNT_VALUE.with(|c| {
            let v = c.get();
            c.set(0);
            v
        });
        // log2(1000)≈10:4 级 × 10 ≈ 40 次。上界 60 留 std 内部实现余量;
        // 下界 28 远高于线性回扫回归的比较量(≈10 或 ≈1000 之外必低于此)。
        assert!(
            comps <= depth * 12 + 12,
            "is_covered 比较次数 {comps} 超出 深度×log2(n) 上界(深度 {depth})"
        );
        assert!(
            comps >= depth * 7,
            "is_covered 比较次数 {comps} 过低 —— 疑似 O(n) 线性回扫回归(每级必须二分)"
        );
    }

    #[test]
    fn index_alert_threshold_executable() {
        // 裁决 #6 + 阈值规范(新阈值落地带验证):500k 告警决策可执行化
        // —— 决策纯函数 + 真实插入规模 + gauge 落点,缺一不可。
        assert_eq!(
            crate::ossfs::TRASH_INDEX_ALERT_THRESHOLD,
            500_000,
            "索引规模告警阈值(规格 C5;常量防漂移另见 mod.rs refresh_constants_pinned)"
        );
        assert!(!TrashState::index_size_alert(500_000), "等于阈值不告警");
        assert!(TrashState::index_size_alert(500_001), "超阈值 1 条即告警");
        // 索引插入 500_001 条触发告警条件(纯内存 HashMap,快)
        let mut idx = TombstoneIndex::default();
        for i in 0..500_001 {
            idx.files.insert(format!("f{i:06}"), d((2026, 8, 16)));
        }
        assert_eq!(idx.len(), 500_001);
        assert!(
            TrashState::index_size_alert(idx.len()),
            "500_001 条插入后超阈值"
        );
        // store_index_entries 必须落到 gauge(告警与 gauge 同源)
        let state = TrashState::new(
            ".trash/".to_string(),
            TrashRefreshMode::Lazy,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(crate::ossfs::TRASH_GC_INTERVAL_SECS),
            crate::ossfs::TRASH_RETENTION_DAYS,
        );
        state.store_index_entries(idx.len());
        assert_eq!(
            state.index_entries.load(Ordering::Relaxed),
            500_001,
            "gauge 与 store_index_entries 同步"
        );
    }

    #[test]
    fn dirs_sorted_invariant() {
        // 乱序插入后 dirs 保持升序、无重复(二分的前提);date 随 key 保存
        let mut idx = TombstoneIndex::default();
        for dir in ["z/", "a/", "m/", "b/", "a/"] {
            idx.insert(dir, true, d((2026, 8, 16)));
        }
        for i in 1..idx.dirs.len() {
            assert!(
                idx.dirs[i - 1].0 < idx.dirs[i].0,
                "dirs must stay sorted: {:?}",
                idx.dirs
            );
        }
        assert_eq!(idx.dirs.len(), 4, "重复插入必须幂等去重");
        // insert("docs", true) 归一化补尾斜杠
        let mut idx = TombstoneIndex::default();
        idx.insert("docs", true, d((2026, 8, 16)));
        assert_eq!(idx.dirs, vec![("docs/".to_string(), d((2026, 8, 16)))]);
        // 固定种子伪随机序列(长度 > 二分扫描最坏回扫深度)
        let mut seed = 0x5eed_u64;
        let mut idx = TombstoneIndex::default();
        for _ in 0..200 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let n = (seed % 30) as usize;
            idx.insert(&format!("dir{n}/sub"), true, d((2026, 8, 16)));
        }
        for i in 1..idx.dirs.len() {
            assert!(idx.dirs[i - 1].0 < idx.dirs[i].0, "sorted invariant");
        }
    }

    #[test]
    fn remove_flips_coverage() {
        // 文件移除
        let mut idx = TombstoneIndex::default();
        idx.insert("a.txt", false, d((2026, 8, 16)));
        assert!(idx.is_covered("a.txt"));
        idx.remove("a.txt", false);
        assert!(!idx.is_covered("a.txt"));
        // 不存在 no-op
        idx.remove("a.txt", false);
        idx.remove("zzz", false);
        assert!(!idx.is_covered("a.txt"));

        // 目录移除:无尾斜杠形态也能移除(与 insert 归一化对称)
        let mut idx = index_with_dirs(&["docs/"]);
        assert!(idx.is_covered("docs/a.txt"));
        idx.remove("docs", true);
        assert!(!idx.is_covered("docs/a.txt"));
        idx.remove("docs", true); // no-op
        assert!(!idx.is_covered("docs/a.txt"));

        // 文件与目录同名互不干扰
        let mut idx = TombstoneIndex::default();
        idx.insert("x", false, d((2026, 8, 16)));
        idx.insert("x", true, d((2026, 8, 16)));
        assert!(idx.is_covered("x"));
        idx.remove("x", false);
        assert!(idx.is_covered("x"), "目录墓碑仍覆盖");
        idx.remove("x", true);
        assert!(!idx.is_covered("x"));
    }

    #[test]
    fn rebuild_replaces_and_dedups() {
        let mut idx = index_with_dirs(&["old/"]);
        idx.insert("old.txt", false, d((2026, 8, 16)));
        let tombstones = vec![
            ("docs/a.txt".to_string(), false, d((2026, 8, 16))),
            ("docs/".to_string(), true, d((2026, 8, 16))),
            (
                "docs/a.txt".to_string(),
                false,
                d((2026, 8, 16)), // 同名多日期墓碑只留一条
            ),
            ("z/".to_string(), true, d((2026, 8, 16))),
            ("docs".to_string(), true, d((2026, 8, 16))), // 无尾斜杠归一化
            ("a/".to_string(), true, d((2026, 8, 16))),
        ];
        idx.rebuild(tombstones.into_iter());
        // 整体替换:旧条目消失
        assert!(!idx.is_covered("old.txt"));
        assert!(idx.is_covered("docs/a.txt"));
        // 排序去重:docs/ 只有一条
        assert_eq!(
            idx.dirs,
            vec![
                ("a/".to_string(), d((2026, 8, 16))),
                ("docs/".to_string(), d((2026, 8, 16))),
                ("z/".to_string(), d((2026, 8, 16))),
            ]
        );
        assert_eq!(idx.files.len(), 1);
    }

    #[test]
    fn index_keeps_latest_date_per_key() {
        // 裁决 R4/R7:同名多日期墓碑只留一条,date 保留最新(系统视图冷路径
        // 按索引 date 反查墓碑 key —— 必须指向最新墓碑)。
        let mut idx = TombstoneIndex::default();
        // 先旧后新(远端列表字典序 = 日期分区升序,旧日期先出现)
        idx.insert("docs/a.txt", false, d((2026, 8, 15)));
        idx.insert("docs/a.txt", false, d((2026, 8, 16)));
        assert_eq!(
            idx.files.get("docs/a.txt"),
            Some(&d((2026, 8, 16))),
            "文件索引保留最新 date"
        );
        // 乱序回填(增量批次:今天分区先、昨天分区后)也不能覆盖成旧 date
        idx.insert("docs/b.txt", false, d((2026, 8, 16)));
        idx.insert("docs/b.txt", false, d((2026, 8, 15)));
        assert_eq!(idx.files.get("docs/b.txt"), Some(&d((2026, 8, 16))));
        // 目录同语义
        idx.insert("docs", true, d((2026, 8, 15)));
        idx.insert("docs", true, d((2026, 8, 16)));
        assert_eq!(idx.dirs, vec![("docs/".to_string(), d((2026, 8, 16)))]);
        // rebuild 去重保留最新
        let mut idx = TombstoneIndex::default();
        idx.rebuild(
            vec![
                ("x/".to_string(), true, d((2026, 8, 15))),
                ("x/".to_string(), true, d((2026, 8, 16))),
            ]
            .into_iter(),
        );
        assert_eq!(idx.dirs, vec![("x/".to_string(), d((2026, 8, 16)))]);
        // 移除只按 key(不关心 date)
        idx.remove("x", true);
        assert!(idx.dirs.is_empty());
    }

    #[test]
    fn entries_carry_dates() {
        let mut idx = TombstoneIndex::default();
        idx.insert("a.txt", false, d((2026, 8, 16)));
        idx.insert("docs", true, d((2026, 8, 15)));
        let mut entries = idx.entries();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                ("a.txt".to_string(), false, d((2026, 8, 16))),
                ("docs/".to_string(), true, d((2026, 8, 15))),
            ]
        );
    }

    #[test]
    fn tombstone_key_roundtrip() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        // 特殊字符(空格、'+'、'%'、'#'、Unicode)、多段路径、"a/../b"
        // (S3 key 是不透明字节串,不归一化)、prefix 有/无
        for key in [
            "a b.txt",
            "c+d.txt",
            "e%f.txt",
            "g#h.txt",
            "文档/报告.txt",
            "multi/segment/path.txt",
            "a/../b",
        ] {
            let enc = encode_tombstone_key(".trash/", date, key, false);
            assert_eq!(enc, format!(".trash/2026-08-16/{key}"));
            let dec = decode_tombstone_key(".trash/", &enc).expect("roundtrip");
            assert_eq!(dec.date, date);
            assert_eq!(dec.original_key, key);
            assert!(!dec.is_dir);
        }
        // prefix 变体
        let enc = encode_tombstone_key("ossfs/.trash/", date, "docs/a.txt", false);
        assert_eq!(enc, "ossfs/.trash/2026-08-16/docs/a.txt");
        let dec = decode_tombstone_key("ossfs/.trash/", &enc).unwrap();
        assert_eq!(dec.original_key, "docs/a.txt");
        assert!(!dec.is_dir);
        // 目录:尾 '/' 原样;encode 幂等(original_key 已带尾斜杠不双写 —— 防 "docs//")
        let enc = encode_tombstone_key(".trash/", date, "docs/", true);
        assert_eq!(enc, ".trash/2026-08-16/docs/");
        let dec = decode_tombstone_key(".trash/", &enc).unwrap();
        assert!(dec.is_dir);
        assert_eq!(dec.original_key, "docs/");
        // 无尾斜杠目录输入归一化
        let enc = encode_tombstone_key(".trash/", date, "docs", true);
        assert_eq!(enc, ".trash/2026-08-16/docs/");
        // decode 失败矩阵
        for bad in [
            "x/2026-08-16/a.txt",      // 非 trash 前缀
            "2026-08-16/a.txt",        // 前缀完全不符
            ".trash/2026-08-16",       // 缺 '/'、无 original key
            ".trash/2026-13-99/a.txt", // 坏日期(月越界)
            ".trash/today/a.txt",      // 非日期
            ".trash/2026-08-16/",      // 空 original_key(裸日期分区)
            ".trash/",                 // 空余
        ] {
            assert!(
                decode_tombstone_key(".trash/", bad).is_none(),
                "must reject {bad:?}"
            );
        }
    }

    #[test]
    fn tombstone_body_serde_roundtrip() {
        // 文件墓碑:etag/size 保留,is_dir=false
        let file = TombstoneBody {
            etag: Some("\"mock-etag\"".into()),
            size: Some(42),
            is_dir: false,
            recycle_name: None,
            recycle_i: None,
        };
        let json = serde_json::to_vec(&file).unwrap();
        let back: TombstoneBody = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.etag.as_deref(), Some("\"mock-etag\""));
        assert_eq!(back.size, Some(42));
        assert!(!back.is_dir);
        // 目录墓碑 = {"is_dir":true}(无 etag/size,skip_serializing_if 生效)
        let dir = TombstoneBody {
            etag: None,
            size: None,
            is_dir: true,
            recycle_name: None,
            recycle_i: None,
        };
        let json = serde_json::to_vec(&dir).unwrap();
        assert_eq!(json, br#"{"is_dir":true}"#.to_vec());
        let back: TombstoneBody = serde_json::from_slice(&json).unwrap();
        assert!(back.is_dir);
        assert!(back.etag.is_none() && back.size.is_none());
        // 未知字段忽略(serde 默认,前向兼容)
        let extra = br#"{"is_dir":false,"etag":"e","size":1,"future_field":true}"#;
        let back: TombstoneBody = serde_json::from_slice(extra).unwrap();
        assert_eq!(back.etag.as_deref(), Some("e"));
        assert_eq!(back.size, Some(1));
        assert!(!back.is_dir);
        // 系统回收站字段(裁决 R2/R8):recycle_name/recycle_i 往返 + 缺失默认 None
        let sys = TombstoneBody {
            etag: None,
            size: Some(42),
            is_dir: false,
            recycle_name: Some("$R4de00001a.txt".into()),
            recycle_i: Some(vec![1, 2, 3]),
        };
        let json = serde_json::to_vec(&sys).unwrap();
        let back: TombstoneBody = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.recycle_name.as_deref(), Some("$R4de00001a.txt"));
        assert_eq!(back.recycle_i.as_deref(), Some(&vec![1, 2, 3][..]));
        // 旧版墓碑(无新字段)→ None(前向兼容)
        let legacy = br#"{"is_dir":false,"size":42}"#;
        let back: TombstoneBody = serde_json::from_slice(legacy).unwrap();
        assert!(back.recycle_name.is_none());
        assert!(back.recycle_i.is_none());
        // None 字段不序列化(保持旧版字节兼容:旧客户端读取新墓碑不炸)
        let none_i = TombstoneBody {
            etag: None,
            size: Some(7),
            is_dir: false,
            recycle_name: None,
            recycle_i: None,
        };
        let json = serde_json::to_vec(&none_i).unwrap();
        assert_eq!(json, br#"{"size":7,"is_dir":false}"#.to_vec());
    }

    #[test]
    fn tombstone_index_is_file_covered_and_entries() {
        let mut idx = TombstoneIndex::default();
        idx.insert("a.txt", false, d((2026, 8, 16)));
        idx.insert("docs", true, d((2026, 8, 16))); // 归一化 "docs/"
        // is_file_covered 仅 files 精确命中(清墓碑门控:目录覆盖不算)
        assert!(idx.is_file_covered("a.txt"));
        assert!(!idx.is_file_covered("docs"));
        assert!(
            !idx.is_file_covered("docs/x.txt"),
            "目录覆盖不进 files 门控"
        );
        // entries 全量枚举(带 date,裁决 R4)
        let mut entries = idx.entries();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                ("a.txt".to_string(), false, d((2026, 8, 16))),
                ("docs/".to_string(), true, d((2026, 8, 16))),
            ]
        );
        // len = files + dirs
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn date_partition_utc_crosses_day_boundary() {
        fn systime_at(rfc3339: &str) -> std::time::SystemTime {
            let dt: chrono::DateTime<chrono::Utc> = rfc3339.parse().unwrap();
            std::time::SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(dt.timestamp().max(0) as u64))
                .unwrap()
        }
        // 东八区 8/17 07:59 = UTC 8/16 23:59 —— 分区必须按 UTC 日期
        assert_eq!(
            date_partition_utc(systime_at("2026-08-16T23:59:00Z")),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()
        );
        // UTC 午夜整点后的第一秒
        assert_eq!(
            date_partition_utc(systime_at("2026-08-17T00:00:01Z")),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()
        );
    }

    /// L4 机制钉:refresh 捕获代际 → GC 并发完成(代际推进)→ 旧代际快照
    /// 的 apply_added 必须整体丢弃 —— 否则已删墓碑被重插回索引,隐藏至
    /// 下轮全量重建(同进程刷新任务与 GC 竞态,规格 4.4 风险 4)。
    /// 竞态本身(列表快照与 apply 之间的调度窗口)无法确定性复现,这里
    /// 直接按「refresh 捕获旧代际、GC 已推进代际」的等价状态驱动机制。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn apply_added_discards_stale_generation_snapshot() {
        let (_mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::from_millis(1)).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let state = TrashState::new(
            ".trash/".to_string(),
            TrashRefreshMode::Lazy,
            Duration::from_secs(30),
            Duration::from_secs(600),
            Duration::from_secs(86400),
            crate::ossfs::TRASH_RETENTION_DAYS,
        );
        fs.trash = Some(state.clone());
        // refresh 在 GC 之前捕获代际
        let stale_gen = state.generation.load(Ordering::SeqCst);
        // GC 并发完成:代际推进(trash_gc 收尾的同一操作)
        state.generation.fetch_add(1, Ordering::SeqCst);
        // refresh 把陈旧快照(含 GC 刚删的墓碑 x.txt)交给 apply
        let applied = state
            .apply_added(
                &fs,
                &[("x.txt".to_string(), false, d((2026, 8, 16)))],
                stale_gen,
            )
            .await
            .unwrap();
        assert!(!applied, "旧代际快照必须被丢弃(L4)");
        assert!(
            !state.index.read().unwrap().is_covered("x.txt"),
            "已删墓碑不得重插回索引"
        );
        // 当前代际的 apply 正常工作
        let cur_gen = state.generation.load(Ordering::SeqCst);
        let applied = state
            .apply_added(
                &fs,
                &[("y.txt".to_string(), false, d((2026, 8, 16)))],
                cur_gen,
            )
            .await
            .unwrap();
        assert!(applied, "当前代际快照正常应用");
        assert!(state.index.read().unwrap().is_covered("y.txt"));
    }

    // ---------- 单元 4:$I 捕获落 body(set_recycle_i) ----------

    /// 单元 4 统一种子:文件墓碑(索引 + by_name/by_key + mock 对象)。
    /// recycle_name None 模拟「索引有、反向索引未填充」的跨端场景
    /// (裁决 R3 ③ 冷路径)。
    fn seed_win_tombstone(
        mock: &crate::ossfs::MockS3,
        trash: &TrashState,
        original_key: &str,
        recycle_name: Option<&str>,
        etag: Option<&str>,
        size: Option<u64>,
    ) {
        trash
            .index
            .write()
            .unwrap()
            .insert(original_key, false, d((2026, 8, 16)));
        let tomb_key = encode_tombstone_key(&trash.prefix, d((2026, 8, 16)), original_key, false);
        if let Some(name) = recycle_name {
            trash
                .recycle_names
                .write()
                .unwrap()
                .by_name
                .insert(name.to_string(), tomb_key.clone());
            trash
                .recycle_names
                .write()
                .unwrap()
                .by_key
                .insert(original_key.to_string(), name.to_string());
        }
        let body = TombstoneBody {
            etag: etag.map(str::to_string),
            size,
            is_dir: false,
            recycle_name: recycle_name.map(str::to_string),
            recycle_i: None,
        };
        mock.set_object(&tomb_key, serde_json::to_vec(&body).unwrap());
    }

    fn plain_put_targets(mock: &crate::ossfs::MockS3) -> Vec<String> {
        mock.recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.method == "PUT" && {
                    let q = r.target.to_lowercase();
                    !q.contains("partnumber") && !q.contains("uploadid")
                }
            })
            .map(|r| r.target.clone())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_recycle_i_condition_failed_does_not_resurrect_ghost() {
        // F8:GET→PUT 窗口内墓碑被并发 restore/永久删/清空删除(force
        // 412 模拟)→ 条件写失败 → 丢弃捕获字节、不复活幽灵墓碑。
        // 修复前无条件 PUT 把已删墓碑重新写回,条目以幽灵形态残留
        // (stat 可合成但 read/restore 404)。
        let (mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::ZERO).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let trash = win_state();
        fs.trash = Some(trash.clone());
        seed_win_tombstone(
            &mock,
            &trash,
            "docs/a.txt",
            Some("$R4de00001a.txt"),
            None,
            None,
        );
        mock.force_precondition_failed.store(true, Ordering::SeqCst);
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", vec![0x01, 0x02])
            .await
            .expect("条件写失败必须吞掉为 no-op(不阻塞 restore)");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(back.recycle_i, None, "捕获字节不得写入(不复活幽灵墓碑)");
        // 正向对照:无并发删除时捕获正常落 body
        mock.force_precondition_failed
            .store(false, Ordering::SeqCst);
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", vec![0x03])
            .await
            .expect("无并发删除捕获落 body");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(back.recycle_i.as_deref(), Some(&[0x03][..]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_recycle_i_updates_tombstone_body_preserving_etag_size() {
        // 裁决 R8:捕获字节落墓碑 body(update 式写,保 etag/size);
        // P8:桶中无真实 $I 对象(唯一 PUT 是墓碑)。
        let (mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::ZERO).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let trash = win_state();
        fs.trash = Some(trash.clone());
        seed_win_tombstone(
            &mock,
            &trash,
            "docs/a.txt",
            Some("$R4de00001a.txt"),
            Some("\"e-tag-1\""),
            Some(42),
        );
        let bytes = vec![0x01u8, 0, 0, 0, 0x11, 0x22];
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", bytes.clone())
            .await
            .expect("set_recycle_i");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(back.recycle_i.as_deref(), Some(&bytes[..]));
        assert_eq!(back.etag.as_deref(), Some("\"e-tag-1\""), "etag 保留");
        assert_eq!(back.size, Some(42), "size 保留");
        assert_eq!(
            back.recycle_name.as_deref(),
            Some("$R4de00001a.txt"),
            "recycle_name 保留"
        );
        let puts = plain_put_targets(&mock);
        assert_eq!(puts.len(), 1, "仅墓碑一次 PUT");
        assert!(
            !puts[0].contains("$I4de00001a"),
            "P8:桶中不得出现真实 $I 对象,got {puts:?}"
        );
        // 幂等:重入覆盖
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", vec![9, 9])
            .await
            .expect("重入覆盖");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(back.recycle_i.as_deref(), Some(&[9, 9][..]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_recycle_i_truncates_over_4k() {
        // 阈值验证:MAX_RECYCLE_I_BYTES 落地带测试(阈值规范)。
        let (mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::ZERO).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let trash = win_state();
        fs.trash = Some(trash.clone());
        seed_win_tombstone(
            &mock,
            &trash,
            "docs/a.txt",
            Some("$R4de00001a.txt"),
            None,
            None,
        );
        let big = vec![0xABu8; MAX_RECYCLE_I_BYTES + 100];
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", big)
            .await
            .expect("set_recycle_i");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(
            back.recycle_i.unwrap().len(),
            MAX_RECYCLE_I_BYTES,
            "超限截断"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_recycle_i_missing_tombstone_is_noop() {
        // 捕获丢失:对应 $R 墓碑不可解析 → warn + no-op,零请求。
        let (mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::ZERO).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let trash = win_state();
        fs.trash = Some(trash.clone());
        trash
            .set_recycle_i(&fs, "$Ideadbeef.txt", vec![1, 2, 3])
            .await
            .expect("no-op Ok");
        assert_eq!(
            mock.recorded.lock().unwrap().len(),
            0,
            "未命中必须零远程(不阻塞 restore)"
        );
        // 非 $I 形态:防御性 no-op
        trash
            .set_recycle_i(&fs, "random.txt", vec![1])
            .await
            .expect("非 $I no-op");
        assert_eq!(mock.recorded.lock().unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_recycle_i_cold_scan_fills_by_key_uncovered() {
        // 裁决 R3 ③:by_name 未填充(如另一客户端软删、本端未刷新)时,
        // 冷路径按需 GET body 扫描填充后仍能落捕获。
        let (mock, port) = crate::ossfs::MockS3::start(Vec::new(), Duration::ZERO).await;
        let mut fs = crate::ossfs::test_fs(port, 32);
        let trash = win_state();
        fs.trash = Some(trash.clone());
        // 模拟「索引有、body 带 recycle_name、反向索引未填充」的跨端
        // 场景(裁决 R3 ③ 冷路径)
        seed_win_tombstone(
            &mock,
            &trash,
            "docs/a.txt",
            Some("$R4de00001a.txt"),
            None,
            None,
        );
        trash.recycle_names.write().unwrap().by_name.clear();
        trash.recycle_names.write().unwrap().by_key.clear();
        let bytes = vec![7u8, 7, 7];
        trash
            .set_recycle_i(&fs, "$I4de00001a.txt", bytes.clone())
            .await
            .expect("冷路径仍应落捕获");
        let back: TombstoneBody =
            serde_json::from_slice(&mock.objects.lock().unwrap()[".trash/2026-08-16/docs/a.txt"])
                .unwrap();
        assert_eq!(back.recycle_i.as_deref(), Some(&bytes[..]));
        assert!(
            trash
                .recycle_names
                .read()
                .unwrap()
                .by_name
                .contains_key("$R4de00001a.txt"),
            "冷路径填充 by_name"
        );
    }

    #[test]
    fn synthesized_i_len_counts_utf16_units() {
        // F11:4B 长度字段按 UTF-16 单元数计(修复前 chars().count() ——
        // emoji 等非 BMP 字符少算,依赖长度字段的第三方回收站查看器
        // 解析截断/错位)。
        let ascii = "C:\\docs\\a.txt";
        assert_eq!(
            synthesized_i_len(ascii),
            8 + 8 + 8 + 4 + 2 * ascii.chars().count(),
            "纯 ASCII:chars == UTF-16 单元"
        );
        let emoji = "C:\\文档\\😀.txt";
        let units = emoji.encode_utf16().count();
        assert_eq!(synthesized_i_len(emoji), 8 + 8 + 8 + 4 + 2 * units);
        assert!(units > emoji.chars().count(), "emoji 是多 UTF-16 单元字符");
    }

    #[test]
    fn i_entry_has_r_tombstone_matrix() {
        let trash = win_state();
        assert!(!trash.i_entry_has_r_tombstone("$I4de00001a.txt"), "未种子");
        trash
            .recycle_names
            .write()
            .unwrap()
            .by_name
            .insert("$R4de00001a.txt".into(), "k".into());
        assert!(trash.i_entry_has_r_tombstone("$I4de00001a.txt"));
        trash
            .recycle_names
            .write()
            .unwrap()
            .by_name
            .insert("$RDEADBEEF.bin".into(), "k".into());
        assert!(
            trash.i_entry_has_r_tombstone("$IDEADBEEF.bin"),
            "hex 大小写均可"
        );
        assert!(
            !trash.i_entry_has_r_tombstone("$R4de00001a.txt"),
            "$R 非 $I 形态"
        );
        assert!(!trash.i_entry_has_r_tombstone("$Ixyz.txt"), "非 8 位 hex");
        assert!(!trash.i_entry_has_r_tombstone("plain.txt"));
    }
}
