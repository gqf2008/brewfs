//! 回收站(soft delete / trash)索引与墓碑编解码。
//!
//! 形态:删除只写一个小墓碑对象到 `<trash_prefix><YYYY-MM-DD>/<原key>`,
//! 原对象留在原地,由挂载端 [`crate::ossfs::ObjectFs::hidden_key`] 过滤隐藏。
//! 恢复 = 删墓碑;真正清除 = GC(单元 4)。多端同步 = 周期拉取 `.trash/`
//! 前缀重建/增量索引(单元 3)。
//!
//! metadata-less 原则:墓碑本身就是唯一状态源,本模块不引入本地元数据库。

use crate::ossfs::{ObjectFs, next_page_token};
use anyhow::{Context as _, Result};
use std::collections::HashSet;
use std::sync::RwLock;
use std::sync::atomic::Ordering;

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

/// 被删 key 索引。精确命中(文件墓碑)或前缀覆盖(目录墓碑)。
/// files 用 HashSet(精确匹配),dirs 用排序 Vec(前缀二分)—— 不用布隆过滤器的
/// 理由见设计稿 D2:布隆不可删除(与恢复冲突),且误报方向是藏起活文件。
#[derive(Debug, Default)]
pub struct TombstoneIndex {
    /// 被删精确 key(文件,含命名空间前缀,无尾斜杠)
    pub files: HashSet<String>,
    /// 被删目录前缀(一律以 '/' 结尾,含命名空间前缀),升序、无重复
    pub dirs: Vec<String>,
}

impl TombstoneIndex {
    /// key 是否被覆盖:files 精确命中,或 dirs 中存在 key 的前缀。
    /// 关键正确性细节:目录形态双探测 —— key 不以 '/' 结尾时必须再探测 key+"/"。
    /// (stat("/docs") 得 key "docs",目录墓碑存 "docs/";只对 key 前缀匹配会漏,
    /// 经 marker HEAD 把已删目录复活。)
    pub fn is_covered(&self, key: &str) -> bool {
        if self.files.contains(key) {
            return true;
        }
        if self.dirs.is_empty() {
            return false;
        }
        let dir_key = if key.ends_with('/') {
            key.to_string()
        } else {
            format!("{key}/")
        };
        let i = self.dirs.partition_point(|d| d.as_str() < dir_key.as_str());
        // 二分正确性:覆盖 dir_key 的目录墓碑 D 满足 D <= dir_key(相等时 D == dir_key,
        // 落在 partition_point 下标处)。D < dir_key 的全部在 [0..i);D == dir_key
        // 在 dirs[i](当 i < len 时)—— 取 [0..=i],i == len 时退化为 [0..len)。
        // 从尾部向前扫先命中最长前缀。最坏回扫长度 = 路径深度(通常 < 20)。
        // 500k 条目 ≈ 19 次比较,纳秒级。
        let end = if i < self.dirs.len() { i + 1 } else { i };
        self.dirs[..end]
            .iter()
            .rev()
            .any(|d| dir_key.starts_with(d.as_str()))
    }

    /// 插入墓碑。is_dir=true 归一化补尾斜杠并保 dirs 升序;重复插入幂等。
    pub fn insert(&mut self, key: &str, is_dir: bool) {
        if is_dir {
            let dir = if key.ends_with('/') {
                key.to_string()
            } else {
                format!("{key}/")
            };
            if let Err(pos) = self.dirs.binary_search(&dir) {
                self.dirs.insert(pos, dir);
            }
        } else {
            self.files.insert(key.to_string());
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
            if let Ok(pos) = self.dirs.binary_search(&dir) {
                self.dirs.remove(pos);
            }
        } else {
            self.files.remove(key);
        }
    }

    /// 整体替换;dirs sort_unstable+dedup;files 由 HashSet 天然去重(同名多日期墓碑)。
    pub fn rebuild(&mut self, tombstones: impl Iterator<Item = (String, bool)>) {
        let mut files = HashSet::new();
        let mut dirs = Vec::new();
        for (key, is_dir) in tombstones {
            if is_dir {
                let dir = if key.ends_with('/') {
                    key
                } else {
                    format!("{key}/")
                };
                dirs.push(dir);
            } else {
                files.insert(key);
            }
        }
        dirs.sort_unstable();
        dirs.dedup();
        self.files = files;
        self.dirs = dirs;
    }
}

/// 回收站运行状态:墓碑前缀 + 本地索引。挂在 `ObjectFs.trash` 上
/// (`Option<Arc<TrashState>>`,None = 回收站关闭,硬删除)。
/// 锁纪律:调用方不得跨 await 持有 index 锁;读锁只应在 is_covered 内瞬时持有。
#[derive(Debug)]
pub(crate) struct TrashState {
    /// 墓碑前缀,如 "ossfs/.trash/"(含命名空间,尾斜杠)
    pub prefix: String,
    /// 本地索引(files + dirs)
    pub index: RwLock<TombstoneIndex>,
}

/// 以 trash_prefix 分页枚举墓碑对象 key。start_after 传 Some 时携带
/// ListObjectsV2 start-after 参数(单元 3);None 为从头全量。
/// 分页间续 token 处理复用 next_page_token 的 truncated 护栏(#60)。
/// 不 acquire limiter permit:调用方决定(rebuild 全程持一个 permit,
/// eager 挂点靠 poll_inflight 互斥,均见各调用点注释)。
/// s3_lists 计数与 list_impl 对齐(每页 +1)。
pub(crate) async fn list_trash_keys(
    fs: &ObjectFs,
    start_after: Option<&str>,
    mut on_page: impl FnMut(Vec<String>) -> Result<()>,
) -> Result<()> {
    let Some(trash) = &fs.trash else {
        return Ok(());
    };
    let trash_prefix = trash.prefix.clone();
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

    fn index_with_dirs(dirs: &[&str]) -> TombstoneIndex {
        let mut idx = TombstoneIndex::default();
        for d in dirs {
            idx.insert(d, true);
        }
        idx
    }

    #[test]
    fn is_covered_matrix() {
        // 文件精确命中
        let mut idx = TombstoneIndex::default();
        idx.files.insert("docs/a.txt".into());
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
    fn dirs_sorted_invariant() {
        // 乱序插入后 dirs 保持升序、无重复(二分的前提)
        let mut idx = TombstoneIndex::default();
        for d in ["z/", "a/", "m/", "b/", "a/"] {
            idx.insert(d, true);
        }
        for i in 1..idx.dirs.len() {
            assert!(
                idx.dirs[i - 1] < idx.dirs[i],
                "dirs must stay sorted: {:?}",
                idx.dirs
            );
        }
        assert_eq!(idx.dirs.len(), 4, "重复插入必须幂等去重");
        // insert("docs", true) 归一化补尾斜杠
        let mut idx = TombstoneIndex::default();
        idx.insert("docs", true);
        assert_eq!(idx.dirs, vec!["docs/".to_string()]);
        // 固定种子伪随机序列(长度 > 二分扫描最坏回扫深度)
        let mut seed = 0x5eed_u64;
        let mut idx = TombstoneIndex::default();
        for _ in 0..200 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let n = (seed % 30) as usize;
            idx.insert(&format!("dir{n}/sub"), true);
        }
        for i in 1..idx.dirs.len() {
            assert!(idx.dirs[i - 1] < idx.dirs[i], "sorted invariant");
        }
    }

    #[test]
    fn remove_flips_coverage() {
        // 文件移除
        let mut idx = TombstoneIndex::default();
        idx.insert("a.txt", false);
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
        idx.insert("x", false);
        idx.insert("x", true);
        assert!(idx.is_covered("x"));
        idx.remove("x", false);
        assert!(idx.is_covered("x"), "目录墓碑仍覆盖");
        idx.remove("x", true);
        assert!(!idx.is_covered("x"));
    }

    #[test]
    fn rebuild_replaces_and_dedups() {
        let mut idx = index_with_dirs(&["old/"]);
        idx.files.insert("old.txt".into());
        let tombstones = vec![
            ("docs/a.txt".to_string(), false),
            ("docs/".to_string(), true),
            ("docs/a.txt".to_string(), false), // 同名多日期墓碑只留一条
            ("z/".to_string(), true),
            ("docs".to_string(), true), // 无尾斜杠归一化
            ("a/".to_string(), true),
        ];
        idx.rebuild(tombstones.into_iter());
        // 整体替换:旧条目消失
        assert!(!idx.is_covered("old.txt"));
        assert!(idx.is_covered("docs/a.txt"));
        // 排序去重:docs/ 只有一条
        assert_eq!(
            idx.dirs,
            vec!["a/".to_string(), "docs/".to_string(), "z/".to_string()]
        );
        assert_eq!(idx.files.len(), 1);
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
}
