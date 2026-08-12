//! `ossfs` — mount an S3-compatible bucket (Aliyun OSS, MinIO, ...) as a local
//! network drive with **no local metadata database**. The bucket is the single
//! source of truth; paths are encoded into object keys.
//!
//! Platform mount adapters:
//! - Windows: WinFsp (`ossfs::winfsp`)
//! - macOS / Linux: FUSE (`ossfs::fuse`)

pub mod ossfs;

// Re-export the OSS filesystem surface for `ossmount` and external users.
pub use crate::ossfs::*;
