//! Write-path upload planning helpers.
//!
//! Split out of `writer.rs` (part of the writer.rs decomposition): upload plan
//! bookkeeping, persist/upload join helpers and the write-origin classification
//! used by the upload pipeline. Pure code motion — no behavior changes.

use crate::chunk::writer::UploadPriority;
use bytes::Bytes;
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOriginKind {
    Unknown,
    NormalOnly,
    CachedOnly,
    Mixed,
}

pub(crate) struct UploadPlan {
    pub(crate) chunk_id: u64,
    pub(crate) data: Vec<(usize, Vec<Bytes>)>,
    pub(crate) slice_id: Option<u64>,
    pub(crate) uploaded: u64,
    pub(crate) write_origin: WriteOriginKind,
}

pub(crate) async fn join_best_effort_persist<P, U, T>(
    persist: Option<P>,
    upload: U,
) -> (Option<anyhow::Result<()>>, T)
where
    P: Future<Output = anyhow::Result<()>>,
    U: Future<Output = T>,
{
    match persist {
        Some(persist) => {
            let (persist_result, upload_result) = tokio::join!(persist, upload);
            (Some(persist_result), upload_result)
        }
        None => (None, upload.await),
    }
}

pub(crate) async fn join_writeback_stage_then_upload<P, U, T>(
    persist: P,
    upload: U,
) -> (anyhow::Result<()>, T)
where
    P: Future<Output = anyhow::Result<()>>,
    U: Future<Output = T>,
{
    let persist_result = persist.await;
    let upload_result = upload.await;
    (persist_result, upload_result)
}

pub(crate) fn should_stage_first_writeback_upload(
    priority: UploadPriority,
    has_persist: bool,
    write_origin: WriteOriginKind,
) -> bool {
    has_persist
        && matches!(priority, UploadPriority::Writeback)
        && matches!(write_origin, WriteOriginKind::CachedOnly)
}
