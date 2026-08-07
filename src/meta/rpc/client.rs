//! gRPC client implementing the `MetaStore` trait (`RpcMetaStore`).
//!
//! Lets `MetaClient` (and therefore VFS/FUSE) talk to a standalone metadata
//! service instead of a database directly. The generated client types come
//! from `brewfs-meta-proto`.
//!
//! Known v1 limitations (documented in gqf2008/brewfs#19):
//! - xattr / ACL / get_names / get_paths / GC-accounting have no RPC method
//!   yet in the contract and return `MetaError::NotImplemented`. They will be
//!   added together with the invalidation work (#20/#21).
//! - `write` (atomic slice-append + size-extend) is not a single RPC in v1;
//!   it is emulated with `AppendSlice` + `SetFileSize`, so the two steps are
//!   not atomic on the wire.

use crate::chunk::SliceDesc;
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::rpc::server::MetaServiceImpl; // not used directly; keeps module linkage clear
use crate::meta::store::{
    CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, MetaStore, MetaStoreCapabilities,
    OpenFlags, SetAttrFlags, SetAttrRequest, StatFsSnapshot,
};
use brewfs_meta_proto::v1::meta_service_client::MetaServiceClient;
use brewfs_meta_proto::v1::{
    self as proto, FileLockType as ProtoFileLockType, FileType as ProtoFileType,
    OpenFlags as ProtoOpenFlags, SetAttrFlags as ProtoSetAttrFlags,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// `MetaStore` implementation backed by a metadata service gRPC endpoint.
#[derive(Clone)]
pub struct RpcMetaStore {
    client: MetaServiceClient<Channel>,
}

impl RpcMetaStore {
    /// Connect to a metadata service at `endpoint` (e.g. `http://127.0.0.1:7001`).
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let client = MetaServiceClient::connect(endpoint.into()).await?;
        Ok(Self { client })
    }

    async fn stat_inner(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let resp = self
            .client
            .clone()
            .stat(proto::StatRequest { ino })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.attr.map(file_attr_from_proto))
    }
}

// ---------------------------------------------------------------------------
// Conversions (client side; mirrors server.rs)
// ---------------------------------------------------------------------------

fn file_type_from_proto(kind: i32) -> Option<FileType> {
    match ProtoFileType::try_from(kind).ok()? {
        ProtoFileType::Regular => Some(FileType::File),
        ProtoFileType::Directory => Some(FileType::Dir),
        ProtoFileType::Symlink => Some(FileType::Symlink),
        ProtoFileType::Fifo => Some(FileType::Fifo),
        ProtoFileType::Socket => Some(FileType::Socket),
        ProtoFileType::CharacterDevice => Some(FileType::CharDevice),
        ProtoFileType::BlockDevice => Some(FileType::BlockDevice),
        ProtoFileType::Unspecified => None,
    }
}

fn file_attr_from_proto(attr: proto::FileAttr) -> FileAttr {
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        kind: file_type_from_proto(attr.kind).unwrap_or(FileType::File),
        mode: attr.perm,
        rdev: attr.rdev,
        uid: attr.uid,
        gid: attr.gid,
        atime: attr.atime_ns,
        mtime: attr.mtime_ns,
        ctime: attr.ctime_ns,
        nlink: attr.nlink,
    }
}

fn dir_entry_from_proto(entry: proto::DirEntry) -> DirEntry {
    DirEntry {
        name: entry.name,
        ino: entry.ino,
        kind: file_type_from_proto(entry.kind).unwrap_or(FileType::File),
    }
}

fn slice_desc_from_proto(slice: proto::SliceDesc) -> SliceDesc {
    SliceDesc {
        slice_id: slice.slice_id,
        chunk_id: slice.chunk_id,
        offset: slice.offset,
        length: slice.length,
    }
}

fn open_flags_to_proto(flags: OpenFlags) -> ProtoOpenFlags {
    let bits = flags.bits();
    ProtoOpenFlags {
        read: bits & OpenFlags::RDONLY.bits() != 0 && bits & OpenFlags::WRONLY.bits() == 0,
        write: bits & OpenFlags::WRONLY.bits() != 0
            || bits & OpenFlags::RDWR.bits() == OpenFlags::RDWR.bits(),
        append: flags.contains(OpenFlags::APPEND),
        truncate: flags.contains(OpenFlags::TRUNC),
        create: flags.contains(OpenFlags::CREATE),
        ..Default::default()
    }
}

fn set_attr_to_proto(req: &SetAttrRequest) -> proto::SetAttrRequest {
    proto::SetAttrRequest {
        mode: req.mode,
        uid: req.uid,
        gid: req.gid,
        size: req.size,
        atime_ns: req.atime,
        mtime_ns: req.mtime,
    }
}

fn set_attr_flags_to_proto(flags: SetAttrFlags) -> ProtoSetAttrFlags {
    ProtoSetAttrFlags {
        atime_now: flags.contains(SetAttrFlags::SET_ATIME_NOW),
        mtime_now: flags.contains(SetAttrFlags::SET_MTIME_NOW),
        ..Default::default()
    }
}

fn lock_type_to_proto(t: FileLockType) -> i32 {
    let wire = match t {
        FileLockType::Read => ProtoFileLockType::Shared,
        FileLockType::Write => ProtoFileLockType::Exclusive,
        FileLockType::UnLock => ProtoFileLockType::Unlock,
    };
    wire as i32
}

fn lock_type_from_proto(t: i32) -> FileLockType {
    match ProtoFileLockType::try_from(t) {
        Ok(ProtoFileLockType::Exclusive) => FileLockType::Write,
        Ok(ProtoFileLockType::Unlock) => FileLockType::UnLock,
        _ => FileLockType::Read,
    }
}

/// Translate a gRPC status back into `MetaError`.
///
/// v1 keeps the mapping coarse: the wire code is not embedded in the status
/// details yet, so transport-level semantics are recovered from the gRPC
/// code. Finer codes (e.g. NotDirectory vs DirectoryNotEmpty) arrive in the
/// next contract iteration together with invalidation (#20).
fn status_to_meta(status: tonic::Status) -> MetaError {
    let msg = status.message().to_string();
    match status.code() {
        tonic::Code::NotFound => MetaError::NotFound(0),
        tonic::Code::AlreadyExists => MetaError::AlreadyExists {
            parent: 0,
            name: String::new(),
        },
        tonic::Code::InvalidArgument => {
            MetaError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))
        }
        tonic::Code::Unimplemented => MetaError::NotImplemented,
        tonic::Code::PermissionDenied => MetaError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            msg,
        )),
        tonic::Code::Aborted => {
            MetaError::ContinueRetry(crate::meta::store::RetryReason::TransactionConflict)
        }
        tonic::Code::ResourceExhausted => {
            MetaError::Io(std::io::Error::new(std::io::ErrorKind::StorageFull, msg))
        }
        tonic::Code::DeadlineExceeded => {
            MetaError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, msg))
        }
        tonic::Code::Internal => MetaError::Internal(msg),
        other => MetaError::Anyhow(anyhow::anyhow!("meta rpc error ({other:?}): {msg}")),
    }
}

// ---------------------------------------------------------------------------
// MetaStore impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl MetaStore for RpcMetaStore {
    fn name(&self) -> &'static str {
        "rpc"
    }

    fn capabilities(&self) -> MetaStoreCapabilities {
        MetaStoreCapabilities {
            batch_stat: true,
            ..Default::default()
        }
    }

    fn root_ino(&self) -> i64 {
        1
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        Ok(())
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.clone().stat_inner(ino).await
    }

    async fn batch_stat(&self, inos: &[i64]) -> Result<Vec<Option<FileAttr>>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .batch_stat(proto::BatchStatRequest {
                inos: inos.to_vec(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp
            .attrs
            .into_iter()
            .map(|a| (a.ino != 0).then(|| file_attr_from_proto(a)))
            .collect())
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .lookup(proto::LookupRequest {
                parent,
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(Some(resp.ino))
    }

    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .lookup_with_attr(proto::LookupRequest {
                parent,
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        let attr = resp
            .attr
            .map(|a| file_attr_from_proto(a))
            .ok_or_else(|| MetaError::Internal("missing attr in lookup_with_attr".into()))?;
        Ok(Some((resp.ino, attr)))
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .lookup_path(proto::LookupPathRequest {
                path: path.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        let kind = file_type_from_proto(resp.kind)
            .ok_or_else(|| MetaError::Internal("invalid kind".into()))?;
        Ok(Some((resp.ino, kind)))
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .readdir(proto::ReaddirRequest { ino })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.entries.into_iter().map(dir_entry_from_proto).collect())
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .mkdir(proto::MkdirRequest { parent, name })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.ino)
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .rmdir(proto::RmdirRequest {
                parent,
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .create_file(proto::CreateFileRequest { parent, name })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.ino)
    }

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .unlink(proto::UnlinkRequest {
                parent,
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .rename(proto::RenameRequest {
                old_parent,
                old_name: old_name.to_string(),
                new_parent,
                new_name,
                noreplace: false,
                exchange: false,
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .link(proto::LinkRequest {
                ino,
                parent,
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        resp.attr
            .map(|a| file_attr_from_proto(a))
            .ok_or_else(|| MetaError::Internal("missing attr in link".into()))
    }

    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .symlink(proto::SymlinkRequest {
                parent,
                name: name.to_string(),
                target: target.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        let attr = self
            .stat_inner(resp.ino)
            .await?
            .ok_or(MetaError::NotFound(resp.ino))?;
        Ok((resp.ino, attr))
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .read_symlink(proto::ReadSymlinkRequest { ino })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.target)
    }

    async fn chmod(&self, ino: i64, new_mode: u32) -> Result<FileAttr, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .chmod(proto::ChmodRequest {
                ino,
                mode: new_mode,
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        resp.attr
            .map(|a| file_attr_from_proto(a))
            .ok_or_else(|| MetaError::Internal("missing attr in chmod".into()))
    }

    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .set_attr(proto::SetAttrRequestWrapper {
                ino,
                request: Some(set_attr_to_proto(req)),
                flags: Some(set_attr_flags_to_proto(flags)),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        resp.attr
            .map(|a| file_attr_from_proto(a))
            .ok_or_else(|| MetaError::Internal("missing attr in set_attr".into()))
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .open(proto::OpenRequest {
                ino,
                flags: Some(open_flags_to_proto(flags)),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        resp.attr
            .map(|a| file_attr_from_proto(a))
            .ok_or_else(|| MetaError::Internal("missing attr in open".into()))
    }

    async fn close(&self, ino: i64) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .close(proto::CloseRequest { ino })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .set_file_size(proto::SetFileSizeRequest { ino, size })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .truncate(proto::TruncateRequest {
                ino,
                size,
                chunk_size,
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        // v1: emulated as AppendSlice + SetFileSize (not atomic on the wire).
        let mut client = self.client.clone();
        client
            .append_slice(proto::AppendSliceRequest {
                chunk_id,
                slice: Some(proto::SliceDesc {
                    slice_id: slice.slice_id,
                    chunk_id,
                    offset: slice.offset,
                    length: slice.length,
                }),
            })
            .await
            .map_err(status_to_meta)?;
        client
            .set_file_size(proto::SetFileSizeRequest {
                ino,
                size: new_size,
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .get_slices(proto::GetSlicesRequest {
                chunk_id,
                version: 0,
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.slices.into_iter().map(slice_desc_from_proto).collect())
    }

    async fn read_slices(&self, ino: i64, chunk_index: u32) -> Result<Vec<SliceDesc>, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .read_slices(proto::ReadSlicesRequest { ino, chunk_index })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.slices.into_iter().map(slice_desc_from_proto).collect())
    }

    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .append_slice(proto::AppendSliceRequest {
                chunk_id,
                slice: Some(proto::SliceDesc {
                    slice_id: slice.slice_id,
                    chunk_id,
                    offset: slice.offset,
                    length: slice.length,
                }),
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .next_id(proto::NextIdRequest {
                key: key.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.id)
    }

    async fn get_counter(&self, name: &str) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .get_counter(proto::GetCounterRequest {
                name: name.to_string(),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.value)
    }

    async fn incr_counter(&self, name: &str, delta: i64) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .incr_counter(proto::IncrCounterRequest {
                name: name.to_string(),
                delta,
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(resp.value)
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .stat_fs(proto::StatFsRequest {})
            .await
            .map_err(status_to_meta)?
            .into_inner();
        let snap = resp
            .snapshot
            .ok_or_else(|| MetaError::Internal("missing statfs".into()))?;
        Ok(StatFsSnapshot {
            total_space: snap.total_blocks * snap.bsize,
            available_space: snap.available_blocks * snap.bsize,
            used_inodes: snap.files,
            available_inodes: snap.ffree,
        })
    }

    async fn get_flock(&self, ino: i64, owner: i64) -> Result<FileLockType, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .get_flock(proto::GetFlockRequest { ino, owner })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(lock_type_from_proto(resp.lock_type))
    }

    async fn set_flock(
        &self,
        ino: i64,
        owner: i64,
        _block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .set_flock(proto::SetFlockRequest {
                ino,
                owner,
                lock_type: lock_type_to_proto(lock_type),
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn get_plock(
        &self,
        ino: i64,
        query: &FileLockQuery,
    ) -> Result<crate::meta::file_lock::FileLockInfo, MetaError> {
        let mut client = self.client.clone();
        let resp = client
            .get_plock(proto::GetPlockRequest {
                ino,
                owner: query.owner,
                range: Some(proto::FileLockRange {
                    start: query.range.start,
                    end: query.range.end,
                }),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(crate::meta::file_lock::FileLockInfo {
            lock_type: lock_type_from_proto(resp.lock_type),
            range: query.range,
            pid: 0,
        })
    }

    async fn set_plock(
        &self,
        ino: i64,
        owner: i64,
        _block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        _pid: u32,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .set_plock(proto::SetPlockRequest {
                ino,
                owner,
                range: Some(proto::FileLockRange {
                    start: range.start,
                    end: range.end,
                }),
                lock_type: lock_type_to_proto(lock_type),
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn start_session(
        &self,
        session_info: SessionInfo,
        _token: CancellationToken,
    ) -> Result<crate::meta::client::session::Session, MetaError> {
        let mut client = self.client.clone();
        let mount_point = session_info.mount_point.clone().unwrap_or_default();
        let host_name = session_info.host_name.clone();
        let process_id = session_info.process_id;
        let resp = client
            .start_session(proto::StartSessionRequest {
                info: Some(proto::SessionInfo {
                    hostname: host_name,
                    mount_point,
                    process_name: String::new(),
                    pid: process_id,
                    version: 0,
                }),
            })
            .await
            .map_err(status_to_meta)?
            .into_inner();
        Ok(crate::meta::client::session::Session::new(
            uuid::Uuid::from_u128(resp.session_id as u128),
            chrono::Utc::now().timestamp(),
            session_info,
        ))
    }

    async fn shutdown_session(&self) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .shutdown_session(proto::ShutdownSessionRequest {})
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn cleanup_sessions(&self) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .cleanup_sessions(proto::CleanupSessionsRequest {})
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn create_file_with_attr(
        &self,
        parent: i64,
        name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        let ino = self.create_file(parent, name).await?;
        let attr = self.stat(ino).await.ok().flatten();
        Ok(CreateEntryResult { ino, attr })
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        client
            .rename(proto::RenameRequest {
                old_parent,
                old_name: old_name.to_string(),
                new_parent,
                new_name: new_name.to_string(),
                noreplace: false,
                exchange: true,
            })
            .await
            .map_err(status_to_meta)?;
        Ok(())
    }

    async fn get_names(&self, _ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        // No RPC in the v1 contract; path helpers land with invalidation (#20).
        Err(MetaError::NotImplemented)
    }

    async fn get_paths(&self, _ino: i64) -> Result<Vec<String>, MetaError> {
        Err(MetaError::NotImplemented)
    }

    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        Err(MetaError::NotImplemented)
    }

    async fn remove_file_metadata(&self, _ino: i64) -> Result<(), MetaError> {
        Err(MetaError::NotImplemented)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::client::MetaClientOptions;
    use crate::meta::factory::create_meta_store_from_url;
    use crate::meta::layer::MetaLayer;
    use crate::meta::rpc::server::MetaServiceImpl;
    use brewfs_meta_proto::v1::meta_service_server;
    use std::time::Duration;
    use tokio_stream::wrappers::TcpListenerStream;

    /// Start a metadata service over an in-memory sqlite store.
    async fn start_server() -> String {
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let store = meta_handle.store();
        store.initialize().await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(meta_service_server::MetaServiceServer::new(
                    MetaServiceImpl::new(store),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    /// Run the same metadata operation sequence against any MetaStore.
    /// Returns (dir_ino, file_ino, entries, slices, flock_type).
    #[allow(clippy::type_complexity)]
    async fn exercise(store: &dyn MetaStore) -> (i64, i64, Vec<DirEntry>, Vec<SliceDesc>) {
        let dir_ino = store.mkdir(1, "d".to_string()).await.unwrap();
        let file_ino = store.create_file(dir_ino, "f".to_string()).await.unwrap();
        let entries = store.readdir(dir_ino).await.unwrap();
        let slice = SliceDesc {
            slice_id: 7,
            chunk_id: 42,
            offset: 0,
            length: 4096,
        };
        store.append_slice(42, slice).await.unwrap();
        let slices = store.get_slices(42).await.unwrap();
        store
            .rename(dir_ino, "f", dir_ino, "g".to_string())
            .await
            .unwrap();
        (dir_ino, file_ino, entries, slices)
    }

    #[tokio::test]
    async fn rpc_store_core_operations() {
        let endpoint = start_server().await;
        let rpc = RpcMetaStore::connect(endpoint).await.unwrap();
        let direct = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap()
            .store();

        // Same operation sequence through RPC and through a direct store:
        // results must be equivalent (behavioral parity).
        let (rpc_dir, rpc_file, rpc_entries, rpc_slices) = exercise(&rpc).await;
        let (direct_dir, direct_file, direct_entries, direct_slices) = exercise(&direct).await;

        assert_eq!(rpc_dir, direct_dir);
        assert_eq!(rpc_file, direct_file);
        assert_eq!(rpc_entries.len(), direct_entries.len());
        assert_eq!(rpc_entries[0].name, direct_entries[0].name);
        assert_eq!(rpc_entries[0].ino, direct_entries[0].ino);
        assert_eq!(rpc_slices.len(), direct_slices.len());
        assert_eq!(rpc_slices[0].slice_id, direct_slices[0].slice_id);
        assert_eq!(rpc_slices[0].length, direct_slices[0].length);

        // stat parity
        let rpc_attr = rpc.stat(rpc_file).await.unwrap().unwrap();
        let direct_attr = direct.stat(direct_file).await.unwrap().unwrap();
        assert_eq!(rpc_attr.ino, direct_attr.ino);
        assert_eq!(rpc_attr.kind, direct_attr.kind);
        assert_eq!(rpc_attr.size, direct_attr.size);

        // batch_stat positional semantics
        let batch = rpc.batch_stat(&[1, rpc_dir, 999_999]).await.unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].as_ref().unwrap().ino, 1);
        assert_eq!(batch[1].as_ref().unwrap().ino, rpc_dir);
        assert!(batch[2].is_none());

        // error mapping: never-existing inode -> MetaError::NotFound
        let err = rpc.stat(999_999).await.unwrap_err();
        assert!(matches!(err, MetaError::NotFound(_)));
    }

    #[tokio::test]
    async fn meta_client_works_over_rpc_store() {
        let endpoint = start_server().await;
        let rpc = RpcMetaStore::connect(endpoint).await.unwrap();
        // with_options returns an Arc that is also held internally, so method
        // resolution on Arc<MetaClient> would hit auto_impl's `MetaStore for
        // Arc<T>` candidate (unsatisfied bounds). Call MetaLayer methods via
        // UFCS on &MetaClient to keep the cache layer exercised.
        let client = crate::meta::client::MetaClient::with_options(
            Arc::new(rpc),
            crate::meta::config::CacheCapacity::default(),
            crate::meta::config::CacheTtl::default(),
            Default::default(),
        );

        MetaLayer::initialize(&*client).await.unwrap();
        let dir_ino = MetaLayer::mkdir(&*client, 1, "d".to_string())
            .await
            .unwrap();
        let found = MetaLayer::lookup(&*client, 1, "d").await.unwrap();
        assert_eq!(found, Some(dir_ino));
        let attr = MetaLayer::stat(&*client, dir_ino).await.unwrap().unwrap();
        assert_eq!(attr.ino, dir_ino);

        let file_ino = MetaLayer::create_file(&*client, dir_ino, "f".to_string())
            .await
            .unwrap();
        let entries = MetaLayer::readdir(&*client, dir_ino).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ino, file_ino);
    }

    #[tokio::test]
    async fn watch_driven_cache_invalidation_between_clients() {
        let endpoint = start_server().await;
        let ttl = crate::meta::config::CacheTtl {
            inode_ttl: Duration::from_secs(300),
            path_ttl: Duration::from_secs(300),
        };
        let make_client = |endpoint: String| async move {
            let opts = MetaClientOptions {
                watch_endpoint: Some(endpoint.clone()),
                ..Default::default()
            };
            crate::meta::client::MetaClient::with_options(
                Arc::new(RpcMetaStore::connect(endpoint).await.unwrap()),
                crate::meta::config::CacheCapacity::default(),
                ttl.clone(),
                opts,
            )
        };
        let a = make_client(endpoint.clone()).await;
        let b = make_client(endpoint.clone()).await;
        MetaLayer::initialize(&*a).await.unwrap();
        MetaLayer::initialize(&*b).await.unwrap();

        // A creates a directory; B warms its (empty) readdir cache.
        let dir_ino = MetaLayer::mkdir(&*a, 1, "d".to_string()).await.unwrap();
        assert!(MetaLayer::readdir(&*b, dir_ino).await.unwrap().is_empty());

        // A adds a file; the invalidation event must expire B's cached listing.
        MetaLayer::create_file(&*a, dir_ino, "f".to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entries = MetaLayer::readdir(&*b, dir_ino).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "f");
    }
}
