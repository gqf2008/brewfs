//! Metadata service gRPC server.
//!
//! Bridges the generated `MetaService` contract (`brewfs-meta-proto`) to the
//! existing `MetaStore` trait. The server keeps the backend as the single
//! source of truth; clients retain their own caches and subscribe to
//! invalidation events (`MetaWatch`, tracked in gqf2008/brewfs#20).
//!
//! Error translation uses `crate::meta::rpc::error::meta_error_code` so the
//! wire codes stay aligned with `MetaError -> VfsError -> errno`.

use super::error::meta_error_code;
use crate::chunk::SliceDesc;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{
    DirEntry, FileAttr, FileType, MetaError, MetaStore, OpenFlags, SetAttrFlags, SetAttrRequest,
    StatFsSnapshot,
};
use brewfs_meta_proto::v1::{
    self as proto, FileLockType as ProtoFileLockType, FileType as ProtoFileType,
    OpenFlags as ProtoOpenFlags, SetAttrFlags as ProtoSetAttrFlags,
    meta_service_server::MetaService,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC server implementing the metadata service contract over a `MetaStore`.
#[derive(Clone)]
pub struct MetaServiceImpl<S> {
    store: Arc<S>,
}

impl<S> MetaServiceImpl<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

fn file_type_to_proto(kind: FileType) -> i32 {
    let t = match kind {
        FileType::File => ProtoFileType::Regular,
        FileType::Dir => ProtoFileType::Directory,
        FileType::Symlink => ProtoFileType::Symlink,
        FileType::Fifo => ProtoFileType::Fifo,
        FileType::Socket => ProtoFileType::Socket,
        FileType::CharDevice => ProtoFileType::CharacterDevice,
        FileType::BlockDevice => ProtoFileType::BlockDevice,
    };
    t as i32
}

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

fn file_attr_to_proto(attr: &FileAttr) -> proto::FileAttr {
    proto::FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime_ns: attr.atime,
        mtime_ns: attr.mtime,
        ctime_ns: attr.ctime,
        kind: file_type_to_proto(attr.kind),
        perm: attr.mode,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: 4096,
        crtime_ns: 0,
    }
}

fn dir_entry_to_proto(entry: &DirEntry) -> proto::DirEntry {
    proto::DirEntry {
        name: entry.name.clone(),
        ino: entry.ino,
        kind: file_type_to_proto(entry.kind),
    }
}

fn slice_desc_to_proto(slice: &SliceDesc) -> proto::SliceDesc {
    proto::SliceDesc {
        slice_id: slice.slice_id,
        chunk_id: slice.chunk_id,
        offset: slice.offset,
        length: slice.length,
    }
}

fn slice_desc_from_proto(slice: &proto::SliceDesc) -> SliceDesc {
    SliceDesc {
        slice_id: slice.slice_id,
        chunk_id: slice.chunk_id,
        offset: slice.offset,
        length: slice.length,
    }
}

fn open_flags_from_proto(flags: &ProtoOpenFlags) -> OpenFlags {
    let mut out = OpenFlags::empty();
    if flags.read && flags.write {
        out |= OpenFlags::RDWR;
    } else if flags.write {
        out |= OpenFlags::WRONLY;
    } else {
        out |= OpenFlags::RDONLY;
    }
    if flags.append {
        out |= OpenFlags::APPEND;
    }
    if flags.truncate {
        out |= OpenFlags::TRUNC;
    }
    if flags.create {
        out |= OpenFlags::CREATE;
    }
    out
}

fn set_attr_from_proto(req: &proto::SetAttrRequest) -> SetAttrRequest {
    SetAttrRequest {
        mode: req.mode,
        uid: req.uid,
        gid: req.gid,
        size: req.size,
        atime: req.atime_ns,
        mtime: req.mtime_ns,
        ctime: None,
        flags: None,
    }
}

fn set_attr_flags_from_proto(flags: &ProtoSetAttrFlags) -> SetAttrFlags {
    let mut out = SetAttrFlags::empty();
    if flags.atime_now {
        out |= SetAttrFlags::SET_ATIME_NOW;
    }
    if flags.mtime_now {
        out |= SetAttrFlags::SET_MTIME_NOW;
    }
    out
}

fn lock_type_from_proto(t: i32) -> Option<FileLockType> {
    match ProtoFileLockType::try_from(t).ok()? {
        ProtoFileLockType::Shared => Some(FileLockType::Read),
        ProtoFileLockType::Exclusive => Some(FileLockType::Write),
        ProtoFileLockType::Unlock => Some(FileLockType::UnLock),
        ProtoFileLockType::Unspecified => None,
    }
}

fn lock_range_from_proto(range: Option<&proto::FileLockRange>) -> FileLockRange {
    range
        .map(|r| FileLockRange {
            start: r.start,
            end: r.end,
        })
        .unwrap_or(FileLockRange {
            start: 0,
            end: u64::MAX,
        })
}

fn stat_fs_to_proto(snapshot: &StatFsSnapshot) -> proto::StatFsSnapshot {
    const BSIZE: u64 = 4096;
    proto::StatFsSnapshot {
        total_blocks: snapshot.total_space / BSIZE,
        used_blocks: snapshot
            .total_space
            .saturating_sub(snapshot.available_space)
            / BSIZE,
        available_blocks: snapshot.available_space / BSIZE,
        files: snapshot.used_inodes,
        ffree: snapshot.available_inodes,
        bsize: BSIZE,
        frsize: BSIZE,
        namelen: 255,
    }
}

fn status(err: MetaError) -> Status {
    let code = grpc_code(meta_error_code(&err));
    Status::new(code, err.to_string())
}

/// Map a wire error code to a gRPC status code. Wire codes deliberately
/// mirror errno semantics; the mapping below keeps the common cases aligned
/// with tonic's standard codes.
fn grpc_code(code: brewfs_meta_proto::v1::MetaErrorCode) -> tonic::Code {
    use brewfs_meta_proto::v1::MetaErrorCode as C;
    match code {
        C::NotFound
        | C::LockNotFound
        | C::InvalidHandle
        | C::InvalidFilename
        | C::FilenameTooLong
        | C::InvalidPath
        | C::InvalidInput
        | C::IsDirectory
        | C::FileTooLarge
        | C::TooManyLinks
        | C::CrossDevice
        | C::QuotaExceeded
        | C::StorageFull
        | C::ReadOnly
        | C::PermissionDenied
        | C::ResourceBusy
        | C::TimedOut => tonic::Code::FailedPrecondition,
        C::AlreadyExists | C::Conflict => tonic::Code::Aborted,
        C::NotDirectory | C::DirectoryNotEmpty | C::NotSupported | C::NotImplemented => {
            tonic::Code::Unimplemented
        }
        C::LockConflict | C::Deadlock => tonic::Code::Aborted,
        C::IoError | C::Internal | C::Unspecified => tonic::Code::Internal,
    }
}

// ---------------------------------------------------------------------------
// MetaService implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl<S> MetaService for MetaServiceImpl<S>
where
    S: MetaStore + Send + Sync + 'static,
{
    // ---- queries ----
    async fn stat(
        &self,
        request: Request<proto::StatRequest>,
    ) -> Result<Response<proto::StatResponse>, Status> {
        let ino = request.into_inner().ino;
        let attr = self
            .store
            .stat(ino)
            .await
            .map_err(status)?
            .ok_or_else(|| Status::not_found(format!("inode {ino}")))?;
        Ok(Response::new(proto::StatResponse {
            attr: Some(file_attr_to_proto(&attr)),
        }))
    }

    async fn batch_stat(
        &self,
        request: Request<proto::BatchStatRequest>,
    ) -> Result<Response<proto::BatchStatResponse>, Status> {
        let inos = request.into_inner().inos;
        let attrs = self.store.batch_stat(&inos).await.map_err(status)?;
        Ok(Response::new(proto::BatchStatResponse {
            attrs: attrs
                .into_iter()
                .filter_map(|a| a.map(|a| file_attr_to_proto(&a)))
                .collect(),
        }))
    }

    async fn lookup(
        &self,
        request: Request<proto::LookupRequest>,
    ) -> Result<Response<proto::LookupResponse>, Status> {
        let req = request.into_inner();
        let ino = self
            .store
            .lookup(req.parent, &req.name)
            .await
            .map_err(status)?
            .ok_or_else(|| Status::not_found(format!("{}/{}", req.parent, req.name)))?;
        Ok(Response::new(proto::LookupResponse { ino }))
    }

    async fn lookup_with_attr(
        &self,
        request: Request<proto::LookupRequest>,
    ) -> Result<Response<proto::LookupWithAttrResponse>, Status> {
        let req = request.into_inner();
        let attr = self
            .store
            .lookup_with_attr(req.parent, &req.name)
            .await
            .map_err(status)?
            .ok_or_else(|| Status::not_found(format!("{}/{}", req.parent, req.name)))?;
        Ok(Response::new(proto::LookupWithAttrResponse {
            ino: attr.0,
            attr: Some(file_attr_to_proto(&attr.1)),
        }))
    }

    async fn lookup_path(
        &self,
        request: Request<proto::LookupPathRequest>,
    ) -> Result<Response<proto::LookupPathResponse>, Status> {
        let path = request.into_inner().path;
        let (ino, kind) = self
            .store
            .lookup_path(&path)
            .await
            .map_err(status)?
            .ok_or_else(|| Status::not_found(path.clone()))?;
        Ok(Response::new(proto::LookupPathResponse {
            ino,
            kind: file_type_to_proto(kind),
        }))
    }

    async fn readdir(
        &self,
        request: Request<proto::ReaddirRequest>,
    ) -> Result<Response<proto::ReaddirResponse>, Status> {
        let ino = request.into_inner().ino;
        let entries = self.store.readdir(ino).await.map_err(status)?;
        Ok(Response::new(proto::ReaddirResponse {
            entries: entries.iter().map(dir_entry_to_proto).collect(),
        }))
    }

    // ---- namespace mutations ----
    async fn mkdir(
        &self,
        request: Request<proto::MkdirRequest>,
    ) -> Result<Response<proto::MkdirResponse>, Status> {
        let req = request.into_inner();
        let ino = self
            .store
            .mkdir(req.parent, req.name)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::MkdirResponse { ino }))
    }

    async fn rmdir(&self, request: Request<proto::RmdirRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.store
            .rmdir(req.parent, &req.name)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    async fn create_file(
        &self,
        request: Request<proto::CreateFileRequest>,
    ) -> Result<Response<proto::CreateFileResponse>, Status> {
        let req = request.into_inner();
        let ino = self
            .store
            .create_file(req.parent, req.name)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::CreateFileResponse { ino }))
    }

    async fn unlink(&self, request: Request<proto::UnlinkRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.store
            .unlink(req.parent, &req.name)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    async fn rename(&self, request: Request<proto::RenameRequest>) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let _ = (req.noreplace, req.exchange);
        self.store
            .rename(req.old_parent, &req.old_name, req.new_parent, req.new_name)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    async fn link(
        &self,
        request: Request<proto::LinkRequest>,
    ) -> Result<Response<proto::LinkResponse>, Status> {
        let req = request.into_inner();
        let attr = self
            .store
            .link(req.ino, req.parent, &req.name)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::LinkResponse {
            attr: Some(file_attr_to_proto(&attr)),
        }))
    }

    async fn symlink(
        &self,
        request: Request<proto::SymlinkRequest>,
    ) -> Result<Response<proto::SymlinkResponse>, Status> {
        let req = request.into_inner();
        let (ino, _attr) = self
            .store
            .symlink(req.parent, &req.name, &req.target)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::SymlinkResponse { ino }))
    }

    async fn read_symlink(
        &self,
        request: Request<proto::ReadSymlinkRequest>,
    ) -> Result<Response<proto::ReadSymlinkResponse>, Status> {
        let ino = request.into_inner().ino;
        let target = self.store.read_symlink(ino).await.map_err(status)?;
        Ok(Response::new(proto::ReadSymlinkResponse { target }))
    }

    async fn chmod(
        &self,
        request: Request<proto::ChmodRequest>,
    ) -> Result<Response<proto::ChmodResponse>, Status> {
        let req = request.into_inner();
        let attr = self.store.chmod(req.ino, req.mode).await.map_err(status)?;
        Ok(Response::new(proto::ChmodResponse {
            attr: Some(file_attr_to_proto(&attr)),
        }))
    }

    async fn set_attr(
        &self,
        request: Request<proto::SetAttrRequestWrapper>,
    ) -> Result<Response<proto::SetAttrResponse>, Status> {
        let req = request.into_inner();
        let inner_req = req
            .request
            .ok_or_else(|| Status::invalid_argument("missing request"))?;
        let flags = req.flags.unwrap_or_default();
        let attr = self
            .store
            .set_attr(
                req.ino,
                &set_attr_from_proto(&inner_req),
                set_attr_flags_from_proto(&flags),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(proto::SetAttrResponse {
            attr: Some(file_attr_to_proto(&attr)),
        }))
    }

    async fn open(
        &self,
        request: Request<proto::OpenRequest>,
    ) -> Result<Response<proto::OpenResponse>, Status> {
        let req = request.into_inner();
        let flags = match req.flags {
            Some(f) => open_flags_from_proto(&f),
            None => OpenFlags::RDONLY,
        };
        let attr = self.store.open(req.ino, flags).await.map_err(status)?;
        Ok(Response::new(proto::OpenResponse {
            attr: Some(file_attr_to_proto(&attr)),
        }))
    }

    async fn close(&self, request: Request<proto::CloseRequest>) -> Result<Response<()>, Status> {
        let ino = request.into_inner().ino;
        self.store.close(ino).await.map_err(status)?;
        Ok(Response::new(()))
    }

    async fn set_file_size(
        &self,
        request: Request<proto::SetFileSizeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.store
            .set_file_size(req.ino, req.size)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    async fn truncate(
        &self,
        request: Request<proto::TruncateRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.store
            .truncate(req.ino, req.size, req.chunk_size)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    // ---- slices ----
    async fn get_slices(
        &self,
        request: Request<proto::GetSlicesRequest>,
    ) -> Result<Response<proto::GetSlicesResponse>, Status> {
        let req = request.into_inner();
        let _ = req.version; // version negotiation is validated client-side (#22)
        let slices = self.store.get_slices(req.chunk_id).await.map_err(status)?;
        Ok(Response::new(proto::GetSlicesResponse {
            version: 0,
            slices: slices.iter().map(slice_desc_to_proto).collect(),
        }))
    }

    async fn read_slices(
        &self,
        request: Request<proto::ReadSlicesRequest>,
    ) -> Result<Response<proto::ReadSlicesResponse>, Status> {
        let req = request.into_inner();
        let slices = self
            .store
            .read_slices(req.ino, req.chunk_index)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::ReadSlicesResponse {
            slices: slices.iter().map(slice_desc_to_proto).collect(),
        }))
    }

    async fn append_slice(
        &self,
        request: Request<proto::AppendSliceRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let slice = req
            .slice
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing slice"))?;
        self.store
            .append_slice(req.chunk_id, slice_desc_from_proto(slice))
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    // ---- counters / stats ----
    async fn next_id(
        &self,
        request: Request<proto::NextIdRequest>,
    ) -> Result<Response<proto::NextIdResponse>, Status> {
        let key = request.into_inner().key;
        let id = self.store.next_id(&key).await.map_err(status)?;
        Ok(Response::new(proto::NextIdResponse { id }))
    }

    async fn get_counter(
        &self,
        request: Request<proto::GetCounterRequest>,
    ) -> Result<Response<proto::GetCounterResponse>, Status> {
        let name = request.into_inner().name;
        let value = self.store.get_counter(&name).await.map_err(status)?;
        Ok(Response::new(proto::GetCounterResponse { value }))
    }

    async fn incr_counter(
        &self,
        request: Request<proto::IncrCounterRequest>,
    ) -> Result<Response<proto::IncrCounterResponse>, Status> {
        let req = request.into_inner();
        let value = self
            .store
            .incr_counter(&req.name, req.delta)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::IncrCounterResponse { value }))
    }

    async fn stat_fs(
        &self,
        _request: Request<proto::StatFsRequest>,
    ) -> Result<Response<proto::StatFsResponse>, Status> {
        let snapshot = self.store.stat_fs().await.map_err(status)?;
        Ok(Response::new(proto::StatFsResponse {
            snapshot: Some(stat_fs_to_proto(&snapshot)),
        }))
    }

    // ---- locks ----
    async fn get_flock(
        &self,
        request: Request<proto::GetFlockRequest>,
    ) -> Result<Response<proto::GetFlockResponse>, Status> {
        let req = request.into_inner();
        let lock_type = self
            .store
            .get_flock(req.ino, req.owner)
            .await
            .map_err(status)?;
        let wire = match lock_type {
            FileLockType::Read => ProtoFileLockType::Shared,
            FileLockType::Write => ProtoFileLockType::Exclusive,
            FileLockType::UnLock => ProtoFileLockType::Unlock,
        };
        Ok(Response::new(proto::GetFlockResponse {
            lock_type: wire as i32,
        }))
    }

    async fn set_flock(
        &self,
        request: Request<proto::SetFlockRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let lock_type = lock_type_from_proto(req.lock_type)
            .ok_or_else(|| Status::invalid_argument("invalid flock type"))?;
        self.store
            .set_flock(req.ino, req.owner, false, lock_type)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    async fn get_plock(
        &self,
        request: Request<proto::GetPlockRequest>,
    ) -> Result<Response<proto::GetPlockResponse>, Status> {
        let req = request.into_inner();
        let range = lock_range_from_proto(req.range.as_ref());
        let query = FileLockQuery {
            owner: req.owner,
            // The lookup only uses owner + range; lock_type is not consulted.
            lock_type: FileLockType::Read,
            range,
        };
        let info = self
            .store
            .get_plock(req.ino, &query)
            .await
            .map_err(status)?;
        let wire = match info.lock_type {
            FileLockType::Read => ProtoFileLockType::Shared,
            FileLockType::Write => ProtoFileLockType::Exclusive,
            FileLockType::UnLock => ProtoFileLockType::Unlock,
        };
        Ok(Response::new(proto::GetPlockResponse {
            lock_type: wire as i32,
        }))
    }

    async fn set_plock(
        &self,
        request: Request<proto::SetPlockRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let lock_type = lock_type_from_proto(req.lock_type)
            .ok_or_else(|| Status::invalid_argument("invalid plock type"))?;
        let range = lock_range_from_proto(req.range.as_ref());
        // pid is not part of the v1 contract; 0 keeps lock ownership tied to
        // the owner id until session plumbing lands in #21.
        self.store
            .set_plock(req.ino, req.owner, false, lock_type, range, 0)
            .await
            .map_err(status)?;
        Ok(Response::new(()))
    }

    // ---- sessions ----
    async fn start_session(
        &self,
        request: Request<proto::StartSessionRequest>,
    ) -> Result<Response<proto::StartSessionResponse>, Status> {
        let info = request
            .into_inner()
            .info
            .ok_or_else(|| Status::invalid_argument("missing session info"))?;
        let session_info = crate::meta::client::session::SessionInfo {
            version: format!("rpc-v1/{}", info.version),
            host_name: info.hostname,
            ip_addrs: Vec::new(),
            mount_point: Some(info.mount_point),
            mount_time: chrono::Utc::now(),
            process_id: info.pid,
            created_at: chrono::Utc::now(),
        };
        let token = tokio_util::sync::CancellationToken::new();
        let session = self
            .store
            .start_session(session_info, token)
            .await
            .map_err(status)?;
        Ok(Response::new(proto::StartSessionResponse {
            session_id: session.session_id.as_u128() as u64,
            volume: String::new(),
        }))
    }

    async fn shutdown_session(
        &self,
        _request: Request<proto::ShutdownSessionRequest>,
    ) -> Result<Response<()>, Status> {
        self.store.shutdown_session().await.map_err(status)?;
        Ok(Response::new(()))
    }

    async fn cleanup_sessions(
        &self,
        _request: Request<proto::CleanupSessionsRequest>,
    ) -> Result<Response<proto::CleanupSessionsResponse>, Status> {
        self.store.cleanup_sessions().await.map_err(status)?;
        Ok(Response::new(proto::CleanupSessionsResponse { removed: 0 }))
    }
}

/// Serve the metadata service on `addr` backed by `store`.
///
/// Process-internal deployment mode: the same binary can start this server
/// next to a FUSE mount. Independent-process and HA deployment land in
/// gqf2008/brewfs#21.
pub async fn serve<S>(
    store: Arc<S>,
    addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error>
where
    S: MetaStore + Send + Sync + 'static,
{
    tonic::transport::Server::builder()
        .add_service(proto::meta_service_server::MetaServiceServer::new(
            MetaServiceImpl::new(store),
        ))
        .serve(addr)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::factory::create_meta_store_from_url;
    use brewfs_meta_proto::v1::meta_service_client::MetaServiceClient;
    use brewfs_meta_proto::v1::meta_service_server;
    use brewfs_meta_proto::v1::{
        AppendSliceRequest, CreateFileRequest, GetSlicesRequest, LookupRequest, MkdirRequest,
        ReaddirRequest, RenameRequest, SliceDesc, StatRequest, UnlinkRequest,
    };
    use tokio_stream::wrappers::TcpListenerStream;

    async fn start_server() -> (String, tokio::task::JoinHandle<()>) {
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let store = meta_handle.store();
        store.initialize().await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(meta_service_server::MetaServiceServer::new(
                    MetaServiceImpl::new(store),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn roundtrip_namespace_and_slices() {
        let (endpoint, _handle) = start_server().await;
        let mut client = MetaServiceClient::connect(endpoint).await.unwrap();

        // mkdir -> lookup -> stat
        let mkdir_resp = client
            .mkdir(MkdirRequest {
                parent: 1,
                name: "d".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let dir_ino = mkdir_resp.ino;
        let lookup_resp = client
            .lookup(LookupRequest {
                parent: 1,
                name: "d".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(lookup_resp.ino, dir_ino);
        let stat_resp = client
            .stat(StatRequest { ino: dir_ino })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stat_resp.attr.unwrap().ino, dir_ino);

        // create_file -> readdir
        let file_ino = client
            .create_file(CreateFileRequest {
                parent: dir_ino,
                name: "f".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .ino;
        let entries = client
            .readdir(ReaddirRequest { ino: dir_ino })
            .await
            .unwrap()
            .into_inner()
            .entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ino, file_ino);

        // append_slice -> get_slices
        client
            .append_slice(AppendSliceRequest {
                chunk_id: 42,
                slice: Some(SliceDesc {
                    slice_id: 7,
                    chunk_id: 42,
                    offset: 0,
                    length: 4096,
                }),
            })
            .await
            .unwrap();
        let slices = client
            .get_slices(GetSlicesRequest {
                chunk_id: 42,
                version: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .slices;
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].slice_id, 7);

        // rename
        client
            .rename(RenameRequest {
                old_parent: dir_ino,
                old_name: "f".into(),
                new_parent: dir_ino,
                new_name: "g".into(),
                noreplace: false,
                exchange: false,
            })
            .await
            .unwrap();

        // unlink -> stat not found
        client
            .unlink(UnlinkRequest {
                parent: dir_ino,
                name: "g".into(),
            })
            .await
            .unwrap();
        let stat_err = client
            .stat(StatRequest { ino: file_ino })
            .await
            .unwrap_err();
        assert_eq!(stat_err.code(), tonic::Code::FailedPrecondition);

        // stat missing inode maps to NotFound wire code
        let _ = client.stat(StatRequest { ino: 999_999 }).await.unwrap_err();
    }
}
