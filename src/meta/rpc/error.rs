//! Mapping between `MetaError` and the metadata service wire error codes.
//!
//! The wire codes are defined in
//! `proto/brewfs/meta/v1/meta_service.proto` (`MetaErrorCode`). The mapping
//! below intentionally mirrors the existing `MetaError -> VfsError -> errno`
//! translation in `src/vfs/error.rs` so the RPC path does not introduce a
//! second, divergent error model.

use crate::meta::store::MetaError;
use brewfs_meta_proto::v1::MetaErrorCode;
use std::io::ErrorKind;

/// Map a `MetaError` to the wire error code.
///
/// The message carried by the gRPC status is produced by the error's
/// `Display` impl; this function only selects the code so client and
/// server stay consistent.
pub fn meta_error_code(err: &MetaError) -> MetaErrorCode {
    match err {
        MetaError::NotFound(_) | MetaError::ParentNotFound(_) => MetaErrorCode::NotFound,
        MetaError::AlreadyExists { .. } => MetaErrorCode::AlreadyExists,
        MetaError::NotDirectory(_) => MetaErrorCode::NotDirectory,
        MetaError::DirectoryNotEmpty(_) => MetaErrorCode::DirectoryNotEmpty,
        MetaError::InvalidPath(_) => MetaErrorCode::InvalidPath,
        MetaError::InvalidFilename => MetaErrorCode::InvalidFilename,
        MetaError::FilenameTooLong => MetaErrorCode::FilenameTooLong,
        MetaError::TooManySymlinks => MetaErrorCode::InvalidInput,
        MetaError::NotSupported(_) => MetaErrorCode::NotSupported,
        MetaError::NotImplemented => MetaErrorCode::NotImplemented,
        MetaError::ContinueRetry(reason) => {
            let _ = reason;
            MetaErrorCode::Conflict
        }
        MetaError::MaxRetriesExceeded => MetaErrorCode::Conflict,
        MetaError::LockConflict { .. } => MetaErrorCode::LockConflict,
        MetaError::LockNotFound { .. } => MetaErrorCode::LockNotFound,
        MetaError::DeadlockDetected { .. } => MetaErrorCode::Deadlock,
        MetaError::InvalidHandle(_) => MetaErrorCode::InvalidHandle,
        MetaError::Io(err) => io_error_code(err),
        MetaError::Database(_)
        | MetaError::Serialization(_)
        | MetaError::Config(_)
        | MetaError::SessionNotFound(_)
        | MetaError::Anyhow(_)
        | MetaError::Internal(_) => MetaErrorCode::Internal,
    }
}

fn io_error_code(err: &std::io::Error) -> MetaErrorCode {
    match err.kind() {
        ErrorKind::NotFound => MetaErrorCode::NotFound,
        ErrorKind::AlreadyExists => MetaErrorCode::AlreadyExists,
        ErrorKind::PermissionDenied => MetaErrorCode::PermissionDenied,
        ErrorKind::WouldBlock => MetaErrorCode::LockConflict,
        ErrorKind::TimedOut => MetaErrorCode::TimedOut,
        ErrorKind::StorageFull => MetaErrorCode::StorageFull,
        ErrorKind::QuotaExceeded => MetaErrorCode::QuotaExceeded,
        ErrorKind::CrossesDevices => MetaErrorCode::CrossDevice,
        ErrorKind::TooManyLinks => MetaErrorCode::TooManyLinks,
        ErrorKind::InvalidInput => MetaErrorCode::InvalidInput,
        ErrorKind::Interrupted => MetaErrorCode::Conflict,
        ErrorKind::Unsupported => MetaErrorCode::NotSupported,
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe
        | ErrorKind::NetworkUnreachable
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkDown => MetaErrorCode::IoError,
        _ => MetaErrorCode::IoError,
    }
}

#[cfg(test)]
mod tests {
    use super::meta_error_code;
    use crate::meta::file_lock::FileLockRange;
    use crate::meta::store::{MetaError, RetryReason};
    use brewfs_meta_proto::v1::MetaErrorCode;
    use std::io;

    fn code(err: MetaError) -> MetaErrorCode {
        meta_error_code(&err)
    }

    #[test]
    fn maps_not_found_family() {
        assert_eq!(code(MetaError::NotFound(1)), MetaErrorCode::NotFound);
        assert_eq!(code(MetaError::ParentNotFound(1)), MetaErrorCode::NotFound);
    }

    #[test]
    fn maps_namespace_errors() {
        assert_eq!(
            code(MetaError::AlreadyExists {
                parent: 1,
                name: "a".into()
            }),
            MetaErrorCode::AlreadyExists
        );
        assert_eq!(
            code(MetaError::NotDirectory(1)),
            MetaErrorCode::NotDirectory
        );
        assert_eq!(
            code(MetaError::DirectoryNotEmpty(1)),
            MetaErrorCode::DirectoryNotEmpty
        );
        assert_eq!(
            code(MetaError::InvalidFilename),
            MetaErrorCode::InvalidFilename
        );
        assert_eq!(
            code(MetaError::FilenameTooLong),
            MetaErrorCode::FilenameTooLong
        );
        assert_eq!(
            code(MetaError::InvalidPath("/bad".into())),
            MetaErrorCode::InvalidPath
        );
        assert_eq!(
            code(MetaError::TooManySymlinks),
            MetaErrorCode::InvalidInput
        );
    }

    #[test]
    fn maps_support_and_retry_errors() {
        assert_eq!(
            code(MetaError::NotSupported("x".into())),
            MetaErrorCode::NotSupported
        );
        assert_eq!(
            code(MetaError::NotImplemented),
            MetaErrorCode::NotImplemented
        );
        assert_eq!(
            code(MetaError::ContinueRetry(RetryReason::LockContention)),
            MetaErrorCode::Conflict
        );
        assert_eq!(code(MetaError::MaxRetriesExceeded), MetaErrorCode::Conflict);
    }

    #[test]
    fn maps_lock_errors() {
        let range = FileLockRange { start: 0, end: 10 };
        assert_eq!(
            code(MetaError::LockConflict {
                inode: 1,
                owner: 2,
                range
            }),
            MetaErrorCode::LockConflict
        );
        assert_eq!(
            code(MetaError::LockNotFound {
                inode: 1,
                owner: 2,
                range
            }),
            MetaErrorCode::LockNotFound
        );
        assert_eq!(
            code(MetaError::DeadlockDetected { owners: vec![1] }),
            MetaErrorCode::Deadlock
        );
        assert_eq!(
            code(MetaError::InvalidHandle(7)),
            MetaErrorCode::InvalidHandle
        );
    }

    #[test]
    fn maps_io_error_kinds() {
        assert_eq!(
            code(MetaError::Io(io::Error::new(io::ErrorKind::TimedOut, "t"))),
            MetaErrorCode::TimedOut
        );
        assert_eq!(
            code(MetaError::Io(io::Error::new(
                io::ErrorKind::StorageFull,
                "s"
            ))),
            MetaErrorCode::StorageFull
        );
        assert_eq!(
            code(MetaError::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "p"
            ))),
            MetaErrorCode::PermissionDenied
        );
        assert_eq!(
            code(MetaError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "w"
            ))),
            MetaErrorCode::LockConflict
        );
        assert_eq!(
            code(MetaError::Io(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "c"
            ))),
            MetaErrorCode::IoError
        );
        assert_eq!(
            code(MetaError::Io(io::Error::new(io::ErrorKind::Other, "o"))),
            MetaErrorCode::IoError
        );
    }

    #[test]
    fn maps_internal_errors() {
        assert_eq!(
            code(MetaError::Internal("boom".into())),
            MetaErrorCode::Internal
        );
        assert_eq!(
            code(MetaError::Database(sea_orm::DbErr::Custom("db".into()))),
            MetaErrorCode::Internal
        );
        assert_eq!(
            code(MetaError::Serialization("ser".into())),
            MetaErrorCode::Internal
        );
        assert_eq!(
            code(MetaError::Config("cfg".into())),
            MetaErrorCode::Internal
        );
        assert_eq!(
            code(MetaError::Anyhow(anyhow::anyhow!("any"))),
            MetaErrorCode::Internal
        );
    }

    #[test]
    fn every_wire_code_roundtrips() {
        let codes = [
            MetaErrorCode::NotFound,
            MetaErrorCode::AlreadyExists,
            MetaErrorCode::NotDirectory,
            MetaErrorCode::DirectoryNotEmpty,
            MetaErrorCode::InvalidFilename,
            MetaErrorCode::FilenameTooLong,
            MetaErrorCode::InvalidPath,
            MetaErrorCode::NotSupported,
            MetaErrorCode::NotImplemented,
            MetaErrorCode::LockConflict,
            MetaErrorCode::LockNotFound,
            MetaErrorCode::Deadlock,
            MetaErrorCode::InvalidHandle,
            MetaErrorCode::PermissionDenied,
            MetaErrorCode::ReadOnly,
            MetaErrorCode::IoError,
            MetaErrorCode::Conflict,
            MetaErrorCode::Internal,
            MetaErrorCode::QuotaExceeded,
            MetaErrorCode::StorageFull,
            MetaErrorCode::CrossDevice,
            MetaErrorCode::TooManyLinks,
            MetaErrorCode::InvalidInput,
            MetaErrorCode::ResourceBusy,
            MetaErrorCode::TimedOut,
        ];
        for c in codes {
            let n: i32 = c.into();
            let back = MetaErrorCode::try_from(n).expect("code must roundtrip");
            assert_eq!(c, back);
            assert!(!format!("{back:?}").is_empty());
        }
    }
}
