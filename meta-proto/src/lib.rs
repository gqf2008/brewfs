//! BrewFS metadata service gRPC contract.
//!
//! Generated from `proto/brewfs/meta/v1/meta_service.proto` (design:
//! `doc/architecture/meta-service.md`). Server and client implementations in
//! the brewfs crate reuse these types via `brewfs_meta_proto::v1`.
pub mod v1 {
    tonic::include_proto!("brewfs.meta.v1");
}
