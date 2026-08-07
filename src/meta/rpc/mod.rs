//! Metadata service RPC contract and error mapping.
//!
//! The gRPC contract types live in `brewfs-meta-proto` (generated from
//! `proto/brewfs/meta/v1/meta_service.proto`). This module keeps the
//! mapping between the internal `MetaError` domain and the wire error
//! codes so server and client implementations share one translation.
pub mod client;
pub mod deploy;
pub mod error;
pub mod server;
