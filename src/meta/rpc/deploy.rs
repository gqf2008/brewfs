//! Standalone metadata service deployment (`brewfs meta serve`).
//!
//! Independent-process form of the metadata service (#21). A leader lease is
//! taken from the backend global lock; gRPC health reports SERVING only while
//! the lease is held, so a load balancer can route clients to the current
//! leader and drop it when the lease expires.
//!
//! v1 scope: single active leader, no client-side failover (clients connect
//! to a fixed address or an LB that follows health). TLS is not implemented
//! yet; bearer-token auth is available via `--token`.

use crate::meta::rpc::server::MetaServiceImpl;
use crate::meta::store::{LockName, MetaStore};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Status};
use tonic_health::ServingStatus;

/// Runtime options for `brewfs meta serve` (constructed from CLI args in main).
#[derive(Debug, Clone)]
pub struct MetaServeOptions {
    pub meta_url: String,
    pub listen: String,
    pub token: Option<String>,
    pub leader_ttl_secs: u64,
}

/// Start the standalone metadata service and block forever.
pub async fn meta_serve_cmd(options: MetaServeOptions) -> anyhow::Result<()> {
    let addr: SocketAddr = options
        .listen
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --listen address {}: {e}", options.listen))?;
    let meta_handle = crate::meta::factory::create_meta_store_from_url(&options.meta_url).await?;
    let store = meta_handle.store();
    store.initialize().await?;
    tracing::info!(backend = store.name(), listen = %addr, "metadata service starting");

    // Leader lease: refresh periodically; health flips with it.
    let leader = Arc::new(AtomicBool::new(false));
    let ttl = options.leader_ttl_secs.max(1);
    {
        let store = Arc::clone(&store);
        let leader = Arc::clone(&leader);
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_secs(ttl / 3).max(Duration::from_secs(1)));
            loop {
                ticker.tick().await;
                let held = store
                    .get_global_lock(LockName::MetaServiceLeader, ttl)
                    .await;
                leader.store(held, Ordering::Relaxed);
                tracing::debug!(leader = held, "metadata service leader lease refreshed");
            }
        });
    }

    let svc = MetaServiceImpl::new(store);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("brewfs.meta.v1.MetaService", ServingStatus::Serving)
        .await;

    // Leader status drives overall health for load-balancer failover.
    {
        let health_reporter = health_reporter.clone();
        let leader = Arc::clone(&leader);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                let status = if leader.load(Ordering::Relaxed) {
                    ServingStatus::Serving
                } else {
                    ServingStatus::NotServing
                };
                health_reporter.set_service_status("", status).await;
            }
        });
    }

    let health = health_service;
    match options.token.as_deref() {
        Some(token) => {
            // Leak the token for a 'static interceptor (server lifetime).
            let token: &'static str = Box::leak(token.to_string().into_boxed_str());
            Server::builder()
                .add_service(
                    brewfs_meta_proto::v1::meta_service_server::MetaServiceServer::with_interceptor(
                        svc.clone(),
                        auth_interceptor(token),
                    ),
                )
                .add_service(
                    brewfs_meta_proto::v1::meta_watch_server::MetaWatchServer::with_interceptor(
                        svc,
                        auth_interceptor(token),
                    ),
                )
                .add_service(health)
                .serve(addr)
                .await?;
        }
        None => {
            Server::builder()
                .add_service(
                    brewfs_meta_proto::v1::meta_service_server::MetaServiceServer::new(svc.clone()),
                )
                .add_service(brewfs_meta_proto::v1::meta_watch_server::MetaWatchServer::new(svc))
                .add_service(health)
                .serve(addr)
                .await?;
        }
    }
    Ok(())
}

fn auth_interceptor(token: &'static str) -> impl tonic::service::Interceptor + Clone {
    move |mut req: Request<()>| {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if auth == format!("Bearer {token}") {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}

/// Attach a bearer token to every request (client side).
pub fn with_token<T>(mut req: Request<T>, token: &str) -> Result<Request<T>, Status> {
    let value: MetadataValue<_> = format!("Bearer {token}")
        .parse()
        .map_err(|_| Status::internal("invalid bearer token configured on client"))?;
    req.metadata_mut().insert("authorization", value);
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::rpc::client::RpcMetaStore;
    use crate::meta::store::MetaError;
    use tokio_stream::wrappers::TcpListenerStream;

    async fn spawn_test_server(token: Option<String>) -> String {
        let meta_handle = crate::meta::factory::create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap();
        let store = meta_handle.store();
        store.initialize().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let svc = MetaServiceImpl::new(store);
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_service_status("brewfs.meta.v1.MetaService", ServingStatus::Serving)
            .await;
        tokio::spawn(async move {
            let mut server = Server::builder();
            match token {
                Some(token) => {
                    let token: &'static str = Box::leak(token.into_boxed_str());
                    server
                        .add_service(
                            brewfs_meta_proto::v1::meta_service_server::MetaServiceServer::with_interceptor(
                                svc.clone(),
                                auth_interceptor(token),
                            ),
                        )
                        .add_service(
                            brewfs_meta_proto::v1::meta_watch_server::MetaWatchServer::with_interceptor(
                                svc,
                                auth_interceptor(token),
                            ),
                        )
                        .add_service(health_service)
                        .serve_with_incoming(TcpListenerStream::new(listener))
                        .await
                        .unwrap();
                }
                None => {
                    server
                        .add_service(
                            brewfs_meta_proto::v1::meta_service_server::MetaServiceServer::new(svc),
                        )
                        .add_service(health_service)
                        .serve_with_incoming(TcpListenerStream::new(listener))
                        .await
                        .unwrap();
                }
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn meta_serve_accepts_requests_and_reports_health() {
        let endpoint = spawn_test_server(None).await;

        let rpc = RpcMetaStore::connect(endpoint.clone()).await.unwrap();
        let dir_ino = rpc.mkdir(1, "d".to_string()).await.unwrap();
        assert!(rpc.lookup(1, "d").await.unwrap().is_some());
        assert_eq!(dir_ino, 2);

        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
        let resp = health
            .check(tonic_health::pb::HealthCheckRequest {
                service: "brewfs.meta.v1.MetaService".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
    }

    #[tokio::test]
    async fn meta_serve_token_auth_enforced() {
        let endpoint = spawn_test_server(Some("secret".to_string())).await;

        // Without token: unauthenticated (mapped to a generic RPC error).
        let rpc = RpcMetaStore::connect(endpoint.clone()).await.unwrap();
        let err = rpc.mkdir(1, "d".to_string()).await.unwrap_err();
        assert!(matches!(err, MetaError::Anyhow(_)));

        // With token: works.
        let mut client =
            brewfs_meta_proto::v1::meta_service_client::MetaServiceClient::connect(endpoint)
                .await
                .unwrap();
        let mut req = Request::new(brewfs_meta_proto::v1::MkdirRequest {
            parent: 1,
            name: "d".into(),
        });
        req.metadata_mut()
            .insert("authorization", MetadataValue::from_static("Bearer secret"));
        client.mkdir(req).await.unwrap();
    }
}
