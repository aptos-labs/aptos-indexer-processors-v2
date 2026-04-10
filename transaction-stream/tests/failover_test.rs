// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_indexer_transaction_stream::{
    config::{BackupCatchupConfig, Endpoint, ReconnectionConfig, TransactionStreamConfig},
    transaction_stream::TransactionStream,
};
use aptos_protos::indexer::v1::{
    GetTransactionsRequest, TransactionsResponse,
    raw_data_server::{RawData, RawDataServer},
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use url::Url;

struct WorkingResponseStream {
    done: bool,
    start_version: u64,
    end_version: u64,
}

impl WorkingResponseStream {
    fn new(start_version: u64, end_version: u64) -> Self {
        Self {
            done: false,
            start_version,
            end_version,
        }
    }
}

impl Stream for WorkingResponseStream {
    type Item = Result<TransactionsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        self.done = true;
        Poll::Ready(Some(Ok(TransactionsResponse {
            transactions: vec![],
            chain_id: Some(1),
            processed_range: Some(aptos_protos::indexer::v1::ProcessedRange {
                first_version: self.start_version,
                last_version: self.end_version,
            }),
        })))
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

pub struct FailingMockGrpcServer {
    connection_count: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl RawData for FailingMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        Err(Status::unavailable("Server is unavailable"))
    }
}

impl FailingMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>) -> Self {
        Self { connection_count }
    }

    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;
        let stream = TcpListenerStream::new(listener);
        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );
        tokio::spawn(async move {
            let _ = server.serve_with_incoming(stream).await;
        });
        Ok(bound_addr.port())
    }
}

pub struct WorkingMockGrpcServer {
    connection_count: Arc<AtomicU64>,
    start_version: u64,
}

#[tonic::async_trait]
impl RawData for WorkingMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        let stream = WorkingResponseStream::new(self.start_version, self.start_version + 99);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl WorkingMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>, start_version: u64) -> Self {
        Self {
            connection_count,
            start_version,
        }
    }

    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;
        let stream = TcpListenerStream::new(listener);
        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );
        tokio::spawn(async move {
            let _ = server.serve_with_incoming(stream).await;
        });
        Ok(bound_addr.port())
    }
}

fn create_base_config(primary_port: u16) -> TransactionStreamConfig {
    TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse(&format!(
            "http://127.0.0.1:{}",
            primary_port
        ))
        .unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("primary_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_response_item_timeout_secs: 2,
        reconnection_config: ReconnectionConfig {
            timeout_secs: 1,
            max_retries: 2,
            initial_delay_ms: 50,
            max_delay_ms: 10_000,
            enable_jitter: false,
        },
        transaction_filter: None,
        backup_endpoints: vec![],
        backup_catchup_config: BackupCatchupConfig {
            max_wait_secs: 0,
            poll_interval_secs: 1,
        },
    }
}

#[tokio::test]
async fn test_failover_to_backup_endpoint() {
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup after primary fails");

    assert!(primary_connection_count.load(Ordering::SeqCst) > 0);
    assert!(backup_connection_count.load(Ordering::SeqCst) > 0);

    let result = transaction_stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed from backup");
}

#[tokio::test]
async fn test_all_endpoints_exhausted() {
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = FailingMockGrpcServer::new(backup_connection_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let result = TransactionStream::new(config).await;
    assert!(result.is_err(), "Should fail when all endpoints exhausted");
    assert!(primary_connection_count.load(Ordering::SeqCst) > 0);
    assert!(backup_connection_count.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn test_config_endpoint_helpers() {
    let config = TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse("http://primary.example.com").unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("primary_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_response_item_timeout_secs: 60,
        reconnection_config: ReconnectionConfig::default(),
        transaction_filter: None,
        backup_catchup_config: BackupCatchupConfig::default(),
        backup_endpoints: vec![
            Endpoint {
                address: Url::parse("http://backup1.example.com").unwrap(),
                auth_token: None,
                is_primary: false,
            },
            Endpoint {
                address: Url::parse("http://backup2.example.com").unwrap(),
                auth_token: Some("backup2_token".to_string()),
                is_primary: false,
            },
        ],
    };

    let endpoints = config.get_endpoints();
    assert_eq!(endpoints.len(), 3);
    assert_eq!(endpoints[0].address.as_str(), "http://primary.example.com/");
    assert_eq!(endpoints[0].auth_token.as_deref(), Some("primary_token"));
    assert_eq!(endpoints[1].address.as_str(), "http://backup1.example.com/");
    assert_eq!(endpoints[1].auth_token, None);
    assert_eq!(endpoints[2].address.as_str(), "http://backup2.example.com/");
    assert_eq!(endpoints[2].auth_token.as_deref(), Some("backup2_token"));
}

#[tokio::test]
async fn test_reconnect_tries_primary_first() {
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup");

    assert!(primary_connection_count.load(Ordering::SeqCst) > 0);

    primary_connection_count.store(0, Ordering::SeqCst);
    let result = transaction_stream.reconnect_to_grpc_with_retries().await;
    assert!(result.is_ok(), "Should reconnect via backup");
    assert!(
        primary_connection_count.load(Ordering::SeqCst) > 0,
        "Primary should have been retried during reconnection"
    );
}

#[tokio::test]
async fn test_backward_compatibility_no_backup_endpoints() {
    let working_connection_count = Arc::new(AtomicU64::new(0));
    let working_server = WorkingMockGrpcServer::new(working_connection_count.clone(), 0);
    let working_port = working_server
        .run()
        .await
        .expect("Failed to start working server");

    let config = create_base_config(working_port);
    assert!(config.backup_endpoints.is_empty());
    assert_eq!(config.total_endpoints(), 1);

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Failed to create transaction stream");

    let result = transaction_stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Should work with no backup endpoints");
}

#[tokio::test]
async fn test_exponential_backoff_reconnection() {
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_base_config(primary_port);
    config.reconnection_config.initial_delay_ms = 100;
    config.reconnection_config.enable_jitter = false;
    config.reconnection_config.max_retries = 1;
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup");

    let _ = transaction_stream.get_next_transaction_batch().await;

    primary_connection_count.store(0, Ordering::SeqCst);
    backup_connection_count.store(0, Ordering::SeqCst);

    let start = std::time::Instant::now();
    let result = transaction_stream.reconnect_to_grpc_with_retries().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Should reconnect via backup");
    assert!(
        elapsed >= std::time::Duration::from_millis(250),
        "Expected at least ~300ms from exponential backoff (100+200), but got {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_backoff_default_values() {
    let defaults = ReconnectionConfig::default();
    assert_eq!(defaults.timeout_secs, 5);
    assert_eq!(defaults.max_retries, 5);
    assert_eq!(defaults.initial_delay_ms, 50);
    assert_eq!(defaults.max_delay_ms, 10_000);
    assert!(defaults.enable_jitter);
}
