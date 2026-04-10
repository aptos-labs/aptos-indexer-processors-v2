// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use aptos_indexer_transaction_stream::{
    config::{BackupCatchupConfig, ReconnectionConfig, TransactionStreamConfig},
    transaction_stream::TransactionStream,
};
use aptos_protos::indexer::v1::{
    GetTransactionsRequest, TransactionsResponse,
    raw_data_server::{RawData, RawDataServer},
};
use futures::{Future, Stream};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use url::Url;

/// A stream that stalls for a specified duration before yielding a response
struct StallingResponseStream {
    stall_duration: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    done: bool,
}

impl StallingResponseStream {
    fn new(stall_duration: Duration) -> Self {
        Self {
            stall_duration,
            sleep: None,
            done: false,
        }
    }
}

impl Stream for StallingResponseStream {
    type Item = Result<TransactionsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        // Initialize sleep if not already done
        if self.sleep.is_none() {
            self.sleep = Some(Box::pin(tokio::time::sleep(self.stall_duration)));
        }

        // Poll the sleep future
        if let Some(sleep) = self.sleep.as_mut() {
            match sleep.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    self.done = true;
                    return Poll::Ready(Some(Ok(TransactionsResponse {
                        transactions: vec![],
                        chain_id: Some(1),
                        processed_range: Some(aptos_protos::indexer::v1::ProcessedRange {
                            first_version: 0,
                            last_version: 0,
                        }),
                    })));
                },
            }
        }

        Poll::Pending
    }
}

/// A mock gRPC server that stalls for a specified duration before returning a response.
/// After max_connections is reached, it rejects new connections with an error.
pub struct StallingMockGrpcServer {
    stall_duration: Duration,
    connection_count: Arc<AtomicU64>,
    max_connections: u64,
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

#[tonic::async_trait]
impl RawData for StallingMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        let count = self.connection_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Reject connections after max_connections
        if count > self.max_connections {
            return Err(Status::unavailable(
                "Server is no longer accepting connections",
            ));
        }

        let stream = StallingResponseStream::new(self.stall_duration);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl StallingMockGrpcServer {
    pub fn new(
        stall_duration: Duration,
        connection_count: Arc<AtomicU64>,
        max_connections: u64,
    ) -> Self {
        Self {
            stall_duration,
            connection_count,
            max_connections,
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

#[tokio::test]
async fn test_transaction_stream_reconnects_on_timeout() {
    let connection_count = Arc::new(AtomicU64::new(0));
    let stall_duration = Duration::from_secs(6);
    let timeout_duration_secs = 5;

    // Server accepts 2 connections:
    // 1. Initial connection from TransactionStream::new
    // 2. First reconnection attempt after timeout
    // After that, reconnection fails and the test completes
    let max_connections = 2;

    // Start the stalling mock server
    let server =
        StallingMockGrpcServer::new(stall_duration, connection_count.clone(), max_connections);
    let port = server.run().await.expect("Failed to start mock server");

    // Create config with 5 second timeout and minimal retries for faster test
    let config = TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse(&format!("http://127.0.0.1:{}", port))
            .unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("test_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_response_item_timeout_secs: timeout_duration_secs,
        reconnection_config: ReconnectionConfig {
            timeout_secs: 5,
            max_retries: 2,
            initial_delay_ms: 50,
            max_delay_ms: 10_000,
            enable_jitter: false,
        },
        transaction_filter: None,
        backup_endpoints: vec![],
        backup_catchup_config: BackupCatchupConfig::default(),
    };

    // Initialize the transaction stream (uses connection 1)
    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Failed to create transaction stream");

    assert_eq!(
        connection_count.load(Ordering::SeqCst),
        1,
        "Should have 1 initial connection"
    );

    let start = std::time::Instant::now();

    // Try to get next batch - this should:
    // 1. Timeout after 5 seconds (stream stalls for 6s)
    // 2. Reconnect (uses connection 2, also stalls)
    // 3. Timeout again after 5 seconds
    // 4. Try to reconnect but fail (max connections reached)
    // 5. Return error
    let result = transaction_stream.get_next_transaction_batch().await;
    let elapsed = start.elapsed();

    // Should have failed after timeouts and failed reconnection
    assert!(result.is_err(), "Expected error after failed reconnection");

    // Verify we waited at least one timeout period
    assert!(
        elapsed >= Duration::from_secs(timeout_duration_secs),
        "Should have waited at least {} seconds. Elapsed: {:?}",
        timeout_duration_secs,
        elapsed
    );

    // Verify reconnection was attempted (connection count > 1)
    let total_connections = connection_count.load(Ordering::SeqCst);
    assert!(
        total_connections > 1,
        "Should have attempted reconnection. Total connections: {}",
        total_connections
    );

    println!(
        "Test completed in {:?} with {} total connections",
        elapsed, total_connections
    );
}
