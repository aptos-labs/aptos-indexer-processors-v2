// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    config::{Endpoint, TransactionStreamConfig, wait_for_next_retry},
    utils::{additional_headers::AdditionalHeaders, time::timestamp_to_iso},
};
use anyhow::{Result, anyhow};
use aptos_moving_average::MovingAverage;
use aptos_protos::{
    indexer::v1::{GetTransactionsRequest, TransactionsResponse, raw_data_client::RawDataClient},
    transaction::v1::Transaction,
    util::timestamp::Timestamp,
};
use aptos_transaction_filter::BooleanTransactionFilter;
use futures_util::StreamExt;
use prost::Message;
use sample::{SampleRate, sample};
use std::time::Duration;
use tokio::time::timeout;
use tonic::{
    Response, Streaming,
    transport::{Channel, ClientTlsConfig},
};
use tracing::{error, info, warn};

/// GRPC request metadata key for the token ID.
const GRPC_API_GATEWAY_API_KEY_HEADER: &str = "authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
/// GRPC connection id
const GRPC_CONNECTION_ID: &str = "x-aptos-connection-id";
/// 256MB
pub const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 256;

/// TransactionsPBResponse is a struct that holds the transactions fetched from the stream.
/// It also includes some contextual information about the transactions.
#[derive(Clone)]
pub struct TransactionsPBResponse {
    pub transactions: Vec<Transaction>,
    pub chain_id: u64,
    // We put start/end versions here as filtering means there are potential "gaps" here now
    pub start_version: u64,
    pub end_version: u64,
    pub start_txn_timestamp: Option<Timestamp>,
    pub end_txn_timestamp: Option<Timestamp>,
    pub size_in_bytes: u64,
}

/// Helper function to build a GRPC request for fetching transactions.
pub fn grpc_request_builder(
    starting_version: Option<u64>,
    transactions_count: Option<u64>,
    grpc_auth_token: Option<String>,
    request_name_header: String,
    additional_headers: AdditionalHeaders,
    transaction_filter: Option<BooleanTransactionFilter>,
) -> tonic::Request<GetTransactionsRequest> {
    let mut request = tonic::Request::new(GetTransactionsRequest {
        starting_version,
        transactions_count,
        transaction_filter: transaction_filter.map(Into::into),
        ..GetTransactionsRequest::default()
    });
    if let Some(auth_token) = grpc_auth_token {
        request.metadata_mut().insert(
            GRPC_API_GATEWAY_API_KEY_HEADER,
            format!("Bearer {}", auth_token).parse().unwrap(),
        );
    }
    request.metadata_mut().insert(
        GRPC_REQUEST_NAME_HEADER,
        request_name_header.parse().unwrap(),
    );
    additional_headers.drain_into_metadata_map(request.metadata_mut());
    request
}

/// Given a `TransactionStreamConfig`, this function will return a stream of transactions
/// using the primary endpoint. It also handles timeouts and retries.
pub async fn get_stream(
    transaction_stream_config: TransactionStreamConfig,
) -> Result<Response<Streaming<TransactionsResponse>>> {
    let endpoints = transaction_stream_config.get_endpoints();
    get_stream_for_endpoint(transaction_stream_config, &endpoints[0]).await
}

/// Given a `TransactionStreamConfig` and endpoint, returns a stream of transactions.
pub async fn get_stream_for_endpoint(
    transaction_stream_config: TransactionStreamConfig,
    endpoint: &Endpoint,
) -> Result<Response<Streaming<TransactionsResponse>>> {
    let endpoint_address_str = endpoint.address.to_string();
    let reconnection = &transaction_stream_config.reconnection_config;

    info!(
        stream_address = endpoint_address_str,
        is_primary = endpoint.is_primary,
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        "[Transaction Stream] Setting up rpc channel"
    );

    let channel = Channel::from_shared(endpoint_address_str.clone())
        .expect(
            "[Transaction Stream] Failed to build GRPC channel, perhaps because the data service URL is invalid",
        )
        .http2_keep_alive_interval(transaction_stream_config.indexer_grpc_http2_ping_interval())
        .keep_alive_timeout(transaction_stream_config.indexer_grpc_http2_ping_timeout());

    // If the scheme is https, add a TLS config.
    let channel = if endpoint.address.scheme() == "https" {
        let config = ClientTlsConfig::new();
        channel
            .tls_config(config)
            .expect("[Transaction Stream] Failed to create TLS config")
    } else {
        channel
    };

    info!(
        stream_address = endpoint_address_str,
        is_primary = endpoint.is_primary,
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        "[Transaction Stream] Setting up GRPC client"
    );

    // Retry connecting to the GRPC client with exponential backoff. The backoff iterator
    // is bounded by max_retries so the iterator controls the retry budget.
    let mut retry_count: u64 = 0;
    let mut backoff = reconnection
        .backoff_iter()
        .take(reconnection.max_retries as usize);
    let res = loop {
        match timeout(
            reconnection.timeout(),
            RawDataClient::connect(channel.clone()),
        )
        .await
        {
            Ok(Ok(client)) => break Ok(client),
            Ok(Err(e)) => {
                warn!(
                    stream_address = endpoint_address_str,
                    is_primary = endpoint.is_primary,
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retry_count = retry_count + 1,
                    error = ?e,
                    "[Transaction Stream] Error connecting to GRPC client"
                );
                if wait_for_next_retry(&mut backoff, &mut retry_count)
                    .await
                    .is_none()
                {
                    break Err(anyhow!("Error connecting to GRPC client").context(e));
                }
            },
            Err(e) => {
                warn!(
                    stream_address = endpoint_address_str,
                    is_primary = endpoint.is_primary,
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retry_count = retry_count + 1,
                    error = ?e,
                    "[Transaction Stream] Timed out connecting to GRPC client"
                );
                if wait_for_next_retry(&mut backoff, &mut retry_count)
                    .await
                    .is_none()
                {
                    break Err(anyhow!("Timed out connecting to GRPC client"));
                }
            },
        }
    };

    let raw_data_client = res?;

    let mut rpc_client = raw_data_client
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_RESPONSE_SIZE)
        .max_encoding_message_size(MAX_RESPONSE_SIZE);

    let count = transaction_stream_config.request_ending_version.map(|v| {
        (v as i64
            - transaction_stream_config
                .starting_version
                .unwrap_or_else(|| {
                    panic!("starting_version is required when using request_ending_version")
                }) as i64
            + 1) as u64
    });

    info!(
        stream_address = endpoint_address_str,
        is_primary = endpoint.is_primary,
        start_version = transaction_stream_config.starting_version,
        end_version = transaction_stream_config.request_ending_version,
        num_of_transactions = ?count,
        "[Transaction Stream] Setting up GRPC stream",
    );

    // Retry the gRPC GetTransactions request with exponential backoff.
    let mut retry_count: u64 = 0;
    let mut backoff = reconnection
        .backoff_iter()
        .take(reconnection.max_retries as usize);
    loop {
        let timeout_res = timeout(reconnection.timeout(), async {
            let request = grpc_request_builder(
                transaction_stream_config.starting_version,
                count,
                endpoint.auth_token.clone(),
                transaction_stream_config.request_name_header.clone(),
                transaction_stream_config.additional_headers.clone(),
                transaction_stream_config.transaction_filter.clone(),
            );
            rpc_client.get_transactions(request).await
        })
        .await;
        match timeout_res {
            Ok(Ok(response)) => break Ok(response),
            Ok(Err(e)) => {
                warn!(
                    stream_address = endpoint_address_str,
                    is_primary = endpoint.is_primary,
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retry_count = retry_count + 1,
                    error = ?e,
                    "[Transaction Stream] Error making grpc request. Retrying..."
                );
                if wait_for_next_retry(&mut backoff, &mut retry_count)
                    .await
                    .is_none()
                {
                    break Err(anyhow!("Error making grpc request").context(e));
                }
            },
            Err(e) => {
                warn!(
                    stream_address = endpoint_address_str,
                    is_primary = endpoint.is_primary,
                    start_version = transaction_stream_config.starting_version,
                    end_version = transaction_stream_config.request_ending_version,
                    retry_count = retry_count + 1,
                    error = ?e,
                    "[Transaction Stream] Timeout making grpc request. Retrying...",
                );
                if wait_for_next_retry(&mut backoff, &mut retry_count)
                    .await
                    .is_none()
                {
                    break Err(anyhow!("Timeout making grpc request").context(e));
                }
            },
        }
    }
}

/// Helper function to get the chain id from the stream.
pub async fn get_chain_id(transaction_stream_config: TransactionStreamConfig) -> Result<u64> {
    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        "[Transaction Stream] Connecting to GRPC stream to get chain id",
    );

    // Minimal channel setup for a single query
    let mut channel = Channel::from_shared(
        transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
    )
    .expect(
        "[Transaction Stream] Failed to build GRPC channel, perhaps because the data service URL is invalid",
    );

    // Add TLS if needed
    if transaction_stream_config
        .indexer_grpc_data_service_address
        .scheme()
        == "https"
    {
        channel = channel
            .tls_config(ClientTlsConfig::new())
            .expect("[Transaction Stream] Failed to create TLS config");
    }

    // Make a point query for the latest transaction
    let request = grpc_request_builder(
        None,
        Some(1),
        transaction_stream_config.auth_token.clone(),
        transaction_stream_config.request_name_header.clone(),
        transaction_stream_config.additional_headers.clone(),
        transaction_stream_config.transaction_filter.clone(),
    );

    let response = RawDataClient::connect(channel)
        .await?
        .get_transactions(request)
        .await?;
    let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        stream_address = transaction_stream_config
            .indexer_grpc_data_service_address
            .to_string(),
        connection_id = connection_id,
        "[Transaction Stream] Successfully connected to GRPC stream to get chain id",
    );

    match tokio::time::timeout(
        transaction_stream_config.indexer_grpc_response_item_timeout(),
        resp_stream.next(),
    )
    .await
    {
        // Received datastream response
        Ok(response) => match response {
            Some(Ok(r)) => match r.chain_id {
                Some(chain_id) => Ok(chain_id),
                None => {
                    error!(
                        stream_address = transaction_stream_config
                            .indexer_grpc_data_service_address
                            .to_string(),
                        connection_id = connection_id,
                        "[Transaction Stream] Chain Id doesn't exist."
                    );
                    Err(anyhow!("Chain Id doesn't exist"))
                },
            },
            Some(Err(rpc_error)) => {
                error!(
                    stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                    connection_id = connection_id,
                    error = ?rpc_error,
                    "[Transaction Stream] Error receiving datastream response for chain id"
                );
                Err(anyhow!("Error receiving datastream response for chain id").context(rpc_error))
            },
            None => {
                error!(
                    stream_address = transaction_stream_config
                        .indexer_grpc_data_service_address
                        .to_string(),
                    connection_id = connection_id,
                    "[Transaction Stream] Stream ended before getting response for chain id"
                );
                Err(anyhow!("Stream ended before getting response for chain id"))
            },
        },
        // Timeout receiving datastream response
        Err(e) => {
            warn!(
                stream_address = transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                connection_id = connection_id,
                start_version = transaction_stream_config.starting_version,
                end_version = transaction_stream_config.request_ending_version,
                error = ?e,
                "[Transaction Stream] Timeout receiving datastream response for chain id."
            );
            Err(anyhow!(
                "Timeout receiving datastream response for chain id"
            ))
        },
    }
}

/// Probes an endpoint to discover its latest available transaction version.
///
/// Sends a request for 1 transaction with no starting_version, which causes
/// the server to return its latest available transaction. Returns the version
/// of that transaction, or an error if the probe fails.
async fn probe_latest_version(
    transaction_stream_config: &TransactionStreamConfig,
    endpoint: &Endpoint,
) -> Result<u64> {
    let endpoint_address_str = endpoint.address.to_string();

    let channel = Channel::from_shared(endpoint_address_str.clone())
        .expect("[Transaction Stream] Failed to build GRPC channel for probe");

    let channel = if endpoint.address.scheme() == "https" {
        channel
            .tls_config(ClientTlsConfig::new())
            .expect("[Transaction Stream] Failed to create TLS config for probe")
    } else {
        channel
    };

    let request = grpc_request_builder(
        None,
        Some(1),
        endpoint.auth_token.clone(),
        transaction_stream_config.request_name_header.clone(),
        transaction_stream_config.additional_headers.clone(),
        transaction_stream_config.transaction_filter.clone(),
    );

    let response = timeout(
        transaction_stream_config.reconnection_config.timeout(),
        async {
            let client = RawDataClient::connect(channel)
                .await
                .map_err(|e| anyhow!("Failed to connect for probe: {}", e))?;
            let mut rpc_client = client
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                .max_decoding_message_size(MAX_RESPONSE_SIZE)
                .max_encoding_message_size(MAX_RESPONSE_SIZE);
            rpc_client
                .get_transactions(request)
                .await
                .map_err(|e| anyhow!("Failed to get transactions for probe: {}", e))
        },
    )
    .await
    .map_err(|_| anyhow!("Timeout probing endpoint for latest version"))??;

    let mut resp_stream = response.into_inner();

    match timeout(
        transaction_stream_config.indexer_grpc_response_item_timeout(),
        resp_stream.next(),
    )
    .await
    {
        Ok(Some(Ok(r))) => {
            let version = match r.processed_range {
                Some(range) => range.last_version,
                None => r
                    .transactions
                    .last()
                    .map(|t| t.version)
                    .ok_or_else(|| anyhow!("Probe response had no transactions"))?,
            };
            Ok(version)
        },
        Ok(Some(Err(e))) => Err(anyhow!("Error in probe response: {}", e)),
        Ok(None) => Err(anyhow!("Probe stream ended without response")),
        Err(_) => Err(anyhow!("Timeout waiting for probe response")),
    }
}

/// Waits for a non-primary endpoint to catch up to the needed version.
///
/// Polls the endpoint's latest version at the configured interval. Returns `Ok(())`
/// when the endpoint's latest version >= `needed_version`, or `Err` if the max wait
/// time is exceeded or probing consistently fails.
async fn wait_for_endpoint_catchup(
    transaction_stream_config: &TransactionStreamConfig,
    endpoint: &Endpoint,
    needed_version: u64,
) -> Result<()> {
    let catchup = &transaction_stream_config.backup_catchup_config;
    if catchup.max_wait_secs == 0 {
        return Ok(());
    }
    let endpoint_address_str = endpoint.address.to_string();
    let deadline = tokio::time::Instant::now() + catchup.max_wait();
    let mut consecutive_errors: u64 = 0;
    const MAX_CONSECUTIVE_ERRORS: u64 = 10;

    info!(
        stream_address = endpoint_address_str,
        needed_version = needed_version,
        max_wait_secs = catchup.max_wait_secs,
        poll_interval_secs = catchup.poll_interval_secs,
        "[Transaction Stream] Waiting for backup endpoint to catch up"
    );

    loop {
        if tokio::time::Instant::now() >= deadline {
            warn!(
                stream_address = endpoint_address_str,
                needed_version = needed_version,
                max_wait_secs = catchup.max_wait_secs,
                "[Transaction Stream] Backup catchup timeout exceeded"
            );
            return Err(anyhow!(
                "Backup endpoint did not catch up to version {} within {}s",
                needed_version,
                catchup.max_wait_secs
            ));
        }

        match probe_latest_version(transaction_stream_config, endpoint).await {
            Ok(latest_version) => {
                consecutive_errors = 0;

                if latest_version >= needed_version {
                    info!(
                        stream_address = endpoint_address_str,
                        latest_version = latest_version,
                        needed_version = needed_version,
                        "[Transaction Stream] Backup endpoint has caught up"
                    );
                    return Ok(());
                }

                let gap = needed_version as i64 - latest_version as i64;
                info!(
                    stream_address = endpoint_address_str,
                    latest_version = latest_version,
                    needed_version = needed_version,
                    gap = gap,
                    "[Transaction Stream] Backup endpoint still behind, waiting..."
                );
            },
            Err(e) => {
                consecutive_errors += 1;
                warn!(
                    stream_address = endpoint_address_str,
                    error = ?e,
                    consecutive_errors = consecutive_errors,
                    "[Transaction Stream] Error probing backup endpoint version"
                );
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return Err(anyhow!(
                        "Too many consecutive probe errors ({}) for backup endpoint",
                        consecutive_errors
                    ));
                }
            },
        }

        tokio::time::sleep(catchup.poll_interval()).await;
    }
}

/// TransactionStream is a struct that holds the state of the stream and provides methods to fetch transactions
/// from the stream.
pub struct TransactionStream {
    transaction_stream_config: TransactionStreamConfig,
    stream: Streaming<TransactionsResponse>,
    connection_id: String,
    reconnection_retries: u64,
    last_fetched_version: Option<i64>,
    fetch_ma: MovingAverage,
    current_endpoint_is_primary: bool,
}

impl TransactionStream {
    pub async fn new(transaction_stream_config: TransactionStreamConfig) -> Result<Self> {
        let endpoints = transaction_stream_config.get_endpoints();

        let mut last_error = None;
        for endpoint in endpoints.iter() {
            // For non-primary endpoints, wait for catchup before connecting.
            if !endpoint.is_primary {
                if let Some(starting_version) = transaction_stream_config.starting_version {
                    if let Err(e) = wait_for_endpoint_catchup(
                        &transaction_stream_config,
                        endpoint,
                        starting_version,
                    )
                    .await
                    {
                        warn!(
                            is_primary = endpoint.is_primary,
                            error = ?e,
                            "[Transaction Stream] Backup catchup failed, skipping endpoint"
                        );
                        last_error = Some(e);
                        continue;
                    }
                }
            }

            match Self::init_stream(&transaction_stream_config, endpoint).await {
                Ok((stream, connection_id)) => {
                    if !endpoint.is_primary {
                        info!(
                            is_primary = endpoint.is_primary,
                            "[Transaction Stream] Successfully connected using backup endpoint"
                        );
                    }
                    return Ok(Self {
                        transaction_stream_config: transaction_stream_config.clone(),
                        stream,
                        connection_id,
                        reconnection_retries: 0,
                        last_fetched_version: transaction_stream_config
                            .starting_version
                            .map(|v| v as i64 - 1),
                        fetch_ma: MovingAverage::new(3000),
                        current_endpoint_is_primary: endpoint.is_primary,
                    });
                },
                Err(e) => {
                    warn!(
                        is_primary = endpoint.is_primary,
                        error = ?e,
                        "[Transaction Stream] Failed to connect to endpoint, trying next"
                    );
                    last_error = Some(e);
                },
            }
        }

        Err(last_error.unwrap_or_else(|| unreachable!("at least one endpoint always exists")))
    }

    async fn init_stream(
        transaction_stream_config: &TransactionStreamConfig,
        endpoint: &Endpoint,
    ) -> Result<(Streaming<TransactionsResponse>, String)> {
        let endpoint_address_str = endpoint.address.to_string();

        info!(
            stream_address = endpoint_address_str,
            is_primary = endpoint.is_primary,
            start_version = transaction_stream_config.starting_version,
            end_version = transaction_stream_config.request_ending_version,
            "[Transaction Stream] Connecting to GRPC stream",
        );
        let resp_stream =
            get_stream_for_endpoint(transaction_stream_config.clone(), endpoint).await?;
        let connection_id = match resp_stream.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        info!(
            stream_address = endpoint_address_str,
            is_primary = endpoint.is_primary,
            connection_id = connection_id,
            start_version = transaction_stream_config.starting_version,
            end_version = transaction_stream_config.request_ending_version,
            "[Transaction Stream] Successfully connected to GRPC stream",
        );
        Ok((resp_stream.into_inner(), connection_id))
    }

    /// Gets a batch of transactions from the stream. Batch size is set in the grpc server.
    /// There could be several special scenarios:
    /// 1. If we lose the connection, we will try reconnecting X times before crashing.
    /// 2. If we specified an end version and we hit that, we will stop fetching.
    /// 3. If no transactions are received within the response item timeout,
    ///    we will automatically reconnect and retry.
    pub async fn get_next_transaction_batch(&mut self) -> Result<TransactionsPBResponse> {
        loop {
            let grpc_channel_recv_latency = std::time::Instant::now();

            let txn_pb_res = match tokio::time::timeout(
                self.transaction_stream_config
                    .indexer_grpc_response_item_timeout(),
                self.stream.next(),
            )
            .await
            {
                Ok(response) => match response {
                    Some(Ok(r)) => {
                        self.reconnection_retries = 0;

                        let start_version = match r.processed_range {
                            Some(range) => range.first_version,
                            None => r.transactions.as_slice().first().unwrap().version,
                        };
                        let end_version = match r.processed_range {
                            Some(range) => range.last_version,
                            None => r.transactions.as_slice().last().unwrap().version,
                        };

                        let start_txn_timestamp =
                            r.transactions.as_slice().first().and_then(|t| t.timestamp);
                        let end_txn_timestamp =
                            r.transactions.as_slice().last().and_then(|t| t.timestamp);

                        let size_in_bytes = r.encoded_len() as u64;
                        let chain_id: u64 = r
                            .chain_id
                            .expect("[Transaction Stream] Chain Id doesn't exist.");
                        let num_txns = r.transactions.len();
                        let duration_in_secs = grpc_channel_recv_latency.elapsed().as_secs_f64();
                        self.fetch_ma.tick_now(num_txns as u64);

                        sample!(
                            SampleRate::Duration(Duration::from_secs(1)),
                            info!(
                                stream_address = self
                                    .transaction_stream_config
                                    .indexer_grpc_data_service_address
                                    .to_string(),
                                connection_id = self.connection_id,
                                start_version = start_version,
                                end_version = end_version,
                                start_txn_timestamp_iso = start_txn_timestamp
                                    .as_ref()
                                    .map(timestamp_to_iso)
                                    .unwrap_or_default(),
                                end_txn_timestamp_iso = end_txn_timestamp
                                    .as_ref()
                                    .map(timestamp_to_iso)
                                    .unwrap_or_default(),
                                num_of_transactions = end_version - start_version + 1,
                                size_in_bytes = size_in_bytes,
                                duration_in_secs = duration_in_secs,
                                tps = self.fetch_ma.avg().ceil() as u64,
                                bytes_per_sec = size_in_bytes as f64 / duration_in_secs,
                                "[Transaction Stream] Received transactions from GRPC.",
                            )
                        );

                        if let Some(last_fetched_version) = self.last_fetched_version
                            && last_fetched_version + 1 != start_version as i64
                        {
                            error!(
                                last_fetched_version = self.last_fetched_version,
                                expected_start_version = self.last_fetched_version.map(|v| v + 1),
                                actual_start_version = start_version,
                                "[Transaction Stream] Received batch with gap from GRPC stream"
                            );
                            return Err(anyhow!("Received batch with gap from GRPC stream"));
                        }
                        self.last_fetched_version = Some(end_version as i64);

                        return Ok(TransactionsPBResponse {
                            transactions: r.transactions,
                            chain_id,
                            start_version,
                            end_version,
                            start_txn_timestamp,
                            end_txn_timestamp,
                            size_in_bytes,
                        });
                    },
                    Some(Err(rpc_error)) => {
                        warn!(
                            stream_address = self.transaction_stream_config.indexer_grpc_data_service_address.to_string(),
                            connection_id = self.connection_id,
                            start_version = self.transaction_stream_config.starting_version,
                            end_version = self.transaction_stream_config.request_ending_version,
                            error = ?rpc_error,
                            "[Transaction Stream] Error receiving datastream response."
                        );
                        Err(anyhow!("Error receiving datastream response"))
                    },
                    None => {
                        warn!(
                            stream_address = self
                                .transaction_stream_config
                                .indexer_grpc_data_service_address
                                .to_string(),
                            connection_id = self.connection_id,
                            start_version = self.transaction_stream_config.starting_version,
                            end_version = self.transaction_stream_config.request_ending_version,
                            "[Transaction Stream] Stream ended."
                        );
                        Err(anyhow!("Stream ended"))
                    },
                },
                Err(_) => {
                    warn!(
                        stream_address = self
                            .transaction_stream_config
                            .indexer_grpc_data_service_address
                            .to_string(),
                        connection_id = self.connection_id,
                        start_version = self.transaction_stream_config.starting_version,
                        end_version = self.transaction_stream_config.request_ending_version,
                        timeout_secs = self
                            .transaction_stream_config
                            .indexer_grpc_response_item_timeout_secs,
                        "[Transaction Stream] Response item timeout. Reconnecting..."
                    );
                    self.reconnect_to_grpc_with_retries().await?;
                    continue;
                },
            };
            return txn_pb_res;
        }
    }

    pub fn is_end_of_stream(&self) -> bool {
        if let (Some(ending_version), Some(last_fetched_version)) = (
            self.transaction_stream_config.request_ending_version,
            self.last_fetched_version,
        ) {
            last_fetched_version >= ending_version as i64
        } else {
            false
        }
    }

    pub async fn reconnect_to_grpc_with_retries(&mut self) -> Result<()> {
        let endpoints = self.transaction_stream_config.get_endpoints();
        let reconnection = self.transaction_stream_config.reconnection_config.clone();

        // Single backoff iterator shared across all endpoints so delays keep growing.
        let total_budget = reconnection.max_retries as usize * endpoints.len();
        let mut backoff = reconnection.backoff_iter().take(total_budget);
        let mut retry_count: u64 = 0;

        for endpoint in endpoints.iter() {
            let endpoint_address_str = endpoint.address.to_string();

            // For non-primary endpoints, wait for catchup before trying to reconnect.
            if !endpoint.is_primary {
                if let Some(needed_version) = self.last_fetched_version.map(|v| (v + 1) as u64) {
                    match wait_for_endpoint_catchup(
                        &self.transaction_stream_config,
                        endpoint,
                        needed_version,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!(
                                stream_address = endpoint_address_str,
                                "[Transaction Stream] Backup caught up, proceeding to connect"
                            );
                        },
                        Err(e) => {
                            warn!(
                                stream_address = endpoint_address_str,
                                error = ?e,
                                "[Transaction Stream] Backup catchup failed, skipping endpoint"
                            );
                            continue;
                        },
                    }
                }
            }

            for _ in 0..reconnection.max_retries {
                let delay = match wait_for_next_retry(&mut backoff, &mut retry_count).await {
                    Some(d) => d,
                    None => break,
                };

                info!(
                    stream_address = endpoint_address_str,
                    is_primary = endpoint.is_primary,
                    retry_count,
                    delay_ms = delay.as_millis() as u64,
                    "[Transaction Stream] Waiting before reconnect attempt"
                );

                match self.reconnect_to_grpc(endpoint).await {
                    Ok(_) => {
                        self.current_endpoint_is_primary = endpoint.is_primary;
                        return Ok(());
                    },
                    Err(e) => {
                        warn!(
                            stream_address = endpoint_address_str,
                            is_primary = endpoint.is_primary,
                            retry_count,
                            error = ?e,
                            "[Transaction Stream] Error reconnecting to GRPC stream"
                        );
                    },
                }
            }

            info!(
                stream_address = endpoint_address_str,
                is_primary = endpoint.is_primary,
                "[Transaction Stream] Endpoint exhausted, trying next"
            );
        }

        error!(
            total_endpoints = endpoints.len(),
            "[Transaction Stream] All {} endpoints exhausted. Will not retry.",
            endpoints.len()
        );
        Err(anyhow!(
            "All {} endpoints exhausted. Will not retry.",
            endpoints.len()
        ))
    }

    async fn reconnect_to_grpc(&mut self, endpoint: &Endpoint) -> Result<()> {
        let request_starting_version = self.last_fetched_version.map(|v| (v + 1) as u64);
        let endpoint_address_str = endpoint.address.to_string();

        info!(
            stream_address = endpoint_address_str,
            is_primary = endpoint.is_primary,
            requested_starting_version = request_starting_version,
            requested_ending_version = self.transaction_stream_config.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Transaction Stream] Reconnecting to GRPC stream"
        );

        let config_with_version = TransactionStreamConfig {
            starting_version: request_starting_version,
            ..self.transaction_stream_config.clone()
        };

        let response = get_stream_for_endpoint(config_with_version, endpoint).await?;

        let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
            Some(connection_id) => connection_id.to_str().unwrap().to_string(),
            None => "".to_string(),
        };
        self.connection_id = connection_id;
        self.stream = response.into_inner();

        info!(
            stream_address = endpoint_address_str,
            is_primary = endpoint.is_primary,
            connection_id = self.connection_id,
            starting_version = request_starting_version,
            ending_version = self.transaction_stream_config.request_ending_version,
            reconnection_retries = self.reconnection_retries,
            "[Transaction Stream] Successfully reconnected to GRPC stream"
        );
        Ok(())
    }

    pub async fn get_chain_id(self) -> Result<u64> {
        get_chain_id(self.transaction_stream_config).await
    }
}
