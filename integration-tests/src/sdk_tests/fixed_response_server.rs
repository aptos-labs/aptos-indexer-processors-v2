// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

//! A data service that ignores the requested range, so a test can feed a processor a batch the
//! SDK mock never serves: one that runs past the requested end, or whose `processed_range`
//! disagrees with its transactions.

use aptos_indexer_processor_sdk::aptos_protos::indexer::v1::{
    GetTransactionsRequest, TransactionsResponse,
    raw_data_server::{RawData, RawDataServer},
};
use futures::Stream;
use std::pin::Pin;
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    transport::{Server, server::TcpIncoming},
};
use url::Url;

/// Answers every request, including the chain ID probe, with the same single response.
struct FixedResponse(TransactionsResponse);

#[tonic::async_trait]
impl RawData for FixedResponse {
    type GetTransactionsStream =
        Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

    async fn get_transactions(
        &self,
        _request: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::iter([Ok(self
            .0
            .clone())]))))
    }
}

/// Serves `response` on a free local port for the rest of the test and returns its address, to
/// use as `TransactionStreamConfig::indexer_grpc_data_service_address`. The response needs a
/// `chain_id` matching the database's, because the processor's chain ID check reads it too.
pub async fn serve_fixed_response(response: TransactionsResponse) -> Url {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = RawDataServer::new(FixedResponse(response))
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Zstd);
    tokio::spawn(
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()),
    );
    Url::parse(&format!("http://{address}")).unwrap()
}
