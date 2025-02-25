// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::processors::account_restoration::{
    account_restoration_models::{
        auth_key_account_addresses::AuthKeyAccountAddress, public_key_auth_keys::PublicKeyAuthKey,
    },
    account_restoration_processor_helpers::parse_account_restoration_models,
};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;

pub struct AccountRestorationExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for AccountRestorationExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<AuthKeyAccountAddress>, Vec<PublicKeyAuthKey>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        let (auth_key_account_addresses, public_key_auth_keys) =
            parse_account_restoration_models(&transactions.data);

        Ok(Some(TransactionContext {
            data: (auth_key_account_addresses, public_key_auth_keys),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for AccountRestorationExtractor {}

impl NamedStep for AccountRestorationExtractor {
    fn name(&self) -> String {
        "AccountRestorationExtractor".to_string()
    }
}
