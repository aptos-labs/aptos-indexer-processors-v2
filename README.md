# Aptos Core Processors (SDK version)
Processors that index data from the Aptos Transaction Stream (GRPC). These processors have been (re)-written using the new Indexer SDK.

- **Note: Official releases coming soon!**

## Overview
This tutorial shows you how to run the Aptos core processors in this repo.

If you want to index a custom contract, we recommend using the [Quickstart Guide](https://aptos.dev/en/build/indexer/indexer-sdk/quickstart).

### Prerequisite

- A running PostgreSQL instance, with a valid database. More tutorial can be
  found [here](https://github.com/aptos-labs/aptos-core/tree/main/crates/indexer#postgres)

- [diesel-cli](https://diesel.rs/guides/getting-started)

- A `config.yaml` file. See example [here](./processor/example-config.yaml).

#### `config.yaml` Explanation

- `processor_config`
    - `type`: which processor to run
    - `channel_size`: size of channel in between steps
    - Some processors require additional configuration. See the full list of configs [here](./processor/src/config/processor_config.rs#L102).

- `processor_mode`: The processor can be run in these modes:
    - Default (bootstrap) mode: On first run, the processor will start from `initial_starting_version`. Upon restart, the processor continues from `processor_status.last_success_version` saved in DB. 
        ```
        processor_mode:
            type: default
            initial_starting_version: 0
        ```
    - Backfill mode: Running in backfill mode will track the backfill status in `backfill_processor_status` table. Give your backfill a unique identifier, `backfill_id`. If the backfill restarts, it will continue from `backfill_processor_status.last_success_version`. 
        ```
        processor_mode:
            type: backfill
            backfill_id: bug_fix_101 # Appended to `processor_type` for a unique backfill identifier
            initial_starting_version: 0 # Processor starts here unless there is a greater checkpointed version
            ending_version: 1000 # If no ending_version is set, it will use `processor_status.last_success_version`
            overwrite_checkpoint: false # Overwrite checkpoints if it exists, restarting the backfill from `initial_starting_version`. Defaults to false
        ```
    - Testing mode: This mode is used to replay the processor for specific transaction versions. The processor always starts at `override_starting_version` and does not update the `processor_status` table. If no `ending_version` is set, the processor will run only using `override_starting_version` (1 transaction).
        ```
        processor_mode:
            type: testing
            override_starting_version: 100
            ending_version: 200 # Optional. Defaults to override_starting_version
        ``

- `transaction_stream_config`
    - `indexer_grpc_data_service_address`: Data service non-TLS endpoint address. See [available Transaction Stream endpoints](https://aptos.dev/en/build/indexer/txn-stream/aptos-hosted-txn-stream).
    - `auth_token`: Auth token used for connection. See [instructions on how to get an auth token](https://aptos.dev/en/build/indexer/txn-stream/aptos-hosted-txn-stream).
    - `request_name_header`: request name header to append to the grpc request; name of the processor
    - `additional_headers`: addtional headers to append to the grpc request
    - `indexer_grpc_http2_ping_interval_in_secs`: client-side grpc HTTP2 ping interval.
    - `indexer_grpc_http2_ping_timeout_in_secs`: client-side grpc HTTP2 ping timeout.
    - `indexer_grpc_reconnection_timeout_secs`: grpc reconnection timeout
    - `indexer_grpc_response_item_timeout_secs`: grpc response item timeout
   
- `db_config`
    - `type`: type of storage, `postgres_config` or `parquet_config`
    - `connection_string`: PostgresQL DB connection string


### Use docker image for existing processors (Only for **Unix/Linux**)

- Use the provided `Dockerfile` and `config.yaml` (update accordingly)
    - Build: `cd ecosystem/indexer-grpc/indexer-grpc-parser && docker build . -t indexer-processor`
    - Run: `docker run indexer-processor:latest`

### Use source code for existing parsers

- Use the provided `config.yaml` (update accordingly)
- Run `cd processor && cargo run --release -- -c config.yaml`


### Manually running diesel-cli
- `cd` into the database folder you use under `processor/src/db/`, then run it.

## Processor Specific Notes

### Supported Coin Type Mappings
See mapping in [v2_fungible_asset_balances.rs](https://github.com/aptos-labs/aptos-indexer-processors/blob/main/rust/processor/src/db/common/models/fungible_asset_models/v2_fungible_asset_balances.rs#L40) for a list supported coin type mappings.
