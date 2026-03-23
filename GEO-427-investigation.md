# GEO-427: Investigation — Missing Transfer Events on Store Deletion

## Summary

When a fungible asset (FA) store is **transferred as an object** (i.e., the
`0x1::object::TransferEvent` / `0x1::object::Transfer` event fires), the
**fungible asset processor does not record an activity row** in the
`fungible_asset_activities` table. This means the `GetFungibleAssetActivities`
GraphQL query (backed by that table) will miss balance-affecting transfers that
happen via object ownership changes rather than via explicit
`0x1::fungible_asset::Deposit` / `Withdraw` events.

## Root Cause

There are two completely separate processors in this codebase that handle
overlapping concerns:

### 1. Fungible Asset Processor (`processors/fungible_asset/`)

This processor indexes the `fungible_asset_activities` table. It processes
**only these event types** as activities (see `v2_fungible_asset_utils.rs`
`FungibleAssetEvent::from_event`):

- `0x1::fungible_asset::DepositEvent` (v1)
- `0x1::fungible_asset::WithdrawEvent` (v1)
- `0x1::fungible_asset::FrozenEvent` (v1)
- `0x1::fungible_asset::Deposit` (v2 / module events)
- `0x1::fungible_asset::Withdraw` (v2 / module events)
- `0x1::fungible_asset::Frozen` (v2 / module events)

Plus legacy v1 coin events (`0x1::coin::DepositEvent`, `WithdrawEvent`) and a
synthetic gas fee event.

**Critically, it does NOT handle `0x1::object::TransferEvent` or
`0x1::object::Transfer`.** The string `TransferEvent` does not appear anywhere
in the `processors/fungible_asset/` directory.

### 2. Token V2 Processor (`processors/token_v2/`)

This processor **does** handle `0x1::object::TransferEvent` /
`0x1::object::Transfer` (see `v2_token_utils.rs` `V2TokenEvent::from_event`).
However, it writes to the `token_activities_v2` table, not
`fungible_asset_activities`. The token v2 processor treats `TransferEvent` as
an NFT/token-level activity (amount = 1, from/to addresses) and has no
awareness that the transferred object might be a fungible asset store.

### The Gap

When a fungible asset store is **transferred as an object** — meaning the
`ObjectCore.owner` changes from address A to address B — the following happens
on chain:

1. An `0x1::object::TransferEvent` is emitted (with `from`, `to`, `object`
   fields).
2. A `WriteResource` for `ObjectCore` on the store's address is emitted,
   updating the `owner` field.
3. **No** `0x1::fungible_asset::Deposit` or `Withdraw` event is emitted,
   because the FA balance itself didn't change — only the ownership of the
   store object changed.

The fungible asset processor:

- **Activities**: Since there is no `Deposit`/`Withdraw` event, no
  `fungible_asset_activities` row is created.
- **Balances**: The `current_fungible_asset_balances` table IS updated because
  the processor reads `ObjectCore` from write resources in Loop 1
  (`fungible_asset_processor_helpers.rs` lines 171–203). So the balance table
  gets the new owner. But no activity is logged.

When a fungible asset store is **deleted** (e.g., the object is destroyed):

1. A `0x1::fungible_asset::FungibleStoreDeletion` event is emitted (since a
   certain chain version — older transactions lack this).
2. The processor picks up the deletion event and uses it to look up metadata
   (owner, asset type) for activity rows that refer to the deleted store.
3. If `Withdraw`/`Deposit` events are also emitted in the same transaction,
   those activities ARE indexed. But if the store is simply destroyed with no
   `Withdraw` event (e.g., the remaining balance was zero, or the entire
   transfer happens via object transfer + deletion), no activity is recorded.

## Concrete Example

The address cited in the Slack thread:
```
0x4ea3c7d6fd8ee6e752ca70420d4aac1fda379db4475520249faf8e04ad31c5a4
```

If this account received fungible assets via object transfers (i.e., someone
transferred a secondary FA store object to them), those transfers would:
- Update `current_fungible_asset_balances` (balance snapshot is correct).
- **NOT** create `fungible_asset_activities` rows.

So querying `GetFungibleAssetActivities` and summing deposits/withdrawals would
produce a different result than the actual on-chain balance.

## What IS Indexed Correctly

| Scenario | Activities Indexed? | Balances Updated? |
|----------|-------------------|------------------|
| `fungible_asset::Deposit` / `Withdraw` | Yes | Yes |
| `coin::DepositEvent` / `WithdrawEvent` (v1) | Yes | Yes |
| Gas fee (synthetic event) | Yes | Yes |
| `FungibleStoreDeletion` event | No (only used for metadata lookup) | Yes (zero balance) |
| `object::TransferEvent` on an FA store | **No** | Yes (owner updated) |
| `object::Transfer` on an FA store | **No** | Yes (owner updated) |

## Possible Fixes

### Option A: Handle `TransferEvent` in the Fungible Asset Processor

In `fungible_asset_processor_helpers.rs`, during the event loop (Loop 4), also
check for `0x1::object::TransferEvent` / `0x1::object::Transfer`. When the
transferred object address matches a known fungible asset store (from the
`fungible_asset_object_helper` mapping or `store_address_to_deleted_fa_store_events`),
create a synthetic pair of activity rows:

- A "withdraw" activity (amount = store balance, from = old owner)
- A "deposit" activity (amount = store balance, to = new owner)

Or a single "transfer" activity row with a new event type.

This requires:

1. Parsing `TransferEvent` in the fungible asset processor (import from
   `token_v2` models or duplicate the struct).
2. Looking up the FA store's balance and asset type from the object aggregated
   data.
3. Deciding on the activity event type string (e.g.,
   `0x1::object::TransferEvent` or a synthetic name).

### Option B: Create a Separate "Object Transfer Activity" Table

Add a new table that specifically tracks object transfers with FA-relevant
metadata. Clients can union `fungible_asset_activities` with this new table.

### Option C: Tell API Consumers to Also Query `token_activities_v2`

The `0x1::object::TransferEvent` is already indexed in `token_activities_v2`.
Clients could query both tables and join. However, `token_activities_v2` does
not have the FA-specific fields (asset type, amount, storage_id) so this is
insufficient for balance reconciliation.

## Recommendation

**Option A** is the most practical. The fungible asset processor already has all
the data it needs (object metadata, store balances) in the
`fungible_asset_object_helper` mapping. Adding `TransferEvent` handling in
Loop 4 would create the missing activity rows without requiring schema changes,
new tables, or client-side joins.

## Files to Change (for Option A)

1. `processor/src/processors/fungible_asset/fungible_asset_models/v2_fungible_asset_utils.rs`
   — Add `TransferEvent` parsing (or re-export from `token_v2`).
2. `processor/src/processors/fungible_asset/fungible_asset_models/v2_fungible_asset_activities.rs`
   — Add a `get_v2_from_transfer_event` method.
3. `processor/src/processors/fungible_asset/fungible_asset_processor_helpers.rs`
   — In Loop 4, check for transfer events on FA stores and emit activity rows.
4. Integration tests — Add test fixtures for object transfer of FA stores.
