/**
 * Minimal reproduction of WASM crash in @aptos-labs/confidential-assets.
 *
 * Any operation requiring a range proof (normalizeBalance, transfer, withdraw,
 * rotateEncryptionKey) crashes with `RuntimeError: unreachable` inside the
 * WASM module's batch_range_proof function.
 *
 * Prerequisites:
 *   - A running localnet: `aptos node run-localnet -f -y`
 *
 * Usage:
 *   npx tsx repro-wasm-crash.ts
 */

import {
  Account,
  AccountAddress,
  Aptos,
  AptosConfig,
} from "@aptos-labs/ts-sdk";
import {
  ConfidentialAsset,
  TwistedEd25519PrivateKey,
  initializeWasm,
} from "@aptos-labs/confidential-assets";

const LOCALNET_REST_URL = "http://127.0.0.1:8080/v1";
const LOCALNET_FAUCET_URL = "http://127.0.0.1:8081";
const TOKEN_ADDRESS = AccountAddress.A;

async function main() {
  await initializeWasm();

  const aptos = new Aptos(
    new AptosConfig({
      network: "local" as any,
      fullnode: LOCALNET_REST_URL,
      faucet: LOCALNET_FAUCET_URL,
    }),
  );

  const confidentialAsset = new ConfidentialAsset({
    config: aptos.config,
    confidentialAssetModuleAddress: "0x1",
    withFeePayer: false,
  });

  // ── Setup: create two accounts, register, deposit, rollover ──────────

  const alice = Account.generate();
  const bob = Account.generate();
  const aliceDK = TwistedEd25519PrivateKey.generate();
  const bobDK = TwistedEd25519PrivateKey.generate();

  console.log(`Alice: ${alice.accountAddress}`);
  console.log(`Bob:   ${bob.accountAddress}`);

  await aptos.fundAccount({
    accountAddress: alice.accountAddress,
    amount: 500_000_000,
    options: { waitForIndexer: false },
  });
  await aptos.fundAccount({
    accountAddress: bob.accountAddress,
    amount: 500_000_000,
    options: { waitForIndexer: false },
  });
  console.log("Funded both accounts.");

  await confidentialAsset.registerBalance({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    decryptionKey: aliceDK,
  });
  await confidentialAsset.registerBalance({
    signer: bob,
    tokenAddress: TOKEN_ADDRESS,
    decryptionKey: bobDK,
  });
  console.log("Registered confidential balances.");

  await confidentialAsset.deposit({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    amount: 100,
  });
  console.log("Deposited 100 into Alice's confidential balance.");

  const rolls = await confidentialAsset.rolloverPendingBalance({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
  });
  console.log(`Rolled over pending balance (${rolls.length} tx).`);

  // ── All of the following crash with RuntimeError: unreachable ─────────

  console.log("\n--- Attempting normalizeBalance (requires range proof) ---");
  try {
    await confidentialAsset.normalizeBalance({
      tokenAddress: TOKEN_ADDRESS,
      senderDecryptionKey: aliceDK,
      signer: alice,
    });
    console.log("normalizeBalance succeeded");
  } catch (e: any) {
    console.error(`CRASHED: ${e.message}\n${e.stack}\n`);
  }

  console.log("--- Attempting transfer (requires range proof) ---");
  try {
    await confidentialAsset.transfer({
      senderDecryptionKey: aliceDK,
      amount: 10n,
      signer: alice,
      tokenAddress: TOKEN_ADDRESS,
      recipient: bob.accountAddress,
    });
    console.log("transfer succeeded");
  } catch (e: any) {
    console.error(`CRASHED: ${e.message}\n${e.stack}\n`);
  }

  console.log("--- Attempting withdraw (requires range proof) ---");
  try {
    await confidentialAsset.withdraw({
      signer: alice,
      tokenAddress: TOKEN_ADDRESS,
      senderDecryptionKey: aliceDK,
      amount: 5,
    });
    console.log("withdraw succeeded");
  } catch (e: any) {
    console.error(`CRASHED: ${e.message}\n${e.stack}\n`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
