/**
 * End-to-end test for the confidential_asset_processor.
 *
 * Usage:
 *   tsx test-processor.ts --aptos-core /path/to/aptos-core --processor-root /path/to/processor [--existing-localnet-data-dir /path/to/dir]
 *
 * This script:
 *   1. Starts a localnet in a temp directory (unless --existing-localnet-data-dir)
 *   2. Exercises all 12 confidential asset event types on-chain
 *   3. Runs the processor against the localnet's GRPC stream
 *   4. Queries Postgres and verifies the indexed rows
 */

import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import {
  Account,
  AccountAddress,
  Aptos,
  AptosConfig,
  Network,
  Ed25519PrivateKey,
} from "@aptos-labs/ts-sdk";
import {
  ConfidentialAsset,
  TwistedEd25519PrivateKey,
  initializeWasm,
} from "@aptos-labs/confidential-assets";
import { Harness } from "@aptos-labs/forklift";
import pg from "pg";

// ─── CLI args ───────────────────────────────────────────────────────────────

function parseArgs() {
  const args = process.argv.slice(2);
  let aptosCorePath: string | undefined;
  let processorRoot: string | undefined;
  let existingLocalnetDataDir: string | undefined;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--aptos-core" && args[i + 1]) {
      aptosCorePath = args[++i];
    } else if (args[i] === "--processor-root" && args[i + 1]) {
      processorRoot = args[++i];
    } else if (args[i] === "--existing-localnet-data-dir" && args[i + 1]) {
      existingLocalnetDataDir = args[++i];
    }
  }

  if (!aptosCorePath || !processorRoot) {
    console.error(
      "Usage: tsx test-processor.ts --aptos-core /path/to/aptos-core --processor-root /path/to/processor [--existing-localnet-data-dir /path/to/dir]",
    );
    process.exit(1);
  }

  const frameworkPath = path.join(aptosCorePath, "aptos-move/framework/aptos-framework");
  if (!fs.existsSync(frameworkPath)) {
    console.error(`aptos-framework not found at ${frameworkPath}`);
    process.exit(1);
  }

  if (!fs.existsSync(processorRoot)) {
    console.error(`processor root not found at ${processorRoot}`);
    process.exit(1);
  }

  if (existingLocalnetDataDir && !fs.existsSync(existingLocalnetDataDir)) {
    console.error(`existing localnet data dir not found at ${existingLocalnetDataDir}`);
    process.exit(1);
  }

  return {
    aptosCorePath: path.resolve(aptosCorePath),
    processorRoot: path.resolve(processorRoot),
    existingLocalnetDataDir: existingLocalnetDataDir ? path.resolve(existingLocalnetDataDir) : undefined,
  };
}

const { aptosCorePath, processorRoot, existingLocalnetDataDir } = parseArgs();

// ─── Constants ──────────────────────────────────────────────────────────────

const LOCALNET_PG_PORT = 5433;
const LOCALNET_PG_USER = "postgres";
const LOCALNET_GRPC_URL = "http://127.0.0.1:50051";
const TOKEN_ADDRESS = AccountAddress.A;
const CORE_RESOURCES_ADDRESS = AccountAddress.from(
  "0x000000000000000000000000000000000000000000000000000000000a550c18",
);
const PROCESSOR_DB_NAME = "ca_processor_test";
const PROCESSOR_DB_URL = `postgresql://${LOCALNET_PG_USER}@127.0.0.1:${LOCALNET_PG_PORT}/${PROCESSOR_DB_NAME}`;

const TEST_DIR = existingLocalnetDataDir ?? fs.mkdtempSync(path.join(os.tmpdir(), "ca-test-"));
const MINT_KEY_PATH = path.join(TEST_DIR, "mint.key");

const SCRIPT_DIR = path.dirname(new URL(import.meta.url).pathname);
const GOVERNANCE_DIR = path.join(SCRIPT_DIR, "governance");

type TestingProfile = "alice" | "bob" | "core";

const aliceProfile: TestingProfile = "alice";
const bobProfile: TestingProfile = "bob";
const coreProfile: TestingProfile = "core";

// ─── Helpers ────────────────────────────────────────────────────────────────

function log(msg: string) {
  console.log(`\n=== ${msg} ===`);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function exec(cmd: string, opts?: { cwd?: string }) {
  console.log(`  $ ${cmd}`);
  return execSync(cmd, { stdio: "pipe", ...opts }).toString().trim();
}

function spawnProcess(
  command: string,
  args: string[],
  opts?: { env?: NodeJS.ProcessEnv },
): ChildProcess {
  console.log(`  $ ${command} ${args.join(" ")}`);
  const proc = spawn(command, args, {
    stdio: ["ignore", "pipe", "pipe"],
    ...opts,
  });
  proc.stdout?.on("data", (d: Buffer) => process.stdout.write(d));
  proc.stderr?.on("data", (d: Buffer) => process.stderr.write(d));
  return proc;
}

async function waitForLocalnetReady(timeoutMs: number) {
  await sleep(5000);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch("http://127.0.0.1:8070");
      if (res.ok) return;
    } catch {}
    await sleep(2000);
  }
  throw new Error("Timed out waiting for localnet readiness on port 8070");
}

function privateKeyHex(key: Ed25519PrivateKey): string {
  return "0x" + Buffer.from(key.toUint8Array()).toString("hex");
}

// ─── Move.toml preparation ─────────────────────────────────────────────────

const MOVE_TOML_PATH = path.join(GOVERNANCE_DIR, "Move.toml");
const MOVE_TOML_ORIGINAL = fs.readFileSync(MOVE_TOML_PATH, "utf-8");

function prepareGovernancePackage() {
  const frameworkPath = path.join(aptosCorePath, "aptos-move/framework/aptos-framework");
  const content = MOVE_TOML_ORIGINAL.replaceAll("APTOS_FRAMEWORK_PATH", frameworkPath);
  fs.writeFileSync(MOVE_TOML_PATH, content);
}

function restoreGovernancePackage() {
  fs.writeFileSync(MOVE_TOML_PATH, MOVE_TOML_ORIGINAL);
}

// ─── Localnet lifecycle ─────────────────────────────────────────────────────

let localnetProcess: ChildProcess | undefined;

async function startLocalnet() {
  log("Starting localnet (with indexer API for Postgres)");
  console.log(`  Test directory: ${TEST_DIR}`);
  localnetProcess = spawnProcess("aptos", [
    "node",
    "run-localnet",
    "-f",
    "-y",
    `--test-dir=${TEST_DIR}`,
    "--with-indexer-api",
  ]);

  console.log("Waiting for localnet to be ready...");
  await waitForLocalnetReady(180_000);
  console.log("Localnet is ready.");
}

function stopLocalnet() {
  if (localnetProcess) {
    console.log("Stopping localnet...");
    localnetProcess.kill("SIGTERM");
    localnetProcess = undefined;
  }
}

// ─── Forklift harness setup ─────────────────────────────────────────────────

function patchProfileAddress(harness: Harness, profileName: string, address: AccountAddress) {
  const configPath = path.join(harness.getWorkingDir(), ".aptos", "config.yaml");
  const derivedAddr = harness.getAccountAddress(profileName);
  let content = fs.readFileSync(configPath, "utf-8");
  content = content.replace(derivedAddr, address.toString());
  fs.writeFileSync(configPath, content);
}

function setupCoreResourcesProfile(harness: Harness) {
  if (!fs.existsSync(MINT_KEY_PATH)) {
    throw new Error(`mint.key not found at ${MINT_KEY_PATH}`);
  }
  const keyBytes = fs.readFileSync(MINT_KEY_PATH);
  const rawKey = keyBytes.slice(1);
  const keyHex = "0x" + Buffer.from(rawKey).toString("hex");
  harness.initCliProfile(coreProfile, keyHex);
  patchProfileAddress(harness, coreProfile, CORE_RESOURCES_ADDRESS);
}

// ─── Postgres helpers ───────────────────────────────────────────────────────

async function setupProcessorDb() {
  log("Setting up processor database");
  const client = new pg.Client({
    host: "127.0.0.1",
    port: LOCALNET_PG_PORT,
    user: LOCALNET_PG_USER,
    database: "postgres",
  });
  await client.connect();
  await client.query(`DROP DATABASE IF EXISTS ${PROCESSOR_DB_NAME}`);
  await client.query(`CREATE DATABASE ${PROCESSOR_DB_NAME}`);
  await client.end();
  console.log(`Created database ${PROCESSOR_DB_NAME}`);
}

async function queryActivities(): Promise<any[]> {
  const client = new pg.Client({ connectionString: PROCESSOR_DB_URL });
  await client.connect();
  const result = await client.query(
    "SELECT * FROM confidential_asset_activities ORDER BY transaction_version, event_index",
  );
  await client.end();
  return result.rows;
}

// ─── Processor lifecycle ────────────────────────────────────────────────────

let processorProcess: ChildProcess | undefined;
let processorExitCode: number | null = null;
let processorExitSignal: string | null = null;

function prepareProcessorConfig(): string {
  const templatePath = path.join(SCRIPT_DIR, "processor-config.template.yaml");
  const template = fs.readFileSync(templatePath, "utf-8");
  const content = template
    .replace("<<GRPC_URL>>", LOCALNET_GRPC_URL)
    .replace("<<DB_URL>>", PROCESSOR_DB_URL);
  const configPath = path.join(SCRIPT_DIR, "processor-config.yaml");
  fs.writeFileSync(configPath, content);
  return configPath;
}

function findWorkspaceRoot(startDir: string): string {
  let dir = path.resolve(startDir);
  while (dir !== path.dirname(dir)) {
    const cargoToml = path.join(dir, "Cargo.toml");
    if (fs.existsSync(cargoToml)) {
      const content = fs.readFileSync(cargoToml, "utf-8");
      if (content.includes("[workspace]")) return dir;
    }
    dir = path.dirname(dir);
  }
  throw new Error(`Could not find Cargo workspace root from ${startDir}`);
}

async function startProcessor() {
  const workspaceRoot = findWorkspaceRoot(processorRoot);
  log(`Building processor (workspace root: ${workspaceRoot})`);
  exec("cargo build -p processor", { cwd: workspaceRoot });

  const binaryPath = path.join(workspaceRoot, "target/debug/processor");
  if (!fs.existsSync(binaryPath)) {
    throw new Error(`Processor binary not found at ${binaryPath}`);
  }

  log("Starting processor");
  const configPath = prepareProcessorConfig();
  processorExitCode = null;
  processorExitSignal = null;
  processorProcess = spawnProcess(binaryPath, ["-c", configPath], {
    env: { ...process.env, RUST_LOG: "info" },
  });
  processorProcess.on("exit", (code, signal) => {
    processorExitCode = code;
    processorExitSignal = signal as string | null;
  });
  console.log("Processor started, waiting for it to process some transactions...");
}

function assertProcessorAlive() {
  if (processorExitCode !== null) {
    throw new Error(
      `Processor crashed with exit code ${processorExitCode} (signal: ${processorExitSignal})`,
    );
  }
}

function stopProcessor() {
  if (processorProcess) {
    console.log("Stopping processor...");
    processorProcess.kill("SIGTERM");
    processorProcess = undefined;
  }
}

// ─── Main test flow ─────────────────────────────────────────────────────────

/** Addresses populated during on-chain activity generation. */
let aliceAddress: string;
let bobAddress: string;

async function generateOnChainActivity(aptos: Aptos, harness: Harness) {
  log("Generating on-chain confidential asset activity");

  await initializeWasm();

  const confidentialAsset = new ConfidentialAsset({
    config: aptos.config,
    confidentialAssetModuleAddress: "0x1",
    withFeePayer: false,
  });

  const alicePrivKey = Ed25519PrivateKey.generate();
  const bobPrivKey = Ed25519PrivateKey.generate();

  harness.initCliProfile(aliceProfile, privateKeyHex(alicePrivKey));
  harness.initCliProfile(bobProfile, privateKeyHex(bobPrivKey));

  const alice = Account.fromPrivateKey({ privateKey: alicePrivKey });
  const bob = Account.fromPrivateKey({ privateKey: bobPrivKey });
  aliceAddress = alice.accountAddress.toString();
  bobAddress = bob.accountAddress.toString();

  const aliceDK = TwistedEd25519PrivateKey.generate();
  const bobDK = TwistedEd25519PrivateKey.generate();

  console.log(`Alice: ${aliceAddress}`);
  console.log(`Bob:   ${bobAddress}`);

  harness.fundAccount(aliceProfile, 500_000_000);
  harness.fundAccount(bobProfile, 500_000_000);
  console.log("Funded accounts.");

  // 1. Register (Registered event x2).
  console.log("1. Registering confidential balances...");
  const regAlice = await confidentialAsset.registerBalance({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    decryptionKey: aliceDK,
  });
  console.log(`   Alice registered: ${regAlice.success}`);

  const regBob = await confidentialAsset.registerBalance({
    signer: bob,
    tokenAddress: TOKEN_ADDRESS,
    decryptionKey: bobDK,
  });
  console.log(`   Bob registered: ${regBob.success}`);

  // 2. Deposit (Deposited event).
  console.log("2. Depositing...");
  const depositTx = await confidentialAsset.deposit({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    amount: 100,
  });
  console.log(`   Deposit: ${depositTx.success}`);

  // 3. Rollover (RolledOver event).
  console.log("3. Rolling over pending balance...");
  const rolloverTxs = await confidentialAsset.rolloverPendingBalance({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
  });
  for (const tx of rolloverTxs) {
    console.log(`   Rollover tx: ${tx.success}`);
  }

  // 4. Normalize (Normalized event).
  console.log("4. Normalizing balance...");
  const normTx = await confidentialAsset.normalizeBalance({
    tokenAddress: TOKEN_ADDRESS,
    senderDecryptionKey: aliceDK,
    signer: alice,
  });
  console.log(`   Normalize: ${normTx.success}`);

  // 5. Transfer (Transferred event).
  console.log("5. Transferring...");
  const transferTx = await confidentialAsset.transfer({
    senderDecryptionKey: aliceDK,
    amount: 10n,
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    recipient: bob.accountAddress,
  });
  console.log(`   Transfer: ${transferTx.success}`);

  // 6. Withdraw (Withdrawn event).
  console.log("6. Withdrawing...");
  const withdrawTx = await confidentialAsset.withdraw({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    senderDecryptionKey: aliceDK,
    amount: 5,
  });
  console.log(`   Withdraw: ${withdrawTx.success}`);

  // 7. KeyRotated event (also triggers RolledOver + IncomingTransfersPauseChanged).
  console.log("7. Rotating encryption key...");
  const newAliceDK = TwistedEd25519PrivateKey.generate();
  await confidentialAsset.deposit({
    signer: alice,
    tokenAddress: TOKEN_ADDRESS,
    amount: 10,
  });
  const rotationTxs = await confidentialAsset.rotateEncryptionKey({
    signer: alice,
    senderDecryptionKey: aliceDK,
    newSenderDecryptionKey: newAliceDK,
    tokenAddress: TOKEN_ADDRESS,
  });
  for (const tx of rotationTxs) {
    console.log(`   Key rotation tx: ${tx.success}`);
  }

  // 8. System-level events via governance scripts (using Forklift's runMoveScript).
  log("Generating system-level events via governance");

  const moveTomlContent = fs.readFileSync(path.join(GOVERNANCE_DIR, "Move.toml"), "utf-8");
  if (moveTomlContent.includes("APTOS_FRAMEWORK_PATH")) {
    throw new Error(`Move.toml still contains placeholder! Content:\n${moveTomlContent}`);
  }
  console.log("Move.toml patched OK.");

  setupCoreResourcesProfile(harness);
  harness.fundAccount(coreProfile, 100_000_000);

  const govDir = path.resolve(GOVERNANCE_DIR);

  // 8a. AllowListingChanged — enable.
  console.log("8a. Enabling allow listing...");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_allow_listing",
    args: ["bool:true"],
  });

  // 8b. ConfidentialityForAssetTypeChanged — allow APT.
  console.log("8b. Allow-listing APT...");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_confidentiality_for_apt",
    args: ["bool:true"],
  });

  // 8c. GlobalAuditorChanged — set.
  console.log("8c. Setting global auditor...");
  const auditorDK = TwistedEd25519PrivateKey.generate();
  const auditorEkHex = Buffer.from(auditorDK.publicKey().toUint8Array()).toString("hex");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_global_auditor",
    args: [`hex:0x${auditorEkHex}`],
  });

  // 8d. AssetSpecificAuditorChanged — set.
  console.log("8d. Setting asset-specific auditor...");
  const assetAuditorDK = TwistedEd25519PrivateKey.generate();
  const assetAuditorEkHex = Buffer.from(assetAuditorDK.publicKey().toUint8Array()).toString("hex");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_asset_specific_auditor",
    args: [`address:${TOKEN_ADDRESS.toString()}`, `hex:0x${assetAuditorEkHex}`],
  });

  // 8e. AllowListingChanged — disable.
  console.log("8e. Disabling allow listing...");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_allow_listing",
    args: ["bool:false"],
  });

  // 8f. GlobalAuditorChanged — remove.
  console.log("8f. Removing global auditor...");
  harness.runMoveScript({
    sender: coreProfile,
    packageDir: govDir,
    scriptName: "set_global_auditor",
    args: ["hex:0x"],
  });

  console.log("All on-chain activity generated.");
}

// ─── Verification ───────────────────────────────────────────────────────────

const ZERO_ADDRESS = AccountAddress.ZERO.toStringLong();

function assert(condition: boolean, msg: string) {
  if (!condition) throw new Error(msg);
}

function eventData(row: any): any {
  return typeof row.event_data === "string" ? JSON.parse(row.event_data) : row.event_data;
}

function verify(rows: any[]) {
  log("Verifying indexed data");
  console.log(`Total rows: ${rows.length}`);
  assert(rows.length > 0, "No rows found in confidential_asset_activities!");

  const standardizedAlice = AccountAddress.from(aliceAddress).toStringLong();
  const standardizedBob = AccountAddress.from(bobAddress).toStringLong();
  const standardizedTokenAddress = TOKEN_ADDRESS.toStringLong();

  // ── Event type presence ─────────────────────────────────────────────

  const expectedEventTypes = [
    "Registered",
    "Deposited",
    "Withdrawn",
    "Transferred",
    "Normalized",
    "RolledOver",
    "KeyRotated",
    "IncomingTransfersPauseChanged",
    "AllowListingChanged",
    "ConfidentialityForAssetTypeChanged",
    "GlobalAuditorChanged",
    "AssetSpecificAuditorChanged",
  ];
  const foundTypes = new Set(rows.map((r: any) => r.event_type));
  console.log(`Event types found: ${[...foundTypes].sort().join(", ")}`);

  const missing = expectedEventTypes.filter((t) => !foundTypes.has(t));
  assert(missing.length === 0, `Missing event types: ${missing.join(", ")}`);
  console.log("All 12 event types present.");

  // ── Common field validation on every row ────────────────────────────

  for (const row of rows) {
    assert(row.transaction_version != null, "Missing transaction_version");
    assert(row.event_index != null, "Missing event_index");
    assert(!!row.event_type, "Missing event_type");
    assert(!!row.owner_address, "Missing owner_address");
    assert(row.event_data != null, "Missing event_data");
    assert(row.event_data_version === "1.0.0", `Bad event_data_version: ${row.event_data_version}`);
    assert(row.block_height != null, "Missing block_height");
    assert(row.is_transaction_success === true, `Expected successful tx: ${JSON.stringify(row)}`);
    assert(!!row.transaction_timestamp, "Missing transaction_timestamp");
  }
  console.log("Common field validation passed.");

  // ── Ordering: rows should be sorted by (transaction_version, event_index) ──

  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1];
    const curr = rows[i];
    const ordered =
      Number(prev.transaction_version) < Number(curr.transaction_version) ||
      (Number(prev.transaction_version) === Number(curr.transaction_version) &&
        Number(prev.event_index) < Number(curr.event_index));
    assert(ordered, `Rows not ordered at index ${i}: v${prev.transaction_version}:${prev.event_index} >= v${curr.transaction_version}:${curr.event_index}`);
  }
  console.log("Row ordering (transaction_version, event_index) verified.");

  // ── Per-event-type assertions ──────────────────────────────────────

  const byType = (t: string) => rows.filter((r: any) => r.event_type === t);

  // Registered: exactly 2 (Alice, Bob), each with ek, asset_type = APT.
  const registered = byType("Registered");
  assert(registered.length === 2, `Expected 2 Registered, got ${registered.length}`);
  const addresses = new Set(registered.map((r: any) => r.owner_address));
  assert(addresses.has(standardizedAlice), "Registered missing Alice");
  assert(addresses.has(standardizedBob), "Registered missing Bob");
  for (const r of registered) {
    assert(r.asset_type === standardizedTokenAddress, `Registered bad asset_type: ${r.asset_type}`);
    assert(r.counterparty_address == null, "Registered should not have counterparty");
    assert(r.amount == null, "Registered should not have amount");
    const d = eventData(r);
    assert(d.ek && d.ek.data && typeof d.ek.data === "string", `Registered missing ek.data: ${JSON.stringify(d)}`);
  }
  console.log("Registered: 2 events, Alice + Bob, ek present.");

  // Deposited: at least 1, each with amount > 0 and asset_type.
  const deposited = byType("Deposited");
  assert(deposited.length >= 1, `Expected >= 1 Deposited, got ${deposited.length}`);
  for (const d of deposited) {
    assert(d.amount != null && Number(d.amount) > 0, `Deposited bad amount: ${d.amount}`);
    assert(d.asset_type === standardizedTokenAddress, `Deposited bad asset_type: ${d.asset_type}`);
    assert(d.counterparty_address == null, "Deposited should not have counterparty");
  }
  assert(deposited[0].owner_address === standardizedAlice, "First Deposited should be Alice");
  assert(Number(deposited[0].amount) === 100, `First Deposited amount should be 100, got ${deposited[0].amount}`);
  console.log(`Deposited: ${deposited.length} events, amounts > 0, first is Alice=100.`);

  // RolledOver: at least 1.
  const rolled = byType("RolledOver");
  assert(rolled.length >= 1, `Expected >= 1 RolledOver, got ${rolled.length}`);
  for (const r of rolled) {
    assert(r.asset_type === standardizedTokenAddress, `RolledOver bad asset_type: ${r.asset_type}`);
    assert(r.amount == null, "RolledOver should not have amount");
  }
  console.log(`RolledOver: ${rolled.length} events.`);

  // Normalized: exactly 1.
  const normalized = byType("Normalized");
  assert(normalized.length === 1, `Expected 1 Normalized, got ${normalized.length}`);
  assert(normalized[0].owner_address === standardizedAlice, "Normalized should be Alice");
  assert(normalized[0].asset_type === standardizedTokenAddress, "Normalized bad asset_type");
  console.log("Normalized: 1 event, Alice.");

  // Transferred: exactly 1.
  const transferred = byType("Transferred");
  assert(transferred.length === 1, `Expected 1 Transferred, got ${transferred.length}`);
  assert(transferred[0].owner_address === standardizedAlice, "Transferred sender should be Alice");
  assert(transferred[0].counterparty_address === standardizedBob, "Transferred recipient should be Bob");
  assert(transferred[0].asset_type === standardizedTokenAddress, "Transferred bad asset_type");
  assert(transferred[0].amount == null, "Transferred should not have plain amount");
  const td = eventData(transferred[0]);
  assert(Array.isArray(td.amount_P), "Transferred missing amount_P array");
  assert(Array.isArray(td.amount_R_sender), "Transferred missing amount_R_sender");
  assert(Array.isArray(td.amount_R_recip), "Transferred missing amount_R_recip");
  assert(typeof td.memo === "string", "Transferred missing memo");
  console.log("Transferred: 1 event, Alice->Bob, encrypted amounts present.");

  // Withdrawn: exactly 1.
  const withdrawn = byType("Withdrawn");
  assert(withdrawn.length === 1, `Expected 1 Withdrawn, got ${withdrawn.length}`);
  assert(withdrawn[0].owner_address === standardizedAlice, "Withdrawn should be Alice");
  assert(withdrawn[0].counterparty_address != null, "Withdrawn missing counterparty");
  assert(withdrawn[0].asset_type === standardizedTokenAddress, "Withdrawn bad asset_type");
  assert(Number(withdrawn[0].amount) === 5, `Withdrawn amount should be 5, got ${withdrawn[0].amount}`);
  console.log("Withdrawn: 1 event, Alice, amount=5.");

  // KeyRotated: exactly 1, with new_ek.
  const keyRotated = byType("KeyRotated");
  assert(keyRotated.length === 1, `Expected 1 KeyRotated, got ${keyRotated.length}`);
  assert(keyRotated[0].owner_address === standardizedAlice, "KeyRotated should be Alice");
  const kd = eventData(keyRotated[0]);
  assert(kd.new_ek && kd.new_ek.data, "KeyRotated missing new_ek");
  console.log("KeyRotated: 1 event, Alice, new_ek present.");

  // IncomingTransfersPauseChanged: at least 1.
  const paused = byType("IncomingTransfersPauseChanged");
  assert(paused.length >= 1, `Expected >= 1 IncomingTransfersPauseChanged, got ${paused.length}`);
  for (const p of paused) {
    const d = eventData(p);
    assert(typeof d.paused === "boolean", `IncomingTransfersPauseChanged missing paused field`);
  }
  console.log(`IncomingTransfersPauseChanged: ${paused.length} events.`);

  // ── System events: all should have owner_address = 0x0 ─────────────

  const systemEventTypes = new Set([
    "AllowListingChanged",
    "ConfidentialityForAssetTypeChanged",
    "GlobalAuditorChanged",
    "AssetSpecificAuditorChanged",
  ]);
  for (const row of rows) {
    if (systemEventTypes.has(row.event_type)) {
      assert(
        row.owner_address === ZERO_ADDRESS,
        `System event ${row.event_type} has owner_address=${row.owner_address}, expected ${ZERO_ADDRESS}`,
      );
    }
  }
  console.log("System events: all have owner_address=0x0.");

  // AllowListingChanged: exactly 2 (enable + disable).
  const allowListing = byType("AllowListingChanged");
  assert(allowListing.length === 2, `Expected 2 AllowListingChanged, got ${allowListing.length}`);
  assert(eventData(allowListing[0]).enabled === true, "First AllowListingChanged should be enabled=true");
  assert(eventData(allowListing[1]).enabled === false, "Second AllowListingChanged should be enabled=false");
  console.log("AllowListingChanged: 2 events (enable, disable).");

  // ConfidentialityForAssetTypeChanged: exactly 1.
  const confForAsset = byType("ConfidentialityForAssetTypeChanged");
  assert(confForAsset.length === 1, `Expected 1 ConfidentialityForAssetTypeChanged, got ${confForAsset.length}`);
  assert(confForAsset[0].asset_type === standardizedTokenAddress, "ConfidentialityForAssetTypeChanged bad asset_type");
  assert(eventData(confForAsset[0]).allowed === true, "ConfidentialityForAssetTypeChanged should be allowed=true");
  console.log("ConfidentialityForAssetTypeChanged: 1 event, APT, allowed=true.");

  // GlobalAuditorChanged: exactly 2 (set + remove).
  const globalAuditor = byType("GlobalAuditorChanged");
  assert(globalAuditor.length === 2, `Expected 2 GlobalAuditorChanged, got ${globalAuditor.length}`);
  for (const g of globalAuditor) {
    const d = eventData(g);
    assert(d.auditor_epoch != null, `GlobalAuditorChanged missing auditor_epoch`);
  }
  // First should set (has ek), second should remove (no ek).
  assert(eventData(globalAuditor[0]).auditor_ek != null, "First GlobalAuditorChanged should have auditor_ek");
  assert(eventData(globalAuditor[1]).auditor_ek == null, "Second GlobalAuditorChanged should not have auditor_ek");
  console.log("GlobalAuditorChanged: 2 events (set, remove).");

  // AssetSpecificAuditorChanged: exactly 1.
  const assetAuditor = byType("AssetSpecificAuditorChanged");
  assert(assetAuditor.length === 1, `Expected 1 AssetSpecificAuditorChanged, got ${assetAuditor.length}`);
  assert(assetAuditor[0].asset_type === standardizedTokenAddress, "AssetSpecificAuditorChanged bad asset_type");
  const assetAudData = eventData(assetAuditor[0]);
  assert(assetAudData.auditor_ek != null, "AssetSpecificAuditorChanged should have auditor_ek");
  assert(assetAudData.auditor_epoch != null, "AssetSpecificAuditorChanged should have auditor_epoch");
  console.log("AssetSpecificAuditorChanged: 1 event, APT, auditor_ek present.");

  // ── Summary ────────────────────────────────────────────────────────

  log("Summary");
  for (const et of expectedEventTypes) {
    console.log(`  ${et}: ${rows.filter((r: any) => r.event_type === et).length}`);
  }
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main() {
  let exitCode = 0;
  let harness: Harness | undefined;

  try {
    prepareGovernancePackage();

    // Step 1: Start localnet (or reuse an existing one).
    if (existingLocalnetDataDir) {
      log(`Reusing existing localnet data dir: ${existingLocalnetDataDir}`);
      await waitForLocalnetReady(45_000);
    } else {
      await startLocalnet();
    }

    // Step 2: Create Forklift harness for live interaction with the localnet.
    harness = Harness.createLive("local");

    // Step 3: Set up the processor's Postgres database.
    await setupProcessorDb();

    // Step 4: Generate on-chain activity.
    const aptos = new Aptos(
      new AptosConfig({
        network: Network.LOCAL,
      }),
    );

    await generateOnChainActivity(aptos, harness);

    // Step 5: Start the processor and let it index.
    await startProcessor();
    log("Waiting for processor to index all transactions");

    const minExpectedTypes = 12;
    const deadline = Date.now() + 60_000;
    let rows: any[] = [];
    while (Date.now() < deadline) {
      await sleep(5000);
      assertProcessorAlive();
      try {
        rows = await queryActivities();
        const eventTypes = new Set(rows.map((r: any) => r.event_type));
        console.log(`  Indexed ${rows.length} rows, ${eventTypes.size} event types so far...`);
        if (eventTypes.size >= minExpectedTypes) break;
      } catch (e: any) {
        console.log(`  Waiting for table... (${e.message})`);
      }
    }

    assertProcessorAlive();
    stopProcessor();

    // Step 6: Verify.
    verify(rows);
    log("ALL CHECKS PASSED");
  } catch (e: any) {
    console.error(`\nFAILED: ${e.message}`);
    if (e.stack) console.error(e.stack);
    exitCode = 1;
  } finally {
    restoreGovernancePackage();
    harness?.cleanup();
    stopProcessor();
    if (!existingLocalnetDataDir) {
      stopLocalnet();
      try {
        fs.rmSync(TEST_DIR, { recursive: true, force: true });
      } catch {}
    }
  }

  process.exit(exitCode);
}

main();
