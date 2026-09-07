import { execFileSync } from "node:child_process";

const COMMAND_TIMEOUT_MS = Number(process.env.E2E_COMMAND_TIMEOUT_MS ?? "30000");
const HTTP_TIMEOUT_MS = Number(process.env.E2E_HTTP_TIMEOUT_MS ?? "15000");

export const SSP_BASE_URL = process.env.SSP_BASE_URL ?? "http://127.0.0.1:5000";
const SPARK_ADMIN_TOKEN = process.env.SPARK_ADMIN_TOKEN ?? "";

const apiKeys = new Map();

function command(commandName, args, timeout = COMMAND_TIMEOUT_MS) {
  return execFileSync(commandName, args, {
    encoding: "utf8",
    timeout,
    maxBuffer: 10 * 1024 * 1024,
  }).trim();
}

function ldkApiKey(container) {
  if (!apiKeys.has(container)) {
    const key = command("docker", [
      "exec",
      container,
      "sh",
      "-c",
      "od -A n -t x1 /data/regtest/api_key | tr -d ' \\n'",
    ]);
    if (!/^[0-9a-f]+$/i.test(key)) {
      throw new Error(`invalid LDK API key from ${container}`);
    }
    apiKeys.set(container, key);
  }
  return apiKeys.get(container);
}

export function ldkJson(container, ...args) {
  if (!container) throw new Error("LDK container is required");
  const output = command(
    "docker",
    [
      "exec",
      container,
      "ldk-server-cli",
      "--base-url",
      "localhost:3536",
      "--api-key",
      ldkApiKey(container),
      "--tls-cert",
      "/data/tls.crt",
      ...args,
    ],
    Number(process.env.E2E_LDK_TIMEOUT_MS ?? "60000"),
  );
  try {
    return JSON.parse(output);
  } catch {
    throw new Error(`LDK returned invalid JSON for ${args[0]}: ${output}`);
  }
}

export async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
  });
  const responseText = await response.text();
  let body;
  try {
    body = JSON.parse(responseText);
  } catch {
    throw new Error(`${url} returned ${response.status} with invalid JSON: ${responseText}`);
  }
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

export async function assertLiveHealth() {
  if (!SPARK_ADMIN_TOKEN) throw new Error("set SPARK_ADMIN_TOKEN");
  const health = await fetchJson(`${SSP_BASE_URL}/health`);
  if (health.status !== "ok") {
    throw new Error(`SSP is unhealthy: ${JSON.stringify(health)}`);
  }
  const identity = await fetchJson(`${SSP_BASE_URL}/identity`);
  if (!/^[0-9a-f]{66}$/i.test(identity.identityPublicKey ?? "")) {
    throw new Error("SSP identity endpoint has no valid public key");
  }
  const status = await fetchJson(`${SSP_BASE_URL}/status`, {
    headers: { Authorization: `Bearer ${SPARK_ADMIN_TOKEN}` },
  });
  if (status.ldk_mode !== "live") {
    throw new Error(`SSP Lightning backend is not live: ${JSON.stringify(status)}`);
  }
  if (status.ssp_identity_pubkey !== identity.identityPublicKey) {
    throw new Error("SSP public and operational identities do not match");
  }
  return status;
}

export function walletOptions(health) {
  const configuredNetwork = String(
    process.env.SPARK_NETWORK ?? health.network ?? "LOCAL",
  ).toUpperCase();
  const network = configuredNetwork === "REGTEST" ? "LOCAL" : configuredNetwork;
  return {
    network,
    sspClientOptions: {
      baseUrl: SSP_BASE_URL,
      identityPublicKey: health.ssp_identity_pubkey,
      schemaEndpoint: "graphql/spark/rc",
    },
    optimizationOptions: { auto: false, multiplicity: 0 },
    tokenOptimizationOptions: { enabled: false },
  };
}

export async function initializeWallet() {
  const sdk = process.env.SPARK_SDK_DIST;
  if (!sdk) throw new Error("set SPARK_SDK_DIST");
  const health = await assertLiveHealth();
  const { SparkWallet } = await import(sdk);
  const initialized = await SparkWallet.initialize({ options: walletOptions(health) });
  return { ...initialized, health };
}

export async function poll(label, check, options = {}) {
  const timeoutMs = options.timeoutMs ?? 120_000;
  const intervalMs = options.intervalMs ?? 2_000;
  const deadline = Date.now() + timeoutMs;
  let last = "no result";
  let lastError;
  while (Date.now() < deadline) {
    try {
      const result = await check();
      last = result;
      if (result) return result;
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  const detail = lastError instanceof Error ? lastError.message : JSON.stringify(last);
  throw new Error(`${label} timed out after ${timeoutMs}ms; last result: ${detail}`);
}

export function paymentHashOf(payment) {
  const value =
    payment?.kind?.kind?.bolt11?.hash ??
    payment?.kind?.bolt11?.hash ??
    payment?.kind?.hash;
  return typeof value === "string" ? value.toLowerCase() : undefined;
}

export function paymentsByHash(container, paymentHash, direction) {
  const response = ldkJson(container, "list-payments");
  return (response.list ?? []).filter(
    (payment) =>
      paymentHashOf(payment) === paymentHash.toLowerCase() &&
      (!direction || payment.direction === direction),
  );
}

export function paymentByHash(container, paymentHash, direction) {
  const matches = paymentsByHash(container, paymentHash, direction);
  if (matches.length > 1) {
    throw new Error(`LDK has ${matches.length} ${direction ?? ""} payments for ${paymentHash}`);
  }
  return matches[0];
}

export function assertPayment(payment, expected) {
  if (!payment) throw new Error(`payment was not found: ${JSON.stringify(expected)}`);
  for (const [field, value] of Object.entries(expected)) {
    if (payment[field] !== value) {
      throw new Error(
        `payment ${field} is ${JSON.stringify(payment[field])}; expected ${JSON.stringify(value)}`,
      );
    }
  }
  return payment;
}

export async function cleanupWallet(wallet) {
  if (wallet) await wallet.cleanupConnections();
}
