#!/bin/bash
# Bring up the regtest stack, fund the embedded Spark wallet, run the SDK e2e.
# Stack stays up on success.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker-compose.regtest.yml)

failure_logs() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "=== E2E failure logs ===" >&2
    "${COMPOSE[@]}" ps -a >&2 || true
    "${COMPOSE[@]}" logs --tail=150 \
      bitcoind bitcoin-init spark-operator-0 spark-operator-1 \
      spark-operator-2 ldk-server ldk-server-2 ssp >&2 || true
  fi
  exit "$status"
}

export SPARK_REF="${SPARK_REF:-$PWD/vendor/spark}"
export SDK_REF="${SDK_REF:-$SPARK_REF}"
export LDK_SERVER_REF="${LDK_SERVER_REF:-$PWD/vendor/ldk-server}"
for source_ref in "$SPARK_REF" "$LDK_SERVER_REF"; do
  if [ ! -f "$source_ref/Dockerfile" ]; then
    echo "Missing source checkout: $source_ref" >&2
    echo "Run: git submodule update --init --recursive (or check your path overrides)" >&2
    exit 1
  fi
done
trap failure_logs EXIT
export SPARK_DANGEROUSLY_DISABLE_TLS_VERIFICATION=1
export MINING=1
export SPARK_ADMIN_TOKEN="${SPARK_ADMIN_TOKEN:-regtest-spark-admin-token}"
if [ -z "${BITCOIN_RPC_PORT:-}" ]; then
  BITCOIN_RPC_PORT=$(node -e 'const net=require("net");const s=net.createServer();s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close()})')
fi
export BITCOIN_RPC_PORT
export BITCOIN_RPC_URL="${BITCOIN_RPC_URL:-http://127.0.0.1:$BITCOIN_RPC_PORT}"
export BITCOIN_RPC_USER="${BITCOIN_RPC_USER:-testutil}"
export BITCOIN_RPC_PASSWORD="${BITCOIN_RPC_PASSWORD:-testutilpassword}"

echo "=== compose up ==="
"${COMPOSE[@]}" up --build -d

echo "=== wait for SOs (8535-8537) ==="
for i in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/8535) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8536) 2>/dev/null && (echo > /dev/tcp/127.0.0.1/8537) 2>/dev/null; then
    echo "SO ports open"; break
  fi
  if [ "$i" = 60 ]; then echo "SOs did not come up"; exit 1; fi
  sleep 5
done

echo "=== wait for SO signing keyshares ==="
for attempt in $(seq 1 120); do
  keyshares_ready=1
  for index in 0 1 2; do
    count=""
    if ! count=$("${COMPOSE[@]}" exec -T postgres psql \
      -U postgres -d "sparkoperator_${index}" -tAc \
      "SELECT count(*) FROM signing_keyshares WHERE status = 'AVAILABLE';" \
      2>/dev/null); then
      keyshares_ready=0
      break
    fi
    count=$(echo "$count" | tr -d '[:space:]')
    case "$count" in
      ''|*[!0-9]*|0) keyshares_ready=0; break ;;
    esac
  done
  if [ "$keyshares_ready" = "1" ]; then
    echo "SO signing keyshares ready"
    break
  fi
  if [ "$attempt" = 120 ]; then
    echo "SO signing keyshares did not become ready"
    exit 1
  fi
  sleep 5
done

echo "=== wait for SO SSP endpoints ==="
for attempt in $(seq 1 60); do
  ssp_endpoints_ready=1
  for index in 0 1 2; do
    if ! "${COMPOSE[@]}" exec -T "spark-operator-${index}" \
      bash -c 'echo >/dev/tcp/127.0.0.1/8536' 2>/dev/null; then
      ssp_endpoints_ready=0
      break
    fi
  done
  if [ "$ssp_endpoints_ready" = "1" ]; then
    echo "SO SSP endpoints ready"
    break
  fi
  if [ "$attempt" = 60 ]; then
    echo "SO SSP endpoints did not become ready"
    exit 1
  fi
  sleep 2
done

echo "=== start SSPs after operators are ready ==="
"${COMPOSE[@]}" up -d --no-deps ssp ssp-2

echo "=== wait for SSP ==="
for i in $(seq 1 90); do
  if curl -sf http://127.0.0.1:5000/health > /dev/null; then break; fi
  if [ "$i" = 90 ]; then echo "SSP not healthy"; exit 1; fi
  sleep 4
done
curl -s http://127.0.0.1:5000/health; echo

echo "=== embedded Spark wallet funded ==="
SPARK_JSON=$(curl --fail --silent --show-error --max-time 15 \
  -H "Authorization: Bearer $SPARK_ADMIN_TOKEN" \
  http://127.0.0.1:5000/status)
SPARK_BAL=$(echo "$SPARK_JSON" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).spark?.available_sats??0)}catch{console.log(0)}})") || SPARK_BAL=0
SPARK_TOPUP=$(echo "$SPARK_JSON" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{console.log(JSON.parse(s).spark?.needs_topup===true?'yes':'no')}catch{console.log('no')}})") || SPARK_TOPUP=no
case "$SPARK_BAL" in
  ''|*[!0-9]*) SPARK_BAL=0 ;;
esac
echo "Spark available: $SPARK_BAL topup_flag: $SPARK_TOPUP"
# A single coarse leaf makes the two swaps below exercise an initial split and
# then a repeated split of its change child.
if [ "${SPARK_BAL:-0}" = "0" ] || [ "${SPARK_BAL:-0}" = "null" ] || [ "${SPARK_BAL:-0}" -lt 114000 ] || [ "$SPARK_TOPUP" = "yes" ]; then
  echo "funding/topping up embedded Spark wallet..."
  FUND_LADDER=114000 FUND_MULTIPLICITY=1 node e2e/fund-ssp.mjs
fi

SSP_ID=$(curl -s http://127.0.0.1:5000/identity | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{console.log(JSON.parse(s).identityPublicKey??'')})")
STATUS_ID=$(echo "$SPARK_JSON" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);if(j.ssp_identity_pubkey!==j.spark?.identity_pubkey)process.exit(1);console.log(j.ssp_identity_pubkey)})")
if [ -z "$SSP_ID" ] || [ "$SSP_ID" != "$STATUS_ID" ]; then
  echo "SSP identity mismatch: public=$SSP_ID status=$STATUS_ID"; exit 1
fi
echo "SSP and embedded wallet identity aligned: $SSP_ID"

SDK_DIST="$SDK_REF/sdks/js/packages/spark-sdk/dist/index.node.js"
if [ ! -f "$SDK_DIST" ]; then
  echo "SDK not built at $SDK_DIST (run yarn build:sdk in sdks/js first)"; exit 1
fi
echo "=== run e2e ==="
SPARK_SDK_DIST="$SDK_DIST" node e2e/e2e.mjs

echo "=== verify atomic swap state on every operator ==="
for index in 0 1 2; do
  linked=$("${COMPOSE[@]}" exec -T postgres psql \
    -U postgres -d "sparkoperator_${index}" -tAc \
    "SELECT count(*) FROM transfers counter JOIN transfers primary_transfer ON counter.transfer_counter_swap_transfer = primary_transfer.id WHERE counter.type = 'COUNTER_SWAP_V3' AND counter.status = 'COMPLETED' AND primary_transfer.type = 'PRIMARY_SWAP_V3' AND primary_transfer.status = 'COMPLETED' AND counter.total_value = primary_transfer.total_value;")
  unsettled=$("${COMPOSE[@]}" exec -T postgres psql \
    -U postgres -d "sparkoperator_${index}" -tAc \
    "SELECT count(*) FROM transfers WHERE type IN ('PRIMARY_SWAP_V3', 'COUNTER_SWAP_V3') AND status <> 'COMPLETED';")
  linked=$(echo "$linked" | tr -d '[:space:]')
  unsettled=$(echo "$unsettled" | tr -d '[:space:]')
  if [ "$linked" -lt 1 ] || [ "$unsettled" -ne 0 ]; then
    echo "operator ${index} has linked=${linked} unsettled=${unsettled} swap transfers"
    exit 1
  fi

  max_depth=$("${COMPOSE[@]}" exec -T postgres psql \
    -U postgres -d "sparkoperator_${index}" -tAc \
    "WITH RECURSIVE depths AS (
       SELECT id, tree_node_parent, 0 AS depth FROM tree_nodes WHERE tree_node_parent IS NULL
       UNION ALL
       SELECT child.id, child.tree_node_parent, parent.depth + 1
       FROM tree_nodes child JOIN depths parent ON child.tree_node_parent = parent.id
     ) SELECT COALESCE(MAX(depth), 0) FROM depths;")
  direct_grandchildren=$("${COMPOSE[@]}" exec -T postgres psql \
    -U postgres -d "sparkoperator_${index}" -tAc \
    "WITH RECURSIVE depths AS (
       SELECT id, tree_node_parent, direct_tx, 0 AS depth FROM tree_nodes WHERE tree_node_parent IS NULL
       UNION ALL
       SELECT child.id, child.tree_node_parent, child.direct_tx, parent.depth + 1
       FROM tree_nodes child JOIN depths parent ON child.tree_node_parent = parent.id
     ) SELECT count(*) FROM depths WHERE depth >= 2 AND direct_tx IS NOT NULL;")
  transferred_parent_splits=$("${COMPOSE[@]}" exec -T postgres psql \
    -U postgres -d "sparkoperator_${index}" -tAc \
    "SELECT count(DISTINCT parent.id)
     FROM tree_nodes parent
     JOIN tree_nodes child ON child.tree_node_parent = parent.id
     JOIN trees tree ON parent.tree_node_tree = tree.id
     JOIN deposit_addresses deposit ON tree.deposit_address_tree = deposit.id
     WHERE parent.owner_identity_pubkey <> deposit.owner_identity_pubkey;")
  max_depth=$(echo "$max_depth" | tr -d '[:space:]')
  direct_grandchildren=$(echo "$direct_grandchildren" | tr -d '[:space:]')
  transferred_parent_splits=$(echo "$transferred_parent_splits" | tr -d '[:space:]')
  if [ "$max_depth" -lt 2 ] || [ "$direct_grandchildren" -lt 1 ] || [ "$transferred_parent_splits" -lt 1 ]; then
    echo "operator ${index} has max split depth=${max_depth} direct grandchildren=${direct_grandchildren} transferred-parent splits=${transferred_parent_splits}"
    exit 1
  fi
done
