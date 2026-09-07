#!/bin/bash
# LDK channel ops against the compose ldk-server (regtest).
# Needs the ldk-server-cli binary + node data dir. Run on the compose host.
# Usage: ./channels.sh <node-info|connect <pubkey@host:port>|open <pubkey> <amount_sats>|list-channels|list-peers>
set -e
cd "$(dirname "$0")/.."

LDK_DATA="${LDK_DATA:-$(docker volume inspect open-ssp_ldk-data --format '{{.Mountpoint}}' 2>/dev/null)}"
if [ -z "$LDK_DATA" ]; then echo "ldk-data volume not found; is the stack up?"; exit 1; fi
if [ ! -r "$LDK_DATA/regtest/api_key" ]; then echo "need read access to $LDK_DATA (try sudo)"; exit 1; fi

# Build the CLI once from the ldk-server checkout (cached afterwards).
LDK_REF="${LDK_SERVER_REF:-$PWD/vendor/ldk-server}"
CLI="$LDK_REF/target/release/ldk-server-cli"
if [ ! -x "$CLI" ]; then
  echo "building ldk-server-cli (one time)..."
  (cd "$LDK_REF" && cargo build --release -p ldk-server-cli)
fi

API_KEY_HEX=$(xxd -p "$LDK_DATA/regtest/api_key" | tr -d '\n')
exec "$CLI" --base-url 127.0.0.1:3536 --api-key "$API_KEY_HEX" --tls-cert "$LDK_DATA/tls.crt" "$@"
