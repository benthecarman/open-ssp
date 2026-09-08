#!/usr/bin/env sh
# Compatibility entry point; all test logic is in the Rust Breez SDK runner.
set -eu
cd "$(dirname "$0")/.."
exec cargo regtest test "$@"
