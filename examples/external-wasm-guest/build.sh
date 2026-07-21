#!/usr/bin/env bash
# Build the third-party-style WASM guest (X2.1).
#
# Requires: rustup, Rust 1.97.0 (repository pin), wasm32-wasip2 target.
# The checked-in artifact under dist/ is what CI validates; re-run this
# script when changing guest/ or wit/, then commit the updated dist/*.wasm.
set -euo pipefail
cd "$(dirname "$0")"

rustup run 1.97.0 rustup target add wasm32-wasip2 >/dev/null 2>&1 || true
(cd guest && rustup run 1.97.0 cargo build --release --target wasm32-wasip2)

dir="guest/target/wasm32-wasip2/release"
mkdir -p dist
for candidate in "$dir/acme_gross.wasm" "$dir/libacme_gross.wasm"; do
    if [ -f "$candidate" ]; then
        cp "$candidate" ./dist/acme_gross.wasm
        echo "built: $(pwd)/dist/acme_gross.wasm"
        echo "test:  pramen transform test --component $(pwd)/dist/acme_gross.wasm"
        exit 0
    fi
done
echo "FAIL: built artifact not found under $dir/" >&2
exit 1
