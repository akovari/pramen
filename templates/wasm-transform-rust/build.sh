#!/usr/bin/env bash
# Build the guest component for wasm32-wasip2. Requires rustup and the
# repository-pinned toolchain (1.97.0).
set -euo pipefail
cd "$(dirname "$0")"

rustup run 1.97.0 rustup target add wasm32-wasip2 >/dev/null 2>&1 || true
(cd guest && rustup run 1.97.0 cargo build --release --target wasm32-wasip2)

dir="guest/target/wasm32-wasip2/release"
for candidate in "$dir/my_transform.wasm" "$dir/libmy_transform.wasm"; do
    if [ -f "$candidate" ]; then
        cp "$candidate" ./my_transform.wasm
        echo "built: $(pwd)/my_transform.wasm"
        echo "test:  pramen transform test --component $(pwd)/my_transform.wasm"
        exit 0
    fi
done
echo "FAIL: built artifact not found under $dir/" >&2
exit 1
