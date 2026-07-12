# S1.4 spike: WASM–Arrow boundary

Disposable spike code; the permanent report is
[`docs/spikes/s1-4-wasm-arrow.md`](../../docs/spikes/s1-4-wasm-arrow.md).

```bash
rustup target add wasm32-wasip2
(cd guest && cargo build --release --target wasm32-wasip2)
(cd host && cargo build --release \
    && ./target/release/s1-4-host \
       ../guest/target/wasm32-wasip2/release/s1_4_guest.wasm)
```
