# Rust WASM transform template (X1.2)

Starter guest for Pramen's `type: wasm` pipeline transform. The ABI matches
spike S1.4 and `crates/pramen-wasm/wit/transform.wit`: one exported
`run` function taking Arrow IPC stream bytes and returning transformed IPC
bytes.

## Build

From this directory:

```bash
chmod +x build.sh
./build.sh
```

Requires Rust **1.97.0** and the `wasm32-wasip2` target (the script uses
`rustup run 1.97.0` from the repository toolchain pin).

## Test offline

```bash
pramen transform test --component "$(pwd)/my_transform.wasm"
```

The default fixture batch has `id`, `amount`, and `note` columns; the
template appends `amount_gross = amount * 1.21`.

## Use in a pipeline

```yaml
transforms:
  - type: wasm
    id: gross
    component: path/to/my_transform.wasm
    limits:
      memoryMb: 256
      fuel: 10000000000
```

Paths are resolved relative to the pipeline document directory.

## Customize

Edit `guest/src/lib.rs` — change `transform_batch` to your column logic.
Keep the IPC envelope and WIT export unchanged so `pramen transform test`
and the runtime host stay compatible.
