# External WASM guest (X2.1 third-party proof)

This directory simulates a transform authored **outside** the Pramen
repository. It is the Phase 2 extensibility proof: a third-party Component
Model guest that implements the published WIT ABI and passes
`pramen transform test` offline.

It is deliberately **not** the production SDK template
(`templates/wasm-transform-rust/`). An outsider would only need:

1. The WIT world `pramen:transform@0.1.0` (vendored here under `wit/`,
   matching `crates/pramen-wasm/wit/transform.wit` / the site cookbook).
2. Public docs: [WASM transforms cookbook](../../site/src/content/docs/cookbook/wasm-transforms.md).
3. A Rust toolchain with the `wasm32-wasip2` target.

There are **no** `path` dependencies into Pramen crates. `arrow` and
`wit-bindgen` versions stand in for what would be pinned from crates.io.

## Layout

```
examples/external-wasm-guest/
  wit/transform.wit     # vendored published ABI
  guest/                # standalone Cargo project (outside the workspace)
  dist/acme_gross.wasm  # checked-in component (CI + tests use this)
  build.sh              # reproducible rebuild
```

## Rebuild

```bash
chmod +x build.sh
./build.sh
```

Requires Rust **1.97.0** and `wasm32-wasip2` (installed by the script via
`rustup`). After rebuilding, commit `dist/acme_gross.wasm` if the bytes
change.

From the repository root:

```bash
mise run wasm-external-guest
```

## Validate (offline)

Against the checked-in artifact (no rebuild required):

```bash
pramen transform test --component examples/external-wasm-guest/dist/acme_gross.wasm
```

Or:

```bash
mise run wasm-external-conformance
```

The conformance fixture supplies `id`, `amount`, and `note`; this guest
appends `amount_gross = amount * 1.21`.

## Use in a pipeline

```yaml
transforms:
  - type: wasm
    id: acme-gross
    component: examples/external-wasm-guest/dist/acme_gross.wasm
```

Paths are resolved relative to the pipeline document directory.
