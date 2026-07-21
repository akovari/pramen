---
title: WASM transforms
description: Sandboxed WebAssembly component transforms from a local path or digest-pinned OCI reference.
---

`type: wasm` runs a Component Model guest over Arrow IPC under memory,
fuel, and size limits. Guests must pass `pramen transform test`.

## Local component

```yaml
transforms:
  - id: enrich
    type: wasm
    component: ./components/enrich.wasm   # relative to the pipeline file
    limits:
      memoryMb: 256
      fuel: 10000000000
```

Build a starter guest from `templates/wasm-transform-rust/` in the
repository.

## Third-party proof (X2.1)

Pramen's extensibility exit criterion is a transform authored **outside**
the in-repo SDK template that still passes the offline conformance suite.

`examples/external-wasm-guest/` is that proof: a standalone Cargo project
with a vendored copy of the published WIT (`pramen:transform@0.1.0`), no
`path` deps into Pramen crates, and a checked-in component under
`dist/acme_gross.wasm`.

```bash
# rebuild (needs Rust 1.97.0 + wasm32-wasip2)
mise run wasm-external-guest

# validate offline
pramen transform test --component examples/external-wasm-guest/dist/acme_gross.wasm
# or: mise run wasm-external-conformance
```

An outsider needs only the WIT world (also in
`crates/pramen-wasm/wit/transform.wit`), this cookbook, and a
`wasm32-wasip2` toolchain — see the [external guest README](https://github.com/akovari/pramen/tree/main/examples/external-wasm-guest).

## OCI by digest

Tag-only references are rejected. Pulls are fail-closed unless the digest
(or registry/repository prefix) is allow-listed:

```yaml
transforms:
  - id: enrich
    type: wasm
    component: oci://ghcr.io/acme/enrich@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
runtime:
  wasmOciAllowlist:
    - sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
    # or: ghcr.io/acme/enrich
```

`PRAMEN_WASM_OCI_ALLOWLIST` (comma-separated) merges with the spec list.
Pulled artifacts land in the digest-keyed Wasmtime cache. Signature
verification is a hook today (default allow-all); cosign/sigstore is
planned.

## Conformance

```bash
pramen transform test --component path/or/oci-ref …
```

The command runs fixture batches through production limits and checks row
count plus the presence of `amount_gross` (the default fixture column
guests are expected to derive from `amount`).
