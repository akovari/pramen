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

The command runs fixture batches through production limits and diffs
schema + data.
