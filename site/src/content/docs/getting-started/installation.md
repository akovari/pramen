---
title: Installation
description: Install Pramen from source today; release binaries are coming with v0.1.
---

:::note
Pramen has not shipped its first binary release yet. Release binaries for
Linux (x86_64, aarch64), macOS (aarch64), and Windows (x86_64) with shell
and PowerShell installers are prepared via `cargo-dist` and arrive with
v0.1. Until then, build from source.
:::

## Build from source

You need a recent stable Rust toolchain (the repository pins the exact
version via `rust-toolchain.toml`).

```bash
git clone https://github.com/akovari/pramen
cd pramen
cargo build --release -p pramen
./target/release/pramen --version
```

The result is a single self-contained binary — no runtime dependencies, no
database drivers, no Python environment.

## Verify

```bash
pramen validate examples/local-parquet-to-postgres.yaml
```

You should see:

```text
OK: `local-parquet-to-postgres` is a valid pramen.dev/v1alpha1 pipeline
```

## For contributors

The repository uses [mise](https://mise.jdx.dev) to pin tools and run the
same checks CI runs:

```bash
mise install
mise run ci     # fmt, clippy, tests, cargo-deny, docs — identical to CI
```

See [`AGENTS.md`](https://github.com/akovari/pramen/blob/main/AGENTS.md)
for the full working protocol.
