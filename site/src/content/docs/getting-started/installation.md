---
title: Installation
description: Install Pramen from a release binary or build from source.
---

:::note
Release binaries are built with `cargo-dist` for Linux (x86_64 and
aarch64, static musl), macOS (aarch64), and Windows (x86_64), plus shell
and PowerShell installers. Current workspace version is **0.2.0** — build
from source below until a matching GitHub Release tag is published.
:::

## Release binaries

When a GitHub Release is published, pick the artifact for your platform:

| Platform | Artifact |
|----------|----------|
| Linux x86_64 | `pramen-*-x86_64-unknown-linux-musl.tar.gz` |
| Linux aarch64 | `pramen-*-aarch64-unknown-linux-musl.tar.gz` |
| macOS (Apple Silicon) | `pramen-*-aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `pramen-*-x86_64-pc-windows-msvc.zip` |

Shell and PowerShell installers are also attached to the release. After
installing, verify:

```bash
pramen --version
pramen validate examples/local-parquet-to-postgres.yaml
```

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
