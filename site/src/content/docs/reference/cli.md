---
title: CLI reference
description: The pramen command surface — validate, explain, run, ai.
---

The v1 CLI has four commands. Global flags come before or after the
subcommand.

## Global flags

| Flag | Values | Default | Notes |
| --- | --- | --- | --- |
| `--log-format` | `pretty`, `json`, `silent` | `pretty` | Logs go to stderr; `json` is one object per line for collectors |
| `--version` / `-V` | | | Print version |

Log filtering respects `RUST_LOG` (e.g. `RUST_LOG=debug`).

## `pramen validate <file>`

Parse and validate a pipeline document. Reports every problem at once,
each with a path into the document. Performs no I/O beyond reading the
file — safe for editor hooks and CI.

Exit codes: `0` valid · `2` validation issues · `1` unreadable file.

## `pramen explain <file> [--json]`

Print the resolved plan: source, each transform with its type (and model
routing for `ai.*` steps), sink, and runtime settings. `--json` emits the
normalized document as JSON for scripting. Secrets never appear in either
form.

```text
pipeline: governed-semantic-enrichment
  source: object_store s3://example-input/events/ (parquet)
  transform normalize: sql
  transform classify: ai.extract via bedrock/anthropic.claude-3-haiku..., execution Auto, 2 output field(s), on invalid Review
  sink: postgres analytics.events (mode Append, dsn from $PRAMEN_POSTGRES_DSN)
  runtime: target batch 8388608 B, max inflight 268435456 B, checkpoint file:///var/lib/pramen/checkpoints/
```

## `pramen run <file>`

Validate, plan, and execute the pipeline. Deterministic pipelines
(Parquet source, SQL transforms, Postgres sink) run today; a pipeline
using not-yet-shipped features fails at plan time with a pointer to the
tracking task, before touching any data.

- The sink connection string comes from the environment variable named by
  `spec.sink.dsnEnv`.
- Ctrl-C cancels cooperatively; the transaction is rolled back and the
  target table is untouched.
- On success, a one-line summary reports rows in/out, batches, bytes, and
  throughput.

Planned flags: `--smoke` (record cap + cheapest model + hard cost ceiling).

## `pramen ai`

AI governance utilities — evaluation against a golden corpus
(`ai evaluate`), review-queue workflows (`ai review`). Ships with the AI
workstream.
