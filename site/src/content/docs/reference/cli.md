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

Validate, plan, and execute the pipeline. Parquet and NDJSON sources
(local or `s3://`), SQL transforms, governed `ai.extract`/`ai.classify`
steps (providers `mock`, `openai-compat`, `bedrock`; online or
provider-batch execution with crash reconciliation), checkpointed
incremental runs, and the Postgres sink in `append` or `upsert` mode run
today; a pipeline using not-yet-shipped features (`execution: batch` on
a provider without a batch adapter, Azure/GCS) fails at plan time with a
clear message, before touching any data.

- The sink connection string comes from the environment variable named by
  `spec.sink.dsnEnv`.
- S3 access is configured purely from the standard `AWS_*` environment
  (see the [S3 recipe](/pramen/cookbook/s3-sources/)); Bedrock uses the
  AWS default credential chain.
- Semantic steps record every validated result in the inference ledger
  (`.pramen/ledger.sqlite`, or `PRAMEN_LEDGER_PATH`) before use; replays
  reuse recorded results instead of re-dispatching.
- An `openai-compat` model reads its optional API key from
  `PRAMEN_OPENAI_API_KEY`.
- With `runtime.checkpoint` set, completed source files are skipped and
  newly consumed ones are durably recorded after the sink commits; a run
  with nothing left to do reports `nothing to do` and exits successfully.
- Ctrl-C cancels cooperatively; the transaction is rolled back and the
  target table is untouched.
- On success, a one-line summary reports rows in/out, batches, bytes, and
  throughput.

Planned flags: `--smoke` (record cap + cheapest model + hard cost ceiling).

## `pramen ai status [--ledger <path>]`

Show the inference ledger's work-item counts by state (pending, submitted,
completed, failed). Defaults to `$PRAMEN_LEDGER_PATH` or
`.pramen/ledger.sqlite`.

## `pramen ai evaluate`

Measure a model's quality, cost, and latency on a versioned golden corpus
— through exactly the provider adapters the pipeline uses, so measured
quality transfers. Results land in a timestamped directory
(`report.json` + per-item `items.jsonl`), making quality regressions
across prompt or model revisions diffable artifacts.

```console
$ pramen ai evaluate --input-price 0.25 --output-price 1.25
corpus: support-tickets v1 (520 items)
provider/model: mock/mock-1
schema-valid: 520/520 (100.0%)
field              weight  accuracy  macro-F1
category              3.0     0.000     0.000
priority              2.0     0.000     0.000
product               1.0     0.000         -
requires_review       1.0     0.492         -
weighted score: 0.070
tokens: 58335 in / 13280 out (~$0.0312)
latency: p50 0.0 ms, p95 0.1 ms
results: .pramen/eval/20260712T075538Z-mock-mock-1
```

The mock provider fabricates schema-perfect but semantically random
output, so its scores double as a sanity floor: 100% schema-valid,
near-zero accuracy (booleans land near coin-flip). A real model runs
through `--provider openai-compat --endpoint http://localhost:11434/v1
--model <name>` (Ollama, vLLM, llama.cpp) or `--provider bedrock`.

Flags: `--corpus` (default `corpora/support-tickets.v1.yaml`),
`--provider` (`mock`, `openai-compat`, `bedrock`; default `mock`),
`--model`, `--endpoint`, `--region`, `--limit N` (first N items only),
`--out` (results root, default `.pramen/eval`), and
`--input-price`/`--output-price` (USD per million tokens, adds the cost
estimate). Scoring reports the schema-valid rate, per-field exact-match
accuracy, macro-F1 for string fields, one rubric-weighted overall score,
token totals, and latency percentiles. Evaluations bypass the ledger by
design: they measure the model, so nothing is reused or recorded.

The checked-in corpus is 520 synthetic support tickets, labelled by
construction and regenerable with
`cargo run -p pramen-ai --example gen_corpus`.

Planned subcommands: `ai review` (review-queue workflow).
