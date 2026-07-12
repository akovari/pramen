---
title: CLI reference
description: The pramen command surface — validate, explain, run, transform, ai.
---

The v1 CLI has five top-level command groups. Global flags come before or
after the subcommand.

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
(local or `s3://`), SQL transforms, sandboxed `type: wasm` component
transforms (Arrow IPC in/out via Wasmtime), governed `ai.extract`/`ai.classify`
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

### `--smoke [--smoke-rows N]`

A bounded rehearsal of the real pipeline before committing to the whole
dataset: the source is capped at `--smoke-rows` rows (default 100), every
semantic transform's `maxRunTokens` ceiling is clamped to 50,000 tokens
(kept if the pipeline declares a lower one), and the checkpoint store is
neither consulted nor updated — a partial run must never mark work units
complete. Rows still land in the real sink under the same transactional
contract, so the smoke run also proves connectivity and schema fit.

```console
$ pramen run --smoke examples/local-tickets-ai-classify.yaml
smoke run complete: 100 rows in / 100 rows out in 137.05ms
```

### `--otlp-endpoint <url>`

Push the final run metrics (rows/batches/bytes in and out, run duration,
attributed with the pipeline name) to an OTLP collector over
HTTP/protobuf when the run completes. `<url>` is the collector base URL,
e.g. `http://localhost:4318`; also settable via `PRAMEN_OTLP_ENDPOINT`.
An unreachable collector is a warning, never a run failure.

For log collection, `--log-format json` emits one JSON object per line
on stderr with a pinned envelope (`timestamp`, `level`, `target`,
`message`, plus each event's own flattened fields) — the key set is
snapshot-tested, so it will not drift silently.

## `pramen ai status [--ledger <path>]`

Show the inference ledger's work-item counts by state (pending, submitted,
completed, failed) and the review queue's decision counts. Defaults to
`$PRAMEN_LEDGER_PATH` or `.pramen/ledger.sqlite`.

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

## `pramen ai review`

The human side of `onInvalid: review`: records whose model output failed
validation sit durably in the review queue — withheld from every run's
output and **never re-dispatched or re-billed while they wait** — until
someone decides.

```console
$ pramen ai review list
2 pending (0 accepted, 0 rejected all-time)

  key:       d6064d36194eb523950f83af233aa14fa98b9971b9ff5b30b2500e2052046339
  transform: classify  queued: 2026-07-12T11:04:41.310Z
  reason:    field `category` has wrong type (expected Utf8, got number); missing field `confidence`
  inputs:    {"description":"printer on fire"}
  model out: {"category": 3}

$ pramen ai review accept --key d6064d36 --output '{"category": "hardware", "confidence": 0.95}'
accepted d6064d36…: recorded as a completed human-review result; the next
run emits this record from the ledger at zero model cost

$ pramen ai review reject --key d9b4390d
rejected d9b4390d…: the record is permanently dropped (replays never re-dispatch it)
```

Subcommands (all take `--ledger <path>`, defaulting like `ai status`):

- **`list`** — pending items, oldest first: work key, transform, reason,
  the record's inputs, and the raw model output.
- **`export`** — the pending queue as JSONL on stdout, one self-contained
  object per item (full work spec included), ready for labeling tools.
- **`accept --key <k> --output '<json>'`** — a corrected output. It is
  validated against the item's declared schema — human decisions obey
  exactly the model's contract — then recorded in the ledger as a
  completed result attributed to `human-review` with zero tokens.
  Invalid corrections are refused with the violation list.
- **`reject --key <k>`** — permanently drop the record; replays neither
  re-dispatch nor re-bill it.

Unique key prefixes are accepted; ambiguous ones are refused so a
decision can never land on the wrong record.

## `pramen transform test`

Run a WebAssembly component through production resource limits against the
S1.4 conformance fixture (synthetic `id` / `amount` / `note` batch). Verifies
the output schema includes the expected derived column — offline, zero cost.

```console
$ pramen transform test
OK: component `.../fixtures/s1_4_guest.wasm` transformed 8192 row(s); output has `amount_gross`

$ pramen transform test --component ./my_transform.wasm --rows 1024
```

Flags:

- `--component <path>` — `.wasm` artifact (defaults to the checked-in S1.4
  fixture in `crates/pramen-wasm/fixtures/`)
- `--rows <n>` — fixture batch size (default 8192)

Build your own guest from [`templates/wasm-transform-rust`](https://github.com/akovari/pramen/tree/main/templates/wasm-transform-rust),
then point a pipeline's `type: wasm` step at the artifact.
