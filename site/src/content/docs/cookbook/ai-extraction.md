---
title: Budgeted AI extraction
description: Schema-bound, budget-capped semantic extraction with the durable inference ledger — runnable today.
---

Governed semantic transforms are live: `ai.extract` and `ai.classify` run
against the durable inference ledger with pre-dispatch budgets and strict
output validation. Three providers ship today — `mock` (deterministic,
offline, free — for dry-runs and tests), `openai-compat` (vLLM, Ollama,
llama.cpp, or any OpenAI-protocol endpoint), and `bedrock` (Amazon Bedrock
Converse, credentials from the AWS default chain, region pinned per model
declaration).

## The scenario

Support tickets arrive as NDJSON with free-text descriptions. You need a
`category` and a `confidence` as real typed columns in PostgreSQL, at a
cost you can predict, from a run you can safely restart.

## The pipeline

This is `examples/local-tickets-ai-classify.yaml` from the repository,
runnable end to end on a laptop:

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: local-tickets-ai-classify
spec:
  models:
    classifier:
      provider: mock          # swap for openai-compat + endpoint for real inference
      model: mock-1
  source:
    type: object_store
    url: /tmp/pramen-ai-input
    format: { type: ndjson }
  transforms:
    # 1. Deterministic cleanup first — cheaper than tokens.
    - id: clean
      type: sql
      query: >
        SELECT id, description
        FROM input
        WHERE description IS NOT NULL AND length(description) > 3

    # 2. Schema-bound classification with hard budgets.
    - id: classify
      type: ai.classify
      model: classifier
      inputs: [description]
      instruction: >
        Classify the support ticket into a category and estimate a
        confidence score between 0 and 1.
      output:
        fields:
          - { name: category, type: utf8 }
          - { name: confidence, type: float64 }
      validation:
        onInvalid: fail
      budget:
        maxInputTokensPerRecord: 2048
        maxOutputTokensPerRecord: 256
        maxRunTokens: 500000     # hard stop for the whole run; reuse is free
  sink:
    type: postgres
    target: analytics.tickets_classified
```

To use a real local model instead of the mock, change the model block:

```yaml
models:
  classifier:
    provider: openai-compat
    model: llama3.1
    endpoint: http://localhost:11434/v1   # Ollama
```

## What the runtime guarantees

- **Budgets bite before dispatch.** A record whose input exceeds the
  configured token ceiling is rejected up front — not billed and then
  complained about. Output caps are passed to the provider as hard limits.
  `maxRunTokens` bounds the whole run: crossing it fails fast with the
  consumed count, and everything already completed stays in the ledger,
  so the re-run picks up where the money ran out.
- **A circuit breaker is always armed.** Twenty-five consecutive invalid
  outputs (configurable via `breaker.maxConsecutiveInvalid`) abort the
  run — a spike like that means a systemic problem, and burning budget to
  drop the rest of the dataset helps nobody.
- **Every completed inference is durable.** Each validated result is
  recorded in the SQLite (WAL) ledger *before* it is used. Kill the run at
  any point and restart: finished records are reused at zero cost.
- **Only valid output reaches the table.** Model output is validated
  against the declared fields — types, nullability, no missing or extra
  fields. Failures follow `onInvalid`: `fail` the run, `drop` the record
  (counted and logged), or `review` (queue workflow lands in X1.6).
- **Re-runs are incremental.** The work key covers inputs, instruction,
  output schema, provider, model, and parameters. Tomorrow's run over a
  grown dataset pays only for new tickets; changing the instruction
  re-executes exactly the affected work.

## Provider-batch execution

Set `execution: batch` on the transform to trade latency for cost:
instead of one provider call per ledger miss, misses are collected while
input streams through, submitted as one asynchronous provider job,
polled to completion, and joined back to the buffered rows. Provider
batch APIs typically price at ~50% of online rates.

```yaml
    - id: classify
      type: ai.classify
      model: classifier
      execution: batch
      # ...inputs, instruction, output, validation, budget as before
```

The job id is recorded per item in the ledger *before* results are
awaited. If the run dies after submission, the next run finds the open
job in the ledger, waits for it, and ingests its results — nothing is
resubmitted, nothing is billed twice. `pramen ai status` shows such
in-flight work as `submitted`.

A runnable end-to-end example lives at
[examples/local-tickets-ai-classify-batch.yaml](https://github.com/akovari/pramen/blob/main/examples/local-tickets-ai-classify-batch.yaml).
Batch execution requires a batch-capable provider. `mock` implements it
for offline testing, and `openai-compat` implements it via the OpenAI
Files + Batches APIs — hosted OpenAI supports these; most self-hosted
servers (Ollama, plain vLLM) do not and fail submission with a typed
error rather than silently queuing. The Bedrock batch adapter is the
remaining cloud leg of P1.8.

## Inspect the ledger

```console
$ pramen ai status
ledger: .pramen/ledger.sqlite
  pending:   0
  submitted: 0
  completed: 6
  failed:    0
```

The ledger lives at `.pramen/ledger.sqlite` by default; set
`PRAMEN_LEDGER_PATH` to share one across working directories.
