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
