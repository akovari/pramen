---
title: Quickstart
description: One binary, one YAML file, governed AI-enriched rows in PostgreSQL.
---

This walkthrough runs a real governed-AI pipeline on your machine: NDJSON
support tickets on disk, one SQL cleanup step, one governed `ai.classify`
step, and a transactional bulk load into PostgreSQL. It is offline and
free — the deterministic `mock` provider stands in for a model — and the
same pipeline switches to a real model by changing one `provider` line.

Every step below is executed by
[`scripts/quickstart.sh`](https://github.com/akovari/pramen/blob/main/scripts/quickstart.sh)
in CI on every change and timed against a ten-minute bar, so this page
cannot silently drift from what works. (Locally the pipeline itself
finishes in seconds.)

## 1. Prerequisites

- A built `pramen` binary ([installation](/pramen/getting-started/installation/))
- A PostgreSQL you can write to — local install or a throwaway container:

```bash
docker run -d --name pramen-quickstart \
  -e POSTGRES_PASSWORD=quickstart -p 5432:5432 postgres:17-alpine
```

## 2. Generate input

A thousand synthetic support tickets, one JSON object per line:

```bash
mkdir -p /tmp/pramen-ai-input
awk 'BEGIN {
  for (i = 1; i <= 1000; i++)
    printf("{\"id\": %d, \"description\": \"ticket %d: subsystem %d reports a fault\"}\n", i, i, i % 7)
}' > /tmp/pramen-ai-input/tickets.ndjson
```

## 3. Create the target table

```bash
psql postgres://postgres:quickstart@localhost:5432/postgres <<'SQL'
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.tickets_classified (
    id          bigint NOT NULL,
    description text NOT NULL,
    category    text NOT NULL,
    confidence  double precision NOT NULL
);
SQL
```

## 4. The pipeline

This is the repository's
[`examples/local-tickets-ai-classify.yaml`](https://github.com/akovari/pramen/blob/main/examples/local-tickets-ai-classify.yaml):

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: local-tickets-ai-classify
spec:
  models:
    classifier:
      provider: mock        # swap for openai-compat or bedrock later
      model: mock-1
  source:
    type: object_store
    url: /tmp/pramen-ai-input
    format:
      type: ndjson
  transforms:
    - id: clean
      type: sql
      query: >
        SELECT id, description
        FROM input
        WHERE description IS NOT NULL AND length(description) > 3
    - id: classify
      type: ai.classify
      model: classifier
      inputs: [description]
      instruction: >
        Classify the support ticket into a category and estimate a
        confidence score between 0 and 1.
      output:
        fields:
          - name: category
            type: utf8
          - name: confidence
            type: float64
      validation:
        onInvalid: fail
      budget:
        maxInputTokensPerRecord: 2048
        maxOutputTokensPerRecord: 256
  sink:
    type: postgres
    target: analytics.tickets_classified
    mode: append
```

Inside a SQL transform, the incoming stream is always the table `input`.

## 5. Validate, smoke, run

```bash
pramen validate examples/local-tickets-ai-classify.yaml
```

`validate` reports **every** problem at once, each with a path into the
document — no fix-one-rerun loops. Before committing to the whole
dataset, rehearse cheaply — `--smoke` caps the source at 100 rows and
clamps every semantic transform's token ceiling:

```bash
export PRAMEN_POSTGRES_DSN=postgres://postgres:quickstart@localhost:5432/postgres
pramen run --smoke examples/local-tickets-ai-classify.yaml
```

```text
smoke run complete: 100 rows in / 100 rows out in 137.05ms
```

Then the real thing:

```bash
pramen run examples/local-tickets-ai-classify.yaml
```

```text
run complete: 1000 rows in / 1000 rows out in 236.31ms
  (4232 rows/s out, 1 batches, 0.1 MiB written)
```

The load happens inside a single transaction using PostgreSQL's binary
`COPY` protocol: if anything fails mid-run — including Ctrl-C — the table
is left exactly as it was. Every classification was recorded in the
durable inference ledger first; run it again and nothing is
re-dispatched:

```bash
pramen ai status
```

```text
ledger: .pramen/ledger.sqlite
  pending:   0
  submitted: 0
  completed: 1000
  failed:    0
```

## 6. Make it real

Point the model at an actual backend — everything else stays identical,
including budgets, validation, and the ledger:

```yaml
  models:
    classifier:
      provider: openai-compat          # e.g. a local Ollama
      endpoint: http://localhost:11434/v1
      model: llama3.1:8b
```

## Where next

- [The pipeline document](/pramen/concepts/pipeline-spec/) — every field, and
  the thinking behind the shape.
- [Governed AI enrichment](/pramen/concepts/governed-ai/) — budgets,
  validation, the inference ledger, and provider-batch execution.
- [Cookbook: AI extraction](/pramen/cookbook/ai-extraction/) — richer
  semantic pipelines, including `execution: batch`.
- [Cookbook: filter and derive with SQL](/pramen/cookbook/filter-and-derive/)
  — practical deterministic transform patterns.
