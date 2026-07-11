---
title: Quickstart
description: From nothing to transformed rows in PostgreSQL in a few minutes.
---

This walkthrough runs a real pipeline on your machine: Parquet files on
disk, one SQL transform, and a transactional bulk load into PostgreSQL.

## 1. Prerequisites

- A built `pramen` binary ([installation](/pramen/getting-started/installation/))
- A PostgreSQL you can write to — local install or a throwaway container:

```bash
docker run -d --name pramen-quickstart \
  -e POSTGRES_PASSWORD=quickstart -p 5432:5432 postgres:17-alpine
```

## 2. Create the target table

```bash
psql postgres://postgres:quickstart@localhost:5432/postgres <<'SQL'
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE analytics.events (
    id           bigint NOT NULL,
    category     text NOT NULL,
    amount       double precision NOT NULL,
    amount_gross double precision NOT NULL
);
SQL
```

## 3. Write the pipeline

Save this as `pipeline.yaml` (it is also in the repository as
`examples/local-parquet-to-postgres.yaml`):

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: local-parquet-to-postgres
spec:
  source:
    type: object_store
    url: /tmp/pramen-input        # a directory of .parquet files
    format:
      type: parquet
  transforms:
    - id: enrich
      type: sql
      query: >
        SELECT id, category, amount, amount * 1.21 AS amount_gross
        FROM input
        WHERE category <> 'epsilon'
  sink:
    type: postgres
    target: analytics.events
    mode: append
```

Inside a SQL transform, the incoming stream is always the table `input`.

## 4. Validate and inspect

```bash
pramen validate pipeline.yaml
pramen explain pipeline.yaml
```

`validate` reports **every** problem at once, each with a path into the
document — no fix-one-rerun loops:

```text
pramen: pipeline.yaml has 2 validation issue(s):
  - spec.transforms[0].id: must not be empty
  - spec.sink.target: `events` must be a qualified `schema.table` name
```

## 5. Run

The connection string is a secret, so it never appears in the pipeline
document — it comes from the environment:

```bash
export PRAMEN_POSTGRES_DSN=postgres://postgres:quickstart@localhost:5432/postgres
pramen run pipeline.yaml
```

```text
run complete: 200000 rows in / 160000 rows out in 1.62s
  (98942 rows/s out, 28 batches, 6.1 MiB written)
```

The load happens inside a single transaction using PostgreSQL's binary
`COPY` protocol: if anything fails mid-run — including Ctrl-C — the table
is left exactly as it was.

## Where next

- [The pipeline document](/pramen/concepts/pipeline-spec/) — every field, and
  the thinking behind the shape.
- [Cookbook: filter and derive with SQL](/pramen/cookbook/filter-and-derive/)
  — practical transform patterns.
- [Governed AI enrichment](/pramen/concepts/governed-ai/) — where
  `ai.extract` fits and how the inference ledger keeps costs sane.
