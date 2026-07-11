---
title: Testing pipelines locally
description: Validate, explain, run against throwaway or local databases — no cloud required.
---

Everything about a Pramen pipeline can be exercised on a laptop with zero
cloud access. That is a design rule of the project (local-first testing,
ADR 0005), and it works in your favor when developing pipelines too.

## The tight loop

```bash
pramen validate pipeline.yaml   # catches every schema problem at once
pramen explain pipeline.yaml    # shows the resolved plan before any I/O
pramen run pipeline.yaml        # the real thing
```

`validate` is instant and side-effect free — wire it into your editor save
hook or CI. `explain --json` emits the resolved plan as JSON for scripting.

## A throwaway database in one line

```bash
docker run -d --name pipeline-dev \
  -e POSTGRES_PASSWORD=dev -p 5433:5432 \
  --tmpfs /var/lib/postgresql/data \
  postgres:17-alpine

export PRAMEN_POSTGRES_DSN=postgres://postgres:dev@localhost:5433/postgres
```

The `--tmpfs` data directory keeps everything in memory: fast, and gone
when the container stops.

## Using a local PostgreSQL install

A local (e.g. Homebrew) PostgreSQL works just as well — give the pipeline
its own role and database so experiments stay contained:

```sql
CREATE ROLE pramen LOGIN PASSWORD 'pramen';
CREATE DATABASE pramen_dev OWNER pramen;
```

```bash
export PRAMEN_POSTGRES_DSN=postgres://pramen:pramen@localhost:5432/pramen_dev
```

## Generate test Parquet without leaving SQL

DuckDB is a convenient scratch generator for input fixtures:

```sql
-- duckdb
COPY (
  SELECT
    range                            AS id,
    (range % 10000) / 100.0          AS amount,
    ['alpha','beta','gamma'][(range % 3) + 1] AS category,
    now() + INTERVAL (range) SECOND  AS created_at
  FROM range(100000)
) TO '/tmp/pramen-input/part-0.parquet' (FORMAT parquet);
```

## Verify a run like you mean it

A run summary tells you rows in/out; the database tells you correctness.
Keep a verification query next to each pipeline:

```sql
SELECT count(*)                      AS rows,
       count(*) FILTER (WHERE amount_gross IS NULL) AS missing_gross,
       round(avg(amount_gross)::numeric, 2)         AS avg_gross
FROM analytics.events;
```

Because a failed run commits nothing, re-running after a fix never leaves
you reasoning about half-loaded state.

## For contributors: the repository's own tests

The repo's database tests are env-guarded: they run when
`PRAMEN_TEST_POSTGRES_DSN` is set and skip cleanly otherwise, so `cargo
test` passes offline. A machine-local `mise.local.toml` (gitignored) is
the intended home for that variable.
