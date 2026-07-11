---
title: Loading PostgreSQL fast
description: How the binary COPY sink works, how to shape tables for it, and what the numbers say.
---

Pramen loads PostgreSQL through the binary `COPY` protocol, encoded
directly from Arrow batches in pure Rust. Measured against the same data
and database, it sustains **3.1× the throughput of `psql \copy` with CSV**
(367k rows/s vs 117k rows/s on a 5M-row, 6-column load) — mostly because
the server never parses text.

## The type matrix

Arrow batch columns map to PostgreSQL types by name:

| Arrow type | PostgreSQL type |
| --- | --- |
| `Int32` | `integer` |
| `Int64` | `bigint` |
| `Float64` | `double precision` |
| `Utf8` / `LargeUtf8` / `Utf8View` | `text` |
| `Boolean` | `boolean` |
| `Timestamp(µs, UTC)` | `timestamptz` |

NULLs pass through for nullable columns. Anything outside the matrix fails
with a precise error naming the offending Arrow type — cast it in a SQL
transform first (`date`, `uuid`, `jsonb`, and `numeric` are on the
roadmap).

## Shape the target for bulk loads

```sql
CREATE TABLE analytics.events (
    id           bigint NOT NULL,
    category     text NOT NULL,
    amount       double precision NOT NULL,
    created_at   timestamptz NOT NULL
);
```

Standard bulk-loading advice applies and compounds with the binary
protocol:

- **Create indexes after the initial load**, not before — index maintenance
  dominates large appends.
- **Batch pipelines beat trickle inserts.** One Pramen run is one
  transaction; a million rows arrive as one atomic, WAL-efficient load.
- For repeated full reloads, load into a fresh table and swap with
  `ALTER TABLE ... RENAME` — the swap is instant.

## Transactionality

The whole run is a single transaction: `BEGIN` at connect, one `COPY`
stream, `COMMIT` only when every upstream stage finished cleanly. A failed
or cancelled run leaves the table untouched — there is no partially-loaded
state to clean up, ever.

## Connection and credentials

The sink reads its connection string from an environment variable named in
the pipeline (`dsnEnv`, default `PRAMEN_POSTGRES_DSN`):

```bash
export PRAMEN_POSTGRES_DSN='postgres://loader:secret@db.internal:5432/analytics'
pramen run pipeline.yaml
```

Connection strings never appear in pipeline documents, plans, or logs.
