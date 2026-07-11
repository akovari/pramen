---
title: Filter and derive with SQL
description: Practical SQL transform patterns — filtering, derivations, renames, and type shaping.
---

The `sql` transform runs DataFusion SQL over the stream, with the incoming
data always visible as the table `input`. These recipes cover the patterns
that come up in real movement pipelines.

## Filter rows, keep the schema

```yaml
transforms:
  - id: only-valid
    type: sql
    query: SELECT * FROM input WHERE amount IS NOT NULL AND amount > 0
```

## Derive new columns

```yaml
transforms:
  - id: enrich
    type: sql
    query: >
      SELECT
        id,
        amount,
        amount * 1.21                    AS amount_gross,
        date_trunc('hour', created_at)   AS hour,
        length(payload)                  AS payload_len
      FROM input
```

## Normalize text before an AI step

Cheap deterministic cleanup before a model call saves tokens and improves
consistency — lowercase, trim, and drop empties:

```yaml
transforms:
  - id: normalize
    type: sql
    query: >
      SELECT ticket_id, lower(trim(description)) AS description, created_at
      FROM input
      WHERE description IS NOT NULL AND length(trim(description)) > 0
```

## Rename and reorder to match the target table

The Postgres sink maps columns **by name** from the batch schema to the
`COPY` column list, so the SQL projection is where you make the shapes
line up:

```yaml
transforms:
  - id: shape-for-target
    type: sql
    query: >
      SELECT
        event_id      AS id,
        event_type    AS category,
        amount_cents / 100.0 AS amount
      FROM input
```

## Cast to the sink's type matrix

The v1 Postgres sink supports `bigint`, `double precision`, `text`,
`boolean`, and `timestamptz`. Cast anything else explicitly:

```yaml
transforms:
  - id: cast
    type: sql
    query: >
      SELECT
        CAST(id AS BIGINT)            AS id,
        CAST(score AS DOUBLE)         AS score,
        CAST(flags AS VARCHAR)        AS flags
      FROM input
```

## Chain transforms instead of nesting

Each step sees the previous step's output as `input`. Prefer several small,
named steps over one giant query — `pramen explain` then reads like a plan:

```yaml
transforms:
  - id: parse
    type: sql
    query: SELECT id, lower(raw_category) AS category, amount FROM input
  - id: business-rules
    type: sql
    query: SELECT * FROM input WHERE category <> 'internal'
```

## A constraint worth knowing

v1 SQL semantics are **per-batch**: row-wise filters, projections, and
derivations behave identically to whole-stream execution, but a
`GROUP BY` would aggregate within each micro-batch, not across the whole
input. Cross-batch aggregation is a planned engine feature — until then,
aggregate in the destination database, which is very good at it.
