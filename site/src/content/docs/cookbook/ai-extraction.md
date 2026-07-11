---
title: Budgeted AI extraction
description: The committed recipe for schema-bound, budget-capped extraction — shipping with the AI workstream.
---

:::caution[Planned]
The `ai.extract` operator is in active development (workstream P1.5–P1.12).
The pipeline schema below already validates today; execution lands with
the AI workstream. This recipe documents the committed contract so you can
design pipelines against it now.
:::

## The scenario

Support tickets arrive as Parquet with free-text descriptions. You need a
`category` and a `priority` as real typed columns in PostgreSQL, at a cost
you can predict, from a run you can safely restart.

## The pipeline

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: ticket-enrichment
spec:
  models:
    fast:
      provider: bedrock
      model: anthropic.claude-3-haiku-20240307-v1:0
      region: eu-central-1
  source:
    type: object_store
    url: /data/tickets/
    format: { type: parquet }
  transforms:
    # 1. Deterministic cleanup first — cheaper than tokens.
    - id: normalize
      type: sql
      query: >
        SELECT ticket_id, lower(trim(description)) AS description
        FROM input
        WHERE description IS NOT NULL

    # 2. Schema-bound extraction with hard budgets.
    - id: classify
      type: ai.extract
      model: fast
      execution: auto          # let the runtime pick online vs batch pricing
      inputs: [description]
      instruction: >
        Classify the support ticket into a business category
        (billing, technical, account, other) and a priority
        (low, normal, high, urgent).
      output:
        fields:
          - { name: category, type: utf8, nullable: false }
          - { name: priority, type: utf8, nullable: false }
      validation:
        onInvalid: review      # invalid model output goes to review, not your table
      budget:
        maxInputTokensPerRecord: 2048
        maxOutputTokensPerRecord: 64
  sink:
    type: postgres
    target: support.enriched_tickets
```

## What the runtime guarantees

- **Budgets bite before dispatch.** A record whose input exceeds 2048
  tokens is rejected up front — not billed and then complained about.
- **Every completed inference is durable.** Kill the run at any point and
  restart: finished records are reused from the ledger at zero cost.
- **Only valid output reaches the table.** Model output that fails the
  declared schema follows `onInvalid` — here, routed to review instead of
  polluting `support.enriched_tickets`.
- **Re-runs are incremental.** Tomorrow's run over a grown dataset pays
  only for new tickets; changing the instruction re-executes exactly the
  affected work.

## Try it cheaply first

`pramen run --smoke` (planned alongside the operators) runs the same
pipeline with a record cap, the cheapest configured model, and a hard cost
ceiling — real enriched rows for a few cents before you commit to a full
run.
