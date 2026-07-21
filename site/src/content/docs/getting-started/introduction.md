---
title: Introduction
description: What Pramen is, what it is for, and what it deliberately is not.
---

Pramen (Czech for *spring* — a source of water) is a data movement and
transformation runtime built around three commitments:

1. **Columnar and bounded.** Data moves as Apache Arrow record batches
   through bounded channels. A slow destination applies backpressure all the
   way to the source, so peak memory does not grow with input size.
2. **Governed LLM enrichment.** Semantic transforms (`ai.extract`,
   `ai.classify`, `ai.generate`) are schema-bound operations with hard
   budgets, strict output validation, and a durable, content-addressed
   inference ledger.
   Completed model calls are never re-billed — not after a crash, not on a
   re-run, not on a partial replay.
3. **Radically simple operations.** One static binary with zero native
   driver dependencies. A pipeline is one YAML file. The v1 promise is
   measurable: download the binary, write the file, and have enriched rows
   in PostgreSQL in under ten minutes.

## Who it is for

Platform and data teams moving data from object storage into operational
databases — especially when some columns need a language model to exist at
all (classification, extraction from free text, normalization), and when
that inference has to be auditable, budgeted, and restart-safe.

## What it is not

- **Not a warehouse.** Pramen feeds databases; it does not replace them.
  If your data already lives in a warehouse with AI SQL functions and the
  results stay there, use those.
- **Not a streaming platform.** There are no topics, consumer groups, or
  cluster coordinators. Pramen scales through independent workers first.
- **Not an agent framework.** Semantic transforms have fixed inputs, fixed
  instructions, typed outputs, and no tools or loops. That constraint is
  what makes governance enforceable.

## Current status

Pramen is in early implementation and moving fast. Deterministic pipelines
(Parquet → SQL → PostgreSQL) run end to end today; semantic transforms,
Provider-batch execution, upsert sinks, and Azure/GCS are in active
development.
The [status and roadmap](/pramen/project/roadmap/) page tracks exactly what
works now, and every performance claim on this site links to a
[measured result](/pramen/project/benchmarks/).
