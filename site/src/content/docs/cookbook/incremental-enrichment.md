---
title: Incremental re-enrichment
description: How content-addressed work keys turn recurring AI runs into pay-only-for-changes runs.
---

The two mechanisms this recipe relies on are both live: the inference
ledger reuses every recorded result by content-addressed work key, and
[checkpointing](/pramen/concepts/runtime/#checkpoints-and-incremental-runs)
skips already-loaded source files entirely.

## The problem

Enrichment is rarely a one-shot backfill. Datasets grow, records get
corrected, prompts improve. Naive pipelines re-run the model over
everything, every time — the invoice scales with dataset size instead of
change size.

## How Pramen approaches it

Every unit of semantic work has a **work key**: a content hash over the
canonicalized input values, instruction, model identity, output schema,
and prompt revision. The ledger maps work keys to validated results.

On every run, for every record:

| Situation | What happens | What it costs |
| --- | --- | --- |
| Same inputs, same prompt/model | Ledger hit — result reused | nothing |
| New record | Ledger miss — dispatched | one inference |
| Record's input text changed | Different work key — dispatched | one inference |
| Prompt revision bumped | All affected keys change — re-dispatched | full re-enrichment, on purpose |
| Model changed | Same as prompt change | full re-enrichment, on purpose |

There is no bookkeeping for you to write: no watermark columns, no
"last processed at" state to corrupt, no manual diffing. Identity is
content, so correctness survives crashes, retries, and out-of-order
processing.

## Operational patterns this enables

**The daily enrichment run.** Schedule the same pipeline daily over the
full (growing) dataset. With `runtime.checkpoint` set, already-loaded
files are not even read again; new files' records that duplicate old
content still reuse ledger results. Cost tracks new records only.
Duplicate-heavy sources (retries, CDC replays) deduplicate for free —
identical content is one work key. The formal reuse contract and measured savings are in [RQ2 memoization](https://github.com/akovari/pramen/blob/main/docs/research/rq2-memoization.md).

**Prompt iteration with a controlled blast radius.** The prompt revision is
part of the key, so improving an instruction and re-running re-executes
exactly that transform's work — other `ai.*` steps in the pipeline keep
their cached results.

**Backfill + steady state as one pipeline.** There is no separate backfill
mode. The first run pays for everything; every later run pays for the
delta. The `--smoke` cap makes the first run's cost predictable too.
