# ADR 0007: Fan-out commit barrier (all sinks, then checkpoint)

Status: accepted  
Date: 2026-07-21  
Task: E1.3

## Goal metric

Correctness under crash between sinks: after a successful run that fans out
to **N** sinks, either **all** sinks have committed durable output for the
consumed work units **or** the checkpoint marker has **not** advanced (so a
replay redelivers those units). Target: zero observed cases where a checkpoint
advances after only a subset of sinks committed, under the fault-injection
suite (kill after k-of-N commits).

Secondary: asymmetric sink speed must not unbounded-buffer the faster branch
(bounded channels; backpressure propagates to the shared producer).

## Options considered

1. **All-sinks-commit-then-checkpoint** — every sink finishes its write
   phase, then every sink `commit`s, then the checkpoint marker advances
   (architecture §11 ordering extended to N sinks). Fan-out via optional
   `from` on transforms/sinks; **fan-in rejected** in v1alpha1.
2. **Per-sink checkpoints** — independent markers per sink; replays can
   skip already-committed branches. More complex store schema; changes ADR
   0006’s single-marker model.
3. **Two-phase / staging commit** — prepare on all sinks, then commit; or
   stage locally until all succeed. Strongest atomicity; highest sink
   coupling and new failure modes.

## Measurement

Design-phase analysis against architecture §11 and ADR 0006 (at-least-once
file checkpoints). Option 2 changes the checkpoint contract (reopen ADR
0006). Option 3 adds sink capability requirements not present on today’s
Postgres COPY sink. Option 1 preserves a single marker and matches the
existing “sink commits → marker advances” story with an explicit N-sink
barrier. Runtime tests pin: identical rows on two collecting sinks;
failure on one sink cancels before any commit and before checkpoint;
bounded-channel backpressure under a held sink.

## Decision

Choose **option 1**. Spec changes (additive):

- Transforms and sinks may declare `from` (stage id or `source`). Omitted
  `from` preserves today’s linear default (previous transform, or `source`
  for the first transform / sole sink).
- `spec.sink` remains valid for a single sink. `spec.sinks` (each with
  `id` + optional `from` + sink fields) enables multi-sink fan-out.
  Exactly one of `sink` / non-empty `sinks` must be present.
- Fan-in (two producers into one stage) is a validation error until a later
  ADR.

Runtime: clone/`RecordBatch` tee across fan-out edges; sink tasks complete
writes; **barrier**; then all `commit`s; then checkpoint completion.

## Reopen triggers

- A production sink cannot participate in a post-write commit barrier
  (needs prepare/commit split or per-sink markers).
- Fan-in, joins, or multi-source pipelines become a product requirement.
- Heterogeneous schemas per branch (today every branch carries the same
  Arrow schema from its producer).
- Exactly-once multi-sink delivery is required (would force option 2 or 3).
