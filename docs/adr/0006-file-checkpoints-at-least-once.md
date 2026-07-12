# ADR 0006: File-granular checkpoints and the at-least-once delivery contract

Status: accepted  
Date: 2026-07-11  
Task: P1.3 (checkpoint store), P1.14 (delivery semantics)

## Goal metric

A crashed or repeated run never silently re-loads completed source files,
and never silently loses claimed-but-unfinished work: replaying a finished
run loads zero rows; adding one file to the source loads exactly that file.
The checkpoint store itself must survive `kill -9` during any append with
at most one torn (and detectable) record.

## Options considered

1. **No checkpointing in v1** — every run re-reads the whole source. Simple,
   but incremental ingestion (the dominant production pattern: a growing
   object prefix) re-pays the full load and, with `append` sinks, duplicates
   data on every run.
2. **Row- or batch-granular offsets** (Kafka-style) — finest resumption,
   but requires ordered, seekable sources and a transactional handshake with
   the sink per batch. Wrong shape for immutable object stores, where the
   natural unit is the object.
3. **File-granular work units on an append-only log** (chosen) — the
   checkpoint unit is one immutable source file, identified by path + size +
   mtime, recorded in an fsync'd JSONL log behind a `CheckpointStore` trait.
4. **SQLite for the checkpoint store** — same durability, heavier machinery
   than needed for a monotonic claim/complete log; unlike the inference
   ledger there is no read-back-per-record hot path. The trait keeps this
   swappable (and X1.8's Postgres backend will use it).

## Decision

- One immutable source file = one work unit (architecture §11). Unit
  identity is `SHA-256(pipeline, url, size, mtime)`: a rewritten file is new
  work; an untouched file is never re-read.
- `FileCheckpointStore`: append-only `checkpoints.jsonl` in the configured
  directory, one fsync per record. Replay on open; a torn *final* line (the
  crash-during-append signature) is discarded and truncated; a torn line
  anywhere else is corruption and fails loudly.
- Protocol per run: enumerate units → skip completed → durably **claim**
  pending units → read/transform/load → sink commit → durably **complete**.
  Claims are advisory in the single-process runtime (a stale claim is
  re-claimed on the next run) but keep the protocol coordinator-ready.
- The v1 sink commits one transaction per run, so completions are written
  only after the data is visible.

## Delivery contract (at-least-once)

The crash window is between sink commit (step 4) and completion marking
(step 5): a crash exactly there re-loads those units on the next run. With
`mode: append` this duplicates rows; that is the documented at-least-once
contract. Mitigations:

- `mode: upsert` (P1.4) makes replays idempotent on the declared merge
  keys — staged in a session-local temp table, merged with
  `INSERT … ON CONFLICT`, last write wins deterministically within a run;
- semantic (`ai.*`) work is *already* exactly-once-billed regardless, via
  the inference ledger — duplicated loads reuse recorded results.

Both sides are pinned by L2 tests (`delivery_contract_append_duplicates_
upsert_does_not`) and verified end to end, including the simulated
commit-then-crash window with an upsert sink (exact row counts on replay).

Never claimed: exactly-once delivery from checkpointing input positions
alone (architecture §10 explicitly rejects that claim).

## Measurement

Behavioral tests in `pramen-core::checkpoint` cover: identity change on
rewrite/touch, durability across reopen, idempotent completion, torn-final-
line recovery with truncation, and mid-log corruption refusing to open.
End-to-end verified: run → replay (`nothing to do`, 0 rows) → add one file
→ run (only new rows) → target row count exact with zero duplicates.

## Reopen triggers

- Multi-process execution needs real leases: revisit claims (lease expiry,
  fencing tokens) and move to the shared Postgres backend (X1.8).
- Sources without stable mtime (some object stores) — switch the identity
  component to etag/version ID when remote enumeration lands (P1.1).
- Sub-file splits (huge single files) — extend `WorkUnit` with a split
  field; the key derivation already leaves room.
