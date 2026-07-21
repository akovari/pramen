---
title: Measured results
description: Every performance claim, with its methodology and raw numbers.
---

Pramen's development rule: **no performance claim without a measurement**,
and every measurement gets a report with machine details and methodology —
spike reports in
[`docs/spikes/`](https://github.com/akovari/pramen/tree/main/docs/spikes),
benchmark-suite reports in
[`docs/benchmarks/`](https://github.com/akovari/pramen/tree/main/docs/benchmarks).
For buyer-oriented positioning against alternatives, see
[Compared to alternatives](/pramen/project/comparison/) (orientation +
generated scoreboard). Regenerate offline research figures with
`mise run reproduce` ([AE checklist](https://github.com/akovari/pramen/blob/main/docs/research/artifact-evaluation.md)).
These are the headline results so far. All numbers below are
from an Apple Silicon laptop; treat them as relative evidence, not absolute
promises.

## Benchmark suite v1 (reproducible)

The suite (`scripts/bench.sh`) generates its input deterministically —
5M rows, six-type column mix, 0.302 GiB of Snappy Parquet — and runs the
same projection + derivation + filter (4M rows out) through four paths,
measuring wall time, CPU seconds, and peak RSS with `/usr/bin/time`
(ranges over three runs in one session):

| Path | Wall time | Rows out/s | CPU s | Peak RSS |
| --- | --- | --- | --- | --- |
| **Pramen end-to-end → PostgreSQL** | 6.9–9.2 s | 434k–581k | 1.2–1.4 | ~0.5 GiB |
| DuckDB → PostgreSQL (its `postgres` extension, same query, same server) | 6.5–9.9 s | 403k–620k | 6.9–10.9 | ~45 MiB |
| DataFusion direct (same SQL, no sink) | 1.0 s | 4.0M | 0.4 | 361 MiB |
| DuckDB (same SQL → native table, no PostgreSQL) | 0.5 s | 8.2M | 2.1 | 689 MiB |

Reading: on the like-for-like PostgreSQL load, **wall time is a tie
dominated by the server** (run-to-run WAL/checkpoint variance exceeds
the tool difference), while **Pramen does the same job on ~7× less
CPU**. DuckDB streams this leg at an excellent ~45 MiB RSS; Pramen holds
~0.5 GiB (bounded channels + transactional COPY buffering) — both flat
in input size. Pramen's transform layer *is* DataFusion, so the gap to
the engine ceiling is the load path; the encoder itself sustains
5.6–6.5M rows/s in isolation (Criterion), at most ~10% of that budget.
([full report with per-run numbers and caveats](https://github.com/akovari/pramen/blob/main/docs/benchmarks/2026-07-12-v1.md))

## Memoization savings (RQ2 / E2.2)

Offline suite (`scripts/rq2-memoization.sh`, mock provider + SQLite ledger)
pins the reuse contract with published JSON under
[`docs/research/`](https://github.com/akovari/pramen/tree/main/docs/research):

| Scenario | Headline |
| --- | --- |
| Crash/replay (online) | 100% result reuse; 0 tokens billed on replay |
| Crash/reconcile (batch) | 0 rebill calls after submit-then-crash |
| Incremental re-enrichment | Only changed + new keys re-billed (10/45 in the fixture) |
| Duplicate-heavy (200 rows / 20 unique) | 90% savings vs naive per-row dispatch |

Full contract and methodology:
[`docs/research/rq2-memoization.md`](https://github.com/akovari/pramen/blob/main/docs/research/rq2-memoization.md).

## Governance overhead per semantic record

Criterion micro-benches (`cargo bench -p pramen-ai`): canonicalize + hash
a full work specification in **44 µs**; record a completed result in the
fsync'd WAL ledger in **715 µs**; reuse a recorded result in **5 µs**.
The entire governance fixed cost is under a millisecond — reusing a
governed result is ~5,000× cheaper than repeating a 250 ms model call.

## PostgreSQL loading: binary COPY vs `psql \copy`

5,000,000 rows × 6 columns (int64, float64, text, bool, timestamptz,
nullable text), PostgreSQL 17, single connection, identical data both
paths.

| Path | Wall time | Rows/s |
| --- | --- | --- |
| **Pramen binary `COPY` encoder** | **13.6 s** | **367,000** |
| `psql \copy` from CSV | 42.8 s | 117,000 |

**3.1× faster.** The gap is server-side CSV parsing and text→binary
conversion that the binary protocol never performs.
([full report](https://github.com/akovari/pramen/blob/main/docs/spikes/s1-3-postgres-copy.md))

## Bounded-memory SQL over Parquet

Filter + projection + derivation over multi-file Snappy Parquet through
DataFusion streaming execution with a hard memory pool.

| Dataset | Pool limit | Peak RSS | Output throughput |
| --- | --- | --- | --- |
| 8M rows / 175 MiB | 512 MiB | 183 MiB | 1.68M rows/s |
| 16M rows / 351 MiB | 512 MiB | 184 MiB | 2.99M rows/s |
| 16M rows / 351 MiB | 256 MiB | 185 MiB | 2.73M rows/s |

**Doubling the input moved peak memory by ~1 MiB.** Memory is a function
of configuration, not dataset size.
([full report](https://github.com/akovari/pramen/blob/main/docs/spikes/s1-2-parquet-datafusion.md))

## Inference ledger overhead

SQLite (WAL mode) content-addressed ledger, measured at 10k and 100k work
items.

| Operation | Cost per work item |
| --- | --- |
| Cold path (record new result) | 205–312 µs |
| Warm path (reuse existing result) | 27–44 µs |

Noise next to any model call (tens of ms to seconds). Crash tests: process
killed mid-run loses **zero** completed results; replay of a completed run
reuses **100%** of them.
([full report](https://github.com/akovari/pramen/blob/main/docs/spikes/s1-1-ledger-bedrock.md))

## End-to-end vertical

The quickstart pipeline (Parquet → SQL filter/derivation → transactional
COPY into PostgreSQL): 200,000 rows in, 160,000 rows out in **1.6 s**
including connection setup and commit — on a debug build.
