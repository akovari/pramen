---
title: Measured results
description: Every performance claim, with its methodology and raw numbers.
---

Pramen's development rule: **no performance claim without a measurement**,
and every measurement gets a report with machine details and methodology in
[`docs/spikes/`](https://github.com/akovari/pramen/tree/main/docs/spikes).
These are the headline results so far. All numbers below are from an Apple
Silicon laptop; treat them as relative evidence, not absolute promises —
the formal benchmark suite (P1.20) will publish reproducible baselines.

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
