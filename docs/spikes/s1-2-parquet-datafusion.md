# Spike S1.2 — Parquet source + DataFusion SQL under bounded memory

Status: complete. Validates the v1 deterministic-transform choice
(SQL/expressions via DataFusion, ADR 0002) and the P1.10/P1.12 design.

## What was built

`spikes/s1-2-parquet-datafusion` — a standalone crate that:

- generates a deterministic multi-file Snappy Parquet dataset (5 columns:
  int64, float64, utf8, timestamptz, variable-width utf8 payload);
- registers it in a DataFusion `SessionContext` configured with a
  `FairSpillPool` hard memory limit and 8192-row output batches;
- streams a representative query (filter + projection + arithmetic
  derivation + `date_trunc` + `length`) via `execute_stream`, consuming
  batches as a Pramen operator would;
- samples peak RSS during the stream.

## Measurement

Apple Silicon macOS, release build. Query returns 40% of input rows.

| Dataset | Pool limit | Peak RSS | Throughput |
| --- | --- | --- | --- |
| 8M rows / 175 MiB parquet | 512 MiB | 183 MiB | 1.68M rows/s out, 92 MiB/s scan |
| 16M rows / 351 MiB parquet | 512 MiB | 184 MiB | 2.99M rows/s out, 164 MiB/s scan |
| 16M rows / 351 MiB parquet | 256 MiB | 185 MiB | 2.73M rows/s out, 150 MiB/s scan |

Exit criterion — peak memory must not scale with input size — met: doubling
the dataset moved peak RSS by ~1 MiB. Streaming execution never approached
the pool limit for this pipeline shape (no blocking operators); the pool
matters once aggregations/joins appear, and `FairSpillPool` is the right
default for those.

## Conclusions

- DataFusion `execute_stream` + `FairSpillPool` gives exactly the bounded
  pipeline execution model the architecture requires; no custom operator
  work needed for v1 SQL transforms.
- The `input` table naming convention from the spec maps directly to
  `register_parquet`/`register_table` per transform stage.
- Throughput (~3M rows/s for filter+project on a laptop) is far above the
  v1 targets; the Postgres sink (S1.3: 367k rows/s) will be the bottleneck,
  which is the expected and acceptable shape.
- DataFusion is a heavy dependency (adds minutes to cold builds). It goes
  in `pramen-io`/engine, not `pramen-core`, to keep core cheap to iterate.

## Follow-ups feeding Phase 1

- P1.10: wire `SessionContext` per pipeline with the spec's
  `maxInflightBytes` driving the pool size.
- P1.12: NDJSON via DataFusion's JSON reader with the same harness.
- Benchmark harness (T1.7) should adopt this dataset generator for
  regression detection.
