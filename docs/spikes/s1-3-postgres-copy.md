# Spike S1.3 — native Arrow → PostgreSQL `COPY BINARY`

Status: complete. Validates ADR 0001 (native Postgres COPY, no ADBC in v1).

## What was built

`spikes/s1-3-postgres-copy` — a standalone crate that:

- encodes Arrow `RecordBatch`es directly into the PostgreSQL binary COPY
  wire format (`src/encoder.rs`): 19-byte header, per-tuple field counts,
  big-endian per-field lengths and payloads, `-1` trailer;
- covers the v1 type-matrix subset exercised here: `Int64`, `Float64`,
  `Utf8`, `Boolean`, `Timestamp(µs, UTC)` → `timestamptz`, plus NULLs
  (encoder also supports `Int32`);
- streams 2 MiB chunks through `tokio-postgres` `copy_in`;
- verifies row count and sampled values (float, text, bool, NULL-pattern)
  after each load;
- produces the identical dataset as CSV for the conventional baseline.

## Measurement

5,000,000 rows x 6 columns (~100 bytes/row wire), PostgreSQL 17 in Docker
(tmpfs data dir to remove disk noise), Apple Silicon macOS, single
connection. Timing includes inline Arrow batch generation.

| Path | Wall time | Rows/s | Notes |
| --- | --- | --- | --- |
| Rust binary COPY (this spike) | 13.6 s (warm), 16.3 s (cold) | 367k / 307k | 37 MiB/s wire, values verified |
| `psql \copy ... format csv` | 42.8 s | 117k | identical data, pre-generated CSV file |

Binary COPY is **3.1x faster** than the `psql \copy` CSV baseline. Exit
criterion was ≥90% of the baseline; exceeded by a wide margin. Most of the
gap is server-side CSV parsing plus text→binary conversion that the binary
format avoids.

## Conclusions

- ADR 0001 confirmed: a pure-Rust binary COPY sink is both the static-binary
  option and the fastest option; no need to reopen ADBC for v1.
- The encoder is small (~100 lines for six types); extending to the full v1
  matrix (int32/date/jsonb/uuid/numeric) is incremental work in P1.13, with
  `numeric` the only nontrivial encoding.
- Timestamps need the 2000-01-01 PG epoch offset; covered by a value-level
  round-trip check in the spike and must be property-tested in P1.13.
- Chunked `copy_in` with 2 MiB frames was sufficient to saturate; no
  benefit expected from larger frames at this row width.

## Follow-ups feeding P1.13 (Postgres sink)

- Idempotent commit strategies (staging table + merge) on top of this path.
- Full type matrix + property tests against a testcontainers Postgres (L2).
- Multi-connection parallel COPY only if a real workload shows the single
  connection as the bottleneck (measure first).
