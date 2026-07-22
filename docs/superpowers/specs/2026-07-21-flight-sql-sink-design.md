# Flight SQL sink (E1.2) — design

Date: 2026-07-21  
Status: approved (approach 1: real `arrow-flight` in default binary)  
Task: E1.2

## Goal

Ship an append-only Flight SQL sink beside native PostgreSQL COPY, using a
real `arrow-flight` 56 client (matching workspace Arrow), with L1 tests
against an in-process mock server. Secrets stay in env vars.

## Spec

```yaml
type: flightSql
endpoint: "http://127.0.0.1:50051"   # http or https
target: schema.table                 # catalog.schema.table also accepted (2–3 parts)
mode: append                         # upsert rejected at validate
tokenEnv: PRAMEN_FLIGHT_SQL_TOKEN    # optional; unset/empty = no auth header
```

Mutually exclusive with other sink fields via existing `SinkSpec` tagged enum.
Fan-out (`spec.sinks`) works unchanged (ADR 0007).

## Runtime

`FlightSqlSink` in `pramen-io`:

1. `connect` — parse endpoint, read optional bearer token from `tokenEnv`.
2. `write` — buffer `RecordBatch`es (no network until commit).
3. `commit` — open tonic channel, `FlightSqlServiceClient`, optional
   `Authorization: Bearer …`, bulk-append via Flight SQL
   `CommandStatementIngest` (fallback documented if 56 lacks ingest: prepared
   `INSERT` + DoPut of Arrow IPC), drain PutResult stream, then drop buffers.

Failed runs never call `commit` → target unchanged (same contract as COPY).

## Dependencies

- Workspace: `arrow-flight = { version = "56", default-features = false, features = ["flight-sql"] }`
- Transitive `tonic` / prost — license-check via `cargo deny`
- Default binary includes the sink (no Cargo feature gate)

## Tests

- Spec: parse; reject `mode: upsert`; reject empty endpoint/target; schema regen
- L1: mock Flight SQL-capable server in-process; sink writes N rows; assert
  server received matching schema + row count; cancel-before-commit leaves
  server empty
- No Docker / cloud in PR CI

## ADR

ADR 0008: append-only Flight SQL in the default binary; reopen if binary size
or static-link story regresses beyond an accepted threshold, or when upsert
is demanded.

## Out of scope

Upsert, ADBC (E1.1), mTLS/OAuth matrix, live warehouse acceptance, E1.4
connector matrix.
