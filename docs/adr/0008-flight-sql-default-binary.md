# ADR 0008: Flight SQL sink in the default binary (append-only)

Status: accepted  
Date: 2026-07-21  
Task: E1.2

## Goal metric

A `type: flightSql` sink that can append Arrow batches to a Flight SQL
endpoint over gRPC, covered by an offline L1 mock-server test, without
breaking the single-binary distribution story on tier-1 targets
(compile + `cargo deny` green). Reopen if release binary size grows by
more than **15%** versus the prior release solely from this dependency,
measured on Linux x86_64 `dist` profile.

## Options considered

1. **Real `arrow-flight` 56 + tonic in the default binary** — production
   protocol path; L1 mock server; larger binary.
2. **Cargo feature `flight-sql`** — lean default binary; dual CI matrix.
3. **Transport stub only** — no real protocol until a follow-up.

## Measurement

Design-phase packaging analysis (architecture §10, ADR 0001 spirit) plus
post-merge `dist` binary size check on Linux. Option 2 adds CI complexity
for a Phase 3 expansion sink; option 3 delays the task’s definition of
done. Option 1 matches the approved E1.2 design.

## Decision

Choose **option 1**. Append-only only; upsert rejected at validate.
Bearer token via `tokenEnv` (default `PRAMEN_FLIGHT_SQL_TOKEN`). Bulk
path prefers Flight SQL `CommandStatementIngest` when available in
arrow-flight 56.

## Reopen triggers

- Linux `dist` binary grows >15% attributable to this change.
- Static musl link fails because of tonic/openssl-style deps (must stay
  rustls).
- Product demand for upsert / idempotent Flight SQL merge.
- ADBC profile (E1.1) subsumes this path for the same destinations.
