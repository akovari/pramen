# ADR 0010: Defer ADBC / first-warehouse sink (E1.1) under free-only budget

Status: accepted  
Date: 2026-07-22  
Task: E1.1  
Supersedes in part: reopen path from [ADR 0001](0001-native-postgres-copy-not-adbc-in-v1.md)  
Related: [ADR 0009](0009-local-only-ollama-acceptance.md) (no paid subscriptions)

## Goal metric

Ship multi-warehouse ADBC sinks only when (a) a concrete first warehouse is
chosen with a **free or already-licensed** local/dev path for CI and
acceptance, and (b) packaging stays honest about the lean static binary vs
the driver-container profile (architecture §10). Success is not “ADBC
exists,” it is a dated support-matrix row + L1/L2 tests without paid cloud.

## Options considered

1. **Implement ADBC now against a paid warehouse** (Snowflake / BigQuery /
   Databricks / …) — contradicts ADR 0009 budget; needs credentials and a
   first-warehouse ADR naming demand.
2. **Implement ADBC against a free local target** (e.g. DuckDB ADBC, or a
   second path into local PostgreSQL via ADBC) — possible, but duplicates
   native COPY for Postgres (ADR 0001) and does not unlock multi-warehouse
   demand; packaging cost of native drivers remains.
3. **Defer E1.1** until user demand names a warehouse *and* a free/local
   acceptance path exists (or a funded paid budget reopens ADR 0009) —
   keeps the lean binary story; Flight SQL + Postgres COPY already cover
   Phase 3 product sinks that are free-local testable.

## Measurement

- Current sinks with free local acceptance: Postgres COPY, object-store
  sources, Flight SQL mock (ADR 0008), connector inspect/matrix (E1.4).
- No free CI-friendly ADBC warehouse driver set is adopted in-tree today.
- Architecture still lists ADBC as expansion-phase packaging, not lean
  default.

## Decision

**Option 3.** E1.1 is **deferred / not planned** under the current
no-subscription budget. Document the deferral here; keep ADR 0001’s reopen
triggers plus:

- A written first-warehouse choice (who asked, which engine, which free
  or paid acceptance path).
- An ADR amending packaging if the lean binary promise changes.

## Reopen triggers

- Named warehouse demand with a free local (or funded) acceptance path.
- A maintained pure-Rust ADBC driver removes the container-driver profile
  objection (also an ADR 0001 reopen).
- ADR 0009 reopened with a funded cloud budget that includes warehouse
  credentials for L3 acceptance.
