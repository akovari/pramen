# ADR 0001: Native PostgreSQL COPY in v1; ADBC rejected until warehouse expansion

Status: accepted  
Date: 2026-07-10  
Task: S1.3, P1.4 (v1 sink); E1.1 (ADBC return path)

## Goal metric

One dependency-free static binary per tier-1 target, and v1 sink throughput
within 10% of the best available bulk path into PostgreSQL.

## Options considered

1. ADBC PostgreSQL driver — one API for many future warehouses, Arrow-native
   ingestion, but the driver is a native C/C++ library that breaks static
   distribution and forces container/driver-directory packaging in v1.
2. Native pure-Rust `COPY FROM STDIN BINARY` (tokio-postgres) — serves local
   PostgreSQL, Aurora, and RDS identically; keeps the single static binary;
   Arrow-to-COPY encoding under Pramen's control; PostgreSQL-only.

## Measurement

Design-phase analysis (architecture §10, packaging §10); throughput and type
matrix to be confirmed by spike S1.3 with a ≥90%-of-`psql \copy` exit bar.

## Decision

Option 2 for v1. ADBC is not abandoned: it returns in Phase 3 (E1.1) as a
separate distribution profile when multi-warehouse sinks arrive, which is the
problem ADBC exists to solve.

## Reopen triggers

- A maintained, pure-Rust ADBC PostgreSQL driver reaches feature parity.
- Spike S1.3 misses the throughput bar or the type matrix proves untenable.
- A second sink family is demanded before Phase 3.
