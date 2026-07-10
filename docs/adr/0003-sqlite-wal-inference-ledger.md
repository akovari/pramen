# ADR 0003: SQLite (WAL mode) as the first durable inference ledger

Status: accepted  
Date: 2026-07-10  
Task: S1.1 (validation spike); P1.6 (productionization); X1.8 (shared backend)

## Goal metric

Zero completed inference results lost across kill -9 at any point; ledger
overhead small relative to model-call latency (measured per work item at 10k
and 100k items in S1.1).

## Options considered

1. SQLite in WAL mode — embedded, zero-ops, transactional, fits the
   single-binary story; single-writer, single-node.
2. Postgres-backed ledger from day one — shared across a fleet, but adds a
   required external service to the lean profile and contradicts the
   ten-minute promise.
3. Append-only log files + index — no dependency, but reinvents transactional
   semantics the ledger must not get wrong.

## Measurement

Design-phase analysis; durability and overhead exit bars measured by spike
S1.1. The ledger interface is written against a trait so the Postgres
backend (X1.8) is an implementation, not a redesign.

## Decision

Option 1 for v1, with the backend trait boundary fixed in P1.6.

## Reopen triggers

- S1.1 overhead numbers materially change pipeline throughput.
- Fleet deployments need shared reuse before X1.8 is scheduled.
- Work-item write concurrency exceeds what a single SQLite writer sustains.
