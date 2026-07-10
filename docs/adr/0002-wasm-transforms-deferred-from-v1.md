# ADR 0002: WASM component transforms deferred from v1; SQL/expressions ship first

Status: accepted (deferral, not rejection)  
Date: 2026-07-10  
Task: S1.4 (gating spike); X1.1–X1.4 (delivery)

## Goal metric

Time from binary download to first enriched rows in PostgreSQL under ten
minutes, with no user toolchain required.

## Options considered

1. WASM components in v1 — polyglot, sandboxed user transforms from day one,
   but every early user needs a component toolchain, and the ABI work delays
   the semantic-layer differentiators.
2. DataFusion SQL/expression transforms in v1, WASM at the first post-v1
   milestone — zero-toolchain onboarding; covers selection, filtering,
   normalization, derivation for the target business cases; WASM ABI design
   (architecture §8) unchanged, only re-sequenced.

## Measurement

Design-phase analysis: the v1 business cases require no user-defined native
logic beyond SQL expressions. IPC overhead and limit behavior to be measured
by spike S1.4 before Phase 2 commits to the ABI.

## Decision

Option 2. `pramen-wasm` exists as a placeholder crate; the pipeline schema
reserves `type: wasm` so the milestone lands without a spec break.

## Reopen triggers

- A concrete v1 user workload that SQL/expressions cannot express.
- Spike S1.4 reveals ABI decisions that would force a v1 spec break later.
