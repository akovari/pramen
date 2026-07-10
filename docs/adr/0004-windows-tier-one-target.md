# ADR 0004: Windows x86_64 is a blocking tier-1 target from the start

Status: accepted  
Date: 2026-07-10  
Task: T1.3 (CI matrix), T1.4 (release targets)

## Goal metric

Identical CI gates on Linux x86_64/aarch64, macOS aarch64, and Windows
x86_64 (msvc) for every PR; release binaries for all four targets.

## Options considered

1. Windows as tier-2 (build + test, non-blocking) until user demand.
2. Windows as blocking tier-1 from the start.

## Measurement

Product decision by the project owner (2026-07-10), accepting the CI cost.
Caveat recorded: container-backed integration tests (Postgres, MinIO) run on
Linux runners only; Windows runs the full unit and non-container suite.

## Decision

Option 2. Windows-specific breakage blocks merges immediately rather than
accumulating.

## Reopen triggers

- Windows CI flakiness or duration materially slows the team; demotion would
  need its own ADR with measurements.
