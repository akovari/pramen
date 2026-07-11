---
title: Design decisions
description: Architecture Decision Records — goal metrics, options, measurements, and reopen triggers.
---

Every significant decision is recorded as an ADR in
[`docs/adr/`](https://github.com/akovari/pramen/tree/main/docs/adr). An ADR
states the **goal metric**, the options considered, the **measurement**
that discriminated between them, and explicit **reopen triggers** — the
future conditions under which the decision should be revisited. "We
preferred X" without a number is not a completed decision.

## Current ADRs

| ADR | Decision | Discriminating evidence |
| --- | --- | --- |
| [0001](https://github.com/akovari/pramen/blob/main/docs/adr/0001-native-postgres-copy-not-adbc-in-v1.md) | Native pure-Rust `COPY`, not ADBC, for the v1 sink | Static binary requirement; confirmed by S1.3: 3.1× the `psql \copy` baseline |
| [0002](https://github.com/akovari/pramen/blob/main/docs/adr/0002-wasm-transforms-deferred-from-v1.md) | WASM transforms deferred from v1; SQL/expressions first | Ten-minute onboarding goal; WASM remains the committed extension mechanism |
| [0003](https://github.com/akovari/pramen/blob/main/docs/adr/0003-sqlite-wal-inference-ledger.md) | SQLite (WAL) for the v1 inference ledger | S1.1: zero lost results across crashes, µs-level overhead |
| [0004](https://github.com/akovari/pramen/blob/main/docs/adr/0004-windows-tier-one-target.md) | Windows x86_64 is a blocking tier-1 CI target | Product decision; identical gates on all four targets |
| [0005](https://github.com/akovari/pramen/blob/main/docs/adr/0005-local-first-testing-strategy.md) | Local-first, four-layer testing (mocks → protocol stubs → local services → weekly cloud) | PR gates run with zero cloud access and zero credentials |

## Vocabulary

The project maintains a
[controlled vocabulary](https://github.com/akovari/pramen/blob/main/docs/vocabulary.md)
— one term per concept (*work key*, *recorded result*, *semantic
transform*, *reconciliation*, …) used consistently across code, docs, and
issues, with forbidden synonyms listed. It keeps long-lived discussions
and the eventual paper precise.
