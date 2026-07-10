# Pramen — Agent and Contributor Guide

Pramen is a Rust data movement and transformation runtime with governed,
schema-bound LLM enrichment. Design phase artifacts:

- `docs/architecture.md` — the design authority; do not contradict it silently.
- `docs/implementation-plan.md` — the task list; pick tasks by ID, respect
  group ordering and dependencies.
- `docs/adr/` — decision records; `docs/vocabulary.md` — controlled terms.

## Working protocol

- One task ID = one branch (`cursor/<task-id>-<slug>`) = one PR.
- A change that alters a decision in `docs/architecture.md` §17 requires an
  ADR in the same PR, stating goal metric, options, measurement, and reopen
  triggers. Decisions without a number are not decisions.
- Spike code goes in `spikes/` and is disposable; spike *reports* go in
  `docs/spikes/` and are permanent. Never import spike code from production
  crates.
- Update `docs/implementation-plan.md` task status in the PR that completes
  the task.

## Commands (once the workspace exists — task T1.1/T1.2)

Everything runs through mise; CI runs the same tasks.

- `mise run ci` — full local gate (fmt, clippy, deny, nextest, doc tests)
- `mise run test` / `mise run cov` — tests / coverage with ratchet check
- `mise run bench` — criterion benches against stored baselines

## Code standards

- Pinned stable toolchain (`rust-toolchain.toml`), edition 2024.
- `#![forbid(unsafe_code)]`; `clippy -D warnings`; `#![deny(missing_docs)]`
  on library crates.
- No `unwrap`/`expect` outside tests; errors are typed and documented.
- Secrets never appear in normalized plans, logs, or snapshots.
- Tests: nextest; insta for CLI/plan output; proptest for canonicalization
  and encoders; testcontainers (Postgres) and MinIO for integration; wiremock
  or recorded fixtures for providers — PRs must pass offline; real-provider
  tests run in the scheduled weekly workflow under budget alarms.
- Conventional commits.

## Vocabulary discipline

Use the terms from `docs/vocabulary.md` exactly (work unit, work item,
recorded result, semantic transform, review routing). If a needed term is
missing, add it in the same PR rather than improvising a synonym.

## Performance and cost discipline

- Perf-relevant PRs include or update a criterion bench; >5% regression on
  designated benches fails CI and needs an ADR to accept.
- Any code path that can call a paid model must enforce budget ceilings
  before dispatch and must be covered by a test with a mock provider.
