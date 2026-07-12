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
- `mise run bench-e2e` — end-to-end benchmark suite (`scripts/bench.sh`;
  needs `PRAMEN_POSTGRES_DSN`; publishes reports to `docs/benchmarks/`)
- `mise run perf-gate` — the CI perf-regression gate locally
  (`scripts/perf-gate.sh [base-ref]`: benches the merge-base in a
  worktree, re-benches HEAD, fails on >5% regression at the lower 95%
  CI bound of designated benches)
- `mise run release-quickstart` — P2.2 gate: release binary + measured
  quickstart (`scripts/release-quickstart.sh`; needs `PRAMEN_POSTGRES_DSN`)

## Code standards

- Pinned stable toolchain (`rust-toolchain.toml`), edition 2024.
- `#![forbid(unsafe_code)]`; clippy with `CARGO_BUILD_WARNINGS=deny`
  (cargo-level warning denial, Rust 1.97+); `#![deny(missing_docs)]`
  on library crates.
- No `unwrap`/`expect` outside tests; errors are typed and documented.
- Secrets never appear in normalized plans, logs, or snapshots.
- Tests are local-first (ADR 0005), layered L0–L3: trait mocks with billing
  counters; protocol stubs with recorded fixtures and AWS endpoint override;
  real local services (testcontainers Postgres, MinIO, OpenAI-compatible
  local models, fake batch service); weekly budget-alarmed cloud acceptance.
  PRs must pass with zero cloud access and zero credentials.
- Shared test fixtures live in the dev-only `pramen-testkit` crate: L1
  HTTP stubs (`http::one_shot_raw`, `http::one_shot_json`,
  `http::serve_router`, all capturing requests) and the L2 env guards
  (`env::postgres_dsn()`, `env::s3_url()`). Use them instead of
  hand-rolling `TcpListener` loops or `std::env::var` guards in tests.
- L2 database tests are env-guarded: set `PRAMEN_TEST_POSTGRES_DSN` to run
  them, unset to skip. A machine-local `mise.local.toml` (gitignored) is the
  right place for that variable when a local PostgreSQL is available.
- L2 object-store tests are likewise guarded by `PRAMEN_TEST_S3_URL`
  (e.g. `s3://pramen-test/events/`) with standard `AWS_*` variables pointing
  at a local MinIO (`AWS_ENDPOINT=http://localhost:9000`,
  `AWS_ALLOW_HTTP=true`).
- Runtime environment variables: `PRAMEN_LEDGER_PATH` overrides the
  inference ledger location (default `.pramen/ledger.sqlite`), and
  `PRAMEN_OPENAI_API_KEY` supplies the optional key for `openai-compat`
  models.
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
