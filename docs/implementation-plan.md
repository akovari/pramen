# Pramen Implementation Plan

Status: in execution  
Companion to: [architecture.md](architecture.md)  
Last reviewed: 2026-07-21

## Status snapshot (2026-07-21)

Authoritative per-task state lives in the mirrored GitHub issues; this
snapshot orients a reader without leaving the file.

| Area | State |
| --- | --- |
| Milestone T | T1.1–T1.7 done. T1.7: Criterion benches + `scripts/perf-gate.sh`, a merge-base-vs-HEAD regression gate failing the Perf workflow when the lower 95% CI bound of the change exceeds +5% on designated benches. T1.6: layered strategy per ADR 0005, with the shared fixtures extracted into the dev-only `pramen-testkit` crate (L1 HTTP protocol stubs: one-shot raw/JSON + a routing multi-request server with request capture; uniform L2 env guards for `PRAMEN_TEST_POSTGRES_DSN`/`PRAMEN_TEST_S3_URL`), and a CI job running the env-guarded L2 suite against a PostgreSQL service container. |
| Phase 0 spikes | S1.1 offline legs done (ledger, crash recovery, protocol stubs; live Bedrock leg awaits credentials). S1.2 done: peak RSS flat (~184 MiB) while input doubles, ~3M rows/s. S1.3 done: binary COPY at 3.1x the `psql \copy` baseline, ADR 0001 confirmed. S1.4 done: WIT component round-trips Arrow IPC at ~43 ns/row (8k-row batches, ~2% of the load path), memory/fuel/deadline limits trap deterministically, fuel exactly reproducible — the Phase 2 ABI is viable. S2.1 and S2.2's cloud legs await credentials. |
| Group F | F1, F2, F3 done (spec + validation + JSON Schema artifact; bounded-channel runner with commit-safe shutdown; log formats + metrics registry). F3 completed 2026-07-12: the JSONL event envelope is pinned by a snapshot test, and `run --otlp-endpoint` (or `PRAMEN_OTLP_ENDPOINT`) pushes the final run metrics to an OTLP collector over HTTP/protobuf — verified against a local collector stub. |
| Group P1 | Three runnable verticals merged. Deterministic: `pramen run` executes Parquet/NDJSON → SQL → Postgres end to end, from local paths or `s3://` (MinIO-verified, 200k rows). Governed AI: `ai.extract`/`ai.classify` on the production SQLite ledger with pre-dispatch budgets, strict output validation, and `onInvalid` policies — verified end to end with full ledger reuse on replay. Checkpointed: file-granular work units on a crash-safe JSONL store (ADR 0006) — replay loads nothing, a grown directory loads only new files (verified end to end). In: P1.1 (local + S3 sources, including checkpointed S3 enumeration — unit identity from a single `LIST`, MinIO-verified incremental runs), P1.2 (NDJSON), P1.3 (checkpoint store + claim/complete), P1.4 (append + upsert COPY sink: stage + ON CONFLICT merge, idempotent replays, L2-tested), P1.5 (provider trait + capabilities), P1.6 (ledger + pinned canonicalization), P1.7 (Bedrock Converse adapter, L1 stub-tested; live acceptance pending credentials), P1.9 (OpenAI-compatible adapter), P1.10 (schema generation + validation), P1.11 (per-record budgets, output caps, `maxRunTokens` run ceiling, always-armed consecutive-invalid circuit breaker), P1.12 (sequential operator), P1.13 (per-batch SQL), P1.14 (delivery contract pinned by L2 tests: append duplicates on replay, upsert does not; crash-window e2e verified), P1.15 (validate/explain/run + `ai status`), P1.8 core (provider-batch operator: buffer, submit, poll, join; job ids durably recorded per item before awaiting; crash-after-submit reconciles by job and item id with zero re-billing, pinned by tests against the batch-capable mock — Bedrock/OpenAI batch adapters remain), P1.17 + S2.2 harness (`ai evaluate` over the versioned 520-item golden support-ticket corpus with weighted rubrics — schema-valid rate, per-field accuracy, macro-F1, weighted score, tokens/cost, latency percentiles, timestamped diffable results; the frontier table over real Bedrock/vLLM models remains), P1.16 (`run --smoke`: row cap, clamped semantic token ceiling, checkpointing bypassed; measured seconds on the example pipeline at zero cost), P1.18 (quickstart scripted in `scripts/quickstart.sh`, executed and timed in CI against the ten-minute bar — measured 2 s locally excluding build). P1.19 (fault-injection suite: typed `ProviderFault` taxonomy — timeout/throttled/transport/protocol/server — enforced deadline in the openai-compat adapter, six offline L1 fault tests, killed-backend-mid-COPY L2 test proving typed failure + untouched target; mid-run cancellation already pinned by runtime behavioral tests), P1.20 (benchmark suite v1: `scripts/bench.sh` over deterministic generated inputs — end-to-end 434k–590k rows/s into PostgreSQL at ~10 CPU-s/GiB and 376–531 MiB peak RSS vs DataFusion-direct and DuckDB baselines; Criterion micro-benches pin the COPY encoder at 5.6–6.5M rows/s and governance fixed cost under 1 ms/record; first report published in `docs/benchmarks/`), X1.6 pulled forward (review queue: durable routing for `onInvalid: review` in the ledger database — queued records withheld across replays with zero re-dispatch/re-billing; `pramen ai review list/export/accept/reject`; accepted corrections schema-validated and recorded as zero-token `human-review` ledger results, rejections permanent), P1.8 provider-batch adapters (OpenAI Files + Batches in `openai-compat`; Bedrock model invocation jobs with S3 staging, `keys.jsonl` join fallback, L2-tested against MinIO + control-plane stub — live S2.1 acceptance pending credentials). Open: S2.2 frontier runs. |
| Group P2 | P2.2 done (v0.1.0). Workspace now `0.2.0` after Phase 2 Group X1, cargo-dist plan verified, release checklist + `CONTRIBUTING.md` + `SECURITY.md`, release quickstart gate. Tag `v0.1.0` triggers binary publish. Open: P2.1 1M-record AWS acceptance (credentials). |
| Phases 2–3 | Phase 2 done offline: X1.1–X1.8 + X2.1–X2.2. Phase 3 research offline: E2.1 (dispatch cost model + mock frontier; live frontier → S2.2) and E2.2 (memoization contract + measured reuse savings). E2.3 scaffolding: orientation + generated scoreboard + competitor harnesses under `compare/` (measured legs still open). Open: E1.*, E2.3 full, E2.4–E2.5. Cloud-blocked: S2.2 frontier, P2.1 acceptance. |

This plan turns the architecture into ordered, parallelizable tasks. Every
task has an owner-agnostic definition of done and, where it matters, a
measurable exit criterion. Tasks are grouped: **tasks inside a group are
independent and may run in parallel** (separate contributors, agents, or git
worktrees); **groups run in the listed order** unless a dependency note says
otherwise.

## 0. How to execute this plan

- One task = one branch = one PR. Small PRs beat batched ones.
- A task is done when: code + tests merged, CI green on all tier-1 targets,
  docs updated, and — if the task involved a decision — an ADR recorded.
- Decisions are goal-shaped: an ADR states the goal metric, the options, the
  measurement that discriminated between them, and reopen triggers. "We
  preferred X" without a number is not a completed decision.
- Spike code lives in `spikes/` and is disposable; its *reports* live in
  `docs/spikes/` and are permanent. Production code never imports spike code.
- Task IDs below (T1.2, S2.1, …) are stable; reference them in branch names
  (`cursor/t1-2-ci-matrix`), commits, and ADRs.

## 1. Global quality gates (every task, every PR)

| Gate | Standard |
| --- | --- |
| Platforms (tier 1) | Linux x86_64 + aarch64 (musl, fully static), macOS aarch64, Windows x86_64 (msvc) — all blocking in CI |
| Platform caveat | Container-backed integration tests (Postgres, MinIO) run on Linux runners only; Windows and macOS run the full unit and non-container suite |
| Toolchain | Pinned stable Rust via `rust-toolchain.toml`, edition 2024, explicit MSRV checked in CI |
| Lints | `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, workspace `[lints]` table |
| Supply chain | `cargo deny` (advisories, licenses, bans, sources) on every PR; Renovate for dependency updates |
| Tests | `cargo nextest` for unit/integration; `insta` snapshots for CLI/plan output; `proptest` for canonicalization and encoders |
| Coverage | `cargo llvm-cov` with a ratchet: coverage may never decrease; floor 80% lines on `pramen-core` and `pramen-ai` |
| Benchmarks | Criterion micro-benches + `hyperfine` CLI-level benches; designated benches fail the perf workflow on >5% regression |
| Docs | Public items documented, doc tests pass, `#![deny(missing_docs)]` on library crates |
| Commits | Conventional commits; changelog generated by git-cliff at release |
| Unsafe | `#![forbid(unsafe_code)]` everywhere except an isolated, documented module if COPY encoding ever needs it |

Default dependency choices (validated, not re-litigated, by the spikes):
Tokio, arrow-rs, DataFusion, object_store, tokio-postgres (COPY BINARY),
rusqlite (WAL ledger), aws-sdk-bedrockruntime/aws-sdk-bedrock, jsonschema,
serde, tracing + OpenTelemetry, rustls everywhere (no OpenSSL). Wasmtime
enters in Phase 2.

## 2. Milestone T: repo and tooling baseline

Goal: a contributor (human or agent) clones, runs one command, and gets the
identical checks CI runs. Exit: `mise run ci` passes locally and in GitHub
Actions on all tier-1 targets with the skeleton workspace.

**Group T1 — parallel**

- **T1.1 Workspace skeleton.** Cargo workspace with `pramen`, `pramen-core`,
  `pramen-io`, `pramen-ai` (+ empty `pramen-wasm` placeholder), shared
  `[workspace.lints]`, `rust-toolchain.toml`, MSRV, LICENSE (Apache-2.0),
  `#![forbid(unsafe_code)]`. Binary prints version and exits.
- **T1.2 Task runner + hooks.** `mise` for tool versions (rust, just-in-time
  tools) and tasks: `fmt`, `lint`, `test`, `cov`, `bench`, `ci`; lefthook
  pre-commit running fmt + clippy on staged crates.
- **T1.3 CI pipeline.** GitHub Actions: blocking check/test matrix across all
  four tier-1 targets (container-backed integration tests scoped to Linux
  runners), nextest with JUnit output, llvm-cov + ratchet check, cargo-deny,
  MSRV job, docs build. Cache with `Swatinem/rust-cache`.
- **T1.4 Release pipeline.** `cargo-dist`: static musl builds for Linux,
  aarch64 macOS, x86_64 Windows (msvc), checksums, GitHub Releases; dry-run
  job on every PR touching release config. Measured goal: fresh-machine
  install to `pramen --version` in under one minute.
- **T1.5 Docs and tracking infrastructure.** `docs/adr/` with template;
  backfill ADRs for decisions already made (ADBC rejected in v1 with reopen
  triggers; WASM deferred; native COPY; SQLite ledger). Create
  `docs/vocabulary.md` with the controlled terms and forbidden synonyms.
  Mirror this plan to GitHub issues: one issue per task ID, one milestone per
  group, labels per workstream.
- **T1.6 Test infrastructure (local-first, ADR 0005).** Four substitution
  layers so every PR gate is offline and free: L0 trait mocks with billing
  counters; L1 protocol stubs — local HTTP servers with recorded, sanitized
  provider fixtures, AWS SDK endpoint override, static test credentials;
  L2 real local services — testcontainers PostgreSQL, MinIO for the S3 API
  (sources and batch staging), an OpenAI-compatible local model server
  (Ollama/vLLM/llama.cpp) for end-to-end semantic runs, and a fake batch
  service (submit/poll/manifest, delays, partial failures, kill-and-resume)
  for reconciliation testing; L3 weekly budget-alarmed cloud acceptance,
  the only source of quality/cost and load-impact numbers.
  *Done 2026-07-12: the shared fixtures live in the dev-only
  `pramen-testkit` crate — L1 HTTP stubs (`one_shot_raw`,
  `one_shot_json`, `serve_router`, all with request capture) now used
  by every adapter test, and uniform self-skipping L2 guards
  (`env::postgres_dsn()`, `env::s3_url()`). CI runs the guarded L2
  suite against a PostgreSQL service container on every PR; MinIO-backed
  and model-server suites remain opt-in locally per the ADR.*
- **T1.7 Benchmark harness.** Criterion setup, `benches/` conventions, a
  synthetic data generator crate feature (deterministic seeds, configurable
  row shapes), perf workflow with baseline storage and regression gate.
  *Done 2026-07-12: Criterion benches landed with P1.20 (governance +
  COPY encoder over the deterministic generator); `scripts/perf-gate.sh`
  benches the merge-base in a git worktree as the baseline, re-benches
  HEAD against it, and fails when the lower 95% confidence bound of the
  change exceeds +5% on the designated stable benches; runs as the Perf
  workflow on pull requests touching code.*

## 3. Phase 0: risky-boundary spikes

Each spike ends with a report in `docs/spikes/` containing raw numbers,
machine details, and a recommendation; architecture and ADRs are amended if
the numbers disagree with the design. Ordered by risk to the thesis.

**Group S1 — parallel**

- **S1.1 Ledger + Bedrock online (the thesis spike).** SQLite WAL ledger with
  the work-key canonicalization from architecture §9; schema-bound
  support-ticket extraction through Bedrock Converse online in `eu-central-1`.
  Exit metrics: 100% result reuse on replay of a completed run; ledger
  overhead per work item measured at 10k/100k items; kill -9 mid-run loses
  zero completed results.
- **S1.2 Parquet + SQL over a bounded stream.** Partitioned Parquet through
  `object_store` (MinIO + real S3), DataFusion single-input SQL transform over
  the batch stream. Exit metrics: peak RSS stays under a configured ceiling
  regardless of input size; throughput GiB/s recorded vs direct DataFusion as
  the overhead baseline.
- **S1.3 Native Postgres COPY.** Arrow → `COPY FROM STDIN BINARY` encoder for
  the v1 type matrix via tokio-postgres. Exit metrics: ≥90% of `psql \copy`
  throughput on the same data; type matrix documented with explicit
  unsupported-type validation errors.
- **S1.4 WASM–Arrow boundary (gates Phase 2 only).** WIT component
  round-tripping Arrow IPC batches under memory/fuel/deadline limits. Exit
  metrics: IPC encode/decode overhead per batch size; trap and limit behavior
  deterministic. May run last; nothing in v1 depends on it.
  *Done 2026-07-12: a `wasm32-wasip2` + wit-bindgen guest (arrow `ipc`
  feature, 1.1 MiB component) behind a one-function WIT ABI, measured
  under wasmtime 38 — ~43 ns/row at the default 8k-row batch (~2% of
  the measured end-to-end load path), 2.0–4.7x vs native+IPC shrinking
  with batch size, 26 µs instantiation; memory/fuel/deadline limits all
  trap deterministically and fuel is exactly reproducible. Report:
  `docs/spikes/s1-4-wasm-arrow.md`.*

**Group S2 — after S1.1**

- **S2.1 Bedrock batch + reconciliation.** Converse batch through encrypted S3
  staging; kill the process after submission and reconcile by job/record ID on
  restart. Exit metrics: zero lost and zero double-billed records across 10
  induced crash points; measured cost delta batch vs online on the same 1k
  golden records.
- **S2.2 Golden corpus + model frontier.** Versioned synthetic support-ticket
  corpus (≥500 labelled records) with weighted rubrics; fast/cheap vs stronger
  Bedrock model on identical prompts/schemas; same schema through local vLLM
  with structured decoding. Exit: a published quality-cost frontier table and
  a pinned model choice per tier, recorded as an ADR.

Phase 0 exit: all spike reports merged; architecture §17 decisions confirmed
or amended; go/no-go recorded for the v1 plan.

## 4. Phase 1: the lean v1

Goal (unchanged from architecture §16): **published binary + one YAML file →
enriched rows in PostgreSQL in under ten minutes**, crash-safe, budgeted,
measured.

**Group F — foundation (sequential, keep small)**

- **F1 Pipeline spec.** Versioned YAML schema (`v1alpha1`), serde model,
  generated JSON Schema artifact, validation with precise, positioned error
  messages (insta-tested). Includes `models`, `transforms`, `budget`,
  `validation`, `runtime` blocks.
- **F2 Core runtime.** Logical DAG → physical plan for linear pipelines;
  bounded Arrow channels; task lifecycle; backpressure; graceful shutdown;
  structured errors. Property test: no deadlock/leak under randomized
  slow-consumer schedules.
- **F3 Observability spine.** tracing setup, metric registry (architecture
  §13 signal list), `--log-format pretty|json|silent`, JSONL event schema
  (snapshot-tested), optional OTLP export.
  *Done 2026-07-12 (final pieces): the JSON event envelope
  (`timestamp`/`level`/`target`/`message` + flattened event fields) is
  pinned by a schema snapshot test, and `pramen run --otlp-endpoint`
  exports the final run metrics (rows/batches/bytes in/out, duration,
  pipeline attribute) over OTLP HTTP/protobuf; export failure is a
  warning, never a run failure. Verified against a local collector.*

**Group P1 — parallel workstreams (after F)**

*Workstream IO (`pramen-io`)*

- **P1.1** Parquet source: bounded readers, target batch sizing, projection
  pushdown; local + S3 via object_store.
  *Done 2026-07-12 (final piece): checkpointed S3 enumeration — work-unit
  identity (key, size, last-modified) from a single `LIST` via
  object_store; MinIO-verified incremental runs (replay loads nothing, a
  grown prefix loads only new objects).*
- **P1.2** NDJSON source with explicit schema or bounded inference.
- **P1.3** File-based checkpoint store; work-unit claim/complete protocol from
  architecture §11; crash-consistency tests (kill at every step boundary).
- **P1.4** Postgres COPY sink: type matrix from S1.3 productionized; append
  mode + idempotent replace-by-work-unit; delivery contract doc; testcontainers
  conformance suite.

*Workstream AI (`pramen-ai`)*

- **P1.5** Provider trait + capability report (online/batch, structured
  output, idempotency, token accounting, residency).
- **P1.6** Durable ledger productionized from S1.1: canonicalization spec
  document + proptests; migration story for the ledger schema itself.
- **P1.7** Bedrock Converse online adapter (default credential chain, region
  pinning, capability validation, usage capture).
- **P1.8** Bedrock batch adapter from S2.1: staging lifecycle, manifest
  ingestion, restart reconciliation.
  *Done 2026-07-12 (offline legs): model invocation jobs in
  `bedrock` — JSONL inputs and a `keys.jsonl` companion staged under
  `spec.models.*.batch.s3`, submitted via `CreateModelInvocationJob`,
  polled with `GetModelInvocationJob`, results joined by work key with
  an input-hash fallback for mangled record ids; `batch: { roleArn, s3
  }` on the model declaration, validated at plan time. L2-tested against
  MinIO staging and a control-plane protocol stub; live IAM/quota
  acceptance and S2.1 crash numbers remain.*
- **P1.9** vLLM (OpenAI-compatible) online adapter with explicit capability
  report.
- **P1.10** Validation layer: JSON Schema generation from declared Arrow
  output fields; type/nullability/enum enforcement; invalid-result routing
  policy (fail/discard/dead-letter).
- **P1.11** Budgets and circuit breakers: per-record/per-run token and cost
  ceilings enforced *before* dispatch; error and invalid-output spike
  breakers; deterministic tests with a mock provider.
- **P1.12** `ai.extract` + `ai.classify` operators: decompose rows to work
  items, join validated results back by stable row identity, never hold Arrow
  batches across pending remote work (assert via memory tests).

*Workstream engine (`pramen-core`)*

- **P1.13** SQL transform operator: single-input DataFusion SQL over the batch
  stream; schema propagation into plan validation. Overhead vs direct
  DataFusion benchmarked (<10% target).
- **P1.14** End-to-end at-least-once semantics: source → transform → semantic
  → sink with checkpoint commit ordering; integration tests including
  duplicate-delivery assertions.

*Workstream CLI and UX (`pramen`)*

- **P1.15** `validate`, `explain`, `run` with secrets resolution (env +
  file references, never in normalized plans or logs).
- **P1.16** `run --smoke`: record cap, fast/cheap model pin, hard cost
  ceiling; measured to complete on the example pipeline in <2 minutes and
  <$0.50.
- **P1.17** `ai evaluate`: run the golden corpus, emit the metrics table
  (schema-valid rate, F1, cost, latency) to a timestamped results directory.
- **P1.18** Quickstart: example dataset + pipeline in `examples/`; the
  ten-minute path documented and *measured* — a scripted fresh-VM test that
  times download → enriched rows and fails CI docs checks if steps drift.

*Workstream quality (cross-cutting, continuous through P1)*

- **P1.19** Fault-injection suite: provider timeouts, throttles, malformed
  model output, Postgres failover, mid-batch kill; every failure surfaces a
  documented, typed error.
- **P1.20** Benchmark suite v1: end-to-end throughput, CPU-s/GiB, peak RSS,
  ledger overhead, cost per accepted row; baselines vs direct DataFusion and
  DuckDB `COPY`; results published in-repo with generator configs.
  *Done 2026-07-12: `scripts/bench.sh` (deterministic generator, BSD/GNU
  `time` parsing) + Criterion benches in `pramen-ai` (work key, ledger
  cold/warm) and `pramen-io` (COPY encoder); first report in
  `docs/benchmarks/2026-07-12-v1.md`. Same-day revision added the
  like-for-like DuckDB→PostgreSQL leg via its `postgres` extension:
  wall-time tie (server-dominated), Pramen ~7× less CPU, DuckDB ~10×
  less RSS — measured over three runs and published in the report.
  CI perf-regression gating landed with T1.7 (`scripts/perf-gate.sh`,
  `.github/workflows/perf.yml`).*

**Group P2 — integration and release (after P1)**

- **P2.1** Acceptance run: golden pipeline over ≥1M synthetic records on AWS
  (S3 → Aurora), crash/restart mid-run, zero re-billed completed work,
  destination load-impact metrics captured.
- **P2.2** v0.1 release via cargo-dist; quickstart validated on a machine that
  has never seen the repo; announce-readiness checklist (README, examples,
  known limitations honestly listed).
  *Done 2026-07-12: workspace version `0.1.0`; `cargo-dist` plan verified
  (four tier-1 targets + installers); `scripts/release-quickstart.sh` and
  `docs/release/v0.1-checklist.md`; known limitations in README;
  `CONTRIBUTING.md` and `SECURITY.md`; installation docs for release
  binaries. Tag `v0.1.0` triggers the release workflow; fresh-machine
  validation with a downloaded binary remains a post-tag smoke test.*

Phase 1 exit = architecture §16 Phase 1 criterion, plus: coverage floor met,
benchmark baselines locked as regression references.

## 5. Phase 2: extensibility and cloud breadth

**Group X1 — parallel**

- **X1.1** Wasmtime integration: WIT ABI from S1.4, memory/fuel/deadline
  limits, precompiled artifact cache keyed by digest + engine version.
  *Done 2026-07-12: `pramen-wasm` hosts the S1.4 WIT `run` ABI with
  fuel/memory/input/output limits, digest-keyed artifact cache, and
  integration tests against the checked-in S1.4 fixture.*
- **X1.2** `type: wasm` transform in the pipeline spec; conformance suite any
  guest must pass; Rust guest SDK + template repo.
  *Done 2026-07-12: `type: wasm` in the v1alpha1 spec with `component` path
  and optional `limits`; wired into `pramen run` with relative-path
  resolution; Rust guest template in `templates/wasm-transform-rust/`.*
- **X1.3** `pramen transform test`: fixture batches through production limits
  with schema + data diff output.
  *Done 2026-07-12: `pramen transform test` runs the S1.4 conformance
  fixture through production limits and verifies `amount_gross` in the
  output schema.*
- **X1.4** OCI distribution: pull by digest, allow-list, signature
  verification hook.
  *Done 2026-07-21: `component` accepts `oci://…@sha256:…` (tag-only
  rejected at validate); `oci-client` pull into digest-keyed cache;
  `runtime.wasmOciAllowlist` + `PRAMEN_WASM_OCI_ALLOWLIST` fail closed;
  `SignatureVerifier` hook with allow-all default (cosign later).*
- **X1.5** Azure Blob + GCS sources with residency-aware config validation.
  *Done 2026-07-21: `gs://`, `az://` / `abfs(s)://` (and Azure https hosts)
  listing + Parquet/NDJSON reads via `object_store` env builders; checkpoint
  work-unit identity from LIST metadata; `runtime.residency` +
  `source.location` offline validation against model `region` and scheme
  allow-lists (ADR 0005); L2 guards for Azurite / GCS emulators.*
- **X1.6** Review queue: export invalid/low-confidence results, `pramen ai
  review` workflow, re-ingestion of human decisions into the ledger.
  *Done 2026-07-12 (pulled forward from X1 into v1): durable queue in the
  ledger database; `onInvalid: review` withholds records without
  re-dispatch or re-billing across replays; `pramen ai review
  list/export/accept/reject` with schema-validated corrections re-entering
  the ledger as zero-token `human-review` results; verified end to end
  through the CLI against an L1 stub.*
- **X1.7** `ai.generate` operator with bounded output enforcement.
  *Done 2026-07-21: `type: ai.generate` reuses `AiTransform` with a
  text-oriented contract — every output field must be `utf8` with
  `maxChars`, and `budget.maxOutputTokensPerRecord` is required. Bounds
  appear in the generated JSON Schema (`maxLength`), are sent as the
  provider request cap, and are rechecked post-response (over-long fields
  / over-cap reported tokens fail validation and follow `onInvalid`; no
  silent truncation). Operation type is part of the work key. L0 mock
  tests cover budget-before-dispatch, over-long reject, ledger reuse,
  extract≠generate keys, and the circuit breaker. Example:
  `examples/local-tickets-ai-generate.yaml`.*
- **X1.8** Shared backends: Postgres-backed ledger and checkpoint store for
  fleet deployments (interface already fixed in P1.6/P1.3).
  *Done 2026-07-21: `LedgerStore` trait with SQLite (default) and Postgres
  backends (`pramen_work_items` / `pramen_review_queue`); `PostgresCheckpointStore`
  on `pramen_checkpoints`; selected via `postgres://` / `postgresql://` in
  `PRAMEN_LEDGER_PATH` and checkpoint `url`; L2 tests env-guarded by
  `PRAMEN_TEST_POSTGRES_DSN`.*

**Group X2 — after X1**

- **X2.1** Third-party WASM transform authored outside the repo passes
  conformance (the extensibility proof).
  *Done 2026-07-21: `examples/external-wasm-guest/` is a standalone Cargo
  project (empty `[workspace]`, vendored WIT, no path deps into Pramen
  crates) with checked-in `dist/acme_gross.wasm`; `pramen transform test`
  and the `third_party_external_guest_passes_conformance` unit test pass
  offline; `mise run wasm-external-guest` / `wasm-external-conformance`
  document rebuild and validation.*
- **X2.2** Documented, reproducible AWS deployment: systemd + container
  profiles, dashboards for the §13 metrics, runbook.
  *Done 2026-07-21 (artifacts; no live AWS apply): `deploy/` (systemd
  unit+timer, Dockerfile/Compose + OTLP collector, Grafana JSON, example
  pipeline/env), runbook [`docs/deploy/aws-runbook.md`](deploy/aws-runbook.md),
  site cookbook `cookbook/aws-deploy`, offline validation via
  `scripts/validate-deploy.sh` + `crates/pramen/tests/deploy_artifacts.rs`.
  Dashboard panels cover the OTLP series exported today (`pramen.rows_*`,
  `pramen.batches_*`, `pramen.bytes_*`, `pramen.run_duration`); remaining
  §13 signals documented as gaps (`pramen ai status` / JSON logs until the
  registry grows).*

## 6. Phase 3: expansion and the paper

**Group E1 — parallel (product)**

- **E1.1** ADBC sink integration behind a feature/profile; container images
  with tested driver sets; first warehouse target chosen by user demand (ADR).
- **E1.2** Flight SQL sink.
- **E1.3** Fan-out DAGs in spec + runtime.
- **E1.4** Connector SDK + conformance harness; support-level matrix
  published per connector.

**Group E2 — parallel (research, can start during Phase 2)**

- **E2.1** RQ1 dispatch policy: implement the cost model (online vs batch
  under deadline constraints); experiments across record volumes, deadlines,
  and providers; publish the measured frontier.
  **Status: offline model + mock frontier done; live provider frontier
  deferred to S2.2.** (`pramen_ai::dispatch`, `execution: auto` +
  `dispatch` hints, `pramen ai dispatch-plan --sweep`,
  `docs/research/e2-1-dispatch-frontier.md`.)
- **E2.2** RQ2 memoization semantics: formalize the reuse contract; measure
  savings under crash/replay, incremental re-enrichment, duplicate-heavy
  workloads.
  *Done 2026-07-21: reuse contract in `docs/research/rq2-memoization.md`
  (work-key inputs, immutability, invalidation, crash/reconcile vs
  re-bill, review-queue interactions); offline measurement suite in
  `pramen_ai::reuse` + `scripts/rq2-memoization.sh` publishing
  `docs/research/rq2-memoization-metrics.json` — 100% reuse / 0 tokens on
  online replay, 0 rebill on batch reconcile, only changed+new keys
  re-billed on incremental (10/45), 90% savings on 200-row/20-unique
  duplicate workload; CI-pinned by `cargo test -p pramen-ai reuse`.*
- **E2.3** RQ3 comparative evaluation: equivalent enrichment task on Redpanda
  Connect (`aws_bedrock_chat`), warehouse AI SQL, and DocETL; throughput, cost
  per accepted row, golden-set quality.
  *Scaffolded 2026-07-21: `docs/compare/orientation.md` + generated
  `docs/benchmarks/compare-scoreboard.{json,md}` (`mise run compare-scoreboard`,
  CI `--check`); site page `project/comparison`; harnesses under
  `compare/redpanda-connect/` and `compare/docetl/` (`harness_ready`).
  Remaining: dated measured runs for competitor AI / warehouse legs and
  flip scoreboard rows to `measured`.*
- **E2.4** Reproducibility artifact: one-command harness that regenerates
  every figure from generators + pinned configs; artifact-evaluation
  checklist for the target venue.
- **E2.5** Paper drafting against the venue deadline recorded in the venue
  ADR; internal red-team review against the "honest caveats" in
  architecture §2.

## 7. Continuous tracks (no phase)

- Renovate dependency updates with cargo-deny gating; monthly audit review.
- Golden corpus growth and re-evaluation on every prompt/model revision.
- Benchmark regression watch; quarterly baseline refresh with an ADR if a
  regression is accepted deliberately.
- Docs: every merged feature updates architecture.md or is explicitly
  declared an implementation detail.
- Community (from v0.1): CONTRIBUTING, security policy, and issue templates
  shipped; release cadence remains.

## 8. Confirmed plan parameters

1. GitHub Actions is the CI platform. Tasks are mirrored to GitHub issues
   (one issue per task ID, milestones per group) as part of T1.5; this file
   remains the canonical structure and is updated in the same PR as any
   issue-state change.
2. Windows x86_64 is a blocking tier-1 target from the start; only
   container-backed integration tests are scoped to Linux runners.
3. Execution model: a small number of parallel agent/contributor tracks, one
   task per branch/worktree; groups above are sized for 2–4 parallel tracks.
4. Development budget is capped under $100/month for cloud + model spend:
   the golden corpus stays at the ≥500-record scale, real-provider suites run
   weekly (not nightly) with AWS budget alarms, and the 1M-record acceptance
   run (P2.1) is a one-off, explicitly approved spend. PR gates are fully
   local per ADR 0005: protocol stubs with recorded fixtures, MinIO,
   testcontainers PostgreSQL, local model servers, and a fake batch service —
   zero cloud access, zero credentials.
5. Paper venue: decided after Phase 0 spikes, but every experiment (spike
   reports, golden evaluation, benchmark suite) is designed to VLDB-grade
   rigor from day one — generators, pins, machine specs, and raw results
   published for everything.
