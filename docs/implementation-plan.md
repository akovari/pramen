# Pramen Implementation Plan

Status: in execution  
Companion to: [architecture.md](architecture.md)  
Last reviewed: 2026-07-11

## Status snapshot (2026-07-11)

Authoritative per-task state lives in the mirrored GitHub issues; this
snapshot orients a reader without leaving the file.

| Area | State |
| --- | --- |
| Milestone T | T1.1–T1.5 done. T1.6: strategy decided (ADR 0005) and L1/L2 patterns proven in spike code and the Postgres sink test; shared fixtures not yet extracted. T1.7 not started. |
| Phase 0 spikes | S1.1 offline legs done (ledger, crash recovery, protocol stubs; live Bedrock leg awaits credentials). S1.2 done: peak RSS flat (~184 MiB) while input doubles, ~3M rows/s. S1.3 done: binary COPY at 3.1x the `psql \copy` baseline, ADR 0001 confirmed. S1.4, S2.1, S2.2 not started. |
| Group F | F1, F2, F3 done (spec + validation + JSON Schema artifact; bounded-channel runner with commit-safe shutdown; log formats + metrics registry). OTLP export and the JSONL event schema remain open on F3's issue. |
| Group P1 | Three runnable verticals merged. Deterministic: `pramen run` executes Parquet/NDJSON → SQL → Postgres end to end, from local paths or `s3://` (MinIO-verified, 200k rows). Governed AI: `ai.extract`/`ai.classify` on the production SQLite ledger with pre-dispatch budgets, strict output validation, and `onInvalid` policies — verified end to end with full ledger reuse on replay. Checkpointed: file-granular work units on a crash-safe JSONL store (ADR 0006) — replay loads nothing, a grown directory loads only new files (verified end to end). In: P1.1 (local + S3 sources; remote checkpoint enumeration open), P1.2 (NDJSON), P1.3 (checkpoint store + claim/complete), P1.4 (append + upsert COPY sink: stage + ON CONFLICT merge, idempotent replays, L2-tested), P1.5 (provider trait + capabilities), P1.6 (ledger + pinned canonicalization), P1.7 (Bedrock Converse adapter, L1 stub-tested; live acceptance pending credentials), P1.9 (OpenAI-compatible adapter), P1.10 (schema generation + validation), P1.11 (per-record budgets, output caps, `maxRunTokens` run ceiling, always-armed consecutive-invalid circuit breaker), P1.12 (sequential operator), P1.13 (per-batch SQL), P1.14 (delivery contract pinned by L2 tests: append duplicates on replay, upsert does not; crash-window e2e verified), P1.15 (validate/explain/run + `ai status`), P1.8 core (provider-batch operator: buffer, submit, poll, join; job ids durably recorded per item before awaiting; crash-after-submit reconciles by job and item id with zero re-billing, pinned by tests against the batch-capable mock — Bedrock/OpenAI batch adapters remain), P1.17 + S2.2 harness (`ai evaluate` over the versioned 520-item golden support-ticket corpus with weighted rubrics — schema-valid rate, per-field accuracy, macro-F1, weighted score, tokens/cost, latency percentiles, timestamped diffable results; the frontier table over real Bedrock/vLLM models remains), P1.16 (`run --smoke`: row cap, clamped semantic token ceiling, checkpointing bypassed; measured seconds on the example pipeline at zero cost), P1.18 (quickstart scripted in `scripts/quickstart.sh`, executed and timed in CI against the ten-minute bar — measured 2 s locally excluding build). Open: P1.8 cloud adapters, S2.2 frontier runs, P1.19–P1.20, review-queue routing. |
| Phases 2–3 | Not started. |

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
- **T1.7 Benchmark harness.** Criterion setup, `benches/` conventions, a
  synthetic data generator crate feature (deterministic seeds, configurable
  row shapes), perf workflow with baseline storage and regression gate.

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

**Group P1 — parallel workstreams (after F)**

*Workstream IO (`pramen-io`)*

- **P1.1** Parquet source: bounded readers, target batch sizing, projection
  pushdown; local + S3 via object_store.
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

**Group P2 — integration and release (after P1)**

- **P2.1** Acceptance run: golden pipeline over ≥1M synthetic records on AWS
  (S3 → Aurora), crash/restart mid-run, zero re-billed completed work,
  destination load-impact metrics captured.
- **P2.2** v0.1 release via cargo-dist; quickstart validated on a machine that
  has never seen the repo; announce-readiness checklist (README, examples,
  known limitations honestly listed).

Phase 1 exit = architecture §16 Phase 1 criterion, plus: coverage floor met,
benchmark baselines locked as regression references.

## 5. Phase 2: extensibility and cloud breadth

**Group X1 — parallel**

- **X1.1** Wasmtime integration: WIT ABI from S1.4, memory/fuel/deadline
  limits, precompiled artifact cache keyed by digest + engine version.
- **X1.2** `type: wasm` transform in the pipeline spec; conformance suite any
  guest must pass; Rust guest SDK + template repo.
- **X1.3** `pramen transform test`: fixture batches through production limits
  with schema + data diff output.
- **X1.4** OCI distribution: pull by digest, allow-list, signature
  verification hook.
- **X1.5** Azure Blob + GCS sources with residency-aware config validation.
- **X1.6** Review queue: export invalid/low-confidence results, `pramen ai
  review` workflow, re-ingestion of human decisions into the ledger.
- **X1.7** `ai.generate` operator with bounded output enforcement.
- **X1.8** Shared backends: Postgres-backed ledger and checkpoint store for
  fleet deployments (interface already fixed in P1.6/P1.3).

**Group X2 — after X1**

- **X2.1** Third-party WASM transform authored outside the repo passes
  conformance (the extensibility proof).
- **X2.2** Documented, reproducible AWS deployment: systemd + container
  profiles, dashboards for the §13 metrics, runbook.

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
- **E2.2** RQ2 memoization semantics: formalize the reuse contract; measure
  savings under crash/replay, incremental re-enrichment, duplicate-heavy
  workloads.
- **E2.3** RQ3 comparative evaluation: equivalent enrichment task on Redpanda
  Connect (`aws_bedrock_chat`), warehouse AI SQL, and DocETL; throughput, cost
  per accepted row, golden-set quality.
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
- Community (from v0.1): CONTRIBUTING, issue templates, security policy,
  release cadence.

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
