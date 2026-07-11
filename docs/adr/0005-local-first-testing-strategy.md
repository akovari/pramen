# ADR 0005: Local-first testing strategy — cloud only for weekly acceptance

Status: accepted  
Date: 2026-07-11  
Task: T1.6 (infrastructure), S2.1 (batch reconciliation), P1.19 (fault injection)

## Goal metric

Every PR gate runs with zero cloud access, zero spend, and no credentials on
all four tier-1 platforms; the full pipeline — including semantic transforms
and provider-batch reconciliation — is testable end to end on a developer
laptop. Cloud calls are confined to the weekly budget-alarmed suite.

## Options considered

1. **Mock at the code seam only** (trait-level mocks) — fast, but never
   exercises serialization, HTTP error mapping, credentials plumbing, or
   response parsing; the provider adapter itself stays untested until cloud.
2. **LocalStack for AWS emulation** — Bedrock emulation sits in the paid
   tier, coverage of Converse/batch semantics is partial, and it adds a heavy
   dependency for what is mostly plain HTTPS + S3.
3. **Layered local-first substitution** (chosen): protocol-level stubs and
   real local services, each layer substitutable for the one above it.

## Decision

Four test layers; each cloud API must have a seam *and* an offline
protocol-level substitute, not just a code mock.

- **L0 — pure logic.** Trait-level mocks (with billing counters so reuse is
  asserted, never assumed), property tests, no I/O. Runs everywhere,
  including Windows and macOS CI.
- **L1 — protocol stubs.** Local HTTP servers serving canned or recorded
  provider responses; the AWS SDK is pointed at `localhost` via endpoint
  override with static test credentials. Covers Converse request/response
  mapping, structured-output parsing, token accounting, and the error
  taxonomy (throttling, timeouts, malformed model output). Fixtures are
  recorded once from real L3 runs, sanitized, and committed. Runs everywhere.
- **L2 — local integration.** Real services, no cloud:
  - PostgreSQL via testcontainers for COPY, type matrix, idempotency, and
    crash tests;
  - MinIO for the S3 API — object-store sources *and* batch JSONL staging;
  - real local inference through any OpenAI-compatible server (Ollama,
    vLLM, llama.cpp) with a small model, for end-to-end semantic runs and
    golden-harness validation;
  - a **fake batch service** implementing submit/poll/manifest semantics
    (configurable delays, partial failures, job IDs, kill-and-resume),
    because no Bedrock batch emulator exists and reconciliation logic is
    exactly what must be provably correct.
  Container-backed tests run on Linux CI runners and dev machines.
- **L3 — cloud acceptance.** Weekly, budget-alarmed: real Bedrock
  (`eu-central-1`, online and batch) and Aurora. The only source of
  quality/cost frontier numbers and destination load-impact results.

Two honesty rules: local-model outputs validate the *harness*, never the
product's quality claims; and L1/L2 substitutes must fail tests when their
behavior is known to diverge from a recorded real interaction, rather than
silently drifting.

## Measurement

Spike S1.1 extension (2026-07-11): the Bedrock Converse adapter runs its
full request/parse/validate/record path against a localhost stub with static
credentials, offline. The same adapter code path is used for L3.

## Reopen triggers

- A provider API change that recorded fixtures cannot express (e.g. a
  streaming-only requirement).
- LocalStack or an equivalent gains free, faithful Bedrock online+batch
  emulation worth consolidating on.
- The fake batch service's semantics diverge from observed Bedrock behavior
  in an L3 run; the fake must then be corrected the same week.
