# ADR 0009: Local-only live acceptance via Ollama (no paid cloud)

Status: accepted  
Date: 2026-07-22  
Task: S1.1 live leg (substitute), S2.2 frontier (substitute), E2.3 measured legs (local), plan §8 budget

## Goal metric

All “live” enrichment and competitive-comparison gates for Phase 3 must be
runnable with **zero paid subscriptions**: local Ollama (OpenAI-compatible),
local PostgreSQL / MinIO as already used in CI, and optional free local
competitor tools (Redpanda Connect OSS, DocETL). Success = dated reports
under `docs/research/` / `docs/benchmarks/` that name model pins, machine
notes, and regenerate commands — not Bedrock bill lines.

## Options considered

1. **Keep Bedrock/AWS as the only “live” bar** — blocks S1.1/S2.1/S2.2/P2.1/E2.3
   measured legs without spend.
2. **Temporary free-tier cloud credits** — still vendor accounts and expiry;
   not zero-subscription.
3. **Local Ollama + openai-compat as the live acceptance profile** — same
   adapters and golden corpus; cost column uses $0 or an illustrative open
   rate card; Bedrock-specific live legs marked paid-only / deferred.

## Measurement

- Ollama `llama3.2:3b` (~2 GB) answers OpenAI-compatible
  `POST /v1/chat/completions` on this machine (60 GiB RAM, 16 cores).
- Existing `pramen ai evaluate --provider openai-compat --endpoint
  http://127.0.0.1:11434/v1` path needs no new adapter.
- Development budget already capped under $100/month with PR gates fully
  local (ADR 0005); this ADR extends that discipline to Phase 3 “live”
  acceptance when no subscription is available.

## Decision

**Option 3.** Pin the default local live profile:

| Knob | Value |
| --- | --- |
| Provider | `openai-compat` |
| Endpoint | `http://127.0.0.1:11434/v1` |
| Model | `llama3.2:3b` (small; upgrade only with a dated report) |
| API key | unset / empty (`PRAMEN_OPENAI_API_KEY` optional) |
| Prices | `$0` for local runs unless an illustrative open rate card is stated |

Consequences:

- **S2.2 / S1.1-shaped acceptance** may complete as **local-open-model**
  reports; they must not be labeled “Bedrock measured.”
- **S2.1 live Bedrock batch** and **P2.1 AWS 1M** remain **paid-only /
  not planned** under this budget.
- **E2.3** measured legs use local Ollama behind competitor tools when those
  CLIs are available; warehouse AI SQL stays qualitative/`deferred`.
- **E1.1 ADBC** is unaffected except that the first warehouse ADR must also
  prefer free/local targets or explicit deferral.

## Reopen triggers

- A funded AWS/Bedrock (or other paid) budget is approved in writing.
- Local Ollama cannot satisfy a venue AE requirement that mandates a named
  hosted model — then a venue ADR must name the exception.
- `llama3.2:3b` proves too weak for schema-valid rates on the golden corpus
  (e.g. sustained schema-valid ≪ mock baseline with no prompt fix) — then
  pin a larger local model in a follow-up report, still free.
