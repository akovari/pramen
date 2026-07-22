# Paper draft (E2.5) — venue-agnostic prose

Status: working draft (not venue-formatted)  
Date: 2026-07-22  
Outline: [paper-outline.md](paper-outline.md)  
Caveats authority: [architecture.md](../architecture.md) §2–3

This file holds offline prose for sections that do not need a venue template.
RQ result sections (§4–6) stay in their measured reports until a venue ADR
fixes page budget and AE packaging. Do not paste live Bedrock numbers here;
local Ollama substitutes are labeled as such (ADR 0009).

---

## 1. Introduction / positioning

Data movement tools increasingly embed large language models as pipeline
steps. In practice those steps are usually online, per-message calls that
write free-form text back into a payload. Replays re-bill; crashes lose
in-flight calls; schema is aspirational. Systems that *do* treat semantic
operators seriously (DocETL, Palimpzest, in-warehouse AI SQL) either sit
above a Python dataframe tier or assume the data already lives inside one
warehouse.

**Pramen** is an Arrow-native source–transform–sink runtime that treats
governed LLM enrichment as a *systems* problem: bounded channels,
checkpointed work units, schema-bound outputs, budgets, and a durable
content-addressed inference ledger so completed results survive restart
and replay without re-billing. Deterministic stages (decode, SQL, COPY)
run at columnar speed; semantic stages are dispatched deliberately —
including to provider batch APIs when deadlines allow.

We do not claim to replace Flink, Spark, or warehouse AI SQL for data that
already belongs in those systems. We claim a credible wedge for
operational and cross-system enrichment where batch pricing, typed
columns, and restart-safe reuse matter more than cluster features.

## 2. Threats / honest caveats

Borrowed from architecture §2–3 and enforced in the red-team checklist:

- **At-least-once, not exactly-once.** Checkpoints and ledger reuse do not
  make arbitrary sinks exactly-once. Connectors state their delivery
  contract; append sinks can duplicate across the post-commit window.
- **Warehouse AI SQL often wins on its home turf.** If data and results
  stay in Databricks / Snowflake / BigQuery and hosted models are
  acceptable, in-warehouse functions are the default. Pramen’s wedge is
  data that is not in — or not destined for — a single warehouse.
- **“An LLM step” is not novel.** Redpanda Connect and peers already call
  models. Differentiation is batch scheduling, ledger reuse, schema
  validation, budgets, and review routing — not the existence of a call.
- **Offline mock frontiers ≠ live hosted frontiers.** Mock rate cards and
  local Ollama runs (ADR 0009) must not be labeled as Bedrock SLAs.
- **Small local models are weak.** Schema-valid rates on `llama3.2:3b` are
  acceptance gates for the free path, not quality claims for production
  tiers.

## 3. System overview

A pipeline is a validated DAG. Nodes exchange Arrow `RecordBatch` values
over bounded channels (backpressure is structural). Stages are sources,
transforms (SQL, WASM components, semantic operators), and sinks.

**Semantic path.** `ai.extract` / `ai.classify` (and related) select input
fields, build a work key from canonicalized inputs + prompt/schema pins,
consult the inference ledger, and only then dispatch to a provider
adapter (Bedrock, OpenAI-compatible, mocks). Invalid structured output
follows `onInvalid` (fail, drop, or review). Budgets and circuit breakers
arm before paid calls.

**Delivery.** v1’s lean sink is native PostgreSQL `COPY` (ADR 0001).
Append-only Flight SQL is in the default binary (ADR 0008). Fan-out uses
an all-sinks-then-checkpoint barrier (ADR 0007). ADBC multi-warehouse
profiles are deferred until a free/local or funded target is named
(ADR 0010).

**Packaging.** The lean profile is one static binary without native
driver dependencies. Expansion profiles that need drivers are explicit
and never claimed as the lean promise.

## 8. Related work (short)

| Family | Lesson for Pramen |
| --- | --- |
| Vector / Redpanda Connect | Operational single-binary pipelines; AI processors exist but online-only |
| Flink / Spark / Arroyo / RisingWave | Defer cluster state, windows, exactly-once coordination |
| In-warehouse AI SQL | Default when data already lives there; not always worse |
| DocETL / Palimpzest / CocoIndex | Semantic ops + cost/quality optimization; we bet on Arrow + ledger + batch |
| Airbyte / Meltano | Connector support levels and conformance beat shallow catalogs |
| BAML | Schema-first LLM contracts; we bind Arrow columns + durable ledger |

## 9. Limitations

- Delivery is at-least-once with connector-specific idempotency; do not
  market exactly-once.
- Flight SQL sink is append-only in v1.
- ADBC / first commercial warehouse sink is deferred (ADR 0010).
- WASM transforms are capability-denied by default; network from guests is
  out of scope for v1.
- Live Bedrock batch and 1M-record AWS acceptance are paid-only under
  ADR 0009; local Ollama substitutes are documented separately.
- Competitive scoreboard warehouse-AI row remains qualitative/`deferred`.

## What remains for full E2.5

1. Venue ADR (deadline, page limit, AE rules).
2. Remap this draft + RQ reports into the venue template.
3. Red-team pass against the checklist in `paper-outline.md`.
4. Optional: expand §4–6 here only after venue length is known.
