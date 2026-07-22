# Redpanda Connect comparison harness

Scenario id: `redpanda-connect-ai` on the
[competitive scoreboard](../../docs/benchmarks/compare-scoreboard.md).

## Intent

Measure an equivalent support-ticket classification flow against Redpanda
Connect to quantify Pramen’s schema-bound validation + ledger reuse
advantage — not to assert it.

## Status

`measured` (local Ollama, ADR 0009) —
[2026-07-22 report](../../docs/benchmarks/2026-07-22-local-ollama-competitors.md).
Branded `openai_chat_completion` is Enterprise-gated in Connect 4.53;
the harness uses the OSS `http` processor against Ollama. Bedrock-priced
runs remain paid-only.

## Run (manual / local)

```bash
ollama pull llama3.2:3b
# Redpanda Connect CLI 4.53+ (OSS)
export COMPARE_REDPANDA=1
cd compare/redpanda-connect
redpanda-connect run classify-tickets.yaml
```

Fixture: `fixtures/tickets.ndjson` (first 25 golden items). Keep
`pipeline.threads: 1` on modest machines.

## Fairness rules

- Same input records (golden corpus or a pinned subset).
- Same model pin when comparing to Pramen `ai evaluate`.
- State clearly: Connect OSS HTTP vs Pramen schema-bound path — do not
  mix Enterprise AI processors or paid tiers silently.
