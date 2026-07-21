# Redpanda Connect comparison harness

Scenario id: `redpanda-connect-ai` on the
[competitive scoreboard](../../docs/benchmarks/compare-scoreboard.md).

## Intent

Measure an equivalent support-ticket classification / extraction flow
against Redpanda Connect’s Bedrock (or OpenAI) AI processor to quantify
Pramen’s batch pricing + ledger reuse advantage — not to assert it.

## Status

`harness_ready` — config scaffold only. No numbers until a dated report
is published under `docs/benchmarks/`.

## Run (manual / budget-capped)

```bash
# Requires Redpanda Connect CLI and provider credentials.
export COMPARE_REDPANDA=1
# Follow https://docs.redpanda.com/redpanda-connect/ for install.
redpanda-connect run compare/redpanda-connect/classify-tickets.yaml
```

Record wall time, tokens/cost (from the provider bill or processor
metrics), schema-valid rate on the golden corpus subset, and crash/replay
behavior. Then:

1. Add `docs/benchmarks/YYYY-MM-DD-redpanda-connect-ai.md` with machine,
   versions, config pins, and raw results.
2. Update `docs/benchmarks/compare-scoreboard.json` rows +
   `status: measured` + `report` path.
3. `mise run compare-scoreboard`

## Fairness rules

- Same input records (golden corpus or a pinned subset).
- Same model id / region when using Bedrock.
- State clearly: online-only Connect vs Pramen `execution: batch` or
  online — do not mix pricing tiers silently.
