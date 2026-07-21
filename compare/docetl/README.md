# DocETL comparison harness

Scenario id: `docetl-extract` on the
[competitive scoreboard](../../docs/benchmarks/compare-scoreboard.md).

## Intent

Compare schema-bound extraction quality and cost on a pinned subset of
the support-ticket golden corpus against
[DocETL](https://github.com/ucbepic/docetl).

## Status

`harness_ready` — pipeline scaffold only.

## Run (manual)

```bash
export COMPARE_DOCETL=1
# Install DocETL per upstream docs, then:
docetl run compare/docetl/extract-tickets.yaml
```

Publish `docs/benchmarks/YYYY-MM-DD-docetl-extract.md` (versions, prompt,
schema, schema-valid rate, tokens/cost, wall time), update the scoreboard
JSON to `measured`, then `mise run compare-scoreboard`.

## Fairness rules

- Same labelled subset and field schema as Pramen `ai.extract` /
  `ai.classify` eval.
- Report both quality (weighted score / F1) and cost per accepted row.
