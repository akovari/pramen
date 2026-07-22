# DocETL comparison harness

Scenario id: `docetl-extract` on the
[competitive scoreboard](../../docs/benchmarks/compare-scoreboard.md).

## Intent

Compare schema-bound extraction quality and cost on a pinned subset of
the support-ticket golden corpus against
[DocETL](https://github.com/ucbepic/docetl).

## Status

`measured` (local Ollama, ADR 0009) —
[2026-07-22 report](../../docs/benchmarks/2026-07-22-local-ollama-competitors.md).
Use `openai/llama3.2:3b` + `OPENAI_API_BASE=http://127.0.0.1:11434/v1` and
`output.mode: structured_output` (default tool-calling fails on this small
local model).

## Run (manual / local)

```bash
export COMPARE_DOCETL=1
export OPENAI_API_KEY=ollama
export OPENAI_API_BASE=http://127.0.0.1:11434/v1
# uv tool install docetl
docetl run compare/docetl/extract-tickets.yaml --max-threads 1
```

Fixture: `corpora/support-tickets/tickets.json` (first 25 golden items).

## Fairness rules

- Same labelled subset as Pramen `ai evaluate` when scoring quality.
- Report both completion rate / wall time and (when scored) weighted
  quality; local runs are $0 under ADR 0009.
