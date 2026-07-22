# Local Ollama competitor legs (E2.3 / ADR 0009)

Date: 2026-07-22  
Machine: Linux, 16 cores, 60 GiB RAM  
Model pin: `llama3.2:3b` via Ollama OpenAI-compat `http://127.0.0.1:11434/v1`  
Cost: $0 (local)  
Fixture: first 25 items of `corpora/support-tickets.v1.yaml` →
`corpora/support-tickets/tickets.{json,jsonl}` and
`compare/redpanda-connect/fixtures/tickets.ndjson`

## Pramen baseline (same pin)

See `docs/research/local-ollama-frontier.md` (run
`20260722T083111Z-openai-compat-llama3_2_3b`): 25/25 schema-valid, weighted
~0.589, wall dominated by local decode (~p50 3.3 s/item).

## Redpanda Connect (OSS)

- Binary: Redpanda Connect **4.53.0** (linux amd64 release tarball).
- Config: `compare/redpanda-connect/classify-tickets.yaml`.
- **Note:** branded `openai_chat_completion` requires Enterprise; this run
  uses the OSS `http` processor against Ollama `/v1/chat/completions`,
  `pipeline.threads: 1`, `timeout: 120s` (parallelism overran Ollama and
  produced timeouts).
- Result: **25/25** responses with non-empty model text in
  `compare/redpanda-connect/out/classified.ndjson`.
- Wall: **60.5 s** (~2.4 s/item average).
- Quality: free-form category labels (not scored against the golden rubric
  in this report); categories are loose vs Pramen’s schema-bound enum.

Regenerate:

```bash
ollama pull llama3.2:3b
# install/download redpanda-connect 4.53+
cd compare/redpanda-connect
export COMPARE_REDPANDA=1
redpanda-connect run classify-tickets.yaml
```

## DocETL

- CLI: DocETL **0.3.0** (`uv tool install docetl`).
- Config: `compare/docetl/extract-tickets.yaml` with
  `default_model: openai/llama3.2:3b`, `default_lm_api_base` /
  `OPENAI_API_BASE=http://127.0.0.1:11434/v1`, and
  `output.mode: structured_output` (default tool-calling mode failed on this
  model; native `ollama/*` litellm path returned 404 against this server).
- Result: **25/25** rows with `category` + `confidence` in
  `compare/docetl/out/classified.json`.
- Wall: **68.8 s** (DocETL reported ~66.3 s pipeline time; ~3.7k in / 382 out
  tokens billed as $0).
- Quality: free-form categories (e.g. “Office Equipment”) — not golden-enum
  scored here.

Regenerate:

```bash
export COMPARE_DOCETL=1
export OPENAI_API_KEY=ollama
export OPENAI_API_BASE=http://127.0.0.1:11434/v1
cd compare/docetl
docetl run extract-tickets.yaml --max-threads 1
```

## Warehouse AI SQL

Still **deferred** (no free local warehouse AI SQL target under ADR 0009).

## Takeaway

On this machine, Redpanda Connect (OSS HTTP) and DocETL both complete the
25-ticket enrichment against local Ollama at roughly the same wall time as
Pramen’s evaluate path. Pramen’s differentiator on this fixture is
**schema-bound validation + ledger reuse**, not raw local decode speed.
