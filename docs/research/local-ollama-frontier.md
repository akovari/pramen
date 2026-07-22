# Local Ollama frontier (S2.2 substitute / ADR 0009)

Date: 2026-07-22  
Status: measured (local-open-model — **not** Bedrock)  
ADR: [0009](../adr/0009-local-only-ollama-acceptance.md)

## Pins

| Knob | Value |
| --- | --- |
| Provider | `openai-compat` |
| Endpoint | `http://127.0.0.1:11434/v1` |
| Model | `llama3.2:3b` (Ollama, ~2.0 GB) |
| Corpus | `corpora/support-tickets.v1.yaml` |
| Limit | 25 items (first N; full 520 deferred for wall-clock) |
| Prices | $0 / $0 per MTok |
| Machine | Linux, ~60 GiB RAM, 16 cores |

## Command

```bash
ollama pull llama3.2:3b   # once
pramen ai evaluate \
  --provider openai-compat \
  --endpoint http://127.0.0.1:11434/v1 \
  --model llama3.2:3b \
  --limit 25 \
  --out docs/research/local-ollama \
  --input-price 0 \
  --output-price 0
```

## Results (20260722T083111Z)

| Metric | Value |
| --- | --- |
| Schema-valid | 25/25 (100%) |
| Weighted score | 0.589 |
| category accuracy / macro-F1 | 0.760 / 0.754 |
| priority accuracy / macro-F1 | 0.400 / 0.363 |
| product accuracy / macro-F1 | 0.640 / 0.700 |
| requires_review accuracy | 0.400 |
| Tokens | 4793 in / 478 out (~$0) |
| Latency | p50 3298 ms, p95 3448 ms |

Raw: `docs/research/local-ollama/20260722T083111Z-openai-compat-llama3_2_3b/`.

## Caveats

- Small local model: quality is far below hosted frontier models; this run
  proves the **live adapter path + corpus harness**, not competitive F1.
- Subset of 25 items — regenerate with `--limit` omitted for full corpus
  when wall time is acceptable (~3.3 s × 520 ≈ 30 min order-of-magnitude).
- Bedrock live legs remain paid-only under ADR 0009.
