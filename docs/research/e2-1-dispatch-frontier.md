# E2.1 dispatch frontier (offline / mock-calibrated)

**Label: mock/stub-measured analytical frontier — not live Bedrock.**
Reopen when S2.2 live provider numbers exist.

## Method

The [`pramen_ai::dispatch`] cost model estimates online vs
provider-batch USD cost and wall-clock latency for each
(rate card × record volume × deadline) cell, then recommends the
cheaper mode that still meets the deadline. Token assumptions:
800 input / 200 output per record. Rate cards:

- `mock` — 50% batch discount, ~60s synthetic completion window
- `openai-compat-stub` — 50% batch discount, 1h completion window

Regenerate:

```bash
pramen ai dispatch-plan --sweep --out docs/research/e2-1-dispatch-frontier.md
```

## Frontier

| rate card | records | deadline | recommended | online $ | online s | batch $ | batch s | reason |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |
| mock | 100 | 300s | batch | 0.5400 | 0.2 | 0.2700 | 62.1 | both meet deadline; batch cheaper ($0.2700 vs $0.5400) |
| mock | 100 | 3600s | batch | 0.5400 | 0.2 | 0.2700 | 62.1 | both meet deadline; batch cheaper ($0.2700 vs $0.5400) |
| mock | 100 | 86400s | batch | 0.5400 | 0.2 | 0.2700 | 62.1 | both meet deadline; batch cheaper ($0.2700 vs $0.5400) |
| mock | 1000 | 300s | batch | 5.4000 | 1.6 | 2.7000 | 63.0 | both meet deadline; batch cheaper ($2.7000 vs $5.4000) |
| mock | 1000 | 3600s | batch | 5.4000 | 1.6 | 2.7000 | 63.0 | both meet deadline; batch cheaper ($2.7000 vs $5.4000) |
| mock | 1000 | 86400s | batch | 5.4000 | 1.6 | 2.7000 | 63.0 | both meet deadline; batch cheaper ($2.7000 vs $5.4000) |
| mock | 10000 | 300s | batch | 54.0000 | 15.7 | 27.0000 | 72.0 | both meet deadline; batch cheaper ($27.0000 vs $54.0000) |
| mock | 10000 | 3600s | batch | 54.0000 | 15.7 | 27.0000 | 72.0 | both meet deadline; batch cheaper ($27.0000 vs $54.0000) |
| mock | 10000 | 86400s | batch | 54.0000 | 15.7 | 27.0000 | 72.0 | both meet deadline; batch cheaper ($27.0000 vs $54.0000) |
| mock | 100000 | 300s | batch | 540.0000 | 156.2 | 270.0000 | 162.0 | both meet deadline; batch cheaper ($270.0000 vs $540.0000) |
| mock | 100000 | 3600s | batch | 540.0000 | 156.2 | 270.0000 | 162.0 | both meet deadline; batch cheaper ($270.0000 vs $540.0000) |
| mock | 100000 | 86400s | batch | 540.0000 | 156.2 | 270.0000 | 162.0 | both meet deadline; batch cheaper ($270.0000 vs $540.0000) |
| openai-compat-stub | 100 | 300s | online | 0.0240 | 2.6 | 0.0120 | 3605.2 | batch misses deadline (3605s > 300s); using online |
| openai-compat-stub | 100 | 3600s | online | 0.0240 | 2.6 | 0.0120 | 3605.2 | batch misses deadline (3605s > 3600s); using online |
| openai-compat-stub | 100 | 86400s | batch | 0.0240 | 2.6 | 0.0120 | 3605.2 | both meet deadline; batch cheaper ($0.0120 vs $0.0240) |
| openai-compat-stub | 1000 | 300s | online | 0.2400 | 25.0 | 0.1200 | 3607.0 | batch misses deadline (3607s > 300s); using online |
| openai-compat-stub | 1000 | 3600s | online | 0.2400 | 25.0 | 0.1200 | 3607.0 | batch misses deadline (3607s > 3600s); using online |
| openai-compat-stub | 1000 | 86400s | batch | 0.2400 | 25.0 | 0.1200 | 3607.0 | both meet deadline; batch cheaper ($0.1200 vs $0.2400) |
| openai-compat-stub | 10000 | 300s | online | 2.4000 | 250.0 | 1.2000 | 3625.0 | batch misses deadline (3625s > 300s); using online |
| openai-compat-stub | 10000 | 3600s | online | 2.4000 | 250.0 | 1.2000 | 3625.0 | batch misses deadline (3625s > 3600s); using online |
| openai-compat-stub | 10000 | 86400s | batch | 2.4000 | 250.0 | 1.2000 | 3625.0 | both meet deadline; batch cheaper ($1.2000 vs $2.4000) |
| openai-compat-stub | 100000 | 300s | online | 24.0000 | 2500.0 | 12.0000 | 3805.0 | neither meets deadline; online finishes sooner (2500s vs 3805s) |
| openai-compat-stub | 100000 | 3600s | online | 24.0000 | 2500.0 | 12.0000 | 3805.0 | batch misses deadline (3805s > 3600s); using online |
| openai-compat-stub | 100000 | 86400s | batch | 24.0000 | 2500.0 | 12.0000 | 3805.0 | both meet deadline; batch cheaper ($12.0000 vs $24.0000) |
