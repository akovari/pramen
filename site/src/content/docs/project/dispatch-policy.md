---
title: Dispatch policy
description: Online vs provider-batch cost model and the offline mock/stub frontier (E2.1 / RQ1).
---

# Dispatch policy (E2.1)

**Label: mock/stub-measured analytical frontier — not live Bedrock.**
Reopen when S2.2 live provider numbers exist.

Research question RQ1 asks: given per-record work, provider online and
batch pricing, batch completion windows, and a pipeline deadline, when
does batch dominate online?

## Cost model

`pramen_ai::dispatch` estimates USD cost and wall-clock latency for both
modes, then recommends the cheaper mode that still meets the deadline.
`execution: auto` uses the same planner when a semantic transform declares:

```yaml
execution: auto
dispatch:
  expectedRecords: 10000
  deadlineSeconds: 3600
  # optional:
  # inputTokensPerRecord: 800
  # outputTokensPerRecord: 200
  # rateCard: mock   # or openai-compat-stub, bedrock-illustrative
```

Without those hints, auto stays **online** (safe for unbounded work).

## CLI

```bash
# One workload
pramen ai dispatch-plan --rate-card mock --records 10000 --deadline-seconds 3600

# Published frontier sweep
pramen ai dispatch-plan --sweep --out docs/research/e2-1-dispatch-frontier.md
```

## Frontier

The checked-in table is regenerated from the analytical model over
volumes `{100, 1k, 10k, 100k}` × deadlines `{5m, 1h, 24h}` × rate cards
`mock` and `openai-compat-stub`. See
[`docs/research/e2-1-dispatch-frontier.md`](https://github.com/akovari/pramen/blob/main/docs/research/e2-1-dispatch-frontier.md).
