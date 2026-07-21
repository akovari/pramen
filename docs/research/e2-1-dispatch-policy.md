# E2.1 — RQ1 dispatch policy (research note)

**Status:** offline cost model + mock/stub frontier **done**; live
Bedrock / hosted-provider frontier **deferred to S2.2**.

**No ADR:** this implements architecture §18 RQ1 and the existing §17
decision that online and asynchronous batch are both execution modes. It
does not reopen a numbered decision.

## Goal

Given per-record work, provider online and batch pricing, batch
completion windows, and a pipeline deadline, decide when batch dispatch
dominates online — and publish a measured cost/latency frontier.

## Deliverables in this change

1. **Cost model** — `pramen_ai::dispatch` (`plan`, rate cards, latency
   model, frontier sweep). Unit-tested math; deterministic L0 sweeps.
2. **`execution: auto`** — when a transform sets
   `dispatch.expectedRecords` + `dispatch.deadlineSeconds`, the operator
   resolves to online or batch via the cost model. Without hints, auto
   stays online.
3. **CLI** — `pramen ai dispatch-plan` (single plan or `--sweep`).
4. **Published frontier** —
   [`e2-1-dispatch-frontier.md`](./e2-1-dispatch-frontier.md), clearly
   labeled mock/stub-calibrated. Regenerate with
   `mise run dispatch-frontier`.
5. **Docs/site** — architecture prose, pipeline schema, CLI reference,
   and `site/.../project/dispatch-policy.md`.

## Method (offline)

For each `(rate card × volume × deadline)` cell:

- Estimate online cost from token totals × online USD/MTok.
- Estimate online wall time as concurrent waves of per-record latency.
- Estimate batch cost at the card's batch prices (typically 50% of
  online).
- Estimate batch wall time as fixed overhead + completion window +
  marginal staging.
- Recommend the cheaper mode among those that meet the deadline; if
  neither meets it, prefer the faster finish.

Default tokens: 800 input / 200 output per record. Default grid: volumes
`{100, 1_000, 10_000, 100_000}`, deadlines `{300, 3_600, 86_400}`
seconds, cards `mock` and `openai-compat-stub`. The
`bedrock-illustrative` card is available for single-point plans but is
**not** claimed as live-measured.

## Deferred

- Live Bedrock Converse online + batch frontier with real latency and
  invoice-backed prices (S2.2 / credentialed acceptance).
- Empirically fitted latency parameters replacing the synthetic mock
  windows.
- Optional YAML-free planning from source cardinality estimates.
