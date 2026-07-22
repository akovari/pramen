# Paper outline (E2.5) — venue-agnostic

Status: draft outline (venue **TBD** — no target selected yet)  
Date: 2026-07-22  
Companion: [architecture.md §18](../architecture.md), [artifact-evaluation.md](artifact-evaluation.md)

This is the offline scaffolding for E2.5. Venue-specific formatting,
page limits, and AE packaging wait on a venue ADR. Until then, keep the
outline and figure inventory current so drafting can start immediately
once a venue is chosen.

## Working title

*Governed semantic operators in a columnar dataflow: cost-optimal,
restart-safe LLM enrichment as a systems problem.*

## Claimed contributions (not the operators themselves)

1. A **durable content-addressed inference ledger** with an explicit reuse
   and reconciliation contract inside an at-least-once Arrow dataflow.
2. Treating **provider batch APIs as a first-class scheduling target**, with
   a cost model and measured online/batch frontier under deadlines.
3. An **end-to-end evaluation artifact** (generators, pins, corpus, scoreboard)
   shared by the product benchmark suite and the paper.

## Section map

| Section | Content source (already exists) | Offline status |
| --- | --- | --- |
| 1. Introduction / positioning | architecture §1–2, `docs/compare/orientation.md` | Ready to draft |
| 2. Threats / honest caveats | architecture §2 caveats | Ready to draft |
| 3. System overview | architecture §3–11 (lean binary, stages, ledger, sinks) | Ready to draft |
| 4. RQ1 Dispatch policy | `e2-1-dispatch-policy.md`, `e2-1-dispatch-frontier.md` | Offline model + mock frontier done; live frontier → S2.2 |
| 5. RQ2 Memoization | `rq2-memoization.md`, `rq2-memoization-metrics.json` | Offline measured; live Bedrock confirm → S2.2 |
| 6. RQ3 Comparison | scoreboard + `compare/` harnesses | Offline legs measured; competitor AI → credentials (E2.3) |
| 7. Implementation notes | connector matrix, COPY spike, WASM spike reports | Ready |
| 8. Related work | architecture §2 families + §19 sources | Ready |
| 9. Limitations | at-least-once window, append-only Flight SQL, no ADBC yet | Ready |
| 10. Artifact | `artifact-manifest.json`, `mise run reproduce` | Offline AE path done (E2.4); venue kit deferred |

## Figures / tables inventory

Regenerate offline with `mise run reproduce` / `reproduce-check`:

| Figure | Path | Notes |
| --- | --- | --- |
| RQ1 frontier | `docs/research/e2-1-dispatch-frontier.md` | Mock rate cards |
| RQ2 metrics | `docs/research/rq2-memoization-metrics.json` | Mock provider |
| Competitive scoreboard | `docs/benchmarks/compare-scoreboard.{json,md}` | Mixed measured / harness_ready |
| Load-path bench | `docs/benchmarks/2026-07-12-v1.md` | Needs Postgres to refresh |
| COPY spike | `docs/spikes/s1-3-postgres-copy.md` | Historical |

## Red-team checklist (pre-submit)

Against architecture §2 honest caveats:

- [ ] Do not claim exactly-once delivery
- [ ] Do not claim warehouse AI SQL is always worse — orientation states when it wins
- [ ] Separate offline mock frontiers from live Bedrock/vLLM numbers
- [ ] Machine specs and regenerate commands on every figure
- [ ] Competitor rows marked `harness_ready` until a dated measured report exists

## Open decisions (block full E2.5)

1. **Venue** — VLDB / CIDR / SIGMOD demo / workshop (architecture §18 shortlist).
2. **Live frontier runs** — S2.2 credentials/budget.
3. **E2.3 measured competitor legs** — Redpanda Connect / DocETL / warehouse AI.

## Next offline writing steps (no venue required)

1. Draft §1–3 and §8–9 from architecture + orientation (prose only).
2. Freeze figure captions that cite offline regenerate commands.
3. When a venue is chosen: ADR with deadline + page limit + AE rules; remap
   this outline into the venue template.
