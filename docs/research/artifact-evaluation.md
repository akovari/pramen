# Artifact evaluation checklist (E2.4)

Companion to architecture §18 and
[`artifact-manifest.json`](./artifact-manifest.json).
This is the checklist a reviewer (or future agent) should be able to
complete from a clean clone without private credentials for the **offline**
core. Cloud / competitor legs are optional and budget-capped.

## One command

```bash
mise run reproduce          # regenerate offline figures into docs/
mise run reproduce-check    # fail if committed figures drift
# optional (local Postgres):
PRAMEN_POSTGRES_DSN=… mise run reproduce -- --with-postgres
```

Equivalent: `./scripts/reproduce-artifact.sh` / `--check`.

## What regenerates offline (PR-safe)

| Figure | Output | Notes |
| --- | --- | --- |
| RQ2 memoization | `docs/research/rq2-memoization-metrics.json` | Mock provider + temp SQLite; CI also pins via `cargo test -p pramen-ai reuse` |
| E2.1 dispatch frontier | `docs/research/e2-1-dispatch-frontier.md` | Analytical / mock-calibrated — not live Bedrock |
| Competitive scoreboard | `docs/benchmarks/compare-scoreboard.{json,md}` | Pulls RQ2 rows; orientation prose is hand-written |

## What needs a local service

| Figure | Requirement |
| --- | --- |
| Bench suite v1 report | `PRAMEN_POSTGRES_DSN` + `./scripts/bench.sh` (machine-dependent ranges; refresh the dated report deliberately) |

## What needs cloud / competitor tooling (not AE-blocking)

| Scenario | Gate |
| --- | --- |
| Redpanda Connect AI | `COMPARE_REDPANDA=1` + Connect CLI + provider credentials — `compare/redpanda-connect/` |
| DocETL extraction | `COMPARE_DOCETL=1` + DocETL + provider credentials — `compare/docetl/` |
| Live Bedrock frontier (S2.2) | AWS credentials; budget alarm |
| P2.1 1M-record AWS acceptance | Explicit spend approval |

## Reviewer checklist

- [ ] Fresh clone; pinned toolchain via `rust-toolchain.toml` / `mise`
- [ ] `mise run reproduce-check` exits 0
- [ ] Spot-check: each numeric claim on the site/README links a report under
      `docs/benchmarks/`, `docs/spikes/`, or `docs/research/`
- [ ] Scoreboard scenarios marked `harness_ready` / `deferred` are not
      presented as measured
- [ ] Machine notes and methodology exist for any wall-time claim cited
- [ ] (Optional) Postgres bench regenerates without changing conclusions
      beyond published ranges, or a new dated report is added
- [ ] (Optional) Cloud legs: budget documented before run; results land as
      dated reports before scoreboard status flips to `measured`

## Venue note

Natural targets (architecture §18): VLDB / CIDR / SIGMOD demo; workshop
venues for early results. This artifact is the evaluation section of the
paper: generators, pins, configs, and raw results — not a separate
slideware dump. Venue-specific packaging (DOI zip layout, ACM AE badge
forms) is deferred to E2.5 when the venue ADR is fixed.
