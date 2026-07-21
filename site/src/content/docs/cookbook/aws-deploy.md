---
title: Deploying on AWS
description: Systemd and container profiles, OTLP dashboards for live metrics, and the operator runbook for S3 → Aurora.
---

Pramen's first production vertical is a single worker:
**S3 → SQL → governed Bedrock extract → Aurora PostgreSQL**. This page is
the short operator entry point; the full runbook with IAM and networking
detail lives in-repo at
[`docs/deploy/aws-runbook.md`](https://github.com/akovari/pramen/blob/main/docs/deploy/aws-runbook.md).
Deploy artifacts (units, Compose, Grafana JSON, example pipeline) are under
[`deploy/`](https://github.com/akovari/pramen/tree/main/deploy).

Live AWS apply is optional for contributors — PR CI stays offline. With
credentials and network access, the runbook is enough to reproduce the
deployment.

## What you get

| Artifact | Role |
| --- | --- |
| `deploy/systemd/pramen.service` + `pramen.timer` | Periodic oneshot `pramen run` with env file |
| `deploy/container/Dockerfile` + `compose.yaml` | Image + Postgres + OTLP collector lab stack |
| `deploy/grafana/pramen-runtime.json` | Panels for metrics actually exported today |
| `deploy/examples/aws-s3-to-aurora.yaml` | Example pipeline (placeholders, no secrets) |

## Metrics that exist today

`pramen run --otlp-endpoint` (or `PRAMEN_OTLP_ENDPOINT`) pushes a **one-shot**
OTLP HTTP/protobuf export at run end from
`crates/pramen-core/src/observe.rs`:

- `pramen.rows_in` / `pramen.rows_out`
- `pramen.batches_in` / `pramen.batches_out`
- `pramen.bytes_in` / `pramen.bytes_out`
- `pramen.run_duration` (seconds)

Architecture §13 also lists channel occupancy, stage latencies, retries,
checkpoint age, rejected records, WASM instruments, and the full AI
queued/token/cost/cache set. Those are **not** OTLP series yet — use
`pramen ai status`, review/evaluate commands, and `--log-format json`. The
Grafana dashboard states the gaps explicitly so panels stay honest.

## Quick local lab (no AWS)

```bash
./scripts/validate-deploy.sh
docker compose -f deploy/container/compose.yaml up -d postgres otel-collector
# Point a deterministic example at local Postgres, then:
# docker compose -f deploy/container/compose.yaml run --rm pramen run ...
```

## First AWS smoke

1. Fill `deploy/systemd/pramen.env.example` (or Compose `.env`) — DSN, region
   `eu-central-1`, OTLP URL. Prefer an instance/task role over static keys.
2. Edit bucket and table names in the example pipeline; keep
   `runtime.residency.allowedLocations: [eu-central-1]`.
3. `pramen validate` → `pramen run --smoke` → `pramen ai status`.
4. Import `deploy/grafana/pramen-runtime.json` against the collector's
   Prometheus exporter.

See the [runbook](https://github.com/akovari/pramen/blob/main/docs/deploy/aws-runbook.md)
for IAM hints, crash/restart, and cost-alarm pointers.
