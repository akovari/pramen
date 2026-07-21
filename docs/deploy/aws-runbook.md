# AWS deployment runbook (X2.2)

Documented, reproducible deployment of a Pramen worker for the first
vertical: **S3 → SQL → governed Bedrock extract → Aurora PostgreSQL**,
with structured logs and optional OTLP metrics.

Live apply is **not** required to complete this task. Someone with AWS
access should be able to follow this file end to end. Artifacts live under
[`deploy/`](../../deploy/).

Companion site page: [Deploying on AWS](https://akovari.github.io/pramen/cookbook/aws-deploy/).

## Prerequisites

| Piece | Notes |
| --- | --- |
| Pramen binary | Release musl build from GitHub Releases, or `cargo build --release -p pramen` |
| PostgreSQL | Aurora PostgreSQL (or RDS) reachable from the worker; SSL preferred |
| Object storage | S3 bucket/prefix with Parquet (or NDJSON) inputs in the pinned region |
| AI | Bedrock model access in `eu-central-1` (architecture § first hosted profile) |
| Observability (optional) | OTLP HTTP collector on `:4318` (ADOT, Alloy, or `deploy/container` Compose) |
| Host | Linux x86_64 or aarch64; systemd **or** container runtime |

Confirm offline before touching AWS:

```bash
pramen validate deploy/examples/aws-s3-to-aurora.yaml
pramen explain deploy/examples/aws-s3-to-aurora.yaml
./scripts/validate-deploy.sh
```

## IAM hints

Use the **default credential chain** (instance profile, ECS task role, EKS
IRSA). Do not put long-lived access keys in pipeline YAML.

Minimum capabilities for the example profile:

| Service | Actions (indicative) | Why |
| --- | --- | --- |
| S3 (input) | `s3:ListBucket` on the bucket; `s3:GetObject` on the prefix | Source scan + read |
| S3 (Bedrock batch staging, if used) | `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on a staging prefix | Provider-batch JSONL |
| Bedrock | `bedrock:InvokeModel`, `bedrock:Converse`; batch APIs if `execution: batch` | Semantic transforms |
| Aurora / RDS | Network reachability + DB user with `INSERT`/`COPY` (and `CREATE` if bootstrapping) | Sink |
| CloudWatch (optional) | `logs:CreateLogStream`, `logs:PutLogEvents` if shipping journald/container logs | Ops |

Tighten to least privilege for the exact bucket ARNs and model IDs. Pin
`AWS_REGION` / `AWS_DEFAULT_REGION` to `eu-central-1` and keep
`runtime.residency.allowedLocations: [eu-central-1]` in the pipeline so
cross-region drift fails validation.

## Networking

- Worker → S3: gateway or interface VPC endpoint recommended.
- Worker → Bedrock: interface endpoint or public regional endpoint; no
  cross-region inference in the v1 profile.
- Worker → Aurora: security group allows the worker SG/CIDR on `5432`
  (or the custom port); prefer private subnets.
- Worker → OTLP collector: private hop to `:4318` (HTTP/protobuf).

TLS: use `sslmode=require` (or verify-full with the AWS CA) on
`PRAMEN_POSTGRES_DSN`.

## Profiles

### A. systemd (EC2 / bare metal)

1. Install the binary to `/usr/local/bin/pramen`.
2. Create user/dirs and copy units:

   ```bash
   useradd --system --home /var/lib/pramen --create-home --shell /usr/sbin/nologin pramen
   install -d -o pramen -g pramen /var/lib/pramen/{checkpoints,ledger} /etc/pramen
   install -m 0644 deploy/systemd/pramen.service deploy/systemd/pramen.timer \
     /etc/systemd/system/
   install -m 0640 -o root -g pramen deploy/systemd/pramen.env.example \
     /etc/pramen/pramen.env
   install -m 0644 deploy/examples/aws-s3-to-aurora.yaml /etc/pramen/pipeline.yaml
   ```

3. Edit `/etc/pramen/pramen.env` and `/etc/pramen/pipeline.yaml` (bucket,
   DSN, checkpoint URL). Prefer `PRAMEN_LEDGER_PATH=postgres://…` for fleets.
4. `systemctl daemon-reload && systemctl enable --now pramen.timer`
5. One-shot: `systemctl start pramen.service && journalctl -u pramen.service -e`

`Type=oneshot` matches today's run-to-completion CLI. Checkpointed sources
make periodic timer runs incremental.

### B. Container (ECS/Fargate or Compose lab)

- Image: `deploy/container/Dockerfile` (builds the release CLI).
- Lab stack: `deploy/container/compose.yaml` (Postgres + OTLP collector).
- Production: run the same image with task role credentials, inject env from
  Secrets Manager / SSM (never bake secrets into the image), mount or bake
  the pipeline YAML, set `PRAMEN_OTLP_ENDPOINT` to the collector.

```bash
docker compose -f deploy/container/compose.yaml up -d postgres otel-collector
docker compose -f deploy/container/compose.yaml run --rm --no-deps pramen \
  run --smoke --log-format json \
  --otlp-endpoint http://otel-collector:4318 \
  /etc/pramen/pipeline.yaml
```

(Adjust the pipeline for local/MinIO when AWS is unavailable.)

## First-run smoke

On a host that already has AWS + Aurora reachability:

```bash
set -a && source /etc/pramen/pramen.env && set +a
pramen validate "$PRAMEN_PIPELINE"
pramen run --smoke --smoke-rows 50 --log-format json \
  --otlp-endpoint "${PRAMEN_OTLP_ENDPOINT}" \
  "$PRAMEN_PIPELINE"
pramen ai status
```

Expect: capped rows, semantic token ceiling, **no** checkpoint mutation in
smoke mode, a few cents of Bedrock spend at most, and OTLP counters for the
run if the collector is up.

## Crash / restart

- **Mid-run kill:** checkpointed work units that completed stay completed;
  incomplete units re-run. Ledger reuse prevents re-billing finished semantic
  work keys.
- **After Bedrock batch submit:** job ids are durable; restart reconciles by
  job/item id (see architecture §11 / P1.8).
- **systemd:** timer or `systemctl start pramen.service` resumes; inspect
  `journalctl -u pramen.service`.
- **Smoke runs** neither consult nor update the checkpoint store — use a
  non-smoke run for production incremental behavior.

## Logs and metrics checks

### Logs

```bash
journalctl -u pramen.service -o cat | head
# or container logs — expect JSON lines with envelope keys:
# timestamp, level, target, message  (pinned in observe.rs)
```

Useful message fragments: `checkpoint store consulted`, `run complete`,
`smoke run:`, provider fault strings.

### Metrics (OTLP)

Exported **once at run end** to `PRAMEN_OTLP_ENDPOINT` / `--otlp-endpoint`:

| OTLP name | Meaning |
| --- | --- |
| `pramen.rows_in` / `pramen.rows_out` | Records |
| `pramen.batches_in` / `pramen.batches_out` | Batches |
| `pramen.bytes_in` / `pramen.bytes_out` | Arrow buffer bytes |
| `pramen.run_duration` | Wall seconds (gauge) |

Attribute: `pipeline=<metadata.name>`. Scrape via the Compose collector's
Prometheus exporter (`:8889`) and import
[`deploy/grafana/pramen-runtime.json`](../../deploy/grafana/pramen-runtime.json).

### §13 signals not yet on OTLP

Channel occupancy, stage latencies, retries, checkpoint age, rejected-record
counters, WASM instruments, AI queue/token/cost/latency/cache-hit metrics are
**not** in `export_metrics_otlp` today. Use `pramen ai status`, `ai review`,
`ai evaluate`, and JSON logs. The Grafana dashboard documents the gap list
inline — do not invent PromQL for missing series.

## Cost alarms pointer

- Enable **AWS Budgets** (or Cost Anomaly Detection) on Bedrock + Aurora +
  NAT/data transfer for the account/OU that runs Pramen.
- Pipeline-side: set `budget.maxRunTokens` / per-record caps; use
  `pramen run --smoke` before large backfills.
- Project convention: weekly cloud acceptance stays budget-alarmed (ADR 0005);
  PR CI never holds cloud credentials.

## Validation without AWS

```bash
./scripts/validate-deploy.sh
cargo nextest run -p pramen deploy_artifacts
```

These checks parse unit files and dashboard JSON only; they do not call AWS.
