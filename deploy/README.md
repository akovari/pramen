# Deploy artifacts (X2.2)

Reproducible profiles for running Pramen as a worker against AWS (S3 →
SQL → governed AI → Aurora PostgreSQL) or a local stand-in.

| Path | Purpose |
| --- | --- |
| [`systemd/`](systemd/) | Unit + timer + env template for `pramen run` |
| [`container/`](container/) | Dockerfile + Compose (Postgres + optional OTLP collector) |
| [`grafana/`](grafana/) | Dashboard JSON for §13 signals that exist today |
| [`examples/`](examples/) | Example pipeline + env template (no secrets) |
| [`../docs/deploy/aws-runbook.md`](../docs/deploy/aws-runbook.md) | Operator runbook |

Validate offline (no AWS):

```bash
./scripts/validate-deploy.sh
# or: mise run validate-deploy
```
