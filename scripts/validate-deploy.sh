#!/usr/bin/env bash
# Offline smoke check for X2.2 deploy artifacts (no AWS, no Docker build).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail() { echo "validate-deploy: $*" >&2; exit 1; }
need() { [[ -f "$1" ]] || fail "missing $1"; }

echo "validate-deploy: checking tree under $root/deploy"

need deploy/systemd/pramen.service
need deploy/systemd/pramen.timer
need deploy/systemd/pramen.env.example
need deploy/container/Dockerfile
need deploy/container/compose.yaml
need deploy/container/otel-collector.yaml
need deploy/grafana/pramen-runtime.json
need deploy/examples/aws-s3-to-aurora.yaml
need deploy/examples/env.example
need docs/deploy/aws-runbook.md

# systemd: required stanzas
for key in '\[Unit\]' '\[Service\]' '\[Install\]' 'ExecStart=' 'EnvironmentFile='; do
  grep -qE "$key" deploy/systemd/pramen.service \
    || fail "pramen.service missing $key"
done
grep -qE '\[Timer\]' deploy/systemd/pramen.timer || fail "pramen.timer missing [Timer]"
grep -q 'PRAMEN_POSTGRES_DSN=' deploy/systemd/pramen.env.example \
  || fail "env example missing PRAMEN_POSTGRES_DSN"
grep -q 'PRAMEN_OTLP_ENDPOINT=' deploy/systemd/pramen.env.example \
  || fail "env example missing PRAMEN_OTLP_ENDPOINT"
grep -q 'PRAMEN_LEDGER_PATH=' deploy/systemd/pramen.env.example \
  || fail "env example missing PRAMEN_LEDGER_PATH"

# No obvious secrets in templates
if grep -REni \
  -e 'AKIA[0-9A-Z]{16}' \
  -e 'aws_secret_access_key\s*=\s*[^C\s]' \
  -e 'BEGIN (RSA |OPENSSH )?PRIVATE KEY' \
  deploy/systemd/pramen.env.example deploy/examples/env.example 2>/dev/null; then
  fail "possible secret material in env templates"
fi

# Grafana dashboard + collector config parse as JSON / YAML-ish JSON
command -v jq >/dev/null || fail "jq is required"
jq -e '
  .uid == "pramen-runtime-otlp"
  and (.panels | length) >= 4
  and (.title | test("Pramen"))
' deploy/grafana/pramen-runtime.json >/dev/null \
  || fail "grafana dashboard JSON invalid or missing expected fields"

# OTLP names from observe.rs must appear in the dashboard description/panels
for metric in pramen.rows_in pramen.rows_out pramen.batches_in pramen.bytes_in pramen.run_duration; do
  grep -q "$metric" deploy/grafana/pramen-runtime.json \
    || fail "dashboard does not mention OTLP metric $metric"
done

# Example pipeline is v1alpha1 YAML with expected skeleton keys
grep -q 'apiVersion: pramen.dev/v1alpha1' deploy/examples/aws-s3-to-aurora.yaml \
  || fail "example pipeline missing apiVersion"
grep -q 'provider: bedrock' deploy/examples/aws-s3-to-aurora.yaml \
  || fail "example pipeline missing bedrock model"
grep -q 'eu-central-1' deploy/examples/aws-s3-to-aurora.yaml \
  || fail "example pipeline not pinned to eu-central-1"
grep -q 'CHANGE_ME' deploy/examples/aws-s3-to-aurora.yaml \
  || fail "example pipeline should keep CHANGE_ME placeholders (no real bucket)"

# Dockerfile / compose sanity
grep -q '/usr/local/bin/pramen' deploy/container/Dockerfile \
  || fail "Dockerfile missing pramen binary path"
grep -q 'otel-collector' deploy/container/compose.yaml \
  || fail "compose missing otel-collector service"
grep -q 'otlp:' deploy/container/otel-collector.yaml \
  || fail "collector config missing otlp receiver"

echo "validate-deploy: ok"
