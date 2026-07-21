---
title: Object-store sources (S3, Azure, GCS)
description: Reading Parquet and NDJSON from S3, Azure Blob, or GCS with environment credentials and optional residency checks.
---

Sources accept cloud object-store URLs for both Parquet and NDJSON.
Credentials and endpoints come from the environment — nothing sensitive
belongs in the pipeline document.

## Supported URL schemes

| Scheme | Store |
| --- | --- |
| `s3://` | Amazon S3 and S3-compatible (MinIO, …) |
| `gs://` | Google Cloud Storage |
| `az://`, `azure://`, `abfs://`, `abfss://`, `adl://` | Azure Blob / Data Lake |
| `https://{account}.blob.core.windows.net/…` | Azure Blob (HTTPS form) |
| `file://` or bare path | Local filesystem |

## The pipeline

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: cloud-to-postgres
spec:
  source:
    type: object_store
    url: gs://my-bucket/events/     # or s3://… / az://account/container/…
    location: europe-west1          # required when runtime.residency is set
    format: { type: parquet }
  transforms:
    - id: shape
      type: sql
      query: SELECT id, category, amount FROM input WHERE category <> 'epsilon'
  sink:
    type: postgres
    target: analytics.events
    mode: append
  runtime:
    residency:
      allowedLocations: [europe-west1, eu-central-1]
      # allowedSchemes: [gs, s3]   # optional scheme allow-list
```

## Credentials

| Store | Variables |
| --- | --- |
| S3 / MinIO | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, optional `AWS_ENDPOINT` / `AWS_ALLOW_HTTP` |
| GCS | `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, or ADC |
| Azure | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_ACCESS_KEY` (or Azure AD), optional `AZURE_STORAGE_ENDPOINT` / `AZURE_ALLOW_HTTP` for Azurite |

## Residency (offline)

When `runtime.residency` is set:

1. every cloud source must declare `source.location`;
2. that location and every `models.*.region` must appear in
   `allowedLocations`;
3. optional `allowedSchemes` restricts URL schemes.

Validation is declaration-only — no live cloud lookups (ADR 0005).

## Incremental (checkpointed) runs

Set `runtime.checkpoint.url` to a local `file://…` directory or a
`postgres://…` / `postgresql://…` DSN for the shared fleet store. Each
object becomes a work unit whose identity (key, size, last-modified)
comes from a single `LIST`. Replay loads nothing; a grown prefix loads
only new objects.

For a MinIO-focused walkthrough see the [S3 and MinIO](/pramen/cookbook/s3-sources/)
page (still valid for `s3://`).
