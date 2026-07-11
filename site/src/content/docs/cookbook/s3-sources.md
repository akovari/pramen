---
title: S3 and MinIO sources
description: Reading Parquet and NDJSON from S3 or any S3-compatible object store, configured purely from the environment.
---

Sources accept `s3://` URLs for both Parquet and NDJSON. Everything about
the connection — credentials, region, endpoint — comes from the standard
`AWS_*` environment; nothing sensitive ever appears in the pipeline
document.

## The pipeline

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: s3-to-postgres
spec:
  source:
    type: object_store
    url: s3://my-bucket/events/     # trailing slash = scan the prefix
    format: { type: parquet }
  transforms:
    - id: shape
      type: sql
      query: SELECT id, category, amount FROM input WHERE category <> 'epsilon'
  sink:
    type: postgres
    target: analytics.events
    mode: append
```

## Configuration

| Variable | Meaning |
| --- | --- |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Credentials (or any mechanism the environment provides) |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | Region |
| `AWS_ENDPOINT` | Endpoint override for S3-compatible services |
| `AWS_ALLOW_HTTP` | Set `true` for plaintext local endpoints |

## Against MinIO

MinIO speaks the real S3 API, so the identical pipeline runs fully
locally (this is test layer L2 from the
[testing strategy](/pramen/cookbook/local-testing/)):

```console
$ docker run -d --name minio -p 9000:9000 minio/minio server /data

$ export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
$ export AWS_REGION=us-east-1 AWS_ENDPOINT=http://localhost:9000 AWS_ALLOW_HTTP=true
$ pramen run s3-to-postgres.yaml
run complete: 200000 rows in / 160000 rows out in 2.48s (64607 rows/s out, 28 batches, 4.9 MiB written)
```

That number is a measured local run: four Parquet files (200k rows)
streamed out of MinIO, filtered in SQL, and loaded into PostgreSQL in one
transaction.

## Current limits

- Azure Blob and GCS are tracked in X1.5; using an `az://` or `gs://` URL
  fails at plan time with that pointer.
- Checkpointed (incremental) runs currently enumerate local sources only;
  remote work-unit enumeration is the remainder of P1.1. An `s3://` source
  with `runtime.checkpoint` set fails at plan time rather than silently
  running without resumability.
