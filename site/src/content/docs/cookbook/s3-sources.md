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

## Incremental (checkpointed) S3 runs

Checkpointed runs work on `s3://` prefixes exactly as on local
directories: set `runtime.checkpoint` and each object becomes a work
unit whose identity (key, size, last-modified) comes from a single
`LIST` request — no per-object round trips. A replay loads nothing; a
grown prefix loads only the new objects:

```console
$ pramen run s3-tickets.yaml
checkpoint store consulted total_units=2 pending_units=2
run complete: 20000 rows in / 16000 rows out in 844.54ms

$ pramen run s3-tickets.yaml
nothing to do: all 2 work unit(s) under the source are already completed in the checkpoint store

$ # a third object lands under the prefix
$ pramen run s3-tickets.yaml
checkpoint store consulted total_units=3 pending_units=1
run complete: 10000 rows in / 8000 rows out in 626.99ms
```

(Measured against MinIO; the target table ended at exactly the sum of
all three objects' filtered rows, zero duplicates.) Objects on S3 are
immutable and their last-modified only changes on overwrite, so an
overwritten object is correctly treated as new work (ADR 0006).

## Current limits

- Azure Blob and GCS are tracked in X1.5; using an `az://` or `gs://` URL
  fails at plan time with that pointer.
- Prefix listing is single-level (matching DataFusion's scan): objects in
  nested "subdirectories" of the prefix are not enumerated.
