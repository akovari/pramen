---
title: Pipeline schema reference
description: Every field of the pramen.dev/v1alpha1 pipeline document.
---

The machine-readable source of truth is the generated
[JSON Schema](https://github.com/akovari/pramen/blob/main/docs/schema/pipeline.v1alpha1.schema.json);
point your editor's YAML language server at it for completion and inline
validation. This page is the human-readable companion.

Unknown fields are rejected everywhere. Fields marked *default* may be
omitted.

## Top level

| Field | Type | Notes |
| --- | --- | --- |
| `apiVersion` | string | Must be `pramen.dev/v1alpha1` |
| `kind` | string | Must be `Pipeline` |
| `metadata.name` | string | 1–63 chars: lowercase letters, digits, interior hyphens |
| `spec` | object | The pipeline body |

## `spec.models`

Optional map of model names → configurations, referenced by `ai.*`
transforms.

| Field | Type | Notes |
| --- | --- | --- |
| `provider` | string | Adapter id, e.g. `bedrock`, `openai-compat` |
| `model` | string | Provider-specific model identifier |
| `region` | string? | Provider region pin |
| `endpoint` | string? | Endpoint override (self-hosted / stubbed providers) |

## `spec.source`

| Field | Type | Notes |
| --- | --- | --- |
| `type` | string | `object_store` |
| `url` | string | Local path or `file://` today; `s3://` etc. planned |
| `format.type` | string | `parquet` or `ndjson` (NDJSON execution planned) |

## `spec.transforms[]`

Ordered list; may be empty. Every entry needs a unique `id`.

### `type: sql`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Unique step id |
| `query` | string | DataFusion SQL; the incoming stream is the table `input` |

### `type: ai.extract` / `type: ai.classify`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Unique step id |
| `model` | string | Must reference a key in `spec.models` |
| `execution` | string | `auto` *(default, resolves to online)*, `online`, or `batch` (asynchronous provider-batch job with crash reconciliation; requires a batch-capable provider) |
| `inputs` | string[] | Input column names (at least one) |
| `instruction` | string | The fixed instruction; part of the work key |
| `output.fields[]` | object[] | `{ name, type, nullable }`; at least one, unique names |
| `output.fields[].type` | string | `utf8`, `int64`, `float64`, `bool`, `timestamp` |
| `validation.onInvalid` | string | `fail` *(default)*, `drop`, or `review` |
| `budget.maxInputTokensPerRecord` | int? | Positive; per-record pre-dispatch gate |
| `budget.maxOutputTokensPerRecord` | int? | Positive; provider-side cap |
| `budget.maxRunTokens` | int? | Positive; hard per-run ceiling on provider-reported tokens — ledger reuse is free |
| `breaker.maxConsecutiveInvalid` | int | Consecutive invalid outputs that abort the run; default 25, always armed |

## `spec.sink`

| Field | Type | Notes |
| --- | --- | --- |
| `type` | string | `postgres` |
| `target` | string | Qualified `schema.table` |
| `mode` | string | `append` *(default)* or `upsert` (stage + merge on `keys`) |
| `keys` | string[] | Merge-key columns; required (non-empty, unique) for `upsert`, forbidden for `append`; the target needs a unique index over exactly these columns |
| `dsnEnv` | string | Env var holding the connection string; default `PRAMEN_POSTGRES_DSN` |

## `spec.runtime`

Entirely optional.

| Field | Type | Notes |
| --- | --- | --- |
| `targetBatchBytes` | int | Target Arrow batch size; default 8 MiB |
| `maxInflightBytes` | int | In-flight ceiling; default 256 MiB; must be ≥ `targetBatchBytes` |
| `checkpoint.url` | string? | Checkpoint directory (local path or `file://`); enables incremental, resumable runs |

## Validation behavior

`pramen validate` reports **every** issue in one pass, each with a dotted
path into the document:

```text
pramen: pipeline.yaml has 3 validation issue(s):
  - metadata.name: `Bad_Name` must be 1-63 characters of lowercase letters, digits, and interior hyphens
  - spec.transforms[1].model: references undeclared model `missing`; declared models: [enrichment]
  - spec.sink.target: `events` must be a qualified `schema.table` name
```

Exit codes: `0` valid, `2` invalid, `1` unreadable file or internal error.
