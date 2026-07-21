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
| `batch` | object? | Bedrock provider-batch only: `{ roleArn, s3 }` — an IAM role and `s3://` staging prefix for model invocation jobs. `openai-compat` batches through the provider's Files API and needs no `batch` block. |
| `batch.roleArn` | string | IAM service role the job assumes to read/write staged objects |
| `batch.s3` | string | `s3://` staging prefix (inputs and results land under this prefix) |

## `spec.source`

| Field | Type | Notes |
| --- | --- | --- |
| `type` | string | `object_store` |
| `url` | string | Local path / `file://`, `s3://`, `gs://`, `az://` / `abfs(s)://` (and Azure HTTPS hosts) |
| `location` | string? | Declared storage region/location; **required** when `runtime.residency` is set (cloud sources) |
| `format.type` | string | `parquet` or `ndjson` |

## `spec.transforms[]`

Ordered list; may be empty. Every entry needs a unique `id`.

### `type: sql`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Unique step id |
| `query` | string | DataFusion SQL; the incoming stream is the table `input` |

### `type: ai.extract` / `type: ai.classify` / `type: ai.generate`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Unique step id |
| `model` | string | Must reference a key in `spec.models` |
| `execution` | string | `auto` *(default)*, `online`, or `batch` (asynchronous provider-batch job with crash reconciliation; requires a batch-capable provider). `auto` runs the cost model when `dispatch` hints are set; otherwise online. |
| `dispatch` | object? | Planning inputs for `execution: auto`: `expectedRecords`, `deadlineSeconds`, optional `inputTokensPerRecord` / `outputTokensPerRecord` / `rateCard` (`mock`, `openai-compat-stub`, `bedrock-illustrative`) |
| `inputs` | string[] | Input column names (at least one) |
| `instruction` | string | The fixed instruction; part of the work key |
| `output.fields[]` | object[] | `{ name, type, nullable, maxChars? }`; at least one, unique names |
| `output.fields[].type` | string | `utf8`, `int64`, `float64`, `bool`, `timestamp` — `ai.generate` allows only `utf8` |
| `output.fields[].maxChars` | int? | Positive Unicode scalar limit for `utf8` fields; **required** on every `ai.generate` field; over-long model output fails validation (never truncated) |
| `validation.onInvalid` | string | `fail` *(default)*, `drop`, or `review` |
| `budget.maxInputTokensPerRecord` | int? | Positive; per-record pre-dispatch gate |
| `budget.maxOutputTokensPerRecord` | int? | Positive; provider-side cap + post-validation recheck; **required** for `ai.generate` |
| `budget.maxRunTokens` | int? | Positive; hard per-run ceiling on provider-reported tokens — ledger reuse is free |
| `breaker.maxConsecutiveInvalid` | int | Consecutive invalid outputs that abort the run; default 25, always armed |

### `type: wasm`

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Unique step id |
| `component` | string | Local `.wasm` path (absolute or relative to the pipeline file), or digest-pinned `oci://registry/repo@sha256:…` (tag-only rejected) |
| `limits.memoryMb` | int? | Guest linear memory ceiling in mebibytes; default 256 |
| `limits.fuel` | int? | Wasmtime fuel budget per batch; default 10_000_000_000 |
| `limits.maxInputMb` | int? | Maximum Arrow IPC input size in mebibytes; default 64 |
| `limits.maxOutputMb` | int? | Maximum Arrow IPC output size in mebibytes; default 64 |

The guest implements the WIT ABI in `crates/pramen-wasm/wit/transform.wit`:
Arrow IPC bytes in, Arrow IPC bytes out. Build a guest from
`templates/wasm-transform-rust/` and validate with `pramen transform test`.
OCI pulls are fail-closed unless listed in `runtime.wasmOciAllowlist` or
`PRAMEN_WASM_OCI_ALLOWLIST`.

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
| `checkpoint.url` | string? | Checkpoint store: local path / `file://`, or `postgres://` / `postgresql://` for the shared fleet backend |
| `wasmOciAllowlist` | string[]? | Digests (`sha256:…`) and/or `registry/repository` prefixes permitted for OCI WASM pulls; empty + empty env denies all OCI pulls |
| `residency.allowedLocations` | string[]? | When set (non-empty), cloud `source.location` and every `models.*.region` must be in this list |
| `residency.allowedSchemes` | string[]? | Optional cloud URL scheme allow-list (`s3`, `gs`, `az`, …); local/`file` always permitted |

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
