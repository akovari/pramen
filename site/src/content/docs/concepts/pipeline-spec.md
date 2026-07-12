---
title: The pipeline document
description: One versioned YAML file describes the whole pipeline. Strict parsing, helpful validation.
---

A pipeline is a single YAML document with a versioned schema
(`pramen.dev/v1alpha1`). The document is an API: parsing is strict, every
semantic problem is reported with a path, and the accepted surface is
published as a [JSON Schema](https://github.com/akovari/pramen/blob/main/docs/schema/pipeline.v1alpha1.schema.json)
your editor can use for completion and inline validation.

## The complete shape

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: governed-semantic-enrichment   # lowercase, digits, hyphens
spec:
  models:                              # named model configs for ai.* steps
    enrichment:
      provider: bedrock
      model: anthropic.claude-3-haiku-20240307-v1:0
      region: eu-central-1
  source:
    type: object_store
    url: s3://example-input/events/
    format:
      type: parquet                    # or ndjson
  transforms:                          # ordered; may be empty
    - id: normalize
      type: sql
      query: >
        SELECT ticket_id, lower(trim(description)) AS description, created_at
        FROM input
        WHERE description IS NOT NULL
    - id: classify
      type: ai.extract
      model: enrichment                # must reference spec.models
      execution: auto                  # auto | online | batch
      inputs: [description]
      instruction: Classify the record into a business category and explain why.
      output:
        fields:
          - { name: category, type: utf8, nullable: false }
          - { name: rationale, type: utf8, nullable: false }
      validation:
        onInvalid: review              # fail | drop | review
      budget:
        maxInputTokensPerRecord: 4096
        maxOutputTokensPerRecord: 256
        maxRunTokens: 2000000          # hard per-run ceiling; reuse is free
      breaker:
        maxConsecutiveInvalid: 25      # error-spike circuit breaker (default)
  sink:
    type: postgres
    target: analytics.events           # qualified schema.table
    mode: upsert                       # append | upsert
    keys: [id]                         # merge keys (upsert only)
    dsnEnv: PRAMEN_POSTGRES_DSN        # env var holding the connection string
  runtime:
    targetBatchBytes: 8388608
    maxInflightBytes: 268435456
    checkpoint:
      url: file:///var/lib/pramen/checkpoints/
```

## Principles baked into the schema

**Unknown fields are errors.** A typo like `qurey:` fails validation
instead of being silently ignored. This is `deny_unknown_fields`
everywhere.

**All problems at once.** Validation walks the whole document and reports
every issue with a dotted path (`spec.transforms[1].model: references
undeclared model ...`), so a broken file is fixed in one round trip.

**Secrets stay out.** Connection strings and credentials never appear in
the document. The sink names an environment variable (`dsnEnv`); providers
use their native credential chains.

**Defaults are sensible and explicit.** `runtime` can be omitted entirely;
batch sizing, in-flight ceilings, and sink mode all have documented
defaults. A minimal movement pipeline is ~12 lines.

**Linear today, DAG-ready.** The document describes a linear pipeline while
the internal plan is free to become a DAG later — fan-out will arrive
without changing this shape.

## Field reference

The exhaustive field-by-field reference, including types and defaults, is
on the [pipeline schema page](/pramen/reference/pipeline-schema/).
