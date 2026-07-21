# Pramen: Product and Architecture Direction

Status: exploratory design  
Last reviewed: 2026-07-10

## 1. Thesis

Pramen is an open-source, multiplatform data movement and transformation
runtime. It moves bounded and unbounded streams of structured data through a
directed graph of sources, transforms, and sinks.

Its intended strengths are:

- high throughput and predictable memory use from an Apache Arrow data plane;
- one execution model for batch and streaming;
- governed, schema-bound LLM transformations with durable result reuse;
- declarative SQL/expression transforms out of the box, with sandboxed,
  ahead-of-time compilable WebAssembly components as the extension mechanism;
- strong connectivity to object storage and analytical databases;
- a small, operable runtime rather than a distributed database — one static
  binary with no native driver dependencies in its lean profile;
- portable deployment as a single worker process, with shared-nothing
  horizontal scaling where the source supports partitioning.

The first vertical slice is:

> Read Parquet files from S3; normalize with built-in SQL/expression
> transforms; apply a schema-bound semantic extraction; bulk-load the enriched
> result into Amazon Aurora PostgreSQL or RDS for PostgreSQL — from one static
> binary with no external services.

The initial product is a standalone CLI and daemon deployed by platform or data
teams. It is optimized for throughput and cost efficiency; seconds-scale
buffering is acceptable when it improves batch density and destination load
efficiency.

## 2. Product position

Pramen should not initially claim to be a replacement for Apache Flink, Spark,
RisingWave, or Arroyo. Those systems solve distributed state, joins, windows,
checkpoint coordination, and cluster scheduling. Rebuilding those capabilities
would obscure the smaller and more credible opportunity.

Pramen instead occupies the space between connector-first tools and distributed
compute engines:

- more general-purpose and columnar than Vector;
- more data-intensive and batch-aware than Redpanda Connect;
- much smaller operationally than Flink or Spark;
- more programmable than a collection of managed transfer services;
- more operationally governed and database-oriented than an LLM workflow
  library;
- independent of any single warehouse and model host, unlike in-warehouse AI
  SQL functions;
- focused on movement and transformation, not continuously queryable state.

### Existing solutions and lessons

**Vector** validates the source-transform-sink topology, compiled safe
transformations, backpressure, and a single efficient binary. Its primary data
model and ecosystem remain observability-oriented. Pramen should borrow its
operational discipline, not imitate its event model or build a custom language
in v1.

**Redpanda Connect** validates configuration-driven pipelines and broad
connectivity. Its connector catalog is a major competitive advantage, while
Bloblang gives it an approachable mapping experience. Pramen cannot win a
connector-count contest early; it must make Arrow-native bulk movement and
portable compiled transforms meaningfully better.

Redpanda Connect also already ships AI processors for OpenAI, AWS Bedrock,
Vertex AI, and Ollama, so "an LLM step in a pipeline" is not novel. Those
processors make per-message online calls at on-demand pricing, place raw model
text into the message payload, and keep no durable record of completed
inference. A replayed or crashed pipeline re-bills every record, and nothing
validates output into typed columns. Pramen's semantic operators must be
honestly positioned against this: the differentiators are provider batch
scheduling, the durable result ledger, schema-bound typed output, budgets, and
review routing — not the existence of a model call.

**In-warehouse AI SQL** — Databricks `ai_query`, Snowflake Cortex AISQL, and
BigQuery `AI.GENERATE_TABLE` — is the strongest incumbent alternative for
structured LLM extraction, and any Pramen pitch that ignores it is not
credible. For data already resident in one of those platforms, with results
staying there, and with the platform's hosted models acceptable, those
functions are the default choice and Pramen should not pretend otherwise.

The warehouse approach carries structural costs Pramen avoids: data must be
ingested into the warehouse before enrichment and reverse-ETL'd out if the
destination is an operational database; model choice is limited to what the
platform hosts, complicating self-hosted or residency-constrained inference;
enrichment spend is coupled to warehouse compute pricing; and cross-run result
reuse is the user's problem. Pramen's wedge is precisely the data that is not
in — or not destined for — a single warehouse.

**Arroyo, RisingWave, and Flink** own stateful distributed stream processing.
Their existence is the strongest reason to defer cluster coordination and
exactly-once stateful operators. Arroyo also validates Rust, Arrow, DataFusion,
and object-store-backed state, so those technologies alone are not a
differentiator.

**Airbyte and Meltano** demonstrate that connector availability, packaging,
documentation, and maintenance often matter more to users than engine
benchmarks. Every Pramen connector therefore needs an explicit support level,
compatibility matrix, and conformance suite.

**DuckDB, DataFusion, Polars, and Velox** show the performance advantages of
columnar execution. Pramen should consume these ecosystems rather than build a
general query engine.

**DocETL** is the closest match for LLM-powered data processing. It provides
declarative semantic map, filter, reduce, resolve, and extraction operators and
can optimize pipelines for quality and cost. Pramen should not compete by
merely adding prompts to ETL. Its distinct bet is an Arrow-native systems
runtime with provider batch scheduling, durable content-addressed results,
strict provenance, and database delivery contracts.

**Palimpzest** demonstrates semantic operations as optimizable query operators.
**CocoIndex** demonstrates the value of incremental recomputation for AI-ready
data. Both reinforce that avoiding unnecessary model calls is more important
than optimizing the CPU overhead around them.

**Temporal and agent frameworks** address durable multi-step agents. Pramen
should not build an agent framework in v1. If agent transforms arrive later,
their model and tool calls must use a durable activity model rather than run as
an opaque in-memory function.

**BAML** is prior art for schema-first LLM contracts: typed function
signatures for model calls with validated structured output. Pramen applies
the same philosophy one layer down — the contract is a declared set of Arrow
columns rather than a language-level type, and validation feeds a durable
ledger rather than a program variable.

**Weavekit**, an agent-workflow orchestration playground, is not a competitor
but demonstrates project discipline worth copying: a controlled-vocabulary
document that defines each domain term with forbidden synonyms; decision
records that document *rejected* alternatives together with explicit reopen
triggers; a smoke preset that pins the cheapest model and caps work for fast
end-to-end runs; and structured JSONL progress events selectable per CLI
invocation. Its deliberate rejection of a durable work queue in favor of
in-process runs with node-boundary resume mirrors Pramen's
"checkpoint, don't coordinate" v1 posture.

### Distinct technical advantages

No single Pramen feature is unique. The defensible position is a combination
that no current system ships in one runtime:

1. **A columnar data plane under semantic operators.** DocETL and Palimpzest
   are Python/pandas-tier systems; Redpanda Connect and Vector move individual
   messages. Pramen moves Arrow batches end to end, so the deterministic 99% of
   a pipeline (decode, filter, normalize, load) runs at engine speed while the
   semantic 1% is dispatched deliberately.
2. **A durable, content-addressed inference ledger.** Completed model results
   are fingerprinted and reused across retries, restarts, replays, and
   incremental runs. No mainstream pipeline tool records inference as a
   first-class durable artifact; teams rebuild this as ad hoc caches.
3. **Provider batch APIs as a scheduling target.** Bedrock, OpenAI, Anthropic,
   and Gemini batch endpoints price at roughly half of on-demand. Pipeline
   AI processors in competing tools are online-only; Pramen treats
   "suspend durably, submit as provider batch, reconcile later" as a native
   execution mode, which its throughput-first posture makes natural.
4. **Schema-bound AI output as typed data.** Model responses are validated
   against declared Arrow columns with enumerations, nullability, evidence,
   and review routing — not raw text dropped into a message field.
5. **Sandboxed, polyglot deterministic transforms.** WASM components with
   capability-based isolation and resource limits, rather than trusted native
   plugins or a bespoke DSL.
6. **Database delivery contracts.** Bulk-loading strategies, idempotency
   modes, and destination load-impact testing are part of the product, not an
   exercise left to the reader.
7. **Provider-neutral residency with uniform governance.** The same budgets,
   provenance, validation, and review semantics apply to Bedrock, hosted APIs,
   and self-hosted vLLM — including deployments where data must not leave a
   region or network.

### Business cases where Pramen should win

These scenarios name the current alternatives honestly. Each is chosen because
the existing options are genuinely awkward, not merely less fashionable.

**1. Enriching event and interaction data into an operational database.**
Support tickets, CRM interactions, or product feedback land in S3 as Parquet;
the business needs category, priority, intent, and entities as typed columns
in Aurora PostgreSQL powering an application. Today this takes either
(a) warehouse AI SQL plus two extra hops — ingest into the warehouse, enrich,
reverse-ETL to PostgreSQL — with model choice restricted to that platform's
hosting; (b) Redpanda Connect AI processors — online-only pricing, no result
reuse on replay, untyped text output; or (c) a custom Python service that the
team must make restart-safe, budgeted, and auditable themselves. Pramen does
this in one governed pipeline at provider batch pricing with idempotent bulk
loading.

**2. The large semantic backfill that must survive interruption.** A
several-thousand-dollar inference job over millions of historical records
crashes at 80%, a provider throttles, or the pipeline is redeployed mid-run.
With per-message processors or scripts, completed work is re-billed; with
provider batch jobs managed by hand, submitted-but-unretrieved results are
lost. Pramen's ledger makes restart cost for completed work approximately
zero and reconciles in-flight provider batch jobs by request identifier. This
single property routinely pays for the system.

**3. Incremental re-enrichment of slowly changing data.** Daily catalog,
document, or registry dumps where a few percent of records change per cycle.
Content-addressed work keys mean unchanged records reuse recorded results and
only changed records incur model cost — without a hand-built caching layer.
CocoIndex validates this pattern for index maintenance; nothing provides it as
a governed ETL step with database delivery.

**4. Regulated, residency-constrained semantic processing.** EU-resident data
that must be enriched without leaving approved infrastructure: pinned-region
Bedrock without cross-region routing, or self-hosted vLLM when data cannot
leave the network at all — with per-row provenance (model identity, prompt
revision, tokens, validation outcome) for audit. Warehouse AI functions bind
model hosting to the platform's region matrix; scripts have no audit trail.
This aligns directly with EU AI-era compliance expectations and is where
strict-governance defaults become a selling point rather than overhead.

**5. AI spend control as an operational discipline.** Hard per-record and
per-run budgets, circuit breakers on cost and invalid-output spikes, batch
scheduling, prompt-cache accounting, and dedup are enforced by the runtime.
Teams currently discover LLM cost overruns from the invoice; Pramen is
designed so an overrun is a validation failure before dispatch.

The honest caveat for all five: this space moves quickly, and warehouse
vendors in particular are investing heavily. The wedge is durable only where
data residency, destination, model neutrality, or cost economics keep the
workload outside a single vendor's platform — which is a large and growing
share of real deployments, but not all of them.

### The lean v1 profile

Every subsystem above is justified in isolation; shipped together they would
delay the first productive pipeline by months and dilute the differentiators.
The v1 scope is therefore deliberately compressed around one measurable
promise: **download one binary, write one YAML file, get governed semantic
enrichment into PostgreSQL in under ten minutes.**

Concrete simplifications, each reversible by roadmap rather than redesign:

1. **One static binary, zero native dependencies.** The lean profile builds
   S3/local Parquet reading, SQL transforms, the semantic operators, the
   SQLite ledger, and a pure-Rust PostgreSQL sink into a single dependency-free
   executable. No driver directories, no container requirement, no services.
2. **Deterministic v1 transforms are SQL/expressions, not WASM.** DataFusion
   expressions cover the deterministic work the target business cases actually
   need — selection, filtering, normalization, derivation — at engine speed
   with no user toolchain. WASM components remain the committed extensibility
   mechanism and move to the first post-v1 milestone; the ABI design in this
   document stands, only its sequencing changes.
3. **Native PostgreSQL delivery instead of ADBC in v1.** A pure-Rust
   `COPY FROM STDIN BINARY` sink serves PostgreSQL and Aurora identically and
   preserves the static binary. ADBC returns when multi-warehouse sinks
   (Snowflake, BigQuery, Redshift) arrive, which is the problem it exists to
   solve.
4. **Two file formats, not four.** Parquet and NDJSON cover the target
   workloads; CSV and Arrow IPC follow demand.
5. **Five crates, not eleven.** Boundaries are split only after they prove
   stable.
6. **Four CLI commands, not eight.** `validate`, `run`, `explain`, and
   `ai evaluate`; the rest arrive with the features that need them.

What is explicitly *not* cut: the durable inference ledger, provider batch
scheduling, schema-bound validation, budgets, provenance, checkpointing, and
backpressure. Those are the differentiators and the research contribution;
removing any of them would make Pramen another thin LLM pipe.

## 3. Explicit non-goals for the first release

- A cluster scheduler or control-plane service.
- Distributed joins, windows, or aggregations.
- Exactly-once delivery across arbitrary external systems.
- Change-data-capture protocol implementations.
- A custom transformation language.
- A web-based visual pipeline editor.
- Hundreds of shallow connectors.
- A continuously queryable database or materialized-view engine.
- Arbitrary network access from user transformations.
- Autonomous multi-step agents.
- Side-effecting AI tools or model-generated code execution.
- Prompt or model optimization by an LLM.

These are scope boundaries, not permanent prohibitions.

## 4. Core principles

### One dataflow model

A pipeline is a validated directed acyclic graph. Nodes exchange Arrow
`RecordBatch` values. A bounded source ends; an unbounded source does not.
Batch is therefore a bounded stream rather than a separate engine.

### Bounded memory

Every edge has bounded capacity. A downstream slowdown propagates backpressure
to the source. Sources, transforms, and sinks must declare relevant limits,
including batch size, concurrency, in-flight bytes, and retry capacity.

### At-least-once by default

The initial correctness contract is at-least-once processing with idempotent
commit strategies where supported. Pramen must never describe this as
exactly-once merely because it checkpoints input positions.

Exactly-once effects require coordination between source progress and sink
commit. Where a destination supports staging and atomic publication, a
connector may expose stronger semantics with a precise contract.

### Arrow at component boundaries

Connectors decode into Arrow and encode from Arrow. Built-in transforms operate
on arrays, not row objects. Row-wise APIs may exist for convenience, but they
must not become the internal representation.

### Capabilities, not ambient authority

WASM components receive only declared host capabilities. The default transform
can read its input, emit output, report structured diagnostics, and access
deterministic configuration. Filesystem, clock, randomness, secrets, and
network access are denied unless a future capability explicitly grants them.

### Recorded results, not deterministic AI

LLM output is not deterministic infrastructure. Pramen can guarantee that a
recorded result is reused for the same versioned semantic operation; it cannot
guarantee that rerunning a provider produces identical content. Every semantic
result therefore carries the input fingerprint, prompt revision, model and
provider identity, inference parameters, output schema, token usage, timing,
validation outcome, and request identifiers.

Model calls are privileged built-in operations, not network access granted to a
WASM component. This keeps provider credentials, budgets, retries, and audit
policy under runtime control.

## 5. Logical architecture

```text
Pipeline specification
        |
        v
Parser -> validator -> logical DAG -> physical plan
                                      |
              +----------------+----------------+----------------+
              |                |                |                |
        source tasks      local transforms   semantic jobs   sink tasks
      object_store/formats  SQL, later WASM   AI providers   bulk writers
              |                |                |                |
              +------- bounded Arrow channels--+----------------+
                               |
                    checkpoints + AI result ledger
                               |
                       audit + observability
```

### Control path

1. Parse a versioned declarative pipeline specification.
2. Resolve secrets and connector packages without embedding secret values in
   the normalized plan.
3. Infer or load source schemas.
4. Validate edge schemas and transform contracts.
5. Plan SQL/expression transforms; compile and cache WASM components once the
   extensibility milestone lands.
6. Resolve semantic transform revisions, provider capabilities, schemas, and
   hard cost limits.
7. Produce a physical plan containing task parallelism, channel limits, and AI
   dispatch mode.
8. Start tasks, expose health and metrics, and manage graceful shutdown.

### Data path

1. Source discovery turns object keys into immutable work units.
2. Readers decode files into target-sized Arrow batches.
3. Bounded channels provide backpressure.
4. Built-in transforms — and later WASM components — produce zero or more
   output batches.
5. Semantic transforms persist fingerprinted work items instead of retaining
   Arrow batches in memory while remote inference is pending.
6. Online or asynchronous provider workers validate structured results and
   reconstruct enriched Arrow batches.
7. Sink writers group batches according to destination-specific bulk-loading
   constraints.
8. The sink commits a unit, then the source progress marker advances.

## 6. Proposed Rust workspace

The workspace starts small and splits only where a boundary has proven stable.
Premature crate proliferation is a known Rust workspace failure mode: it
freezes internal APIs before they are understood.

```text
crates/
  pramen        # CLI and daemon entry point
  pramen-core   # schemas, plans, runtime, channels, checkpoints, observability
  pramen-io     # formats (Parquet, NDJSON), object stores, PostgreSQL sink
  pramen-ai     # semantic operators, provider adapters, durable result ledger
  pramen-wasm   # component ABI, limits, artifact cache (extensibility milestone)
wit/
  transform.wit
examples/
docs/
```

Candidates for later extraction — a testkit, an ADBC sink crate, a standalone
ledger — should be carved out when their consumers exist, not before.

Likely foundations are Tokio, arrow-rs, object_store, DataFusion, a pure-Rust
PostgreSQL client for `COPY`, rusqlite for the ledger, serde, tracing, and
OpenTelemetry. Wasmtime joins at the extensibility milestone. Versions should
be selected only when implementation begins.

DataFusion carries more weight in the lean profile than originally planned: it
supplies the v1 deterministic transform surface (SQL and expressions over
Arrow batches) in addition to kernels. Pramen still should not force every
pipeline through a general query planner; a single-input SQL transform over a
stream of batches is the supported shape, not arbitrary multi-table SQL.

## 7. Pipeline specification

The human-authored format should be YAML initially, with a versioned JSON
schema and a canonical internal representation. Configuration is an API and
must be designed for migration.

Illustrative shape:

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: governed-semantic-enrichment
spec:
  models:
    enrichment:
      provider: configured-provider
      model: configured-model
  source:
    type: object_store
    url: s3://example-input/events/
    format:
      type: parquet
  transforms:
    - id: normalize
      type: sql
      query: >
        SELECT ticket_id, lower(trim(description)) AS description,
               created_at
        FROM input
        WHERE description IS NOT NULL
    - id: classify
      type: ai.extract
      model: enrichment
      execution: auto
      inputs: [description]
      instruction: Classify the record into a business category and explain why.
      output:
        fields:
          - { name: category, type: utf8, nullable: false }
          - { name: rationale, type: utf8, nullable: false }
      validation:
        onInvalid: review
      budget:
        maxInputTokensPerRecord: 4096
        maxOutputTokensPerRecord: 256
  sink:
    type: postgres
    target: analytics.events
    mode: append
  runtime:
    targetBatchBytes: 8388608
    maxInflightBytes: 268435456
    checkpoint:
      url: file:///var/lib/pramen/checkpoints/
```

The first schema should support a linear pipeline while the internal plan is a
DAG. Fan-out and fan-in can then be added without replacing the runtime. A
`type: wasm` transform referencing an OCI-distributed component becomes valid
at the extensibility milestone without changing this schema's shape.

This file is also the entire onboarding surface: a new user writes a source,
optionally one SQL transform, one `ai.extract` block, and a sink. No SDK,
toolchain, or service deployment is required to be productive.

## 8. WASM transformation ABI

> **Sequencing note:** WASM components are the committed extensibility
> mechanism but are not in the v1 lean profile; v1 deterministic transforms
> are SQL/expressions. This section's design is unchanged and governs the
> first post-v1 milestone.

### Recommended first ABI

Use the WebAssembly Component Model with WIT for lifecycle and metadata, and
Arrow IPC stream bytes for the batch payload.

Conceptually, a component provides:

- `manifest() -> transform-metadata`
- `configure(config, input-schema) -> output-schema`
- `transform(batch-ipc) -> result<list<batch-ipc>, transform-error>`
- `finish() -> result<list<batch-ipc>, transform-error>`

The exact WIT types must be decided by a spike, not by this document.

Arrow IPC imposes serialization and copies. This is acceptable for the first
correct ABI because:

- the Component Model is shared-nothing and does not provide a portable
  zero-copy Arrow memory contract;
- IPC is language-neutral and schema-preserving;
- micro-batches amortize call overhead;
- the ABI stays independent of Rust allocator and Arrow crate versions;
- it gives a stable baseline against which optimization can be measured.

Zero-copy should not be advertised until a safe ownership and lifetime model is
demonstrated. A future core-WASM ABI using guest linear memory and the Arrow C
Data Interface could be an opt-in performance profile, but it would trade away
some component-model portability and simplicity.

### Resource limits

Each transform invocation must enforce:

- maximum guest memory;
- fuel or epoch-based CPU interruption;
- wall-clock deadline;
- maximum input and output bytes;
- maximum number of emitted batches;
- deterministic handling of traps and schema violations.

Compilation should happen before execution where possible. Wasmtime's
serialized precompiled artifacts may be cached by engine version, target,
component digest, and compilation settings. The cache is an optimization, not a
portable distribution format.

### Developer experience

The first supported guest SDK should be Rust because it shares Arrow libraries
and has mature component tooling. A language-neutral ABI permits later SDKs for
C/C++, Go, JavaScript, and other WASM-capable languages, but each must pass the
same conformance suite.

A transform test command should accept fixture batches, execute with production
limits, and compare schema plus data. This is more important for adoption than
supporting many guest languages immediately.

## 9. Governed semantic transformations

### Operator scope

The first AI operator family is deliberately bounded:

- `ai.extract` adds declared structured columns;
- `ai.classify` is a constrained extraction with an enumerated output;
- `ai.generate` produces bounded text fields;
- `ai.embed` may follow, but should use a vector-native bulk API.

These are semantic transforms, not agents. Each invocation has fixed inputs,
instructions, output schema, model policy, and budgets. There are no tools,
loops, side effects, or generated code in the first release.

### Durable work ledger

Remote inference can take milliseconds or hours. An Arrow batch must not remain
live while a provider batch job waits. The semantic operator decomposes selected
rows or document chunks into durable work items, then later joins validated
results back to stable row identities.

A work key includes:

- canonical selected input values or their content digest;
- semantic operation type and prompt/template revision;
- declared output schema and validation policy;
- provider, model, and resolved model revision when available;
- inference parameters and relevant safety settings.

A completed result is immutable. Changing any key material produces new work.
Retries reuse completed results and reconcile submitted provider jobs using
provider request identifiers. An ambiguous timeout may still create duplicate
provider billing where the provider offers no idempotency or lookup mechanism;
Pramen must report this rather than claim exactly-once inference.

The first local ledger can use SQLite in WAL mode, but its interface must permit
a shared backend later. It stores only the selected AI inputs and outputs, not
entire source batches, unless pipeline policy explicitly requests that.

### Provider abstraction

Provider support is capability-based rather than reduced to a lowest-common
denominator. Capabilities include:

- online requests;
- asynchronous batch requests and reconciliation;
- native structured output or tool-schema enforcement;
- request idempotency;
- prompt caching;
- token accounting and cost metadata;
- model revision reporting;
- data residency and retention controls;
- multimodal inputs.

Pramen supports both online and asynchronous execution. `execution: auto`
selects batch mode for throughput-oriented bounded work when the provider
supports it, and otherwise uses rate-limited online requests. Pipeline authors
may require a mode and fail validation if it is unavailable.

Initial adapters should cover one direct hosted provider with a real batch API
and vLLM as the first self-hosted OpenAI-compatible endpoint.
"OpenAI-compatible" does not imply support for batch jobs, structured output,
token accounting, or identical error behavior; the capability report must
remain explicit.

### First hosted profile: Amazon Bedrock

Amazon Bedrock defines the first hosted acceptance profile. The adapter uses
the Converse API for online inference and the Converse invocation format for
batch inference. It sends the transform's generated JSON Schema through
structured output configuration and maps Bedrock usage and request metadata
into the Pramen result ledger.

The initial deployment profile is pinned to `eu-central-1` and prohibits
cross-region inference. It compares two locally available models on the same
golden set: one optimized for throughput and cost, and one optimized for
quality. Exact model IDs are selected only after capability discovery confirms
structured output and both online and batch support in that region.

The adapter must:

- use the AWS default credential chain and avoid handling long-lived access
  keys in pipeline files;
- pin region, model ID or inference profile, and relevant guardrail and prompt
  versions;
- validate that the selected model and region support Converse, structured
  output, the required JSON Schema subset, and the requested execution mode;
- attach the Pramen work key through request metadata or batch `recordId`;
- reconcile online request identifiers and batch job state after restart;
- stage batch JSONL input and output in policy-approved S3 locations with
  encryption, retention, and cleanup controls;
- ingest batch manifests, per-record errors, and input/output token counts;
- report prompt-cache reads and writes separately where supported;
- treat account-level model invocation logging as a deployment policy that
  operators must review, not silently enable it.

Current Bedrock batch limits include finite records, file size, completion
window, and region-specific concurrent-job quotas. These values change and must
come from a versioned capability profile or AWS service configuration rather
than become permanent engine constants.

Using a JSON-Schema constrained response provides structural compliance, not
semantic correctness or deterministic output. Golden evaluation and review
policy still apply.

### Validation and governance

Strict governance is the default:

1. Request native structured output where available.
2. Parse against a generated JSON Schema.
3. Validate Arrow types, nullability, bounds, and enumerations.
4. Apply deterministic normalization only.
5. Route invalid, policy-rejected, or low-confidence results to review or a
   dead-letter output according to pipeline policy.

Automatic LLM repair is a separate paid model call and must be explicitly
enabled, bounded, versioned, and recorded. It is never silently attempted.

Every output row can expose or reference provenance containing prompt revision,
provider, model, request ID, token counts, cost estimate, validation status, and
timestamps. Sensitive prompts and raw responses may be encrypted or omitted
from logs, but an audit digest remains.

### Quality and cost controls

Correct schema does not imply correct meaning. Each semantic transform needs:

- versioned golden fixtures and expected outputs;
- sampled evaluation before promotion;
- optional confidence or verifier gates;
- per-record, per-work-unit, and per-run token/cost ceilings;
- concurrency and provider-rate limits;
- circuit breakers for error, invalid-output, and cost spikes;
- explicit behavior when a model is retired or an alias changes.

Provider batch APIs currently offer substantial cost and throughput advantages
for non-urgent work, but their completion windows and limits differ. Pramen
should optimize dispatch only after correctness and auditability are preserved.

### Future agents

A future `ai.agent` operator would be a separate execution class with durable
step checkpoints, maximum turns, time and cost budgets, allow-listed tools, and
recorded tool results. Read-only tools should precede side-effecting tools.
Non-idempotent actions require explicit deduplication contracts and approvals.
This belongs after semantic transforms have proven the ledger and governance
model.

### First golden evaluation: support tickets

The first evaluation dataset is a versioned, human-labelled collection of
synthetic or properly anonymized support tickets. Production tickets may
contain personal, credential, contractual, and security-sensitive data and
must not be used casually in a provider spike.

The initial output schema should include:

- `category`: an enumerated business category;
- `priority`: an enumerated operational priority;
- `product`: nullable normalized product identifier;
- `customer_intent`: short bounded text;
- `entities`: bounded list of typed entity/value pairs;
- `evidence`: bounded list of exact source excerpts supporting the result;
- `rationale`: short bounded explanation;
- `requires_review`: boolean set by deterministic policy as well as model
  output.

The corpus lives in the repository as versioned YAML: each item carries the
input record, the expected output, and a weighted rubric for fields where
partial credit applies. Evaluation runs write results to a timestamped
directory so quality regressions across prompt or model revisions are
diffable artifacts, not anecdotes.

Evaluation reports schema-valid rate, exact match or F1 for enumerated fields,
entity precision/recall, evidence faithfulness, human agreement, review rate,
tokens, cost, and online/batch latency. A model-provided confidence number is
not treated as calibrated confidence. The Bedrock report presents the
fast/cheap and stronger models as a quality-cost frontier using identical
prompts, schemas, limits, and test records.

## 10. Connector architecture

Connectors should implement small capability-oriented interfaces rather than a
single universal trait.

### Source capabilities

- discovery or partition enumeration;
- schema discovery;
- bounded or unbounded mode;
- resumable position representation;
- split and parallel-read support;
- projection and predicate pushdown where available.

### Sink capabilities

- accepted Arrow types and coercions;
- append, replace, merge, or upsert modes;
- batch and byte limits;
- transaction and staging support;
- idempotency strategy;
- schema creation and evolution behavior;
- rejected-row handling.

### First source matrix

Object storage should use the Rust `object_store` abstraction for S3, Azure,
GCS, local files, and HTTP where supported. First-class v1 file formats:

1. Parquet;
2. NDJSON.

Parquet is the primary optimized path. NDJSON needs explicit schema inference
limits because unbounded inference makes startup and correctness
unpredictable. CSV and Arrow IPC follow demand rather than ship speculatively.

### First sink matrix

The v1 sink is PostgreSQL through a **native pure-Rust `COPY FROM STDIN
BINARY` implementation**, not ADBC. The rationale is packaging: the ADBC
PostgreSQL driver is a native C/C++ library, and shipping it would eliminate
the single-static-binary story for the one destination v1 actually targets. A
Rust `COPY` path serves local PostgreSQL, Aurora PostgreSQL, and RDS for
PostgreSQL identically, with Arrow-to-binary-COPY encoding under Pramen's
control.

ADBC remains the planned expansion mechanism — it exists to make many
warehouses (Snowflake, BigQuery, Redshift, Synapse, ClickHouse, Databricks)
addressable through one API, and it enters the roadmap when the second
warehouse family does. Flight SQL likewise moves to the expansion phase.

Sink targets in order:

1. PostgreSQL (native COPY) as the local conformance and integration target;
2. Amazon Aurora PostgreSQL and RDS for PostgreSQL as the first managed-cloud
   acceptance targets, over the same native path;
3. ADBC-backed warehouses and Flight SQL in the expansion phase.

For Aurora PostgreSQL, test both client-streamed `COPY FROM STDIN` and
server-side S3 import through the `aws_s3` extension.
The latter only applies to supported file formats and deployment policies, but
can avoid moving unchanged S3 data through the Pramen process. Pramen should
select or recommend a strategy based on transformation requirements and
connector capabilities, not assume one path is universally faster.

The Aurora acceptance suite must also measure transaction size, connection
concurrency, write-ahead-log and checkpoint pressure, index and trigger costs,
replica lag, failure recovery, IAM authentication, and TLS behavior. Engine
benchmarks that ignore destination health are not production benchmarks.

Every sink needs a capability report exposed by `pramen inspect connector`.
Unsupported type mappings and write modes must fail during validation whenever
possible.

### Packaging

The lean profile is a single static binary with no native driver
dependencies: `curl`, `chmod`, `pramen run`. This is achievable precisely
because v1 avoids ADBC.

When ADBC-backed warehouses arrive, they ship as an explicitly separate
distribution profile — tested container images bundling selected drivers, a
driver discovery directory, and platform packages. "Single binary" remains an
honest promise for the lean profile and is never claimed for configurations
that require dynamic native drivers.

## 11. Delivery, checkpoints, and failures

The checkpoint unit for the first vertical slice is an immutable source object
or deterministic object split. A durable checkpoint records:

- pipeline identity and normalized-plan hash;
- source object identity, version/etag, and split;
- transform component digest and relevant configuration hash;
- sink target and commit receipt;
- completion timestamp and diagnostic metadata.

Processing order:

1. claim a work unit;
2. read and transform it;
3. load into a staging or idempotent sink operation;
4. commit or publish at the sink;
5. durably mark the source unit complete.

A crash between steps 4 and 5 can duplicate data unless the sink exposes a
stable idempotency key or commit can be discovered. That limitation must be
visible in the connector's delivery contract.

Bad records should be handled by a policy:

- fail the work unit;
- discard with a metric and structured diagnostic;
- route to a dead-letter sink.

Silent coercion or silent data loss is never a default.

## 12. Shared-nothing scaling

The first runtime is one process. Multiple processes can scale safely when an
external system provides work partitioning or when work units can be assigned
without overlap.

For object storage, v1 can run from an explicit manifest partition. A later
coordinator may lease work units using a durable store, but that is not
required to prove the engine.

For Kafka, future workers can share a consumer group. For databases, partition
queries or CDC systems must define their own assignment contracts.

The runtime interfaces should include stable task and checkpoint identities so
a coordinator can be added later, but no distributed consensus abstraction
should be implemented speculatively.

## 13. Observability and operability

Minimum production signals:

- records, batches, and bytes in/out per component;
- active and blocked time;
- channel occupancy and in-flight bytes;
- batch-size distributions;
- source read, transform, serialization, and sink commit latency;
- retry counts and ages;
- checkpoint age and completion;
- rejected record counts;
- WASM compilation, invocation, trap, fuel, and memory metrics;
- AI queued, submitted, completed, reused, invalid, reviewed, and failed items;
- input/output tokens, estimated cost, provider latency, and batch age;
- AI cache hit rate and model/schema revision cardinality.

Logs should be structured and carry pipeline, run, component, work-unit, and
batch identifiers. OpenTelemetry export should be available, but local
Prometheus metrics and readable console diagnostics should work without a
collector.

The v1 CLI is four commands:

- `pramen validate pipeline.yaml`
- `pramen explain pipeline.yaml`
- `pramen run pipeline.yaml`
- `pramen ai evaluate ...`

`pramen run --smoke` is a runtime preset, not a separate mode: it caps the
record count, pins semantic transforms to the pipeline's designated fast/cheap
model, and enforces a hard cost ceiling. It exists for onboarding ("see real
enriched rows for a few cents"), CI, and pre-flight checks before large runs.

All commands accept `--log-format pretty|json|silent`. `json` emits
newline-delimited structured events (run, work unit, semantic dispatch,
validation, commit) suitable for capture without an OpenTelemetry collector.

Later commands arrive with the features that need them: `pramen ai review`
with the review-queue workflow, `pramen transform test` with WASM components,
`pramen inspect connector` with the connector SDK, and `pramen benchmark`
with the published benchmark suite.

## 14. Performance methodology

"Super optimized" is not a useful requirement until translated into measured
budgets. One product metric sits above the engine numbers: **time from binary
download to first enriched rows in PostgreSQL**, with a target under ten
minutes on a laptop.

Initial benchmarking should report:

- end-to-end throughput and CPU-seconds per GiB;
- peak resident memory and bytes in flight;
- p50/p95/p99 batch latency;
- SQL/expression transform overhead versus direct DataFusion;
- WASM IPC encode/decode, guest execution, and overhead versus built-in
  transforms (at the extensibility milestone);
- small-row and wide-column data sets;
- Parquet compression and destination load time separately;
- cold and warm WASM compilation;
- backpressure behavior under a deliberately slow sink;
- semantic work items per second, cache hit rate, and durable-ledger overhead;
- provider queue time, inference time, token usage, and cost per accepted row;
- online versus provider-batch execution for equivalent semantic work.

Compare against at least:

- direct DataFusion/Arrow processing as the lower-overhead baseline;
- DuckDB `COPY` or equivalent for the selected batch path;
- Redpanda Connect for a comparable file-to-database flow where supported;
- a no-transform pipeline to expose framework overhead;
- DocETL for a comparable structured semantic extraction where practical;
- Redpanda Connect's `aws_bedrock_chat` for an equivalent enrichment flow, to
  quantify the batch-pricing and dedup advantage rather than assert it.

Benchmarks must publish data generators, configurations, versions, machine
details, and raw results. Avoid headline records based only on in-memory
synthetic transforms.

## 15. Security model

- Secrets are referenced, never embedded in normalized plans or logs.
- Connector TLS verification is enabled by default.
- WASM components are content-addressed and may be allow-listed by digest.
- OCI distribution should support signature verification later.
- Native plugins are excluded from the first stable extension API because they
  remove process isolation and destabilize the ABI.
- Pipeline and component manifests declare required capabilities.
- Temporary files use private permissions and explicit lifecycle policies.
- Diagnostic samples redact configured sensitive columns.
- AI providers are allow-listed with declared residency and retention policy.
- Prompt and response logging is disabled by default for sensitive columns.
- Hard token, cost, concurrency, and request-size limits are enforced before
  dispatch.
- Model calls cannot acquire connector or WASM credentials.

## 16. Phased roadmap

### Phase 0: risky-boundary spikes

Ordered by risk to the thesis; the semantic ledger comes first because the
paper and the product both stand on it.

1. Implement the durable work ledger on SQLite; run schema-bound support-ticket
   extraction through Bedrock Converse online and batch with the same pinned
   model and schema; prove result reuse across a crash and reconcile an
   in-flight batch job after restart.
2. Compare a fast/cheap and a stronger Bedrock model in `eu-central-1` on the
   golden set, then run the selected schema through a local vLLM endpoint.
3. Read partitioned Parquet through `object_store` with bounded memory and run
   a DataFusion SQL transform over the batch stream.
4. Bulk-load Arrow batches into local PostgreSQL through native Rust
   `COPY FROM STDIN BINARY`; compare with Aurora server-side S3 import.
5. Round-trip representative Arrow batches through a WIT component using IPC,
   measuring copies, compilation, traps, and limits — gating the extensibility
   milestone, not v1.

Exit criterion: evidence that the selected boundaries work and an explicit
record of any changes to the architecture.

### Phase 1: the lean vertical slice (v1)

- linear pipeline spec and validation;
- local filesystem and S3;
- Parquet and NDJSON sources;
- DataFusion SQL/expression transforms;
- `ai.extract`, `ai.classify`, and `ai.generate` with strict schema
  validation, budgets, bounded text enforcement, and the SQLite result
  ledger;
- Bedrock Converse online and batch, EU-pinned, plus the vLLM online adapter;
- native PostgreSQL `COPY` sink covering local, Aurora, and RDS;
- bounded channels, graceful shutdown, metrics, file checkpoints;
- the four-command CLI and deterministic integration tests;
- one static binary for Linux, macOS, and Windows, all blocking CI targets.

Exit criterion: a new user reaches enriched rows in PostgreSQL from a
published binary and one YAML file in under ten minutes; crash/restart tests
show no resubmission of durably recorded results; ambiguous provider
submissions are explicitly surfaced; resource and model cost are measured.

### Phase 2: extensibility and cloud breadth

- WASM component transforms (the ABI in section 8), `pramen transform test`,
  and OCI distribution;
- Azure Blob and GCS sources;
- dead-letter handling and review queue export;
- additional provider adapters as demanded;
- shared checkpoint and ledger backends for fleet deployments.

Exit criterion: a documented, reproducible AWS deployment with load-impact
and crash-recovery results; a third-party-authored WASM transform passing the
conformance suite.

### Phase 3: expansion and product maturity

- ADBC-backed warehouse sinks and Flight SQL, with container driver profiles;
- fan-out DAGs;
- connector SDK and conformance tests;
- semantic transform evaluation and provider conformance suites;
- compatibility policy, security reporting, release automation, and examples;
- the published benchmark and paper artifact (section 18).

### Deferred exploration

- Kafka and unbounded sources;
- windowed/stateful operators;
- coordinator and work leasing;
- schema registry and evolution automation;
- Python or C++ guest SDKs;
- optimized non-IPC WASM ABI;
- native plugins;
- CSV and Arrow IPC formats;
- autonomous agents, tool use, and model-generated code;
- side-effecting AI operations.

## 17. Decisions and open questions

Once implementation starts, significant decisions move into lightweight
decision records under `docs/adr/`. Two practices are adopted deliberately:
rejected and deferred alternatives get their own records (for example
"ADBC in v1 — rejected for packaging" and "WASM in v1 — deferred"), and each
such record names the concrete reopen triggers that would revisit it. A
short controlled-vocabulary document should pin the project's terms — work
unit, work item, recorded result, semantic transform, review routing — with
the synonyms to avoid, since the same vocabulary feeds the paper.

### Decided

- Rust core.
- Apache Arrow record batches internally.
- Unified bounded/unbounded dataflow model.
- Lean v1: one static binary with zero native driver dependencies.
- Tier-one targets: Linux x86_64/aarch64, macOS aarch64, and Windows x86_64,
  all blocking in CI.
- Deterministic v1 transforms are DataFusion SQL/expressions.
- WASM as the user-code extension boundary, delivered as the first post-v1
  milestone rather than in v1.
- v1 database delivery is native pure-Rust PostgreSQL `COPY`; ADBC arrives
  with multi-warehouse expansion.
- v1 file formats are Parquet and NDJSON.
- Shared-nothing first; no cluster coordinator in v1.
- Standalone CLI and daemon as the primary product surface.
- Throughput and cost efficiency over sub-second event latency initially.
- First vertical: S3 through deterministic and semantic transforms to Aurora
  PostgreSQL.
- First production destination: Amazon Aurora PostgreSQL.
- A peer-reviewed systems paper on the semantic execution layer is an explicit
  project goal.
- Schema-bound semantic extraction is the first AI capability.
- Amazon Bedrock is the first hosted provider acceptance profile.
- Support-ticket classification and extraction is the first golden evaluation.
- Bedrock inference is pinned to `eu-central-1` without cross-region routing.
- A fast/cheap and stronger Bedrock model are compared on quality and cost.
- vLLM is the first self-hosted OpenAI-compatible adapter.
- Hosted and self-hosted providers are supported through explicit capability
  adapters.
- Online and asynchronous batch inference are both execution modes.
- Strict audit, validation, budgets, and review routing are defaults.
- Multi-step agents and tools are deferred.
- Apache-2.0 is the recommended eventual license, not yet added.

### Validate through spikes

- Component Model/WIT versus a lower-level core-WASM ABI.
- Practical IPC overhead at target batch sizes.
- Single-input SQL transform overhead versus direct DataFusion execution.
- Arrow-to-binary-`COPY` type coverage and throughput in pure Rust.
- PostgreSQL idempotency and Aurora failover behavior.
- Client-streamed COPY versus server-side S3 import selection.
- Checkpoint granularity for very large objects.
- AI work-key canonicalization and durable ledger throughput.
- Provider reconciliation after ambiguous submission failures.
- Structured-output fidelity across Bedrock and the first self-hosted adapter.
- Bedrock model capability discovery in `eu-central-1` and batch
  reconciliation.
- vLLM structured-decoding behavior, backpressure, and usage accounting.
- Quality evaluation and promotion workflow for prompt/model revisions.

### Product questions for the next design iteration

1. Which exact locally available Bedrock model IDs should represent the
   fast/cheap and stronger tiers?
2. Which open-weight model should pin the vLLM acceptance profile?
3. Should schema evolution be strict by default, or permit configured
   additive changes?
4. Should the first Aurora write mode be append-only, idempotent replace by
   work unit, or keyed upsert?
5. Should a future embeddable Rust library be a supported product surface or
   remain an internal implementation detail?

## 18. Research goal and publication path

A peer-reviewed systems paper is an explicit project goal, and it disciplines
the scope rather than expanding it: the paper's evaluation section and the
product's benchmark suite are the same artifact. DocETL (VLDB) and Palimpzest
(CIDR) demonstrate venue appetite for LLM-powered data processing systems;
neither addresses the execution-layer questions Pramen targets.

### Candidate contribution

Working title shape: *governed semantic operators in a columnar dataflow —
cost-optimal, restart-safe LLM enrichment as a systems problem*. The claimed
contributions are the durable content-addressed inference ledger and the
treatment of provider batch APIs as a first-class scheduling target, not the
operators themselves.

### Research questions

1. **Dispatch policy.** Given per-record work, provider online and batch
   pricing, batch completion windows, and a pipeline deadline, when does
   batch dispatch dominate online dispatch? Contribute a cost model and the
   measured cost/latency frontier on the golden workload across Bedrock and
   vLLM.
2. **Memoization semantics.** What correctness contract does content-addressed
   inference reuse require inside an at-least-once dataflow (key
   canonicalization, revision handling, ambiguous-submission reconciliation)?
   Measure realized savings under crash/replay, incremental re-enrichment,
   and duplicate-heavy workloads.
3. **End-to-end comparison.** Throughput, cost per accepted row, and
   golden-set quality against per-message AI processors (Redpanda Connect),
   in-warehouse AI SQL, and DocETL on an equivalent enrichment task.

### Venue and artifact

Natural targets, in order of fit: VLDB (research or industrial track), CIDR
for the systems-vision framing, or a SIGMOD demo; aiDM/DEEM-style workshops
are a fallback for early results. The artifact is the reproducible benchmark:
data generators, pipeline configurations, model pins, raw results, and the
evaluation corpus — which the performance methodology in section 14 already
requires for credibility reasons.

## 19. Sources

- [Apache Arrow ADBC Rust documentation](https://arrow.apache.org/adbc/23/rust/index.html)
- [ADBC driver implementation status](https://arrow.apache.org/adbc/main/driver/status.html)
- [ADBC Snowflake driver](https://arrow.apache.org/adbc/23/driver/snowflake.html)
- [ADBC PostgreSQL driver](https://arrow.apache.org/adbc/main/driver/postgresql.html)
- [Aurora PostgreSQL S3 import](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PostgreSQL.S3Import.html)
- [AWS guidance for PostgreSQL bulk loading](https://aws.amazon.com/blogs/database/optimized-bulk-loading-in-amazon-rds-for-postgresql/)
- [Vector Remap Language](https://vector.dev/docs/reference/vrl/)
- [Redpanda Connect component catalog](https://docs.redpanda.com/redpanda-connect/components/about/)
- [Redpanda Connect Bloblang](https://docs.redpanda.com/redpanda-connect/guides/bloblang/about)
- [Arroyo on Arrow and DataFusion](https://www.arroyo.dev/blog/why-arrow-and-datafusion/)
- [WebAssembly Component Model linking](https://github.com/WebAssembly/component-model/blob/main/design/mvp/Linking.md)
- [Wasmtime shared-memory component discussion](https://github.com/bytecodealliance/wasmtime/issues/10491)
- [DocETL](https://github.com/ucbepic/docetl)
- [CocoIndex](https://github.com/cocoindex-io/cocoindex)
- [Palimpzest](https://palimpzest.org/)
- [OpenAI Batch API](https://developers.openai.com/api/docs/guides/batch)
- [Anthropic Message Batches](https://platform.claude.com/docs/en/build-with-claude/batch-processing)
- [Gemini Batch API](https://ai.google.dev/gemini-api/docs/batch-api)
- [Temporal durable agents integration](https://github.com/temporalio/sdk-python/tree/main/temporalio/contrib/openai_agents)
- [Bedrock structured outputs](https://docs.aws.amazon.com/bedrock/latest/userguide/structured-output.html)
- [Bedrock Converse API](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html)
- [Bedrock Converse batch format announcement](https://aws.amazon.com/about-aws/whats-new/2026/02/amazon-bedrock-batch-inference-supports-converse-api-format/)
- [Bedrock batch inference results](https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference-results.html)
- [Redpanda Connect AI processors](https://www.redpanda.com/blog/ai-connectors-gpu-runtime-support)
- [Redpanda Connect aws_bedrock_chat](https://docs.redpanda.com/redpanda-connect/components/processors/aws_bedrock_chat/)
- [Databricks ai_query](https://docs.databricks.com/gcp/en/sql/language-manual/functions/ai_query)
- [Snowflake Cortex AI functions](https://docs.snowflake.com/user-guide/snowflake-cortex/aisql)
- [BigQuery generative AI functions](https://docs.cloud.google.com/bigquery/docs/generative-ai-overview)
- [BAML](https://github.com/BoundaryML/baml)
- [Weavekit](https://github.com/engineersamuel/weavekit)

These sources establish current capabilities and constraints; all performance
and compatibility claims still require Pramen-specific measurement.
