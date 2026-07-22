# Pramen Vocabulary

Controlled terms for docs, code, ADRs, and the paper. Use these exactly; if a
needed term is missing, add it here in the same PR rather than improvising a
synonym. Each entry lists terms to avoid because they blur a distinction the
system depends on.

**Pipeline**:
A validated directed acyclic graph of one source, transforms, and one or
more sinks, declared in a versioned YAML specification. Linear documents
(omit `from`, singular `sink`) remain the default; fan-out uses optional
`from` edges and `sinks` (ADR 0007). Fan-in is not supported in v1alpha1.
_Avoid_: job, flow, DAG (as a user-facing noun).

**Run**:
One execution of a pipeline by one worker process, with its own checkpoints,
metrics, and budget accounting.
_Avoid_: session, batch (that word is reserved for Arrow and provider batch).

**Batch (Arrow)**:
An Arrow `RecordBatch` flowing between pipeline components.
_Avoid_: chunk, block, micro-batch (in code and docs; acceptable in
positioning prose).

**Work unit**:
The checkpointable unit of *source* progress — an immutable source object or
deterministic split. Completing a work unit advances the source marker.
_Avoid_: task, partition (unless referring to source-native partitioning).

**Work item**:
The durable unit of *semantic* work — selected input values for one model
invocation, identified by its work key. Work items live in the ledger, not in
memory.
_Avoid_: request, prompt, job.

**Work key**:
The content-addressed identity of a work item: canonical inputs, operation
type and prompt revision, output schema, provider/model/parameters. Changing
any key material creates new work.
_Avoid_: cache key, hash (as a noun for this concept).

**Recorded result**:
The immutable, validated output of a completed work item, with provenance
(model identity, tokens, cost, validation outcome, request IDs). Recorded
results are reused; they are never recomputed silently.
_Avoid_: cached response, memoized output.

**Semantic transform**:
A pipeline transform that dispatches work items to a model provider
(`ai.extract`, `ai.classify`, `ai.generate`) under schema, budget, and
validation policy. Not an agent: fixed inputs, no tools, no loops.
_Avoid_: AI step, LLM call, agent.

**Deterministic transform**:
A transform whose output depends only on its input batch and configuration:
SQL/expressions in v1, WASM components later.
_Avoid_: normal transform, regular step.

**Inference ledger**:
The durable store of work items and recorded results. The ledger is the
source of truth for what has been paid for and proven.
_Avoid_: cache, state store.

**Provider**:
A model-serving endpoint behind a capability adapter (Bedrock, vLLM). A
provider reports capabilities; Pramen never assumes them.
_Avoid_: backend, vendor (in code).

**Online / provider-batch execution**:
The two dispatch modes for work items: immediate rate-limited requests versus
asynchronous provider batch jobs reconciled later.
_Avoid_: sync/async (overloaded), realtime/offline.

**Reconciliation**:
Recovering the state of submitted provider work after a restart by request
and job identifiers, so ambiguous submissions are surfaced, not re-billed
silently.
_Avoid_: retry, recovery (alone).

**Validation**:
Structural and typed enforcement of a recorded result against the declared
output schema (types, nullability, enumerations, bounds).
_Avoid_: evaluation (reserved for quality measurement).

**Evaluation**:
Quality measurement of a semantic transform against the golden corpus:
schema-valid rate, F1, cost, latency.
_Avoid_: validation, testing (alone).

**Golden corpus**:
The versioned, labelled dataset with weighted rubrics used for evaluation.
_Avoid_: test set (reserved for code tests), benchmark (reserved for
performance).

**Review routing**:
Sending invalid, policy-rejected, or low-confidence results to a review queue
or dead-letter output per pipeline policy. Humans review *data*, they do not
approve *runs*.
_Avoid_: human-in-the-loop, approval.

**Budget**:
A hard pre-dispatch ceiling on tokens or cost (per record, work unit, or
run). Exceeding a budget is a validation failure before spend, not an alert
after it.
_Avoid_: quota, limit (alone).

**Residency**:
Declared constraints on where source data and model inference may live,
enforced at plan validation from pipeline metadata (`runtime.residency`,
`source.location`, `models.*.region`) without live cloud lookups.
_Avoid_: data locality, geo-pin (as synonyms in specs).

**Checkpoint**:
The durable record that a work unit was committed at the sink. Distinct from
the ledger: checkpoints track source progress; the ledger tracks inference.
_Avoid_: savepoint, snapshot.

**Delivery contract**:
A sink's documented semantics: write modes, idempotency strategy, type
matrix, failure behavior. At-least-once is the default; anything stronger
must be stated in the contract. Exposed on each connector descriptor
(`pramen inspect connector`) and in the support matrix.
_Avoid_: guarantee (unqualified), exactly-once (unless the contract proves it).

**Connector**:
A first-party (or planned) source, sink, or transform with a stable id,
support level, and delivery contract. Inspected via
`pramen inspect connector`; rows live in `docs/connectors/support-matrix.md`.
_Avoid_: plugin (until a dynamic SDK ships), driver (reserved for ADBC
native libraries).

**Support level**:
How far a connector is carried in product support: `supported` (CI-backed
first-party), `preview` (shipped with documented limits), `planned` (named,
not in the lean binary).
_Avoid_: GA, beta, experimental (as synonyms in the matrix).

**Smoke run**:
A `pramen run --smoke` execution: capped records, pinned fast/cheap model,
hard cost ceiling. A preset of a normal run, not a separate mode.
_Avoid_: dry run (that implies no execution), demo mode.

**Spike**:
Disposable code answering a named risk question, producing a permanent
report in `docs/spikes/`. Production code never imports spike code.
_Avoid_: prototype, MVP.
