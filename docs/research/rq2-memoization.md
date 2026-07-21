# RQ2 — Memoization / reuse contract (E2.2)

Status: measured (offline)  
Date: 2026-07-21  
Task: E2.2  
Aligns with: architecture §9 (Governed semantic transformations),
vocabulary (`work key`, `recorded result`, `inference ledger`,
`reconciliation`, `review routing`)

## Research question

What is the precise reuse contract for governed semantic transforms, and
what savings does it deliver under crash/replay, incremental
re-enrichment, and duplicate-heavy workloads?

## The reuse contract

Pramen does not treat model output as a soft cache. It treats each unit of
semantic work as a **work item** whose identity is a content-addressed
**work key**, and whose validated output is an immutable **recorded
result** in the **inference ledger**.

### Work-key inputs

A work key is the SHA-256 of the canonical JSON of:

| Material | Role |
| --- | --- |
| Operation (`ai.extract` / `ai.classify` / `ai.generate`) | Distinguishes operator families |
| Instruction text | Prompt revision: any edit creates new work |
| Selected input values | Canonical JSON of the record's input columns |
| Declared output schema | Typed contract the result must satisfy |
| Provider id + model id | Backend identity |
| Inference parameters | Temperature, output caps, and other output-affecting params |

Canonicalization sorts object keys at every nesting level and omits
insignificant whitespace. Stability of that form is a compatibility
contract: changing it would orphan every recorded result
(`crates/pramen-ai/src/workkey.rs`).

Secrets never appear in work keys, normalized plans, or ledger specs —
only selected AI inputs and declared configuration.

### Immutability of completed results

Once a work item reaches `completed`, the recorded result is immutable:

- `LedgerStore::complete` is idempotent and never overwrites a completed
  row.
- Retries, replays, and later runs that see the same work key return the
  stored output and **do not dispatch**.
- Token and cost counters for the run do not increase on reuse
  (`run_tokens` is unchanged; the mock provider's call counter does not
  advance).

State machine (per work key):

```text
pending ──► submitted ──► completed   (immutable)
   │             │
   └─────────────┴──────► failed      (retryable, unless review-held)
```

### What invalidates reuse

Changing any work-key material produces a *different* work key and
therefore new work. In practice:

| Change | Effect |
| --- | --- |
| Input column value | New key → dispatch |
| Instruction / prompt revision | New key for every affected record |
| Output schema / field types | New key |
| Provider or model id | New key |
| Params (temperature, caps) | New key |
| Unrelated pipeline SQL / sink config | No effect on the key |

There is no TTL, no soft eviction, and no silent recomputation of a
completed result.

### Crash / reconcile vs re-bill

- **Online, completed before crash.** Restart consults the ledger;
  completed results are reused. Measured: 100% reuse, 0 provider calls,
  0 tokens on replay.
- **Provider-batch, crash after submit.** The job id is recorded per
  item *before* results are awaited. Restart **reconciles** by job and
  item id — it never resubmits. Billing happened at submit; reconcile
  adds zero provider calls. Ambiguous provider timeouts (no idempotency
  / lookup) are reported, not papered over as exactly-once inference
  (architecture §9).
- **Failed, not in review.** Retryable: a later run may dispatch again.

### Review-queue interactions

Under `onInvalid: review`:

- The record is withheld from the sink and queued in the same ledger
  database.
- While `pending`, replays **do not re-dispatch** and do not re-bill.
- `accept` schema-validates a human correction and records it as a
  completed `human-review` result with zero tokens — subsequent runs
  reuse it like any other recorded result.
- `reject` is a permanent, auditable drop; replays stay empty for that
  key.

Humans review *data*, not *runs* (vocabulary: review routing).

## Measurement method

Harness: `pramen_ai::reuse::run_suite` — L0 operator path over
`MockProvider` + temporary SQLite ledgers. No network, no credentials.

Regenerate published figures:

```bash
./scripts/rq2-memoization.sh
# or: mise run rq2-memoization
cargo test -p pramen-ai reuse   # pins the exit bars in CI
```

Artifacts:

- Machine-readable: [`rq2-memoization-metrics.json`](./rq2-memoization-metrics.json)
- This note (contract + table)

## Measured results (offline, mock provider)

| Scenario | Records | Provider calls | Tokens billed | Reused | Reuse % | Savings vs naive |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Crash/replay (online) | 50 | 0 | 0 | 50 | 100.0% | — |
| Crash/reconcile (batch) | 24 | 0 | 0 | 24 | 100.0% | — |
| Incremental re-enrichment | 45 | 10 | 295 | 35 | 77.8% | 77.8% |
| Duplicate-heavy workload | 200 | 20 | 580 | 180 | 90.0% | 90.0% |
| Review queue withhold | 1 | 0 | 0 | 0 | 0.0% | — |

Reading of the numbers:

1. **Crash/replay (online).** First pass billed 50 calls / 1500 tokens;
   replay billed **0 / 0** with **100%** result reuse.
2. **Crash/reconcile (batch).** 24 items billed at submit; after a
   simulated crash, reconcile recovered all 24 rows with **0** rebill
   calls.
3. **Incremental re-enrichment.** Baseline 40 unique records. Second
   pass presented 45 rows (35 unchanged, 5 edited inputs, 5 new): only
   the **10** changed/new keys dispatched; 35 reused (77.8% of the
   second-pass rows).
4. **Duplicate-heavy.** 200 rows cycling 20 unique texts → **20**
   dispatches vs 200 naive (**90%** savings).
5. **Review withhold.** Invalid output routed to review (1 call); replay
   issued **0** calls and emitted **0** rows while pending.

## Honest caveats

- Numbers use the deterministic `mock` provider's token accounting, not
  a paid model. Absolute token counts are relative evidence; the
  *ratios* (0 on replay, only-delta on incremental, unique-key
  dedup) are what the contract claims and CI pins.
- Provider-batch savings vs online *pricing* (often ~50% list) are out
  of scope for RQ2; see E2.1 for dispatch-cost policy.
- Ambiguous online timeouts without provider idempotency can still
  double-bill externally; Pramen surfaces that rather than claiming
  exactly-once inference.

## Code map

| Piece | Location |
| --- | --- |
| Work-key canonicalization | `crates/pramen-ai/src/workkey.rs` |
| Ledger state machine | `crates/pramen-ai/src/ledger/` |
| Operator reuse + reconcile | `crates/pramen-ai/src/operator.rs` |
| Measurement suite | `crates/pramen-ai/src/reuse.rs` |
| Publisher | `crates/pramen-ai/examples/rq2_memoization.rs` |
| Script | `scripts/rq2-memoization.sh` |
