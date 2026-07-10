# Spike report S1.1: durable inference ledger + Bedrock Converse online

Status: ledger core validated offline; live Bedrock run pending credentials  
Date: 2026-07-10  
Code: `spikes/s1-1-ledger-bedrock/` (disposable; this report is the artifact)

## What was built

- Work-key canonicalization: sorted-key canonical JSON over operation,
  prompt revision, inputs, output schema, provider, model, and parameters,
  hashed with SHA-256. Object key order is insignificant; array order is
  significant; any material change produces a new key.
- SQLite (WAL, `synchronous=NORMAL`) ledger with the state machine
  `pending → submitted → completed | failed`. Completion is idempotent and
  never overwrites; the provider request ID is recorded *before* dispatch so
  a crash leaves a reconcilable trace.
- A runner that reuses completed results, surfaces ambiguous `submitted`
  items on restart, dispatches only new work, and validates every output
  against the declared JSON Schema (invalid results are recorded with their
  validation failure, not dropped).
- Providers: a deterministic mock with a billing counter (for reuse proofs)
  and a Bedrock Converse online adapter (region-pinned, default credential
  chain, token usage captured from the Converse response).

## Exit criteria and results

| Criterion (plan) | Result |
| --- | --- |
| 100% result reuse on replay of a completed run | **Pass** — replay across a process "restart" (fresh connection) dispatched 0 provider calls, asserted via the mock's billing counter |
| kill -9 loses zero completed results | **Pass (simulated)** — connection dropped between `submitted` and `completed` states; completed work survived, in-flight work surfaced as ambiguous and recovered |
| Ledger overhead at 10k / 100k items | **Measured** — see below |
| Live Bedrock Converse extraction in `eu-central-1` | **Pending** — adapter implemented; needs AWS credentials (`cargo run -- run --provider bedrock --model <id>`) |

## Overhead measurements

MacBook (Apple Silicon, aarch64-apple-darwin), release build, 2026-07-10.
"Cold" includes mock dispatch, JSON Schema validation, and durable recording;
"warm" is pure recorded-result reuse.

| Items | Cold total | Cold per item | Warm total | Warm per item |
| --- | --- | --- | --- | --- |
| 10,000 | 2.05 s | 205 µs | 0.27 s | 27 µs |
| 100,000 | 31.2 s | 312 µs | 4.4 s | 44 µs |

## Conclusions

1. **The thesis holds.** Ledger overhead is 3–4 orders of magnitude below
   online model-call latency (hundreds of ms) and batch-job latency (minutes
   to hours). Durability is effectively free relative to inference.
2. **Known, deliberate inefficiencies** for P1.6 to fix: the JSON Schema
   validator is recompiled per item (cache it per transform revision), and
   writes are per-item transactions (batch them). Both explain most of the
   cold per-item cost and the mild growth from 10k to 100k.
3. **Honest-semantics note confirmed in code**: online requests interrupted
   between submission and completion cannot be looked up later; the runner
   surfaces them as ambiguous (possible double billing) and re-dispatches.
   Provider *batch* jobs are the reconcilable case — that is spike S2.1's
   job, and the `submitted` state with request IDs is already shaped for it.

## Recommendation

Proceed with SQLite WAL as the v1 ledger (confirms ADR 0003). Productionize
in P1.6 with validator caching, transaction batching, and the backend trait.
Run the live Bedrock leg of this spike (same binary, `--provider bedrock`)
before starting S2.1, and append token/cost numbers to this report.
