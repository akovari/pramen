# Connector SDK + conformance (E1.4) — design

Date: 2026-07-22  
Status: approved (approach 3: thin public surface + inspect CLI + harness + matrix)  
Task: E1.4

## Goal

Make every first-party connector inspectable and conformance-tested without
shipping a heavyweight third-party plugin framework yet. Leave a stable
extension point for ADBC (E1.1) and future external connectors.

## Scope

In:

1. **Vocabulary + types** in `pramen-core::connector`: `SupportLevel`,
   `ConnectorKind`, `ConnectorDescriptor` (id, kind, level, summary,
   capabilities / delivery notes).
2. **Built-in registry** listing current connectors (object-store source,
   Postgres COPY sink, Flight SQL sink, SQL transform, WASM transform,
   semantic AI transforms as a group). Levels: `supported` | `preview` |
   `planned`.
3. **CLI** `pramen inspect connector [ID]` (+ `--json`); omit ID → list.
4. **Offline conformance harness** for sinks: commit-barrier contract
   (writes before commit must not be visible; empty commit is ok). A
   reference `RecordingSink` in tests proves the harness; Flight SQL and
   (where already covered) existing sink tests remain the product pins.
5. **Published matrix** `docs/connectors/support-matrix.md` + site pointer;
   a unit test (or `mise` check) that registry IDs ⊆ matrix and vice versa.
6. Docs: architecture § CLI mention stays accurate; plan + roadmap + issue
   #52 updated; AGENTS if a new command appears.

Out:

- Dynamic plugin loading / dylib connectors
- ADBC implementation (E1.1)
- Live warehouse / cloud conformance
- Rewriting existing sinks onto a new trait hierarchy (keep `Sink`/`Source`)

## Support levels

| Level | Meaning |
| --- | --- |
| `supported` | First-party; offline conformance / CI coverage; documented delivery contract |
| `preview` | Shipped with explicit limits (e.g. Flight SQL append-only) |
| `planned` | Named on the matrix; not in the default binary yet (e.g. ADBC) |

## Capability fields (v1)

Per descriptor (text + structured where cheap):

- `id` (stable slug, e.g. `sink.postgres`, `sink.flightSql`, `source.objectStore`)
- `kind`: `source` | `sink` | `transform`
- `support_level`
- `modes` (sinks: `append`, `upsert`; empty if N/A)
- `schemes` (sources: `file`, `s3`, …)
- `delivery` one-liner (at-least-once window, commit barrier, …)
- `notes` (limits, env secrets: `dsnEnv` / `tokenEnv`)

## Conformance (offline)

`pramen_core::connector::conformance::assert_sink_commit_barrier`:

1. Construct sink under test + a visibility probe (closure or trait).
2. `write` N rows → probe sees 0.
3. `commit` → probe sees N.
4. Fresh sink: `write` then drop without commit → probe still 0.

Product sinks keep their own L1/L2 tests; the harness is the SDK contract
new sinks must call. E1.4 wires it for a `RecordingSink` and documents that
`FlightSqlSink` / `PostgresCopySink` already satisfy the same contract via
existing tests (Flight SQL explicitly; Postgres via L2).

Optional CLI: `pramen connector test` deferred — library + `cargo test` is
enough for v1 of E1.4 (mirrors how most sink contracts are pinned today).
`pramen inspect connector` is the user-facing command architecture §13
named.

## ADR

None required unless we reopen “lean binary” for a new default dependency.
No new runtime crates; types live in `pramen-core`.

## Exit criteria

- `pramen inspect connector` lists all built-ins; `--json` schema-stable in a
  snapshot or field-assert test
- Matrix doc matches registry
- Conformance module tested with `RecordingSink`
- Plan / roadmap / #52 / vocabulary updated
- `mise run test` green; Aikido clean on new first-party code
