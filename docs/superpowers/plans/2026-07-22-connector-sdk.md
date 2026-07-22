# E1.4 Connector SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship thin connector descriptors, `pramen inspect connector`, an offline sink commit-barrier harness, and a published support matrix.

**Architecture:** Types + registry + conformance helpers in `pramen-core::connector`; CLI in `pramen`; matrix doc kept in sync by a unit test.

**Tech Stack:** Rust edition 2024, existing `Sink` trait, clap subcommands, serde JSON for `--json`.

## Global Constraints

- One task ID = one branch (`cursor/e1-4-connector-sdk`) = merge to main.
- No new crates; no dynamic plugins; no ADBC.
- Vocabulary: add **Connector**, **Support level**, **Delivery contract** if missing.
- TDD for connector module and CLI parsing/output asserts.
- Aikido scan new first-party code; `mise run test` before merge.

---

## File map

| File | Responsibility |
| --- | --- |
| `crates/pramen-core/src/connector/{mod,types,registry,conformance}.rs` | Types, built-in registry, harness |
| `crates/pramen/src/inspect.rs` | `inspect connector` command |
| `crates/pramen/src/main.rs` | Wire subcommand |
| `docs/connectors/support-matrix.md` | Published matrix |
| `docs/vocabulary.md`, plan, roadmap, architecture CLI list | Docs sync |
| `site/...` optional short page or link from roadmap | Discoverability |

---

### Task 1: Core types + failing registry test

- [ ] Add `pub mod connector` with `SupportLevel`, `ConnectorKind`, `ConnectorDescriptor`
- [ ] Write test: `builtins()` contains `sink.postgres`, `sink.flightSql`, `source.objectStore`
- [ ] Implement registry to pass
- [ ] Matrix sync test stub (descriptor ids ⊆ matrix headings) after matrix file exists

### Task 2: Conformance harness (TDD)

- [ ] Failing test with `RecordingSink`: barrier holds
- [ ] Implement `assert_sink_commit_barrier`
- [ ] Document that product sinks pin the same contract in their crates

### Task 3: CLI `pramen inspect connector`

- [ ] `Inspect { Connector { id, json } }` subcommand
- [ ] Text via `cargo test` on formatting helpers or `assert_cmd` if already used; else unit-test `format_list` / `to_json`
- [ ] Human text + `--json`

### Task 4: Docs + matrix + plan

- [ ] `docs/connectors/support-matrix.md`
- [ ] Vocabulary entries; architecture CLI bullet already mentions inspect — confirm
- [ ] Update implementation-plan, roadmap, AGENTS commands list
- [ ] Close #52 on merge

### Task 5: Verify + merge

- [ ] `mise run test`, Aikido on new files, commit, ff-merge main, push, close issue
