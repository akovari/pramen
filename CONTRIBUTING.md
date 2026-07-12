# Contributing to Pramen

Thank you for your interest in Pramen. This file is the human-facing entry
point; the full working protocol for contributors and agents lives in
[`AGENTS.md`](AGENTS.md).

## Before you start

1. Read [`docs/architecture.md`](docs/architecture.md) for design authority.
2. Pick a task from [`docs/implementation-plan.md`](docs/implementation-plan.md)
   by stable ID (for example `P1.4`, `T1.6`).
3. Open or claim the matching GitHub issue if one exists.

## Workflow

- **One task = one branch = one PR.** Branch names look like
  `cursor/<task-id>-<slug>`.
- **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`, …).
- **Update the implementation plan** in the same PR that completes a task.
- **Architecture changes** that alter a decision in `docs/architecture.md`
  section 17 require an ADR in `docs/adr/`.

## Local setup

Install tools and run the same gates CI runs:

```bash
mise install
mise run ci
```

Optional L2 tests need local services or env vars — see `AGENTS.md` for
`PRAMEN_TEST_POSTGRES_DSN` and `PRAMEN_TEST_S3_URL`.

## Tests

PR gates are fully offline per [ADR 0005](docs/adr/0005-local-first-testing.md).
Use the shared fixtures in `crates/pramen-testkit` for HTTP stubs and env
guards instead of hand-rolling test servers.

## Questions

Open a GitHub issue with the task ID in the title when scope is unclear.
Security reports: see [`SECURITY.md`](SECURITY.md).
