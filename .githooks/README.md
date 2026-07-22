# Local git hooks for Pramen.
#
# Wire once per clone (does not change global git config):
#
#   mise run setup-hooks
#
# That sets `core.hooksPath=.githooks` for this repository only so these
# scripts run even when a global `core.hooksPath` (e.g. standalone Aikido)
# would otherwise win.
#
# - pre-commit: optional Aikido secrets scan + `cargo fmt --check` (if *.rs staged)
# - pre-push:   `mise run lint` (fmt + clippy with CARGO_BUILD_WARNINGS=deny)
#
# Skip: `LEFTHOOK=0 git commit|push` or `--no-verify`.
