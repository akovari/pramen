#!/usr/bin/env bash
# Perf regression gate (T1.7): designated Criterion benches must not
# regress more than 5% against the merge-base.
#
# Method: the merge-base with $1 (default origin/main) is checked out
# into a temporary git worktree and benched with `--save-baseline`;
# HEAD is then benched with `--baseline` against it (shared
# CARGO_TARGET_DIR, so Criterion sees both runs), and the recorded
# change estimates are gated.
#
# Only benches with stable, CPU-bound behavior are designated; the
# fsync-heavy ledger cold path is measured but never gated, because its
# variance on shared runners exceeds any signal.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_REF=${1:-origin/main}
# Regex filter passed to Criterion; the ids below are also the gate list.
FILTER='workkey/canonicalize_and_hash|ledger/warm_reuse_existing_result|copy_encode/8192'
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"

step() { printf '\n==> %s\n' "$*"; }

base_sha=$(git merge-base HEAD "$BASE_REF")
if [ "$base_sha" = "$(git rev-parse HEAD)" ]; then
    echo "HEAD is the baseline commit ($base_sha); nothing to compare"
    exit 0
fi

worktree=$(mktemp -d /tmp/pramen-perf-base-XXXXXX)
rmdir "$worktree"
git worktree add --detach "$worktree" "$base_sha" > /dev/null
trap 'git worktree remove --force "$worktree" > /dev/null 2>&1 || true' EXIT

step "baseline: designated benches at merge-base $base_sha"
(
    cd "$worktree"
    cargo bench -p pramen-ai --bench governance -- --save-baseline perfbase "$FILTER"
    cargo bench -p pramen-io --bench copy_encode -- --save-baseline perfbase "$FILTER"
)

step "candidate: the same benches at HEAD, compared against the baseline"
cargo bench -p pramen-ai --bench governance -- --baseline perfbase "$FILTER"
cargo bench -p pramen-io --bench copy_encode -- --baseline perfbase "$FILTER"

step "gate: mean change per designated bench (fail when even the lower confidence bound exceeds +5%)"
python3 - "$CARGO_TARGET_DIR/criterion" <<'PY'
import json, pathlib, sys

root = pathlib.Path(sys.argv[1])
designated = [
    "workkey/canonicalize_and_hash",
    "ledger/warm_reuse_existing_result",
    "copy_encode/8192",
]
threshold = 0.05
failed = []

for bench in designated:
    # Criterion nests grouped ids (`group/param`) as directories but
    # flattens ungrouped ids by replacing `/` with `_`.
    candidates = [
        root.joinpath(*bench.split("/")) / "change" / "estimates.json",
        root / bench.replace("/", "_") / "change" / "estimates.json",
    ]
    estimates = next((c for c in candidates if c.exists()), None)
    if estimates is None:
        print(f"FAIL {bench}: no change estimates recorded under {root}")
        failed.append(bench)
        continue
    mean = json.loads(estimates.read_text())["mean"]
    point = mean["point_estimate"]
    lower = mean["confidence_interval"]["lower_bound"]
    upper = mean["confidence_interval"]["upper_bound"]
    verdict = "FAIL" if lower > threshold else "ok"
    print(f"{verdict:4} {bench}: {point:+.1%} (95% CI {lower:+.1%} .. {upper:+.1%})")
    if lower > threshold:
        failed.append(bench)

if failed:
    print(f"\nperf gate failed: {', '.join(failed)} regressed beyond +{threshold:.0%}")
    sys.exit(1)
print("\nperf gate passed")
PY
