#!/usr/bin/env bash
# Benchmark suite v1 (P1.20): end-to-end throughput, CPU seconds per GiB,
# and peak RSS for the Parquet -> SQL -> PostgreSQL vertical, measured
# against the DataFusion-direct engine ceiling (and DuckDB, when its CLI
# is installed).
#
# Requirements: cargo, `psql`, and PRAMEN_POSTGRES_DSN pointing at a
# writable database. All inputs are generated deterministically, so runs
# are reproducible bit-for-bit; absolute numbers still depend on the
# machine, which the script records.
#
# Environment:
#   PRAMEN_POSTGRES_DSN  (required) target database
#   BENCH_ROWS           input rows to generate    (default 5000000)
#   BENCH_FILES          parquet files to split into (default 8)
#   BENCH_DIR            input directory             (default /tmp/pramen-bench-input)
set -euo pipefail
cd "$(dirname "$0")/.."

: "${PRAMEN_POSTGRES_DSN:?set PRAMEN_POSTGRES_DSN, e.g. postgres://postgres:quickstart@localhost:5432/postgres}"
BENCH_ROWS=${BENCH_ROWS:-5000000}
BENCH_FILES=${BENCH_FILES:-8}
BENCH_DIR=${BENCH_DIR:-/tmp/pramen-bench-input}
QUERY="SELECT id, category, amount, amount * 1.21 AS amount_gross, active, created_at, note FROM read_parquet('$BENCH_DIR/*.parquet') WHERE category <> 'epsilon'"

step() { printf '\n==> %s\n' "$*"; }

# Run a command under /usr/bin/time and print "wall_s cpu_s max_rss_mib".
# Handles both the BSD (-l, RSS in bytes) and GNU (-v, RSS in KiB) formats.
measure() {
    local out
    out=$(mktemp)
    if [ "$(uname)" = "Darwin" ]; then
        # Command stdout is routed to the caller's stderr so it stays
        # visible without polluting the captured measurement line.
        /usr/bin/time -l "$@" 1>&2 2> "$out"
        awk '
            /real/ && /user/ && /sys/ { wall = $1; cpu = $3 + $5 }
            /maximum resident set size/ { rss = $1 / 1048576 }
            END { printf "%.2f %.2f %.0f\n", wall, cpu, rss }
        ' "$out"
    else
        /usr/bin/time -v "$@" 1>&2 2> "$out"
        awk -F': ' '
            /Elapsed \(wall clock\)/ {
                n = split($2, t, ":")
                wall = t[n] + t[n-1] * 60 + (n == 3 ? t[1] * 3600 : 0)
            }
            /User time/ { cpu += $2 }
            /System time/ { cpu += $2 }
            /Maximum resident set size/ { rss = $2 / 1024 }
            END { printf "%.2f %.2f %.0f\n", wall, cpu, rss }
        ' "$out"
    fi
    rm -f "$out"
}

step "build (release): pramen binary, generator, baseline"
cargo build --release -p pramen -p pramen-io --examples --quiet

step "input: $BENCH_ROWS deterministic rows across $BENCH_FILES parquet files"
marker="$BENCH_DIR/.generated-$BENCH_ROWS-$BENCH_FILES"
if [ ! -f "$marker" ]; then
    rm -rf "$BENCH_DIR" && mkdir -p "$BENCH_DIR"
    target/release/examples/gen_bench_data "$BENCH_DIR" "$BENCH_ROWS" "$BENCH_FILES"
    touch "$marker"
else
    echo "reusing existing dataset in $BENCH_DIR"
fi
input_bytes=$(du -sk "$BENCH_DIR" | awk '{print $1 * 1024}')
input_gib=$(awk -v b="$input_bytes" 'BEGIN { printf "%.3f", b / 1073741824 }')
echo "input size: ${input_gib} GiB on disk (snappy parquet)"

step "baseline: DataFusion direct (same SQL, no sink)"
read -r df_wall df_cpu df_rss <<< "$(measure target/release/examples/datafusion_direct "$BENCH_DIR/")"

if command -v duckdb > /dev/null 2>&1; then
    step "baseline: DuckDB (same SQL into a native DuckDB table)"
    duck_db=$(mktemp -u /tmp/pramen-bench-duck-XXXX.db)
    read -r duck_wall duck_cpu duck_rss <<< "$(measure duckdb "$duck_db" -c "CREATE TABLE bench AS $QUERY")"
    rm -f "$duck_db"
else
    step "baseline: DuckDB CLI not installed; skipping"
    duck_wall="" duck_cpu="" duck_rss=""
fi

step "target table"
psql "$PRAMEN_POSTGRES_DSN" -q <<'SQL'
CREATE SCHEMA IF NOT EXISTS analytics;
DROP TABLE IF EXISTS analytics.bench_events;
CREATE TABLE analytics.bench_events (
    id           bigint NOT NULL,
    category     text NOT NULL,
    amount       double precision NOT NULL,
    amount_gross double precision NOT NULL,
    active       boolean NOT NULL,
    created_at   timestamptz NOT NULL,
    note         text
);
SQL

step "pramen: end to end (Parquet -> SQL -> binary COPY -> PostgreSQL)"
read -r p_wall p_cpu p_rss <<< "$(measure target/release/pramen run benchmarks/parquet-to-postgres.yaml --log-format silent)"
rows_out=$(psql "$PRAMEN_POSTGRES_DSN" -tA -c "SELECT count(*) FROM analytics.bench_events")
expected=$(( BENCH_ROWS / 5 * 4 ))
if [ "$rows_out" -ne "$expected" ]; then
    echo "FAIL: expected $expected rows in analytics.bench_events, found $rows_out" >&2
    exit 1
fi

step "results"
host_info="$(uname -sm), $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')"
echo "machine: $host_info"
echo "dataset: $BENCH_ROWS rows in, $rows_out rows out, ${input_gib} GiB parquet"
printf '%-34s %10s %10s %12s %14s %12s\n' "path" "wall s" "cpu s" "rows out/s" "cpu-s / GiB in" "peak RSS MiB"
row() { # name wall cpu rss rows
    if [ -z "$2" ]; then return; fi
    printf '%-34s %10s %10s %12s %14s %12s\n' "$1" "$2" "$3" \
        "$(awk -v r="$5" -v w="$2" 'BEGIN { printf "%.0f", r / w }')" \
        "$(awk -v c="$3" -v g="$input_gib" 'BEGIN { printf "%.1f", c / g }')" "$4"
}
row "pramen end-to-end (to PostgreSQL)" "$p_wall" "$p_cpu" "$p_rss" "$rows_out"
row "datafusion direct (no sink)" "$df_wall" "$df_cpu" "$df_rss" "$rows_out"
row "duckdb (native table, no PG)" "${duck_wall:-}" "${duck_cpu:-}" "${duck_rss:-}" "$rows_out"
echo
echo "bench complete; see docs/benchmarks/ for published reports"
