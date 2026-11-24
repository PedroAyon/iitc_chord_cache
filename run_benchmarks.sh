#!/usr/bin/env bash
# Runs benchmark scenarios one by one on a fresh cluster, using the Makefile targets.
# Raw results are saved as results/<name>.json (and the console output as results/<name>.log).
# Works with any Makefile where `make bench` writes bench.json and `make results` copies it to ./results.
#
# Usage: bash run_benchmarks.sh          # A B C D
#        bash run_benchmarks.sh B D      # only the given scenarios
#
#   A  10 nodes, concurrency  8   smaller ring
#   B  31 nodes, concurrency  8   baseline
#   C  31 nodes, concurrency 32   higher load
#   D  31 nodes, concurrency  1   single client
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

FILES=${FILES:-500}
REQUESTS=${REQUESTS:-50000}
WAIT=${WAIT:-30}          # seconds for the ring and finger tables to stabilize after "make up"
export LOG_LEVEL=WARN     # same non-debug logging for every scenario

scenario() {
  case "$1" in
    A) NODES=10; CONC=8 ;;
    B) NODES=31; CONC=8 ;;
    C) NODES=31; CONC=32 ;;
    D) NODES=31; CONC=1 ;;
    *) echo "unknown scenario '$1' (valid: A B C D)" >&2; exit 2 ;;
  esac
}

scenarios=("$@")
if (( ${#scenarios[@]} == 0 )); then scenarios=(A B C D); fi
for s in "${scenarios[@]}"; do scenario "$s"; done   # validate names before starting

mkdir -p results
make build

for s in "${scenarios[@]}"; do
  scenario "$s"
  echo
  echo "=== Scenario $s: $NODES nodes, concurrency $CONC ==="

  make down
  make up WORKERS=$((NODES - 1)) WAIT="$WAIT"
  make seed FILES="$FILES"

  summary=$(make -s ring 2>/dev/null | grep ' nodes, ' | tail -n 1 || true)
  if [[ "$summary" != "$NODES nodes, $FILES files stored" ]]; then
    echo "unexpected ring state: '$summary'" >&2
    exit 1
  fi

  make bench FILES="$FILES" REQUESTS="$REQUESTS" CONCURRENCY="$CONC" DIST=both | tee "results/$s.log"
  make results
  mv -f results/bench.json "results/$s.json"
  echo "saved results/$s.json"
done

make down
echo
echo "Done: ${scenarios[*]/%/.json} are in results/"