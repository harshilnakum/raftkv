#!/usr/bin/env bash
# Measure how long clients see no successful operation after a leader is killed with kill -9. Repeats RUNS times.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh
RUNS=${RUNS:-5}
gaps=()
for i in $(seq 1 "$RUNS"); do
  export DATA=$(mktemp -d /tmp/raftkv.XXXXXX)
  cluster start >/dev/null
  wait_ready
  ( sleep 5; cluster kill "$(busiest_leader)" ) &
  out=$($BENCH --threads 8 --duration 10 --warmup 1 --mode write --gaps)
  wait
  g=$(echo "$out" | grep -oE "no successful operation: [0-9]+" | grep -oE "[0-9]+$")
  echo "run $i: longest interval without a successful operation: ${g} ms   ($(echo "$out" | grep ops/s))"
  gaps+=("$g")
  cluster stop >/dev/null 2>&1 || true
  rm -rf "$DATA"
  sleep 1
done
sorted=($(printf '%s\n' "${gaps[@]}" | sort -n))
echo "failover gaps (ms), sorted: ${sorted[*]}"
echo "median ${sorted[$((${#sorted[@]}/2))]} ms, max ${sorted[-1]} ms"
