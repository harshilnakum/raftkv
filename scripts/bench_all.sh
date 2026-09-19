#!/usr/bin/env bash
# Throughput and latency of a real 3-node cluster (all processes on this machine) for writes, linearizable reads, mixed.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh
trap 'cluster stop >/dev/null 2>&1 || true; rm -rf "$DATA"' EXIT
DUR=${DUR:-10}
cluster start >/dev/null
wait_ready
for mode in write read mixed; do
  for threads in ${THREADS:-16 64}; do
    echo "=== $mode, $threads client threads"
    $BENCH --mode "$mode" --threads "$threads" --duration "$DUR" --warmup 2 --keys 10000 --value-size 128
  done
done
