#!/usr/bin/env bash
# End-to-end test on a REAL 3-process cluster: operations, load, hard leader kill, restart, data check.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh
trap 'cluster stop >/dev/null 2>&1 || true; rm -rf "$DATA"' EXIT
fail() { echo "INTEGRATION TEST FAILED: $*"; exit 1; }

cluster start >/dev/null
wait_ready
echo "1. basic operations"
$CTL put color blue
[ "$($CTL get color)" = "blue" ] || fail "get after put"
[ "$($CTL append log a; $CTL append log b)" = "$(printf 'a\nab')" ] || fail "append results"
$CTL cas color blue red | grep -q "^swapped" || fail "cas should swap"
$CTL cas color blue green | grep -q "^not swapped" || fail "cas should not swap"
[ "$($CTL get color)" = "red" ] || fail "value after cas"

echo "2. leader is hard-killed (kill -9) during a write load"
( sleep 4; L=$(busiest_leader); echo "   killing node $L"; cluster kill "$L"; echo "$L" > "$DATA/killed" ) &
$BENCH --threads 8 --duration 9 --warmup 1 --mode write --gaps | tee "$DATA/bench.txt"
wait
grep -q " 0 failed" "$DATA/bench.txt" || fail "client operations failed during failover"
[ "$($CTL get color)" = "red" ] || fail "data lost after leader kill"

echo "3. killed node restarts and catches up"
K=$(cat "$DATA/killed")
cluster restart "$K"
for _ in $(seq 1 50); do
  sleep 0.3
  $CTL info | awk '/^node /{n=$2} /shard/{print n, $2, $NF, $(NF-2)}' > "$DATA/prog.txt"   # node shard applied commit
  # converged when, per shard, every node has applied the same index
  if awk '{k=$2; if(!(k in mx) || $3>mx[k]) mx[k]=$3; cnt[k,$1]=$3} END{ok=1; for(kk in cnt){split(kk,a,SUBSEP); if(cnt[kk]!=mx[a[1]]) ok=0}; exit ok?0:1}' "$DATA/prog.txt"; then converged=1; break; fi
done
[ "${converged:-0}" = 1 ] || fail "restarted node did not catch up"
[ "$($CTL get color)" = "red" ] || fail "data lost after restart"
echo "INTEGRATION TEST PASSED"
