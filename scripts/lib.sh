# shared helpers (sourced by the other scripts)
BUILD=${BUILD:-build}
export DATA=${DATA:-$(mktemp -d /tmp/raftkv.XXXXXX)}
ADDRS="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
CTL="$BUILD/raftkv_ctl --addrs $ADDRS"
BENCH="$BUILD/raftkv_bench --addrs $ADDRS"
cluster() { DATA="$DATA" BUILD="$BUILD" "$(dirname "${BASH_SOURCE[0]}")/cluster.sh" "$@"; }
# node id that currently leads the most shards
busiest_leader() {
  $CTL info | awk '/^node /{n=$2} /LEADER/{c[n]++} END{best=0; for(k in c) if(c[k]>best){best=c[k]; id=k}; print id}'
}
wait_ready() {  # every shard has a leader
  for _ in $(seq 1 100); do
    [ "$($CTL info | grep -c LEADER)" -ge "${SHARDS:-4}" ] && return 0
    sleep 0.2
  done
  echo "cluster did not elect leaders"; return 1
}
