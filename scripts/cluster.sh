#!/usr/bin/env bash
# Local 3-node cluster on 127.0.0.1:7001-7003.   scripts/cluster.sh start|stop|kill N|restart N|info|clean
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=${BUILD:-build}
DATA=${DATA:-/tmp/raftkv-data}
SHARDS=${SHARDS:-4}
PEERS="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
ADDRS="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
pidfile() { echo "$DATA/node$1.pid"; }
start_node() {
  mkdir -p "$DATA"
  setsid nohup "$BUILD/raftkv_server" --id "$1" --peers "$PEERS" --data "$DATA" --shards "$SHARDS" ${EXTRA_ARGS:-} >>"$DATA/node$1.log" 2>&1 < /dev/null &
  echo $! > "$(pidfile "$1")"
}
case "${1:-}" in
  start) for i in 1 2 3; do start_node "$i"; done; sleep 2; "$BUILD/raftkv_ctl" --addrs "$ADDRS" --shards "$SHARDS" info ;;
  stop) for i in 1 2 3; do [ -f "$(pidfile $i)" ] && kill "$(cat "$(pidfile $i)")" 2>/dev/null || true; done ;;
  kill) kill -9 "$(cat "$(pidfile "$2")")" ;;   # hard kill (no clean shutdown)
  restart) start_node "$2" ;;
  info) "$BUILD/raftkv_ctl" --addrs "$ADDRS" --shards "$SHARDS" info ;;
  clean) rm -rf "$DATA" ;;
  addrs) echo "$ADDRS" ;;
  *) echo "usage: $0 start|stop|kill N|restart N|info|clean|addrs"; exit 2 ;;
esac
