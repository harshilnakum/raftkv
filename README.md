# raftkv

[![CI](https://github.com/harshilnakum/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/harshilnakum/raftkv/actions/workflows/ci.yml)

A linearizable, hash-partitioned key-value store built on Raft, in C++20: leader election, log replication, snapshots, a
write-ahead log, gRPC between nodes and clients, and a **deterministic simulator** that runs whole clusters (network,
disks, clocks, clients) from a seed and checks every history for linearizability.

Its main claim is not speed. It is: *every failure this project can produce is replayable from a seed, and the checks that
say "correct" have themselves been shown to fail on broken protocols.*

```
            clients (raftkv_bench, raftkv_ctl, KvClient: retries with a stable (client, seq) => exactly-once)
                 |   gRPC  Kv.Execute
   +-------------+--------------+--------------+          hash(key) % S picks the shard
   |   node 1    |   node 2     |   node 3     |
   | shard 0..S-1| shard 0..S-1 | shard 0..S-1 |          each shard = one Raft group of 3 replicas
   +------+------+------+-------+------+-------+
          |    gRPC RaftTransport (one long-lived stream per peer pair)
     one runner thread per node owns every replica: ticks, messages and client requests are batched,
     then ONE WAL fsync covers the batch (group commit) before any message of that batch is sent
```

## What is in here

| Piece | Where | Notes |
|---|---|---|
| Raft core | `include/raftkv/raft.hpp`, `src/raft.cpp` | No I/O, threads or clocks inside. Election, pipelined replication, snapshots, ReadIndex, CheckQuorum. |
| WAL and snapshots | `wal.hpp`, `src/wal.cpp` | CRC-protected records, torn-tail repair, atomic snapshot files, crash-safe compaction. |
| KV state machine | `kv.hpp` | put / append / compare-and-swap, client sessions for exactly-once, deterministic snapshots. |
| Shard replica | `replica.hpp` | Persist, then send, then apply. Pending requests, linearizable reads, snapshot policy. |
| gRPC server / client | `src/node_server.cpp`, `src/kv_client.cpp` | Runner thread, batching, leader-hint retries. |
| Deterministic simulator | `include/raftkv/sim.hpp`, `src/sim.cpp` | Virtual clock, faulty network, crashable disks, clock skew, nemesis, clients. |
| Linearizability checker | `lincheck.hpp`, `src/lincheck.cpp` | Wing and Gong search with memoisation, per key, handles unknown-outcome operations. |

## How correctness is checked

1. **Unit and property tests** for the log, WAL (crash-consistency fuzz against a model), state machine and checker.
2. **Raft invariants** (election safety, log matching, state-machine safety, no committed entry lost) checked continuously
   in an in-process cluster with random crashes, partitions and snapshots.
3. **Whole-cluster simulation.** Per run: a 3- or 5-node cluster, several shards, concurrent clients, and faults:
   dropped, duplicated, delayed and reordered messages; one-way and asymmetric partitions; power loss (unsynced bytes lost
   or torn, sometimes damaged), including *in the middle of an fsync*; per-node clock skew; process pauses; killing or
   pausing the current leader; a crash right after a node grants a vote. Afterwards the cluster is healed and must
   (a) serve a write and a linearizable read on every shard, (b) converge to identical replica state, and
   (c) the recorded history must be **linearizable**.
4. **The tester is tested.** Six protocol bugs can be switched on (`--bug`); the simulator must fail on them. See
   [docs/TESTING.md](docs/TESTING.md) for which ones it finds and how often, and which one it does not find.
5. ASan/UBSan, GCC and Clang with `-Werror`, an end-to-end script that kills a leader process with `kill -9`.

Campaign on the final code: **100,000 seeds, 0 failures**: 56.7M operations checked, 393K crashes (47.5K power losses mid-fsync), 221K partitions, 21K torn-WAL recoveries (details in [docs/TESTING.md](docs/TESTING.md)).

Reproduce any failure exactly: `raftkv_sim --seeds 1449` replays seed 1449, event for event.

## Quick start

```
# needs a C++20 compiler, CMake, and gRPC/protobuf (Ubuntu: apt install libgrpc++-dev protobuf-compiler-grpc pkg-config)
cmake -S . -B build && cmake --build build -j
ctest --test-dir build                       # unit tests
./build/raftkv_sim --seeds 1..1000           # 1000 fault-injected cluster runs, each checked for linearizability
./build/raftkv_sim --bug small-quorum --seeds 1..20   # watch the tester catch a broken protocol

scripts/cluster.sh start                     # real 3-process cluster on 127.0.0.1:7001-7003
build/raftkv_ctl --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put hello world
scripts/integration_test.sh                  # kill -9 the leader under load, restart it, verify data
docker compose up --build                    # same cluster in containers
```

## Measured (real processes over loopback gRPC)

GitHub-hosted 4-vCPU Linux runner, **all three servers and the load generator on the same shared machine**, 4 shards,
128-byte values. Raw output: [docs/results/github-runner](docs/results/github-runner/summary.txt).

| | 16 client threads | 64 client threads |
|---|---|---|
| Writes (majority-replicated, fsynced) | 3.9K ops/s, p50 4.0 ms, p99 8.5 ms | **7.2K ops/s**, p50 8.7 ms, p99 14.8 ms |
| Linearizable reads (ReadIndex) | 7.8K ops/s, p50 2.0 ms, p99 3.4 ms | **11.7K ops/s**, p50 5.3 ms, p99 10.0 ms |
| Mixed 50/50 | 4.7K ops/s | 7.7K ops/s |

Failover: `kill -9` of the node leading the most shards under a write load, 7 runs: longest interval without a successful
operation **median 336 ms, max 532 ms** (election timeout is 300 to 600 ms); 0 failed client operations in every run.

No comparison with other systems was made, and a dedicated 3-machine cluster would behave differently (network and fsync
latency instead of CPU contention). See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Design decisions and limits

* **Static membership**: three (or five) fixed nodes per group. No dynamic reconfiguration, no learners.
* **No PreVote.** A partitioned node inflates its term and briefly disturbs the leader when it rejoins. Safe, not smooth.
* **Reads** go through the leader with ReadIndex (a quorum heartbeat round, batched across concurrent reads), so they are
  linearizable but not served by followers.
* **Exactly-once** uses one session per client id with strictly increasing sequence numbers; sessions are never expired.
* Single-key operations only: no cross-shard transactions. No authentication or TLS.
* Every request path is exercised by the simulator; the gRPC layer itself is covered by the integration script, not by the
  simulator (the simulator replaces the network with its own).

More in [docs/DESIGN.md](docs/DESIGN.md).

MIT licensed.
