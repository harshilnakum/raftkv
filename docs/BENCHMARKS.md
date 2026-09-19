# Benchmarks

## Method

`scripts/bench_all.sh` starts a real 3-process cluster (4 shards, 128-byte values, 10,000 keys) on one machine and drives it
with `raftkv_bench`: closed-loop client threads over gRPC. Writes are replicated to a majority and fsynced; reads are
linearizable (ReadIndex through the leader). `scripts/failover_bench.sh` kills the node leading the most shards with
`kill -9` under a write load and reports the longest interval in which *no client operation succeeded*.

All three server processes and the load generator run on the same machine and compete for its cores. Numbers are therefore
a floor for what a dedicated cluster would do, and depend heavily on core count and disk fsync latency.

## Results, development VM (1 vCPU, Intel Xeon 2.1 GHz, GCC 13.3, -O2, ext4 without journal)

| Workload (32 client threads) | Throughput | Latency p50 / p99 |
|---|---|---|
| Writes | 2,891 ops/s | 10.3 ms / 23.5 ms |
| Linearizable reads | 5,412 ops/s | 5.1 ms / 14.7 ms |
| Mixed 50/50 | 3,286 ops/s | 8.9 ms / 21.7 ms |

Failover (6 independent runs, election timeout 300 to 600 ms): longest no-success interval 332, 346, 381, 434, 438,
503 ms; **median 434 ms, max 503 ms**; 0 failed client operations in every run (retries with the same client/seq).
Three earlier single runs gave 306 and 479 ms.

## Results, GitHub-hosted runner (4 vCPU shared VM); raw text in `docs/results/github-runner/summary.txt`

| Workload | 16 threads | 64 threads |
|---|---|---|
| Writes | 3,911 ops/s (p50 3.97 ms, p99 8.46 ms) | 7,161 ops/s (p50 8.72 ms, p99 14.77 ms) |
| Linearizable reads | 7,780 ops/s (p50 2.02 ms, p99 3.38 ms) | 11,681 ops/s (p50 5.32 ms, p99 9.96 ms) |
| Mixed 50/50 | 4,747 ops/s (p50 3.25 ms, p99 6.25 ms) | 7,715 ops/s (p50 8.05 ms, p99 14.74 ms) |

Failover, 7 runs (kill -9 of the busiest leader under load): 308, 330, 333, 336, 365, 430, 532 ms; **median 336 ms, max
532 ms**; 0 failed client operations in all runs. Throughput while the failover runs were in progress was about 3.0K writes/s
(8 client threads).

The same workflow re-ran `raftkv_sim --seeds 1..50000`: 50,000 runs, 0 failures (28.3M operations, 196,813 crashes).
These are the same seeds as the local campaign, so the two runs confirm each other rather than adding coverage.

## Caveats

* One core for everything means the numbers mostly measure CPU contention, not the protocol.
* No comparison against etcd or any other system was made.
* Simulator throughput (`raftkv_sim`, about 600 to 800 runs per second per core) is unrelated to the server's.
