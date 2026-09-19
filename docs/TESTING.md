# Testing: what was checked, what was found, what was not

## Campaign on the final code (commit 0987d3f)

`raftkv_sim --seeds 1..100000` (3 nodes, 2 shards, 6 clients, 4 keys, 4 s of faults then healing and recovery checks):

| | |
|---|---|
| Runs | 100,000, **0 failures**, 0 inconclusive |
| Operations checked for linearizability | 56,715,080 completed, plus 60,328 with unknown outcome |
| Crashes | 393,166, of which 47,500 were power loss in the middle of an fsync |
| Partitions | 221,047 (isolate a node / bipartition / one-way / node that can reach only one peer) |
| Torn or corrupt WAL tails repaired at recovery | 21,421 |
| Simulated events | 1.23 billion, highest Raft term reached 45 |

Earlier in development (earlier commits): 6,000 five-node runs and 2,500 single-shard high-contention runs, also 0 failures.

## Real bugs the tests found in this code

None of these were safety violations (no history was ever non-linearizable in the real implementation); they were
liveness and recovery bugs. Each has a regression test or is covered by the chaos and simulator runs.

1. **Lost pipelined entries were never retransmitted** (Raft core, found by the in-process chaos test). A leader that had
   optimistically pipelined entries to a follower that was down never learned they were lost, because the follower never
   received anything to reject. Fix: stall detection on heartbeats demotes the follower to probing.
2. **`has_ready()` stuck true forever** (Raft core, found by the chaos test as a hang). A leader that stepped down with a
   broadcast pending kept the "broadcast needed" flag, and only leaders cleared it.
3. **WAL recovery gap** (found by the simulator, seed 1449). If power failed after a snapshot file became durable but before
   the WAL was rewritten, recovery interpreted the stale WAL correctly but left it in place; entries appended afterwards
   made the *next* recovery fail with "WAL has a gap". Fix: recovery rewrites the WAL. Regression test:
   `wal_appends_after_recovering_from_a_snapshot_ahead_of_the_wal_survive_the_next_recovery`.

Also found in the *tests* while building them: a simulator fidelity error (queuing the ticks of a paused node instead of
dropping them made paused leaders step down instantly and hid a stale-read bug) and a bug in a checker property test.

## Does the tester catch broken protocols?

Six deliberately broken variants of Raft are compiled in behind `Config::bug`. Fraction of simulator runs that fail:

| Injected bug | Detected in | Caught by |
|---|---|---|
| `small-quorum`: commit with fewer than a majority | 943 / 1000 | replica divergence, log invariants |
| `vote-ignores-log`: grant votes without the up-to-date check | 556 / 1000 | linearizability / log invariants |
| `read-before-term-commit`: serve reads before committing an entry of the new term | 41 / 1000 | **linearizability checker** |
| `forget-vote`: the vote is never made durable | 5 / 1000 | two leaders in one term |
| `stale-read`: serve reads without confirming leadership | 2 / 1000 | **linearizability checker** |
| `commit-old-term`: Raft paper figure 8 | **0 / 8000** (5 nodes) | **not found** |

`commit-old-term` needs five nodes and a narrow sequence of crashes and elections, and random search did not reach it.
A scripted reproduction would be needed; that is not written. So: the tester finds 5 of 6, and two of those are found
rarely. That is a limit of the search, not evidence that the corresponding code is correct.

## Not covered

* The gRPC layer is exercised by `scripts/integration_test.sh` (a real `kill -9` of a leader, restart, data check), not by
  the simulator, which substitutes its own network.
* fsync failures that the OS reports (EIO), silent disk corruption in the middle of a synced WAL, wall-clock jumps.
* Membership changes, very large snapshots, adversarial (Byzantine) peers.
* Thread-safety of the gRPC server under ThreadSanitizer (gRPC itself is not instrumented, so it would be noisy).
