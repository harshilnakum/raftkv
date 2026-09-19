# Design notes

## One rule: the Raft core never touches the outside world

`RaftNode` is a state machine. The caller feeds it ticks, messages and proposals, then loops:

    Ready rd = node.ready();     // what to do next
    persist rd (snapshot, hard state, entries)  and make it durable
    send rd.messages             // only AFTER the fsync
    apply rd.committed, serve rd.read_states
    node.advance(rd);

The same object runs unchanged under the unit-test harness, the deterministic simulator and the real gRPC server.
That is what makes the simulator meaningful: it is exercising the production code, not a model of it.

## Replication details that matter

* **Commit rule:** a leader counts replicas only for entries of its *own* term, so it appends a no-op when elected
  (Raft paper, figure 8). The `commit-old-term` injected bug removes that check.
* **Pipelining:** in `Replicate` state the leader advances `next` optimistically. A dropped message is normally noticed by
  a rejection; if nothing newer is sent it would go unnoticed forever, so a follower that is behind and makes no progress
  for several heartbeats is demoted to `Probe`. (This was a real bug, found by the chaos test.)
* **Fast backtracking:** a rejection carries the conflicting term and its first index, so the leader skips whole terms.
* **Snapshots:** a follower whose `next` was compacted away receives `InstallSnapshot`; it is re-sent if the ack is lost.
* **ReadIndex:** the leader records `commit`, then confirms it is still leader with a heartbeat round that any read
  arriving before it can share. It refuses reads until it has committed an entry of its own term.
* **CheckQuorum:** a leader that hears from no majority for an election timeout steps down.

## Storage

The WAL is a sequence of `[len][crc32c][type][payload]` records: hard state, entries, snapshot marker. Replay rules:
an entries record first discards every stored entry with an index at or after its first index (that is how a follower's
conflicting suffix is overwritten); a snapshot marker resets the log to start after that index. A torn or corrupt tail is
cut off. Snapshots are written to a temp file, fsynced, then renamed. After a local snapshot the WAL is rewritten the
same way. If power fails between the snapshot file and the WAL rewrite, recovery *repairs* the WAL, because appending to
the stale one creates a gap that breaks the next recovery (a real bug, found by the simulator).

## Linearizability checking

Each key is checked independently. An operation is a call/return interval. The search (Wing and Gong, with the
memoisation of Lowe / Porcupine) tries to order the operations so that real-time order is respected and every observed
result is legal for a sequential register with put, append and compare-and-swap. Append returns the resulting value
and compare-and-swap returns the value it saw, which makes histories far more constraining than plain reads and writes.
A client that gives up leaves an operation with unknown outcome: it may have taken effect at any time after its call, or
never; it continues under a new process id. The checker is bounded by a state budget and reports "inconclusive"
rather than ever claiming a false violation.

## The simulator

One seeded PRNG and one event queue ordered by (virtual time, sequence). No wall clock, no threads, no real disk.
Nodes are real `ShardReplica`s over an in-memory disk whose crash semantics are: synced bytes survive; unsynced bytes
vanish or survive as a torn prefix, possibly with a damaged last byte. Ticks of a paused node are *dropped*, not queued
(queuing them models a process that fires 100 timers at once, which no real pause does; getting that wrong hid a bug).
