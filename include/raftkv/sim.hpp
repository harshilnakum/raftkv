// Deterministic whole-cluster simulator (FoundationDB-style): virtual clock, lossy/reordering network with partitions,
// crashable disks with torn writes, per-node clock skew and pauses, random clients that record an operation history,
// and a nemesis that injects faults. Everything derives from ONE seed, so any failure replays exactly.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "raftkv/lincheck.hpp"
#include "raftkv/raft.hpp"

namespace raftkv::sim {

struct SimConfig {
  std::uint64_t seed = 1;
  std::uint32_t nodes = 3;
  std::uint32_t shards = 2;
  std::uint32_t clients = 6;
  std::uint32_t keys = 4;
  std::uint64_t chaos_us = 4'000'000;    // virtual time during which faults are injected and clients run
  std::uint64_t settle_us = 6'000'000;   // virtual time after healing in which the cluster must recover
  std::uint64_t tick_us = 10'000;
  std::uint32_t election_ticks = 10;
  std::uint32_t heartbeat_ticks = 2;
  std::uint64_t snapshot_threshold = 25;
  std::uint64_t attempt_timeout_us = 250'000;
  std::uint64_t op_deadline_us = 2'000'000;  // after this the client gives up: outcome unknown
  bool check_quorum = true;                  // leaders step down when they lose contact with a majority
  bool faults = true;                        // false: healthy network, no crashes (baseline)
  Bug bug = Bug::None;                       // inject a deliberate protocol bug to test the tester
  std::uint64_t max_lin_states = 5'000'000;
  bool verbose = false;
};

struct SimStats {
  std::uint64_t ops_ok = 0, ops_info = 0, gets = 0, puts = 0, appends = 0, cases = 0;
  std::uint64_t sync_crashes = 0, crashes = 0, partitions = 0, freezes = 0, net_profile_changes = 0;
  std::uint64_t msgs_sent = 0, msgs_dropped = 0, msgs_duplicated = 0;
  std::uint64_t torn_wal_repairs = 0;  // recoveries that had to cut off a torn/corrupt WAL tail
  std::uint64_t max_term = 0;
  std::uint64_t lin_states = 0;
  std::uint64_t events = 0;
};

struct SimResult {
  bool ok = true;
  std::string failure;  // empty if ok; otherwise a description incl. the seed
  bool lin_unknown = false;  // checker gave up (state budget): inconclusive, not a failure
  SimStats stats;
  std::vector<lin::Op> history;
  std::uint64_t history_digest = 0;  // equal digests <=> identical runs (determinism check)
};

SimResult run(const SimConfig& cfg);

}  // namespace raftkv::sim
