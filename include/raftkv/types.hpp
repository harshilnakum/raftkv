// Core Raft types. Nothing here does I/O.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace raftkv {

using NodeId = std::uint32_t;  // 0 is "none"; real node ids start at 1
using Term = std::uint64_t;
using Index = std::uint64_t;

inline constexpr NodeId kNone = 0;

enum class EntryType : std::uint8_t {
  Noop = 0,     // appended by a new leader to commit entries from earlier terms
  Command = 1,  // opaque payload for the state machine
};

struct Entry {
  Term term = 0;
  Index index = 0;
  EntryType type = EntryType::Command;
  std::string data;
  friend bool operator==(const Entry& a, const Entry& b) {
    return a.term == b.term && a.index == b.index && a.type == b.type && a.data == b.data;
  }
};

struct SnapshotMeta {
  Index index = 0;  // last log index covered by the snapshot
  Term term = 0;    // term of that entry
  friend bool operator==(const SnapshotMeta& a, const SnapshotMeta& b) { return a.index == b.index && a.term == b.term; }
};

struct Snapshot {
  SnapshotMeta meta;
  std::string data;  // serialized state machine at meta.index
};

struct HardState {
  Term term = 0;
  NodeId vote = kNone;
  friend bool operator==(const HardState& a, const HardState& b) { return a.term == b.term && a.vote == b.vote; }
};

enum class MsgType : std::uint8_t {
  RequestVote,
  RequestVoteResp,
  Append,  // AppendEntries; with no entries it is also the heartbeat
  AppendResp,
  InstallSnapshot,
};

struct Message {
  MsgType type = MsgType::Append;
  NodeId from = kNone;
  NodeId to = kNone;
  Term term = 0;

  // RequestVote: candidate's last log index/term. Append: previous entry's index/term.
  Index log_index = 0;
  Term log_term = 0;

  std::vector<Entry> entries;  // Append
  Index commit = 0;            // Append: leader's commit index

  bool reject = false;         // RequestVoteResp: !granted. AppendResp: log mismatch
  Index match_index = 0;       // AppendResp (success): highest index known to match the leader
  Index hint_index = 0;        // AppendResp (reject): where the leader should retry
  Term hint_term = 0;          // AppendResp (reject): term at the conflicting position (0 = follower log too short)

  std::uint64_t read_seq = 0;  // leadership-confirmation round for ReadIndex; echoed in AppendResp

  Snapshot snapshot;  // InstallSnapshot
};

}  // namespace raftkv
