// A deterministic Raft state machine with no I/O, threads or clocks inside (in the style of etcd/raft).
//
// The caller (a "driver") owns time and I/O and runs this loop:
//     node.tick()                 every logical tick
//     node.step(msg)              for every message received
//     node.propose(...) / node.read_index(...)  for client requests
//     while (node.has_ready()) {
//       Ready rd = node.ready();
//       persist rd.snapshot, rd.hard_state, rd.entries (in that order) and make them DURABLE;
//       send rd.messages;                    // only after persisting: Raft's safety depends on it
//       restore/apply rd.snapshot, apply rd.committed, serve rd.read_states;
//       node.advance(rd);
//     }
// Because the node never touches the clock, disk or network, the same code runs under the deterministic simulator
// and under the real gRPC server.
#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "raftkv/raft_log.hpp"
#include "raftkv/rng.hpp"
#include "raftkv/types.hpp"

namespace raftkv {

enum class Role : std::uint8_t { Follower, Candidate, Leader };

// Deliberate protocol bugs, used ONLY to prove that the simulator + linearizability checker can catch them.
enum class Bug : std::uint8_t {
  None = 0,
  SmallQuorum,        // quorum = n/2 instead of n/2+1: commits without a majority
  CommitOldTerm,      // commit by counting replicas for entries of earlier terms (Raft paper, figure 8)
  VoteIgnoresLog,     // grant votes without the "candidate log is up to date" check
  ForgetVote,         // do not persist the vote: a restarted node can vote twice in one term
  StaleRead,          // serve reads without confirming leadership with a quorum
  ReadBeforeTermCommit,  // serve reads before the new leader has committed an entry of its own term
};

struct Config {
  NodeId id = kNone;
  std::vector<NodeId> peers;  // every member of the group, including id
  std::uint32_t election_ticks = 10;
  std::uint32_t heartbeat_ticks = 2;
  std::uint64_t seed = 1;
  std::size_t max_entries_per_msg = 256;
  std::size_t max_bytes_per_msg = 1u << 20;
  std::uint64_t max_inflight_entries = 8192;
  bool check_quorum = true;  // a leader that cannot reach a majority steps down
  Bug bug = Bug::None;
};

struct ReadState {
  std::uint64_t ctx;  // caller's id for the read request
  Index index;        // serve the read once the state machine has applied at least this index
};

struct Ready {
  std::optional<HardState> hard_state;
  std::vector<Entry> entries;            // persist: first truncate any stored entry with index >= entries.front().index
  std::optional<Snapshot> snapshot;      // persist, then restore the state machine from it
  std::vector<Message> messages;         // send AFTER persisting the above
  std::vector<Entry> committed;          // apply to the state machine, in order
  std::vector<ReadState> read_states;    // linearizable reads that may now be served (after applying up to .index)
  std::vector<std::uint64_t> read_aborted;  // reads dropped because leadership was lost: the caller must fail them
  bool empty() const {
    return !hard_state && entries.empty() && !snapshot && messages.empty() && committed.empty() &&
           read_states.empty() && read_aborted.empty();
  }
};

// What a restarting node recovers from stable storage.
struct RestoreState {
  HardState hs;
  Snapshot snap;
  std::vector<Entry> entries;  // log entries following the snapshot, in order
};

class RaftNode {
 public:
  explicit RaftNode(Config cfg, RestoreState restore = {});

  void tick();
  void step(Message m);

  // Leader only: append a command. Returns {index, term} of the new entry, or nullopt if not the leader.
  std::optional<std::pair<Index, Term>> propose(std::string data);

  // Leader only: request a linearizable read barrier (ReadIndex). False if not currently able to serve one
  // (not leader, or no entry of this term committed yet); the caller should redirect or retry.
  bool read_index(std::uint64_t ctx);

  bool has_ready() const;
  Ready ready();
  void advance(const Ready& rd);

  // The state machine took a snapshot at meta.index (<= applied): drop the covered log prefix.
  void compact(Snapshot snap);

  Role role() const { return role_; }
  Term term() const { return term_; }
  NodeId leader() const { return leader_; }
  NodeId id() const { return cfg_.id; }
  Index commit_index() const { return commit_; }
  Index applied_index() const { return applied_; }
  Index stable_index() const { return stable_; }
  const RaftLog& log() const { return log_; }
  HardState hard_state() const { return {term_, vote_}; }
  std::size_t quorum() const;

 private:
  enum class PState : std::uint8_t { Probe, Replicate, Snapshot };
  struct Progress {
    Index match = 0;
    Index next = 1;
    PState state = PState::Probe;
    bool paused = false;  // probe: an append is in flight, wait for the response
    bool recent_active = false;
    std::uint64_t acked_read_seq = 0;
    Index pending_snapshot = 0;
    std::uint32_t snapshot_wait = 0;
    Index hb_match = 0;            // match at the previous heartbeat (stall detection)
    std::uint32_t stalled_hbs = 0;  // consecutive heartbeats with no progress while the follower is behind
  };
  struct PendingRead {
    std::uint64_t ctx;
    Index index;
    std::uint64_t seq;
  };

  void become_follower(Term term, NodeId leader);
  void become_candidate();
  void become_leader();
  void campaign();
  void reset_election_timer();

  void tick_election();
  void tick_leader();
  bool quorum_active();

  void handle_request_vote(const Message& m);
  void handle_vote_resp(const Message& m);
  void handle_append(const Message& m);
  void handle_snapshot(const Message& m);
  void handle_append_resp(const Message& m);

  void send(Message m);
  void send_append(NodeId to, bool force);
  void broadcast_append();
  void broadcast_heartbeat();
  bool maybe_commit();
  void update_reads();
  bool commit_in_term() const;
  bool is_single() const { return cfg_.peers.size() == 1; }

  Config cfg_;
  Rng rng_;
  RaftLog log_;
  Role role_ = Role::Follower;
  Term term_ = 0;
  NodeId vote_ = kNone;
  NodeId leader_ = kNone;
  Index commit_ = 0;
  Index applied_ = 0;
  Index stable_ = 0;  // highest index known durable

  std::uint32_t election_elapsed_ = 0;
  std::uint32_t heartbeat_elapsed_ = 0;
  std::uint32_t randomized_timeout_ = 0;

  std::map<NodeId, Progress> prs_;
  std::map<NodeId, bool> votes_;  // candidate: who answered, and whether they granted

  std::uint64_t read_seq_ = 0;
  std::deque<PendingRead> reads_;
  bool need_read_round_ = false;
  bool need_bcast_ = false;

  bool hs_dirty_ = false;
  std::vector<Message> msgs_;
  std::vector<ReadState> read_states_;
  std::vector<std::uint64_t> read_aborted_;
  std::optional<Snapshot> pending_snapshot_;  // received from the leader, not yet persisted/restored
  Snapshot latest_snapshot_;                  // most recent snapshot (kept to serve followers that fell behind)
};

}  // namespace raftkv
