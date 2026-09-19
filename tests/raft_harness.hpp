// In-process multi-node harness for Raft core tests: instant message delivery, partitions, crash/restart,
// durable-storage images, a trivial replicated log as the "state machine", and safety-invariant checks.
#pragma once

#include <algorithm>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "raftkv/raft.hpp"

namespace th {

using namespace raftkv;

struct Stable {  // what survives a crash
  HardState hs;
  Snapshot snap;
  std::vector<Entry> entries;
};

struct TNode {
  std::unique_ptr<RaftNode> node;
  Stable st;
  bool up = true;
  std::vector<std::string> applied;  // commands applied so far, in order (the "state machine")
  Index applied_index = 0;
  std::vector<ReadState> reads;
  std::vector<std::uint64_t> aborted;
};

inline std::string encode_state(const std::vector<std::string>& v) {
  std::string s;
  for (auto& c : v) s += c + "\n";
  return s;
}
inline std::vector<std::string> decode_state(const std::string& s) {
  std::vector<std::string> v;
  std::stringstream ss(s);
  std::string line;
  while (std::getline(ss, line)) v.push_back(line);
  return v;
}

class Cluster {
 public:
  Cluster(std::size_t n, std::uint64_t seed, Bug bug = Bug::None) : seed_(seed), bug_(bug) {
    for (std::size_t i = 1; i <= n; ++i) ids_.push_back(static_cast<NodeId>(i));
    for (NodeId id : ids_) {
      nodes_[id].node = make_node(id, {});
    }
  }

  std::unique_ptr<RaftNode> make_node(NodeId id, RestoreState rs) {
    Config c;
    c.id = id;
    c.peers = ids_;
    c.seed = seed_;
    c.bug = bug_;
    return std::make_unique<RaftNode>(c, std::move(rs));
  }

  TNode& n(NodeId id) { return nodes_.at(id); }
  const std::vector<NodeId>& ids() const { return ids_; }

  // ---- fault injection ----------------------------------------------------------------------------------
  void crash(NodeId id) {
    n(id).up = false;
    n(id).node.reset();
  }
  void restart(NodeId id) {
    TNode& t = n(id);
    RestoreState rs{t.st.hs, t.st.snap, t.st.entries};
    t.node = make_node(id, std::move(rs));
    t.up = true;
    t.applied = decode_state(t.st.snap.data);
    t.applied_index = t.st.snap.meta.index;
    if (t.st.snap.meta.index == 0) t.applied.clear();
  }
  void partition(std::set<NodeId> a) { side_a_ = std::move(a); partitioned_ = true; }
  void heal() { partitioned_ = false; }
  void isolate(NodeId id) { partition({id}); }
  bool can_talk(NodeId x, NodeId y) const {
    if (!partitioned_) return true;
    return side_a_.count(x) == side_a_.count(y);
  }

  // ---- driving ------------------------------------------------------------------------------------------
  void tick(std::size_t k = 1) {
    for (std::size_t i = 0; i < k; ++i) {
      for (NodeId id : ids_)
        if (n(id).up) n(id).node->tick();
      settle();
    }
  }

  void settle() {
    for (int guard = 0; guard < 100000; ++guard) {
      bool progress = false;
      for (NodeId id : ids_) {
        TNode& t = n(id);
        if (!t.up) continue;
        while (t.node->has_ready()) {
          drive(id);
          progress = true;
        }
      }
      if (!inflight_.empty()) {
        std::vector<Message> batch;
        batch.swap(inflight_);
        for (Message& m : batch) {
          if (!n(m.to).up || !can_talk(m.from, m.to)) continue;
          if (trace && trace_budget-- > 0) {
            static const char* names[] = {"ReqVote", "VoteResp", "Append", "AppendResp", "InstallSnap"};
            std::printf("  %u->%u %s term=%llu logidx=%llu logterm=%llu ents=%zu commit=%llu reject=%d match=%llu hint=%llu/%llu\n", m.from, m.to, names[static_cast<int>(m.type)], (unsigned long long)m.term, (unsigned long long)m.log_index, (unsigned long long)m.log_term, m.entries.size(), (unsigned long long)m.commit, (int)m.reject, (unsigned long long)m.match_index, (unsigned long long)m.hint_index, (unsigned long long)m.hint_term);
          }
          n(m.to).node->step(std::move(m));
        }
        progress = true;
      }
      if (!progress) break;
      if (guard == 99999) fail("settle() did not converge (message livelock)");
    }
    check_invariants();
  }

  void drive(NodeId id) {
    TNode& t = n(id);
    Ready rd = t.node->ready();
    // persist
    if (rd.snapshot) {
      t.st.snap = *rd.snapshot;
      t.st.entries.clear();
      t.applied = decode_state(rd.snapshot->data);
      t.applied_index = rd.snapshot->meta.index;
    }
    if (rd.hard_state) t.st.hs = *rd.hard_state;
    if (!rd.entries.empty()) {
      const Index from = rd.entries.front().index;
      while (!t.st.entries.empty() && t.st.entries.back().index >= from) t.st.entries.pop_back();
      for (auto& e : rd.entries) t.st.entries.push_back(e);
    }
    for (Message& m : rd.messages) inflight_.push_back(std::move(m));
    for (const Entry& e : rd.committed) {
      if (e.type == EntryType::Command) t.applied.push_back(e.data);
      t.applied_index = e.index;
    }
    for (auto& r : rd.read_states) t.reads.push_back(r);
    for (auto r : rd.read_aborted) t.aborted.push_back(r);
    t.node->advance(rd);
  }

  // Snapshot the trivial state machine at the applied index and let the node compact its log.
  void snapshot(NodeId id) {
    TNode& t = n(id);
    if (t.applied_index <= t.node->log().snapshot_meta().index) return;
    const Index idx = t.applied_index;
    Snapshot s;
    s.meta = {idx, *t.node->log().term_at(idx)};
    s.data = encode_state(t.applied);
    t.st.snap = s;
    while (!t.st.entries.empty() && t.st.entries.front().index <= idx) t.st.entries.erase(t.st.entries.begin());
    t.node->compact(s);
  }

  // ---- helpers --------------------------------------------------------------------------------------------
  NodeId leader() {
    NodeId best = kNone;
    Term bt = 0;
    for (NodeId id : ids_)
      if (n(id).up && n(id).node->role() == Role::Leader && n(id).node->term() >= bt) {
        best = id;
        bt = n(id).node->term();
      }
    return best;
  }
  // Tick until a leader exists that a majority of live nodes follow (or give up).
  NodeId wait_leader(std::size_t max_ticks = 400) {
    for (std::size_t i = 0; i < max_ticks; ++i) {
      NodeId l = leader();
      if (l != kNone) {
        std::size_t follow = 0;
        for (NodeId id : ids_) {
          if (!n(id).up || !can_talk(id, l)) continue;
          if (n(id).node->leader() == l && n(id).node->term() == n(l).node->term()) ++follow;
        }
        if (follow * 2 > ids_.size()) return l;
      }
      tick();
    }
    return kNone;
  }
  bool propose(NodeId id, const std::string& cmd) {
    if (!n(id).up) return false;
    const bool ok = n(id).node->propose(cmd).has_value();
    settle();
    return ok;
  }

  // ---- safety invariants (checked after every settle) -------------------------------------------------------
  void check_invariants() {
    // Election safety: at most one leader per term, ever.
    for (NodeId id : ids_) {
      if (!n(id).up || n(id).node->role() != Role::Leader) continue;
      auto [it, inserted] = leader_of_term_.emplace(n(id).node->term(), id);
      if (!inserted && it->second != id) fail("two leaders in term " + std::to_string(n(id).node->term()));
    }
    // Log matching: same (index, term) => identical entry, and identical prefix, across nodes' in-memory logs.
    for (NodeId a : ids_)
      for (NodeId b : ids_) {
        if (a >= b || !n(a).up || !n(b).up) continue;
        const RaftLog& la = n(a).node->log();
        const RaftLog& lb = n(b).node->log();
        const Index lo = std::max(la.first_index(), lb.first_index());
        const Index hi = std::min(la.last_index(), lb.last_index());
        bool same_suffix = false;
        for (Index i = hi; i >= lo && i != 0; --i) {
          if (la.at(i).term == lb.at(i).term) {
            if (!(la.at(i) == lb.at(i))) fail("same index/term, different entry at " + std::to_string(i));
            same_suffix = true;
          } else if (same_suffix) {
            fail("log matching violated: earlier entries differ under a matching entry at " + std::to_string(i));
          }
        }
      }
    // State-machine safety: every node's applied sequence is a prefix of one common sequence.
    for (NodeId a : ids_)
      for (NodeId b : ids_) {
        if (a >= b) continue;
        const auto& x = n(a).applied;
        const auto& y = n(b).applied;
        const std::size_t k = std::min(x.size(), y.size());
        for (std::size_t i = 0; i < k; ++i)
          if (x[i] != y[i]) fail("nodes " + std::to_string(a) + "," + std::to_string(b) + " applied different command at position " + std::to_string(i));
      }
    // Committed entries are never lost: whatever was ever applied anywhere stays a prefix of the longest history.
    for (NodeId a : ids_) {
      const auto& x = n(a).applied;
      for (std::size_t i = 0; i < x.size(); ++i) {
        if (i < history_.size()) {
          if (history_[i] != x[i]) fail("applied history diverged from an earlier committed command at " + std::to_string(i));
        } else {
          history_.push_back(x[i]);
        }
      }
    }
  }

  void fail(const std::string& what) {
    if (failed_) return;
    failed_ = true;
    why_ = what;
    std::printf("    INVARIANT VIOLATION: %s\n", what.c_str());
  }
  bool trace = false;
  int trace_budget = 60;
  bool ok() const { return !failed_; }
  const std::string& why() const { return why_; }

 private:
  std::uint64_t seed_;
  Bug bug_;
  std::vector<NodeId> ids_;
  std::map<NodeId, TNode> nodes_;
  std::vector<Message> inflight_;
  std::set<NodeId> side_a_;
  bool partitioned_ = false;
  std::map<Term, NodeId> leader_of_term_;
  std::vector<std::string> history_;
  bool failed_ = false;
  std::string why_;
};

}  // namespace th
