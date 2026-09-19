// In-memory Raft log with a compacted prefix (snapshot). Entry i lives at ents_[i - first_index()].
#pragma once

#include <cassert>
#include <cstddef>
#include <optional>
#include <vector>

#include "raftkv/types.hpp"

namespace raftkv {

class RaftLog {
 public:
  Index first_index() const { return snap_.index + 1; }
  Index last_index() const { return snap_.index + ents_.size(); }
  const SnapshotMeta& snapshot_meta() const { return snap_; }

  // Term of entry i; 0 for i == 0; the snapshot's term for i == snapshot index; nullopt outside [snapshot, last].
  std::optional<Term> term_at(Index i) const {
    if (i == snap_.index) return snap_.term;  // covers i == 0 with an empty snapshot
    if (i < snap_.index || i > last_index()) return std::nullopt;
    return ents_[i - first_index()].term;
  }
  Term last_term() const { return *term_at(last_index()); }
  bool match_term(Index i, Term t) const {
    const auto x = term_at(i);
    return x && *x == t;
  }
  // Raft's "at least as up-to-date" comparison for elections.
  bool up_to_date(Index cand_index, Term cand_term) const {
    const Term mine = last_term();
    return cand_term > mine || (cand_term == mine && cand_index >= last_index());
  }

  const Entry& at(Index i) const {
    assert(i >= first_index() && i <= last_index());
    return ents_[i - first_index()];
  }

  // Entries [from, ...] limited by count and (approximate) payload bytes; always returns at least one if available.
  std::vector<Entry> slice(Index from, std::size_t max_entries, std::size_t max_bytes) const {
    std::vector<Entry> out;
    if (from < first_index() || from > last_index()) return out;
    std::size_t bytes = 0;
    for (Index i = from; i <= last_index() && out.size() < max_entries; ++i) {
      const Entry& e = at(i);
      if (!out.empty() && bytes + e.data.size() > max_bytes) break;
      bytes += e.data.size();
      out.push_back(e);
    }
    return out;
  }

  // Leader appends one entry; assigns its index. Returns that index.
  Index append_new(Entry e) {
    e.index = last_index() + 1;
    ents_.push_back(std::move(e));
    return ents_.back().index;
  }

  // Follower path: merges `es` (whose first entry follows prev_index) into the log, truncating a conflicting suffix.
  // Returns the index of the first entry that was added or replaced (0 if the log did not change).
  Index merge(const std::vector<Entry>& es) {
    Index first_changed = 0;
    for (const Entry& e : es) {
      if (e.index <= snap_.index) continue;  // already compacted, therefore committed
      if (e.index <= last_index()) {
        if (ents_[e.index - first_index()].term == e.term) continue;  // already have it
        ents_.resize(e.index - first_index());                        // conflict: drop this entry and everything after
      }
      assert(e.index == last_index() + 1);
      if (first_changed == 0) first_changed = e.index;
      ents_.push_back(e);
    }
    return first_changed;
  }

  // Replace the whole log with a snapshot (follower installing a snapshot).
  void restore(const SnapshotMeta& meta) {
    snap_ = meta;
    ents_.clear();
  }

  // Drop entries up to and including `index` (after a local snapshot). Requires first_index() <= index <= last_index().
  void compact(Index index, Term term) {
    assert(index >= snap_.index && index <= last_index());
    if (index == snap_.index) return;
    const std::size_t drop = index - snap_.index;
    ents_.erase(ents_.begin(), ents_.begin() + static_cast<std::ptrdiff_t>(drop));
    snap_ = {index, term};
  }

  // Index of the last entry with the given term, or 0 (used to skip ahead after a rejected append).
  Index last_index_of_term(Term t) const {
    for (Index i = last_index(); i >= first_index() && i != 0; --i)
      if (at(i).term == t) return i;
    return 0;
  }

  // Test helper: number of entries held in memory.
  std::size_t size() const { return ents_.size(); }

 private:
  SnapshotMeta snap_;
  std::vector<Entry> ents_;
};

}  // namespace raftkv
