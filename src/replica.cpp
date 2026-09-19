#include "raftkv/replica.hpp"

namespace raftkv {


// Construction order: storage_ -> recover -> state machine -> raft node (initialised from what was recovered).
ShardReplica::ShardReplica(ShardId shard, Config raft_cfg, FileSystem& fs, const std::string& prefix, ReplicaOptions opt)
    : shard_(shard),
      opt_(opt),
      storage_(fs, prefix),
      raft_([&]() -> RaftNode {
        Recovered rec = storage_.recover();
        repaired_bytes_ = rec.repaired_bytes;
        sm_.restore(rec.snap.data);
        return RaftNode(std::move(raft_cfg), RestoreState{rec.hs, std::move(rec.snap), std::move(rec.entries)});
      }()) {}

void ShardReplica::tick() {
  raft_.tick();
  if (auto_pump_) pump();
}

void ShardReplica::receive(Message m) {
  raft_.step(std::move(m));
  if (auto_pump_) pump();
}

void ShardReplica::propose(const Command& c, Callback cb) {
  const auto r = raft_.propose(c.encode());
  if (!r) {
    ClientResult res;
    res.status = Status::NotLeader;
    res.leader_hint = raft_.leader();
    cb(res);
    return;
  }
  pending_[r->first] = PendingWrite{r->second, std::move(cb)};
  if (auto_pump_) pump();
}

void ShardReplica::read(const std::string& key, Callback cb) {
  const std::uint64_t ctx = next_ctx_++;
  if (!raft_.read_index(ctx)) {
    ClientResult res;
    res.status = Status::NotLeader;
    res.leader_hint = raft_.leader();
    cb(res);
    return;
  }
  reads_[ctx] = PendingRead{key, std::move(cb)};
  if (auto_pump_) pump();
}

std::vector<Envelope> ShardReplica::drain_outbox() {
  std::vector<Envelope> out;
  out.swap(outbox_);
  return out;
}

void ShardReplica::pump() {
  while (raft_.has_ready()) {
    Ready rd = raft_.ready();
    storage_.save(rd);  // durable BEFORE any message of this Ready leaves the process
    for (Message& m : rd.messages) outbox_.push_back(Envelope{shard_, std::move(m)});
    if (rd.snapshot) {
      sm_.restore(rd.snapshot->data);
      fail_all(Status::Retry);  // our log was replaced: outcomes of in-flight requests are unknown
    }
    for (const Entry& e : rd.committed) apply_entry(e);
    for (const ReadState& rs : rd.read_states) read_wait_.emplace(rs.index, rs.ctx);
    for (std::uint64_t ctx : rd.read_aborted) {
      auto it = reads_.find(ctx);
      if (it == reads_.end()) continue;
      ClientResult res;
      res.status = Status::NotLeader;
      res.leader_hint = raft_.leader();
      Callback cb = std::move(it->second.cb);
      reads_.erase(it);
      cb(res);
    }
    raft_.advance(rd);
    serve_reads();
    maybe_snapshot();
  }
}

void ShardReplica::apply_entry(const Entry& e) {
  if (e.type != EntryType::Command) return;
  const Command c = Command::decode(e.data);
  const ApplyResult r = sm_.apply(c);
  auto it = pending_.find(e.index);
  if (it == pending_.end()) return;
  ClientResult res;
  if (it->second.term == e.term) {
    res.status = Status::Ok;
    res.ok = r.ok;
    res.value = r.value;
    res.stale = r.stale;
  } else {
    res.status = Status::Retry;  // a different entry committed at this index: ours was overwritten
  }
  Callback cb = std::move(it->second.cb);
  pending_.erase(it);
  cb(res);
}

void ShardReplica::serve_reads() {
  while (!read_wait_.empty() && read_wait_.begin()->first <= raft_.applied_index()) {
    const std::uint64_t ctx = read_wait_.begin()->second;
    read_wait_.erase(read_wait_.begin());
    auto it = reads_.find(ctx);
    if (it == reads_.end()) continue;
    ClientResult res;
    res.value = sm_.get(it->second.key);
    Callback cb = std::move(it->second.cb);
    reads_.erase(it);
    cb(res);
  }
}

void ShardReplica::fail_all(Status s) {
  std::map<Index, PendingWrite> w;
  w.swap(pending_);
  for (auto& [idx, p] : w) {
    ClientResult res;
    res.status = s;
    p.cb(res);
  }
}

void ShardReplica::maybe_snapshot() {
  const Index applied = raft_.applied_index();
  const Index base = raft_.log().snapshot_meta().index;
  if (opt_.snapshot_threshold == 0 || applied < base + opt_.snapshot_threshold) return;
  Snapshot s;
  s.meta.index = applied;
  s.meta.term = *raft_.log().term_at(applied);
  s.data = sm_.snapshot();
  const std::vector<Entry> remaining = raft_.log().slice(applied + 1, ~std::size_t{0}, ~std::size_t{0});
  storage_.save_local_snapshot(s, raft_.hard_state(), remaining);
  raft_.compact(std::move(s));
}

}  // namespace raftkv
