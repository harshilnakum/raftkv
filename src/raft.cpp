#include "raftkv/raft.hpp"

#include <algorithm>
#include <cassert>

namespace raftkv {

RaftNode::RaftNode(Config cfg, RestoreState rs) : cfg_(std::move(cfg)), rng_(cfg_.seed * 0x9E3779B97F4A7C15ull + cfg_.id) {
  assert(std::find(cfg_.peers.begin(), cfg_.peers.end(), cfg_.id) != cfg_.peers.end());
  assert(cfg_.heartbeat_ticks < cfg_.election_ticks);
  term_ = rs.hs.term;
  vote_ = rs.hs.vote;
  if (rs.snap.meta.index > 0) {
    log_.restore(rs.snap.meta);
    latest_snapshot_ = std::move(rs.snap);
  }
  log_.merge(rs.entries);
  commit_ = log_.snapshot_meta().index;  // the commit index is not persisted; the leader re-teaches it
  applied_ = commit_;
  stable_ = log_.last_index();
  for (NodeId p : cfg_.peers) prs_[p] = Progress{};
  reset_election_timer();
}

std::size_t RaftNode::quorum() const {
  const std::size_t n = cfg_.peers.size();
  if (cfg_.bug == Bug::SmallQuorum) return std::max<std::size_t>(1, n / 2);
  return n / 2 + 1;
}

// ---------------------------------------------------------------------------------------------------------
// Roles and timers
// ---------------------------------------------------------------------------------------------------------

void RaftNode::reset_election_timer() {
  election_elapsed_ = 0;
  randomized_timeout_ = cfg_.election_ticks + static_cast<std::uint32_t>(rng_.below(cfg_.election_ticks));
}

void RaftNode::become_follower(Term term, NodeId leader) {
  if (term != term_) {
    term_ = term;
    vote_ = kNone;
    hs_dirty_ = true;
  }
  if (role_ == Role::Leader) {
    for (const PendingRead& r : reads_) read_aborted_.push_back(r.ctx);
    reads_.clear();
    need_read_round_ = false;
  }
  role_ = Role::Follower;
  leader_ = leader;
  need_bcast_ = false;
  need_read_round_ = false;
  votes_.clear();
  reset_election_timer();
}

void RaftNode::become_candidate() {
  role_ = Role::Candidate;
  ++term_;
  vote_ = cfg_.id;
  hs_dirty_ = true;
  leader_ = kNone;
  votes_.clear();
  votes_[cfg_.id] = true;
  reset_election_timer();
}

void RaftNode::become_leader() {
  role_ = Role::Leader;
  leader_ = cfg_.id;
  heartbeat_elapsed_ = 0;
  election_elapsed_ = 0;
  for (auto& [id, pr] : prs_) {
    pr = Progress{};
    pr.next = log_.last_index() + 1;
    pr.recent_active = true;
  }
  prs_[cfg_.id].match = stable_;
  reads_.clear();
  // A new leader may only commit entries of its own term directly, so it appends a no-op to commit the older ones.
  log_.append_new(Entry{term_, 0, EntryType::Noop, {}});
  need_bcast_ = true;
}

void RaftNode::campaign() {
  become_candidate();
  if (is_single()) {  // a group of one wins immediately
    become_leader();
    return;
  }
  for (NodeId p : cfg_.peers) {
    if (p == cfg_.id) continue;
    Message m;
    m.type = MsgType::RequestVote;
    m.to = p;
    m.log_index = log_.last_index();
    m.log_term = log_.last_term();
    send(std::move(m));
  }
}

void RaftNode::tick() {
  if (role_ == Role::Leader) {
    tick_leader();
  } else {
    tick_election();
  }
}

void RaftNode::tick_election() {
  if (++election_elapsed_ >= randomized_timeout_) campaign();
}

bool RaftNode::quorum_active() {
  std::size_t active = 0;
  for (auto& [id, pr] : prs_) {
    if (id == cfg_.id || pr.recent_active) ++active;
    if (id != cfg_.id) pr.recent_active = false;
  }
  return active >= quorum();
}

void RaftNode::tick_leader() {
  ++heartbeat_elapsed_;
  ++election_elapsed_;
  if (election_elapsed_ >= cfg_.election_ticks) {
    election_elapsed_ = 0;
    if (cfg_.check_quorum && !quorum_active()) {
      become_follower(term_, kNone);
      return;
    }
  }
  if (heartbeat_elapsed_ >= cfg_.heartbeat_ticks) {
    heartbeat_elapsed_ = 0;
    broadcast_heartbeat();
  }
}

// ---------------------------------------------------------------------------------------------------------
// Message handling
// ---------------------------------------------------------------------------------------------------------

void RaftNode::send(Message m) {
  m.from = cfg_.id;
  m.term = term_;
  msgs_.push_back(std::move(m));
}

void RaftNode::step(Message m) {
  if (m.to != cfg_.id) return;
  if (m.term > term_) {
    const bool from_leader = m.type == MsgType::Append || m.type == MsgType::InstallSnapshot;
    become_follower(m.term, from_leader ? m.from : kNone);
  } else if (m.term < term_) {
    // A stale peer: tell it about our term so it steps down.
    if (m.type == MsgType::Append || m.type == MsgType::InstallSnapshot) {
      Message r;
      r.type = MsgType::AppendResp;
      r.to = m.from;
      r.reject = true;
      r.log_index = m.log_index;
      send(std::move(r));
    } else if (m.type == MsgType::RequestVote) {
      Message r;
      r.type = MsgType::RequestVoteResp;
      r.to = m.from;
      r.reject = true;
      send(std::move(r));
    }
    return;
  }

  switch (m.type) {
    case MsgType::RequestVote:
      handle_request_vote(m);
      break;
    case MsgType::RequestVoteResp:
      if (role_ == Role::Candidate) handle_vote_resp(m);
      break;
    case MsgType::Append:
      if (role_ == Role::Candidate) become_follower(term_, m.from);
      if (role_ == Role::Follower) handle_append(m);
      break;
    case MsgType::InstallSnapshot:
      if (role_ == Role::Candidate) become_follower(term_, m.from);
      if (role_ == Role::Follower) handle_snapshot(m);
      break;
    case MsgType::AppendResp:
      if (role_ == Role::Leader) handle_append_resp(m);
      break;
  }
}

void RaftNode::handle_request_vote(const Message& m) {
  Message r;
  r.type = MsgType::RequestVoteResp;
  r.to = m.from;
  const bool can_vote = vote_ == kNone || vote_ == m.from;
  const bool up_to_date = cfg_.bug == Bug::VoteIgnoresLog || log_.up_to_date(m.log_index, m.log_term);
  if (can_vote && up_to_date) {
    vote_ = m.from;
    hs_dirty_ = true;
    reset_election_timer();
    r.reject = false;
  } else {
    r.reject = true;
  }
  send(std::move(r));
}

void RaftNode::handle_vote_resp(const Message& m) {
  votes_[m.from] = !m.reject;
  std::size_t granted = 0, rejected = 0;
  for (auto& [id, g] : votes_) (g ? granted : rejected)++;
  if (granted >= quorum()) {
    become_leader();
  } else if (rejected >= quorum()) {
    become_follower(term_, kNone);
  }
}

void RaftNode::handle_append(const Message& m) {
  reset_election_timer();
  leader_ = m.from;
  Message r;
  r.type = MsgType::AppendResp;
  r.to = m.from;
  r.log_index = m.log_index;
  r.read_seq = m.read_seq;

  if (m.log_index < commit_) {  // stale or duplicate: everything up to commit_ is already agreed on
    r.match_index = commit_;
    send(std::move(r));
    return;
  }
  if (!log_.match_term(m.log_index, m.log_term)) {
    r.reject = true;
    if (m.log_index > log_.last_index()) {
      r.hint_index = log_.last_index() + 1;
      r.hint_term = 0;
    } else {
      r.hint_term = *log_.term_at(m.log_index);
      Index i = m.log_index;
      while (i > log_.first_index() && log_.term_at(i - 1) == r.hint_term) --i;
      r.hint_index = i;
    }
    send(std::move(r));
    return;
  }
  const Index changed = log_.merge(m.entries);
  if (changed != 0) stable_ = std::min(stable_, changed - 1);
  const Index last_new = m.log_index + m.entries.size();
  if (m.commit > commit_) commit_ = std::min(m.commit, last_new);
  r.match_index = last_new;
  send(std::move(r));
}

void RaftNode::handle_snapshot(const Message& m) {
  reset_election_timer();
  leader_ = m.from;
  Message r;
  r.type = MsgType::AppendResp;
  r.to = m.from;
  r.log_index = m.snapshot.meta.index;
  const SnapshotMeta& meta = m.snapshot.meta;
  if (meta.index <= commit_) {
    r.match_index = commit_;
    send(std::move(r));
    return;
  }
  if (log_.match_term(meta.index, meta.term)) {  // we already hold that entry: just learn that it is committed
    commit_ = std::max(commit_, meta.index);
    r.match_index = meta.index;
    send(std::move(r));
    return;
  }
  log_.restore(meta);
  pending_snapshot_ = m.snapshot;
  latest_snapshot_ = m.snapshot;
  commit_ = meta.index;
  stable_ = meta.index;
  r.match_index = meta.index;
  send(std::move(r));
}

void RaftNode::handle_append_resp(const Message& m) {
  auto it = prs_.find(m.from);
  if (it == prs_.end()) return;
  Progress& pr = it->second;
  pr.recent_active = true;
  pr.acked_read_seq = std::max(pr.acked_read_seq, m.read_seq);
  pr.paused = false;
  update_reads();

  if (m.reject) {
    if (pr.state == PState::Snapshot) return;
    Index next = m.hint_index;
    if (m.hint_term != 0) {
      const Index li = log_.last_index_of_term(m.hint_term);
      next = li != 0 ? li + 1 : m.hint_index;
    }
    next = std::min(next, m.log_index);
    next = std::max<Index>(next, 1);
    next = std::max(next, pr.match + 1);
    pr.next = next;
    pr.state = PState::Probe;
    send_append(m.from, true);
    return;
  }

  // A peer cannot hold a matching prefix longer than our own log. This only happens if safety was already violated
  // elsewhere (e.g. a leader elected without all committed entries); ignore the nonsensical ack instead of crashing.
  if (m.match_index > log_.last_index()) return;
  if (pr.state == PState::Snapshot) {
    if (m.match_index >= pr.pending_snapshot) {
      pr.match = std::max(pr.match, m.match_index);
      pr.next = pr.match + 1;
      pr.state = PState::Probe;
      pr.snapshot_wait = 0;
    }
  } else if (m.match_index > pr.match) {
    pr.match = m.match_index;
    pr.next = std::max(pr.next, pr.match + 1);
    if (pr.state == PState::Probe) {
      pr.state = PState::Replicate;
      pr.next = pr.match + 1;
    }
  }
  if (maybe_commit()) need_bcast_ = true;
  if (pr.next <= log_.last_index() || pr.state == PState::Probe) send_append(m.from, false);
}

// ---------------------------------------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------------------------------------

void RaftNode::send_append(NodeId to, bool force) {
  Progress& pr = prs_[to];
  if (pr.state == PState::Snapshot) return;
  if (pr.state == PState::Probe && pr.paused && !force) return;
  if (pr.state == PState::Replicate && pr.next > pr.match + cfg_.max_inflight_entries) return;

  const Index prev = pr.next - 1;
  const auto prev_term = log_.term_at(prev);
  if (!prev_term) {  // the entries this follower needs were compacted away: send a snapshot instead
    if (latest_snapshot_.meta.index == 0) return;  // nothing to send (inconsistent progress; cannot happen in correct runs)
    Message m;
    m.type = MsgType::InstallSnapshot;
    m.to = to;
    m.snapshot = latest_snapshot_;
    pr.state = PState::Snapshot;
    pr.pending_snapshot = latest_snapshot_.meta.index;
    pr.snapshot_wait = 0;
    send(std::move(m));
    return;
  }
  std::vector<Entry> ents = log_.slice(pr.next, cfg_.max_entries_per_msg, cfg_.max_bytes_per_msg);
  if (ents.empty() && !force) return;
  Message m;
  m.type = MsgType::Append;
  m.to = to;
  m.log_index = prev;
  m.log_term = *prev_term;
  m.commit = commit_;
  m.read_seq = read_seq_;
  const std::size_t n = ents.size();
  m.entries = std::move(ents);
  if (n != 0) {
    if (pr.state == PState::Replicate) {
      pr.next += n;
    } else {
      pr.paused = true;
    }
  }
  send(std::move(m));
}

void RaftNode::broadcast_append() {
  for (NodeId p : cfg_.peers)
    if (p != cfg_.id) send_append(p, false);
}

void RaftNode::broadcast_heartbeat() {
  for (NodeId p : cfg_.peers) {
    if (p == cfg_.id) continue;
    Progress& pr = prs_[p];
    if (pr.state == PState::Snapshot && ++pr.snapshot_wait > 3 * cfg_.election_ticks) {
      pr.state = PState::Probe;  // the snapshot (or its ack) was probably lost: send it again
      pr.next = pr.match + 1;
      pr.snapshot_wait = 0;
    }
    if (pr.state == PState::Probe) pr.paused = false;
    // Pipelined (Replicate) entries that were lost never get a rejection if nothing newer is sent. If a follower
    // that is behind has made no progress for several heartbeats, fall back to probing so the entries are resent.
    bool resend = false;
    if (pr.state == PState::Replicate && pr.match < log_.last_index() && pr.match == pr.hb_match) {
      const std::uint32_t limit = std::max<std::uint32_t>(2, cfg_.election_ticks / std::max<std::uint32_t>(1, cfg_.heartbeat_ticks) / 2);
      if (++pr.stalled_hbs >= limit) {
        pr.state = PState::Probe;
        pr.next = pr.match + 1;
        pr.paused = false;
        pr.stalled_hbs = 0;
        resend = true;
      }
    } else {
      pr.stalled_hbs = 0;
    }
    pr.hb_match = pr.match;
    Index prev = pr.match;
    auto prev_term = log_.term_at(prev);
    if (!prev_term) {
      prev = log_.snapshot_meta().index;
      prev_term = log_.term_at(prev);
    }
    Message m;
    m.type = MsgType::Append;
    m.to = p;
    m.log_index = prev;
    m.log_term = *prev_term;
    m.commit = std::min(commit_, prev);
    m.read_seq = read_seq_;
    send(std::move(m));
    if (resend) send_append(p, false);
  }
}

bool RaftNode::commit_in_term() const {
  const auto t = log_.term_at(commit_);
  return t && *t == term_;
}

bool RaftNode::maybe_commit() {
  std::vector<Index> matches;
  matches.reserve(prs_.size());
  for (auto& [id, pr] : prs_) matches.push_back(id == cfg_.id ? stable_ : pr.match);
  std::sort(matches.begin(), matches.end(), std::greater<>());
  const std::size_t q = quorum();
  if (matches.size() < q || q == 0) return false;
  const Index n = matches[q - 1];
  if (n <= commit_) return false;
  if (cfg_.bug != Bug::CommitOldTerm) {
    const auto t = log_.term_at(n);
    if (!t || *t != term_) return false;  // only entries of the current term are committed by counting replicas
  }
  commit_ = n;
  return true;
}

// ---------------------------------------------------------------------------------------------------------
// Client-facing operations
// ---------------------------------------------------------------------------------------------------------

std::optional<std::pair<Index, Term>> RaftNode::propose(std::string data) {
  if (role_ != Role::Leader) return std::nullopt;
  const Index idx = log_.append_new(Entry{term_, 0, EntryType::Command, std::move(data)});
  need_bcast_ = true;
  return std::make_pair(idx, term_);
}

bool RaftNode::read_index(std::uint64_t ctx) {
  if (role_ != Role::Leader) return false;
  if (cfg_.bug != Bug::ReadBeforeTermCommit && !commit_in_term()) return false;
  if (is_single() || cfg_.bug == Bug::StaleRead) {
    read_states_.push_back({ctx, commit_});
    return true;
  }
  reads_.push_back({ctx, commit_, read_seq_ + 1});
  need_read_round_ = true;
  return true;
}

void RaftNode::update_reads() {
  while (!reads_.empty()) {
    std::size_t acks = 1;  // ourselves
    for (auto& [id, pr] : prs_)
      if (id != cfg_.id && pr.acked_read_seq >= reads_.front().seq) ++acks;
    if (acks < quorum()) break;
    read_states_.push_back({reads_.front().ctx, reads_.front().index});
    reads_.pop_front();
  }
}

// ---------------------------------------------------------------------------------------------------------
// Ready / advance / compact
// ---------------------------------------------------------------------------------------------------------

bool RaftNode::has_ready() const {
  if (hs_dirty_ || !msgs_.empty() || !read_states_.empty() || !read_aborted_.empty() || pending_snapshot_ ||
      need_bcast_ || need_read_round_)
    return true;
  if (stable_ < log_.last_index()) return true;
  return applied_ < std::min(commit_, stable_);
}

Ready RaftNode::ready() {
  if (role_ == Role::Leader) {
    if (need_bcast_) broadcast_append();
    if (need_read_round_) {
      ++read_seq_;
      broadcast_heartbeat();
    }
  }
  need_bcast_ = false;  // also cleared when we are no longer leader, otherwise has_ready() would stay true forever
  need_read_round_ = false;
  Ready rd;
  if (hs_dirty_) rd.hard_state = HardState{term_, cfg_.bug == Bug::ForgetVote ? kNone : vote_};  // bug: the vote is never made durable
  if (pending_snapshot_) rd.snapshot = *pending_snapshot_;
  if (stable_ < log_.last_index()) {
    const Index from = std::max(stable_ + 1, log_.first_index());
    rd.entries = log_.slice(from, ~std::size_t{0}, ~std::size_t{0});
  }
  rd.messages = std::move(msgs_);
  msgs_.clear();
  rd.read_states = std::move(read_states_);
  read_states_.clear();
  rd.read_aborted = std::move(read_aborted_);
  read_aborted_.clear();
  if (!pending_snapshot_) {
    const Index upto = std::min(commit_, stable_);
    // Entries persisted by this very Ready are not yet durable, so only entries <= stable_ are handed out.
    if (applied_ < upto) rd.committed = log_.slice(applied_ + 1, upto - applied_, ~std::size_t{0});
  }
  return rd;
}

void RaftNode::advance(const Ready& rd) {
  if (rd.hard_state) hs_dirty_ = false;
  if (rd.snapshot) {
    pending_snapshot_.reset();
    stable_ = rd.snapshot->meta.index;
    applied_ = rd.snapshot->meta.index;
  }
  if (!rd.entries.empty()) stable_ = rd.entries.back().index;
  if (!rd.committed.empty()) applied_ = rd.committed.back().index;
  if (role_ == Role::Leader) {
    prs_[cfg_.id].match = stable_;
    if (maybe_commit()) need_bcast_ = true;
    if (is_single() && commit_ < stable_) {  // single node: durable == replicated
      commit_ = stable_;
    }
  }
}

void RaftNode::compact(Snapshot snap) {
  assert(snap.meta.index <= applied_);
  if (snap.meta.index <= log_.snapshot_meta().index) return;
  log_.compact(snap.meta.index, snap.meta.term);
  latest_snapshot_ = std::move(snap);
}

}  // namespace raftkv
