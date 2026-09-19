#include "raftkv/sim.hpp"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <set>

#include "raftkv/replica.hpp"
#include "raftkv/rng.hpp"
#include "raftkv/sim_fs.hpp"

namespace raftkv::sim {

namespace {

constexpr std::uint64_t kMs = 1000;  // virtual time is in microseconds

std::uint64_t mix(std::uint64_t a, std::uint64_t b) {
  std::uint64_t z = a + 0x9E3779B97F4A7C15ull * (b + 1);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

struct Request {
  std::size_t slot = 0;
  std::uint64_t pid = 0;
  std::uint64_t seq = 0;
  std::uint64_t attempt = 0;
  lin::Kind kind = lin::Kind::Get;
  ShardId shard = 0;
  std::string key, arg, expected;
};

struct Node {
  NodeId id = 0;
  SimFs fs;
  bool up = false;
  std::uint64_t epoch = 0;
  std::uint64_t frozen_until = 0;
  std::uint64_t tick_us = 0;
  std::uint32_t incarnation = 0;
  std::vector<std::unique_ptr<ShardReplica>> reps;
};

struct Client {
  std::uint64_t pid = 0;
  std::uint64_t seq = 0;
  bool busy = false;
  bool probe = false;
  int probe_ops_left = 0;
  std::uint64_t started = 0;
  std::uint64_t attempt = 0;
  lin::Op op;
  ShardId shard = 0;
  std::map<std::string, std::string> seen;
  std::vector<NodeId> hint;  // per shard: last node that answered as leader
};

class World {
 public:
  explicit World(const SimConfig& c) : cfg_(c), rng_(mix(c.seed, 0xC1057E4)) {
    end_ = cfg_.chaos_us + cfg_.settle_us;
    drop_ = cfg_.faults ? 0.01 : 0.0;
    dup_ = cfg_.faults ? 0.01 : 0.0;
    min_delay_ = 200;
    max_delay_ = 3 * kMs;
    blocked_.assign(cfg_.nodes, std::vector<char>(cfg_.nodes, 0));
  }

  SimResult run() {
    std::vector<NodeId> ids;
    for (std::uint32_t i = 1; i <= cfg_.nodes; ++i) ids.push_back(i);
    ids_ = ids;
    for (NodeId id : ids_) {
      auto n = std::make_unique<Node>();
      n->id = id;
      n->tick_us = cfg_.tick_us;
      n->fs.crash_on_sync = [this] { return cfg_.faults && !in_recovery_ && now_ < cfg_.chaos_us && rng_.chance(sync_crash_prob_); };
      nodes_.push_back(std::move(n));
    }
    for (NodeId id : ids_) start_node(node(id));

    clients_.resize(cfg_.clients);
    for (std::size_t i = 0; i < clients_.size(); ++i) {
      clients_[i].pid = ++next_pid_;
      clients_[i].hint.assign(cfg_.shards, kNone);
      at(rng_.below(5 * kMs), [this, i] { client_next(i); });
    }
    if (cfg_.faults) at(150 * kMs + rng_.below(300 * kMs), [this] { nemesis(); });
    at(cfg_.chaos_us, [this] { end_chaos(); });
    at(100 * kMs, [this] { periodic_checks(); });

    while (!events_.empty() && failure_.empty()) {
      Event e = std::move(const_cast<Event&>(events_.top()));
      events_.pop();
      if (e.at > end_) break;
      now_ = e.at;
      e.fn();
      ++stats_.events;
    }
    now_ = std::min(now_, end_);
    if (failure_.empty()) final_checks();

    SimResult res;
    res.stats = stats_;
    res.ok = failure_.empty();
    res.failure = failure_;
    res.lin_unknown = lin_unknown_;
    res.history = history_;
    res.history_digest = digest_history();
    return res;
  }

 private:
  struct Event {
    std::uint64_t at;
    std::uint64_t seq;
    std::function<void()> fn;
  };
  struct Later {
    bool operator()(const Event& a, const Event& b) const { return a.at != b.at ? a.at > b.at : a.seq > b.seq; }
  };

  void at(std::uint64_t t, std::function<void()> fn) { events_.push(Event{t, seq_++, std::move(fn)}); }
  Node& node(NodeId id) { return *nodes_[id - 1]; }

  void fail(const std::string& what) {
    if (failure_.empty())
      failure_ = "seed " + std::to_string(cfg_.seed) + " at t=" + std::to_string(now_ / kMs) + "ms: " + what;
  }

  // ----------------------------------------------------------------------------------------------------------
  // Nodes
  // ----------------------------------------------------------------------------------------------------------

  void start_node(Node& n) {
    n.reps.clear();
    in_recovery_ = true;  // no injected power loss while a node is recovering (its own fsyncs are not the interesting case)
    for (ShardId s = 0; s < cfg_.shards; ++s) {
      Config rc;
      rc.id = n.id;
      rc.peers = ids_;
      rc.election_ticks = cfg_.election_ticks;
      rc.heartbeat_ticks = cfg_.heartbeat_ticks;
      rc.seed = mix(mix(cfg_.seed, n.id * 1000 + s), n.incarnation);
      rc.bug = cfg_.bug;
      rc.check_quorum = cfg_.check_quorum;
      ReplicaOptions opt;
      opt.snapshot_threshold = cfg_.snapshot_threshold;
      try {
        n.reps.push_back(std::make_unique<ShardReplica>(s, rc, n.fs, "s" + std::to_string(s), opt));
      } catch (const std::exception& e) {
        in_recovery_ = false;
        fail("recovery failed on node " + std::to_string(n.id) + " shard " + std::to_string(s) + ": " + e.what());
        return;
      }
      if (n.reps.back()->repaired_bytes() > 0) ++stats_.torn_wal_repairs;
    }
    in_recovery_ = false;
    n.up = true;
    ++n.epoch;
    ++n.incarnation;
    schedule_tick(n.id, n.epoch);
  }

  void schedule_tick(NodeId id, std::uint64_t epoch) {
    at(now_ + node(id).tick_us, [this, id, epoch] {
      Node& n = node(id);
      if (!n.up || n.epoch != epoch) return;
      if (now_ >= n.frozen_until) {  // a paused process does not accumulate ticks: they are simply missed
        on_node(id, [this, id] {
          for (auto& r : node(id).reps) r->tick();
        });
      }
      if (node(id).up && node(id).epoch == epoch) schedule_tick(id, epoch);
    });
  }

  // Runs `fn` on the node if it is up, delaying it while the node is frozen (a GC pause / stalled VM).
  void on_node(NodeId id, std::function<void()> fn) {
    Node& n = node(id);
    if (!n.up) return;
    if (now_ < n.frozen_until) {
      at(n.frozen_until, [this, id, fn] { on_node(id, fn); });
      return;
    }
    try {
      fn();
    } catch (const SimCrash&) {  // power lost in the middle of an fsync
      ++stats_.sync_crashes;
      crash_node(id, 50 * kMs + rng_.below(1200 * kMs));
      return;
    }
    after_node_event(node(id));
  }

  void after_node_event(Node& n) {
    for (auto& rep : n.reps) {
      for (Envelope& env : rep->drain_outbox()) send_raft(n.id, std::move(env));
      const RaftNode& r = rep->raft();
      stats_.max_term = std::max<std::uint64_t>(stats_.max_term, r.term());
      const HardState hs = r.hard_state();
      auto& last = last_vote_[{n.id, rep->shard()}];
      if (cfg_.faults && now_ < cfg_.chaos_us && hs.vote != kNone && hs.vote != n.id && (hs.term != last.first || hs.vote != last.second)) {
        last = {hs.term, hs.vote};
        if (rng_.chance(0.04)) {  // power loss right after casting a vote (the vote reply is already on the wire)
          const NodeId victim = n.id;
          at(now_, [this, victim] { crash_node(victim, 20 * kMs + rng_.below(300 * kMs)); });
        }
      } else if (hs.term != last.first) {
        last = {hs.term, hs.vote};
      }
      if (r.role() == Role::Leader) {  // election safety: at most one leader per (shard, term), ever
        auto [it, fresh] = leader_of_.emplace(std::make_pair(rep->shard(), r.term()), n.id);
        if (!fresh && it->second != n.id)
          fail("two leaders in shard " + std::to_string(rep->shard()) + " term " + std::to_string(r.term()) + ": nodes " +
               std::to_string(it->second) + " and " + std::to_string(n.id));
      }
    }
  }

  void crash_node(NodeId id, std::uint64_t down_us) {
    Node& n = node(id);
    if (!n.up) return;
    ++stats_.crashes;
    for (ShardId sh = 0; sh < cfg_.shards; ++sh) last_vote_.erase({id, sh});
    n.up = false;
    ++n.epoch;
    n.reps.clear();
    n.fs.crash(rng_, 0.5, 0.3);  // power loss: unsynced bytes vanish or survive partly, sometimes damaged
    at(now_ + down_us, [this, id] {
      if (!node(id).up && now_ < end_) start_node(node(id));
    });
  }

  // ----------------------------------------------------------------------------------------------------------
  // Network
  // ----------------------------------------------------------------------------------------------------------

  std::uint64_t delay() { return min_delay_ + rng_.below(max_delay_ - min_delay_ + 1); }

  void send_raft(NodeId from, Envelope env) {
    ++stats_.msgs_sent;
    const NodeId to = env.msg.to;
    if (rng_.chance(drop_)) {
      ++stats_.msgs_dropped;
      return;
    }
    const int copies = rng_.chance(dup_) ? 2 : 1;
    if (copies == 2) ++stats_.msgs_duplicated;
    for (int c = 0; c < copies; ++c) {
      at(now_ + delay(), [this, from, to, env] {
        if (blocked_[from - 1][to - 1]) {
          ++stats_.msgs_dropped;
          return;
        }
        on_node(to, [this, to, env] { node(to).reps[env.shard]->receive(env.msg); });
      });
    }
  }

  void send_client_req(NodeId target, const Request& req) {
    ++stats_.msgs_sent;
    if (rng_.chance(drop_)) {
      ++stats_.msgs_dropped;
      return;
    }
    const int copies = rng_.chance(dup_) ? 2 : 1;
    for (int c = 0; c < copies; ++c) {
      at(now_ + delay(), [this, target, req] { on_node(target, [this, target, req] { handle_request(target, req); }); });
    }
  }

  void send_client_resp(NodeId from, const Request& req, const ClientResult& res) {
    ++stats_.msgs_sent;
    if (rng_.chance(drop_)) {
      ++stats_.msgs_dropped;
      return;
    }
    const int copies = rng_.chance(dup_) ? 2 : 1;
    for (int c = 0; c < copies; ++c) {
      at(now_ + delay(), [this, from, req, res] { client_on_resp(from, req, res); });
    }
  }

  void handle_request(NodeId id, const Request& req) {
    ShardReplica& rep = *node(id).reps[req.shard];
    auto cb = [this, id, req](const ClientResult& r) { send_client_resp(id, req, r); };
    if (req.kind == lin::Kind::Get) {
      rep.read(req.key, cb);
      return;
    }
    Command c;
    c.op = req.kind == lin::Kind::Put ? OpType::Put : (req.kind == lin::Kind::Append ? OpType::Append : OpType::Cas);
    c.client = req.pid;
    c.seq = req.seq;
    c.key = req.key;
    c.value = req.arg;
    c.expected = req.expected;
    rep.propose(c, cb);
  }

  // ----------------------------------------------------------------------------------------------------------
  // Clients
  // ----------------------------------------------------------------------------------------------------------

  void client_next(std::size_t slot) {
    Client& c = clients_[slot];
    if (c.probe) {
      if (c.probe_ops_left == 0) return;
    } else if (now_ >= cfg_.chaos_us) {
      return;
    }
    ++c.seq;
    lin::Op op;
    op.process = c.pid;
    std::string key;
    if (c.probe) {
      key = probe_key_[slot % probe_key_.size()];
      op.kind = c.probe_ops_left == 2 ? lin::Kind::Put : lin::Kind::Get;
    } else {
      key = "k" + std::to_string(rng_.below(cfg_.keys));
      const std::uint64_t r = rng_.below(100);
      op.kind = r < 30 ? lin::Kind::Get : r < 55 ? lin::Kind::Put : r < 78 ? lin::Kind::Append : lin::Kind::Cas;
    }
    op.key = key;
    const std::string uniq = "p" + std::to_string(c.pid) + "." + std::to_string(c.seq);
    if (op.kind == lin::Kind::Put) op.arg = uniq;
    if (op.kind == lin::Kind::Append) op.arg = uniq + ";";
    if (op.kind == lin::Kind::Cas) {
      op.arg = uniq;
      const auto it = c.seen.find(key);
      op.expected = (it != c.seen.end() && rng_.below(100) < 65) ? it->second : (rng_.coin() ? std::string() : "nope");
    }
    op.call = now_;
    c.op = op;
    c.shard = shard_of(key, cfg_.shards);
    c.started = now_;
    c.attempt = 0;
    c.busy = true;
    client_send(slot);
  }

  void client_send(std::size_t slot) {
    Client& c = clients_[slot];
    ++c.attempt;
    NodeId target = c.hint[c.shard];
    if (target == kNone || rng_.below(100) < 20) target = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
    Request req;
    req.slot = slot;
    req.pid = c.pid;
    req.seq = c.seq;
    req.attempt = c.attempt;
    req.kind = c.op.kind;
    req.shard = c.shard;
    req.key = c.op.key;
    req.arg = c.op.arg;
    req.expected = c.op.expected;
    send_client_req(target, req);
    const std::uint64_t pid = c.pid, seq = c.seq, attempt = c.attempt;
    at(now_ + cfg_.attempt_timeout_us, [this, slot, pid, seq, attempt] {
      Client& cl = clients_[slot];
      if (cl.busy && cl.pid == pid && cl.seq == seq && cl.attempt == attempt) client_retry(slot);
    });
  }

  void client_retry(std::size_t slot) {
    Client& c = clients_[slot];
    const std::uint64_t deadline = c.probe ? cfg_.settle_us : cfg_.op_deadline_us;
    if (now_ - c.started > deadline) {
      give_up(slot);
      return;
    }
    client_send(slot);
  }

  // The client stops waiting: the operation may or may not have taken effect. Like a Jepsen process, this client
  // continues under a NEW process id so that its old (still possibly in-flight) request cannot be confused with new ones.
  void give_up(std::size_t slot) {
    Client& c = clients_[slot];
    c.op.ret = lin::kNever;
    history_.push_back(c.op);
    ++stats_.ops_info;
    c.busy = false;
    c.pid = ++next_pid_;
    c.seq = 0;
    if (c.probe) return;  // a probe that gives up is reported as a liveness failure at the end
    at(now_ + 1 * kMs + rng_.below(3 * kMs), [this, slot] { client_next(slot); });
  }

  void client_on_resp(NodeId from, const Request& req, const ClientResult& res) {
    Client& c = clients_[req.slot];
    if (!c.busy || c.pid != req.pid || c.seq != req.seq) return;  // late or duplicate response
    if (res.status == Status::Ok) {
      if (res.stale) return;
      c.hint[c.shard] = from;
      complete(req.slot, res);
      return;
    }
    c.hint[c.shard] = res.leader_hint;  // may be kNone: then the next attempt picks a random node
    if (req.attempt == c.attempt) {
      const std::uint64_t pid = c.pid, seq = c.seq, attempt = c.attempt, slot = req.slot;
      at(now_ + 2 * kMs + rng_.below(8 * kMs), [this, slot, pid, seq, attempt] {
        Client& cl = clients_[slot];
        if (cl.busy && cl.pid == pid && cl.seq == seq && cl.attempt == attempt) client_retry(slot);
      });
    }
  }

  void complete(std::size_t slot, const ClientResult& res) {
    Client& c = clients_[slot];
    lin::Op op = c.op;
    op.ret = now_;
    op.ok = res.ok;
    op.value = res.value;
    if (op.kind == lin::Kind::Put) op.value = op.arg;
    history_.push_back(op);
    ++stats_.ops_ok;
    switch (op.kind) {
      case lin::Kind::Get: ++stats_.gets; c.seen[op.key] = op.value; break;
      case lin::Kind::Put: ++stats_.puts; c.seen[op.key] = op.arg; break;
      case lin::Kind::Append: ++stats_.appends; c.seen[op.key] = op.value; break;
      case lin::Kind::Cas: ++stats_.cases; c.seen[op.key] = op.ok ? op.arg : op.value; break;
    }
    c.busy = false;
    if (c.probe) {
      --c.probe_ops_left;
      if (c.probe_ops_left == 0) ++probes_done_;
      else at(now_ + 1 * kMs, [this, slot] { client_next(slot); });
      return;
    }
    at(now_ + rng_.below(3 * kMs), [this, slot] { client_next(slot); });
  }

  // ----------------------------------------------------------------------------------------------------------
  // Nemesis
  // ----------------------------------------------------------------------------------------------------------

  void heal_network() {
    for (auto& row : blocked_) std::fill(row.begin(), row.end(), 0);
    ++part_gen_;
  }

  void partition_for(std::uint64_t dur) {
    ++stats_.partitions;
    const std::uint64_t gen = ++part_gen_;
    for (auto& row : blocked_) std::fill(row.begin(), row.end(), 0);
    const std::uint64_t kind = rng_.below(4);
    if (kind == 0 || cfg_.nodes < 3) {  // isolate one node (the leader of shard 0, if there is one)
      NodeId victim = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
      for (NodeId id : ids_)
        if (node(id).up && !node(id).reps.empty() && node(id).reps[0]->raft().role() == Role::Leader) victim = id;
      for (NodeId o : ids_)
        if (o != victim) block_both(victim, o);
    } else if (kind == 1) {  // random bipartition
      std::set<NodeId> a;
      for (NodeId id : ids_)
        if (rng_.coin()) a.insert(id);
      if (a.empty()) a.insert(1);
      if (a.size() == ids_.size()) a.erase(1);
      for (NodeId x : ids_)
        for (NodeId y : ids_)
          if (a.count(x) != a.count(y)) blocked_[x - 1][y - 1] = 1;
    } else if (kind == 2) {  // one-way: x can send to y but never hears back
      const NodeId x = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
      NodeId y = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
      if (y == x) y = static_cast<NodeId>(1 + (y % cfg_.nodes));
      blocked_[y - 1][x - 1] = 1;
    } else {  // a node that can talk to only one other node
      const NodeId x = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
      const NodeId keep = static_cast<NodeId>(1 + (x % cfg_.nodes));
      for (NodeId o : ids_)
        if (o != x && o != keep) block_both(x, o);
    }
    at(now_ + dur, [this, gen] {
      if (part_gen_ == gen) heal_network();
    });
  }
  void block_both(NodeId a, NodeId b) {
    blocked_[a - 1][b - 1] = 1;
    blocked_[b - 1][a - 1] = 1;
  }

  NodeId find_leader(ShardId shard) {
    for (NodeId id : ids_)
      if (node(id).up && node(id).reps.size() > shard && node(id).reps[shard]->raft().role() == Role::Leader) return id;
    return kNone;
  }

  void nemesis() {
    if (now_ >= cfg_.chaos_us) return;
    const std::uint64_t r = rng_.below(100);
    const NodeId victim = static_cast<NodeId>(1 + rng_.below(cfg_.nodes));
    if (r < 24) {
      crash_node(victim, 50 * kMs + rng_.below(1200 * kMs));
    } else if (r < 29) {  // total power loss
      const std::uint64_t down = 50 * kMs + rng_.below(800 * kMs);
      for (NodeId id : ids_) crash_node(id, down + rng_.below(200 * kMs));
    } else if (r < 58) {
      partition_for(200 * kMs + rng_.below(2000 * kMs));
    } else if (r < 63) {  // targeted: pause the current leader of a random shard (a long GC pause / stalled VM)
      ++stats_.freezes;
      const NodeId l = find_leader(static_cast<ShardId>(rng_.below(cfg_.shards)));
      const NodeId v = l != kNone ? l : victim;
      node(v).frozen_until = std::max(node(v).frozen_until, now_ + 250 * kMs + rng_.below(900 * kMs));
    } else if (r < 67) {  // targeted: kill the current leader of a random shard
      const NodeId l = find_leader(static_cast<ShardId>(rng_.below(cfg_.shards)));
      crash_node(l != kNone ? l : victim, 50 * kMs + rng_.below(1000 * kMs));
    } else if (r < 72) {
      ++stats_.freezes;
      node(victim).frozen_until = std::max(node(victim).frozen_until, now_ + 30 * kMs + rng_.below(700 * kMs));
    } else if (r < 82) {
      ++stats_.net_profile_changes;
      switch (rng_.below(3)) {
        case 0: drop_ = 0.01; dup_ = 0.01; min_delay_ = 200; max_delay_ = 3 * kMs; break;
        case 1: drop_ = 0.15; dup_ = 0.10; min_delay_ = 200; max_delay_ = 30 * kMs; break;
        default: drop_ = 0.03; dup_ = 0.03; min_delay_ = 5 * kMs; max_delay_ = 120 * kMs; break;
      }
    } else if (r < 92) {  // clock skew: this node's timers run 0.6x .. 1.7x as fast
      node(victim).tick_us = cfg_.tick_us * (60 + rng_.below(110)) / 100;
    } else {
      heal_network();
    }
    at(now_ + 80 * kMs + rng_.below(900 * kMs), [this] { nemesis(); });
  }

  void end_chaos() {
    heal_network();
    drop_ = cfg_.faults ? 0.005 : 0.0;
    dup_ = 0.0;
    min_delay_ = 200;
    max_delay_ = 3 * kMs;
    for (NodeId id : ids_) {
      Node& n = node(id);
      n.frozen_until = 0;
      n.tick_us = cfg_.tick_us;
      if (!n.up) start_node(n);
    }
    // Liveness probes: after healing, every shard must accept a write and serve a linearizable read.
    for (ShardId s = 0; s < cfg_.shards; ++s) {
      std::string key;
      for (int i = 0;; ++i) {
        key = "probe" + std::to_string(i);
        if (shard_of(key, cfg_.shards) == s) break;
      }
      probe_key_.push_back(key);
    }
    for (std::size_t i = 0; i < probe_key_.size(); ++i) {
      Client c;
      c.pid = ++next_pid_;
      c.probe = true;
      c.probe_ops_left = 2;
      c.hint.assign(cfg_.shards, kNone);
      clients_.push_back(std::move(c));
      const std::size_t slot = clients_.size() - 1;
      at(now_ + 500 * kMs + i * kMs, [this, slot] { client_next(slot); });
    }
    probes_expected_ = probe_key_.size();
  }

  // ----------------------------------------------------------------------------------------------------------
  // Checks
  // ----------------------------------------------------------------------------------------------------------

  void periodic_checks() {
    check_replicas(/*final=*/false);
    if (failure_.empty() && now_ + 100 * kMs <= end_) at(now_ + 100 * kMs, [this] { periodic_checks(); });
  }

  void check_replicas(bool final) {
    for (ShardId s = 0; s < cfg_.shards && failure_.empty(); ++s) {
      std::vector<const ShardReplica*> up;
      for (NodeId id : ids_)
        if (node(id).up && node(id).reps.size() > s) up.push_back(node(id).reps[s].get());
      for (std::size_t a = 0; a < up.size(); ++a)
        for (std::size_t b = a + 1; b < up.size(); ++b) {
          const ShardReplica& x = *up[a];
          const ShardReplica& y = *up[b];
          if (x.applied() == y.applied() && x.state().digest() != y.state().digest()) {
            fail("replicas of shard " + std::to_string(s) + " applied the same prefix (" + std::to_string(x.applied()) +
                 ") but hold different state");
            return;
          }
          const RaftLog& lx = x.raft().log();
          const RaftLog& ly = y.raft().log();
          const Index lo = std::max(lx.first_index(), ly.first_index());
          const Index hi = std::min(x.raft().commit_index(), y.raft().commit_index());
          const Index from = hi > 64 && hi - 64 > lo ? hi - 64 : lo;
          for (Index i = from; i <= hi && i != 0; ++i) {
            if (i > lx.last_index() || i > ly.last_index()) break;
            if (!(lx.at(i) == ly.at(i))) {
              fail("committed entry " + std::to_string(i) + " differs between two replicas of shard " + std::to_string(s));
              return;
            }
          }
        }
      if (final && up.size() == ids_.size()) {  // after healing, every replica must have converged
        const Index a0 = up[0]->applied();
        for (auto* r : up)
          if (r->applied() != a0 || r->state().digest() != up[0]->state().digest()) {
            fail("shard " + std::to_string(s) + " replicas did not converge after healing (applied " +
                 std::to_string(r->applied()) + " vs " + std::to_string(a0) + ")");
            return;
          }
      }
    }
  }

  void final_checks() {
    check_replicas(/*final=*/true);
    if (!failure_.empty()) return;
    if (cfg_.faults || true) {
      if (probes_done_ != probes_expected_) {
        fail("liveness: after healing, only " + std::to_string(probes_done_) + " of " + std::to_string(probes_expected_) +
             " shards served a write and a read within " + std::to_string(cfg_.settle_us / kMs) + "ms");
        return;
      }
    }
    // Operations still in flight when the run ended may have taken effect: they are "unknown outcome".
    for (Client& c : clients_)
      if (c.busy) {
        c.op.ret = lin::kNever;
        history_.push_back(c.op);
        ++stats_.ops_info;
      }
    const lin::Result lr = lin::check(history_, cfg_.max_lin_states);
    stats_.lin_states = lr.states_explored;
    if (lr.verdict == lin::Verdict::Violation) {
      std::string msg = "NOT LINEARIZABLE on key '" + lr.bad_key + "'. History of that key:";
      const std::size_t show = std::min<std::size_t>(lr.bad_history.size(), 40);
      for (std::size_t i = 0; i < show; ++i) msg += "\n      " + lin::describe(lr.bad_history[i]);
      if (show < lr.bad_history.size()) msg += "\n      ... (" + std::to_string(lr.bad_history.size() - show) + " more)";
      fail(msg);
    } else if (lr.verdict == lin::Verdict::Unknown) {
      lin_unknown_ = true;
    }
  }

  std::uint64_t digest_history() const {
    std::uint64_t h = 0xCBF29CE484222325ull;
    auto add = [&](std::uint64_t v) {
      for (int i = 0; i < 8; ++i) {
        h ^= (v >> (8 * i)) & 0xFF;
        h *= 0x100000001B3ull;
      }
    };
    auto adds = [&](const std::string& s) {
      for (char c : s) {
        h ^= static_cast<unsigned char>(c);
        h *= 0x100000001B3ull;
      }
      add(s.size());
    };
    for (const lin::Op& o : history_) {
      add(o.process);
      add(static_cast<std::uint64_t>(o.kind));
      adds(o.key);
      adds(o.arg);
      adds(o.expected);
      add(o.call);
      add(o.ret);
      add(o.ok ? 1 : 0);
      adds(o.value);
    }
    add(stats_.events);
    return h;
  }

  SimConfig cfg_;
  Rng rng_;
  std::uint64_t now_ = 0, seq_ = 0, end_ = 0;
  std::priority_queue<Event, std::vector<Event>, Later> events_;
  std::vector<NodeId> ids_;
  std::vector<std::unique_ptr<Node>> nodes_;
  std::vector<std::vector<char>> blocked_;
  std::uint64_t part_gen_ = 0;
  double drop_ = 0, dup_ = 0;
  std::uint64_t min_delay_ = 0, max_delay_ = 0;
  std::vector<Client> clients_;
  std::uint64_t next_pid_ = 0;
  std::vector<std::string> probe_key_;
  std::size_t probes_done_ = 0, probes_expected_ = 0;
  std::vector<lin::Op> history_;
  std::map<std::pair<ShardId, Term>, NodeId> leader_of_;
  std::map<std::pair<NodeId, ShardId>, std::pair<Term, NodeId>> last_vote_;
  SimStats stats_;
  std::string failure_;
  bool lin_unknown_ = false;
  bool in_recovery_ = false;
  double sync_crash_prob_ = 0.0004;
};

}  // namespace

SimResult run(const SimConfig& cfg) { return World(cfg).run(); }

}  // namespace raftkv::sim
