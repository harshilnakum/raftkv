#include "node_server.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <future>
#include <random>

#include "raftkv/posix_fs.hpp"

namespace raftkv {

using Clock = std::chrono::steady_clock;

namespace {

pb::RaftEnvelope to_pb(ShardId shard, const Message& m) {
  pb::RaftEnvelope e;
  e.set_shard(shard);
  e.set_type(static_cast<std::uint32_t>(m.type));
  e.set_from(m.from);
  e.set_to(m.to);
  e.set_term(m.term);
  e.set_log_index(m.log_index);
  e.set_log_term(m.log_term);
  e.set_commit(m.commit);
  e.set_reject(m.reject);
  e.set_match_index(m.match_index);
  e.set_hint_index(m.hint_index);
  e.set_hint_term(m.hint_term);
  e.set_read_seq(m.read_seq);
  for (const Entry& en : m.entries) {
    pb::Entry* pe = e.add_entries();
    pe->set_term(en.term);
    pe->set_index(en.index);
    pe->set_type(static_cast<std::uint32_t>(en.type));
    pe->set_data(en.data);
  }
  if (m.type == MsgType::InstallSnapshot) {
    e.mutable_snapshot()->set_index(m.snapshot.meta.index);
    e.mutable_snapshot()->set_term(m.snapshot.meta.term);
    e.mutable_snapshot()->set_data(m.snapshot.data);
  }
  return e;
}

Message from_pb(const pb::RaftEnvelope& e) {
  Message m;
  m.type = static_cast<MsgType>(e.type());
  m.from = e.from();
  m.to = e.to();
  m.term = e.term();
  m.log_index = e.log_index();
  m.log_term = e.log_term();
  m.commit = e.commit();
  m.reject = e.reject();
  m.match_index = e.match_index();
  m.hint_index = e.hint_index();
  m.hint_term = e.hint_term();
  m.read_seq = e.read_seq();
  m.entries.reserve(static_cast<std::size_t>(e.entries_size()));
  for (const pb::Entry& pe : e.entries()) {
    Entry en;
    en.term = pe.term();
    en.index = pe.index();
    en.type = static_cast<EntryType>(pe.type());
    en.data = pe.data();
    m.entries.push_back(std::move(en));
  }
  if (e.has_snapshot()) {
    m.snapshot.meta.index = e.snapshot().index();
    m.snapshot.meta.term = e.snapshot().term();
    m.snapshot.data = e.snapshot().data();
  }
  return m;
}

}  // namespace

// One outgoing client-stream per peer, with its own thread; messages are dropped while the peer is unreachable
// (Raft retransmits), and the queue is bounded.
struct NodeServer::PeerLink {
  PeerLink(PeerAddr peer) : addr(std::move(peer.addr)) { th = std::thread([this] { loop(); }); }
  ~PeerLink() { stop(); }

  void push(pb::RaftEnvelope e) {
    {
      std::lock_guard<std::mutex> lk(mu);
      if (q.size() > 100000) q.pop_front();
      q.push_back(std::move(e));
    }
    cv.notify_one();
  }
  void stop() {
    {
      std::lock_guard<std::mutex> lk(mu);
      stopping = true;
    }
    cv.notify_all();
    if (th.joinable()) th.join();
  }
  void loop() {
    while (true) {
      {
        std::lock_guard<std::mutex> lk(mu);
        if (stopping) return;
      }
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      auto stub = pb::RaftTransport::NewStub(channel);
      grpc::ClientContext ctx;
      pb::Ack ack;
      auto writer = stub->Stream(&ctx, &ack);
      bool ok = true;
      while (ok) {
        std::deque<pb::RaftEnvelope> batch;
        {
          std::unique_lock<std::mutex> lk(mu);
          cv.wait_for(lk, std::chrono::milliseconds(200), [&] { return stopping || !q.empty(); });
          if (stopping) break;
          batch.swap(q);
        }
        for (const pb::RaftEnvelope& e : batch)
          if (!writer->Write(e)) {
            ok = false;
            break;
          }
      }
      ctx.TryCancel();
      writer->WritesDone();
      writer->Finish();
      std::unique_lock<std::mutex> lk(mu);
      if (stopping) return;
      cv.wait_for(lk, std::chrono::milliseconds(100), [&] { return stopping; });  // back off before reconnecting
    }
  }

  std::string addr;
  std::mutex mu;
  std::condition_variable cv;
  std::deque<pb::RaftEnvelope> q;
  bool stopping = false;
  std::thread th;
};

NodeServer::NodeServer(ServerOptions opt) : opt_(std::move(opt)) {
  std::vector<NodeId> ids;
  for (const PeerAddr& p : opt_.peers) ids.push_back(p.id);
  fs_.push_back(std::make_unique<PosixFs>(opt_.data_dir + "/node" + std::to_string(opt_.id)));
  std::random_device rd;
  for (ShardId s = 0; s < opt_.shards; ++s) {
    Config rc;
    rc.id = opt_.id;
    rc.peers = ids;
    rc.election_ticks = opt_.election_ticks;
    rc.heartbeat_ticks = opt_.heartbeat_ticks;
    rc.seed = (static_cast<std::uint64_t>(rd()) << 32) ^ rd() ^ static_cast<std::uint64_t>(Clock::now().time_since_epoch().count());
    ReplicaOptions ro;
    ro.snapshot_threshold = opt_.snapshot_threshold;
    reps_.push_back(std::make_unique<ShardReplica>(s, rc, *fs_[0], "s" + std::to_string(s), ro));
    reps_.back()->set_auto_pump(false);
  }
  for (const PeerAddr& p : opt_.peers)
    if (p.id != opt_.id) links_[p.id] = std::make_unique<PeerLink>(p);
}

NodeServer::~NodeServer() { stop(); }

void NodeServer::start() {
  running_ = true;
  runner_ = std::thread([this] { run(); });
}

void NodeServer::stop() {
  if (running_.exchange(false)) {
    cv_.notify_all();
    if (runner_.joinable()) runner_.join();
  }
  for (auto& [id, l] : links_) l->stop();
}

void NodeServer::post(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
}

void NodeServer::run() {
  const auto period = std::chrono::milliseconds(opt_.tick_ms);
  auto next_tick = Clock::now() + period;
  while (running_) {
    std::deque<std::function<void()>> batch;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_until(lk, next_tick, [&] { return !tasks_.empty() || !running_; });
      batch.swap(tasks_);
    }
    for (auto& t : batch) t();
    const auto now = Clock::now();
    if (now >= next_tick) {
      for (auto& r : reps_) r->tick();
      next_tick += period;
      if (next_tick < now) next_tick = now + period;  // we fell behind (long fsync): do not burst ticks
    }
    for (auto& r : reps_) r->pump();  // ONE WAL fsync covers everything queued since the last iteration
    flush_outboxes();
  }
}

void NodeServer::flush_outboxes() {
  for (auto& r : reps_) {
    for (Envelope& env : r->drain_outbox()) {
      auto it = links_.find(env.msg.to);
      if (it != links_.end()) it->second->push(to_pb(env.shard, env.msg));
    }
  }
}

void NodeServer::deliver(pb::RaftEnvelope env) {
  if (env.shard() >= reps_.size()) return;
  post([this, e = std::move(env)] { reps_[e.shard()]->receive(from_pb(e)); });
}

void NodeServer::execute(const pb::KvRequest& req, pb::KvResponse* resp) {
  struct Waiter {
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    ClientResult res;
  };
  auto w = std::make_shared<Waiter>();
  const ShardId sh = shard_of(req.key(), opt_.shards);
  post([this, w, req, sh] {
    auto cb = [w](const ClientResult& r) {
      {
        std::lock_guard<std::mutex> lk(w->m);
        w->res = r;
        w->done = true;
      }
      w->cv.notify_all();
    };
    if (req.op() == pb::GET) {
      reps_[sh]->read(req.key(), cb);
      return;
    }
    Command c;
    c.op = req.op() == pb::PUT ? OpType::Put : (req.op() == pb::APPEND ? OpType::Append : OpType::Cas);
    c.client = req.client();
    c.seq = req.seq();
    c.key = req.key();
    c.value = req.value();
    c.expected = req.expected();
    reps_[sh]->propose(c, cb);
  });
  std::unique_lock<std::mutex> lk(w->m);
  if (!w->cv.wait_for(lk, std::chrono::milliseconds(opt_.request_timeout_ms), [&] { return w->done; })) {
    resp->set_status(pb::TIMEOUT);
    return;
  }
  switch (w->res.status) {
    case Status::Ok: resp->set_status(pb::OK); break;
    case Status::NotLeader: resp->set_status(pb::NOT_LEADER); break;
    case Status::Retry: resp->set_status(pb::RETRY); break;
  }
  resp->set_ok(w->res.ok);
  resp->set_value(w->res.value);
  resp->set_leader_hint(w->res.leader_hint);
}

void NodeServer::info(pb::InfoResponse* out) {
  auto p = std::make_shared<std::promise<pb::InfoResponse>>();
  auto f = p->get_future();
  post([this, p] {
    pb::InfoResponse r;
    r.set_node(opt_.id);
    for (auto& rep : reps_) {
      pb::ShardInfo* si = r.add_shards();
      si->set_shard(rep->shard());
      si->set_role(static_cast<std::uint32_t>(rep->raft().role()));
      si->set_term(rep->raft().term());
      si->set_leader(rep->raft().leader());
      si->set_commit(rep->raft().commit_index());
      si->set_applied(rep->applied());
    }
    p->set_value(std::move(r));
  });
  if (f.wait_for(std::chrono::seconds(5)) == std::future_status::ready) *out = f.get();
}

}  // namespace raftkv
