// A real raftkv node: hosts one ShardReplica per shard, driven by a single runner thread (so replicas need no locking),
// with gRPC transport between nodes and a client-facing gRPC API. All replica access happens on the runner thread; gRPC
// handler threads only enqueue tasks and wait for results.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "raftkv.grpc.pb.h"
#include "raftkv/replica.hpp"

namespace raftkv {

struct PeerAddr {
  NodeId id;
  std::string addr;  // host:port
};

struct ServerOptions {
  NodeId id = 1;
  std::vector<PeerAddr> peers;  // every node of the group, including this one
  std::string data_dir = "data";
  ShardId shards = 4;
  std::uint32_t tick_ms = 10;
  std::uint32_t election_ticks = 30;
  std::uint32_t heartbeat_ticks = 3;
  std::uint64_t snapshot_threshold = 10000;
  std::uint64_t request_timeout_ms = 5000;
};

class NodeServer {
 public:
  explicit NodeServer(ServerOptions opt);
  ~NodeServer();
  void start();
  void stop();

  // gRPC handler-thread entry points
  void execute(const pb::KvRequest& req, pb::KvResponse* resp);
  void deliver(pb::RaftEnvelope env);
  void info(pb::InfoResponse* out);

 private:
  struct PeerLink;
  void run();
  void post(std::function<void()> task);
  void flush_outboxes();

  ServerOptions opt_;
  std::vector<std::unique_ptr<FileSystem>> fs_;
  std::vector<std::unique_ptr<ShardReplica>> reps_;
  std::map<NodeId, std::unique_ptr<PeerLink>> links_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  std::atomic<bool> running_{false};
  std::thread runner_;
};

}  // namespace raftkv
