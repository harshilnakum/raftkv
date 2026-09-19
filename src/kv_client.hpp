// Client library: retries across nodes with a stable (client, seq) so a retried write executes exactly once.
// Not thread-safe: use one KvClient per thread.
#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "raftkv.grpc.pb.h"
#include "raftkv/replica.hpp"

namespace raftkv {

class KvClient {
 public:
  // addrs[i] is the address of node id i+1.
  KvClient(const std::vector<std::string>& addrs, ShardId shards, std::uint64_t client_id, std::uint64_t timeout_ms = 10000);

  bool put(const std::string& key, const std::string& value);
  bool append(const std::string& key, const std::string& value, std::string* result = nullptr);
  bool cas(const std::string& key, const std::string& expected, const std::string& value, bool* swapped, std::string* observed = nullptr);
  bool get(const std::string& key, std::string* value);  // linearizable
  bool info(std::size_t node_index, pb::InfoResponse* out);

  std::uint64_t retries() const { return retries_; }

 private:
  bool call(pb::KvRequest req, pb::KvResponse* resp);

  std::vector<std::unique_ptr<pb::Kv::Stub>> stubs_;
  std::vector<std::size_t> hint_;  // per shard: node index of the last known leader
  ShardId shards_;
  std::uint64_t client_id_, seq_ = 0, timeout_ms_, retries_ = 0;
};

}  // namespace raftkv
