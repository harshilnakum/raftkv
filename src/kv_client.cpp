#include "kv_client.hpp"

#include <grpcpp/grpcpp.h>

#include <thread>

namespace raftkv {

KvClient::KvClient(const std::vector<std::string>& addrs, ShardId shards, std::uint64_t client_id, std::uint64_t timeout_ms)
    : hint_(shards, 0), shards_(shards), client_id_(client_id), timeout_ms_(timeout_ms) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(256 << 20);
  for (const std::string& a : addrs) stubs_.push_back(pb::Kv::NewStub(grpc::CreateCustomChannel(a, grpc::InsecureChannelCredentials(), args)));
}

bool KvClient::call(pb::KvRequest req, pb::KvResponse* resp) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);
  const ShardId sh = shard_of(req.key(), shards_);
  std::size_t target = hint_[sh];
  while (std::chrono::steady_clock::now() < deadline) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(3000));
    pb::KvResponse r;
    const grpc::Status st = stubs_[target]->Execute(&ctx, req, &r);
    if (!st.ok()) {  // node down or unreachable: try the next one
      ++retries_;
      target = (target + 1) % stubs_.size();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      continue;
    }
    switch (r.status()) {
      case pb::OK:
        hint_[sh] = target;
        *resp = std::move(r);
        return true;
      case pb::NOT_LEADER:
        ++retries_;
        target = (r.leader_hint() >= 1 && r.leader_hint() <= stubs_.size() && r.leader_hint() - 1 != target)
                     ? r.leader_hint() - 1
                     : (target + 1) % stubs_.size();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        break;
      default:  // RETRY / TIMEOUT: outcome unknown; resend the SAME (client, seq) - the state machine dedups
        ++retries_;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        break;
    }
  }
  return false;
}

bool KvClient::put(const std::string& key, const std::string& value) {
  pb::KvRequest q;
  q.set_client(client_id_);
  q.set_seq(++seq_);
  q.set_op(pb::PUT);
  q.set_key(key);
  q.set_value(value);
  pb::KvResponse r;
  return call(std::move(q), &r);
}

bool KvClient::append(const std::string& key, const std::string& value, std::string* result) {
  pb::KvRequest q;
  q.set_client(client_id_);
  q.set_seq(++seq_);
  q.set_op(pb::APPEND);
  q.set_key(key);
  q.set_value(value);
  pb::KvResponse r;
  if (!call(std::move(q), &r)) return false;
  if (result != nullptr) *result = r.value();
  return true;
}

bool KvClient::cas(const std::string& key, const std::string& expected, const std::string& value, bool* swapped,
                   std::string* observed) {
  pb::KvRequest q;
  q.set_client(client_id_);
  q.set_seq(++seq_);
  q.set_op(pb::CAS);
  q.set_key(key);
  q.set_value(value);
  q.set_expected(expected);
  pb::KvResponse r;
  if (!call(std::move(q), &r)) return false;
  if (swapped != nullptr) *swapped = r.ok();
  if (observed != nullptr) *observed = r.value();
  return true;
}

bool KvClient::get(const std::string& key, std::string* value) {
  pb::KvRequest q;
  q.set_client(client_id_);
  q.set_seq(++seq_);
  q.set_op(pb::GET);
  q.set_key(key);
  pb::KvResponse r;
  if (!call(std::move(q), &r)) return false;
  if (value != nullptr) *value = r.value();
  return true;
}

bool KvClient::info(std::size_t node_index, pb::InfoResponse* out) {
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(2000));
  pb::InfoRequest req;
  return stubs_.at(node_index)->Info(&ctx, req, out).ok();
}

}  // namespace raftkv
