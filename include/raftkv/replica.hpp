// One replica of one shard: the driver that ties a RaftNode to durable storage, the KV state machine and client
// requests. It owns no clock, network or threads: the host (simulator or gRPC server) feeds it ticks and messages and
// collects its outgoing messages. Not thread-safe; a host serialises access.
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "raftkv/fs.hpp"
#include "raftkv/kv.hpp"
#include "raftkv/raft.hpp"
#include "raftkv/wal.hpp"

namespace raftkv {

using ShardId = std::uint32_t;

struct Envelope {
  ShardId shard = 0;
  Message msg;
};

enum class Status : std::uint8_t {
  Ok,         // done; see ClientResult
  NotLeader,  // definitely NOT executed here; try leader_hint or another replica
  Retry,      // outcome unknown (leadership changed under the request); retry the SAME (client, seq) elsewhere
};

struct ClientResult {
  Status status = Status::Ok;
  bool ok = true;       // Cas: swapped?
  std::string value;    // Get: the value; Cas: value observed; Put/Append: resulting value
  NodeId leader_hint = kNone;
  bool stale = false;   // request older than the client's latest already-answered request
};
using Callback = std::function<void(const ClientResult&)>;

struct ReplicaOptions {
  std::uint64_t snapshot_threshold = 1000;  // take a snapshot after this many applied entries
};

inline ShardId shard_of(const std::string& key, ShardId shards) {
  std::uint64_t h = 0xCBF29CE484222325ull;
  for (char c : key) {
    h ^= static_cast<unsigned char>(c);
    h *= 0x100000001B3ull;
  }
  return static_cast<ShardId>(h % shards);
}

class ShardReplica {
 public:
  // Recovers from `fs` (files named "<prefix>.wal/.snap"). Throws StorageError on unrecoverable corruption.
  ShardReplica(ShardId shard, Config raft_cfg, FileSystem& fs, const std::string& prefix, ReplicaOptions opt = {});

  void tick();
  void receive(Message m);
  void propose(const Command& c, Callback cb);
  void read(const std::string& key, Callback cb);

  std::vector<Envelope> drain_outbox();
  void pump();
  // Batching: with auto-pump off, tick/receive/propose/read only queue work; the host calls pump() once for many
  // requests, so one WAL fsync covers all of them (group commit). The simulator keeps the default (on).
  void set_auto_pump(bool on) { auto_pump_ = on; }

  ShardId shard() const { return shard_; }
  const RaftNode& raft() const { return raft_; }
  const KvStateMachine& state() const { return sm_; }
  std::size_t pending_requests() const { return pending_.size() + reads_.size() + read_wait_.size(); }
  std::size_t repaired_bytes() const { return repaired_bytes_; }
  Index applied() const { return raft_.applied_index(); }

 private:
  struct PendingWrite {
    Term term;
    Callback cb;
  };
  struct PendingRead {
    std::string key;
    Callback cb;
  };

  void apply_entry(const Entry& e);
  void serve_reads();
  void maybe_snapshot();
  void fail_all(Status s);

  ShardId shard_;
  ReplicaOptions opt_;
  GroupStorage storage_;
  KvStateMachine sm_;
  std::size_t repaired_bytes_ = 0;
  RaftNode raft_;
  std::vector<Envelope> outbox_;
  std::map<Index, PendingWrite> pending_;
  std::map<std::uint64_t, PendingRead> reads_;                // read_index requested, not yet confirmed
  std::multimap<Index, std::uint64_t> read_wait_;             // confirmed, waiting for apply >= index
  std::uint64_t next_ctx_ = 1;
  bool auto_pump_ = true;
};

}  // namespace raftkv
