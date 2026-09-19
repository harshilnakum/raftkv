// raftkv_server --id N --peers 1=host:port,2=host:port,3=host:port [--data DIR] [--shards S]
//               [--tick-ms 10] [--election-ticks 30] [--heartbeat-ticks 3] [--snapshot-threshold 10000]
#include <grpcpp/grpcpp.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include "node_server.hpp"

using namespace raftkv;

namespace {
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

class KvImpl final : public pb::Kv::Service {
 public:
  explicit KvImpl(NodeServer& n) : node_(n) {}
  grpc::Status Execute(grpc::ServerContext*, const pb::KvRequest* req, pb::KvResponse* resp) override {
    node_.execute(*req, resp);
    return grpc::Status::OK;
  }
  grpc::Status Info(grpc::ServerContext*, const pb::InfoRequest*, pb::InfoResponse* resp) override {
    node_.info(resp);
    return grpc::Status::OK;
  }

 private:
  NodeServer& node_;
};

class TransportImpl final : public pb::RaftTransport::Service {
 public:
  explicit TransportImpl(NodeServer& n) : node_(n) {}
  grpc::Status Stream(grpc::ServerContext*, grpc::ServerReader<pb::RaftEnvelope>* reader, pb::Ack*) override {
    pb::RaftEnvelope e;
    while (reader->Read(&e)) node_.deliver(std::move(e));
    return grpc::Status::OK;
  }

 private:
  NodeServer& node_;
};

std::vector<PeerAddr> parse_peers(const std::string& s) {
  std::vector<PeerAddr> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    const auto eq = item.find('=');
    if (eq == std::string::npos) continue;
    out.push_back({static_cast<NodeId>(std::atoi(item.substr(0, eq).c_str())), item.substr(eq + 1)});
  }
  return out;
}
}  // namespace

int main(int argc, char** argv) {
  ServerOptions opt;
  for (int i = 1; i + 1 < argc; i += 2) {
    const std::string k = argv[i];
    const std::string v = argv[i + 1];
    if (k == "--id") opt.id = static_cast<NodeId>(std::atoi(v.c_str()));
    else if (k == "--peers") opt.peers = parse_peers(v);
    else if (k == "--data") opt.data_dir = v;
    else if (k == "--shards") opt.shards = static_cast<ShardId>(std::atoi(v.c_str()));
    else if (k == "--tick-ms") opt.tick_ms = static_cast<std::uint32_t>(std::atoi(v.c_str()));
    else if (k == "--election-ticks") opt.election_ticks = static_cast<std::uint32_t>(std::atoi(v.c_str()));
    else if (k == "--heartbeat-ticks") opt.heartbeat_ticks = static_cast<std::uint32_t>(std::atoi(v.c_str()));
    else if (k == "--snapshot-threshold") opt.snapshot_threshold = std::strtoull(v.c_str(), nullptr, 10);
    else { std::fprintf(stderr, "unknown flag %s\n", k.c_str()); return 2; }
  }
  std::string listen;
  for (const PeerAddr& p : opt.peers)
    if (p.id == opt.id) listen = p.addr;
  if (listen.empty()) {
    std::fprintf(stderr, "usage: raftkv_server --id N --peers 1=host:port,2=host:port,... [--data DIR] [--shards S]\n");
    return 2;
  }
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  try {
    NodeServer node(opt);
    KvImpl kv(node);
    TransportImpl tr(node);
    grpc::ServerBuilder b;
    b.AddListeningPort("0.0.0.0:" + listen.substr(listen.find(':') + 1), grpc::InsecureServerCredentials());
    b.RegisterService(&kv);
    b.RegisterService(&tr);
    b.SetMaxReceiveMessageSize(256 << 20);
    std::unique_ptr<grpc::Server> server = b.BuildAndStart();
    if (!server) {
      std::fprintf(stderr, "failed to listen on %s\n", listen.c_str());
      return 1;
    }
    node.start();
    std::fprintf(stderr, "raftkv node %u listening on %s (%u shards, data in %s)\n", opt.id, listen.c_str(), opt.shards,
                 opt.data_dir.c_str());
    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::fprintf(stderr, "node %u shutting down\n", opt.id);
    node.stop();
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
    return 1;
  }
  return 0;
}
