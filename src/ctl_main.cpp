// raftkv_ctl --addrs h1:p1,h2:p2,h3:p3 [--shards 4] (info | put K V | get K | append K V | cas K EXPECTED NEW)
#include <cstdio>
#include <cstdlib>
#include <random>
#include <sstream>

#include "kv_client.hpp"

using namespace raftkv;

int main(int argc, char** argv) {
  std::vector<std::string> addrs, rest;
  ShardId shards = 4;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--addrs" && i + 1 < argc) { std::stringstream ss(argv[++i]); std::string x; while (std::getline(ss, x, ',')) addrs.push_back(x); }
    else if (a == "--shards" && i + 1 < argc) shards = static_cast<ShardId>(std::atoi(argv[++i]));
    else rest.push_back(a);
  }
  if (addrs.empty() || rest.empty()) { std::fprintf(stderr, "usage: raftkv_ctl --addrs h1:p1,... (info | put K V | get K | append K V | cas K EXP NEW)\n"); return 2; }
  std::random_device rd;
  KvClient c(addrs, shards, (static_cast<std::uint64_t>(rd()) << 32) | rd(), 15000);
  const std::string& cmd = rest[0];
  if (cmd == "info") {
    for (std::size_t n = 0; n < addrs.size(); ++n) {
      pb::InfoResponse r;
      if (!c.info(n, &r)) { std::printf("node %zu (%s): unreachable\n", n + 1, addrs[n].c_str()); continue; }
      std::printf("node %u (%s):\n", r.node(), addrs[n].c_str());
      for (const auto& s : r.shards()) std::printf("  shard %u: %s term %llu leader %u commit %llu applied %llu\n", s.shard(), s.role() == 2 ? "LEADER  " : s.role() == 1 ? "candidate" : "follower", (unsigned long long)s.term(), s.leader(), (unsigned long long)s.commit(), (unsigned long long)s.applied());
    }
    return 0;
  }
  if (cmd == "put" && rest.size() == 3) return c.put(rest[1], rest[2]) ? 0 : 1;
  if (cmd == "get" && rest.size() == 2) { std::string v; if (!c.get(rest[1], &v)) return 1; std::printf("%s\n", v.c_str()); return 0; }
  if (cmd == "append" && rest.size() == 3) { std::string v; if (!c.append(rest[1], rest[2], &v)) return 1; std::printf("%s\n", v.c_str()); return 0; }
  if (cmd == "cas" && rest.size() == 4) { bool sw = false; std::string seen; if (!c.cas(rest[1], rest[2], rest[3], &sw, &seen)) return 1; std::printf("%s (saw '%s')\n", sw ? "swapped" : "not swapped", seen.c_str()); return 0; }
  std::fprintf(stderr, "bad command\n");
  return 2;
}
