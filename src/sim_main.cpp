// raftkv_sim: run the deterministic simulator over a range of seeds.
//   raftkv_sim --seeds 1..1000 [--nodes 3] [--shards 2] [--clients 6] [--chaos-ms 4000] [--settle-ms 6000]
//              [--bug NAME] [--no-faults] [--verbose] [--stop-on-failure]
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "raftkv/sim.hpp"

using namespace raftkv;

namespace {
const char* kBugs[] = {"none", "small-quorum", "commit-old-term", "vote-ignores-log", "forget-vote", "stale-read", "read-before-term-commit"};
}

int main(int argc, char** argv) {
  sim::SimConfig cfg;
  std::uint64_t from = 1, to = 100;
  bool stop = false, verbose = false;
  for (int i = 1; i < argc; ++i) {
    auto arg = [&](const char* n) { return std::strcmp(argv[i], n) == 0 && i + 1 < argc; };
    if (arg("--seeds")) {
      const std::string s = argv[++i];
      const auto dots = s.find("..");
      if (dots == std::string::npos) from = to = std::strtoull(s.c_str(), nullptr, 10);
      else { from = std::strtoull(s.substr(0, dots).c_str(), nullptr, 10); to = std::strtoull(s.substr(dots + 2).c_str(), nullptr, 10); }
    } else if (arg("--nodes")) cfg.nodes = static_cast<std::uint32_t>(std::atoi(argv[++i]));
    else if (arg("--shards")) cfg.shards = static_cast<std::uint32_t>(std::atoi(argv[++i]));
    else if (arg("--clients")) cfg.clients = static_cast<std::uint32_t>(std::atoi(argv[++i]));
    else if (arg("--keys")) cfg.keys = static_cast<std::uint32_t>(std::atoi(argv[++i]));
    else if (arg("--chaos-ms")) cfg.chaos_us = std::strtoull(argv[++i], nullptr, 10) * 1000;
    else if (arg("--settle-ms")) cfg.settle_us = std::strtoull(argv[++i], nullptr, 10) * 1000;
    else if (arg("--bug")) {
      const std::string b = argv[++i];
      bool found = false;
      for (std::size_t k = 0; k < sizeof(kBugs) / sizeof(kBugs[0]); ++k)
        if (b == kBugs[k]) { cfg.bug = static_cast<Bug>(k); found = true; }
      if (!found) { std::fprintf(stderr, "unknown bug '%s'\n", b.c_str()); return 2; }
    } else if (std::strcmp(argv[i], "--no-faults") == 0) cfg.faults = false;
    else if (std::strcmp(argv[i], "--no-check-quorum") == 0) cfg.check_quorum = false;
    else if (std::strcmp(argv[i], "--verbose") == 0) verbose = true;
    else if (std::strcmp(argv[i], "--stop-on-failure") == 0) stop = true;
    else { std::fprintf(stderr, "unknown argument '%s'\n", argv[i]); return 2; }
  }

  const auto t0 = std::chrono::steady_clock::now();
  std::uint64_t runs = 0, failed = 0, inconclusive = 0, ops = 0, info = 0, crashes = 0, sync_crashes = 0, parts = 0, torn = 0, events = 0;
  std::uint64_t max_term = 0;
  std::string first_failure;
  for (std::uint64_t seed = from; seed <= to; ++seed) {
    cfg.seed = seed;
    const sim::SimResult r = sim::run(cfg);
    ++runs;
    ops += r.stats.ops_ok;
    info += r.stats.ops_info;
    crashes += r.stats.crashes;
    sync_crashes += r.stats.sync_crashes;
    parts += r.stats.partitions;
    torn += r.stats.torn_wal_repairs;
    events += r.stats.events;
    max_term = std::max(max_term, r.stats.max_term);
    if (r.lin_unknown) ++inconclusive;
    if (!r.ok) {
      ++failed;
      if (first_failure.empty()) first_failure = r.failure;
      if (verbose || failed <= 3) std::printf("FAIL %s\n", r.failure.c_str());
      if (stop) break;
    } else if (verbose) {
      std::printf("ok seed %llu: %llu ops (+%llu unknown), %llu crashes, %llu partitions\n", (unsigned long long)seed,
                  (unsigned long long)r.stats.ops_ok, (unsigned long long)r.stats.ops_info, (unsigned long long)r.stats.crashes,
                  (unsigned long long)r.stats.partitions);
    }
  }
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  std::printf("bug=%s nodes=%u shards=%u clients=%u faults=%s\n", kBugs[static_cast<int>(cfg.bug)], cfg.nodes, cfg.shards,
              cfg.clients, cfg.faults ? "on" : "off");
  std::printf("%llu runs in %.1fs: %llu failed, %llu inconclusive (checker state budget)\n", (unsigned long long)runs, secs,
              (unsigned long long)failed, (unsigned long long)inconclusive);
  std::printf("checked %llu completed operations (+%llu unknown-outcome), %llu crashes (%llu of them power loss during fsync), %llu partitions, %llu torn-WAL repairs, %llu events, max term %llu\n",
              (unsigned long long)ops, (unsigned long long)info, (unsigned long long)crashes, (unsigned long long)sync_crashes, (unsigned long long)parts,
              (unsigned long long)torn, (unsigned long long)events, (unsigned long long)max_term);
  return failed == 0 ? 0 : 1;
}
