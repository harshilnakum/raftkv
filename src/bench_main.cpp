// raftkv_bench: closed-loop load generator over real gRPC.
//   raftkv_bench --addrs h1:p1,h2:p2,h3:p3 [--shards 4] [--threads 32] [--duration 10] [--warmup 2]
//                [--mode write|read|mixed] [--value-size 128] [--keys 10000] [--gaps]
// --gaps additionally reports the longest interval with no successful operation (use it while killing a leader to
// measure the unavailability window).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <sstream>
#include <thread>

#include "kv_client.hpp"

using namespace raftkv;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
  std::vector<std::string> addrs;
  ShardId shards = 4;
  int threads = 32, duration = 10, warmup = 2, value_size = 128, keys = 10000;
  std::string mode = "write";
  bool gaps = false;
  for (int i = 1; i < argc; ++i) {
    const std::string k = argv[i];
    auto val = [&]() { return std::string(i + 1 < argc ? argv[++i] : ""); };
    if (k == "--addrs") { std::stringstream ss(val()); std::string a; while (std::getline(ss, a, ',')) addrs.push_back(a); }
    else if (k == "--shards") shards = static_cast<ShardId>(std::atoi(val().c_str()));
    else if (k == "--threads") threads = std::atoi(val().c_str());
    else if (k == "--duration") duration = std::atoi(val().c_str());
    else if (k == "--warmup") warmup = std::atoi(val().c_str());
    else if (k == "--mode") mode = val();
    else if (k == "--value-size") value_size = std::atoi(val().c_str());
    else if (k == "--keys") keys = std::atoi(val().c_str());
    else if (k == "--gaps") gaps = true;
    else { std::fprintf(stderr, "unknown argument %s\n", k.c_str()); return 2; }
  }
  if (addrs.empty()) { std::fprintf(stderr, "usage: raftkv_bench --addrs h1:p1,h2:p2,h3:p3 [...]\n"); return 2; }

  const std::string value(static_cast<std::size_t>(value_size), 'x');
  std::random_device rd;
  if (mode != "write") {  // pre-populate for reads
    KvClient c(addrs, shards, (static_cast<std::uint64_t>(rd()) << 32) | rd());
    for (int k = 0; k < keys; ++k)
      if (!c.put("key" + std::to_string(k), value)) { std::fprintf(stderr, "prefill failed\n"); return 1; }
  }

  struct Local {
    std::vector<std::uint32_t> lat_us;
    std::vector<std::uint64_t> done_ns;
    std::uint64_t ok = 0, fail = 0;
  };
  std::vector<Local> locals(static_cast<std::size_t>(threads));
  std::atomic<bool> stop{false}, measuring{false};
  const auto t_start = Clock::now();
  std::vector<std::thread> ts;
  for (int t = 0; t < threads; ++t) {
    ts.emplace_back([&, t] {
      KvClient c(addrs, shards, (static_cast<std::uint64_t>(rd()) << 32) | rd());
      std::mt19937_64 rng(static_cast<std::uint64_t>(t) * 7919 + 1);
      Local& L = locals[static_cast<std::size_t>(t)];
      while (!stop) {
        const std::string key = "key" + std::to_string(rng() % static_cast<std::uint64_t>(keys));
        const bool do_read = mode == "read" || (mode == "mixed" && (rng() % 100) < 50);
        const auto a = Clock::now();
        bool ok;
        if (do_read) {
          std::string out;
          ok = c.get(key, &out);
        } else {
          ok = c.put(key, value);
        }
        const auto b = Clock::now();
        if (measuring) {
          if (ok) {
            ++L.ok;
            L.lat_us.push_back(static_cast<std::uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(b - a).count()));
          } else {
            ++L.fail;
          }
        }
        if (gaps && ok) L.done_ns.push_back(static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(b - t_start).count()));
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(warmup));
  measuring = true;
  const auto m0 = Clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(duration));
  const auto m1 = Clock::now();
  measuring = false;
  stop = true;
  for (auto& t : ts) t.join();

  std::vector<std::uint32_t> lat;
  std::uint64_t ok = 0, fail = 0;
  for (auto& l : locals) {
    lat.insert(lat.end(), l.lat_us.begin(), l.lat_us.end());
    ok += l.ok;
    fail += l.fail;
  }
  std::sort(lat.begin(), lat.end());
  auto pct = [&](double q) { return lat.empty() ? 0.0 : lat[std::min(lat.size() - 1, static_cast<std::size_t>(q * static_cast<double>(lat.size() - 1) + 0.5))] / 1000.0; };
  const double secs = std::chrono::duration<double>(m1 - m0).count();
  std::printf("mode=%s threads=%d shards=%u value=%dB nodes=%zu\n", mode.c_str(), threads, shards, value_size, addrs.size());
  std::printf("%.0f ops/s over %.1fs (%llu ok, %llu failed)\n", static_cast<double>(ok) / secs, secs, (unsigned long long)ok, (unsigned long long)fail);
  std::printf("latency ms: p50 %.2f  p90 %.2f  p99 %.2f  p99.9 %.2f  max %.2f\n", pct(0.5), pct(0.9), pct(0.99), pct(0.999), lat.empty() ? 0.0 : lat.back() / 1000.0);
  if (gaps) {
    std::vector<std::uint64_t> all;
    for (auto& l : locals) all.insert(all.end(), l.done_ns.begin(), l.done_ns.end());
    std::sort(all.begin(), all.end());
    std::uint64_t best = 0, at = 0;
    for (std::size_t i = 1; i < all.size(); ++i)
      if (all[i] - all[i - 1] > best) { best = all[i] - all[i - 1]; at = all[i - 1]; }
    std::printf("longest interval with no successful operation: %.0f ms (starting at t=%.2fs)\n", static_cast<double>(best) / 1e6, static_cast<double>(at) / 1e9);
  }
  return fail == 0 ? 0 : 1;
}
