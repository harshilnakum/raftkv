#include "mini_test.hpp"
#include "raftkv/sim.hpp"

using namespace raftkv;

namespace {
sim::SimConfig quick(std::uint64_t seed) {
  sim::SimConfig c;
  c.seed = seed;
  c.chaos_us = 3'000'000;
  c.settle_us = 5'000'000;
  return c;
}
// Number of seeds in [from, to] for which the simulator reports a failure with the given injected bug.
int detections(Bug bug, std::uint64_t from, std::uint64_t to, std::uint32_t nodes = 3) {
  int found = 0;
  for (std::uint64_t s = from; s <= to; ++s) {
    sim::SimConfig c = quick(s);
    c.bug = bug;
    c.nodes = nodes;
    if (!sim::run(c).ok) ++found;
  }
  return found;
}
}  // namespace

TEST(sim_healthy_cluster_without_faults_is_linearizable_and_makes_progress) {
  for (std::uint64_t seed = 1; seed <= 20; ++seed) {
    sim::SimConfig c = quick(seed);
    c.faults = false;
    const sim::SimResult r = sim::run(c);
    if (!r.ok) std::printf("    %s\n", r.failure.c_str());
    CHECK(r.ok);
    CHECK(r.stats.ops_ok > 500);
    CHECK_EQ(r.stats.ops_info, 0u);
  }
}

TEST(sim_is_deterministic_same_seed_same_run) {
  for (std::uint64_t seed : {3ull, 77ull, 1234ull}) {
    const sim::SimResult a = sim::run(quick(seed));
    const sim::SimResult b = sim::run(quick(seed));
    CHECK_EQ(a.history_digest, b.history_digest);
    CHECK_EQ(a.stats.events, b.stats.events);
    CHECK_EQ(a.stats.crashes, b.stats.crashes);
    CHECK_EQ(a.history.size(), b.history.size());
  }
  CHECK(sim::run(quick(1)).history_digest != sim::run(quick(2)).history_digest);
}

TEST(sim_correct_implementation_survives_faults_on_3_and_5_nodes) {
  for (std::uint64_t seed = 1; seed <= 150; ++seed) {
    const sim::SimResult r = sim::run(quick(seed));
    if (!r.ok) std::printf("    %s\n", r.failure.c_str());
    CHECK(r.ok);
  }
  for (std::uint64_t seed = 1; seed <= 40; ++seed) {
    sim::SimConfig c = quick(seed);
    c.nodes = 5;
    const sim::SimResult r = sim::run(c);
    if (!r.ok) std::printf("    %s\n", r.failure.c_str());
    CHECK(r.ok);
  }
}

// The tester must be able to fail: each deliberately injected protocol bug is found by the simulator.
TEST(sim_detects_injected_protocol_bugs) {
  CHECK(detections(Bug::SmallQuorum, 1, 30) > 0);
  CHECK(detections(Bug::VoteIgnoresLog, 1, 30) > 0);
  CHECK(detections(Bug::ForgetVote, 1, 400) > 0);
  CHECK(detections(Bug::ReadBeforeTermCommit, 1, 400) > 0);
}
