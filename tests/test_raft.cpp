#include "mini_test.hpp"
#include "raft_harness.hpp"

using namespace th;

namespace {
std::size_t count_applied(Cluster& c, NodeId id) { return c.n(id).applied.size(); }
}  // namespace

TEST(single_node_elects_itself_and_commits) {
  Cluster c(1, 1);
  const NodeId l = c.wait_leader();
  REQUIRE(l == 1);
  CHECK(c.propose(1, "a"));
  CHECK(c.propose(1, "b"));
  c.tick(2);
  CHECK(c.n(1).applied == (std::vector<std::string>{"a", "b"}));
  CHECK(c.ok());
}

TEST(three_nodes_elect_exactly_one_leader) {
  for (std::uint64_t seed = 1; seed <= 30; ++seed) {
    Cluster c(3, seed);
    const NodeId l = c.wait_leader();
    REQUIRE(l != kNone);
    std::size_t leaders = 0;
    for (NodeId id : c.ids())
      if (c.n(id).node->role() == Role::Leader) ++leaders;
    CHECK_EQ(leaders, 1u);
    CHECK(c.ok());
  }
}

TEST(commands_replicate_to_every_node_in_order) {
  Cluster c(3, 7);
  const NodeId l = c.wait_leader();
  REQUIRE(l != kNone);
  for (int i = 0; i < 50; ++i) CHECK(c.propose(l, "cmd" + std::to_string(i)));
  c.tick(5);
  for (NodeId id : c.ids()) {
    REQUIRE(count_applied(c, id) == 50);
    CHECK(c.n(id).applied.front() == "cmd0" && c.n(id).applied.back() == "cmd49");
  }
  CHECK(c.ok());
}

TEST(non_leaders_refuse_proposals_and_reads) {
  Cluster c(3, 3);
  const NodeId l = c.wait_leader();
  for (NodeId id : c.ids()) {
    if (id == l) continue;
    CHECK(!c.n(id).node->propose("x").has_value());
    CHECK(!c.n(id).node->read_index(1));
  }
}

TEST(leader_crash_elects_a_new_leader_and_keeps_committed_entries) {
  Cluster c(3, 11);
  NodeId l = c.wait_leader();
  for (int i = 0; i < 10; ++i) c.propose(l, "a" + std::to_string(i));
  c.tick(5);
  c.crash(l);
  const NodeId l2 = c.wait_leader();
  REQUIRE(l2 != kNone);
  CHECK(l2 != l);
  for (int i = 0; i < 10; ++i) CHECK(c.propose(l2, "b" + std::to_string(i)));
  c.tick(5);
  c.restart(l);
  c.tick(30);
  for (NodeId id : c.ids()) CHECK_EQ(count_applied(c, id), 20u);
  CHECK(c.ok());
}

TEST(isolated_leader_cannot_commit_and_its_uncommitted_entries_are_discarded) {
  Cluster c(3, 21);
  const NodeId old = c.wait_leader();
  REQUIRE(old != kNone);
  c.propose(old, "committed-before");
  c.tick(5);
  c.isolate(old);
  c.n(old).node->propose("lost-1");  // accepted locally, can never reach a majority
  c.n(old).node->propose("lost-2");
  c.settle();
  // The majority side elects a new leader and makes progress.
  NodeId nl = kNone;
  for (int i = 0; i < 400 && nl == kNone; ++i) {
    c.tick();
    for (NodeId id : c.ids())
      if (id != old && c.n(id).node->role() == Role::Leader) nl = id;
  }
  REQUIRE(nl != kNone);
  CHECK(c.propose(nl, "committed-after"));
  c.tick(5);
  CHECK(count_applied(c, old) == 1);  // the isolated node applied nothing new
  c.heal();
  c.tick(60);
  for (NodeId id : c.ids())
    CHECK(c.n(id).applied == (std::vector<std::string>{"committed-before", "committed-after"}));
  CHECK(c.n(old).node->role() == Role::Follower);
  CHECK(c.ok());
}

TEST(leader_without_a_quorum_steps_down) {
  Cluster c(3, 5);
  const NodeId l = c.wait_leader();
  c.isolate(l);
  c.tick(40);
  CHECK(c.n(l).node->role() != Role::Leader);  // check-quorum
}

TEST(a_restarted_follower_catches_up_from_its_persisted_log) {
  Cluster c(3, 9);
  const NodeId l = c.wait_leader();
  NodeId f = l == 1 ? 2 : 1;
  for (int i = 0; i < 5; ++i) c.propose(l, "x" + std::to_string(i));
  c.tick(5);
  c.crash(f);
  for (int i = 5; i < 12; ++i) c.propose(l, "x" + std::to_string(i));
  c.tick(5);
  c.restart(f);
  c.tick(40);
  CHECK_EQ(count_applied(c, f), 12u);
  CHECK(c.ok());
}

TEST(a_lagging_follower_is_brought_up_to_date_with_a_snapshot) {
  Cluster c(3, 13);
  const NodeId l = c.wait_leader();
  NodeId f = l == 1 ? 2 : 1;
  c.crash(f);
  for (int i = 0; i < 40; ++i) c.propose(l, "v" + std::to_string(i));
  c.tick(5);
  // The two live nodes compact their logs: the entries f needs no longer exist.
  for (NodeId id : c.ids())
    if (c.n(id).up) c.snapshot(id);
  CHECK(c.n(l).node->log().first_index() > 1);
  c.restart(f);
  c.tick(60);
  REQUIRE(count_applied(c, f) == 40);
  CHECK(c.n(f).applied.back() == "v39");
  CHECK(c.n(f).node->log().snapshot_meta().index > 0);
  // And it keeps following normally afterwards.
  CHECK(c.propose(l, "after"));
  c.tick(5);
  CHECK_EQ(count_applied(c, f), 41u);
  CHECK(c.ok());
}

TEST(restart_from_snapshot_plus_log_restores_state) {
  Cluster c(3, 17);
  const NodeId l = c.wait_leader();
  for (int i = 0; i < 20; ++i) c.propose(l, "s" + std::to_string(i));
  c.tick(5);
  for (NodeId id : c.ids()) c.snapshot(id);
  for (int i = 20; i < 25; ++i) c.propose(l, "s" + std::to_string(i));
  c.tick(5);
  for (NodeId id : c.ids()) {
    c.crash(id);
    c.restart(id);
  }
  c.wait_leader();
  c.tick(40);
  for (NodeId id : c.ids()) CHECK_EQ(count_applied(c, id), 25u);
  CHECK(c.ok());
}

TEST(read_index_needs_a_quorum_and_a_committed_entry_of_the_term) {
  Cluster c(3, 23);
  const NodeId l = c.wait_leader();
  c.propose(l, "w");
  c.tick(3);
  CHECK(c.n(l).node->read_index(77));
  c.settle();
  REQUIRE(c.n(l).reads.size() == 1);
  CHECK_EQ(c.n(l).reads[0].ctx, 77u);
  CHECK(c.n(l).reads[0].index >= 2);  // covers the noop and the write
  // An isolated leader must NOT confirm reads.
  c.isolate(l);
  CHECK(c.n(l).node->read_index(78));
  c.settle();
  CHECK(c.n(l).reads.size() == 1);  // still only the first
  c.tick(40);                       // it steps down: the pending read is aborted, never served
  CHECK(c.n(l).reads.size() == 1);
  REQUIRE(c.n(l).aborted.size() == 1);
  CHECK_EQ(c.n(l).aborted[0], 78u);
}

// Random chaos: crashes, restarts, partitions, proposals and snapshots, with all safety invariants checked after
// every step.
TEST(chaos_preserves_all_raft_safety_invariants) {
  for (std::uint64_t seed = 1; seed <= 300; ++seed) {
    Cluster c(3 + (seed % 3 == 0 ? 2 : 0), seed);
    Rng rng(seed * 7919);
    int next_cmd = 0;
    for (int step = 0; step < 600 && c.ok(); ++step) {
      const std::uint64_t r = rng.below(100);
      const NodeId id = c.ids()[rng.below(c.ids().size())];
      if (r < 40) {
        c.tick(1 + rng.below(4));
      } else if (r < 65) {
        const NodeId l = c.leader();
        if (l != kNone) c.propose(l, "c" + std::to_string(next_cmd++));
      } else if (r < 72) {
        if (c.n(id).up) c.crash(id);
      } else if (r < 82) {
        if (!c.n(id).up) c.restart(id);
      } else if (r < 88) {
        c.partition({id});
      } else if (r < 93) {
        c.heal();
      } else if (r < 97) {
        if (c.n(id).up) c.snapshot(id);
      } else {
        c.tick(20);
      }
    }
    if (!c.ok()) {
      std::printf("    (seed %llu)\n", static_cast<unsigned long long>(seed));
      CHECK(false);
      return;
    }
  }
}
