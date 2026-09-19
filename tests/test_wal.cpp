#include "mini_test.hpp"
#include "raftkv/sim_fs.hpp"
#include "raftkv/wal.hpp"

using namespace raftkv;

namespace {
Entry E(Index i, Term t, const std::string& d = "x") { return Entry{t, i, EntryType::Command, d}; }
Ready R(std::optional<HardState> hs, std::vector<Entry> es) {
  Ready r;
  r.hard_state = hs;
  r.entries = std::move(es);
  return r;
}
bool same(const std::vector<Entry>& a, const std::vector<Entry>& b) { return a == b; }
}  // namespace

TEST(wal_fresh_storage_recovers_empty) {
  SimFs fs;
  GroupStorage st(fs, "g");
  const Recovered r = st.recover();
  CHECK(r.fresh);
  CHECK(r.entries.empty());
  CHECK_EQ(r.hs.term, 0u);
}

TEST(wal_round_trips_hard_state_and_entries) {
  SimFs fs;
  {
    GroupStorage st(fs, "g");
    st.recover();
    st.save(R(HardState{3, 2}, {E(1, 1), E(2, 2), E(3, 3)}));
    st.save(R(HardState{4, 0}, {E(4, 4, "y")}));
  }
  GroupStorage st2(fs, "g");
  const Recovered r = st2.recover();
  CHECK(!r.fresh);
  CHECK(r.hs == (HardState{4, 0}));
  CHECK(same(r.entries, {E(1, 1), E(2, 2), E(3, 3), E(4, 4, "y")}));
  CHECK_EQ(r.repaired_bytes, 0u);
}

TEST(wal_a_later_append_overwrites_a_conflicting_suffix) {
  SimFs fs;
  GroupStorage st(fs, "g");
  st.recover();
  st.save(R({}, {E(1, 1), E(2, 1), E(3, 1), E(4, 1)}));
  st.save(R({}, {E(3, 2, "new"), E(4, 2, "new")}));  // a new leader rewrites indexes 3..4
  GroupStorage st2(fs, "g");
  CHECK(same(st2.recover().entries, {E(1, 1), E(2, 1), E(3, 2, "new"), E(4, 2, "new")}));
  // a shorter overwrite also drops everything after it
  st.save(R({}, {E(2, 3, "z")}));
  GroupStorage st3(fs, "g");
  CHECK(same(st3.recover().entries, {E(1, 1), E(2, 3, "z")}));
}

TEST(wal_unsynced_writes_are_lost_or_torn_but_never_misread) {
  for (std::uint64_t seed = 1; seed <= 400; ++seed) {
    SimFs fs;
    Rng rng(seed);
    GroupStorage st(fs, "g");
    st.recover();
    st.save(R(HardState{1, 1}, {E(1, 1), E(2, 1)}));  // durable
    st.save_unsynced(R(HardState{2, 0}, {E(3, 2, std::string(1 + rng.below(50), 'q'))}));
    fs.crash(rng);
    GroupStorage st2(fs, "g");
    const Recovered r = st2.recover();
    // Either the unsynced record vanished, or (rarely) it survived intact; in no case is anything else visible.
    const bool base = same(r.entries, {E(1, 1), E(2, 1)}) && r.hs == (HardState{1, 1});
    const bool hs_only = same(r.entries, {E(1, 1), E(2, 1)}) && r.hs == (HardState{2, 0});
    const bool all = r.entries.size() == 3 && r.entries[2].index == 3 && r.hs == (HardState{2, 0});
    CHECK(base || hs_only || all);
    // the repaired log accepts new writes and recovers them
    st2.save(R({}, {E(r.entries.size() + 1, 5, "after")}));
    GroupStorage st3(fs, "g");
    const Recovered r3 = st3.recover();
    CHECK_EQ(r3.entries.back().data, std::string("after"));
    CHECK_EQ(r3.repaired_bytes, 0u);
  }
}

TEST(wal_local_snapshot_compacts_the_log) {
  SimFs fs;
  GroupStorage st(fs, "g");
  st.recover();
  st.save(R(HardState{2, 1}, {E(1, 1), E(2, 1), E(3, 2), E(4, 2), E(5, 2)}));
  Snapshot s;
  s.meta = {3, 2};
  s.data = "STATE@3";
  st.save_local_snapshot(s, HardState{2, 1}, {E(4, 2), E(5, 2)});
  st.save(R({}, {E(6, 2)}));
  GroupStorage st2(fs, "g");
  const Recovered r = st2.recover();
  CHECK(r.snap.meta == (SnapshotMeta{3, 2}));
  CHECK_EQ(r.snap.data, std::string("STATE@3"));
  CHECK(same(r.entries, {E(4, 2), E(5, 2), E(6, 2)}));
  CHECK(r.hs == (HardState{2, 1}));
}

TEST(wal_crash_between_snapshot_file_and_wal_rewrite_recovers) {
  SimFs fs;
  GroupStorage st(fs, "g");
  st.recover();
  st.save(R(HardState{2, 1}, {E(1, 1), E(2, 1), E(3, 2), E(4, 2)}));
  Snapshot s;
  s.meta = {3, 2};
  s.data = "S3";
  st.save_snapshot_file_only(s);  // ... power fails here: the WAL still holds entries 1..4
  Rng rng(1);
  fs.crash(rng);
  GroupStorage st2(fs, "g");
  const Recovered r = st2.recover();
  CHECK(r.snap.meta == (SnapshotMeta{3, 2}));
  CHECK(same(r.entries, {E(4, 2)}));  // entries covered by the snapshot are dropped
}

// Regression (found by the cluster simulator, seed 1449): after recovering from "snapshot file ahead of the WAL", entries
// appended later must survive ANOTHER recovery instead of leaving a gap in the WAL.
TEST(wal_appends_after_recovering_from_a_snapshot_ahead_of_the_wal_survive_the_next_recovery) {
  SimFs fs;
  GroupStorage st(fs, "g");
  st.recover();
  st.save(R(HardState{2, 1}, {E(1, 1), E(2, 1), E(3, 2), E(4, 2)}));
  Snapshot s;
  s.meta = {3, 2};
  s.data = "S3";
  st.save_snapshot_file_only(s);  // power fails before the WAL is rewritten
  Rng rng(1);
  fs.crash(rng);
  GroupStorage st2(fs, "g");
  CHECK(same(st2.recover().entries, {E(4, 2)}));
  st2.save(R({}, {E(5, 2), E(6, 2)}));
  GroupStorage st3(fs, "g");
  const Recovered r = st3.recover();  // used to throw StorageError("WAL has a gap ...")
  CHECK(r.snap.meta == (SnapshotMeta{3, 2}));
  CHECK(same(r.entries, {E(4, 2), E(5, 2), E(6, 2)}));
  // and the same when the snapshot is far ahead of everything in the old WAL
  Snapshot far;
  far.meta = {100, 7};
  far.data = "S100";
  st3.save_snapshot_file_only(far);
  fs.crash(rng);
  GroupStorage st4(fs, "g");
  CHECK(st4.recover().entries.empty());
  st4.save(R({}, {E(101, 7)}));
  GroupStorage st5(fs, "g");
  CHECK(same(st5.recover().entries, {E(101, 7)}));
}

TEST(wal_received_snapshot_replaces_the_whole_log) {
  SimFs fs;
  GroupStorage st(fs, "g");
  st.recover();
  st.save(R(HardState{2, 0}, {E(1, 1), E(2, 1), E(3, 1)}));  // a divergent local log
  Ready rd;
  rd.snapshot = Snapshot{{10, 4}, "S10"};
  rd.hard_state = HardState{4, 0};
  st.save(rd);
  st.save(R({}, {E(11, 4)}));
  GroupStorage st2(fs, "g");
  const Recovered r = st2.recover();
  CHECK(r.snap.meta == (SnapshotMeta{10, 4}));
  CHECK(same(r.entries, {E(11, 4)}));
}

// Random sequences of synced/unsynced saves and local snapshots with power loss at random points: the recovered
// state must equal the last durably-saved state (or that state plus part of a save that never completed).
TEST(wal_fuzz_crash_consistency) {
  for (std::uint64_t seed = 1; seed <= 150; ++seed) {
    SimFs fs;
    Rng rng(seed);
    auto st = std::make_unique<GroupStorage>(fs, "g");
    st->recover();
    HardState hs;
    Snapshot snap;
    std::vector<Entry> log;  // entries after snap
    Term term = 1;
    auto last = [&]() { return snap.meta.index + log.size(); };
    for (int step = 0; step < 200; ++step) {
      const std::uint64_t r = rng.below(100);
      if (r < 55) {  // append (maybe overwriting a suffix)
        Index from = last() + 1;
        if (!log.empty() && rng.chance(0.2)) from = snap.meta.index + 1 + rng.below(log.size());
        const std::size_t n = 1 + rng.below(4);
        std::vector<Entry> es;
        for (std::size_t i = 0; i < n; ++i) es.push_back(E(from + i, term, "v" + std::to_string(rng.below(1000))));
        st->save(R(std::nullopt, es));
        log.resize(from - snap.meta.index - 1);
        for (auto& e : es) log.push_back(e);
      } else if (r < 65) {
        hs = HardState{++term, static_cast<NodeId>(rng.below(3))};
        st->save(R(hs, {}));
      } else if (r < 75 && !log.empty()) {  // local snapshot at a random applied index
        const Index idx = snap.meta.index + 1 + rng.below(log.size());
        Snapshot s;
        s.meta = {idx, log[idx - snap.meta.index - 1].term};
        s.data = "state" + std::to_string(idx);
        std::vector<Entry> rest(log.begin() + static_cast<std::ptrdiff_t>(idx - snap.meta.index), log.end());
        st->save_local_snapshot(s, hs, rest);
        snap = s;
        log = rest;
      } else if (r < 90) {  // an append whose fsync never completed, then power loss
        const std::size_t before = fs.total_bytes();
        (void)before;
        st->save_unsynced(R(std::nullopt, {E(last() + 1, term, "unsynced")}));
        fs.crash(rng);
        st = std::make_unique<GroupStorage>(fs, "g");
        const Recovered rec = st->recover();
        const bool exact = rec.snap.meta == snap.meta && same(rec.entries, log) && rec.hs == hs;
        std::vector<Entry> plus = log;
        plus.push_back(E(last() + 1, term, "unsynced"));
        const bool survived = rec.snap.meta == snap.meta && same(rec.entries, plus);
        if (!exact && !survived) {
          std::printf("    seed %llu step %d: recovered %zu entries, expected %zu\n", static_cast<unsigned long long>(seed), step, rec.entries.size(), log.size());
          CHECK(false);
          return;
        }
        if (survived) log = plus;  // it happened to reach the disk in full
      } else {  // clean restart
        st = std::make_unique<GroupStorage>(fs, "g");
        const Recovered rec = st->recover();
        if (!(rec.snap.meta == snap.meta && same(rec.entries, log) && rec.hs == hs)) {
          std::printf("    seed %llu step %d: clean restart lost data\n", static_cast<unsigned long long>(seed), step);
          CHECK(false);
          return;
        }
      }
    }
  }
}
