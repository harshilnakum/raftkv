#include "mini_test.hpp"
#include "raftkv/kv.hpp"

using namespace raftkv;

namespace {
Command C(OpType op, std::uint64_t client, std::uint64_t seq, const std::string& k, const std::string& v, const std::string& exp = "") {
  Command c;
  c.op = op;
  c.client = client;
  c.seq = seq;
  c.key = k;
  c.value = v;
  c.expected = exp;
  return c;
}
}  // namespace

TEST(kv_command_codec_round_trips) {
  const Command c = C(OpType::Cas, 7, 9, "key", "new", "old");
  const Command d = Command::decode(c.encode());
  CHECK(d.op == OpType::Cas && d.client == 7 && d.seq == 9 && d.key == "key" && d.value == "new" && d.expected == "old");
  bool threw = false;
  try {
    Command::decode(c.encode().substr(0, 5));
  } catch (const DecodeError&) {
    threw = true;
  }
  CHECK(threw);
}

TEST(kv_put_append_cas_semantics) {
  KvStateMachine sm;
  CHECK(sm.apply(C(OpType::Put, 1, 1, "k", "a")).ok);
  CHECK_EQ(sm.get("k"), std::string("a"));
  CHECK_EQ(sm.apply(C(OpType::Append, 1, 2, "k", "b")).value, std::string("ab"));
  auto miss = sm.apply(C(OpType::Cas, 1, 3, "k", "z", "WRONG"));
  CHECK(!miss.ok);
  CHECK_EQ(miss.value, std::string("ab"));
  CHECK_EQ(sm.get("k"), std::string("ab"));
  auto hit = sm.apply(C(OpType::Cas, 1, 4, "k", "z", "ab"));
  CHECK(hit.ok);
  CHECK_EQ(sm.get("k"), std::string("z"));
  CHECK_EQ(sm.get("never-written"), std::string(""));
}

TEST(kv_a_retried_request_executes_exactly_once) {
  KvStateMachine sm;
  sm.apply(C(OpType::Append, 5, 1, "k", "x"));
  const ApplyResult again = sm.apply(C(OpType::Append, 5, 1, "k", "x"));  // duplicate delivery of the same request
  CHECK_EQ(sm.get("k"), std::string("x"));
  CHECK_EQ(again.value, std::string("x"));
  CHECK(!again.stale);
  sm.apply(C(OpType::Append, 5, 2, "k", "y"));
  const ApplyResult old = sm.apply(C(OpType::Append, 5, 1, "k", "x"));  // a very late duplicate of an older request
  CHECK(old.stale);
  CHECK_EQ(sm.get("k"), std::string("xy"));
}

TEST(kv_snapshot_restores_data_and_sessions) {
  KvStateMachine a;
  a.apply(C(OpType::Put, 1, 1, "a", "1"));
  a.apply(C(OpType::Append, 2, 1, "a", "2"));
  a.apply(C(OpType::Put, 3, 1, "b", "x"));
  KvStateMachine b;
  b.restore(a.snapshot());
  CHECK_EQ(b.digest(), a.digest());
  CHECK_EQ(b.get("a"), std::string("12"));
  // the restored replica still deduplicates
  b.apply(C(OpType::Append, 2, 1, "a", "2"));
  CHECK_EQ(b.get("a"), std::string("12"));
  KvStateMachine empty;
  empty.restore("");
  CHECK_EQ(empty.size(), 0u);
}

TEST(kv_snapshot_is_deterministic_regardless_of_insertion_order) {
  KvStateMachine a, b;
  for (int i = 0; i < 50; ++i) a.apply(C(OpType::Put, 1 + static_cast<std::uint64_t>(i), 1, "k" + std::to_string(i), "v"));
  for (int i = 49; i >= 0; --i) b.apply(C(OpType::Put, 1 + static_cast<std::uint64_t>(i), 1, "k" + std::to_string(i), "v"));
  CHECK_EQ(a.snapshot() == b.snapshot() ? 1 : 0, 1);
}
