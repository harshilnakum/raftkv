#include "mini_test.hpp"
#include "raftkv/lincheck.hpp"
#include "raftkv/rng.hpp"

using namespace raftkv;
using namespace raftkv::lin;

namespace {
Op put(std::uint64_t p, const std::string& k, const std::string& v, std::uint64_t c, std::uint64_t r) {
  Op o;
  o.process = p; o.kind = Kind::Put; o.key = k; o.arg = v; o.value = v; o.call = c; o.ret = r;
  return o;
}
Op app(std::uint64_t p, const std::string& k, const std::string& v, const std::string& result, std::uint64_t c, std::uint64_t r) {
  Op o;
  o.process = p; o.kind = Kind::Append; o.key = k; o.arg = v; o.value = result; o.call = c; o.ret = r;
  return o;
}
Op get(std::uint64_t p, const std::string& k, const std::string& v, std::uint64_t c, std::uint64_t r) {
  Op o;
  o.process = p; o.kind = Kind::Get; o.key = k; o.value = v; o.call = c; o.ret = r;
  return o;
}
Op cas(std::uint64_t p, const std::string& k, const std::string& exp, const std::string& nv, bool ok, const std::string& saw, std::uint64_t c, std::uint64_t r) {
  Op o;
  o.process = p; o.kind = Kind::Cas; o.key = k; o.expected = exp; o.arg = nv; o.ok = ok; o.value = saw; o.call = c; o.ret = r;
  return o;
}
Verdict verdict(const std::vector<Op>& h) { return check(h).verdict; }
}  // namespace

TEST(lin_accepts_a_sequential_history) {
  CHECK(verdict({put(1, "k", "a", 1, 2), get(1, "k", "a", 3, 4), put(1, "k", "b", 5, 6), get(2, "k", "b", 7, 8)}) == Verdict::Linearizable);
  CHECK(verdict({}) == Verdict::Linearizable);
  CHECK(verdict({get(1, "k", "", 1, 2)}) == Verdict::Linearizable);  // absent key reads as empty
}

TEST(lin_accepts_concurrent_operations_in_either_order) {
  // put(a) and put(b) overlap, so a later read may legitimately see either.
  CHECK(verdict({put(1, "k", "a", 1, 10), put(2, "k", "b", 2, 11), get(3, "k", "a", 12, 13)}) == Verdict::Linearizable);
  CHECK(verdict({put(1, "k", "a", 1, 10), put(2, "k", "b", 2, 11), get(3, "k", "b", 12, 13)}) == Verdict::Linearizable);
  // a read overlapping a write may see the old or the new value
  CHECK(verdict({put(1, "k", "a", 1, 2), put(2, "k", "b", 3, 10), get(3, "k", "a", 4, 5)}) == Verdict::Linearizable);
  CHECK(verdict({put(1, "k", "a", 1, 2), put(2, "k", "b", 3, 10), get(3, "k", "b", 4, 5)}) == Verdict::Linearizable);
}

TEST(lin_rejects_a_stale_read) {
  // the write of b finished before the read began, yet the read returns the older a
  CHECK(verdict({put(1, "k", "a", 1, 2), put(1, "k", "b", 3, 4), get(2, "k", "a", 5, 6)}) == Verdict::Violation);
}

TEST(lin_rejects_a_lost_write) {
  CHECK(verdict({put(1, "k", "a", 1, 2), get(2, "k", "", 3, 4)}) == Verdict::Violation);
}

TEST(lin_rejects_reads_that_go_back_in_time) {
  // two sequential reads by different processes observing b then a while a's write completed long before b's
  CHECK(verdict({put(1, "k", "a", 1, 2), put(2, "k", "b", 3, 4), get(3, "k", "b", 5, 6), get(4, "k", "a", 7, 8)}) == Verdict::Violation);
}

TEST(lin_rejects_a_value_nobody_wrote) {
  CHECK(verdict({put(1, "k", "a", 1, 2), get(2, "k", "zzz", 3, 4)}) == Verdict::Violation);
}

TEST(lin_unknown_outcome_operations_can_explain_later_reads_but_not_invent_values) {
  Op unknown = put(1, "k", "a", 1, kNever);
  CHECK(verdict({unknown, get(2, "k", "a", 5, 6)}) == Verdict::Linearizable);  // it did take effect
  CHECK(verdict({unknown, get(2, "k", "", 5, 6)}) == Verdict::Linearizable);   // it did not (yet)
  CHECK(verdict({unknown, get(2, "k", "b", 5, 6)}) == Verdict::Violation);     // nobody wrote b
  // an unknown write cannot take effect BEFORE it was called
  CHECK(verdict({get(2, "k", "a", 1, 2), put(1, "k", "a", 5, kNever)}) == Verdict::Violation);
}

TEST(lin_checks_compare_and_swap) {
  CHECK(verdict({put(1, "k", "a", 1, 2), cas(2, "k", "a", "b", true, "a", 3, 4), get(3, "k", "b", 5, 6)}) == Verdict::Linearizable);
  CHECK(verdict({put(1, "k", "a", 1, 2), cas(2, "k", "x", "b", false, "a", 3, 4), get(3, "k", "a", 5, 6)}) == Verdict::Linearizable);
  // claims the swap succeeded although the value was different
  CHECK(verdict({put(1, "k", "a", 1, 2), cas(2, "k", "x", "b", true, "a", 3, 4)}) == Verdict::Violation);
  // two concurrent CAS(a->b) and CAS(a->c): at most one can succeed
  CHECK(verdict({put(1, "k", "a", 1, 2), cas(2, "k", "a", "b", true, "a", 3, 9), cas(3, "k", "a", "c", true, "a", 3, 9)}) == Verdict::Violation);
  CHECK(verdict({put(1, "k", "a", 1, 2), cas(2, "k", "a", "b", true, "a", 3, 9), cas(3, "k", "a", "c", false, "b", 3, 9)}) == Verdict::Linearizable);
}

TEST(lin_checks_append_results_which_reveal_the_order) {
  CHECK(verdict({app(1, "k", "a", "a", 1, 2), app(2, "k", "b", "ab", 3, 4)}) == Verdict::Linearizable);
  CHECK(verdict({app(1, "k", "a", "a", 1, 2), app(2, "k", "b", "b", 3, 4)}) == Verdict::Violation);  // b lost a
  CHECK(verdict({app(1, "k", "a", "ba", 1, 9), app(2, "k", "b", "b", 1, 9)}) == Verdict::Linearizable);  // b first, then a
  CHECK(verdict({app(1, "k", "a", "ab", 1, 9), app(2, "k", "b", "ab", 1, 9)}) == Verdict::Violation);
}

TEST(lin_keys_are_independent) {
  CHECK(verdict({put(1, "x", "1", 1, 2), put(1, "y", "2", 3, 4), get(2, "x", "1", 5, 6), get(2, "y", "2", 7, 8)}) == Verdict::Linearizable);
  const Result r = check({put(1, "x", "1", 1, 2), put(1, "y", "2", 3, 4), get(2, "y", "", 5, 6)});
  CHECK(r.verdict == Verdict::Violation);
  CHECK_EQ(r.bad_key, std::string("y"));
}

// Property test: histories built FROM a sequential execution (each op gets a real-time interval around its
// linearization point) must be accepted; flipping one read to a value that never existed must be rejected.
TEST(lin_random_valid_histories_are_accepted_and_corrupted_ones_rejected) {
  for (std::uint64_t seed = 1; seed <= 300; ++seed) {
    Rng rng(seed);
    const int n = 20 + static_cast<int>(rng.below(60));
    const int keys = 1 + static_cast<int>(rng.below(3));
    std::vector<std::string> state(static_cast<std::size_t>(keys));
    std::vector<Op> h;
    std::size_t last_get = h.size();
    for (int i = 0; i < n; ++i) {
      const std::uint64_t point = 100 + static_cast<std::uint64_t>(i + 1) * 10;
      const std::size_t k = static_cast<std::size_t>(rng.below(static_cast<std::uint64_t>(keys)));
      const std::string key = "k" + std::to_string(k);
      Op o;
      o.process = static_cast<std::uint64_t>(i);
      o.key = key;
      o.call = point - rng.below(45);
      o.ret = point + rng.below(45);
      const std::uint64_t r = rng.below(100);
      if (r < 30) {
        o.kind = Kind::Put; o.arg = "v" + std::to_string(i); o.value = o.arg; state[k] = o.arg;
      } else if (r < 50) {
        o.kind = Kind::Append; o.arg = "." + std::to_string(i); state[k] += o.arg; o.value = state[k];
      } else if (r < 65) {
        o.kind = Kind::Cas; o.expected = rng.coin() ? state[k] : "nope"; o.arg = "c" + std::to_string(i);
        o.value = state[k]; o.ok = o.expected == state[k]; if (o.ok) state[k] = o.arg;
      } else {
        o.kind = Kind::Get; o.value = state[k]; last_get = h.size();
      }
      if (rng.below(15) == 0 && o.kind != Kind::Get) o.ret = kNever;  // client lost the reply
      h.push_back(o);
    }
    CHECK(verdict(h) == Verdict::Linearizable);
    if (last_get < h.size()) {
      auto bad = h;
      bad[last_get].value = "NEVER-WRITTEN";
      CHECK(verdict(bad) == Verdict::Violation);
    }
  }
}
