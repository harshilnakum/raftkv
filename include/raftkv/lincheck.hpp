// Linearizability checker for the KV register model (Wing & Gong search with memoisation, Porcupine-style).
//
// A history is a set of operations, each with a real-time call and return instant. The history is linearizable iff the
// operations can be ordered in a sequence that (1) respects real time (if A returned before B was called, A comes first)
// and (2) is legal for a sequential KV store. Keys are independent, so each key is checked separately.
//
// An operation with no return ("info": the client gave up, the outcome is unknown) may or may not have taken effect, at
// any point after its call: it is treated as never returning, and need not be linearized at all.
#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace raftkv::lin {

enum class Kind : std::uint8_t { Put, Append, Cas, Get };

inline constexpr std::uint64_t kNever = std::numeric_limits<std::uint64_t>::max();

struct Op {
  std::uint64_t process = 0;
  Kind kind = Kind::Get;
  std::string key;
  std::string arg;       // Put/Append: value; Cas: new value
  std::string expected;  // Cas: expected current value
  std::uint64_t call = 0;
  std::uint64_t ret = kNever;  // kNever = outcome unknown
  // Observed result (meaningful only if ret != kNever):
  bool ok = true;         // Cas: swap succeeded
  std::string value;      // Get: value read. Cas: value observed before. Append/Put: resulting value.
};

enum class Verdict { Linearizable, Violation, Unknown };

struct Result {
  Verdict verdict = Verdict::Linearizable;
  std::string bad_key;            // set on Violation/Unknown: the key whose history could not be explained
  std::vector<Op> bad_history;    // that key's operations, sorted by call time
  std::uint64_t states_explored = 0;
};

// max_states bounds the search per key; exceeding it yields Verdict::Unknown (never a false Violation).
Result check(const std::vector<Op>& history, std::uint64_t max_states = 20'000'000);

std::string describe(const Op& op);

}  // namespace raftkv::lin
