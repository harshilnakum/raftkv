#include "raftkv/lincheck.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <unordered_set>

namespace raftkv::lin {

namespace {

struct Entry {
  bool is_call = true;
  int op = 0;
  Entry* match = nullptr;  // call -> its return entry
  Entry* prev = nullptr;
  Entry* next = nullptr;
};

// Sequential model of one key. Returns false if `op` (with its observed result) cannot happen in `state`.
bool step(const std::string& state, const Op& op, std::string& next) {
  const bool completed = op.ret != kNever;
  switch (op.kind) {
    case Kind::Put:
      next = op.arg;
      return !completed || op.value == op.arg;
    case Kind::Append:
      next = state + op.arg;
      return !completed || op.value == next;
    case Kind::Cas: {
      const bool would = state == op.expected;
      if (completed && (op.ok != would || op.value != state)) return false;
      next = would ? op.arg : state;
      return true;
    }
    case Kind::Get:
      next = state;
      return !completed || op.value == state;
  }
  return false;
}

struct CacheKey {
  std::vector<std::uint64_t> bits;
  std::uint64_t state_hash;
  bool operator==(const CacheKey& o) const { return state_hash == o.state_hash && bits == o.bits; }
};
struct CacheHash {
  std::size_t operator()(const CacheKey& k) const {
    std::uint64_t h = k.state_hash;
    for (std::uint64_t w : k.bits) h = (h ^ w) * 0x9E3779B97F4A7C15ull + (h >> 29);
    return static_cast<std::size_t>(h);
  }
};

std::uint64_t hash_str(const std::string& s) {
  std::uint64_t h = 0xCBF29CE484222325ull;
  for (char ch : s) {
    h ^= static_cast<unsigned char>(ch);
    h *= 0x100000001B3ull;
  }
  return h ^ (s.size() * 0x9E3779B97F4A7C15ull);
}

// Returns Linearizable / Violation / Unknown for the operations of a single key.
Verdict check_key(const std::vector<Op>& ops, std::uint64_t max_states, std::uint64_t& explored) {
  const int n = static_cast<int>(ops.size());
  if (n == 0) return Verdict::Linearizable;

  // Build the time-ordered list of call/return events. At equal times calls come first (treated as concurrent).
  std::vector<Entry> store(static_cast<std::size_t>(2 * n));
  std::vector<Entry*> order;
  for (int i = 0; i < n; ++i) {
    Entry& c = store[static_cast<std::size_t>(2 * i)];
    Entry& r = store[static_cast<std::size_t>(2 * i + 1)];
    c.is_call = true;
    c.op = i;
    c.match = &r;
    r.is_call = false;
    r.op = i;
    order.push_back(&c);
    order.push_back(&r);
  }
  auto time_of = [&](const Entry* e) { return e->is_call ? ops[static_cast<std::size_t>(e->op)].call : ops[static_cast<std::size_t>(e->op)].ret; };
  std::stable_sort(order.begin(), order.end(), [&](const Entry* a, const Entry* b) {
    const std::uint64_t ta = time_of(a), tb = time_of(b);
    if (ta != tb) return ta < tb;
    if (a->is_call != b->is_call) return a->is_call;  // calls before returns
    return a->op < b->op;
  });
  Entry head;
  Entry* prev = &head;
  for (Entry* e : order) {
    prev->next = e;
    e->prev = prev;
    prev = e;
  }
  prev->next = nullptr;

  auto lift = [&](Entry* c) {
    Entry* r = c->match;
    c->prev->next = c->next;
    if (c->next) c->next->prev = c->prev;
    r->prev->next = r->next;
    if (r->next) r->next->prev = r->prev;
  };
  auto unlift = [&](Entry* c) {
    Entry* r = c->match;
    if (r->next) r->next->prev = r;
    r->prev->next = r;
    if (c->next) c->next->prev = c;
    c->prev->next = c;
  };

  struct Frame {
    Entry* entry;
    std::string state;
  };
  std::vector<Frame> stack;
  std::vector<std::uint64_t> bits(static_cast<std::size_t>((n + 63) / 64), 0);
  std::unordered_set<CacheKey, CacheHash> cache;
  std::string state;  // absent key == ""
  Entry* entry = head.next;
  while (head.next != nullptr) {
    if (entry->is_call) {
      const Op& op = ops[static_cast<std::size_t>(entry->op)];
      std::string next;
      if (step(state, op, next)) {
        auto nb = bits;
        nb[static_cast<std::size_t>(entry->op / 64)] |= 1ull << (entry->op % 64);
        if (cache.insert(CacheKey{nb, hash_str(next)}).second) {
          if (++explored > max_states) return Verdict::Unknown;
          stack.push_back({entry, state});
          state = std::move(next);
          bits = std::move(nb);
          lift(entry);
          entry = head.next;
          continue;
        }
      }
      entry = entry->next;
    } else {
      if (ops[static_cast<std::size_t>(entry->op)].ret == kNever)
        return Verdict::Linearizable;  // only never-returning ops remain: every completed op is linearized
      if (stack.empty()) return Verdict::Violation;
      Frame f = std::move(stack.back());
      stack.pop_back();
      entry = f.entry;
      state = std::move(f.state);
      bits[static_cast<std::size_t>(entry->op / 64)] &= ~(1ull << (entry->op % 64));
      unlift(entry);
      entry = entry->next;
    }
  }
  return Verdict::Linearizable;
}

}  // namespace

Result check(const std::vector<Op>& history, std::uint64_t max_states) {
  std::map<std::string, std::vector<Op>> by_key;
  for (const Op& op : history) {
    if (op.kind == Kind::Get && op.ret == kNever) continue;  // an unfinished read has no effect and no observation
    by_key[op.key].push_back(op);
  }
  Result res;
  for (auto& [key, ops] : by_key) {
    std::sort(ops.begin(), ops.end(), [](const Op& a, const Op& b) { return a.call < b.call; });
    const Verdict v = check_key(ops, max_states, res.states_explored);
    if (v != Verdict::Linearizable) {
      res.verdict = v;
      res.bad_key = key;
      res.bad_history = ops;
      return res;
    }
  }
  return res;
}

std::string describe(const Op& op) {
  static const char* names[] = {"put", "append", "cas", "get"};
  std::string s = "p" + std::to_string(op.process) + " " + names[static_cast<int>(op.kind)] + "(" + op.key;
  if (op.kind == Kind::Put || op.kind == Kind::Append) s += ", '" + op.arg + "'";
  if (op.kind == Kind::Cas) s += ", '" + op.expected + "'->'" + op.arg + "'";
  s += ") [" + std::to_string(op.call) + ", " + (op.ret == kNever ? std::string("?") : std::to_string(op.ret)) + "]";
  if (op.ret != kNever) {
    if (op.kind == Kind::Get) s += " => '" + op.value + "'";
    if (op.kind == Kind::Cas) s += std::string(" => ") + (op.ok ? "swapped" : "failed") + " (saw '" + op.value + "')";
    if (op.kind == Kind::Append) s += " => '" + op.value + "'";
  } else {
    s += " => unknown";
  }
  return s;
}

}  // namespace raftkv::lin
