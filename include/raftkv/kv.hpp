// The replicated state machine: an ordered-by-key string map with exactly-once client sessions.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>

#include "raftkv/codec.hpp"

namespace raftkv {

enum class OpType : std::uint8_t { Put = 1, Append = 2, Cas = 3 };

struct Command {
  OpType op = OpType::Put;
  std::uint64_t client = 0;  // session id: one outstanding request per client
  std::uint64_t seq = 0;     // strictly increasing per client; a retry re-sends the SAME seq
  std::string key;
  std::string value;     // Put/Append: the value; Cas: the new value
  std::string expected;  // Cas only

  std::string encode() const {
    Writer w;
    w.u8(static_cast<std::uint8_t>(op));
    w.u64(client);
    w.u64(seq);
    w.str(key);
    w.str(value);
    w.str(expected);
    return w.take();
  }
  static Command decode(const std::string& s) {
    Reader r(s);
    Command c;
    c.op = static_cast<OpType>(r.u8());
    c.client = r.u64();
    c.seq = r.u64();
    c.key = r.str();
    c.value = r.str();
    c.expected = r.str();
    return c;
  }
};

struct ApplyResult {
  bool ok = true;      // Cas: whether the swap happened. Put/Append: always true.
  std::string value;   // Cas: the value observed before the operation. Put/Append: the resulting value.
  bool stale = false;  // the request's seq is older than the client's latest: it was already answered
};

class KvStateMachine {
 public:
  // Applies a committed command exactly once per (client, seq); a duplicate returns the cached result.
  ApplyResult apply(const Command& c) {
    Session& s = sessions_[c.client];
    if (c.seq == s.seq && s.seq != 0) return s.result;  // retry of the last request
    if (c.seq < s.seq) {
      ApplyResult r;
      r.stale = true;
      return r;
    }
    ApplyResult r;
    std::string& cur = data_[c.key];
    switch (c.op) {
      case OpType::Put:
        cur = c.value;
        r.value = cur;
        break;
      case OpType::Append:
        cur += c.value;
        r.value = cur;
        break;
      case OpType::Cas:
        r.value = cur;
        if (cur == c.expected) {
          cur = c.value;
        } else {
          r.ok = false;
        }
        break;
    }
    s.seq = c.seq;
    s.result = r;
    return r;
  }

  // Linearizable-read helper: the value applied so far (empty if the key was never written).
  std::string get(const std::string& key) const {
    auto it = data_.find(key);
    return it == data_.end() ? std::string() : it->second;
  }

  std::size_t size() const { return data_.size(); }

  std::string snapshot() const {
    Writer w;
    w.u64(data_.size());
    for (const auto& [k, v] : sorted_data()) {
      w.str(k);
      w.str(v);
    }
    std::map<std::uint64_t, Session> ordered(sessions_.begin(), sessions_.end());
    w.u64(ordered.size());
    for (const auto& [id, s] : ordered) {
      w.u64(id);
      w.u64(s.seq);
      w.u8(s.result.ok ? 1 : 0);
      w.str(s.result.value);
    }
    return w.take();
  }

  void restore(const std::string& bytes) {
    data_.clear();
    sessions_.clear();
    if (bytes.empty()) return;
    Reader r(bytes);
    const std::uint64_t n = r.u64();
    for (std::uint64_t i = 0; i < n; ++i) {
      std::string k = r.str();
      data_[std::move(k)] = r.str();
    }
    const std::uint64_t m = r.u64();
    for (std::uint64_t i = 0; i < m; ++i) {
      const std::uint64_t id = r.u64();
      Session s;
      s.seq = r.u64();
      s.result.ok = r.u8() != 0;
      s.result.value = r.str();
      sessions_[id] = std::move(s);
    }
  }

  // Order-independent-of-history digest of the full state (data + sessions), for replica comparison.
  std::uint64_t digest() const { return fnv(snapshot()); }

 private:
  struct Session {
    std::uint64_t seq = 0;
    ApplyResult result;
  };
  std::map<std::string, std::string> sorted_data() const { return {data_.begin(), data_.end()}; }
  static std::uint64_t fnv(const std::string& s) {
    std::uint64_t h = 0xCBF29CE484222325ull;
    for (char c : s) {
      const auto ch = static_cast<unsigned char>(c);
      h ^= ch;
      h *= 0x100000001B3ull;
    }
    return h;
  }
  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<std::uint64_t, Session> sessions_;
};

}  // namespace raftkv
