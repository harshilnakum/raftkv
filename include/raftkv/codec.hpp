// Tiny deterministic binary codec (little-endian fixed-width integers, length-prefixed strings).
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

namespace raftkv {

class Writer {
 public:
  void u8(std::uint8_t v) { s_.push_back(static_cast<char>(v)); }
  void u32(std::uint32_t v) {
    for (int i = 0; i < 4; ++i) s_.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
  void u64(std::uint64_t v) {
    for (int i = 0; i < 8; ++i) s_.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
  void str(const std::string& v) {
    u32(static_cast<std::uint32_t>(v.size()));
    s_ += v;
  }
  std::string& data() { return s_; }
  std::string take() { return std::move(s_); }

 private:
  std::string s_;
};

struct DecodeError : std::runtime_error {
  using std::runtime_error::runtime_error;
};

class Reader {
 public:
  Reader(const char* p, std::size_t n) : p_(p), end_(p + n) {}
  explicit Reader(const std::string& s) : Reader(s.data(), s.size()) {}

  std::uint8_t u8() {
    need(1);
    return static_cast<std::uint8_t>(*p_++);
  }
  std::uint32_t u32() {
    need(4);
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(p_[i])) << (8 * i);
    p_ += 4;
    return v;
  }
  std::uint64_t u64() {
    need(8);
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(p_[i])) << (8 * i);
    p_ += 8;
    return v;
  }
  std::string str() {
    const std::uint32_t n = u32();
    need(n);
    std::string s(p_, n);
    p_ += n;
    return s;
  }
  bool done() const { return p_ == end_; }
  std::size_t remaining() const { return static_cast<std::size_t>(end_ - p_); }

 private:
  void need(std::size_t n) const {
    if (static_cast<std::size_t>(end_ - p_) < n) throw DecodeError("truncated input");
  }
  const char* p_;
  const char* end_;
};

// CRC-32C (Castagnoli), bitwise; the WAL is not CPU bound.
inline std::uint32_t crc32c(const char* data, std::size_t n, std::uint32_t crc = 0) {
  crc = ~crc;
  for (std::size_t i = 0; i < n; ++i) {
    crc ^= static_cast<std::uint8_t>(data[i]);
    for (int k = 0; k < 8; ++k) crc = (crc >> 1) ^ (0x82F63B78u & (0u - (crc & 1u)));
  }
  return ~crc;
}

}  // namespace raftkv
