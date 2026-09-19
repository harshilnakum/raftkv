// Deterministic PRNG (xoshiro256** seeded through splitmix64): same seed => same behaviour everywhere.
#pragma once

#include <cstdint>

namespace raftkv {

class Rng {
 public:
  explicit Rng(std::uint64_t seed = 1) noexcept { reseed(seed); }
  void reseed(std::uint64_t seed) noexcept {
    for (auto& w : s_) {
      seed += 0x9E3779B97F4A7C15ull;
      std::uint64_t z = seed;
      z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
      z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
      w = z ^ (z >> 31);
    }
  }
  std::uint64_t next() noexcept {
    const std::uint64_t result = rotl(s_[1] * 5, 7) * 9;
    const std::uint64_t t = s_[1] << 17;
    s_[2] ^= s_[0];
    s_[3] ^= s_[1];
    s_[1] ^= s_[2];
    s_[0] ^= s_[3];
    s_[2] ^= t;
    s_[3] = rotl(s_[3], 45);
    return result;
  }
  // Uniform in [0, n); n must be > 0.
  std::uint64_t below(std::uint64_t n) noexcept { return next() % n; }
  bool coin() noexcept { return (next() >> 63) != 0; }
  bool chance(double p) noexcept { return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0) < p; }

 private:
  static constexpr std::uint64_t rotl(std::uint64_t x, int k) noexcept { return (x << k) | (x >> (64 - k)); }
  std::uint64_t s_[4];
};

}  // namespace raftkv
