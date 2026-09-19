// Minimal, dependency-free test harness (so the project builds with nothing but a compiler and CMake).
#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <vector>

namespace mt {

struct Test {
  const char* name;
  void (*fn)();
};
inline std::vector<Test>& registry() {
  static std::vector<Test> r;
  return r;
}
struct Registrar {
  Registrar(const char* n, void (*f)()) { registry().push_back({n, f}); }
};
inline int& failures() {
  static int f = 0;
  return f;
}

template <class T>
std::string show(const T& v) {
  if constexpr (std::is_arithmetic_v<T> || std::is_enum_v<T>) {
    return std::to_string(static_cast<long long>(v));
  } else if constexpr (std::is_convertible_v<T, std::string>) {
    return "\"" + std::string(v) + "\"";
  } else {
    return "?";
  }
}

}  // namespace mt

#define TEST(name)                                          \
  static void test_##name();                                \
  static ::mt::Registrar registrar_##name(#name, &test_##name); \
  static void test_##name()

#define CHECK(cond)                                                      \
  do {                                                                   \
    if (!(cond)) {                                                       \
      std::printf("    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
      ++::mt::failures();                                                \
    }                                                                    \
  } while (0)

#define CHECK_EQ(a, b)                                                                                     \
  do {                                                                                                     \
    const auto va_ = (a);                                                                                  \
    const auto vb_ = (b);                                                                                  \
    if (!(va_ == vb_)) {                                                                                   \
      std::printf("    FAIL %s:%d: %s == %s  (%s vs %s)\n", __FILE__, __LINE__, #a, #b,                   \
                  ::mt::show(va_).c_str(), ::mt::show(vb_).c_str());                                       \
      ++::mt::failures();                                                                                  \
    }                                                                                                      \
  } while (0)

#define REQUIRE(cond)                                                    \
  do {                                                                   \
    if (!(cond)) {                                                       \
      std::printf("    FAIL %s:%d: %s (fatal)\n", __FILE__, __LINE__, #cond); \
      ++::mt::failures();                                                \
      return;                                                            \
    }                                                                    \
  } while (0)
