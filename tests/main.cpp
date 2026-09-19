#include <cstring>

#include "mini_test.hpp"

int main(int argc, char** argv) {
  const char* filter = argc > 1 ? argv[1] : nullptr;
  int ran = 0, failed_tests = 0;
  for (const auto& t : mt::registry()) {
    if (filter != nullptr && std::strstr(t.name, filter) == nullptr) continue;
    const int before = mt::failures();
    std::printf("[ RUN  ] %s\n", t.name);
    std::fflush(stdout);
    t.fn();
    ++ran;
    if (mt::failures() != before) {
      ++failed_tests;
      std::printf("[ FAIL ] %s\n", t.name);
    } else {
      std::printf("[  OK  ] %s\n", t.name);
    }
  }
  std::printf("\n%d tests run, %d failed, %d failed checks\n", ran, failed_tests, mt::failures());
  return mt::failures() == 0 ? 0 : 1;
}
