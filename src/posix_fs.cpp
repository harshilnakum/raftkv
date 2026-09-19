#include "raftkv/posix_fs.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace raftkv {

namespace {
[[noreturn]] void die(const std::string& what) { throw std::runtime_error(what + ": " + std::strerror(errno)); }

class PosixFile : public File {
 public:
  PosixFile(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}
  ~PosixFile() override {
    if (fd_ >= 0) ::close(fd_);
  }
  void append(const std::string& b) override {
    std::size_t done = 0;
    while (done < b.size()) {
      const ssize_t w = ::write(fd_, b.data() + done, b.size() - done);
      if (w < 0) {
        if (errno == EINTR) continue;
        die("write " + path_);
      }
      done += static_cast<std::size_t>(w);
    }
  }
  void sync() override {
    if (::fdatasync(fd_) != 0) die("fdatasync " + path_);
  }
  std::string read_all() override {
    struct stat st;
    if (::fstat(fd_, &st) != 0) die("fstat " + path_);
    std::string out(static_cast<std::size_t>(st.st_size), '\0');
    std::size_t done = 0;
    while (done < out.size()) {
      const ssize_t r = ::pread(fd_, out.data() + done, out.size() - done, static_cast<off_t>(done));
      if (r < 0) {
        if (errno == EINTR) continue;
        die("pread " + path_);
      }
      if (r == 0) break;
      done += static_cast<std::size_t>(r);
    }
    out.resize(done);
    return out;
  }
  void truncate(std::size_t n) override {
    if (::ftruncate(fd_, static_cast<off_t>(n)) != 0) die("ftruncate " + path_);
  }
  std::size_t size() override {
    struct stat st;
    if (::fstat(fd_, &st) != 0) die("fstat " + path_);
    return static_cast<std::size_t>(st.st_size);
  }

 private:
  int fd_;
  std::string path_;
};
}  // namespace

PosixFs::PosixFs(std::string dir) : dir_(std::move(dir)) {
  std::string cur;
  for (std::size_t i = 0; i <= dir_.size(); ++i) {  // mkdir -p
    if (i == dir_.size() || dir_[i] == '/') {
      if (!cur.empty() && ::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) die("mkdir " + cur);
    }
    if (i < dir_.size()) cur.push_back(dir_[i]);
  }
}

bool PosixFs::exists(const std::string& name) {
  struct stat st;
  return ::stat(path(name).c_str(), &st) == 0;
}

std::unique_ptr<File> PosixFs::open(const std::string& name) {
  const int fd = ::open(path(name).c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (fd < 0) die("open " + name);
  return std::make_unique<PosixFile>(fd, path(name));
}

void PosixFs::rename(const std::string& from, const std::string& to) {
  if (::rename(path(from).c_str(), path(to).c_str()) != 0) die("rename " + from);
  sync_dir();
}

void PosixFs::remove(const std::string& name) {
  if (::unlink(path(name).c_str()) != 0 && errno != ENOENT) die("unlink " + name);
}

void PosixFs::sync_dir() {
  const int fd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) die("open dir " + dir_);
  if (::fsync(fd) != 0) {
    ::close(fd);
    die("fsync dir " + dir_);
  }
  ::close(fd);
}

}  // namespace raftkv
