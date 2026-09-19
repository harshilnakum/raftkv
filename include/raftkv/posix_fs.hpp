// Real files: append + fdatasync, atomic rename with a directory fsync.
#pragma once

#include <string>

#include "raftkv/fs.hpp"

namespace raftkv {

class PosixFs : public FileSystem {
 public:
  explicit PosixFs(std::string dir);  // creates the directory if needed
  bool exists(const std::string& name) override;
  std::unique_ptr<File> open(const std::string& name) override;
  void rename(const std::string& from, const std::string& to) override;
  void remove(const std::string& name) override;

 private:
  std::string path(const std::string& name) const { return dir_ + "/" + name; }
  void sync_dir();
  std::string dir_;
};

}  // namespace raftkv
