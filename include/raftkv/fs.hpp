// Minimal file-system abstraction so the WAL runs unchanged on real disks and on the simulator's crashable disk.
#pragma once

#include <memory>
#include <string>
#include <vector>

namespace raftkv {

class File {
 public:
  virtual ~File() = default;
  virtual void append(const std::string& bytes) = 0;
  virtual void sync() = 0;                 // make everything appended so far durable
  virtual std::string read_all() = 0;      // current contents (including not-yet-durable bytes)
  virtual void truncate(std::size_t n) = 0;  // keep only the first n bytes (then sync() to make it durable)
  virtual std::size_t size() = 0;
};

class FileSystem {
 public:
  virtual ~FileSystem() = default;
  virtual bool exists(const std::string& name) = 0;
  // Opens (creating if needed) a file for appending.
  virtual std::unique_ptr<File> open(const std::string& name) = 0;
  // Atomically replaces `to` with `from`; durable when it returns (real impl: rename + fsync of the directory).
  virtual void rename(const std::string& from, const std::string& to) = 0;
  virtual void remove(const std::string& name) = 0;
};

}  // namespace raftkv
