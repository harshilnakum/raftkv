// In-memory, crashable disk for the deterministic simulator.
// Data appended but not sync()ed is lost on crash, or only partly survives ("torn write"), possibly with damaged bytes.
#pragma once

#include <functional>
#include <map>
#include <memory>

#include "raftkv/fs.hpp"
#include "raftkv/rng.hpp"

namespace raftkv {

// Thrown by a simulated fsync when the simulator decides the machine loses power right before it completes.
struct SimCrash {};

class SimFs : public FileSystem {
 public:
  // If set and it returns true, the next sync() throws SimCrash BEFORE making data durable (power loss mid-write).
  std::function<bool()> crash_on_sync;

  struct Data {
    std::string bytes;
    std::size_t durable = 0;
  };

  bool exists(const std::string& name) override { return files_.count(name) != 0; }

  std::unique_ptr<File> open(const std::string& name) override {
    auto& d = files_[name];
    if (!d) d = std::make_shared<Data>();
    return std::make_unique<Handle>(d, this);
  }

  void rename(const std::string& from, const std::string& to) override {
    auto it = files_.find(from);
    if (it == files_.end()) return;
    files_[to] = it->second;
    files_.erase(from);
  }

  void remove(const std::string& name) override { files_.erase(name); }

  // Power loss. Each file keeps its durable bytes; of the unsynced tail it keeps nothing, or a random prefix
  // (a torn write), and a kept tail may have its last byte damaged.
  void crash(Rng& rng, double keep_tail_prob = 0.5, double damage_prob = 0.3) {
    for (auto& [name, d] : files_) {
      if (d->bytes.size() > d->durable) {
        std::size_t keep = 0;
        if (rng.chance(keep_tail_prob)) keep = static_cast<std::size_t>(rng.below(d->bytes.size() - d->durable + 1));
        d->bytes.resize(d->durable + keep);
        if (keep > 0 && rng.chance(damage_prob)) d->bytes.back() = static_cast<char>(d->bytes.back() ^ 0x5A);
      }
      d->durable = d->bytes.size();
    }
  }

  std::size_t total_bytes() const {
    std::size_t n = 0;
    for (auto& [name, d] : files_) n += d->bytes.size();
    return n;
  }

 private:
  class Handle : public File {
   public:
    Handle(std::shared_ptr<Data> d, SimFs* fs) : d_(std::move(d)), fs_(fs) {}
    void append(const std::string& b) override { d_->bytes += b; }
    void sync() override {
      if (fs_->crash_on_sync && fs_->crash_on_sync()) throw SimCrash{};
      d_->durable = d_->bytes.size();
    }
    std::string read_all() override { return d_->bytes; }
    void truncate(std::size_t n) override {
      if (n < d_->bytes.size()) d_->bytes.resize(n);
      if (d_->durable > n) d_->durable = n;
    }
    std::size_t size() override { return d_->bytes.size(); }

   private:
    std::shared_ptr<Data> d_;
    SimFs* fs_;
  };

  std::map<std::string, std::shared_ptr<Data>> files_;
};

}  // namespace raftkv
