// Durable storage for ONE Raft group on one node: a write-ahead log (WAL) plus a snapshot file.
//
// WAL: a sequence of records [u32 body_len][u32 crc32c(body)][body], body = [u8 type][payload]:
//   HardState(term, vote) | Entries(first index, entries...) | SnapshotMarker(index, term)
// Semantics on replay: an Entries record at index i first discards every stored entry with index >= i (this is how a
// follower's conflicting suffix is overwritten); a SnapshotMarker resets the log so it starts right after `index`.
// A torn or corrupt tail (power loss mid-write) is detected by the CRC/length check and cut off.
//
// The caller must call save() and get a return BEFORE sending the messages of the same Ready (the sync inside is what
// makes Raft safe).
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "raftkv/fs.hpp"
#include "raftkv/raft.hpp"

namespace raftkv {

struct Recovered {
  HardState hs;
  Snapshot snap;
  std::vector<Entry> entries;
  bool fresh = true;  // nothing was on disk
  std::size_t repaired_bytes = 0;  // bytes of torn/corrupt tail that were cut off
};

class StorageError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class GroupStorage {
 public:
  GroupStorage(FileSystem& fs, std::string name);

  // Must be called once, before save(). Repairs a torn WAL tail. Throws StorageError on real corruption.
  Recovered recover();

  // Persists a Ready's snapshot / hard state / entries and syncs once.
  void save(const Ready& rd);
  // Test hook: same as save() but returns before the sync (simulates a crash before fsync completes).
  void save_unsynced(const Ready& rd);

  // The state machine snapshotted at snap.meta.index: durably replace the snapshot file and rewrite the WAL so it
  // holds only hs + the entries after the snapshot.
  void save_local_snapshot(const Snapshot& snap, const HardState& hs, const std::vector<Entry>& remaining);

  // Test hook: write only the snapshot file (simulates a crash between the snapshot and the WAL rewrite).
  void save_snapshot_file_only(const Snapshot& snap) { write_snapshot_file(snap); }

  std::size_t wal_size() { return wal_ ? wal_->size() : 0; }

 private:
  void append_records(const Ready& rd);
  static std::string record(std::uint8_t type, const std::string& payload);
  void write_snapshot_file(const Snapshot& snap);
  // Atomically replaces the WAL with: hard state, snapshot marker, remaining entries.
  void rewrite_wal(const HardState& hs, const SnapshotMeta& meta, const std::vector<Entry>& entries);
  bool read_snapshot_file(Snapshot& out);

  FileSystem& fs_;
  std::string wal_name_, snap_name_;
  std::unique_ptr<File> wal_;
};

}  // namespace raftkv
