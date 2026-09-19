#include "raftkv/wal.hpp"

#include "raftkv/codec.hpp"

namespace raftkv {

namespace {
enum : std::uint8_t { kHardState = 1, kEntries = 2, kSnapshotMarker = 3 };

void put_entry(Writer& w, const Entry& e) {
  w.u64(e.term);
  w.u64(e.index);
  w.u8(static_cast<std::uint8_t>(e.type));
  w.str(e.data);
}
Entry get_entry(Reader& r) {
  Entry e;
  e.term = r.u64();
  e.index = r.u64();
  e.type = static_cast<EntryType>(r.u8());
  e.data = r.str();
  return e;
}
}  // namespace

GroupStorage::GroupStorage(FileSystem& fs, std::string name)
    : fs_(fs), wal_name_(name + ".wal"), snap_name_(name + ".snap") {}

std::string GroupStorage::record(std::uint8_t type, const std::string& payload) {
  std::string body;
  body.push_back(static_cast<char>(type));
  body += payload;
  Writer w;
  w.u32(static_cast<std::uint32_t>(body.size()));
  w.u32(crc32c(body.data(), body.size()));
  std::string out = w.take();
  out += body;
  return out;
}

void GroupStorage::write_snapshot_file(const Snapshot& snap) {
  Writer body;
  body.u64(snap.meta.index);
  body.u64(snap.meta.term);
  body.str(snap.data);
  Writer w;
  w.u32(crc32c(body.data().data(), body.data().size()));
  std::string bytes = w.take();
  bytes += body.data();
  const std::string tmp = snap_name_ + ".tmp";
  fs_.remove(tmp);
  auto f = fs_.open(tmp);
  f->append(bytes);
  f->sync();  // contents durable BEFORE the rename makes them visible
  f.reset();
  fs_.rename(tmp, snap_name_);
}

bool GroupStorage::read_snapshot_file(Snapshot& out) {
  if (!fs_.exists(snap_name_)) return false;
  auto f = fs_.open(snap_name_);
  const std::string bytes = f->read_all();
  if (bytes.size() < 4) return false;
  Reader r(bytes);
  const std::uint32_t crc = r.u32();
  if (crc32c(bytes.data() + 4, bytes.size() - 4) != crc) return false;
  out.meta.index = r.u64();
  out.meta.term = r.u64();
  out.data = r.str();
  return true;
}

Recovered GroupStorage::recover() {
  Recovered rec;
  fs_.remove(snap_name_ + ".tmp");  // leftovers of an interrupted snapshot/WAL rewrite are never valid
  fs_.remove(wal_name_ + ".tmp");

  Snapshot file_snap;
  const bool have_snap_file = read_snapshot_file(file_snap);

  wal_ = fs_.open(wal_name_);
  const std::string bytes = wal_->read_all();
  Index snap_index = 0;
  Term snap_term = 0;
  std::size_t pos = 0;
  while (bytes.size() - pos >= 8) {
    Reader h(bytes.data() + pos, 8);
    const std::uint32_t len = h.u32();
    const std::uint32_t crc = h.u32();
    if (len == 0 || bytes.size() - pos - 8 < len) break;  // torn: header or body incomplete
    const char* body = bytes.data() + pos + 8;
    if (crc32c(body, len) != crc) break;  // corrupt
    try {
      Reader r(body + 1, len - 1);
      switch (static_cast<std::uint8_t>(body[0])) {
        case kHardState:
          rec.hs.term = r.u64();
          rec.hs.vote = r.u32();
          break;
        case kEntries: {
          const std::uint32_t n = r.u32();
          for (std::uint32_t i = 0; i < n; ++i) {
            Entry e = get_entry(r);
            const Index last = snap_index + rec.entries.size();
            if (e.index > last + 1) throw StorageError("WAL has a gap before index " + std::to_string(e.index));
            if (e.index <= snap_index) continue;
            rec.entries.resize(e.index - snap_index - 1);  // overwrite: drop this index and everything after it
            rec.entries.push_back(std::move(e));
          }
          break;
        }
        case kSnapshotMarker:
          snap_index = r.u64();
          snap_term = r.u64();
          rec.entries.clear();
          break;
        default:
          throw StorageError("unknown WAL record type");
      }
    } catch (const DecodeError&) {
      break;  // a record that passes the CRC but does not decode is treated like a torn tail
    }
    pos += 8 + len;
    rec.fresh = false;
  }
  if (pos < bytes.size()) {  // cut off the torn/corrupt tail so new records append to a clean log
    rec.repaired_bytes = bytes.size() - pos;
    wal_->truncate(pos);
    wal_->sync();
  }

  if (have_snap_file) {
    rec.fresh = false;
    if (snap_index > file_snap.meta.index) throw StorageError("WAL refers to a snapshot newer than the snapshot file");
    rec.snap = file_snap;
    if (file_snap.meta.index > snap_index) {  // crash between the snapshot file and the WAL rewrite
      std::vector<Entry> kept;
      for (Entry& e : rec.entries)
        if (e.index > file_snap.meta.index) kept.push_back(std::move(e));
      // entries before the snapshot are gone; what remains must follow it directly
      if (!kept.empty() && kept.front().index != file_snap.meta.index + 1)
        throw StorageError("log does not connect to the snapshot");
      rec.entries = std::move(kept);
      // The WAL still describes the older log. Appending to it would create a gap that makes the NEXT recovery fail,
      // so bring it in line with the snapshot now (found by the simulator: crash between snapshot file and WAL rewrite).
      rewrite_wal(rec.hs, file_snap.meta, rec.entries);
    }
  } else if (snap_index > 0) {
    throw StorageError("WAL refers to a snapshot but the snapshot file is missing");
  }
  (void)snap_term;
  return rec;
}

void GroupStorage::append_records(const Ready& rd) {
  if (rd.snapshot) {
    write_snapshot_file(*rd.snapshot);
    Writer w;
    w.u64(rd.snapshot->meta.index);
    w.u64(rd.snapshot->meta.term);
    wal_->append(record(kSnapshotMarker, w.take()));
  }
  if (rd.hard_state) {
    Writer w;
    w.u64(rd.hard_state->term);
    w.u32(rd.hard_state->vote);
    wal_->append(record(kHardState, w.take()));
  }
  if (!rd.entries.empty()) {
    Writer w;
    w.u32(static_cast<std::uint32_t>(rd.entries.size()));
    for (const Entry& e : rd.entries) put_entry(w, e);
    wal_->append(record(kEntries, w.take()));
  }
}

void GroupStorage::save(const Ready& rd) {
  if (!rd.snapshot && !rd.hard_state && rd.entries.empty()) return;
  append_records(rd);
  wal_->sync();
}

void GroupStorage::save_unsynced(const Ready& rd) { append_records(rd); }

void GroupStorage::rewrite_wal(const HardState& hs, const SnapshotMeta& meta, const std::vector<Entry>& entries) {
  std::string content;
  {
    Writer w;
    w.u64(hs.term);
    w.u32(hs.vote);
    content += record(kHardState, w.take());
  }
  {
    Writer w;
    w.u64(meta.index);
    w.u64(meta.term);
    content += record(kSnapshotMarker, w.take());
  }
  if (!entries.empty()) {
    Writer w;
    w.u32(static_cast<std::uint32_t>(entries.size()));
    for (const Entry& e : entries) put_entry(w, e);
    content += record(kEntries, w.take());
  }
  const std::string tmp = wal_name_ + ".tmp";
  fs_.remove(tmp);
  {
    auto f = fs_.open(tmp);
    f->append(content);
    f->sync();
  }
  wal_.reset();
  fs_.rename(tmp, wal_name_);
  wal_ = fs_.open(wal_name_);
}

void GroupStorage::save_local_snapshot(const Snapshot& snap, const HardState& hs, const std::vector<Entry>& remaining) {
  write_snapshot_file(snap);
  rewrite_wal(hs, snap.meta, remaining);
}

}  // namespace raftkv
