// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "base/phmap/btree.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/pk_index_publish_limiter.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/table.h"
#include "storage/storage_engine.h"

namespace starrocks {

class Cache;
class WritableFile;
class PersistentIndexSstablePB;

namespace sstable {
class TableBuilder;
class FilterPolicy;
} // namespace sstable

namespace lake {
// <version, IndexValue>
using IndexValueWithVer = std::pair<int64_t, IndexValue>;
class PersistentIndexBlockCache;

struct SstReadStateMemoryContext : public std::enable_shared_from_this<SstReadStateMemoryContext> {
    explicit SstReadStateMemoryContext(MemTracker* parent) : tracker(-1, "lake_pk_index_sst_read_state", parent) {}
    class Reservation {
    public:
        explicit Reservation(std::shared_ptr<SstReadStateMemoryContext> context,
                             std::function<void(size_t)> reclaim_self = {})
                : _context(std::move(context)), _reclaim_self(std::move(reclaim_self)) {}
        ~Reservation();
        Status add(size_t bytes, bool wait);
        void shrink_to(size_t bytes);
        size_t bytes() const { return _bytes; }

    private:
        std::shared_ptr<SstReadStateMemoryContext> _context;
        std::function<void(size_t)> _reclaim_self;
        size_t _bytes = 0;
    };
    MemTracker tracker;
    PkIndexPublishLimiter limiter;
    // Try-lock eviction only; called without any cache lock before retrying a byte reservation.
    std::function<void(size_t)> reclaim;
    std::mutex memory_mutex;
    std::condition_variable memory_cv;
};

// Drop the local cache copy of `path` so subsequent reads go to remote storage.
// Gated by config::lake_clear_corrupted_cache_data; no-op outside shared-data mode.
Status drop_corrupted_sstable_cache(const std::string& path);

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb, Cache* cache,
                bool need_filter = true, DelVectorPtr delvec = nullptr, const TabletMetadataPtr& metadata = nullptr,
                TabletManager* tablet_mgr = nullptr,
                const std::shared_ptr<SstReadStateMemoryContext>& memory_context = nullptr,
                bool defer_read_state = false, std::function<void(size_t)> reclaim_self = {});

    static Status build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                WritableFile* wf, uint64_t* filesz, PersistentIndexSstableRangePB* range_pb);

    // Build an sstable that contains only tombstone entries (NullIndexValue) for the given keys, all at
    // |version|. Used to apply a large pure-delete without accumulating tombstones in the memtable and
    // triggering additional flushes. |keys| MUST be sorted ascending (bytewise) and deduplicated, as required
    // by TableBuilder. Each entry is encoded exactly like a memtable-flushed tombstone
    // (rssid == rowid == UINT32_MAX), so reads and compaction treat it identically.
    static Status build_tombstone_sstable(const Slice* sorted_keys, size_t n, int64_t version, WritableFile* wf,
                                          uint64_t* filesz, PersistentIndexSstableRangePB* range_pb);

    // multi_get can get multi keys at onces
    // |keys| : Address point to first element of key array.
    // |key_indexes| : the index of key array that we actually want to get.
    // |version| : when < 0, means we want the latest version.
    // |values| : result array of get, should have some count as keys.
    // |found_key_indexes| : the index of key array that we found, it should be the subset of key_indexes_info
    Status multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version, IndexValue* values,
                     KeyIndexSet* found_key_indexes) const;

    sstable::Iterator* new_iterator(const sstable::ReadOptions& options);

    const PersistentIndexSstablePB& sstable_pb() const { return _sstable_pb; }

    // Full path of the underlying sstable file. Only valid after a successful init().
    std::string filename() const { return _filename; }

    // Release the SST's index/filter blocks and file handle, preserving metadata and the delete vector.
    // Returns bytes actually freed, or zero if a reader/iterator pins the read state or an open is in
    // progress. Subsequent reads reopen the file on demand; concurrent opens share one read state.
    size_t evict_read_state() const;

    size_t metadata_memory_usage() const;

    // Caller holds the tablet index lock; used only between joined index operations.
    void set_read_state_cacheable(bool cacheable, const std::shared_ptr<SstReadStateMemoryContext>& context) const;
    void set_read_state_memory_context(const std::shared_ptr<SstReadStateMemoryContext>& context) const {
        std::lock_guard<std::mutex> lock(_read_state_mutex);
        _memory_context = context;
    }
    // Owner holds its tablet lock and has joined readers before changing this callback.
    void set_read_state_reclaimer(const std::function<void(size_t)>& reclaimer) const {
        std::lock_guard<std::mutex> lock(_read_state_mutex);
        _reclaim_self = reclaimer;
    }
    Status reopen_read_state() const;

    // Last known full read-state size, retained after eviction for cache share calculation.
    size_t read_state_memory_demand() const { return _read_state_memory_demand.load(); }

    // Resident bytes of the index block and bloom filter; memory_usage() is their sum.
    size_t index_block_memory_usage() const;
    size_t filter_memory_usage() const;
    size_t memory_usage() const;
    // Resident table and read-state object bytes reserved directly on update, excluded from cache tracker charge.
    size_t directly_accounted_memory_usage() const;
    size_t reserved_read_state_overhead() const;
    struct ReadStateMemoryUsage {
        size_t index = 0;
        size_t filter = 0;
        size_t direct = 0;
        size_t objects = 0;
    };
    ReadStateMemoryUsage read_state_memory_usage() const;

    // Bytes of the delete vector attached to this sstable, if any. Not part of memory_usage().
    size_t delvec_memory_usage() const;

    // Sample keys from the table for parallel compaction task splitting.
    Status sample_keys(std::vector<std::string>* keys, size_t sample_interval_bytes) const;

    // Sample at most |max_samples| actual data keys taken from [seek_key, stop_key), rather than
    // index-block separator keys. Separators may be shortened by FindShortestSeparator and are suitable
    // as opaque seek boundaries, but are not guaranteed to decode as complete primary keys. Tablet split
    // uses this API because its boundaries must be persisted as PK tuples, and it samples within the
    // splitting tablet's own range because an SST is shared by every tablet an earlier split produced.
    Status sample_data_keys(std::vector<std::string>* keys, const Slice& seek_key, const Slice& stop_key,
                            size_t max_samples) const;

    // `_delvec` should only be modified in `init()` via publish version thread
    // which is thread-safe. And after that, it should be immutable.
    DelVectorPtr delvec() const { return _delvec; }

    void set_fileset_id(const UniqueId& fileset_id) {
        _sstable_pb.mutable_fileset_id()->CopyFrom(fileset_id.to_proto());
    }

    static StatusOr<PersistentIndexSstableUniquePtr> new_sstable(
            const PersistentIndexSstablePB& sstable_pb, const std::string& location, Cache* cache,
            bool need_filter = true, const DelVectorPtr& delvec = nullptr, const TabletMetadataPtr& metadata = nullptr,
            TabletManager* tablet_mgr = nullptr,
            const std::shared_ptr<SstReadStateMemoryContext>& memory_context = nullptr, bool defer_read_state = false,
            std::function<void(size_t)> reclaim_self = {});

private:
    struct ReadState;
    StatusOr<std::unique_ptr<RandomAccessFile>> _open_file() const;
    StatusOr<std::shared_ptr<ReadState>> _open_read_state(std::unique_ptr<RandomAccessFile> rf,
                                                          const std::shared_ptr<SstReadStateMemoryContext>& context,
                                                          const std::function<void(size_t)>& reclaim_self) const;
    StatusOr<std::shared_ptr<ReadState>> _get_read_state() const;

    // Readers keep a shared_ptr until their operation (or iterator) finishes. Eviction skips pinned
    // state, so its return value never counts memory that a reader still owns.
    mutable std::atomic<size_t> _read_state_memory_demand{0};
    mutable std::mutex _read_state_mutex;
    mutable std::condition_variable _read_state_cv;
    mutable bool _read_state_opening = false;
    mutable std::function<void(size_t)> _reclaim_self;
    mutable std::shared_ptr<ReadState> _read_state;
    mutable std::weak_ptr<ReadState> _transient_read_state;
    mutable std::shared_ptr<SstReadStateMemoryContext> _memory_context;
    mutable bool _read_state_cacheable = true;
    std::string _filename;
    Cache* _cache = nullptr;
    bool _need_filter = true;
    uint64_t _block_cache_id = 0;
    PersistentIndexSstablePB _sstable_pb;
    DelVectorPtr _delvec;
};

class PersistentIndexSstableStreamBuilder {
public:
    explicit PersistentIndexSstableStreamBuilder(std::unique_ptr<WritableFile> wf, std::string encryption_meta);

    Status add(const Slice& key);
    Status finish(uint64_t* file_size = nullptr);

    uint64_t num_entries() const;
    FileInfo file_info() const;
    std::string file_path() const { return _wf->filename(); }
    std::pair<Slice, Slice> key_range() const;

private:
    std::unique_ptr<sstable::TableBuilder> _table_builder;
    std::unique_ptr<sstable::FilterPolicy> _filter_policy;
    std::unique_ptr<WritableFile> _wf;
    bool _finished{false};
    std::string _encryption_meta;
    uint32_t _sst_rowid = 0;
};

} // namespace lake
} // namespace starrocks
