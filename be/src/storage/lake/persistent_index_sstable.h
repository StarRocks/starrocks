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

#include <memory>
#include <string>
#include <vector>

#include "base/phmap/btree.h"
#include "gen_cpp/lake_types.pb.h"
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

// Drop the local cache copy of `path` so subsequent reads go to remote storage.
// Gated by config::lake_clear_corrupted_cache_data; no-op outside shared-data mode.
Status drop_corrupted_sstable_cache(const std::string& path);

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb, Cache* cache,
                bool need_filter = true, DelVectorPtr delvec = nullptr, const TabletMetadataPtr& metadata = nullptr,
                TabletManager* tablet_mgr = nullptr);

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

    sstable::Iterator* new_iterator(const sstable::ReadOptions& options) { return _sst->NewIterator(options); }

    const PersistentIndexSstablePB& sstable_pb() const { return _sstable_pb; }

    // Full path of the underlying sstable file. Only valid after a successful init().
    std::string filename() const { return _rf->filename(); }

    size_t memory_usage() const;

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

    static StatusOr<PersistentIndexSstableUniquePtr> new_sstable(const PersistentIndexSstablePB& sstable_pb,
                                                                 const std::string& location, Cache* cache,
                                                                 bool need_filter = true,
                                                                 const DelVectorPtr& delvec = nullptr,
                                                                 const TabletMetadataPtr& metadata = nullptr,
                                                                 TabletManager* tablet_mgr = nullptr);

private:
    std::unique_ptr<sstable::Table> _sst{nullptr};
    std::unique_ptr<sstable::FilterPolicy> _filter_policy{nullptr};
    std::unique_ptr<RandomAccessFile> _rf{nullptr};
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
