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
#include <memory>
#include <mutex>
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

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb, Cache* cache,
                bool need_filter = true, DelVectorPtr delvec = nullptr, const TabletMetadataPtr& metadata = nullptr,
                TabletManager* tablet_mgr = nullptr);

    static Status build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                WritableFile* wf, uint64_t* filesz, PersistentIndexSstableRangePB* range_pb);

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

    size_t memory_usage() const;

    // Sample keys from the table for parallel compaction task splitting.
    Status sample_keys(std::vector<std::string>* keys, size_t sample_interval_bytes) const;

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

    // Construct a PersistentIndexSstable in deferred-open state. The sstable's PB metadata
    // (filename, range, fileset_id, max_rss_rowid, encryption_meta) is populated immediately
    // so the containing fileset's per-key routing (`_sstable_map.upper_bound`) works without
    // any OSS reads. The actual `sstable::Table::Open` (footer + index + filter + meta-index
    // block reads) is deferred until the first call that needs it (currently `multi_get` via
    // `ensure_opened()`). The arguments captured here mirror `new_sstable` so the deferred
    // open path can be a drop-in for the eager one.
    static StatusOr<PersistentIndexSstableUniquePtr> new_sstable_lazy(const PersistentIndexSstablePB& sstable_pb,
                                                                      std::string location, Cache* cache,
                                                                      bool need_filter,
                                                                      const TabletMetadataPtr& metadata,
                                                                      TabletManager* tablet_mgr);

    // Trigger the deferred `Table::Open` if this sstable was constructed via
    // `new_sstable_lazy`. Idempotent and thread-safe: concurrent callers race on
    // `_lazy_open_mutex` and only the first one runs the open. After this returns OK the
    // sstable behaves identically to one constructed via the eager `new_sstable`.
    Status ensure_opened() const;

    bool is_opened() const { return _sst != nullptr; }

private:
    std::unique_ptr<sstable::Table> _sst{nullptr};
    std::unique_ptr<sstable::FilterPolicy> _filter_policy{nullptr};
    std::unique_ptr<RandomAccessFile> _rf{nullptr};
    PersistentIndexSstablePB _sstable_pb;
    DelVectorPtr _delvec;

    // Deferred-open state. When `_lazy_pending` is true on a const accessor path, the caller
    // must invoke `ensure_opened()` before touching `_sst` / `_delvec`. The mutex is mutable
    // because `ensure_opened()` is logically const (state mutation is internal).
    mutable std::atomic<bool> _lazy_pending{false};
    mutable std::mutex _lazy_open_mutex;
    std::string _lazy_location;
    Cache* _lazy_cache{nullptr};
    bool _lazy_need_filter{true};
    TabletMetadataPtr _lazy_metadata;
    TabletManager* _lazy_tablet_mgr{nullptr};
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
