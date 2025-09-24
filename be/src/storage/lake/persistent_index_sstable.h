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

#include "gen_cpp/lake_types.pb.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/table.h"
#include "storage/storage_engine.h"
#include "util/phmap/btree.h"

namespace starrocks {

class WritableFile;
class PersistentIndexSstablePB;

namespace sstable {
class TableBuilder;
class FilterPolicy;
} // namespace sstable

namespace lake {
using KeyIndex = size_t;
using KeyIndexSet = std::set<KeyIndex>;
// <version, IndexValue>
using IndexValueWithVer = std::pair<int64_t, IndexValue>;

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb, Cache* cache,
                TabletManager* tablet_mgr, int64_t tablet_id, bool need_filter = true, DelVectorPtr delvec = nullptr);

    static Status build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                WritableFile* wf, uint64_t* filesz);

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

    // `_delvec` should only be modified in `init()` via publish version thread
    // which is thread-safe. And after that, it should be immutable.
    DelVectorPtr delvec() const { return _delvec; }

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

private:
    std::unique_ptr<sstable::TableBuilder> _table_builder;
    std::unique_ptr<sstable::FilterPolicy> _filter_policy;
    std::unique_ptr<WritableFile> _wf;
    bool _finished;
    std::string _encryption_meta;
    uint32_t _sst_rowid = 0;
};

} // namespace lake
} // namespace starrocks
