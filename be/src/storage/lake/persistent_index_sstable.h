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

#include <string>

#include "gen_cpp/lake_types.pb.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/table.h"
#include "storage/storage_engine.h"
#include "util/phmap/btree.h"

namespace starrocks {

class WritableFile;
class PersistentIndexSstablePB;

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
                bool need_filter = true);

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

private:
    std::unique_ptr<sstable::Table> _sst{nullptr};
    std::unique_ptr<sstable::FilterPolicy> _filter_policy{nullptr};
    std::unique_ptr<RandomAccessFile> _rf{nullptr};
    PersistentIndexSstablePB _sstable_pb;
};

} // namespace lake
} // namespace starrocks
