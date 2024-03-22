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

struct KeyIndexesInfo;
class WritableFile;
class PersistentIndexSstablePB;

namespace sstable {

// <version, IndexValue>
using IndexValueWithVer = std::pair<int64_t, IndexValue>;

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb, Cache* cache);

    static Status build_sstable(const phmap::btree_map<std::string, std::list<IndexValueWithVer>, std::less<>>& map,
                                WritableFile* wf, uint64_t* filesz);

    // multi_get can get multi keys at onces
    // |n| : key count that we want to get
    // |keys| : Address point to first element of key array.
    // |key_indexes_info| : the index of key array that we actually want to get.
    // |version| : when < 0, means we want the latest version.
    // |values| : result array of get, should have some count as keys.
    // |found_keys_info| : the index of key array that we found, it should be the subset of key_indexes_info
    Status multi_get(size_t n, const Slice* keys, const KeyIndexesInfo& key_indexes_info, int64_t version,
                     IndexValue* values, KeyIndexesInfo* found_keys_info);

    Iterator* new_iterator(const ReadOptions& options) { return _sst->NewIterator(options); }

private:
    std::unique_ptr<Table> _sst{nullptr};
    std::unique_ptr<FilterPolicy> _filter_policy{nullptr};
    std::unique_ptr<RandomAccessFile> _rf{nullptr};
    PersistentIndexSstablePB _sstable_pb;
};

} // namespace sstable
} // namespace starrocks
