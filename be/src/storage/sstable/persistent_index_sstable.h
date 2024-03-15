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
#include "storage/lake/key_index.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/table.h"
#include "storage/storage_engine.h"
#include "util/phmap/btree.h"

namespace starrocks {

class WritableFile;

namespace sstable {

// <version, value>
using IndexValueWithVer = std::pair<int64_t, int64_t>;

class PersistentIndexSstable {
public:
    PersistentIndexSstable() = default;
    ~PersistentIndexSstable() = default;

    Status init(RandomAccessFile* rf, const int64_t filesz, Cache* cache);

    static Status build_sstable(phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map, WritableFile* wf,
                                uint64_t* filesz);

    Status multi_get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* key_indexes_info,
                     KeyIndexesInfo* found_keys_info, int64_t version);

    Iterator* iterator(const ReadOptions& options) { return _sst->NewIterator(options); }

private:
    std::unique_ptr<Table> _sst{nullptr};
    std::unique_ptr<FilterPolicy> _filter_policy{nullptr};
};

} // namespace sstable
} // namespace starrocks
