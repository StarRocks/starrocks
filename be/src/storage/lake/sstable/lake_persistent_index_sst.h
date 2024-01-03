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
#include "storage/lake/sstable/table.h"
#include "storage/persistent_index.h"
#include "storage/storage_engine.h"
#include "util/phmap/btree.h"

namespace starrocks {

class WritableFile;

namespace lake {

class LakePersistentIndexSstable {
public:
    LakePersistentIndexSstable(RandomAccessFile* rf, const int64_t filesz);
    ~LakePersistentIndexSstable() {}

    static Status build_sstable(
            phmap::btree_map<std::string, std::list<std::pair<int64_t, IndexValue>>, std::less<>>& memtable,
            WritableFile* wf, uint64_t* filesz);

    static void to_protobuf(const std::list<std::pair<int64_t, IndexValue>>& index_value_infos,
                            IndexValueInfoPB* index_value_info_pb);

    Status get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* key_indexes_info,
               KeyIndexesInfo* found_keys_info, int64_t version);

private:
    std::unique_ptr<sstable::Table> _sst{nullptr};
    sstable::Table* _table;
};

} // namespace lake
} // namespace starrocks
