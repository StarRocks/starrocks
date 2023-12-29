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

#include "storage/lake/sstable/lake_persistent_index_sst.h"

#include "fs/fs.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/sstable/table_builder.h"

namespace starrocks {

namespace lake {

Status LakePersistentIndexSstable::build_sstable(
        phmap::btree_map<std::string, std::list<std::pair<int64_t, IndexValue>>, std::less<>>& memtable,
        WritableFile* wf, uint64_t* filesz) {
    sstable::Options options;
    sstable::TableBuilder builder(options, wf);
    for (const auto& pair : memtable) {
        auto index_value_info_pb = std::make_shared<IndexValueInfoPB>();
        to_protobuf(pair.second, index_value_info_pb.get());
        builder.Add(Slice(pair.first), Slice(index_value_info_pb->SerializeAsString()));
    }
    RETURN_IF_ERROR(builder.Finish());
    *filesz = builder.FileSize();
    return Status::OK();
}

void LakePersistentIndexSstable::to_protobuf(const std::list<std::pair<int64_t, IndexValue>>& index_value_infos,
                                             IndexValueInfoPB* index_value_info_pb) {
    auto it = index_value_infos.begin();
    while (it != index_value_infos.end()) {
        index_value_info_pb->mutable_versions()->Add((*it).first);
        index_value_info_pb->mutable_values()->Add((*it).second.get_value());
        ++it;
    }
}

} // namespace lake
} // namespace starrocks
