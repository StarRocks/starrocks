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

LakePersistentIndexSstable::LakePersistentIndexSstable(RandomAccessFile* rf, const int64_t filesz) {
    sstable::Options options;
    sstable::Table::Open(options, rf, filesz, &_table);
    _sst.reset(_table);
}

Status LakePersistentIndexSstable::build_sstable(
        phmap::btree_map<std::string, std::list<std::pair<int64_t, IndexValue>>, std::less<>>& memtable,
        WritableFile* wf, uint64_t* filesz) {
    sstable::Options options;
    sstable::TableBuilder builder(options, wf);
    for (const auto& pair : memtable) {
        auto index_value_info_pb = std::make_unique<IndexValueInfoPB>();
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

Status LakePersistentIndexSstable::get(size_t n, const Slice* keys, IndexValue* values,
                                       KeyIndexesInfo* key_indexes_info, KeyIndexesInfo* found_keys_info,
                                       int64_t version) {
    std::vector<std::string> index_value_infos(n);
    sstable::ReadOptions options;
    RETURN_IF_ERROR(_sst->MultiGet(options, n, keys, key_indexes_info, index_value_infos));
    const auto& key_index_infos = key_indexes_info->key_index_infos;
    for (size_t i = 0; i < key_index_infos.size(); ++i) {
        if (!index_value_infos[key_index_infos[i]].empty()) {
            LOG(INFO) << keys[key_index_infos[i]].to_string();
            IndexValueInfoPB index_value_info;
            if (!index_value_info.ParseFromString(index_value_infos[key_index_infos[i]])) {
                return Status::InternalError("parse index value info failed");
            }
            if (version < 0 && index_value_info.values_size() > 0) {
                values[key_index_infos[i]] = IndexValue(index_value_info.values(0));
                found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
            } else {
                auto versions_size = index_value_info.versions_size();
                for (int j = 0; j < versions_size; ++j) {
                    if (index_value_info.versions(j) == version) {
                        values[key_index_infos[i]] = IndexValue(index_value_info.values(j));
                        found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
                        break;
                    }
                }
            }
        }
    }
    return Status::OK();
}

} // namespace lake
} // namespace starrocks
