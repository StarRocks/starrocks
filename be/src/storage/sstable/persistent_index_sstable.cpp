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

#include "storage/sstable/persistent_index_sstable.h"

#include <butil/time.h> // NOLINT

#include "fs/fs.h"
#include "storage/olap_common.h"
#include "storage/sstable/table_builder.h"
#include "util/trace.h"

namespace starrocks::sstable {

Status PersistentIndexSstable::init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb,
                                    Cache* cache) {
    Options options;
    _filter_policy.reset(const_cast<FilterPolicy*>(NewBloomFilterPolicy(10)));
    options.filter_policy = _filter_policy.get();
    options.block_cache = cache;
    Table* table;
    RETURN_IF_ERROR(Table::Open(options, rf.get(), sstable_pb.filesz(), &table));
    _sst.reset(table);
    _rf = std::move(rf);
    _sstable_pb.CopyFrom(sstable_pb);
    return Status::OK();
}

Status PersistentIndexSstable::build_sstable(
        const phmap::btree_map<std::string, std::list<IndexValueWithVer>, std::less<>>& map, WritableFile* wf,
        uint64_t* filesz) {
    std::unique_ptr<FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<FilterPolicy*>(NewBloomFilterPolicy(10)));
    Options options;
    options.filter_policy = filter_policy.get();
    TableBuilder builder(options, wf);
    for (const auto& [k, v] : map) {
        IndexValueWithVerPB index_value_pb;
        for (const auto& index_value_with_ver : v) {
            index_value_pb.add_versions(index_value_with_ver.first);
            index_value_pb.add_values(index_value_with_ver.second.get_value());
        }
        builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString()));
    }
    RETURN_IF_ERROR(builder.Finish());
    *filesz = builder.FileSize();
    return Status::OK();
}

Status PersistentIndexSstable::multi_get(size_t n, const Slice* keys, const KeyIndexesInfo& key_indexes_info,
                                         int64_t version, IndexValue* values, KeyIndexesInfo* found_keys_info) {
    std::vector<std::string> index_value_infos(n);
    ReadOptions options;
    auto start_ts = butil::gettimeofday_us();
    RETURN_IF_ERROR(_sst->MultiGet(options, n, keys, key_indexes_info, &index_value_infos));
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("multi_get", end_ts - start_ts);
    const auto& key_index_infos = key_indexes_info.key_index_infos;
    for (size_t i = 0; i < key_index_infos.size(); ++i) {
        if (!index_value_infos[key_index_infos[i]].empty()) {
            IndexValueWithVerPB index_value_with_ver_pb;
            if (!index_value_with_ver_pb.ParseFromString(index_value_infos[key_index_infos[i]])) {
                return Status::InternalError("parse index value info failed");
            }
            if (version < 0 && index_value_with_ver_pb.values_size() > 0) {
                values[key_index_infos[i]] = IndexValue(index_value_with_ver_pb.values(0));
                found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
            } else {
                for (int j = 0; j < index_value_with_ver_pb.versions_size(); ++j) {
                    if (index_value_with_ver_pb.versions(j) == version) {
                        values[key_index_infos[i]] = IndexValue(index_value_with_ver_pb.values(j));
                        found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
                        break;
                    }
                }
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::sstable
