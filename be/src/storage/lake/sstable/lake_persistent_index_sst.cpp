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

#include <butil/time.h> // NOLINT

#include "fs/fs.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/sstable/iterator.h"
#include "storage/lake/sstable/merger.h"
#include "storage/lake/sstable/table_builder.h"
#include "util/trace.h"

namespace starrocks {

namespace lake {

Status LakePersistentIndexSstable::init(RandomAccessFile* rf, const int64_t filesz, Cache* cache) {
    sstable::Options options;
    _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    options.filter_policy = _filter_policy.get();
    options.block_cache = cache;
    RETURN_IF_ERROR(sstable::Table::Open(options, rf, filesz, &_table));
    _sst.reset(_table);
    return Status::OK();
}

Status LakePersistentIndexSstable::build_sstable(
        phmap::btree_map<std::string, std::list<std::pair<int64_t, IndexValue>>, std::less<>>& memtable,
        WritableFile* wf, uint64_t* filesz) {
    TRACE_COUNTER_SCOPE_LATENCY_US("build_sstable");
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    for (const auto& pair : memtable) {
        auto index_value_info_pb = std::make_unique<IndexValueInfoPB>();
        auto start_ts = butil::gettimeofday_us();
        to_protobuf(pair.second, index_value_info_pb.get());
        auto end_ts = butil::gettimeofday_us();
        TRACE_COUNTER_INCREMENT("to_pb", end_ts - start_ts);
        start_ts = butil::gettimeofday_us();
        builder.Add(Slice(pair.first), Slice(index_value_info_pb->SerializeAsString()));
        end_ts = butil::gettimeofday_us();
        TRACE_COUNTER_INCREMENT("to_pb_add", end_ts - start_ts);
    }
    auto start_ts = butil::gettimeofday_us();
    RETURN_IF_ERROR(builder.Finish());
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("builder_finish", end_ts - start_ts);
    *filesz = builder.FileSize();
    return Status::OK();
}

Status LakePersistentIndexSstable::merge_ssts(int64_t min_retain_version,
                                              std::vector<std::pair<std::string, int64_t>>& sst_metas, WritableFile* wf,
                                              uint64_t* filesz) {
    bool enable_multi_version = false;
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    std::vector<std::unique_ptr<RandomAccessFile>> read_files;
    read_files.reserve(sst_metas.size());
    std::vector<sstable::Iterator*> iters;
    iters.reserve(sst_metas.size());
    std::vector<std::unique_ptr<sstable::Table>> tables;
    tables.reserve(sst_metas.size());
    sstable::ReadOptions read_options;
    for (auto& sst_meta : sst_metas) {
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(sst_meta.first));
        sstable::Table* table;
        RETURN_IF_ERROR(sstable::Table::Open(options, rf.get(), sst_meta.second, &table));
        std::unique_ptr<sstable::Table> table_ptr = nullptr;
        table_ptr.reset(table);
        sstable::Iterator* iter = table->NewIterator(read_options);
        iters.emplace_back(iter);
        read_files.emplace_back(std::move(rf));
        tables.emplace_back(std::move(table_ptr));
    }
    std::unique_ptr<sstable::Iterator> iter_ptr = nullptr;
    sstable::Iterator* iter = sstable::NewMergingIterator(options.comparator, &iters[0], iters.size());
    iter_ptr.reset(iter);
    iter_ptr->SeekToFirst();
    std::string last_key;
    if (iter_ptr->Valid()) {
        last_key = iter_ptr->key().to_string();
    }
    std::list<std::pair<int64_t, IndexValue>> last_index_value_infos;
    while (iter_ptr->Valid()) {
        auto key = iter_ptr->key().to_string();
        auto value = iter_ptr->value().to_string();
        IndexValueInfoPB index_value_info;
        if (!index_value_info.ParseFromString(value)) {
            return Status::InternalError("parse index value info failed");
        }
        if (key == last_key) {
            if (enable_multi_version) {
                // TODO
            } else {
                DCHECK(index_value_info.versions_size() == 1);
                auto version = index_value_info.versions(0);
                auto index_value = index_value_info.values(0);
                if (last_index_value_infos.size() == 0) {
                    last_index_value_infos.emplace_front(version, index_value);
                } else {
                    DCHECK(last_index_value_infos.size() == 1);
                    if (version > last_index_value_infos.front().first) {
                        std::list<std::pair<int64_t, IndexValue>> new_index_value_infos;
                        new_index_value_infos.emplace_front(version, index_value);
                        last_index_value_infos.swap(new_index_value_infos);
                    }
                }
            }
        } else {
            if (!last_index_value_infos.empty()) {
                auto index_value_info_pb = std::make_unique<IndexValueInfoPB>();
                to_protobuf(last_index_value_infos, index_value_info_pb.get());
                builder.Add(Slice(last_key), Slice(index_value_info_pb->SerializeAsString()));
                last_index_value_infos.clear();
            }
            last_key = key;
            if (enable_multi_version) {
                // TODO
            } else {
                DCHECK(index_value_info.versions_size() == 1);
                last_index_value_infos.emplace_front(index_value_info.versions(0), index_value_info.values(0));
            }
        }
        iter_ptr->Next();
    }
    if (!last_index_value_infos.empty()) {
        auto index_value_info_pb = std::make_unique<IndexValueInfoPB>();
        to_protobuf(last_index_value_infos, index_value_info_pb.get());
        builder.Add(Slice(last_key), Slice(index_value_info_pb->SerializeAsString()));
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

void LakePersistentIndexSstable::to_protobuf(const phmap::btree_map<int64_t, int64_t, std::greater<>>& m,
                                             IndexValueInfoPB* index_value_info_pb) {
    auto it = m.begin();
    while (it != m.end()) {
        index_value_info_pb->mutable_versions()->Add((*it).first);
        index_value_info_pb->mutable_values()->Add((*it).second);
        ++it;
    }
}

Status LakePersistentIndexSstable::get(size_t n, const Slice* keys, IndexValue* values,
                                       KeyIndexesInfo* key_indexes_info, KeyIndexesInfo* found_keys_info,
                                       int64_t version) {
    std::vector<std::string> index_value_infos(n);
    sstable::ReadOptions options;
    auto start_ts = butil::gettimeofday_us();
    RETURN_IF_ERROR(_sst->MultiGet(options, n, keys, key_indexes_info, index_value_infos));
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("multi_get", end_ts - start_ts);
    const auto& key_index_infos = key_indexes_info->key_index_infos;
    for (size_t i = 0; i < key_index_infos.size(); ++i) {
        if (!index_value_infos[key_index_infos[i]].empty()) {
            IndexValueInfoPB index_value_info;
            start_ts = butil::gettimeofday_us();
            if (!index_value_info.ParseFromString(index_value_infos[key_index_infos[i]])) {
                return Status::InternalError("parse index value info failed");
            }
            end_ts = butil::gettimeofday_us();
            TRACE_COUNTER_INCREMENT("parse_from_string", end_ts - start_ts);
            start_ts = butil::gettimeofday_us();
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
            end_ts = butil::gettimeofday_us();
            TRACE_COUNTER_INCREMENT("set_found_keys_info", end_ts - start_ts);
        }
    }
    return Status::OK();
}

} // namespace lake
} // namespace starrocks
