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

#include "storage/lake/persistent_index_memtable.h"

#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/filenames.h"
#include "storage/lake/sstable/lake_persistent_index_sst.h"

namespace starrocks::lake {

PersistentIndexMemtable::PersistentIndexMemtable(TabletManager* tablet_mgr, int64_t tablet_id)
        : _tablet_mgr(tablet_mgr), _tablet_id(tablet_id) {}

Status PersistentIndexMemtable::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                       KeyIndexesInfo* not_found, size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        const auto value = values[i];
        auto it = _map.find(key);
        if (it == _map.end()) {
            not_found->key_index_infos.emplace_back(i);
            std::list<IndexValueInfo> index_value_infos;
            index_value_infos.emplace_front(version, value);
            _map.emplace(key, index_value_infos);
        } else {
            auto& index_value_infos = it->second;
            auto old_value = index_value_infos.front().second;
            old_values[i] = old_value;
            nfound += old_value.get_value() != NullIndexValue;
            index_value_infos.emplace_front(version, value);
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        auto size = keys[i].get_size();
        const auto value = values[i];
        auto it = _map.find(key);
        if (it == _map.end()) {
            std::list<IndexValueInfo> index_value_infos;
            index_value_infos.emplace_front(version, value);
            _map.emplace(key, index_value_infos);
        } else {
            auto& index_value_infos = it->second;
            auto old_index_value_info = index_value_infos.front();
            if (version == old_index_value_info.first) {
                std::string msg = strings::Substitute("PersistentIndexMemtable<$0> insert found duplicate key $1", size,
                                                      hexdump((const char*)key.data(), size));
                return Status::AlreadyExist(msg);
            }
            index_value_infos.emplace_front(version, value);
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexesInfo* not_found,
                                      size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        auto it = _map.find(key);
        if (it == _map.end()) {
            old_values[i] = NullIndexValue;
            not_found->key_index_infos.emplace_back(i);
            std::list<IndexValueInfo> index_value_infos;
            index_value_infos.emplace_front(version, IndexValue(NullIndexValue));
            _map.emplace(key, index_value_infos);
        } else {
            auto& index_value_infos = it->second;
            auto old_index_value = index_value_infos.front().second;
            nfound += old_index_value.get_value() != NullIndexValue;
            index_value_infos.emplace_front(version, IndexValue(NullIndexValue));
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::replace(const Slice* keys, const IndexValue* values,
                                        const std::vector<size_t>& replace_idxes, int64_t version) {
    for (unsigned long idx : replace_idxes) {
        auto key = keys[idx].to_string();
        const auto value = values[idx];
        auto it = _map.find(key);
        if (it != _map.end()) {
            auto& index_value_infos = it->second;
            index_value_infos.emplace_front(version, value);
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* not_found,
                                    size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = std::string_view(keys[i]);
        auto it = _map.find(key);
        if (it == _map.end()) {
            values[i] = NullIndexValue;
            not_found->key_index_infos.emplace_back(i);
        } else {
            auto& index_value_infos = it->second;
            if (version < 0) {
                auto old_index_value = index_value_infos.front().second;
                values[i] = old_index_value.get_value();
                nfound += old_index_value.get_value() != NullIndexValue;
            } else {
                auto index_it = index_value_infos.begin();
                auto found = false;
                while (index_it != index_value_infos.end()) {
                    auto index_value_info = *index_it;
                    if (index_value_info.first == version) {
                        auto old_index_value_info = index_value_info.second;
                        values[i] = old_index_value_info.get_value();
                        nfound += old_index_value_info.get_value() != NullIndexValue;
                        found = true;
                        break;
                    }
                    ++index_it;
                }
                if (!found) {
                    not_found->key_index_infos.emplace_back(i);
                }
            }
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* keys_info,
               KeyIndexesInfo* found_keys_info, int64_t version) {
    const auto& key_index_infos = keys_info->key_index_infos;
    for (size_t i = 0; i < key_index_infos.size(); ++i) {
        auto key = std::string_view(keys[key_index_infos[i]]);
        auto it = _map.find(key);
        if (it != _map.end()) {
            auto& index_value_infos = it->second;
            if (version < 0) {
                auto old_index_value = index_value_infos.front().second;
                values[key_index_infos[i]] = old_index_value.get_value();
                found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
            } else {
                auto index_it = index_value_infos.begin();
                while (index_it != index_value_infos.end()) {
                    auto index_value_info = *index_it;
                    if (index_value_info.first == version) {
                        found_keys_info->key_index_infos.emplace_back(key_index_infos[i]);
                        auto old_index_value_info = index_value_info.second;
                        values[key_index_infos[i]] = old_index_value_info.get_value();
                        break;
                    }
                    index_it++;
                }
            }
        }
    }
    return Status::OK();
}

void PersistentIndexMemtable::clear() {
    _map.clear();
}

size_t PersistentIndexMemtable::memory_usage() {
    size_t mem_usage = 0; 
    for (auto const& it : _map) {
        mem_usage += it.first.size();
        mem_usage += it.second.size() * sizeof(IndexValueInfo);
    }
    return mem_usage;
}

Status PersistentIndexMemtable::flush(int64_t txn_id, SstablePB *sstable) {
    auto name = gen_sst_filename(txn_id);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(_tablet_id, name)));
    uint64_t filesz;
    RETURN_IF_ERROR(LakePersistentIndexSstable::build_sstable(_map, wf.get(), &filesz));
    RETURN_IF_ERROR(wf->close());
    sstable->set_filename(name);
    sstable->set_filesz(filesz);
    return Status::OK();
}

} // namespace starrocks::lake
