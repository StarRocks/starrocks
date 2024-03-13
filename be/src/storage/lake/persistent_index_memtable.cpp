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

namespace starrocks::lake {

void PersistentIndexMemtable::insert(const std::string& key, int64_t version, const IndexValue& value) {
    std::list<IndexValueInfo> index_value_infos;
    index_value_infos.emplace_front(version, value);
    _map.emplace(key, index_value_infos);
}

void PersistentIndexMemtable::update(std::list<IndexValueInfo>& index_value_infos, int64_t version,
                                     const IndexValue& value) {
    std::list<IndexValueInfo> t;
    t.emplace_front(version, value);
    index_value_infos.swap(t);
}

Status PersistentIndexMemtable::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                       KeyIndexesInfo* not_found, size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        const auto value = values[i];
        auto it = _map.find(key);
        if (it == _map.end()) {
            not_found->key_index_infos.emplace_back(i);
            insert(key, version, value);
        } else {
            auto& index_value_infos = it->second;
            auto old_value = index_value_infos.front().second;
            old_values[i] = old_value;
            nfound += old_value.get_value() != NullIndexValue;
            update(it->second, version, value);
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
        if (it != _map.end()) {
            std::string msg = strings::Substitute("PersistentIndexMemtable<$0> insert found duplicate key $1", size,
                                                  hexdump((const char*)key.data(), size));
            return Status::AlreadyExist(msg);
        } else {
            insert(key, version, value);
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
            insert(key, version, IndexValue(NullIndexValue));
        } else {
            auto& index_value_infos = it->second;
            auto old_index_value = index_value_infos.front().second;
            old_values[i] = old_index_value;
            nfound += old_index_value.get_value() != NullIndexValue;
            update(index_value_infos, version, IndexValue(NullIndexValue));
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
        if (it == _map.end()) {
            insert(key, version, value);
        } else {
            update(it->second, version, value);
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
            // Assuming we want the latest (first) value due to emplace_front in updates/inserts
            auto& index_value_infos = it->second;
            auto old_index_value = index_value_infos.front().second;
            values[i] = old_index_value;
            nfound += old_index_value.get_value() != NullIndexValue;
        }
    }
    *num_found = nfound;
    return Status::OK();
}

void PersistentIndexMemtable::clear() {
    _map.clear();
}

} // namespace starrocks::lake
