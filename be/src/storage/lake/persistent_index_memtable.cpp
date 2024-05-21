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

#include "storage/lake/persistent_index_sstable.h"
#include "util/trace.h"

namespace starrocks::lake {

void PersistentIndexMemtable::update_index_value(std::list<IndexValueWithVer>* index_value_infos, int64_t version,
                                                 const IndexValue& value) {
    std::list<IndexValueWithVer> t;
    t.emplace_front(version, value);
    index_value_infos->swap(t);
}

Status PersistentIndexMemtable::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                       KeyIndexSet* not_founds, size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        const auto value = values[i];
        std::list<IndexValueWithVer> index_value_vers;
        index_value_vers.emplace_front(version, value);
        if (auto [it, inserted] = _map.emplace(key, index_value_vers); inserted) {
            not_founds->insert(i);
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_vers = it->second;
            auto old_value = old_index_value_vers.front().second;
            old_values[i] = old_value;
            nfound += old_value.get_value() != NullIndexValue;
            update_index_value(&old_index_value_vers, version, value);
        }
    }
    *num_found = nfound;
    _max_version = std::max(_max_version, version);
    return Status::OK();
}

Status PersistentIndexMemtable::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("persistent_index_memtable_insert_us");
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        auto size = keys[i].get_size();
        const auto value = values[i];
        std::list<IndexValueWithVer> index_value_vers;
        index_value_vers.emplace_front(version, value);
        if (auto [it, inserted] = _map.emplace(key, index_value_vers); !inserted) {
            std::string msg = strings::Substitute("PersistentIndexMemtable<$0> insert found duplicate key $1", size,
                                                  hexdump((const char*)key.data(), size));
            LOG(WARNING) << msg;
            return Status::AlreadyExist(msg);
        }
        _keys_size += key.capacity() + sizeof(std::string);
    }
    _max_version = std::max(_max_version, version);
    return Status::OK();
}

Status PersistentIndexMemtable::erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexSet* not_founds,
                                      size_t* num_found, int64_t version) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        std::list<IndexValueWithVer> index_value_vers;
        index_value_vers.emplace_front(version, IndexValue(NullIndexValue));
        if (auto [it, inserted] = _map.emplace(key, index_value_vers); inserted) {
            old_values[i] = NullIndexValue;
            not_founds->insert(i);
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_vers = it->second;
            auto old_index_value = old_index_value_vers.front().second;
            old_values[i] = old_index_value;
            nfound += old_index_value.get_value() != NullIndexValue;
            update_index_value(&old_index_value_vers, version, IndexValue(NullIndexValue));
        }
    }
    *num_found = nfound;
    _max_version = std::max(_max_version, version);
    return Status::OK();
}

Status PersistentIndexMemtable::replace(const Slice* keys, const IndexValue* values,
                                        const std::vector<size_t>& replace_idxes, int64_t version) {
    for (unsigned long idx : replace_idxes) {
        auto key = keys[idx].to_string();
        const auto value = values[idx];
        std::list<IndexValueWithVer> index_value_vers;
        index_value_vers.emplace_front(version, value);
        if (auto [it, inserted] = _map.emplace(key, index_value_vers); !inserted) {
            update_index_value(&it->second, version, value);
        } else {
            _keys_size += key.capacity() + sizeof(std::string);
        }
    }
    _max_version = std::max(_max_version, version);
    return Status::OK();
}

Status PersistentIndexMemtable::get(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* not_founds,
                                    int64_t version) const {
    for (size_t i = 0; i < n; ++i) {
        auto key = std::string_view(keys[i]);
        auto it = _map.find(key);
        if (it == _map.end()) {
            values[i] = NullIndexValue;
            not_founds->insert(i);
        } else {
            // Assuming we want the latest (first) value due to emplace_front in updates/inserts
            auto& index_value_vers = it->second;
            auto index_value = index_value_vers.front().second;
            values[i] = index_value;
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::get(const Slice* keys, IndexValue* values, const KeyIndexSet& key_indexes,
                                    KeyIndexSet* found_key_indexes, int64_t version) const {
    for (auto& key_index : key_indexes) {
        auto key = std::string_view(keys[key_index]);
        auto it = _map.find(key);
        if (it != _map.end()) {
            // Assuming we want the latest (first) value due to emplace_front in updates/inserts
            auto& index_value_vers = it->second;
            auto& index_value = index_value_vers.front().second;
            values[key_index] = index_value.get_value();
            found_key_indexes->insert(key_index);
        }
    }
    return Status::OK();
}

size_t PersistentIndexMemtable::memory_usage() const {
    return _keys_size + _map.size() * sizeof(IndexValueWithVer);
}

Status PersistentIndexMemtable::flush(WritableFile* wf, uint64_t* filesize) {
    return PersistentIndexSstable::build_sstable(_map, wf, filesize);
}

void PersistentIndexMemtable::clear() {
    _map.clear();
}

} // namespace starrocks::lake
