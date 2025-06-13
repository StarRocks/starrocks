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

void PersistentIndexMemtable::update_index_value(IndexValueWithVer* index_value_info, int64_t version,
                                                 const IndexValue& value) {
    index_value_info->first = version;
    index_value_info->second = value;
}

Status PersistentIndexMemtable::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                       KeyIndexSet* not_founds, size_t* num_found, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_upsert_us");
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        const auto value = values[i];
        if (auto [it, inserted] = _map.emplace(key, std::make_pair(version, value)); inserted) {
            not_founds->insert(i);
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_ver = it->second;
            auto old_value = old_index_value_ver.second;
            old_values[i] = old_value;
            nfound += old_value.get_value() != NullIndexValue;
            update_index_value(&old_index_value_ver, version, value);
        }
        _max_rss_rowid = std::max(_max_rss_rowid, value.get_value());
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_insert_us");
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        auto size = keys[i].get_size();
        const auto value = values[i];
        if (auto [it, inserted] = _map.emplace(key, std::make_pair(version, value)); inserted) {
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_ver = it->second;
            auto old_index_value = old_index_value_ver.second;
            if (old_index_value.get_value() != NullIndexValue) {
                // shouldn't happen
                std::string msg = strings::Substitute("PersistentIndexMemtable<$0> insert found duplicate key $1", size,
                                                      hexdump((const char*)key.data(), size));
                LOG(ERROR) << msg;
                if (!config::experimental_lake_ignore_pk_consistency_check) {
                    return Status::AlreadyExist(msg);
                } else {
                    update_index_value(&old_index_value_ver, version, value);
                }
            } else {
                // cover delete operation.
                update_index_value(&old_index_value_ver, version, value);
            }
        }
        _max_rss_rowid = std::max(_max_rss_rowid, value.get_value());
    }
    return Status::OK();
}

Status PersistentIndexMemtable::erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexSet* not_founds,
                                      size_t* num_found, int64_t version, uint32_t rowset_id) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_erase_us");
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        if (auto [it, inserted] = _map.emplace(key, std::make_pair(version, IndexValue(NullIndexValue))); inserted) {
            old_values[i] = NullIndexValue;
            not_founds->insert(i);
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_ver = it->second;
            auto old_index_value = old_index_value_ver.second;
            old_values[i] = old_index_value;
            nfound += old_index_value.get_value() != NullIndexValue;
            update_index_value(&old_index_value_ver, version, IndexValue(NullIndexValue));
        }
    }
    // Delete is after upsert, so using UINT32_MAX as it's rowid
    _max_rss_rowid = std::max(_max_rss_rowid, ((uint64_t)rowset_id) << 32 | (uint64_t)UINT32_MAX);
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::erase_with_filter(size_t n, const Slice* keys, const std::vector<bool>& filter,
                                                  int64_t version, uint32_t rowset_id) {
    for (size_t i = 0; i < n; ++i) {
        if (filter[i]) {
            // skip
            continue;
        }
        auto key = keys[i].to_string();
        if (auto [it, inserted] = _map.emplace(key, std::make_pair(version, IndexValue(NullIndexValue))); inserted) {
            _keys_size += key.capacity() + sizeof(std::string);
        } else {
            auto& old_index_value_ver = it->second;
            update_index_value(&old_index_value_ver, version, IndexValue(NullIndexValue));
        }
    }
    // Delete is after upsert, so using UINT32_MAX as it's rowid
    _max_rss_rowid = std::max(_max_rss_rowid, ((uint64_t)rowset_id) << 32 | (uint64_t)UINT32_MAX);
    return Status::OK();
}

Status PersistentIndexMemtable::replace(const Slice* keys, const IndexValue* values,
                                        const std::vector<size_t>& replace_idxes, int64_t version) {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_replace_us");
    for (unsigned long idx : replace_idxes) {
        auto key = keys[idx].to_string();
        const auto value = values[idx];
        if (auto [it, inserted] = _map.emplace(key, std::make_pair(version, value)); !inserted) {
            update_index_value(&it->second, version, value);
        } else {
            _keys_size += key.capacity() + sizeof(std::string);
        }
        _max_rss_rowid = std::max(_max_rss_rowid, value.get_value());
    }
    return Status::OK();
}

Status PersistentIndexMemtable::get(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* not_founds,
                                    int64_t version) const {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_get_us");
    for (size_t i = 0; i < n; ++i) {
        auto key = std::string_view(keys[i]);
        auto it = _map.find(key);
        if (it == _map.end()) {
            values[i] = NullIndexValue;
            not_founds->insert(i);
        } else {
            // Assuming we want the latest (first) value due to emplace_front in updates/inserts
            auto& index_value_ver = it->second;
            auto index_value = index_value_ver.second;
            values[i] = index_value;
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::get(const Slice* keys, IndexValue* values, const KeyIndexSet& key_indexes,
                                    KeyIndexSet* found_key_indexes, int64_t version) const {
    TRACE_COUNTER_SCOPE_LATENCY_US("pindex_memtable_get_us");
    for (auto& key_index : key_indexes) {
        auto key = std::string_view(keys[key_index]);
        auto it = _map.find(key);
        if (it != _map.end()) {
            // Assuming we want the latest (first) value due to emplace_front in updates/inserts
            auto& index_value_ver = it->second;
            auto& index_value = index_value_ver.second;
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
