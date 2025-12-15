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

#include "storage/lake/persistent_index_sstable_fileset.h"

#include "storage/lake/persistent_index_sstable.h"
#include "storage/sstable/comparator.h"
#include "util/trace.h"

namespace starrocks::lake {

Status PersistentIndexSstableFileset::init(std::vector<std::unique_ptr<PersistentIndexSstable>>& sstables) {
    if (is_inited()) {
        return Status::InternalError("sstable fileset is already initialized");
    }
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    for (auto&& sstable : sstables) {
        if (sstable->sstable_pb().has_range()) {
            DCHECK(sstable->sstable_pb().has_fileset_id());
            // Make sure sstable is inorder via comparator
            if (!_sstable_map.empty()) {
                const auto& last_end_key = _sstable_map.rbegin()->first.second;
                if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().range().start_key())) >= 0) {
                    return Status::InternalError("sstables are not in order or have overlap key range");
                }
                if (_fileset_id != sstable->sstable_pb().fileset_id()) {
                    return Status::InternalError("inconsistent fileset_id in sstables");
                }
            }
            _fileset_id = sstable->sstable_pb().fileset_id();
            // Extract keys before moving sstable to avoid undefined behavior
            std::string start_key = sstable->sstable_pb().range().start_key();
            std::string end_key = sstable->sstable_pb().range().end_key();
            _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
        } else {
            if (_standalone_sstable != nullptr) {
                return Status::InternalError("more than one standalone sstable in fileset");
            }
            _fileset_id = UniqueId::gen_uid();
            sstable->set_fileset_id(_fileset_id);
            _standalone_sstable = std::move(sstable);
        }
    }
    return Status::OK();
}

Status PersistentIndexSstableFileset::init(std::unique_ptr<PersistentIndexSstable>& sstable) {
    if (is_inited()) {
        return Status::InternalError("sstable fileset is already initialized");
    }
    if (sstable->sstable_pb().has_range()) {
        if (!sstable->sstable_pb().has_fileset_id()) {
            // New fileset
            _fileset_id = UniqueId::gen_uid();
            sstable->set_fileset_id(_fileset_id);
        } else {
            _fileset_id = sstable->sstable_pb().fileset_id();
        }
        // Extract keys before moving sstable to avoid undefined behavior
        std::string start_key = sstable->sstable_pb().range().start_key();
        std::string end_key = sstable->sstable_pb().range().end_key();
        _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
    } else {
        _fileset_id = UniqueId::gen_uid();
        sstable->set_fileset_id(_fileset_id);
        _standalone_sstable = std::move(sstable);
    }
    return Status::OK();
}

Status PersistentIndexSstableFileset::merge_from(std::unique_ptr<PersistentIndexSstable>& sstable) {
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    DCHECK(sstable->sstable_pb().has_range());
    // Make sure sstable is inorder via comparator
    if (!_sstable_map.empty()) {
        const auto& last_end_key = _sstable_map.rbegin()->first.second;
        if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().range().start_key())) >= 0) {
            return Status::InternalError("sstables are not in order or have overlap key range");
        }
    } else {
        return Status::InternalError("sstable fileset is not init yet");
    }
    // Extract keys before moving sstable to avoid undefined behavior
    std::string start_key = sstable->sstable_pb().range().start_key();
    std::string end_key = sstable->sstable_pb().range().end_key();
    // This sstable belong to same fileset.
    sstable->set_fileset_id(_fileset_id);
    _sstable_map.emplace(std::make_pair(std::move(start_key), std::move(end_key)), std::move(sstable));
    return Status::OK();
}

Status PersistentIndexSstableFileset::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                                IndexValue* values, KeyIndexSet* found_key_indexes) const {
    const sstable::Comparator* comparator = sstable::BytewiseComparator();
    // 0. if single standalone sstable, directly get.
    if (_standalone_sstable != nullptr) {
        DCHECK(_sstable_map.empty());
        return _standalone_sstable->multi_get(keys, key_indexes, version, values, found_key_indexes);
    }
    // 1. divide key_indexes into different groups according to sstables
    std::unordered_map<PersistentIndexSstable*, KeyIndexSet> sstable_key_indexes_map;
    {
        TRACE_COUNTER_SCOPE_LATENCY_US("fileset_get_divide_us");
        for (auto& key_index : key_indexes) {
            auto it = _sstable_map.upper_bound(keys[key_index]);
            if (it != _sstable_map.begin()) {
                --it;
                const auto& [key_pair, sstable] = *it;
                const auto& [start_key, end_key] = key_pair;
                if (comparator->Compare(keys[key_index], Slice(end_key)) <= 0) {
                    // key in range [start_key, end_key]
                    sstable_key_indexes_map[sstable.get()].insert(key_index);
                }
            }
        }
    }
    // 2. multi get from each sstable
    for (const auto& [sstable, sstable_key_indexes] : sstable_key_indexes_map) {
        RETURN_IF_ERROR(sstable->multi_get(keys, sstable_key_indexes, version, values, found_key_indexes));
    }
    return Status::OK();
}

const std::string& PersistentIndexSstableFileset::standalone_sstable_filename() const {
    return _standalone_sstable->sstable_pb().filename();
}

void PersistentIndexSstableFileset::get_all_sstable_pbs(PersistentIndexSstableMetaPB* sstable_pbs) const {
    for (const auto& [key_pair, sstable] : _sstable_map) {
        sstable_pbs->add_sstables()->CopyFrom(sstable->sstable_pb());
    }
    if (_standalone_sstable != nullptr) {
        sstable_pbs->add_sstables()->CopyFrom(_standalone_sstable->sstable_pb());
    }
}

size_t PersistentIndexSstableFileset::memory_usage() const {
    size_t total_memory = 0;
    for (const auto& [key_pair, sstable] : _sstable_map) {
        total_memory += sstable->memory_usage();
    }
    if (_standalone_sstable != nullptr) {
        total_memory += _standalone_sstable->memory_usage();
    }
    return total_memory;
}

void PersistentIndexSstableFileset::print_debug_info(std::stringstream& ss) {
    ss << " Fileset ID: " << _fileset_id.to_string();
    ss << " Number of sstables: " << _sstable_map.size();
    for (const auto& [key_pair, sstable] : _sstable_map) {
        ss << " Sstable filesize: " << sstable->sstable_pb().filesize();
    }
    if (_standalone_sstable != nullptr) {
        ss << " Standalone sstable filesize: " << _standalone_sstable->sstable_pb().filesize();
    }
}

} // namespace starrocks::lake