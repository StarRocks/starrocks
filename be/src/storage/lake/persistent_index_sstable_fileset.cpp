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

namespace starrocks::lake {

Status PersistentIndexSstableFileset::init(std::vector<std::unique_ptr<PersistentIndexSstable>>& sstables) {
    sstable::Comparator* comparator = sstable::BytewiseComparator();
    for (auto&& sstable : sstables) {
        if (sstable->sstable_pb().has_start_key() && sstable->sstable_pb().has_end_key()) {
            DCHECK(sstable->sstable_pb().has_fileset_id());
            // Make sure sstable is inorder via comparator
            if (!_sstable_map.empty()) {
                const auto& last_end_key = _sstable_map.rbegin()->second.first;
                if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().start_key())) >= 0) {
                    return Status::InternalError("sstables are not in order or have overlap key range");
                }
                if (_fileset_id != sstable->sstable_pb().fileset_id()) {
                    return Status::InternalError("inconsistent fileset_id in sstables");
                }
            }
            _fileset_id = sstable->sstable_pb().fileset_id();
            _sstable_map.emplace(sstable->sstable_pb().start_key(),
                                 std::make_pair(sstable->sstable_pb().end_key(), std::move(sstable)));
        } else {
            DCHECK(!sstable->sstable_pb().has_fileset_id());
            if (_standalone_sstable != nullptr) {
                return Status::InternalError("more than one standalone sstable in fileset");
            }
            _standalone_sstable = std::move(sstable);
        }
    }
    return Status::OK();
}

Status PersistentIndexSstableFileset::init(std::unique_ptr<PersistentIndexSstable>& sstable) {
    if (sstable->sstable_pb().has_start_key() && sstable->sstable_pb().has_end_key()) {
        DCHECK(sstable->sstable_pb().has_fileset_id());
        _sstable_map.emplace(sstable->sstable_pb().start_key(),
                             std::make_pair(sstable->sstable_pb().end_key(), std::move(sstable)));
    } else {
        DCHECK(!sstable->sstable_pb().has_fileset_id());
        _standalone_sstable = std::move(sstable);
    }
    return Status::OK();
}

Status PersistentIndexSstableFileset::append(std::unique_ptr<PersistentIndexSstable>& sstable) {
    sstable::Comparator* comparator = sstable::BytewiseComparator();
    DCHECK(sstable->sstable_pb().has_fileset_id());
    // Make sure sstable is inorder via comparator
    if (!_sstable_map.empty()) {
        const auto& last_end_key = _sstable_map.rbegin()->second.first;
        if (comparator->Compare(Slice(last_end_key), Slice(sstable->sstable_pb().start_key())) >= 0) {
            return Status::InternalError("sstables are not in order or have overlap key range");
        }
        if (_fileset_id != sstable->sstable_pb().fileset_id()) {
            return Status::InternalError("inconsistent fileset_id in sstables");
        }
    }
    _fileset_id = sstable->sstable_pb().fileset_id();
    _sstable_map.emplace(sstable->sstable_pb().start_key(),
                         std::make_pair(sstable->sstable_pb().end_key(), std::move(sstable)));
    return Status::OK();
}

Status PersistentIndexSstableFileset::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                                IndexValue* values, KeyIndexSet* found_key_indexes) const {
    sstable::Comparator* comparator = sstable::BytewiseComparator();
    // 1. divide key_indexes into different groups according to sstables
    std::map<PersistentIndexSstable*, KeyIndexSet> sstable_key_indexes_map;
    for (auto& key_index : key_indexes) {
        auto it = _sstable_map.upper_bound(std::string_view(keys[key_index].data, keys[key_index].size));
        if (it != _sstable_map.begin()) {
            --it;
            const auto& [start_key, end_sstable_pair] = *it;
            const auto& [end_key, sstable] = end_sstable_pair;
            if (comparator->Compare(keys[key_index], Slice(end_key)) <= 0) {
                // key in range [start_key, end_key]
                sstable_key_indexes_map[sstable.get()].insert(key_index);
            }
        }
    }
    // 2. multi get from each sstable
    for (const auto& [sstable, sstable_key_indexes] : sstable_key_indexes_map) {
        RETURN_IF_ERROR(sstable->multi_get(keys, sstable_key_indexes, version, values, found_key_indexes));
    }
    return Status::OK();
}

} // namespace starrocks::lake