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

#include "storage/lake/cloud_native_persistent_index.h"

#include "storage/lake/persistent_index_memtable.h"

namespace starrocks::lake {

CloudNativePersistentIndex::CloudNativePersistentIndex(std::string path) : PersistentIndex(std::move(path)) {
    _memtable = std::make_unique<PersistentIndexMemtable>();
}

CloudNativePersistentIndex::~CloudNativePersistentIndex() {
    _memtable->clear();
}

Status CloudNativePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    return _memtable->get(n, keys, values, &not_founds, &num_found);
}

Status CloudNativePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                          IOStat* stat) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    return _memtable->upsert(n, keys, values, old_values, &not_founds, &num_found);
}

Status CloudNativePersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1) {
    return _memtable->insert(n, keys, values);
}

Status CloudNativePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    return _memtable->erase(n, keys, old_values, &not_founds, &num_found);
}

Status CloudNativePersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                               const uint32_t max_src_rssid, std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (found_values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) <= max_src_rssid) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_memtable->replace(keys, values, replace_idxes));
    return Status::OK();
}

Status CloudNativePersistentIndex::minor_compact() {
    return Status::OK();
}

Status CloudNativePersistentIndex::major_compact(int64_t min_retain_version) {
    return Status::OK();
}

} // namespace starrocks::lake
