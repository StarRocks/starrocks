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

#include "storage/lake/lake_persistent_index.h"

#include "fs/fs_util.h"
#include "gen_cpp/BackendService_types.h"
#include "storage/lake/filenames.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/tablet_manager.h"
#include "storage/sstable/persistent_index_sstable.h"

namespace starrocks::lake {

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id)
        : PersistentIndex(""),
          _memtable(std::make_unique<PersistentIndexMemtable>()),
          _cache(new_lru_cache(config::lake_pk_index_sst_cache_limit)),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id) {}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
    _sstables.clear();
}

Status LakePersistentIndex::init(const PersistentIndexSstableMetaPB& sstable_meta) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = true};
    for (auto& pindex_sstable : sstable_meta.sstables()) {
        for (auto& sstable_meta : pindex_sstable.sstable_metas()) {
            ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(
                                              opts, _tablet_mgr->sst_location(_tablet_id, sstable_meta.filename())));
            auto sstable = std::make_unique<sstable::PersistentIndexSstable>();
            RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_meta.filesz(), _cache.get()));
            _sstables.emplace_back(std::move(sstable));
        }
    }
    return Status::OK();
}

bool LakePersistentIndex::is_memtable_full() {
    const auto memtable_mem_size = _memtable->memory_usage();
    return memtable_mem_size >= config::lake_memtable_max_mem_usage;
}

void LakePersistentIndex::flush_to_immutable_memtable() {
    _immutable_memtable = std::move(_memtable);
    _memtable = std::make_unique<PersistentIndexMemtable>();
}

Status LakePersistentIndex::get_from_sstables(size_t n, const Slice* keys, IndexValue* values,
                                              KeyIndexesInfo* key_indexes_info, int64_t version) {
    if (key_indexes_info->size() == 0 || _sstables.empty()) {
        return Status::OK();
    }
    size_t sstables_size = _sstables.size();
    std::sort(key_indexes_info->key_index_infos.begin(), key_indexes_info->key_index_infos.end());
    for (size_t i = sstables_size - 1; i >= 0; --i) {
        KeyIndexesInfo found_key_indexes_info;
        RETURN_IF_ERROR(_sstables[i]->multi_get(n, keys, *key_indexes_info, version, values, &found_key_indexes_info));
        if (found_key_indexes_info.size() != 0) {
            std::sort(found_key_indexes_info.key_index_infos.begin(), found_key_indexes_info.key_index_infos.end());
            // modify key_indexess_info
            key_indexes_info->set_difference(found_key_indexes_info);
        }
        if (key_indexes_info->size() == 0) {
            break;
        }
    }
    return Status::OK();
}

Status LakePersistentIndex::get_from_immutable_memtable(size_t n, const Slice* keys, IndexValue* values,
                                                        KeyIndexesInfo* key_indexes_info, int64_t version) {
    if (_immutable_memtable == nullptr || key_indexes_info->size() == 0) {
        return Status::OK();
    }
    std::sort(key_indexes_info->key_index_infos.begin(), key_indexes_info->key_index_infos.end());
    KeyIndexesInfo found_key_indexes_info;
    RETURN_IF_ERROR(_immutable_memtable->get(n, keys, values, key_indexes_info, &found_key_indexes_info, version));
    if (found_key_indexes_info.size() != 0) {
        std::sort(found_key_indexes_info.key_index_infos.begin(), found_key_indexes_info.key_index_infos.end());
        // modify key_indexes_info
        key_indexes_info->set_difference(found_key_indexes_info);
    }
    return Status::OK();
}

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    // Assuming we always want the latest value now
    RETURN_IF_ERROR(_memtable->get(n, keys, values, &not_founds, &num_found, -1));
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, values, &not_founds, -1));
    RETURN_IF_ERROR(get_from_sstables(n, keys, values, &not_founds, -1));
    return Status::OK();
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, _version.major_number()));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(minor_compact());
        flush_to_immutable_memtable();
    }
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, old_values, &not_founds, -1));
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &not_founds, -1));
    return Status::OK();
}

Status LakePersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    RETURN_IF_ERROR(_memtable->insert(n, keys, values, version));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(minor_compact());
        flush_to_immutable_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    KeyIndexesInfo not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->erase(n, keys, old_values, &not_founds, &num_found, _version.major_number()));
    RETURN_IF_ERROR(get_from_immutable_memtable(n, keys, old_values, &not_founds, -1));
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &not_founds, -1));
    return Status::OK();
}

Status LakePersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
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
    RETURN_IF_ERROR(_memtable->replace(keys, values, replace_idxes, _version.major_number()));
    return Status::OK();
}

Status LakePersistentIndex::minor_compact() {
    if (_immutable_memtable == nullptr) {
        return Status::OK();
    }
    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(location));
    uint64_t filesz;
    RETURN_IF_ERROR(_immutable_memtable->flush(wf.get(), &filesz));
    auto sstable = std::make_unique<sstable::PersistentIndexSstable>();
    RandomAccessFileOptions opts{.skip_fill_local_cache = true};
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, location));
    RETURN_IF_ERROR(sstable->init(std::move(rf), filesz, _cache.get()));
    _immutable_memtable = nullptr;
    return Status::OK();
}

Status LakePersistentIndex::major_compact(int64_t min_retain_version) {
    return Status::OK();
}

} // namespace starrocks::lake
