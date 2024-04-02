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
#include "storage/lake/filenames.h"
#include "storage/lake/persistent_index_memtable.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

LakePersistentIndex::LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id)
        : PersistentIndex(""),
          _memtable(std::make_unique<PersistentIndexMemtable>()),
          _tablet_mgr(tablet_mgr),
          _tablet_id(tablet_id) {}

LakePersistentIndex::~LakePersistentIndex() {
    _memtable->clear();
    _sstables.clear();
}

void LakePersistentIndex::set_difference(KeyIndexSet* key_indexes, const KeyIndexSet& found_key_indexes) {
    if (!found_key_indexes.empty()) {
        KeyIndexSet t;
        std::set_difference(key_indexes->begin(), key_indexes->end(), found_key_indexes.begin(),
                            found_key_indexes.end(), std::inserter(t, t.end()));
        key_indexes->swap(t);
    }
}

bool LakePersistentIndex::is_memtable_full() const {
    const auto memtable_mem_size = _memtable->memory_usage();
    return memtable_mem_size >= config::l0_max_mem_usage / 2;
}

Status LakePersistentIndex::minor_compact() {
    auto filename = gen_sst_filename();
    auto location = _tablet_mgr->sst_location(_tablet_id, filename);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(location));
    uint64_t filesize = 0;
    RETURN_IF_ERROR(_immutable_memtable->flush(wf.get(), &filesize));
    RETURN_IF_ERROR(wf->close());

    auto sstable = std::make_unique<PersistentIndexSstable>();
    RandomAccessFileOptions opts{.skip_fill_local_cache = true};
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(opts, location));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.set_version(_version.major_number());
    RETURN_IF_ERROR(sstable->init(std::move(rf), sstable_pb, nullptr));
    _sstables.emplace_back(std::move(sstable));
    return Status::OK();
}

Status LakePersistentIndex::flush_memtable() {
    if (_immutable_memtable != nullptr) {
        RETURN_IF_ERROR(minor_compact());
    }
    _immutable_memtable = std::make_unique<PersistentIndexMemtable>();
    _memtable.swap(_immutable_memtable);
    return Status::OK();
}

Status LakePersistentIndex::get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                                              int64_t version) const {
    if (key_indexes->empty() || _sstables.empty()) {
        return Status::OK();
    }
    for (auto iter = _sstables.rbegin(); iter != _sstables.rend(); ++iter) {
        KeyIndexSet found_key_indexes;
        RETURN_IF_ERROR((*iter)->multi_get(keys, *key_indexes, version, values, &found_key_indexes));
        set_difference(key_indexes, found_key_indexes);
        if (key_indexes->empty()) {
            break;
        }
    }
    return Status::OK();
}

Status LakePersistentIndex::get_from_immutable_memtable(const Slice* keys, IndexValue* values,
                                                        const KeyIndexSet& key_indexes, KeyIndexSet* found_key_indexes,
                                                        int64_t version) const {
    if (_immutable_memtable == nullptr || key_indexes.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_immutable_memtable->get(keys, values, key_indexes, found_key_indexes, version));
    return Status::OK();
}

Status LakePersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    KeyIndexSet not_founds;
    // Assuming we always want the latest value now
    RETURN_IF_ERROR(_memtable->get(n, keys, values, &not_founds, -1));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, values, &key_indexes, -1));
    return Status::OK();
}

Status LakePersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                   IOStat* stat) {
    std::set<KeyIndex> not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->upsert(n, keys, values, old_values, &not_founds, &num_found, _version.major_number()));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, old_values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &key_indexes, -1));
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version) {
    RETURN_IF_ERROR(_memtable->insert(n, keys, values, version));
    if (is_memtable_full()) {
        RETURN_IF_ERROR(flush_memtable());
    }
    // TODO: check whether keys exist in immutable_memtable and ssts
    return Status::OK();
}

Status LakePersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    KeyIndexSet not_founds;
    size_t num_found;
    RETURN_IF_ERROR(_memtable->erase(n, keys, old_values, &not_founds, &num_found, _version.major_number()));
    KeyIndexSet& key_indexes = not_founds;
    KeyIndexSet found_key_indexes;
    RETURN_IF_ERROR(get_from_immutable_memtable(keys, old_values, key_indexes, &found_key_indexes, -1));
    set_difference(&key_indexes, found_key_indexes);
    RETURN_IF_ERROR(get_from_sstables(n, keys, old_values, &key_indexes, -1));
    if (is_memtable_full()) {
        return flush_memtable();
    }
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
    if (is_memtable_full()) {
        return flush_memtable();
    }
    return Status::OK();
}

Status LakePersistentIndex::major_compact(int64_t min_retain_version) {
    return Status::OK();
}

} // namespace starrocks::lake
