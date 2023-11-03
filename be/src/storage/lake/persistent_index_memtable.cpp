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

PersistentIndexMemtable::PersistentIndexMemtable(Tablet* tablet) : _tablet(tablet) {}

Status PersistentIndexMemtable::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                       KeyIndexesInfo* not_found, size_t* num_found) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        const auto value = values[i];
        if (auto [it, inserted] = _map.emplace(key, value); inserted) {
            not_found->key_index_infos.emplace_back(i);
        } else {
            auto old_value = it->second;
            old_values[i] = old_value;
            nfound += old_value.get_value() != NullIndexValue;
            it->second = value;
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::insert(size_t n, const Slice* keys, const IndexValue* values) {
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        auto size = keys[i].get_size();
        const auto value = values[i];
        if (auto [it, inserted] = _map.emplace(key, value); !inserted) {
            std::string msg = strings::Substitute("PersistentIndexMemtable<$0> insert found duplicate key $1", size,
                                                  hexdump((const char*)key.data(), size));
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexesInfo* not_found,
                                      size_t* num_found) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = keys[i].to_string();
        if (auto [it, inserted] = _map.emplace(key, IndexValue(NullIndexValue)); inserted) {
            old_values[i] = NullIndexValue;
            not_found->key_index_infos.emplace_back(i);
        } else {
            old_values[i] = it->second;
            nfound += it->second.get_value() != NullIndexValue;
            it->second = NullIndexValue;
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status PersistentIndexMemtable::replace(const Slice* keys, const IndexValue* values,
                                        const std::vector<size_t>& replace_idxes) {
    for (unsigned long idx : replace_idxes) {
        auto key = keys[idx].to_string();
        const auto value = values[idx];
        if (auto [it, inserted] = _map.emplace(key, value); !inserted) {
            it->second = value;
        }
    }
    return Status::OK();
}

Status PersistentIndexMemtable::get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* not_found,
                                    size_t* num_found) {
    size_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        auto key = std::string_view(keys[i]);
        auto iter = _map.find(key);
        if (iter == _map.end()) {
            values[i] = NullIndexValue;
            not_found->key_index_infos.emplace_back(i);
        } else {
            values[i] = iter->second;
            nfound += iter->second.get_value() != NullIndexValue;
        }
    }
    *num_found = nfound;
    return Status::OK();
}

void PersistentIndexMemtable::clear() {
    _map.clear();
}

size_t PersistentIndexMemtable::memory_usage() {
    size_t mem_usage = _map.size() * kIndexValueSize;
    for (auto const& it : _map) {
        mem_usage += it.first.size();
    }
    return mem_usage;
}

Status PersistentIndexMemtable::flush(SstableInfo* sstable, int64_t txn_id) {
    auto name = gen_sst_filename(txn_id);
    ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(_tablet->sst_location(name)));
    uint64_t filesz;
    RETURN_IF_ERROR(LakePersistentIndexSstable::build_sstable(_map, wf.get(), &filesz));
    RETURN_IF_ERROR(wf->close());
    sstable->filename = std::string(name);
    sstable->filesz = filesz;
    return Status::OK();
}

} // namespace starrocks::lake
