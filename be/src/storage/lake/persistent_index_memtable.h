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

#pragma once

#include <list>

#include "storage/lake/key_index.h"
#include "storage/lake/tablet.h"
#include "storage/persistent_index.h"
#include "util/phmap/btree.h"

namespace starrocks::lake {
class SstablePB;
class TabletManager;

struct SstableInfo {
    std::string filename;
    uint64_t filesz;

    void to_pb(SstablePB* sstable) const {
        sstable->set_filename(filename);
        sstable->set_filesz(filesz);
    }
};

using IndexValueInfo = std::pair<int64_t, IndexValue>;

class PersistentIndexMemtable {
public:
    PersistentIndexMemtable() = default;

    PersistentIndexMemtable(TabletManager* tablet_mgr, int64_t tablet_id);

    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                  KeyIndexesInfo* not_found, size_t* num_found, int64_t version);

    Status insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version);

    Status erase(size_t n, const Slice* keys, IndexValue* old_values, KeyIndexesInfo* not_found, size_t* num_found,
                 int64_t version);

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes,
                   int64_t version);

    Status get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* not_found, size_t* num_found,
               int64_t version);

    Status get(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* keys_info,
               KeyIndexesInfo* found_keys_info, int64_t version);

    void clear();

    size_t memory_usage();

    Status flush(int64_t txn_id, SstablePB *sstable);

private:
    // phmap::flat_hash_map<std::string, std::list<IndexValueInfo>> _map;
    phmap::btree_map<std::string, std::list<IndexValueInfo>, std::less<>> _map;
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id;
};

} // namespace starrocks::lake
