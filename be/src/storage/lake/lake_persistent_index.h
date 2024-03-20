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

#include "storage/persistent_index.h"
#include "storage/sstable/persistent_index_sstable.h"

namespace starrocks {
class Cache;
struct KeyIndexesInfo;
class PersistentIndexSstableMetaPB;
class PersistentIndexSstablePB;

namespace sstable {
class PersistentIndexSstable;
} // namespace sstable
namespace lake {

class PersistentIndexMemtable;
class TabletManager;

class LakePersistentIndex : public PersistentIndex {
public:
    explicit LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id);

    ~LakePersistentIndex();

    DISALLOW_COPY(LakePersistentIndex);

    Status init(const PersistentIndexSstableMetaPB& sstable_meta);

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    Status get(size_t n, const Slice* keys, IndexValue* values) override;

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |stat|: used for collect statistic
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                  IOStat* stat = nullptr) override;

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    Status erase(size_t n, const Slice* keys, IndexValue* old_values) override;

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed) override;

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |version|: version of values
    Status insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version);

    Status minor_compact();

    Status major_compact(int64_t min_retain_version);

private:
    void flush_to_immutable_memtable();

    bool is_memtable_full();

    Status get_from_immutable_memtable(size_t n, const Slice* keys, IndexValue* values,
                                       KeyIndexesInfo* key_indexes_info, int64_t version);

    Status get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexesInfo* key_indexes_info,
                             int64_t version);

private:
    std::unique_ptr<PersistentIndexMemtable> _memtable;
    std::unique_ptr<PersistentIndexMemtable> _immutable_memtable;
    std::unique_ptr<Cache> _cache;
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id{0};
    std::vector<std::unique_ptr<sstable::PersistentIndexSstable>> _sstables;
};

} // namespace lake
} // namespace starrocks
