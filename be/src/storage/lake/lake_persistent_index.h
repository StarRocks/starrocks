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

namespace starrocks {

namespace sstable {
class Iterator;
class TableBuilder;
} // namespace sstable

namespace lake {

using KeyIndex = size_t;
using KeyIndexSet = std::set<KeyIndex>;
class MetaFileBuilder;
class PersistentIndexMemtable;
class PersistentIndexSstable;
class TabletManager;
class TxnLogPB;
class TxnLogPB_OpCompaction;

using IndexValueWithVer = std::pair<int64_t, IndexValue>;

// LakePersistentIndex is not thread-safe.
// Caller should take care of the multi-thread safety
class LakePersistentIndex : public PersistentIndex {
public:
    explicit LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id);

    ~LakePersistentIndex();

    DISALLOW_COPY(LakePersistentIndex);

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

    Status major_compact(int64_t min_retain_version, std::shared_ptr<TxnLogPB>& txn_log);

    Status apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction);

    void commit(MetaFileBuilder* builder);

private:
    Status flush_memtable();

    bool is_memtable_full() const;

    // batch get
    // |keys|: key array as raw buffer
    // |values|: value array
    // |key_indexes|: the indexes of keys.
    // |found_key_indexes|: founded indexes of keys
    // |version|: version of values
    Status get_from_immutable_memtable(const Slice* keys, IndexValue* values, const KeyIndexSet& key_indexes,
                                       KeyIndexSet* found_key_indexes, int64_t version) const;

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |key_indexes|: the indexes of keys. If a key is found, its index will be erased.
    // |version|: version of values
    Status get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                             int64_t version) const;

    static void set_difference(KeyIndexSet* key_indexes, const KeyIndexSet& found_key_indexes);

    static void build_index_value_vers(const std::string& key, const std::list<IndexValueWithVer>& index_value_vers,
                                       sstable::TableBuilder* builder);

    std::unique_ptr<sstable::Iterator> prepare_merging_iterator();

    Status merge_sstables(std::unique_ptr<sstable::Iterator> iter_ptr, sstable::TableBuilder* builder);

private:
    std::unique_ptr<PersistentIndexMemtable> _memtable;
    std::unique_ptr<PersistentIndexMemtable> _immutable_memtable{nullptr};
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id{0};
    // The size of sstables is not expected to be too large.
    // In major compaction, some sstables will be picked to be merged into one.
    std::vector<std::unique_ptr<PersistentIndexSstable>> _sstables;
};

} // namespace lake
} // namespace starrocks
