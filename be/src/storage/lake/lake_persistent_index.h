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

#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/persistent_index.h"

namespace starrocks {
class TxnLogPB;
class TxnLogPB_OpCompaction;

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

class KeyValueMerger {
public:
    explicit KeyValueMerger(const std::string& key, uint64_t max_rss_rowid, sstable::TableBuilder* builder,
                            bool merge_base_level)
            : _key(std::move(key)),
              _max_rss_rowid(max_rss_rowid),
              _builder(builder),
              _merge_base_level(merge_base_level) {}

    Status merge(const std::string& key, const std::string& value, uint64_t max_rss_rowid);

    void finish() { flush(); }

private:
    void flush();

private:
    std::string _key;
    uint64_t _max_rss_rowid = 0;
    sstable::TableBuilder* _builder;
    std::list<IndexValueWithVer> _index_value_vers;
    // If do merge base level, that means we can delete NullIndexValue items safely.
    bool _merge_base_level = false;
};

// LakePersistentIndex is not thread-safe.
// Caller should take care of the multi-thread safety
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
    // |rowset_id|: The rowset that keys belong to. Used for setup rebuild point
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, uint32_t rowset_id);

    // Use erase with `rowset_id` instead of this one.
    Status erase(size_t n, const Slice* keys, IndexValue* old_values) override {
        return Status::NotSupported("LakePersistentIndex::erase not supported");
    }

    // batch insert delete operations, used when rebuild index.
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |filter| : used for filter keys that need to skip. `True` means need skip.
    // |version|: version of values
    // |rowset_id|: The rowset that keys belong to. Used for setup rebuild point
    Status replay_erase(size_t n, const Slice* keys, const std::vector<bool>& filter, int64_t version,
                        uint32_t rowset_id);

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed) override;

    // batch replace without return old values
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |replace_indexes|: The index of the |keys| array that need to replace.
    Status replace(size_t n, const Slice* keys, const IndexValue* values,
                   const std::vector<uint32_t>& replace_indexes) override;

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |version|: version of values
    Status insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version);

    Status minor_compact();

    static Status major_compact(TabletManager* tablet_mgr, const TabletMetadata& metadata, TxnLogPB* txn_log);

    Status apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction);

    Status commit(MetaFileBuilder* builder);

    Status load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                 const MetaFileBuilder* builder);

    size_t memory_usage() const override;

    static void pick_sstables_for_merge(const PersistentIndexSstableMetaPB& sstable_meta,
                                        std::vector<PersistentIndexSstablePB>* sstables, bool* merge_base_level);

    // Check if this rowset need to rebuild, return `True` means need to rebuild this rowset.
    static bool needs_rowset_rebuild(const RowsetMetadataPB& rowset, uint32_t rebuild_rss_id);

private:
    Status flush_memtable();

    bool is_memtable_full() const;

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |key_indexes|: the indexes of keys. If a key is found, its index will be erased.
    // |version|: version of values
    Status get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                             int64_t version) const;

    // rebuild delete operation from rowset.
    Status load_dels(const RowsetPtr& rowset, const Schema& pkey_schema, int64_t rowset_version);

    static void set_difference(KeyIndexSet* key_indexes, const KeyIndexSet& found_key_indexes);

    // get sstable's iterator that need to compact and modify txn_log
    static Status prepare_merging_iterator(TabletManager* tablet_mgr, const TabletMetadata& metadata, TxnLogPB* txn_log,
                                           std::vector<std::shared_ptr<PersistentIndexSstable>>* merging_sstables,
                                           std::unique_ptr<sstable::Iterator>* merging_iter_ptr,
                                           bool* merge_base_level);

    static Status merge_sstables(std::unique_ptr<sstable::Iterator> iter_ptr, sstable::TableBuilder* builder,
                                 bool base_level_merge);

private:
    std::unique_ptr<PersistentIndexMemtable> _memtable;
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id{0};
    // The size of sstables is not expected to be too large.
    // In major compaction, some sstables will be picked to be merged into one.
    // sstables are ordered with the smaller version on the left.
    std::vector<std::unique_ptr<PersistentIndexSstable>> _sstables;
};

} // namespace lake
} // namespace starrocks
