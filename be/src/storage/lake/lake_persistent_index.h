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

#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/sstable_predicate_utils.h"
#include "storage/sstable/table_builder.h"

namespace starrocks {
class TxnLogPB;
class TxnLogPB_OpCompaction;

namespace sstable {
class Iterator;
} // namespace sstable

namespace lake {

using KeyIndex = size_t;
using KeyIndexSet = std::set<KeyIndex>;
class MetaFileBuilder;
class PersistentIndexMemtable;
class PersistentIndexSstable;
class TabletManager;
class PersistentIndexSstableFileset;
class LakePersistentIndexParallelCompactMgr;

class KeyValueMerger {
public:
    explicit KeyValueMerger(const std::string& key, uint64_t max_rss_rowid, bool merge_base_level,
                            TabletManager* tablet_mgr, int64_t tablet_id, bool enable_multiple_output_files)
            : _key(std::move(key)),
              _max_rss_rowid(max_rss_rowid),
              _merge_base_level(merge_base_level),
              _tablet_mgr(tablet_mgr),
              _tablet_id(tablet_id),
              _enable_multiple_output_files(enable_multiple_output_files) {}

    ~KeyValueMerger();

    struct TableBuilderWrapper {
        std::unique_ptr<sstable::TableBuilder> table_builder;
        std::string filename;
        std::string encryption_meta;
        std::unique_ptr<WritableFile> wf;
        std::unique_ptr<sstable::FilterPolicy> filter_policy;
    };

    struct KeyValueMergerOutput {
        std::string filename;
        uint64_t filesize;
        std::string encryption_meta;
        std::string start_key;
        std::string end_key;
    };

    Status merge(const sstable::Iterator* iter_ptr);

    // return list<filename, filesize, encryption_meta, start_key, end_key>
    StatusOr<std::vector<KeyValueMergerOutput>> finish();

    Status create_table_builder();

private:
    Status flush();

private:
    std::string _key;
    uint64_t _max_rss_rowid = 0;
    std::list<IndexValueWithVer> _index_value_vers;
    // If do merge base level, that means we can delete NullIndexValue items safely.
    bool _merge_base_level = false;
    TabletManager* _tablet_mgr = nullptr;
    int64_t _tablet_id = 0;
    sstable::CachedPredicateEvaluator _predicate_evaluator;
    // Enable multiple output files. We will generate multiple output files when
    // data volume larger than pk_index_target_file_size.
    bool _enable_multiple_output_files = false;
    std::vector<TableBuilderWrapper> _output_builders;
};

// LakePersistentIndex is not thread-safe.
// Caller should take care of the multi-thread safety
class LakePersistentIndex : public PersistentIndex {
public:
    explicit LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id);

    ~LakePersistentIndex();

    DISALLOW_COPY(LakePersistentIndex);

    Status init(const TabletMetadataPtr& metadata);

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

    Status ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range, uint32_t rssid,
                      int64_t version, const DelvecPagePB& delvec_page, DelVectorPtr delvec);

    static Status major_compact(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, TxnLogPB* txn_log);

    static Status parallel_major_compact(LakePersistentIndexParallelCompactMgr* compact_mgr, TabletManager* tablet_mgr,
                                         const TabletMetadataPtr& metadata, TxnLogPB* txn_log);

    Status apply_opcompaction(const TxnLogPB_OpCompaction& op_compaction);

    Status commit(MetaFileBuilder* builder);

    Status load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                 const MetaFileBuilder* builder);

    size_t memory_usage() const override;

    int32_t current_fileset_index() const { return (int32_t)_sstable_filesets.size() - 1; }

    // During large import, we may have many sst files to ingest and get, so we do parallel compaction to speedup the process.
    StatusOr<AsyncCompactCBPtr> ingest_sst_compact(lake::LakePersistentIndexParallelCompactMgr* compact_mgr,
                                                   TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                                   int32_t fileset_start_idx);

    static void pick_sstables_for_merge(const PersistentIndexSstableMetaPB& sstable_meta,
                                        std::vector<PersistentIndexSstablePB>* sstables, bool* merge_base_level);

    // Check if this rowset need to rebuild, return `True` means need to rebuild this rowset.
    static bool needs_rowset_rebuild(const RowsetMetadataPB& rowset, uint32_t rebuild_rss_id);

    // Return the files cnt that need to rebuild.
    static size_t need_rebuild_file_cnt(const TabletMetadataPB& metadata,
                                        const PersistentIndexSstableMetaPB& sstable_meta);

private:
    Status flush_memtable();

    bool is_memtable_full() const;

    bool too_many_rebuild_files() const;

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
    static Status prepare_merging_iterator(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                           TxnLogPB* txn_log,
                                           std::vector<std::shared_ptr<PersistentIndexSstable>>* merging_sstables,
                                           std::unique_ptr<sstable::Iterator>* merging_iter_ptr,
                                           bool* merge_base_level);

    static StatusOr<std::vector<KeyValueMerger::KeyValueMergerOutput>> merge_sstables(
            std::unique_ptr<sstable::Iterator> iter_ptr, bool base_level_merge, TabletManager* tablet_mgr,
            int64_t tablet_id);

    Status merge_sstable_into_fileset(std::unique_ptr<PersistentIndexSstable>& sstable);

    void print_debug_info() const;

private:
    std::unique_ptr<PersistentIndexMemtable> _memtable;
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id{0};
    size_t _need_rebuild_file_cnt{0};
    // Collection of sstable fileset, from old to new.
    std::vector<std::unique_ptr<PersistentIndexSstableFileset>> _sstable_filesets;
    // total write bytes
    size_t _total_write_bytes{0};
    // total compaction bytes
    size_t _total_compaction_bytes{0};
};

} // namespace lake
} // namespace starrocks
