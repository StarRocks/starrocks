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

#include <functional>

#include "column/vectorized_fwd.h"
#include "storage/lake/lake_persistent_index_key_value_merger.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/persistent_index.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/table_builder.h"

namespace starrocks {
class TxnLogPB;
class TxnLogPB_OpCompaction;
class ParallelUpsertContext;
class ThreadPoolToken;
struct ParallelPublishSlot;

namespace sstable {
class Iterator;
} // namespace sstable

namespace lake {

class MetaFileBuilder;
class SegmentPKIterator;
struct SegmentPKChunkRef;

// Absolute source-segment rowids of the rows SegmentPKChunkRef::owned marks as this tablet's, in
// chunk order. Read it off the mask BEFORE filtering the column; filtering renumbers the survivors
// and the correspondence is lost. Empty mask -- an ordinary, non-cross publish -- yields nothing,
// because every row already belongs here.
std::vector<uint32_t> owned_rowids_of(const SegmentPKChunkRef& current);
class PersistentIndexMemtable;
class PersistentIndexSstable;
class TabletManager;
class PersistentIndexSstableFileset;

// The one primary-key index implementation a shared-data tablet has: a write-ahead memtable in front
// of a stack of sstable filesets on shared storage.
//
// Standalone on purpose. It used to derive from PersistentIndex, the local-disk implementation, but
// took nothing from it except two scalar members and the vtable -- it overrode 7 of that class's 39
// virtuals and inherited an l0/l1/l2 file layout, a DataDir and a PersistentIndexMetaPB it never
// touched. Nothing holds a lake index polymorphically either (UpdateManager's index cache stores
// LakePrimaryIndex by value), so the base bought no dispatch. It still shares the value types --
// IndexValue, KeyIndexSet -- which is a dependency on persistent_index.h, not on its implementation.
//
// Not thread-safe. Callers serialize through LakePrimaryIndex's lock.
class LakePersistentIndex {
public:
    explicit LakePersistentIndex(TabletManager* tablet_mgr, int64_t tablet_id);

    ~LakePersistentIndex();

    DISALLOW_COPY(LakePersistentIndex);

    Status init(const TabletMetadataPtr& metadata);

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    Status get(size_t n, const Slice* keys, IndexValue* values);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |stat|: used for collect statistic
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values, IOStat* stat = nullptr,
                  ParallelUpsertContext* ctx = nullptr);

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    // |del_rssid|: rssid stamped for these deletes (rowset_id + op_offset); used as the rebuild point
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, uint32_t del_rssid);

    // Apply a large delete by ingesting the tombstone sstable |del_sst_meta| that was pre-built at import
    // time (PkTabletWriter::flush_del_file), avoiding tombstone accumulation and additional memtable flushes.
    // Flushes the existing memtable first (so the ingested sstable becomes the newest layer), reverse-looks-up
    // each key's rss_rowid into |old_values| for the delete vector, then ingests the sstable stamped with
    // |del_rssid| and |version| (the sstable was written with entry version 0). Cloud-native only.
    //
    // Precondition: |keys| holds no repeated primary key -- one del file is one memtable flush, and
    // MemTable::_sort aggregates duplicate primary keys away before re-sorting by the sort key. Unlike
    // erase(), which resolves a repeat against the tombstone its first occurrence just wrote and so
    // reports it once, this reverse-looks-up every position independently and would report the same
    // rss_rowid once per occurrence, overshooting the delete vector's cardinality at publish.
    Status bulk_erase(size_t n, const Slice* keys, IndexValue* old_values, uint32_t del_rssid,
                      const FileMetaPB& del_sst_meta, const PersistentIndexSstableRangePB& del_sst_range,
                      int64_t version);

    // batch insert delete operations, used when rebuild index.
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |filter| : used for filter keys that need to skip. `True` means need skip.
    // |version|: version of values
    // |del_rssid|: rssid stamped for these deletes (rowset_id + op_offset); used as the rebuild point
    Status replay_erase(size_t n, const Slice* keys, const std::vector<bool>& filter, int64_t version,
                        uint32_t del_rssid);

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed);

    // batch replace without return old values
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |replace_indexes|: The index of the |keys| array that need to replace.
    Status replace(size_t n, const Slice* keys, const IndexValue* values, const std::vector<uint32_t>& replace_indexes);

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |version|: version of values
    Status insert(size_t n, const Slice* keys, const IndexValue* values, int64_t version);

    Status ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range, uint32_t rssid,
                      int64_t version, const DelvecPagePB& delvec_page, DelVectorPtr delvec);

    static Status major_compact(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, TxnLogPB* txn_log);

    static Status parallel_major_compact(LakePersistentIndexParallelCompactMgr* compact_mgr, TabletManager* tablet_mgr,
                                         const TabletMetadataPtr& metadata, TxnLogPB* txn_log);

    Status apply_opcompaction(const TabletMetadataPtr& metadata, const TxnLogPB_OpCompaction& op_compaction);

    Status commit(MetaFileBuilder* builder, int64_t generation_version = 0);

    Status load_from_lake_tablet(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                                 const MetaFileBuilder* builder);

    size_t memory_usage() const;

    int32_t current_fileset_index() const { return (int32_t)_sstable_filesets.size() - 1; }

    // Fixed encoded key size, or 0 for variable-length keys. Set from the tablet's primary-key
    // schema and encoding type in load_from_lake_tablet(), so it is only meaningful once loaded.
    size_t key_size() const { return _key_size; }

    // During large import, we may have many sst files to ingest and get, so we do parallel compaction to speedup the process.
    StatusOr<AsyncCompactCBPtr> early_sst_compact(lake::LakePersistentIndexParallelCompactMgr* compact_mgr,
                                                  TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                                  int32_t fileset_start_idx);

    static void pick_sstables_for_merge(const PersistentIndexSstableMetaPB& sstable_meta,
                                        std::vector<PersistentIndexSstablePB>* sstables, bool* merge_base_level);

    // Assign the generation version (PersistentIndexSstablePB.generation_version) to each
    // sstable in |new_meta|: a sstable that already carries a non-zero value is left
    // untouched; a file present in |prev_meta| keeps its recorded value (a legacy 0 stays 0,
    // so lake vacuum retention treats it conservatively); a file absent from |prev_meta| is
    // new this publish and is stamped with |generation_version|.
    static void assign_generation_versions(PersistentIndexSstableMetaPB* new_meta,
                                           const PersistentIndexSstableMetaPB& prev_meta, int64_t generation_version);

    // Check if this rowset need to rebuild, return `True` means need to rebuild this rowset.
    static bool needs_rowset_rebuild(const RowsetMetadataPB& rowset, uint32_t rebuild_rss_id);

    // Return the {file_cnt, row_cnt} that need to rebuild in a single rowset traversal.
    static std::pair<size_t, int64_t> need_rebuild_counts(const TabletMetadataPB& metadata,
                                                          const PersistentIndexSstableMetaPB& sstable_meta);

    // Stamp the version that every subsequent memtable entry carries. Called once per publish,
    // before any upsert/erase/replace.
    //
    // A plain version number, not an EditVersion: only the major number ever reached the memtable,
    // so holding both halves meant callers built an `EditVersion(v, 0)` for the minor to be dropped
    // again on the way in. (Before #78570 this arrived through the inherited
    // PersistentIndex::prepare(version, n), which also flipped four flags only the local-disk
    // implementation reads.)
    void set_publish_version(int64_t version) { _publish_version = version; }

    Status flush_memtable(bool force = false);

    Status sync_flush_all_memtables(int64_t wait_timeout_us);

    // Publish-phase SST flush stats tracking
    void reset_publish_sst_stats() {
        _publish_sst_flush_count = 0;
        _publish_sst_flush_bytes = 0;
    }
    int32_t publish_sst_flush_count() const { return _publish_sst_flush_count; }
    int64_t publish_sst_flush_bytes() const { return _publish_sst_flush_bytes; }

    // ---- Row-oriented publish API --------------------------------------------------------------
    //
    // The batch methods above take encoded keys as a Slice array; these take a rowset's primary-key
    // Column plus the (rssid, rowid) coordinates its rows occupy, and encode the keys themselves.
    // Overloading is safe -- no Column-based signature is convertible to a Slice-based one -- and
    // keeps one name per operation regardless of how the caller holds its keys.

    Status get(const Column& pks, std::vector<uint64_t>* rowids);

    Status upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t idx_begin, uint32_t idx_end,
                  DeletesMap* deletes);

    Status upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, ParallelPublishSlot* slot,
                  ParallelUpsertContext* ctx);

    // Each key keyed to its own rowid rather than to rowid_start + position. Cross publish only:
    // the rows a child owns are scattered through the source segment, so they are not contiguous.
    Status upsert(uint32_t rssid, const std::vector<uint32_t>& rowids, const Column& pks, ParallelPublishSlot* slot,
                  ParallelUpsertContext* ctx);

    Status erase(const Column& pks, DeletesMap* deletes, uint32_t del_rssid);

    Status bulk_erase(const Column& pks, DeletesMap* deletes, uint32_t del_rssid, const FileMetaPB& del_sst_meta,
                      const PersistentIndexSstableRangePB& del_sst_range, int64_t version);

    Status replace(uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                   const Column& pks);

    Status try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed);

    // ---- Parallel publish, across chunks ---------------------------------------------------------
    //
    // Spread a rowset's chunks over |token|, which the publish supplies. This is the caller-driven
    // fan-out axis; the internal one (parallel_reverse_lookup, _open_sstables_parallel) splits a
    // single batch across key subsets and sstable files on pk_index_execution_thread_pool. A task
    // running on |token| must not enter the internal axis, or the two pools wait on each other.

    Status parallel_get(ThreadPoolToken* token, SegmentPKIterator* segment_pk_iterator, DeletesMap* new_deletes);

    // Retrieve rss_rowids for all segments at once, submitting every segment's chunks to one shared
    // token so the fan-out crosses segments rather than restarting per segment.
    Status batch_parallel_get_rss_rowids(ThreadPoolToken* token,
                                         std::vector<std::unique_ptr<SegmentPKIterator>>& pk_iters,
                                         std::vector<std::vector<uint64_t>>* rss_rowids_per_segment,
                                         std::vector<Filter>* owned_per_segment);

    Status parallel_upsert(ThreadPoolToken* token, uint32_t rssid, SegmentPKIterator* segment_pk_iterator,
                           DeletesMap* new_deletes);

    // Upsert only the rows |current.owned| marks as this tablet's, each keyed to the rowid it has in
    // the SOURCE segment rather than its position among the survivors. |slot->pk_column| holds the
    // encoded keys and is filtered in place. Cross publish only: an ordinary publish carries an empty
    // mask and keeps the plain overload, which needs no per-row rowid vector.
    //
    // Parallel upsert runs in three steps, and this is step 1:
    // 1. upsert into the memtable                                      (serial)
    // 2. resolve the replaced rowids from inactive memtables and ssts  (parallel)
    // 3. flush_memtable(), which writes an sst once the memtable fills  (serial)
    //
    // The slot belongs to the caller: it also carries the encoded column, whose bytes the index keeps
    // referencing after this returns when the lookup is deferred. Every call site hands over a fresh
    // slot, so the append-only scratch inside it always starts empty.
    Status upsert_owned(uint32_t rssid, const SegmentPKChunkRef& current, ParallelPublishSlot* slot,
                        ParallelUpsertContext* ctx);

private:
    // Open all SSTables in parallel using thread pool.
    // Returns opened SSTables in the same order as sstable_meta.sstables().
    static StatusOr<std::vector<PersistentIndexSstableUniquePtr>> _open_sstables_parallel(
            const PersistentIndexSstableMetaPB& sstable_meta, TabletManager* tablet_mgr, int64_t tablet_id,
            Cache* cache, const TabletMetadataPtr& metadata);

    bool is_memtable_full() const;

    bool too_many_rebuild_files() const;

    bool too_many_rebuild_rows() const;

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |key_indexes|: the indexes of keys. If a key is found, its index will be erased.
    // |version|: version of values
    Status get_from_sstables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                             int64_t version) const;

    Status get_from_inactive_memtables(size_t n, const Slice* keys, IndexValue* values, KeyIndexSet* key_indexes,
                                       int64_t version) const;

    // Reverse-look up |num_tasks| subsets of positions from the inactive memtables + sstables into
    // |old_values|, fanning out on pk_index_execution_thread_pool when |parallel_worthwhile|, else serial.
    // |make_subset(i)| yields task i's key indexes and is invoked inside the task, so a caller iterating a
    // contiguous range never materializes one giant KeyIndexSet. Shared by erase() and bulk_erase().
    Status parallel_reverse_lookup(size_t n, const Slice* keys, IndexValue* old_values, size_t num_tasks,
                                   const std::function<KeyIndexSet(size_t)>& make_subset, bool parallel_worthwhile);

    // rebuild delete operation from rowset.
    Status load_dels(const RowsetPtr& rowset, const Schema& pkey_schema, int64_t rowset_version);

    static void set_difference(KeyIndexSet* key_indexes, const KeyIndexSet& found_key_indexes);

    // get sstable's iterator that need to compact and modify txn_log
    static Status prepare_merging_iterator(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                           TxnLogPB* txn_log,
                                           std::vector<std::shared_ptr<PersistentIndexSstable>>* merging_sstables,
                                           std::unique_ptr<sstable::Iterator>* merging_iter_ptr, bool* merge_base_level,
                                           bool* contain_shared_sstables);

    static StatusOr<std::vector<KeyValueMerger::KeyValueMergerOutput>> merge_sstables(
            std::unique_ptr<sstable::Iterator> iter_ptr, bool base_level_merge, TabletManager* tablet_mgr,
            const TabletMetadataPtr& metadata, bool contain_shared_sstables);

    Status merge_sstable_into_fileset(std::unique_ptr<PersistentIndexSstable>& sstable);

private:
    std::shared_ptr<PersistentIndexMemtable> _memtable;
    std::vector<std::shared_ptr<PersistentIndexMemtable>> _inactive_memtables;
    TabletManager* _tablet_mgr{nullptr};
    int64_t _tablet_id{0};
    size_t _need_rebuild_file_cnt{0};
    int64_t _need_rebuild_row_cnt{0};
    // Collection of sstable fileset, from old to new.
    std::vector<std::unique_ptr<PersistentIndexSstableFileset>> _sstable_filesets;

    // Counters for SST files flushed during publish phase
    int32_t _publish_sst_flush_count{0};
    int64_t _publish_sst_flush_bytes{0};

    // Fixed encoded key size, or 0 for variable-length keys. Set from the PK schema in
    // load_from_lake_tablet(), which is also where it was set while this derived from PersistentIndex.
    size_t _key_size{0};
    // The version stamped on memtable entries; see set_publish_version().
    int64_t _publish_version{0};
};

} // namespace lake
} // namespace starrocks
