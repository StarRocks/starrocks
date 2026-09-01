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

#include <string>
#include <unordered_map>
#include <vector>

#include "column/vectorized_fwd.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_index.h"

namespace starrocks {

class ParallelUpsertContext;
struct ParallelPublishSlot;

namespace lake {

class Tablet;
class MetaFileBuilder;
class TabletManager;
class LakePersistentIndex;
class LakePersistentIndexParallelCompactMgr;
class SegmentPKIterator;

struct SegmentPKChunkRef;

// Turn SegmentPKChunkRef::owned -- the mask a cross publish puts on a chunk -- into the two shapes
// PrimaryIndex::upsert accepts. Both are no-ops on an ordinary publish, where the mask is empty.
//
// owned_rowids_of: absolute source-segment rowids of the owned rows, in chunk order. Read it off the
// mask BEFORE filtering the column; filtering renumbers the survivors.
std::vector<uint32_t> owned_rowids_of(const SegmentPKChunkRef& current);

class LakePrimaryIndex : public PrimaryIndex {
public:
    LakePrimaryIndex() : PrimaryIndex() {}
    LakePrimaryIndex(const Schema& pk_schema) : PrimaryIndex(pk_schema) {}
    ~LakePrimaryIndex() override = default;

    // Fetch all primary keys from the tablet associated with this index into memory
    // to build a hash index.
    //
    // [thread-safe]
    Status lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                     const MetaFileBuilder* builder);

    bool is_load(int64_t base_version);

    int64_t data_version() const { return _data_version; }
    void update_data_version(int64_t version) { _data_version = version; }

    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> fetch_guard() {
        return std::make_unique<std::lock_guard<std::shared_timed_mutex>>(_mutex);
    }

    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> try_fetch_guard() {
        if (_mutex.try_lock()) {
            return std::make_unique<std::lock_guard<std::shared_timed_mutex>>(_mutex, std::adopt_lock);
        }
        return nullptr;
    }

    std::shared_timed_mutex* get_index_lock() { return &_mutex; }

    Status apply_opcompaction(const TabletMetadataPtr& metadata, const TxnLogPB_OpCompaction& op_compaction);

    Status commit(const TabletMetadataPtr& metadata, MetaFileBuilder* builder, int64_t generation_version = 0);

    // Force any in-memory memtables of the cloud-native persistent index to
    // be flushed into sstables on shared storage. Used by the reshard flush
    // path where the default commit()'s heuristic flush is not sufficient.
    Status sync_flush_persistent_index(int64_t wait_timeout_us);

    Status ingest_sst(const FileMetaPB& sst_meta, const PersistentIndexSstableRangePB& sst_range, uint32_t rssid,
                      int64_t version, const DelvecPagePB& delvec_page, DelVectorPtr delvec);

    // This function is used for handling delete operation in cloud native PK table.
    // Unlike the base PrimaryIndex::erase, it needs `del_rssid` to set up the rebuild point.
    //
    // |key_col| contains the *encoded* primary keys to be deleted from this index.
    // The position of deleted keys will be appended into |new_deletes|.
    //
    // |del_rssid| rssid stamped for these deletes (rowset_id + op_offset). Used as the rebuild point
    // (cloud native index only).
    Status erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes, uint32_t del_rssid);

    // Same as erase(), but applies the delete by ingesting the tombstone sstable |del_sst_meta| that was
    // pre-built at import time, instead of accumulating every tombstone in the memtable and triggering
    // additional flushes. |pks| still supplies the keys reverse-looked-up to build the delete vector; that
    // lookup can run in parallel in both erase paths. Cloud-native index only.
    Status bulk_erase(const TabletMetadataPtr& metadata, const Column& pks, DeletesMap* deletes, uint32_t del_rssid,
                      const FileMetaPB& del_sst_meta, const PersistentIndexSstableRangePB& del_sst_range,
                      int64_t version);

    int32_t current_fileset_index() const;

    StatusOr<AsyncCompactCBPtr> early_sst_compact(LakePersistentIndexParallelCompactMgr* compact_mgr,
                                                  TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                                                  int32_t fileset_start_idx);

    // This function could be called in cloud native persistent index only.
    Status parallel_get(ThreadPoolToken* token, SegmentPKIterator* segment_pk_iterator, DeletesMap* new_deletes);

    // Parallel query of PK index to retrieve rss_rowids for all segments at once.
    // Submits chunks from all segments to a single shared thread pool token, enabling
    // cross-segment parallelism. Each rss_rowids[i] = (rssid << 32 | rowid) for the
    // i-th primary key, or NullIndexValue if the key doesn't exist.
    // Used by column mode partial update to build the update-row-to-source-row mapping.
    // To learn each segment's physical rowid base (range_start), call
    // SegmentPKIterator::physical_rowid_base() on the iterator after this returns.
    // |owned_per_segment|: if non-null, receives each segment's ownership mask -- the concatenation of
    // its chunks' SegmentPKChunkRef::owned, one byte per entry of rss_rowids_per_segment[i]. A segment
    // whose chunks carried no mask (no CrossPublishRowSelector, i.e. every publish but a SPLIT child's
    // cross publish) leaves its entry empty, which every consumer reads as "own every row". Callers
    // that would ACT on a missing key -- inserting the row, allocating an id for it -- need this: the
    // rss_rowid alone cannot tell "no old row" from "a sibling's row".
    Status batch_parallel_get_rss_rowids(ThreadPoolToken* token,
                                         std::vector<std::unique_ptr<SegmentPKIterator>>& pk_iters,
                                         std::vector<std::vector<uint64_t>>* rss_rowids_per_segment,
                                         std::vector<Filter>* owned_per_segment = nullptr);

    // This function will be called when parallel upsert happens.
    // The process flow of parallel upsert is:
    // 1. upsert into memtable. (serialize)
    // 2. parallel get from inactive memtables and sstables. (parallel)
    // 3. Call `flush_memtable`, and flush memtable into sstable when memtable is full. (serialize)
    // Upsert only the rows |current.owned| marks as this tablet's, each keyed to the rowid it has in
    // the SOURCE segment rather than its position among the survivors. |slot->pk_column| holds the
    // encoded keys and is filtered in place.
    //
    // Cross publish only: an ordinary publish carries an empty mask and keeps the plain overload,
    // which needs no per-row rowid vector and works on the in-memory index too.
    //
    // The slot belongs to the caller: it also carries the encoded column, whose bytes the index
    // keeps referencing after this returns when the upsert runs asynchronously. Both call sites hand
    // over a fresh slot, so the append-only scratch inside it always starts empty.
    Status upsert_owned(uint32_t rssid, const SegmentPKChunkRef& current, ParallelPublishSlot* slot,
                        ParallelUpsertContext* context);

    Status parallel_upsert(ThreadPoolToken* token, uint32_t rssid, SegmentPKIterator* segment_pk_iterator,
                           DeletesMap* new_deletes);

    // Flush memtable data into sstable.
    Status flush_memtable(bool force = false);

    // Publish-phase SST flush stats (for cloud native persistent index)
    void reset_publish_sst_stats();
    int32_t publish_sst_flush_count() const;
    int64_t publish_sst_flush_bytes() const;

private:
    Status _do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                         const MetaFileBuilder* builder);

    // The cloud-native index this class delegates to, or nullptr when the index is not loaded.
    // Shared-data primary-key tablets have no other implementation, so the downcast is
    // unconditional -- see the definition for why that holds.
    LakePersistentIndex* _lake_index() const;

private:
    // We don't support multi version in PrimaryIndex yet, but we will record latest data version for some checking
    int64_t _data_version = 0;
    // make sure at most 1 thread is read or write primary index
    std::shared_timed_mutex _mutex;
};

} // namespace lake
} // namespace starrocks
