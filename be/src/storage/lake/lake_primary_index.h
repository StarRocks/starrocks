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

#include <memory>
#include <mutex>
#include <shared_mutex>
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

// The tablet-level primary-key index of a shared-data tablet: owns the load state, the reader/writer
// lock the publish path serializes on, and the one LakePersistentIndex that does the work.
//
// Standalone on purpose. It used to derive from PrimaryIndex, the shared-nothing implementation, and
// inherited a surface it could not use -- load(Tablet*), reset(Tablet*), major_compaction(DataDir*),
// commit(PersistentIndexMetaPB*), on_commited(), abort(), insert(), replace() -- plus an in-memory
// hash index that PrimaryIndex::_set_schema() allocated for every primary-key tablet and that a lake
// tablet never reads. Nothing held a lake index through a PrimaryIndex pointer (UpdateManager's index
// cache stores this class by value), so the base bought no dispatch, only reach.
//
// What it still borrows from primary_index.h are three shared names: the DeletesMap shape,
// ROWID_MASK, and the static build_persistent_keys() key-marshalling helper.
class LakePrimaryIndex {
public:
    using segment_rowid_t = uint32_t;
    using DeletesMap = std::unordered_map<uint32_t, std::vector<segment_rowid_t>>;

    LakePrimaryIndex() = default;
    ~LakePrimaryIndex();

    LakePrimaryIndex(const LakePrimaryIndex&) = delete;
    LakePrimaryIndex& operator=(const LakePrimaryIndex&) = delete;

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

    // ---- Surface the publish path shares with the shared-nothing index --------------------------
    // These used to be inherited from PrimaryIndex. Kept, with the same signatures, because
    // UpdateManager and the compaction-conflict resolver call them on a LakePrimaryIndex.

    // Look up each primary key's current rss_rowid, or NullIndexValue when absent.
    Status get(const Column& pks, std::vector<uint64_t>* rowids) const;

    // Insert or update pks[idx_begin, idx_end) at (rssid, rowid_start + i), collecting whatever
    // rowids they replaced into |deletes|.
    Status upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t idx_begin, uint32_t idx_end,
                  DeletesMap* deletes);

    // Parallel-publish overloads. The memtable write happens synchronously here; the lookup of the
    // rowids being replaced is deferred to `ctx`'s runner when it has one, which is why `slot` --
    // whose pk_column owns the bytes the lookup reads -- must outlive the join. The context receives
    // the replaced rowids either way; the caller must not append them itself. See
    // ParallelUpsertContext::defers_lookup().
    //
    // The second form addresses an arbitrary subset of rows by absolute rowid: pks[i] lands at
    // (rssid, rowids[i]).
    Status upsert(uint32_t rssid, uint32_t rowid_start, const Column& pks, ParallelPublishSlot* slot,
                  ParallelUpsertContext* ctx);
    Status upsert(uint32_t rssid, const std::vector<uint32_t>& rowids, const Column& pks, ParallelPublishSlot* slot,
                  ParallelUpsertContext* ctx);

    // Point pks[replace_indexes[i]] at (rssid, rowid_start + replace_indexes[i]). Used by the
    // compaction conflict resolver to hand it the rows that survived.
    Status replace(uint32_t rssid, uint32_t rowid_start, const std::vector<uint32_t>& replace_indexes,
                   const Column& pks);

    // Replace the entries whose current rss_rowid is at or below |max_src_rssid|, reporting the
    // positions that did not match in |failed|. Used by compaction apply.
    Status try_replace(uint32_t rssid, uint32_t rowid_start, const Column& pks, uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed);

    // Stamp the version every memtable entry written by this publish carries. Called once, before
    // any upsert/erase, by UpdateManager::prepare_primary_index.
    Status prepare(int64_t version);

    // Drop the loaded index, so the next lake_load() rebuilds it. [thread-safe]
    void unload();

    bool is_loaded() const;
    Status get_load_status() const;
    std::size_t memory_usage() const;
    std::string to_string() const;

private:
    Status _do_lake_load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata, int64_t base_version,
                         const MetaFileBuilder* builder);

    void _unload_without_lock();

private:
    // The index implementation, or null while unloaded. Typed, so reaching it needs no downcast --
    // this member used to be PrimaryIndex's shared_ptr<PersistentIndex>, which cost a
    // dynamic_cast at every one of the delegating methods below.
    std::shared_ptr<LakePersistentIndex> _index;

    // Guards the load bookkeeping (_loaded / _status / _index), not the index contents.
    mutable std::mutex _load_lock;
    bool _loaded = false;
    Status _status;

    int64_t _tablet_id = 0;

    // We don't support multi version yet, but we record the latest data version for some checking
    int64_t _data_version = 0;
    // make sure at most 1 thread is read or write primary index
    std::shared_timed_mutex _mutex;
};

// DynamicCache logs its values (see dynamic_cache.h), so the cached type has to be streamable.
inline std::ostream& operator<<(std::ostream& os, const LakePrimaryIndex& o) {
    os << o.to_string();
    return os;
}

} // namespace lake
} // namespace starrocks
