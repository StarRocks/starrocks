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

#include "storage/del_vector.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "util/dynamic_cache.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/threadpool.h"

namespace starrocks {

namespace lake {

class TxnLogPB_OpWrite;
class LocationProvider;
class Tablet;
class MetaFileBuilder;
class UpdateManager;
struct AutoIncrementPartialUpdateState;
using IndexEntry = DynamicCache<uint64_t, LakePrimaryIndex>::Entry;

class LakeDelvecLoader : public DelvecLoader {
public:
    LakeDelvecLoader(UpdateManager* update_mgr, const MetaFileBuilder* pk_builder)
            : _update_mgr(update_mgr), _pk_builder(pk_builder) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

private:
    UpdateManager* _update_mgr = nullptr;
    const MetaFileBuilder* _pk_builder = nullptr;
};

class UpdateManager {
public:
    UpdateManager(LocationProvider* location_provider, MemTracker* mem_tracker = nullptr);
    ~UpdateManager() {}
    void set_tablet_mgr(TabletManager* tablet_mgr) { _tablet_mgr = tablet_mgr; }
    void set_cache_expire_ms(int64_t expire_ms) { _cache_expire_ms = expire_ms; }

    int64_t get_cache_expire_ms() const { return _cache_expire_ms; }

    // publish primary key tablet, update primary index and delvec, then update meta file
    Status publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, int64_t txn_id, const TabletMetadata& metadata,
                                      Tablet* tablet, IndexEntry* index_entry, MetaFileBuilder* builder,
                                      int64_t base_version);

    // get rowids from primary index by each upserts
    Status get_rowids_from_pkindex(Tablet* tablet, int64_t base_version, const std::vector<ColumnUniquePtr>& upserts,
                                   std::vector<std::vector<uint64_t>*>* rss_rowids);
    Status get_rowids_from_pkindex(Tablet* tablet, int64_t base_version, const std::vector<ColumnUniquePtr>& upserts,
                                   std::vector<std::vector<uint64_t>>* rss_rowids);

    // get column data by rssid and rowids
    Status get_column_values(Tablet* tablet, const TabletMetadata& metadata, const TxnLogPB_OpWrite& op_write,
                             const TabletSchema& tablet_schema, std::vector<uint32_t>& column_ids, bool with_default,
                             std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                             vector<std::unique_ptr<Column>>* columns,
                             AutoIncrementPartialUpdateState* auto_increment_state = nullptr);
    // get delvec by version
    Status get_del_vec(const TabletSegmentId& tsid, int64_t version, const MetaFileBuilder* builder,
                       DelVectorPtr* pdelvec);

    // get delvec from tablet meta file
    Status get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, DelVector* delvec);
    // set delvec cache
    Status set_cached_del_vec(const std::vector<std::pair<TabletSegmentId, DelVectorPtr>>& cache_delvec_updates,
                              int64_t version);

    // get del nums from rowset, for compaction policy
    size_t get_rowset_num_deletes(int64_t tablet_id, int64_t version, const RowsetMetadataPB& rowset_meta);

    Status publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction, const TabletMetadata& metadata,
                                      Tablet* tablet, IndexEntry* index_entry, MetaFileBuilder* builder,
                                      int64_t base_version);

    // remove primary index entry from cache, called when publish version error happens.
    // Because update primary index isn't idempotent, so if primary index update success, but
    // publish failed later, need to clear primary index.
    void remove_primary_index_cache(uint32_t tablet_id);

    bool try_remove_primary_index_cache(uint32_t tablet_id);

    // if base version != index.data_version, need to clear index cache
    Status check_meta_version(const Tablet& tablet, int64_t base_version);

    // update primary index data version when meta file finalize success.
    void update_primary_index_data_version(const Tablet& tablet, int64_t version);

    void expire_cache();

    void evict_cache(int64_t memory_urgent_level, int64_t memory_high_level);
    void preload_update_state(const TxnLog& op_write, Tablet* tablet);

    // check if pk index's cache ref == ref_cnt
    bool TEST_check_primary_index_cache_ref(uint32_t tablet_id, uint32_t ref_cnt);

    bool TEST_check_update_state_cache_noexist(uint32_t tablet_id, int64_t txn_id);

    Status update_primary_index_memory_limit(int32_t update_memory_limit_percent) {
        int64_t byte_limits = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
        int32_t update_mem_percent = std::max(std::min(100, update_memory_limit_percent), 0);
        _index_cache.set_capacity(byte_limits * update_mem_percent);
        return Status::OK();
    }

    MemTracker* compaction_state_mem_tracker() const { return _compaction_state_mem_tracker.get(); }

    // get or create primary index, and prepare primary index state
    StatusOr<IndexEntry*> prepare_primary_index(const TabletMetadata& metadata, Tablet* tablet,
                                                MetaFileBuilder* builder, int64_t base_version, int64_t new_version);

    // commit primary index, only take affect when it is local persistent index
    Status commit_primary_index(IndexEntry* index_entry, Tablet* tablet);

    // release index entry if it isn't nullptr
    void release_primary_index(IndexEntry* index_entry);

    void lock_shard_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).lock_shared(); }

    void unlock_shard_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).unlock_shared(); }

    bool try_lock_pk_index_shard(int64_t tablet_id) { return _get_pk_index_shard_lock(tablet_id).try_lock(); }

    void unlock_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).unlock(); }

private:
    // print memory tracker state
    void _print_memory_stats();
    Status _do_update(uint32_t rowset_id, int32_t upsert_idx, const std::vector<ColumnUniquePtr>& upserts,
                      PrimaryIndex& index, int64_t tablet_id, DeletesMap* new_deletes);

    Status _do_update_with_condition(Tablet* tablet, const TabletMetadata& metadata, const TxnLogPB_OpWrite& op_write,
                                     const TabletSchema& tablet_schema, uint32_t rowset_id, int32_t upsert_idx,
                                     int32_t condition_column, const std::vector<ColumnUniquePtr>& upserts,
                                     PrimaryIndex& index, int64_t tablet_id, DeletesMap* new_deletes);

    int32_t _get_condition_column(const TxnLogPB_OpWrite& op_write, const TabletSchema& tablet_schema);

    Status _handle_index_op(Tablet* tablet, int64_t base_version, const std::function<void(LakePrimaryIndex&)>& op);

    std::shared_mutex& _get_pk_index_shard_lock(int64_t tabletId) { return _get_pk_index_shard(tabletId).lock; }

    struct PkIndexShard {
        mutable std::shared_mutex lock;
    };

    PkIndexShard& _get_pk_index_shard(int64_t tabletId) {
        return _pk_index_shards[tabletId & (config::pk_index_map_shard_size - 1)];
    }

    static const size_t kPrintMemoryStatsInterval = 300; // 5min
private:
    // default 6min
    int64_t _cache_expire_ms = 360000;
    // primary index
    DynamicCache<uint64_t, LakePrimaryIndex> _index_cache;

    // rowset cache
    DynamicCache<string, RowsetUpdateState> _update_state_cache;
    std::atomic<int64_t> _last_clear_expired_cache_millis = 0;
    LocationProvider* _location_provider = nullptr;
    TabletManager* _tablet_mgr = nullptr;

    // memory checkers
    MemTracker* _update_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _index_cache_mem_tracker;
    std::unique_ptr<MemTracker> _update_state_mem_tracker;
    std::unique_ptr<MemTracker> _compaction_state_mem_tracker;

    std::vector<PkIndexShard> _pk_index_shards;
};

} // namespace lake

} // namespace starrocks