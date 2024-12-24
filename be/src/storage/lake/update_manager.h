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
#include "storage/lake/update_compaction_state.h"
#include "util/dynamic_cache.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/threadpool.h"

namespace starrocks {

class Cache;
class TxnLogPB_OpWrite;

namespace lake {

class LocationProvider;
class Tablet;
class MetaFileBuilder;
class UpdateManager;
struct AutoIncrementPartialUpdateState;
using IndexEntry = DynamicCache<uint64_t, LakePrimaryIndex>::Entry;

class PersistentIndexBlockCache {
public:
    explicit PersistentIndexBlockCache(MemTracker* mem_tracker, int64_t cache_limit);

    void update_memory_usage();

    Cache* cache() { return _cache.get(); }

private:
    std::mutex _mutex;
    size_t _memory_usage{0};
    std::unique_ptr<Cache> _cache;
    std::unique_ptr<MemTracker> _mem_tracker;
};

// This is a converter between rowset segment id and segment file info. We need this converter
// because we store rowset segment id instead of real file info in PrimaryKey Index value to save memory,
// and this converter can help us to find segment file info that we want.
class RssidFileInfoContainer {
public:
    void add_rssid_to_file(const TabletMetadata& metadata);
    void add_rssid_to_file(const RowsetMetadataPB& meta, uint32_t rowset_id, uint32_t segment_id,
                           const std::map<int, FileInfo>& replace_segments);

    const std::unordered_map<uint32_t, FileInfo>& rssid_to_file() const { return _rssid_to_file_info; }
    const std::unordered_map<uint32_t, uint32_t>& rssid_to_rowid() const { return _rssid_to_rowid; }

private:
    // From rowset segment id to segment file
    std::unordered_map<uint32_t, FileInfo> _rssid_to_file_info;
    // From rowset segment id to rowset id
    std::unordered_map<uint32_t, uint32_t> _rssid_to_rowid;
};

class UpdateManager {
public:
    UpdateManager(std::shared_ptr<LocationProvider> location_provider, MemTracker* mem_tracker);
    ~UpdateManager();
    void set_tablet_mgr(TabletManager* tablet_mgr) { _tablet_mgr = tablet_mgr; }
    void set_cache_expire_ms(int64_t expire_ms) { _cache_expire_ms = expire_ms; }

    int64_t get_cache_expire_ms() const { return _cache_expire_ms; }

    // publish primary key tablet, update primary index and delvec, then update meta file
    Status publish_primary_key_tablet(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                      const TabletMetadataPtr& metadata, Tablet* tablet, IndexEntry* index_entry,
                                      MetaFileBuilder* builder, int64_t base_version);

    Status publish_column_mode_partial_update(const TxnLogPB_OpWrite& op_write, int64_t txn_id,
                                              const TabletMetadataPtr& metadata, Tablet* tablet,
                                              MetaFileBuilder* builder, int64_t base_version);

    // get rowids from primary index by each upserts
    Status get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version, const std::vector<ColumnUniquePtr>& upserts,
                                   std::vector<std::vector<uint64_t>*>* rss_rowids, bool need_lock);
    Status get_rowids_from_pkindex(int64_t tablet_id, int64_t base_version, const ColumnUniquePtr& upsert,
                                   std::vector<uint64_t>* rss_rowids, bool need_lock);

    // get column data by rssid and rowids
    Status get_column_values(const RowsetUpdateStateParams& params, std::vector<uint32_t>& column_ids,
                             bool with_default, std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                             vector<std::unique_ptr<Column>>* columns,
                             const std::map<string, string>* column_to_expr_value = nullptr,
                             AutoIncrementPartialUpdateState* auto_increment_state = nullptr);
    // get delvec by version
    Status get_del_vec(const TabletSegmentId& tsid, int64_t version, const MetaFileBuilder* builder, bool fill_cache,
                       DelVectorPtr* pdelvec);

    // get delvec from tablet meta file
    Status get_del_vec_in_meta(const TabletSegmentId& tsid, int64_t meta_ver, bool fill_cache, DelVector* delvec);
    // set delvec cache
    Status set_cached_del_vec(const std::vector<std::pair<TabletSegmentId, DelVectorPtr>>& cache_delvec_updates,
                              int64_t version);

    // get del nums from rowset, for compaction policy
    size_t get_rowset_num_deletes(int64_t tablet_id, int64_t version, const RowsetMetadataPB& rowset_meta);

    Status publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                      const TabletMetadata& metadata, const Tablet& tablet, IndexEntry* index_entry,
                                      MetaFileBuilder* builder, int64_t base_version);

    Status light_publish_primary_compaction(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                            const TabletMetadata& metadata, const Tablet& tablet,
                                            IndexEntry* index_entry, MetaFileBuilder* builder, int64_t base_version);

    bool try_remove_primary_index_cache(uint32_t tablet_id);

    void unload_primary_index(int64_t tablet_id);

    // if base version != index.data_version, need to clear index cache
    Status check_meta_version(const Tablet& tablet, int64_t base_version);

    // update primary index data version when meta file finalize success.
    void update_primary_index_data_version(const Tablet& tablet, int64_t version);

    int64_t get_primary_index_data_version(int64_t tablet_id);

    void expire_cache();

    void evict_cache(int64_t memory_urgent_level, int64_t memory_high_level);
    void preload_update_state(const TxnLog& op_write, Tablet* tablet);
    void preload_compaction_state(const TxnLog& txnlog, const Tablet& tablet, const TabletSchemaCSPtr& tablet_schema);

    // check if pk index's cache ref == ref_cnt
    bool TEST_check_primary_index_cache_ref(uint32_t tablet_id, uint32_t ref_cnt);

    bool TEST_check_update_state_cache_absent(uint32_t tablet_id, int64_t txn_id);
    bool TEST_check_compaction_cache_absent(uint32_t tablet_id, int64_t txn_id);
    void TEST_remove_compaction_cache(uint32_t tablet_id, int64_t txn_id);

    Status update_primary_index_memory_limit(int32_t update_memory_limit_percent) {
        int64_t byte_limits = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
        int32_t update_mem_percent = std::max(std::min(100, update_memory_limit_percent), 0);
        _index_cache.set_capacity(byte_limits * update_mem_percent);
        return Status::OK();
    }

    MemTracker* compaction_state_mem_tracker() const { return _compaction_state_mem_tracker.get(); }

    MemTracker* update_state_mem_tracker() const { return _update_state_mem_tracker.get(); }

    MemTracker* index_mem_tracker() const { return _index_cache_mem_tracker.get(); }

    MemTracker* mem_tracker() const { return _update_mem_tracker; }

    // get or create primary index, and prepare primary index state
    StatusOr<IndexEntry*> prepare_primary_index(const TabletMetadataPtr& metadata, MetaFileBuilder* builder,
                                                int64_t base_version, int64_t new_version,
                                                std::unique_ptr<std::lock_guard<std::shared_timed_mutex>>& lock);

    // release index entry if it isn't nullptr
    void release_primary_index_cache(IndexEntry* index_entry);
    // remove index entry if it isn't nullptr
    void remove_primary_index_cache(IndexEntry* index_entry);

    void unload_and_remove_primary_index(int64_t tablet_id);

    StatusOr<IndexEntry*> rebuild_primary_index(const TabletMetadataPtr& metadata, MetaFileBuilder* builder,
                                                int64_t base_version, int64_t new_version,
                                                std::unique_ptr<std::lock_guard<std::shared_timed_mutex>>& lock);

    DynamicCache<uint64_t, LakePrimaryIndex>& index_cache() { return _index_cache; }

    void lock_shard_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).lock_shared(); }

    void unlock_shard_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).unlock_shared(); }

    bool try_lock_pk_index_shard(int64_t tablet_id) { return _get_pk_index_shard_lock(tablet_id).try_lock(); }

    void unlock_pk_index_shard(int64_t tablet_id) { _get_pk_index_shard_lock(tablet_id).unlock(); }

    void try_remove_cache(uint32_t tablet_id, int64_t txn_id);

    void set_enable_persistent_index(int64_t tablet_id, bool enable_persistent_index);

    Status execute_index_major_compaction(const TabletMetadata& metadata, TxnLogPB* txn_log);

    PersistentIndexBlockCache* block_cache() { return _block_cache.get(); }

    Status pk_index_major_compaction(int64_t tablet_id, DataDir* data_dir);

private:
    // print memory tracker state
    void _print_memory_stats();
    Status _do_update(uint32_t rowset_id, int32_t upsert_idx, const ColumnUniquePtr& upsert, PrimaryIndex& index,
                      DeletesMap* new_deletes);

    Status _do_update_with_condition(const RowsetUpdateStateParams& params, uint32_t rowset_id, int32_t upsert_idx,
                                     int32_t condition_column, const ColumnUniquePtr& upsert, PrimaryIndex& index,
                                     DeletesMap* new_deletes);

    int32_t _get_condition_column(const TxnLogPB_OpWrite& op_write, const TabletSchema& tablet_schema);

    Status _handle_index_op(int64_t tablet_id, int64_t base_version, bool need_lock,
                            const std::function<void(LakePrimaryIndex&)>& op);

    std::shared_mutex& _get_pk_index_shard_lock(int64_t tabletId) { return _get_pk_index_shard(tabletId).lock; }

    struct PkIndexShard {
        mutable std::shared_mutex lock;
    };

    PkIndexShard& _get_pk_index_shard(int64_t tabletId) {
        return _pk_index_shards[tabletId & (config::pk_index_map_shard_size - 1)];
    }

    // decide whether use light publish compaction stategy or not
    bool _use_light_publish_primary_compaction(int64_t tablet_id, int64_t txn_id);

    static const size_t kPrintMemoryStatsInterval = 300; // 5min
private:
    // default 6min
    int64_t _cache_expire_ms = 360000;
    // primary index
    DynamicCache<uint64_t, LakePrimaryIndex> _index_cache;

    // rowset cache
    DynamicCache<string, RowsetUpdateState> _update_state_cache;
    // compaction cache
    DynamicCache<string, CompactionState> _compaction_cache;
    std::atomic<int64_t> _last_clear_expired_cache_millis = 0;
    std::shared_ptr<LocationProvider> _location_provider;
    TabletManager* _tablet_mgr = nullptr;

    // memory checkers
    MemTracker* _update_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _index_cache_mem_tracker;
    std::unique_ptr<MemTracker> _update_state_mem_tracker;
    std::unique_ptr<MemTracker> _compaction_state_mem_tracker;

    std::vector<PkIndexShard> _pk_index_shards;

    std::unique_ptr<PersistentIndexBlockCache> _block_cache;
};

} // namespace lake

} // namespace starrocks
