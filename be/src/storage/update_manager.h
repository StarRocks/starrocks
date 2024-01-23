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
#include "storage/delta_column_group.h"
#include "storage/olap_common.h"
#include "storage/primary_index.h"
#include "util/dynamic_cache.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/threadpool.h"

namespace starrocks {

using std::string;

class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
class MemTracker;
class KVStore;
class RowsetUpdateState;
class RowsetColumnUpdateState;
class Tablet;
class PersistentIndexCompactionManager;

class LocalDelvecLoader : public DelvecLoader {
public:
    LocalDelvecLoader(KVStore* meta) : _meta(meta) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

private:
    KVStore* _meta = nullptr;
};

class LocalDeltaColumnGroupLoader : public DeltaColumnGroupLoader {
public:
    LocalDeltaColumnGroupLoader(KVStore* meta) : _meta(meta) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs);
    Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                DeltaColumnGroupList* pdcgs);
    KVStore* meta() const { return _meta; }

private:
    KVStore* _meta = nullptr;
};

// UpdateManager maintain update feature related data structures, including
// PrimaryIndexe cache, RowsetUpdateState cache, DelVector cache and
// async apply thread pool.
class UpdateManager {
public:
    UpdateManager(MemTracker* mem_tracker);
    ~UpdateManager();

    Status init();

    void set_cache_expire_ms(int64_t expire_ms) { _cache_expire_ms = expire_ms; }

    int64_t get_cache_expire_ms() const { return _cache_expire_ms; }

    Status get_del_vec_in_meta(KVStore* meta, const TabletSegmentId& tsid, int64_t version, DelVector* delvec,
                               int64_t* latest_version);

    Status set_del_vec_in_meta(KVStore* meta, const TabletSegmentId& tsid, const DelVector& delvec);

    Status get_del_vec(KVStore* meta, const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

    Status get_latest_del_vec(KVStore* meta, const TabletSegmentId& tsid, DelVectorPtr* pdelvec);

    Status set_cached_del_vec(const TabletSegmentId& tsid, const DelVectorPtr& delvec);

    Status on_rowset_finished(Tablet* tablet, Rowset* rowset);

    void on_rowset_cancel(Tablet* tablet, Rowset* rowset);

    ThreadPool* apply_thread_pool() { return _apply_thread_pool.get(); }
    ThreadPool* get_pindex_thread_pool() { return _get_pindex_thread_pool.get(); }
    PersistentIndexCompactionManager* get_pindex_compaction_mgr() { return _persistent_index_compaction_mgr.get(); }

    DynamicCache<uint64_t, PrimaryIndex>& index_cache() { return _index_cache; }

    DynamicCache<string, RowsetUpdateState>& update_state_cache() { return _update_state_cache; }

    DynamicCache<string, RowsetColumnUpdateState>& update_column_state_cache() { return _update_column_state_cache; }

    Status get_delta_column_group(KVStore* meta, const TabletSegmentId& tsid, int64_t version,
                                  DeltaColumnGroupList* dcgs);

    MemTracker* compaction_state_mem_tracker() const { return _compaction_state_mem_tracker.get(); }

    Status set_cached_delta_column_group(KVStore* meta, const TabletSegmentId& tsid, const DeltaColumnGroupPtr& dcg);

    Status set_cached_empty_delta_column_group(KVStore* meta, const TabletSegmentId& tsid);

    bool get_cached_delta_column_group(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* dcgs);

    void clear_cache();

    void clear_cached_del_vec(const std::vector<TabletSegmentId>& tsids);

    void clear_cached_delta_column_group(const std::vector<TabletSegmentId>& tsids);

    StatusOr<size_t> clear_delta_column_group_before_version(KVStore* meta, const std::string& tablet_path,
                                                             int64_t tablet_id, int64_t min_readable_version);

    void expire_cache();

    void evict_cache(int64_t memory_urgent_level, int64_t memory_high_level);

    MemTracker* mem_tracker() const { return _update_mem_tracker; }

    string memory_stats();

    string detail_memory_stats();

    string topn_memory_stats(size_t topn);

    Status update_primary_index_memory_limit(int32_t update_memory_limit_percent) {
        int64_t byte_limits = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
        int32_t update_mem_percent = std::max(std::min(100, update_memory_limit_percent), 0);
        _index_cache.set_capacity(byte_limits * update_mem_percent);
        return Status::OK();
    }

    // Used in UT only
    bool TEST_update_state_exist(Tablet* tablet, Rowset* rowset);
    bool TEST_primary_index_refcnt(int64_t tablet_id, uint32_t expected_cnt);

private:
    // default 6min
    int64_t _cache_expire_ms = 360000;

    MemTracker* _update_mem_tracker = nullptr;

    DynamicCache<uint64_t, PrimaryIndex> _index_cache;
    std::unique_ptr<MemTracker> _index_cache_mem_tracker;

    DynamicCache<string, RowsetUpdateState> _update_state_cache;
    DynamicCache<string, RowsetColumnUpdateState> _update_column_state_cache;
    std::unique_ptr<MemTracker> _update_state_mem_tracker;

    std::unique_ptr<MemTracker> _compaction_state_mem_tracker;

    std::atomic<int64_t> _last_clear_expired_cache_millis{0};

    // DelVector related states
    std::mutex _del_vec_cache_lock;
    std::unordered_map<TabletSegmentId, DelVectorPtr> _del_vec_cache;
    std::unique_ptr<MemTracker> _del_vec_cache_mem_tracker;

    // Delta Column Group cache, dcg is short for `Delta Column Group`
    std::mutex _delta_column_group_cache_lock;
    std::map<TabletSegmentId, DeltaColumnGroupList> _delta_column_group_cache;
    std::unique_ptr<MemTracker> _delta_column_group_cache_mem_tracker;

    std::unique_ptr<ThreadPool> _apply_thread_pool;
    std::unique_ptr<ThreadPool> _get_pindex_thread_pool;
    std::unique_ptr<PersistentIndexCompactionManager> _persistent_index_compaction_mgr;

    UpdateManager(const UpdateManager&) = delete;
    const UpdateManager& operator=(const UpdateManager&) = delete;
};

} // namespace starrocks
