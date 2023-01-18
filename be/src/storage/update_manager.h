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
#include "storage/olap_common.h"
#include "storage/primary_index.h"
#include "util/dynamic_cache.h"
#include "util/threadpool.h"

namespace starrocks {

using std::string;

class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
class MemTracker;
class KVStore;
class RowsetUpdateState;
class Tablet;

class LocalDelvecLoader : public DelvecLoader {
public:
    LocalDelvecLoader(KVStore* meta) : _meta(meta) {}
    Status load(const TabletSegmentId& tsid, int64_t version, DelVectorPtr* pdelvec);

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

    DynamicCache<uint64_t, PrimaryIndex>& index_cache() { return _index_cache; }

    DynamicCache<string, RowsetUpdateState>& update_state_cache() { return _update_state_cache; }

    MemTracker* compaction_state_mem_tracker() const { return _compaction_state_mem_tracker.get(); }

    void clear_cache();

    void clear_cached_del_vec(const std::vector<TabletSegmentId>& tsids);

    void expire_cache();

    MemTracker* mem_tracker() const { return _update_mem_tracker; }

    string memory_stats();

    string detail_memory_stats();

    string topn_memory_stats(size_t topn);

private:
    // default 6min
    int64_t _cache_expire_ms = 360000;

    MemTracker* _update_mem_tracker = nullptr;

    DynamicCache<uint64_t, PrimaryIndex> _index_cache;
    std::unique_ptr<MemTracker> _index_cache_mem_tracker;

    DynamicCache<string, RowsetUpdateState> _update_state_cache;
    std::unique_ptr<MemTracker> _update_state_mem_tracker;

    std::unique_ptr<MemTracker> _compaction_state_mem_tracker;

    std::atomic<int64_t> _last_clear_expired_cache_millis{0};

    // DelVector related states
    std::mutex _del_vec_cache_lock;
    std::unordered_map<TabletSegmentId, DelVectorPtr> _del_vec_cache;
    std::unique_ptr<MemTracker> _del_vec_cache_mem_tracker;

    std::unique_ptr<ThreadPool> _apply_thread_pool;

    UpdateManager(const UpdateManager&) = delete;
    const UpdateManager& operator=(const UpdateManager&) = delete;
};

} // namespace starrocks
