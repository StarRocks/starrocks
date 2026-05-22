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

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/mem_tracker_fwd.h"

namespace starrocks {

class MetricRegistry;

class GlobalEnv {
public:
    static GlobalEnv* GetInstance() {
        static GlobalEnv s_global_env;
        return &s_global_env;
    }

    GlobalEnv() = default;
    ~GlobalEnv() { _is_init = false; }

    Status init(MetricRegistry* metrics);
    void stop() {
        _is_init = false;
        _reset_tracker();
    }

    static bool is_init();

    MemTracker* process_mem_tracker() { return _process_mem_tracker.get(); }
    MemTracker* query_pool_mem_tracker() { return _query_pool_mem_tracker.get(); }
    std::shared_ptr<MemTracker> query_pool_mem_tracker_shared() { return _query_pool_mem_tracker; }
    MemTracker* connector_scan_pool_mem_tracker() { return _connector_scan_pool_mem_tracker.get(); }
    MemTracker* load_mem_tracker() { return _load_mem_tracker.get(); }
    MemTracker* metadata_mem_tracker() { return _metadata_mem_tracker.get(); }
    MemTracker* tablet_metadata_mem_tracker() { return _tablet_metadata_mem_tracker.get(); }
    MemTracker* rowset_metadata_mem_tracker() { return _rowset_metadata_mem_tracker.get(); }
    MemTracker* segment_metadata_mem_tracker() { return _segment_metadata_mem_tracker.get(); }
    MemTracker* column_metadata_mem_tracker() { return _column_metadata_mem_tracker.get(); }
    MemTracker* tablet_schema_mem_tracker() { return _tablet_schema_mem_tracker.get(); }
    MemTracker* column_zonemap_index_mem_tracker() { return _column_zonemap_index_mem_tracker.get(); }
    MemTracker* ordinal_index_mem_tracker() { return _ordinal_index_mem_tracker.get(); }
    MemTracker* bitmap_index_mem_tracker() { return _bitmap_index_mem_tracker.get(); }
    MemTracker* bloom_filter_index_mem_tracker() { return _bloom_filter_index_mem_tracker.get(); }
    MemTracker* builtin_inverted_index_mem_tracker() { return _builtin_inverted_index_mem_tracker.get(); }
    MemTracker* segment_zonemap_mem_tracker() { return _segment_zonemap_mem_tracker.get(); }
    MemTracker* short_key_index_mem_tracker() { return _short_key_index_mem_tracker.get(); }
    MemTracker* compaction_mem_tracker() { return _compaction_mem_tracker.get(); }
    MemTracker* schema_change_mem_tracker() { return _schema_change_mem_tracker.get(); }
    // The value of `page_cache_mem_tracker` is manually counted and is attached to the process_mem_tracker tree.
    // It is not based on the `ThreadLocalMemTracker`.
    // Therefore, when counting the memory, the `MemTracker::set` interface can be used,
    // while the consume/release interfaces cannot be used.
    // Otherwise, it will cause problems in the memory statistics of the process.
    MemTracker* page_cache_mem_tracker() { return _page_cache_mem_tracker.get(); }
    MemTracker* jit_cache_mem_tracker() { return _jit_cache_mem_tracker.get(); }
    MemTracker* update_mem_tracker() { return _update_mem_tracker.get(); }
    MemTracker* passthrough_mem_tracker() { return _passthrough_mem_tracker.get(); }
    MemTracker* brpc_iobuf_mem_tracker() { return _brpc_iobuf_mem_tracker.get(); }
    MemTracker* clone_mem_tracker() { return _clone_mem_tracker.get(); }
    MemTracker* consistency_mem_tracker() { return _consistency_mem_tracker.get(); }
    MemTracker* replication_mem_tracker() { return _replication_mem_tracker.get(); }
    MemTracker* datacache_mem_tracker() { return _datacache_mem_tracker.get(); }
    MemTracker* jemalloc_metadata_traker() { return _jemalloc_metadata_tracker.get(); }
    std::shared_ptr<MemTracker> get_mem_tracker_by_type(MemTrackerType type);
    std::vector<std::shared_ptr<MemTracker>> mem_trackers() const;

    static int64_t calc_max_query_memory(int64_t process_mem_limit, int64_t percent);

    int64_t process_mem_limit() const;

private:
    static bool _is_init;

    Status _init_mem_tracker(MetricRegistry* metrics);
    void _reset_tracker();

    std::shared_ptr<MemTracker> regist_tracker(MemTrackerType type, int64_t bytes_limit, MemTracker* parent);

    // root process memory tracker
    std::shared_ptr<MemTracker> _process_mem_tracker;

    // Track usage of jemalloc
    std::shared_ptr<MemTracker> _jemalloc_metadata_tracker;

    // Limit the memory used by the query. At present, it can use 90% of the be memory limit
    std::shared_ptr<MemTracker> _query_pool_mem_tracker;
    std::shared_ptr<MemTracker> _connector_scan_pool_mem_tracker;

    // Limit the memory used by load
    std::shared_ptr<MemTracker> _load_mem_tracker;

    // metadata l0
    std::shared_ptr<MemTracker> _metadata_mem_tracker;

    // metadata l1
    std::shared_ptr<MemTracker> _tablet_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _rowset_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _segment_metadata_mem_tracker;
    std::shared_ptr<MemTracker> _column_metadata_mem_tracker;

    // metadata l2
    std::shared_ptr<MemTracker> _tablet_schema_mem_tracker;
    std::shared_ptr<MemTracker> _segment_zonemap_mem_tracker;
    std::shared_ptr<MemTracker> _short_key_index_mem_tracker;
    std::shared_ptr<MemTracker> _column_zonemap_index_mem_tracker;
    std::shared_ptr<MemTracker> _ordinal_index_mem_tracker;
    std::shared_ptr<MemTracker> _bitmap_index_mem_tracker;
    std::shared_ptr<MemTracker> _bloom_filter_index_mem_tracker;
    std::shared_ptr<MemTracker> _builtin_inverted_index_mem_tracker;

    // The memory used for compaction
    std::shared_ptr<MemTracker> _compaction_mem_tracker;

    // The memory used for schema change
    std::shared_ptr<MemTracker> _schema_change_mem_tracker;

    // The memory used for page cache
    std::shared_ptr<MemTracker> _page_cache_mem_tracker;

    // The memory used for jit cache
    std::shared_ptr<MemTracker> _jit_cache_mem_tracker;

    // The memory tracker for update manager
    std::shared_ptr<MemTracker> _update_mem_tracker;

    // record mem usage in passthrough
    std::shared_ptr<MemTracker> _passthrough_mem_tracker;
    std::shared_ptr<MemTracker> _brpc_iobuf_mem_tracker;

    std::shared_ptr<MemTracker> _clone_mem_tracker;

    std::shared_ptr<MemTracker> _consistency_mem_tracker;

    std::shared_ptr<MemTracker> _replication_mem_tracker;

    // The memory used for datacache
    std::shared_ptr<MemTracker> _datacache_mem_tracker;

    std::map<MemTrackerType, std::shared_ptr<MemTracker>> _mem_tracker_map;
};

} // namespace starrocks
