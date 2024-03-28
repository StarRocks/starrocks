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
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"

namespace starrocks {

class DataDir;
class ThreadPool;

namespace lake {

class UpdateManager;

using TabletAndScore = std::pair<int64_t, double>;

#ifdef USE_STAROS
class LocalPkIndexManager {
public:
    LocalPkIndexManager() = default;

    ~LocalPkIndexManager();

    Status init();

    static void gc(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids);

    static void evict(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids);

    // remove pk index meta first, and if success then remove dir.
    static Status clear_persistent_index(int64_t tablet_id);

    void schedule(const std::function<std::vector<TabletAndScore>()>& pick_algo, UpdateManager* update_mgr);

    // Mark tablet is running and increase disk concurrency
    void mark_running(int64_t tablet_id, DataDir* data_dir);

    // Mark tablet is no running and decrease disk concurrency
    void unmark_running(int64_t tablet_id, DataDir* data_dir);

    // change the thread pool thread count
    Status update_max_threads(int max_threads);

    // Call pick algo function, and refresh ready tablet queue
    void update_ready_tablet_queue(const std::function<std::vector<TabletAndScore>()>& pick_algo);

    // Is tablet in running state
    bool is_running(int64_t tablet_id);

    // Is tablet's disk out of concurrency limit
    bool disk_limit(DataDir* data_dir);

private:
    static bool need_evict_tablet(const std::string& tablet_pk_path);

private:
    std::mutex _mutex;
    // Sorted by prority
    std::vector<TabletAndScore> _ready_tablets_queue;
    std::unordered_set<int64_t> _running_tablets;
    std::unique_ptr<ThreadPool> _worker_thread_pool;
    std::unordered_map<DataDir*, uint64_t> _data_dir_to_task_num_map;
    size_t _last_schedule_time = 0;
};
#else
class LocalPkIndexManager {
public:
    static void gc(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {}

    static void evict(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {}

    // remove pk index meta first, and if success then remove dir.
    static Status clear_persistent_index(int64_t tablet_id) { return Status::OK(); }

private:
    static bool need_evict_tablet(const std::string& tablet_pk_path) { return false; }
};
#endif // USE_STAROS

} // namespace lake
} // namespace starrocks
