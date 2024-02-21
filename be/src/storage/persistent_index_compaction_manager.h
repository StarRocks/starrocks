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
#include <set>
#include <utility>
#include <vector>

#include "storage/olap_common.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"

namespace starrocks {

class ThreadPool;

class PersistentIndexCompactionManager {
public:
    PersistentIndexCompactionManager() {}
    ~PersistentIndexCompactionManager();
    Status init();
    void schedule(const std::function<std::vector<TabletAndScore>()>& pick_algo);
    // Mark tablet is running and increase disk concurrency
    void mark_running(Tablet* tablet);
    // Mark tablet is no running and decrease disk concurrency
    void unmark_running(Tablet* tablet);
    // change the thread pool thread count
    Status update_max_threads(int max_threads);
    // Call pick algo function, and refresh ready tablet queue
    void update_ready_tablet_queue(const std::function<std::vector<TabletAndScore>()>& pick_algo);
    // Is tablet in running state
    bool is_running(Tablet* tablet);
    // Is tablet's disk out of concurrency limit
    bool disk_limit(Tablet* tablet);

private:
    std::mutex _mutex;
    // Sorted by prority
    std::vector<TabletAndScore> _ready_tablets_queue;
    std::unordered_set<int64_t> _running_tablets;
    std::unique_ptr<ThreadPool> _worker_thread_pool;
    std::unordered_map<DataDir*, uint64_t> _data_dir_to_task_num_map;
    size_t _last_schedule_time = 0;
};

} // namespace starrocks