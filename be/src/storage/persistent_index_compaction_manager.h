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

namespace starrocks {

class ThreadPool;

class PersistentIndexCompactionManager {
public:
    PersistentIndexCompactionManager() {}
    ~PersistentIndexCompactionManager();
    Status init();
    void schedule();
    void mark_running(int64_t tablet_id);
    void unmark_running(int64_t tablet_id);
    Status update_max_threads(int max_threads);

private:
    bool _too_many_tasks();
    bool _need_skip(int64_t tablet_id);
    static const uint32_t MAX_RUNNING_TABLETS = 1000;

    std::mutex _mutex;
    std::unordered_set<int64_t> _running_tablets;
    std::unique_ptr<ThreadPool> _worker_thread_pool;
};

} // namespace starrocks