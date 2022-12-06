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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "util/threadpool.h"

namespace starrocks {

class DataDir;
class CompactionTask;
class CompactionCandidate;

// to schedule tablet to run compaction task
class CompactionScheduler {
public:
    CompactionScheduler();
    ~CompactionScheduler() = default;

    void schedule();

    void notify();

private:
    // wait until current running tasks are below max_concurrent_num
    void _wait_to_run();

    static bool _can_schedule_next();

    std::shared_ptr<CompactionTask> _try_get_next_compaction_task() const;

    // if check fails, should not reschedule the tablet
    static bool _check_precondition(const CompactionCandidate& candidate);

private:
    std::unique_ptr<ThreadPool> _compaction_pool;

    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _round = 0;
};

} // namespace starrocks
