// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class Tablet;
class DataDir;
class CompactionTask;

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

    bool _can_schedule_next();

    Tablet* _try_get_next_tablet();

    bool _can_do_compaction(Tablet* tablet, bool* need_reschedule);

    // if check fails, should not reschedule the tablet
    bool _check_precondition(Tablet* tablet);

    bool _can_do_compaction_task(Tablet* tablet, CompactionTask* compaction_task);

private:
    PriorityThreadPool _compaction_pool;

    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _round;
};

} // namespace starrocks
