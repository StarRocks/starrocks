// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    ~CompactionScheduler();

    void schedule();

    void notify();

private:
    void _schedule();

    // wait until current running tasks are below max_concurrent_num
    void _wait_to_run();

    static bool _can_schedule_next();

    std::shared_ptr<CompactionTask> _try_get_next_compaction_task() const;

    // if check fails, should not reschedule the tablet
    static bool _check_precondition(const CompactionCandidate& candidate);

private:
    std::unique_ptr<ThreadPool> _compaction_pool = nullptr;
    std::thread _scheduler_thread;

    std::atomic<bool> _bg_worker_stopped{false};
    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _round = 0;
};

} // namespace starrocks
