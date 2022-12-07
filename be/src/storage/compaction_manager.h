// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "storage/compaction_candidate.h"
#include "storage/compaction_task.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "util/threadpool.h"

namespace starrocks {

class CompactionScheduler;
class StorageEngine;

class CompactionManager {
public:
    CompactionManager();

    ~CompactionManager();

    void init_max_task_num(int32_t num);

    size_t candidates_size() {
        std::lock_guard lg(_candidates_mutex);
        return _compaction_candidates.size();
    }

    void update_candidates(std::vector<CompactionCandidate> candidates);

    void remove_candidate(int64_t tablet_id);

    bool pick_candidate(CompactionCandidate* candidate);

    void update_tablet_async(TabletSharedPtr tablet);

    void update_tablet(TabletSharedPtr tablet);

    void register_scheduler(CompactionScheduler* scheduler) {
        std::lock_guard lg(_scheduler_mutex);
        _schedulers.push_back(scheduler);
    }

    bool register_task(CompactionTask* compaction_task);

    void unregister_task(CompactionTask* compaction_task);

    void clear_tasks();

    uint16_t running_tasks_num() {
        std::lock_guard lg(_tasks_mutex);
        return _running_tasks.size();
    }

    bool check_if_exceed_max_task_num() {
        bool exceed = false;
        std::lock_guard lg(_tasks_mutex);
        if (config::max_compaction_concurrency == 0) {
            LOG(WARNING) << "register compaction task failed for compaction is disabled";
            exceed = true;
        } else if (_running_tasks.size() >= _max_task_num) {
            VLOG(2) << "register compaction task failed for running tasks reach max limit:" << _max_task_num;
            exceed = true;
        }
        return exceed;
    }

    int32_t max_task_num() { return _max_task_num; }

    uint16_t running_cumulative_tasks_num_for_dir(DataDir* data_dir) {
        std::lock_guard lg(_tasks_mutex);
        return _data_dir_to_cumulative_task_num_map[data_dir];
    }

    uint16_t running_base_tasks_num_for_dir(DataDir* data_dir) {
        std::lock_guard lg(_tasks_mutex);
        return _data_dir_to_base_task_num_map[data_dir];
    }

    uint64_t next_compaction_task_id() { return ++_next_task_id; }

private:
    CompactionManager(const CompactionManager& compaction_manager) = delete;
    CompactionManager(CompactionManager&& compaction_manager) = delete;
    CompactionManager& operator=(const CompactionManager& compaction_manager) = delete;
    CompactionManager& operator=(CompactionManager&& compaction_manager) = delete;

    void _notify_schedulers();
    void _dispatch_worker();
    bool _check_precondition(const CompactionCandidate& candidate);

    std::mutex _candidates_mutex;
    // protect by _mutex
    std::set<CompactionCandidate, CompactionCandidateComparator> _compaction_candidates;

    std::mutex _tasks_mutex;
    std::atomic<uint64_t> _next_task_id;
    std::unordered_set<CompactionTask*> _running_tasks;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_cumulative_task_num_map;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_base_task_num_map;
    std::unordered_map<CompactionType, uint16_t> _type_to_task_num_map;
    std::unique_ptr<ThreadPool> _update_candidate_pool;
    std::mutex _dispatch_mutex;
    std::thread _dispatch_update_candidate_thread;
    std::map<int64_t, std::pair<TabletSharedPtr, int32_t>> _dispatch_map;
    std::atomic<bool> _stop = false;
    int32_t _max_dispatch_count = 0;

    std::mutex _scheduler_mutex;
    std::vector<CompactionScheduler*> _schedulers;
    int32_t _max_task_num = 0;
    bool _disable_update_tablet = false;
};

} // namespace starrocks
