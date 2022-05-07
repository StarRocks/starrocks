// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "storage/compaction_candidate.h"
#include "storage/compaction_task.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "util/threadpool.h"

namespace starrocks {

class CompactionScheduler;

class CompactionManager {
public:
    CompactionManager();

    ~CompactionManager() = default;

    size_t candidates_size() {
        std::lock_guard lg(_candidates_mutex);
        return _compaction_candidates.size();
    }

    void update_candidates(std::vector<CompactionCandidate> candidates);

    void insert_candidates(std::vector<CompactionCandidate> candidates);

    CompactionCandidate pick_candidate();

    void update_tablet_async(TabletSharedPtr tablet, bool need_update_context, bool is_compaction = false);
    void update_tablet(TabletSharedPtr tablet, bool need_update_context, bool is_compaction);

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

    std::mutex _scheduler_mutex;
    std::vector<CompactionScheduler*> _schedulers;
};

} // namespace starrocks
