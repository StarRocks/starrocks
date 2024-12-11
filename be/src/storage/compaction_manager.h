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

class StorageEngine;

class CompactionManager {
public:
    CompactionManager();

<<<<<<< HEAD
    ~CompactionManager();
=======
    ~CompactionManager() = default;

    void stop();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    void init_max_task_num(int32_t num);

    size_t candidates_size() {
        std::lock_guard lg(_candidates_mutex);
        return _compaction_candidates.size();
    }

    void update_candidates(std::vector<CompactionCandidate> candidates);

    void remove_candidate(int64_t tablet_id);

    bool pick_candidate(CompactionCandidate* candidate);

<<<<<<< HEAD
    void update_tablet_async(TabletSharedPtr tablet);

    void update_tablet(TabletSharedPtr tablet);
=======
    void submit_compaction_task(const CompactionCandidate& compaction_candidate);

    void update_tablet_async(const TabletSharedPtr& tablet);

    void update_tablet(const TabletSharedPtr& tablet);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    bool register_task(CompactionTask* compaction_task);

    void unregister_task(CompactionTask* compaction_task);

    void clear_tasks();

    void get_running_status(std::string* json_result);

    uint16_t running_tasks_num() {
        std::lock_guard lg(_tasks_mutex);
<<<<<<< HEAD
        return _running_tasks.size();
=======
        size_t res = 0;
        for (const auto& it : _running_tasks) {
            res += it.second.size();
        }
        return res;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    bool check_if_exceed_max_task_num() {
        bool exceed = false;
        if (config::max_compaction_concurrency == 0) {
            LOG_ONCE(WARNING) << "register compaction task failed for compaction is disabled";
            exceed = true;
        }
        std::lock_guard lg(_tasks_mutex);
<<<<<<< HEAD
        if (_running_tasks.size() >= _max_task_num) {
=======
        size_t running_tasks_num = 0;
        for (const auto& it : _running_tasks) {
            running_tasks_num += it.second.size();
        }
        if (running_tasks_num >= _max_task_num) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            VLOG(2) << "register compaction task failed for running tasks reach max limit:" << _max_task_num;
            exceed = true;
        }
        return exceed;
    }

<<<<<<< HEAD
    int32_t max_task_num() { return _max_task_num; }
=======
    int32_t max_task_num() const {
        std::lock_guard lg(_tasks_mutex);
        return _max_task_num;
    }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    uint16_t running_cumulative_tasks_num_for_dir(DataDir* data_dir) {
        std::lock_guard lg(_tasks_mutex);
        return _data_dir_to_cumulative_task_num_map[data_dir];
    }

    uint16_t running_base_tasks_num_for_dir(DataDir* data_dir) {
        std::lock_guard lg(_tasks_mutex);
        return _data_dir_to_base_task_num_map[data_dir];
    }

    uint64_t next_compaction_task_id() { return ++_next_task_id; }

    void schedule();

    Status update_max_threads(int max_threads);

<<<<<<< HEAD
=======
    int32_t compute_max_compaction_task_num() const;

    void set_max_compaction_concurrency(int threads_num);

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    double max_score();

    double last_score();

    int64_t base_compaction_concurrency();

    int64_t cumulative_compaction_concurrency();

<<<<<<< HEAD
=======
    bool has_running_task(const TabletSharedPtr& tablet);

    void stop_compaction(const TabletSharedPtr& tablet);

    bool check_compaction_disabled(const CompactionCandidate& candidate);

    std::unordered_set<CompactionTask*> get_running_task(const TabletSharedPtr& tablet);

    int get_waiting_task_num();

    ThreadPool* TEST_get_compaction_thread_pool() { return _compaction_pool.get(); }

    void disable_table_compaction(int64_t table_id, int64_t deadline);

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
private:
    CompactionManager(const CompactionManager& compaction_manager) = delete;
    CompactionManager(CompactionManager&& compaction_manager) = delete;
    CompactionManager& operator=(const CompactionManager& compaction_manager) = delete;
    CompactionManager& operator=(CompactionManager&& compaction_manager) = delete;

    void _dispatch_worker();
    bool _check_precondition(const CompactionCandidate& candidate);
<<<<<<< HEAD
=======
    bool _check_compaction_disabled(const CompactionCandidate& candidate);
    void _set_force_cumulative(CompactionCandidate* candidate);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    void _schedule();
    void _notify();
    // wait until current running tasks are below max_concurrent_num
    void _wait_to_run();
    bool _can_schedule_next();
    std::shared_ptr<CompactionTask> _try_get_next_compaction_task();

    std::mutex _candidates_mutex;
    // protect by _mutex
    std::set<CompactionCandidate, CompactionCandidateComparator> _compaction_candidates;

<<<<<<< HEAD
    std::mutex _tasks_mutex;
    std::atomic<uint64_t> _next_task_id;
    std::unordered_set<CompactionTask*> _running_tasks;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_cumulative_task_num_map;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_base_task_num_map;
    std::unordered_map<CompactionType, uint16_t> _type_to_task_num_map;
=======
    mutable std::mutex _tasks_mutex;
    std::atomic<uint64_t> _next_task_id;
    std::map<int64_t, std::unordered_set<CompactionTask*>> _running_tasks;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_cumulative_task_num_map;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_base_task_num_map;
    std::unordered_map<CompactionType, uint16_t> _type_to_task_num_map;
    std::unordered_map<int64_t, int64_t> _table_to_disable_deadline_map;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    std::unique_ptr<ThreadPool> _update_candidate_pool;
    std::mutex _dispatch_mutex;
    std::thread _dispatch_update_candidate_thread;
    std::map<int64_t, std::pair<TabletSharedPtr, int32_t>> _dispatch_map;
    std::atomic<bool> _stop = false;
    int32_t _max_dispatch_count = 0;

    int32_t _max_task_num = 0;
    int64_t _base_compaction_concurrency = 0;
    int64_t _cumulative_compaction_concurrency = 0;
    double _last_score = 0;

    bool _disable_update_tablet = false;

    std::atomic<bool> _bg_worker_stopped{false};
    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _round = 0;

    std::unique_ptr<ThreadPool> _compaction_pool = nullptr;
    std::thread _scheduler_thread;
<<<<<<< HEAD
=======

    mutable std::mutex _compact_threads_mutex;
    int32_t _max_compaction_concurrency = 0;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
};

} // namespace starrocks
