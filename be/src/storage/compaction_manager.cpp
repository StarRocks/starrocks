// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_manager.h"

#include "storage/compaction_scheduler.h"
#include "storage/data_dir.h"
#include "util/thread.h"
namespace starrocks {

std::unique_ptr<CompactionManager> CompactionManager::_instance(new CompactionManager());

CompactionManager* CompactionManager::instance() {
    return _instance.get();
}

void CompactionManager::update_candidates(std::vector<CompactionCandidate> candidates) {
    bool should_notify = false;
    {
        std::lock_guard lg(_candidates_mutex);
        for (auto& candidate : candidates) {
            size_t num = _compaction_candidates.erase(candidate);
            should_notify |= num == 0;
            _compaction_candidates.emplace(std::move(candidate));
        }
    }
    VLOG(2) << "_compaction_candidates size:" << _compaction_candidates.size()
            << ", to add candidates:" << candidates.size();
    if (should_notify) {
        VLOG(2) << "new compaction candidate added. notify scheduler";
        _notify_schedulers();
    }
}

void CompactionManager::insert_candidates(std::vector<CompactionCandidate> candidates) {
    std::lock_guard lg(_candidates_mutex);
    for (auto& candidate : candidates) {
        _compaction_candidates.emplace(std::move(candidate));
    }
}

CompactionCandidate CompactionManager::pick_candidate() {
    std::lock_guard lg(_candidates_mutex);
    if (_compaction_candidates.empty()) {
        static CompactionCandidate invalid_candidate;
        return invalid_candidate;
    }

    auto iter = _compaction_candidates.begin();
    CompactionCandidate ret = *iter;
    _compaction_candidates.erase(iter);
    return ret;
}

void CompactionManager::update_tablet_async(TabletSharedPtr tablet, bool need_update_context, bool is_compaction) {
    PriorityThreadPool::Task task;
    task.work_function = [tablet, need_update_context, is_compaction, this] {
        update_tablet(tablet, need_update_context, is_compaction);
    };
    bool ret = _update_candidate_pool.try_offer(task);
    if (!ret) {
        LOG(FATAL) << "update candidate failed for queue is full. capacity:"
                   << _update_candidate_pool.get_queue_capacity()
                   << ", queue size:" << _update_candidate_pool.get_queue_size();
    }
}

void CompactionManager::update_tablet(TabletSharedPtr tablet, bool need_update_context, bool is_compaction) {
    std::vector<CompactionCandidate> candidates = tablet->get_compaction_candidates(need_update_context);
    if (candidates.empty()) {
        return;
    }
    if (is_compaction) {
        LOG(INFO) << "compaction finished. and should do compaction again";
    }
    update_candidates(candidates);
}

bool CompactionManager::register_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return false;
    }
    std::lock_guard lg(_tasks_mutex);
    if (config::max_compaction_task_num >= 0 && _running_tasks.size() >= config::max_compaction_task_num) {
        LOG(WARNING) << "register compaction task failed for running tasks reach max limit:"
                     << config::max_compaction_task_num;
        return false;
    }
    if (compaction_task->compaction_level() == 0 && config::max_cumulative_compaction_task >= 0 &&
        _level_to_task_num_map[0] >= config::max_cumulative_compaction_task) {
        LOG(WARNING) << "register compaction task failed for cumulative limit:"
                     << config::max_cumulative_compaction_task;
        return false;
    } else if (compaction_task->compaction_level() == 1 && config::max_base_compaction_task >= 0 &&
               _level_to_task_num_map[1] >= config::max_base_compaction_task) {
        LOG(WARNING) << "register compaction task failed for base limit:" << config::max_base_compaction_task;
        return false;
    }
    TabletSharedPtr& tablet = compaction_task->tablet();
    DataDir* data_dir = tablet->data_dir();
    if (config::max_compaction_task_per_disk >= 0 &&
        _data_dir_to_task_num_map[data_dir] >= config::max_compaction_task_per_disk) {
        LOG(WARNING) << "register compaction task failed for disk's running tasks reach limit:"
                     << config::max_compaction_task_per_disk;
        return false;
    }
    auto p = _running_tasks.insert(compaction_task);
    if (!p.second) {
        // duplicate task
        LOG(WARNING) << "duplicate task, compaction_task:" << compaction_task->task_id()
                     << ", tablet:" << tablet->tablet_id();
        return false;
    }
    _level_to_task_num_map[compaction_task->compaction_level()]++;
    _data_dir_to_task_num_map[data_dir]++;
    return true;
}

void CompactionManager::unregister_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return;
    }
    std::lock_guard lg(_tasks_mutex);
    auto size = _running_tasks.erase(compaction_task);
    if (size > 0) {
        TabletSharedPtr& tablet = compaction_task->tablet();
        DataDir* data_dir = tablet->data_dir();
        _level_to_task_num_map[compaction_task->compaction_level()]--;
        _data_dir_to_task_num_map[data_dir]--;
    }
}

void CompactionManager::clear_tasks() {
    std::lock_guard lg(_tasks_mutex);
    _running_tasks.clear();
    _data_dir_to_task_num_map.clear();
    _level_to_task_num_map.clear();
}

void CompactionManager::_notify_schedulers() {
    std::lock_guard lg(_scheduler_mutex);
    for (auto& scheduler : _schedulers) {
        scheduler->notify();
    }
}

} // namespace starrocks
