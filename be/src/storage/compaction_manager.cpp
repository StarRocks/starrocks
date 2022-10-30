// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/compaction_manager.h"

#include "storage/compaction_scheduler.h"
#include "storage/data_dir.h"
#include "util/thread.h"

namespace starrocks {

CompactionManager::CompactionManager() : _next_task_id(0) {
    auto st = ThreadPoolBuilder("up_candidates")
                      .set_min_threads(1)
                      .set_max_threads(5)
                      .set_max_queue_size(100000)
                      .build(&_update_candidate_pool);
    DCHECK(st.ok());
}

CompactionManager::~CompactionManager() {
    _update_candidate_pool->wait();
}

void CompactionManager::init_max_task_num(int32_t num) {
    _max_task_num = num;
}

void CompactionManager::update_candidates(std::vector<CompactionCandidate> candidates) {
    size_t erase_num = 0;
    {
        std::lock_guard lg(_candidates_mutex);
        // TODO(meegoo): This is very inefficient to implement, just to fix bug, it will refactor later
        for (auto iter = _compaction_candidates.begin(); iter != _compaction_candidates.end();) {
            bool has_erase = false;
            for (auto& candidate : candidates) {
                if (candidate.tablet->tablet_id() == iter->tablet->tablet_id() && candidate.type == iter->type) {
                    iter = _compaction_candidates.erase(iter);
                    erase_num++;
                    has_erase = true;
                    break;
                }
            }
            if (!has_erase) {
                iter++;
            }
        }
        for (auto& candidate : candidates) {
            candidate.score = candidate.tablet->compaction_score(candidate.type) * 100;
            _compaction_candidates.emplace(std::move(candidate));
        }
        VLOG(2) << "_compaction_candidates size:" << _compaction_candidates.size()
                << ", to add candidates:" << candidates.size() << " erase candidates: " << erase_num;
    }
    if (candidates.size() != erase_num) {
        VLOG(2) << "new compaction candidate added. notify scheduler";
        _notify_schedulers();
    }
}

void CompactionManager::insert_candidates(std::vector<CompactionCandidate> candidates) {
    std::lock_guard lg(_candidates_mutex);
    for (auto& candidate : candidates) {
        candidate.score = candidate.tablet->compaction_score(candidate.type) * 100;
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

void CompactionManager::update_tablet_async(const TabletSharedPtr& tablet, bool need_update_context, bool is_compaction) {
    Status st = _update_candidate_pool->submit_func([tablet, need_update_context, is_compaction, this] {
        update_tablet(tablet, need_update_context, is_compaction);
    });
    if (!st.ok()) {
        LOG(WARNING) << "update candidate failed. status:" << st.to_string();
    }
}

void CompactionManager::update_tablet(const TabletSharedPtr& tablet, bool need_update_context, bool is_compaction) {
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
    if (!compaction_task || check_if_exceed_max_task_num()) {
        return false;
    }
    std::lock_guard lg(_tasks_mutex);
    TabletSharedPtr& tablet = compaction_task->tablet();
    DataDir* data_dir = tablet->data_dir();
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        if (config::cumulative_compaction_num_threads_per_disk >= 0 &&
            _data_dir_to_cumulative_task_num_map[data_dir] >= config::cumulative_compaction_num_threads_per_disk) {
            LOG(WARNING) << "register compaction task failed for disk's running cumulative tasks reach limit:"
                         << config::cumulative_compaction_num_threads_per_disk;
            return false;
        }
    } else if (compaction_task->compaction_type() == BASE_COMPACTION) {
        if (config::base_compaction_num_threads_per_disk >= 0 &&
            _data_dir_to_base_task_num_map[data_dir] >= config::base_compaction_num_threads_per_disk) {
            LOG(WARNING) << "register compaction task failed for disk's running base tasks reach limit:"
                         << config::base_compaction_num_threads_per_disk;
            return false;
        }
    }
    auto p = _running_tasks.insert(compaction_task);
    if (!p.second) {
        // duplicate task
        LOG(WARNING) << "duplicate task, compaction_task:" << compaction_task->task_id()
                     << ", tablet:" << tablet->tablet_id();
        return false;
    }
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _data_dir_to_cumulative_task_num_map[data_dir]++;
    } else {
        _data_dir_to_base_task_num_map[data_dir]++;
    }
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
        if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
            _data_dir_to_cumulative_task_num_map[data_dir]--;
        } else {
            _data_dir_to_base_task_num_map[data_dir]--;
        }
    }
}

void CompactionManager::clear_tasks() {
    std::lock_guard lg(_tasks_mutex);
    _running_tasks.clear();
    _data_dir_to_cumulative_task_num_map.clear();
    _data_dir_to_base_task_num_map.clear();
}

void CompactionManager::_notify_schedulers() {
    std::lock_guard lg(_scheduler_mutex);
    for (auto& scheduler : _schedulers) {
        scheduler->notify();
    }
}

} // namespace starrocks
