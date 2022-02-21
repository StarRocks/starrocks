// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_scheduler.h"

#include <chrono>
#include <thread>

#include "storage/compaction_candidate.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

using namespace std::chrono_literals;

namespace starrocks {

CompactionScheduler::CompactionScheduler() : _compaction_pool("compact_pool", config::max_compaction_task_num, 1000) {
    CompactionManager::instance()->register_scheduler(this);
}

void CompactionScheduler::schedule() {
    LOG(INFO) << "start compaction scheduler";
    while (true) {
        ++_round;
        _wait_to_run();
        std::shared_ptr<CompactionTask> compaction_task = _try_get_next_compaction_task();
        if (!compaction_task) {
            std::unique_lock<std::mutex> lk(_mutex);
            _cv.wait_for(lk, 10000ms);
        } else {
            compaction_task->set_compaction_scheduler(this);
            compaction_task->set_task_id(CompactionManager::instance()->next_compaction_task_id());
            PriorityThreadPool::Task task;
            task.work_function = [compaction_task] { compaction_task->start(); };
            LOG(INFO) << "submit task to compaction pool"
                      << ", task_id:" << compaction_task->task_id()
                      << ", tablet_id:" << compaction_task->tablet()->tablet_id()
                      << ", compaction level:" << (int32_t)compaction_task->compaction_level()
                      << ", compaction score:" << compaction_task->compaction_score()
                      << " for round:" << _round;
            bool ret = _compaction_pool.try_offer(task);
            if (!ret) {
                LOG(WARNING) << "submit compaction task to compaction pool failed."
                             << ", pool queue size:" << _compaction_pool.get_queue_size()
                             << ", queue capacity:" << _compaction_pool.get_queue_capacity();
                compaction_task->tablet()->reset_compaction(compaction_task->compaction_level());
                CompactionCandidate candidate;
                candidate.tablet = compaction_task->tablet();
                candidate.level = compaction_task->compaction_level();
                CompactionManager::instance()->update_candidates({candidate});
            }
        }
    }
}

void CompactionScheduler::notify() {
    std::unique_lock<std::mutex> lk(_mutex);
    _cv.notify_one();
}

bool CompactionScheduler::_can_schedule_next() {
    int32_t max_task_num = std::min(
            config::max_compaction_task_num,
            static_cast<int32_t>(StorageEngine::instance()->get_store_num() * config::max_compaction_task_per_disk));
    return config::enable_compaction && CompactionManager::instance()->running_tasks_num() < max_task_num &&
           CompactionManager::instance()->candidates_size() > 0;
}

void CompactionScheduler::_wait_to_run() {
    std::unique_lock<std::mutex> lk(_mutex);
    // check _can_schedule_next every five second to avoid deadlock and support modifying config online
    while (!_cv.wait_for(lk, 5000ms, [this] { return _can_schedule_next(); })) {
    }
}
bool CompactionScheduler::_can_do_compaction_task(Tablet* tablet, CompactionTask* compaction_task) {
    // create new compaction task successfully
    DCHECK(tablet);
    DCHECK(compaction_task);
    bool need_reset_task = true;
    DeferOp reset_op([&] {
        if (need_reset_task) {
            tablet->reset_compaction(compaction_task->compaction_level());
        }
    });
    // to compatible with old compaction framework
    // TODO: can be optimized to use just one lock
    int64_t last_failure_ms = 0;
    if (compaction_task->compaction_level() == 0) {
        uint16_t level_num = CompactionManager::instance()->running_tasks_num_for_level(0);
        if (config::max_cumulative_compaction_task >= 0 && level_num >= config::max_cumulative_compaction_task) {
            LOG(INFO) << "skip tablet:" << tablet->tablet_id()
                      << " for cumulative compaction limit:" << config::max_cumulative_compaction_task;
            return false;
        }
        std::unique_lock lk(tablet->get_cumulative_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " for cumulative lock";
            return false;
        }
        last_failure_ms = tablet->last_cumu_compaction_failure_time();
    } else {
        uint16_t level_num = CompactionManager::instance()->running_tasks_num_for_level(1);
        if (config::max_base_compaction_task >= 0 && level_num >= config::max_base_compaction_task) {
            LOG(INFO) << "skip tablet:" << tablet->tablet_id()
                      << " for base compaction limit:" << config::max_base_compaction_task;
            return false;
        }
        std::unique_lock lk(tablet->get_base_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " for base lock";
            return false;
        }
        last_failure_ms = tablet->last_base_compaction_failure_time();
    }
    int64_t now_ms = UnixMillis();
    if (now_ms - last_failure_ms <= config::min_compaction_failure_interval_sec * 1000) {
        LOG(INFO) << "Too often to schedule compaction, skip it."
                  << "compaction_level=" << compaction_task->compaction_level()
                  << ", last_failure_time_ms=" << last_failure_ms << ", tablet_id=" << tablet->tablet_id();
        return false;
    }

    DataDir* data_dir = tablet->data_dir();
    // control the concurrent running tasks's limit
    // just try best here for that there may be concurrent CompactionSchedulers
    // hard limit will be checked when CompactionManager::register()
    uint16_t num = CompactionManager::instance()->running_tasks_num_for_dir(data_dir);
    if (config::max_compaction_task_per_disk >= 0 && num >= config::max_compaction_task_per_disk) {
        LOG(INFO) << "skip tablet:" << tablet->tablet_id()
                  << " for limit of compaction task per disk. disk path:" << data_dir->path()
                  << ", running num:" << num;
        return false;
    }
    // found a qualified tablet
    // qualified tablet will be removed from candidates
    need_reset_task = false;
    return true;
}

bool CompactionScheduler::_check_precondition(const CompactionCandidate& candidate) {
    if (!candidate.tablet) {
        LOG(WARNING) << "null tablet";
        return false;
    }
    const TabletSharedPtr& tablet = candidate.tablet;
    if (!tablet->need_compaction(candidate.level)) {
        // check need compaction
        // if it is false, skip this tablet and remove it from candidate
        LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " because need_compaction is false";
        return false;
    }

    if (tablet->tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " because tablet state is:" << tablet->tablet_state()
                  << ", not RUNNING";
        return false;
    }

    // check for alter task for safety
    // maybe this logic can be removed if tablet is the schema change dest tablet, need_compaction will never return true
    AlterTabletTaskSharedPtr cur_alter_task = tablet->alter_task();
    if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED &&
        cur_alter_task->alter_state() != ALTER_FAILED) {
        TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
        TabletSharedPtr related_tablet = tablet_manager->get_tablet(cur_alter_task->related_tablet_id());
        if (related_tablet != nullptr && tablet->creation_time() > related_tablet->creation_time()) {
            // Current tablet is newly created during schema-change or rollup, skip it
            LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " because it is newly-created for schema change";
            return false;
        }
    }

    std::shared_ptr<CompactionTask> compaction_task = tablet->get_compaction(candidate.level, false);
    if (compaction_task) {
        // tablet already has a running compaction task, skip it
        LOG(INFO) << "skip tablet:" << tablet->tablet_id()
                  << " because there is another running compaction task:" << compaction_task->task_id();
        return false;
    }
    return true;
}

bool CompactionScheduler::_can_do_compaction(const CompactionCandidate& candidate, bool* need_reschedule,
                                             std::shared_ptr<CompactionTask>* compaction_task) {
    DCHECK(compaction_task);
    // when the following conditions fail, should not reschedule the candidate tablet
    *need_reschedule = false;
    bool precondition_ok = _check_precondition(candidate);
    if (!precondition_ok) {
        return false;
    }

    // when the following conditions fail, should reschedule the candidate tablet
    *need_reschedule = true;
    // create a new compaction task
    const TabletSharedPtr& tablet = candidate.tablet;
    std::shared_ptr<CompactionTask> tmp_task = tablet->get_compaction(candidate.level, true);
    if (tmp_task) {
        DataDir* data_dir = tablet->data_dir();
        if (data_dir->reach_capacity_limit(tmp_task->input_rowsets_size())) {
            LOG(WARNING) << "skip tablet:" << tablet->tablet_id()
                         << " because data dir reaches capacity limit. input rowsets size:"
                         << tmp_task->input_rowsets_size();
            return false;
        }
        bool can_do = _can_do_compaction_task(tablet.get(), tmp_task.get());
        if (can_do) {
            *compaction_task = std::move(tmp_task);
        }
        *need_reschedule = !can_do;
        return can_do;
    } else {
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because creating compaction task failed.";
        return false;
    }
}

std::shared_ptr<CompactionTask> CompactionScheduler::_try_get_next_compaction_task() {
    VLOG(2) << "try to get next qualified tablet for round:" << _round
            << ", current candidates size:" << CompactionManager::instance()->candidates_size();
    // tmp_tablets save the tmp picked candidates tablets
    std::vector<CompactionCandidate> tmp_candidates;
    CompactionCandidate compaction_candidate;
    bool found = false;
    std::shared_ptr<CompactionTask> compaction_task;
    while (true) {
        if (!_can_schedule_next()) {
            VLOG(2) << "_can_schedule_next is false. skip";
            break;
        }
        compaction_candidate = CompactionManager::instance()->pick_candidate();
        VLOG(2) << "get candidate:" << compaction_candidate.to_string();
        if (!compaction_candidate.is_valid()) {
            // means there no candidate tablet, break
            LOG(INFO) << "do not get a qualified candidate";
            break;
        }
        VLOG(2) << "try tablet:" << compaction_candidate.tablet->tablet_id()
                << ", level:" << (int32_t)compaction_candidate.level;
        bool need_reschedule = true;
        found = _can_do_compaction(compaction_candidate, &need_reschedule, &compaction_task);
        if (need_reschedule) {
            tmp_candidates.emplace_back(std::move(compaction_candidate));
        } else {
            DCHECK(found);
        }
        if (found) {
            break;
        }
    }
    VLOG(2) << "tmp tablets size:" << tmp_candidates.size();
    CompactionManager::instance()->insert_candidates(std::move(tmp_candidates));
    if (found) {
        VLOG(2) << "get a qualified tablet:" << compaction_candidate.tablet->tablet_id()
                << ", level:" << (int32_t)compaction_candidate.level
                << ", compaction task:" << compaction_task->get_task_info();
        return compaction_task;
    } else {
        VLOG(2) << "no qualified tablet.";
        return nullptr;
    }
}

} // namespace starrocks
