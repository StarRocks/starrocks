// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/compaction_scheduler.h"

#include <chrono>
#include <thread>

#include "common/config.h"
#include "storage/compaction_candidate.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "util/defer_op.h"

using namespace std::chrono_literals;

namespace starrocks {

CompactionScheduler::CompactionScheduler() {
    auto st = ThreadPoolBuilder("compact_pool")
                      .set_min_threads(1)
                      .set_max_threads(std::max(1, StorageEngine::instance()->compaction_manager()->max_task_num()))
                      .set_max_queue_size(1000)
                      .build(&_compaction_pool);
    DCHECK(st.ok());
    StorageEngine::instance()->compaction_manager()->register_scheduler(this);
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
            compaction_task->set_task_id(StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
            LOG(INFO) << "submit task to compaction pool"
                      << ", task_id:" << compaction_task->task_id()
                      << ", tablet_id:" << compaction_task->tablet()->tablet_id()
                      << ", compaction type:" << compaction_task->compaction_type()
                      << ", compaction score:" << compaction_task->compaction_score() << " for round:" << _round;
            auto st = _compaction_pool->submit_func([compaction_task] { compaction_task->start(); });
            if (!st.ok()) {
                LOG(WARNING) << "submit compaction task to compaction pool failed. status:" << st.to_string();
                compaction_task->tablet()->reset_compaction(compaction_task->compaction_type());
                CompactionCandidate candidate;
                candidate.tablet = compaction_task->tablet();
                candidate.type = compaction_task->compaction_type();
                StorageEngine::instance()->compaction_manager()->update_candidates({candidate});
            }
        }
    }
}

void CompactionScheduler::notify() {
    std::unique_lock<std::mutex> lk(_mutex);
    _cv.notify_one();
}

bool CompactionScheduler::_can_schedule_next() {
    return !StorageEngine::instance()->compaction_manager()->check_if_exceed_max_task_num() &&
           StorageEngine::instance()->compaction_manager()->candidates_size() > 0;
}

void CompactionScheduler::_wait_to_run() {
    std::unique_lock<std::mutex> lk(_mutex);
    // check _can_schedule_next every five second to avoid deadlock and support modifying config online
    while (!_cv.wait_for(lk, 5000ms, [] { return _can_schedule_next(); })) {
    }
}

bool CompactionScheduler::_can_do_compaction_task(Tablet* tablet, CompactionTask* compaction_task) {
    // create new compaction task successfully
    DCHECK(tablet);
    DCHECK(compaction_task);
    bool need_reset_task = true;
    DeferOp reset_op([&] {
        if (need_reset_task) {
            tablet->reset_compaction(compaction_task->compaction_type());
        }
    });
    // to compatible with old compaction framework
    // TODO: can be optimized to use just one lock
    int64_t last_failure_ts;
    DataDir* data_dir = tablet->data_dir();
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        std::unique_lock lk(tablet->get_cumulative_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id() << " for cumulative lock";
            return false;
        }
        // control the concurrent running tasks's limit
        // just try best here for that there may be concurrent CompactionSchedulers
        // hard limit will be checked when CompactionManager::register()
        uint16_t num = StorageEngine::instance()->compaction_manager()->running_cumulative_tasks_num_for_dir(data_dir);
        if (config::cumulative_compaction_num_threads_per_disk >= 0 &&
            num >= config::cumulative_compaction_num_threads_per_disk) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id()
                    << " for limit of cumulative compaction task per disk. disk path:" << data_dir->path()
                    << ", running num:" << num;
            return false;
        }
        last_failure_ts = tablet->last_cumu_compaction_failure_time();
    } else {
        std::unique_lock lk(tablet->get_base_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id() << " for base lock";
            return false;
        }
        uint16_t num = StorageEngine::instance()->compaction_manager()->running_base_tasks_num_for_dir(data_dir);
        if (config::base_compaction_num_threads_per_disk >= 0 && num >= config::base_compaction_num_threads_per_disk) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id()
                    << " for limit of base compaction task per disk. disk path:" << data_dir->path()
                    << ", running num:" << num;
            return false;
        }
        last_failure_ts = tablet->last_base_compaction_failure_time();
    }
    if (UnixMillis() - last_failure_ts <= config::min_compaction_failure_interval_sec * 1000) {
        VLOG(2) << "Too often to schedule failure compaction, skip it."
                << "compaction_type=" << compaction_task->compaction_type()
                << ", min_compaction_failure_interval_sec=" << config::min_compaction_failure_interval_sec
                << ", last_failure_timestamp=" << last_failure_ts / 1000 << ", tablet_id=" << tablet->tablet_id();
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
    if (!tablet->need_compaction(candidate.type)) {
        // check need compaction
        // if it is false, skip this tablet and remove it from candidate
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because need_compaction is false";
        return false;
    }

    if (tablet->tablet_state() != TABLET_RUNNING) {
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because tablet state is:" << tablet->tablet_state()
                << ", not RUNNING";
        return false;
    }

    std::shared_ptr<CompactionTask> compaction_task = tablet->get_compaction(candidate.type, false);
    if (compaction_task) {
        // tablet already has a running compaction task, skip it
        VLOG(2) << "skip tablet:" << tablet->tablet_id()
                << " because there is another running compaction task:" << compaction_task->task_id();
        return false;
    }
    return true;
}

bool CompactionScheduler::_can_do_compaction(const CompactionCandidate& candidate,
                                             std::shared_ptr<CompactionTask>* compaction_task) {
    DCHECK(compaction_task);

    // when the following conditions fail, should reschedule the candidate tablet
    // create a new compaction task
    const TabletSharedPtr& tablet = candidate.tablet;
    std::shared_ptr<CompactionTask> tmp_task = tablet->get_compaction(candidate.type, true);
    if (UNLIKELY(tmp_task == nullptr)) {
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because creating compaction task failed.";
        return false;
    }

    DataDir* data_dir = tablet->data_dir();
    if (data_dir->capacity_limit_reached(tmp_task->input_rowsets_size())) {
        LOG(WARNING) << "skip tablet:" << tablet->tablet_id()
                     << " because data dir reaches capacity limit. input rowsets size:"
                     << tmp_task->input_rowsets_size();
        return false;
    }

    bool can_do = _can_do_compaction_task(tablet.get(), tmp_task.get());
    if (can_do) {
        *compaction_task = std::move(tmp_task);
    }
    return can_do;
}

std::shared_ptr<CompactionTask> CompactionScheduler::_try_get_next_compaction_task() const {
    VLOG(2) << "try to get next qualified tablet for round:" << _round
            << ", current candidates size:" << StorageEngine::instance()->compaction_manager()->candidates_size();
    // tmp_tablets save the tmp picked candidates tablets
    std::vector<CompactionCandidate> tmp_candidates;
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task;

    while (true) {
        if (!_can_schedule_next()) {
            VLOG(2) << "_can_schedule_next is false. skip";
            break;
        }
        compaction_candidate = StorageEngine::instance()->compaction_manager()->pick_candidate();
        VLOG(2) << "get candidate:" << compaction_candidate.to_string();
        if (!compaction_candidate.is_valid()) {
            // means there no candidate tablet, break
            LOG(INFO) << "do not get a qualified candidate";
            break;
        }

        int64_t tablet_id = compaction_candidate.tablet->tablet_id();
        CompactionType compaction_type = compaction_candidate.type;
        VLOG(2) << "try tablet:" << tablet_id << ", compaction type:" << to_string(compaction_type);

        if (!_check_precondition(compaction_candidate)) {
            VLOG(2) << "check compaction precondition failed, candidate info: " << compaction_candidate.to_string();
            break;
        }

        if (_can_do_compaction(compaction_candidate, &compaction_task)) {
            VLOG(2) << "get a qualified tablet:" << tablet_id << ", compaction type:" << to_string(compaction_type)
                    << ", compaction task:" << compaction_task->get_task_info();
            break;
        }

        // reschedule task if the compaction cannot be done.
        tmp_candidates.emplace_back(std::move(compaction_candidate));
    }
    VLOG(2) << "tmp tablets size:" << tmp_candidates.size();
    StorageEngine::instance()->compaction_manager()->insert_candidates(std::move(tmp_candidates));
    return compaction_task;
}

} // namespace starrocks
