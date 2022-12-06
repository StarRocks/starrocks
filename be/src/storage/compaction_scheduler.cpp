// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
#include "util/starrocks_metrics.h"

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
            _cv.wait_for(lk, 1000ms);
        } else {
            if (compaction_task->compaction_type() == CompactionType::BASE_COMPACTION) {
                StarRocksMetrics::instance()->tablet_base_max_compaction_score.set_value(
                        compaction_task->compaction_score());
            } else {
                StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.set_value(
                        compaction_task->compaction_score());
            }

            compaction_task->set_compaction_scheduler(this);
            compaction_task->set_task_id(StorageEngine::instance()->compaction_manager()->next_compaction_task_id());
            LOG(INFO) << "submit task to compaction pool"
                      << ", task_id:" << compaction_task->task_id()
                      << ", tablet_id:" << compaction_task->tablet()->tablet_id()
                      << ", compaction_type:" << starrocks::to_string(compaction_task->compaction_type())
                      << ", compaction_score:" << compaction_task->compaction_score() << " for round:" << _round
                      << ", task_queue_size:" << StorageEngine::instance()->compaction_manager()->candidates_size();
            auto st = _compaction_pool->submit_func([compaction_task] { compaction_task->start(); });
            if (!st.ok()) {
                LOG(WARNING) << "submit compaction task " << compaction_task->task_id()
                             << " to compaction pool failed. status:" << st.to_string();
                compaction_task->tablet()->reset_compaction();
                CompactionCandidate candidate;
                candidate.tablet = compaction_task->tablet();
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
    while (!_cv.wait_for(lk, 100ms, [] { return _can_schedule_next(); })) {
    }
}

std::shared_ptr<CompactionTask> CompactionScheduler::_try_get_next_compaction_task() const {
    VLOG(2) << "try to get next qualified tablet for round:" << _round
            << ", current candidates size:" << StorageEngine::instance()->compaction_manager()->candidates_size();
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;

    if (StorageEngine::instance()->compaction_manager()->pick_candidate(&compaction_candidate)) {
        compaction_task = compaction_candidate.tablet->create_compaction_task();
    }

    return compaction_task;
}

} // namespace starrocks
