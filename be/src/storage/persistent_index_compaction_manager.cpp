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

#include "storage/persistent_index_compaction_manager.h"

#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"

namespace starrocks {

PersistentIndexCompactionManager::~PersistentIndexCompactionManager() {
    if (_worker_thread_pool != nullptr) {
        _worker_thread_pool->shutdown();
    }
}

Status PersistentIndexCompactionManager::init() {
    const int max_pk_index_compaction_thread_cnt =
            config::pindex_major_compaction_num_threads > 0
                    ? config::pindex_major_compaction_num_threads
                    : std::max((size_t)1, StorageEngine::instance()->get_store_num() * 2);
    RETURN_IF_ERROR(ThreadPoolBuilder("pk_index_compaction_worker")
                            .set_min_threads(1)
                            .set_max_threads(max_pk_index_compaction_thread_cnt)
                            .build(&_worker_thread_pool));
    REGISTER_THREAD_POOL_METRICS(pk_index_compaction, _worker_thread_pool);

    return Status::OK();
}

class PkIndexMajorCompactionTask : public Runnable {
public:
    PkIndexMajorCompactionTask(TabletSharedPtr tablet, PersistentIndexCompactionManager* mgr)
            : _tablet(std::move(tablet)), _mgr(mgr) {}

    void run() override {
        DeferOp defer([&]() {
            // Must call `unmark_running()` after run() end.
            _mgr->unmark_running(_tablet->tablet_id(), _tablet->data_dir());
        });
        std::shared_lock migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
        if (!migration_rlock.owns_lock() || Tablet::check_migrate(_tablet)) {
            // Skip pk index major compaction when this tablet is migrating.
            return;
        }
        WARN_IF_ERROR(_tablet->updates()->pk_index_major_compaction(), "Failed to run PkIndexMajorCompactionTask");
    }

private:
    TabletSharedPtr _tablet;
    PersistentIndexCompactionManager* _mgr;
};

void PersistentIndexCompactionManager::schedule(const std::function<std::vector<TabletAndScore>()>& pick_algo) {
    update_ready_tablet_queue(pick_algo);
    for (auto it = _ready_tablets_queue.begin(); it != _ready_tablets_queue.end();) {
        auto& tablet_score = *it;
        auto tablet_id = tablet_score.first;
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (is_running(tablet_id) || tablet == nullptr) {
            // remove this tablet because it is already running
            it = _ready_tablets_queue.erase(it);
            continue;
        }
        if (disk_limit(tablet->data_dir())) {
            // skip it, may re - run it next round.
            ++it;
            continue;
        }
        mark_running(tablet_id, tablet->data_dir());
        std::shared_ptr<Runnable> r = std::make_shared<PkIndexMajorCompactionTask>(tablet, this);
        auto st = _worker_thread_pool->submit(std::move(r));
        if (!st.ok()) {
            // Resource busy, break and quit
            unmark_running(tablet_id, tablet->data_dir());
            LOG(ERROR) << strings::Substitute("submit pk index compaction task failed: $0", st.to_string());
            break;
        }
        it = _ready_tablets_queue.erase(it);
    }
}

void PersistentIndexCompactionManager::update_ready_tablet_queue(
        const std::function<std::vector<TabletAndScore>()>& pick_algo) {
    size_t current_time = time(nullptr);
    if (current_time - _last_schedule_time > config::pindex_major_compaction_schedule_interval_seconds) {
        // need re-schedule
        _ready_tablets_queue = pick_algo();
        _last_schedule_time = current_time;
    }
}

void PersistentIndexCompactionManager::mark_running(int64_t tablet_id, DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.insert(tablet_id);
    _data_dir_to_task_num_map[data_dir]++;
}

void PersistentIndexCompactionManager::unmark_running(int64_t tablet_id, DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.erase(tablet_id);
    _data_dir_to_task_num_map[data_dir]--;
}

bool PersistentIndexCompactionManager::is_running(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mutex);
    return _running_tablets.count(tablet_id) > 0;
}

bool PersistentIndexCompactionManager::disk_limit(DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    return _data_dir_to_task_num_map[data_dir] >= std::max(1, config::pindex_major_compaction_limit_per_disk);
}

Status PersistentIndexCompactionManager::update_max_threads(int max_threads) {
    if (_worker_thread_pool != nullptr) {
        RETURN_IF_ERROR(_worker_thread_pool->update_max_threads(max_threads));
    }
    return Status::OK();
}

} // namespace starrocks
