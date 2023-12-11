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
    int max_pk_index_compaction_thread_cnt =
            config::pindex_major_compaction_num_threads > 0
                    ? config::pindex_major_compaction_num_threads
                    : std::max((size_t)1, StorageEngine::instance()->get_store_num() * 2);
    RETURN_IF_ERROR(ThreadPoolBuilder("pk_index_compaction_worker")
                            .set_min_threads(1)
                            .set_max_threads(max_pk_index_compaction_thread_cnt)
                            .build(&_worker_thread_pool));
    REGISTER_GAUGE_STARROCKS_METRIC(pk_index_compaction_queue_count,
                                    [this]() { return _worker_thread_pool->num_queued_tasks(); });

    return Status::OK();
}

class PkIndexMajorCompactionTask : public Runnable {
public:
    PkIndexMajorCompactionTask(TabletSharedPtr tablet, PersistentIndexCompactionManager* mgr)
            : _tablet(std::move(tablet)), _mgr(mgr) {}

    void run() override {
        WARN_IF_ERROR(_tablet->updates()->pk_index_major_compaction(), "Failed to run PkIndexMajorCompactionTask");
        _mgr->unmark_running(_tablet->tablet_id());
    }

private:
    TabletSharedPtr _tablet;
    PersistentIndexCompactionManager* _mgr;
};

void PersistentIndexCompactionManager::schedule() {
    if (_too_many_tasks()) {
        // too many running tasks, stop schedule
        LOG(WARNING) << "PersistentIndex compaction schedule failed, too many running tasks";
        return;
    }
    std::vector<TabletAndScore> pick_tablets =
            StorageEngine::instance()->tablet_manager()->pick_tablets_to_do_pk_index_major_compaction();
    for (auto& tablet_score : pick_tablets) {
        const int64_t tablet_id = tablet_score.first->tablet_id();
        if (_need_skip(tablet_id)) {
            continue;
        }
        mark_running(tablet_id);
        std::shared_ptr<Runnable> r = std::make_shared<PkIndexMajorCompactionTask>(tablet_score.first, this);
        auto st = _worker_thread_pool->submit(std::move(r));
        if (!st.ok()) {
            unmark_running(tablet_id);
            LOG(ERROR) << strings::Substitute("submit pk index compaction task failed: $0", st.to_string());
            break;
        }
    }
}

void PersistentIndexCompactionManager::mark_running(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.insert(tablet_id);
}

void PersistentIndexCompactionManager::unmark_running(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.erase(tablet_id);
}

bool PersistentIndexCompactionManager::_too_many_tasks() {
    std::lock_guard<std::mutex> guard(_mutex);
    return _running_tablets.size() > MAX_RUNNING_TABLETS;
}

bool PersistentIndexCompactionManager::_need_skip(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mutex);
    return _running_tablets.count(tablet_id) > 0;
}

Status PersistentIndexCompactionManager::update_max_threads(int max_threads) {
    if (_worker_thread_pool != nullptr) {
        RETURN_IF_ERROR(_worker_thread_pool->update_max_threads(max_threads));
    }
    return Status::OK();
}

} // namespace starrocks