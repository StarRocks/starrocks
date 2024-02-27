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

#ifdef USE_STAROS
#include "storage/lake/local_pk_index_manager.h"

#include <chrono>

#include "fs/fs_util.h"
#include "gutil/strings/join.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"
#include "testutil/sync_point.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"

namespace starrocks::lake {

LocalPkIndexManager::~LocalPkIndexManager() {
    if (_worker_thread_pool != nullptr) {
        _worker_thread_pool->shutdown();
    }
}

Status LocalPkIndexManager::init() {
    const int max_pk_index_compaction_thread_cnt =
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

class LakePkIndexMajorCompactionTask : public Runnable {
public:
    LakePkIndexMajorCompactionTask(int64_t tablet_id, DataDir* data_dir, LocalPkIndexManager* mgr,
                                   UpdateManager* update_mgr)
            : _tablet_id(tablet_id), _data_dir(data_dir), _mgr(mgr), _update_mgr(update_mgr) {}

    void run() override {
        WARN_IF_ERROR(_update_mgr->pk_index_major_compaction(_tablet_id, _data_dir),
                      "Failed to run PkIndexMajorCompactionTask");
        _mgr->unmark_running(_tablet_id, _data_dir);
    }

private:
    int64_t _tablet_id;
    DataDir* _data_dir;
    LocalPkIndexManager* _mgr;
    UpdateManager* _update_mgr;
};

void LocalPkIndexManager::schedule(const std::function<std::vector<TabletAndScore>()>& pick_algo,
                                   UpdateManager* update_mgr) {
    update_ready_tablet_queue(pick_algo);
    for (auto it = _ready_tablets_queue.begin(); it != _ready_tablets_queue.end();) {
        auto& tablet_score = *it;
        auto tablet_id = tablet_score.first;
        if (is_running(tablet_id)) {
            // remove this tablet because it is already running
            it = _ready_tablets_queue.erase(it);
            continue;
        }
        auto* data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
        if (data_dir == nullptr) {
            continue;
        }
        if (disk_limit(data_dir)) {
            // skip it, may re-run it next round.
            ++it;
            continue;
        }
        mark_running(tablet_id, data_dir);
        std::shared_ptr<Runnable> r =
                std::make_shared<LakePkIndexMajorCompactionTask>(tablet_id, data_dir, this, update_mgr);
        auto st = _worker_thread_pool->submit(std::move(r));
        if (!st.ok()) {
            // Resource busy, break and quit
            unmark_running(tablet_id, data_dir);
            LOG(ERROR) << strings::Substitute("submit pk index compaction task failed: $0", st.to_string());
            break;
        }
        it = _ready_tablets_queue.erase(it);
    }
}

void LocalPkIndexManager::update_ready_tablet_queue(const std::function<std::vector<TabletAndScore>()>& pick_algo) {
    size_t current_time = time(nullptr);
    if (current_time - _last_schedule_time > config::pindex_major_compaction_schedule_interval_seconds) {
        // need re-schedule
        _ready_tablets_queue = pick_algo();
        _last_schedule_time = current_time;
    }
}

void LocalPkIndexManager::mark_running(int64_t tablet_id, DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.insert(tablet_id);
    _data_dir_to_task_num_map[data_dir]++;
}

void LocalPkIndexManager::unmark_running(int64_t tablet_id, DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    _running_tablets.erase(tablet_id);
    _data_dir_to_task_num_map[data_dir]--;
}

bool LocalPkIndexManager::is_running(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mutex);
    return _running_tablets.count(tablet_id) > 0;
}

bool LocalPkIndexManager::disk_limit(DataDir* data_dir) {
    std::lock_guard<std::mutex> guard(_mutex);
    return _data_dir_to_task_num_map[data_dir] >= std::max(1, config::pindex_major_compaction_limit_per_disk);
}

Status LocalPkIndexManager::update_max_threads(int max_threads) {
    if (_worker_thread_pool != nullptr) {
        RETURN_IF_ERROR(_worker_thread_pool->update_max_threads(max_threads));
    }
    return Status::OK();
}

Status LocalPkIndexManager::clear_persistent_index(int64_t tablet_id) {
    // remove meta in RocksDB
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    if (data_dir == nullptr) {
        // Properly handle the case where the data_dir is null before proceeding.
        return Status::NotFound("Data directory not found for tablet_id=" + std::to_string(tablet_id));
    }

    WriteBatch wb;
    auto status = TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id);
    if (status.ok()) {
        status = data_dir->get_meta()->write_batch(&wb);
        if (!status.ok()) {
            LOG(WARNING) << "fail to remove persistent index meta, tablet_id=[" + std::to_string(tablet_id)
                         << "] error[" << status.to_string() << "]";
        } else {
            // remove tablet persistent_index dir
            auto pk_path = data_dir->get_persistent_index_path();
            auto tablet_pk_path = pk_path + "/" + std::to_string(tablet_id);
            status = fs::remove_all(tablet_pk_path);
            if (!status.ok()) {
                LOG(WARNING) << "fail to remove local persistent index dir=[" + tablet_pk_path << "] error["
                             << status.to_string() << "]";
            }
        }
    }

    return status;
}

void LocalPkIndexManager::gc(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {
    auto tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
    int64_t t_start = MonotonicMillis();

    std::vector<int64_t> not_in_worker_tablet_ids;
    std::vector<int64_t> dir_changed_tablet_ids;
    std::vector<int64_t> removed_dir_tablet_ids;

    auto pk_path = data_dir->get_persistent_index_path();
    LOG(INFO) << "start to gc local persistent index dir:" << pk_path;
    for (const auto& tablet_id : tablet_ids) {
        int64_t id = 0;
        try {
            id = std::stoll(tablet_id);
        } catch (std::invalid_argument const& ex) {
            LOG(ERROR) << "Invalid tablet: " << tablet_id;
            continue;
        }
        // judge whether tablet should be in the data_dir or not,
        // for data_dir may change if config:storage_path changed.
        // just remove if not.
        if (StorageEngine::instance()->get_persistent_index_store(id) != data_dir) {
            dir_changed_tablet_ids.push_back(id);
            if (clear_persistent_index(id).ok()) {
                removed_dir_tablet_ids.push_back(id);
            }
        } else if (!tablet_manager->is_tablet_in_worker(id)) {
            // the shard may be scheduled to other nodes
            if (!update_manager->try_lock_pk_index_shard(id)) {
                LOG(WARNING) << "Fail to lock pk index, tablet id: " << id;
                continue;
            }
            not_in_worker_tablet_ids.emplace_back(id);
            // judge whether tablet is scheduled again,
            // and pk_index_shard write_lock has been hold, so no process will build the persistent index.
            if (!tablet_manager->is_tablet_in_worker(id)) {
                // try to remove pk index cache to avoid continuing to use the index in the cache after deletion.
                if (update_manager->try_remove_primary_index_cache(id)) {
                    if (clear_persistent_index(id).ok()) {
                        removed_dir_tablet_ids.push_back(id);
                    }
                }
            }
            update_manager->unlock_pk_index_shard(id);
        }
    }
    int64_t t_end = MonotonicMillis();
    LOG(INFO) << "finish gc local persistent index dir: " << pk_path
              << ", found tablet not in the worker, tablet_ids: " << JoinInts(not_in_worker_tablet_ids, ",")
              << ", data_dir changed tablet_ids: " << JoinInts(dir_changed_tablet_ids, ",")
              << ", and removed dir successfully, tablet_ids: " << JoinInts(removed_dir_tablet_ids, ",")
              << ", cost:" << t_end - t_start << "ms";
}

bool LocalPkIndexManager::need_evict_tablet(const std::string& tablet_pk_path) {
    bool ret = true;
    auto now = time(nullptr);

    auto mtime_or = FileSystem::Default()->get_file_modified_time(tablet_pk_path);
    if (!mtime_or.ok()) {
        return false;
    }
    auto mtime = *mtime_or;
    if (now - mtime < config::lake_local_pk_index_unused_threshold_seconds) {
        ret = false;
    }
    TEST_SYNC_POINT_CALLBACK("LocalPkIndexManager::evict:2", &ret);
    if (!ret) {
        return ret;
    }

    auto st = FileSystem::Default()->iterate_dir2(tablet_pk_path, [&](DirEntry entry) {
        if (now - entry.mtime.value() < config::lake_local_pk_index_unused_threshold_seconds) {
            ret = false;
            return false;
        }
        return true;
    });
    if (!st.ok()) {
        return false;
    }
    TEST_SYNC_POINT_CALLBACK("LocalPkIndexManager::evict:3", &ret);
    return ret;
}

void LocalPkIndexManager::evict(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {
    bool need_evict = false;
    auto space_info_or = FileSystem::Default()->space(data_dir->path());
    if (space_info_or.ok()) {
        auto space_info = *space_info_or;
        need_evict = (double)space_info.free < (double)space_info.capacity * config::starlet_cache_evict_low_water;
    }
    TEST_SYNC_POINT_CALLBACK("LocalPkIndexManager::evict:1", &need_evict);
    if (!need_evict) {
        return;
    }

    auto pk_path = data_dir->get_persistent_index_path();
    LOG(INFO) << "start to evict local persistent index dir:" << pk_path;
    int64_t t_start = MonotonicMillis();

    std::vector<int64_t> tablet_ids_to_be_evicted;
    std::vector<int64_t> removed_dir_tablet_ids;

    for (const auto& tablet_id : tablet_ids) {
        int64_t id = 0;
        try {
            id = std::stoll(tablet_id);
        } catch (std::invalid_argument const& ex) {
            LOG(ERROR) << "Invalid tablet: " << tablet_id;
            continue;
        }
        auto tablet_pk_path = pk_path + "/" + tablet_id;
        if (!need_evict_tablet(tablet_pk_path)) {
            continue;
        }

        tablet_ids_to_be_evicted.emplace_back(id);
        if (!update_manager->try_lock_pk_index_shard(id)) {
            LOG(WARNING) << "Fail to lock pk index, tablet id: " << id;
            continue;
        }
        if (update_manager->try_remove_primary_index_cache(id)) {
            if (clear_persistent_index(id).ok()) {
                removed_dir_tablet_ids.push_back(id);
            }
        }
        update_manager->unlock_pk_index_shard(id);
    }

    int64_t t_end = MonotonicMillis();
    LOG(INFO) << "finish evict local persistent index dir: " << pk_path
              << ", found tablet_ids to be evicted: " << JoinInts(tablet_ids_to_be_evicted, ",")
              << ", and removed dir successfully, tablet_ids: " << JoinInts(removed_dir_tablet_ids, ",")
              << ", cost:" << t_end - t_start << "ms";
}

} // namespace starrocks::lake

#endif // USE_STAROS
