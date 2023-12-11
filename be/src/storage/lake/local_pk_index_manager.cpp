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

namespace starrocks::lake {

Status LocalPkIndexManager::clear_persistent_index(DataDir* data_dir, int64_t tablet_id, const std::string& dir) {
    // remove meta in RocksDB
    WriteBatch wb;
    auto status = TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id);
    if (status.ok()) {
        status = data_dir->get_meta()->write_batch(&wb);
        if (!status.ok()) {
            LOG(WARNING) << "fail to remove persistent index meta, tablet_id=[" + std::to_string(tablet_id)
                         << "] error[" << status.to_string() << "]";
        } else {
            // remove tablet persistent_index dir
            status = fs::remove_all(dir);
            if (!status.ok()) {
                LOG(WARNING) << "fail to remove local persistent index dir=[" + dir << "] error[" << status.to_string()
                             << "]";
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
        auto tablet_pk_path = pk_path + "/" + tablet_id;
        int64_t id = 0;
        try {
            std::stoll(tablet_id);
        } catch (std::invalid_argument const& ex) {
            LOG(ERROR) << "Invalid tablet: " << tablet_id;
            continue;
        }
        // judge whether tablet should be in the data_dir or not,
        // for data_dir may change if config:storage_path changed.
        // just remove if not.
        if (StorageEngine::instance()->get_persistent_index_store(id) != data_dir) {
            dir_changed_tablet_ids.push_back(id);
            if (clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
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
                    if (clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
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
            if (clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
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
