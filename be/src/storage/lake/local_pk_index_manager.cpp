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

#include "storage/lake/local_pk_index_manager.h"

#include <chrono>

#include "fs/fs_util.h"
#include "gutil/strings/join.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

void LocalPkIndexManager::gc(UpdateManager* update_manager,
                             std::unordered_map<DataDir*, std::set<std::string>>& store_to_tablet_ids) {
    auto tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
    int64_t t_start = MonotonicMillis();

    std::vector<int64_t> not_in_worker_tablet_ids;
    std::vector<int64_t> dir_changed_tablet_ids;
    std::vector<int64_t> removed_dir_tablet_ids;

    for (const auto& [data_dir, tablet_ids] : store_to_tablet_ids) {
        auto pk_path = data_dir->get_persistent_index_path();
        LOG(INFO) << "start to gc local persistent index dir:" << pk_path;
        for (const auto& tablet_id : tablet_ids) {
            auto tablet_pk_path = pk_path + "/" + tablet_id;
            int64_t id = std::stoll(tablet_id);
            // judge whether tablet should be in the data_dir or not,
            // for data_dir may change if config:storage_path changed.
            // just remove if not.
            if (StorageEngine::instance()->get_persistent_index_store(id) != data_dir) {
                dir_changed_tablet_ids.push_back(id);
                if (StorageEngine::instance()->clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
                    removed_dir_tablet_ids.push_back(id);
                }
            } else if (!tablet_manager->is_tablet_in_worker(id)) {
                // the shard may be scheduled to other nodes
                if (update_manager->try_lock_pk_index_shard(id)) {
                    not_in_worker_tablet_ids.emplace_back(id);
                    // judge whether tablet is scheduled again,
                    // and pk_index_shard write_lock has been hold, so no process will build the persistent index.
                    if (!tablet_manager->is_tablet_in_worker(id)) {
                        // try to remove pk index cache to avoid continuing to use the index in the cache after deletion.
                        if (update_manager->try_remove_primary_index_cache(id)) {
                            if (StorageEngine::instance()->clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
                                removed_dir_tablet_ids.push_back(id);
                            }
                        }
                    }
                    update_manager->unlock_pk_index_shard(id);
                }
            }
        }
        int64_t t_end = MonotonicMillis();
        LOG(INFO) << "finish gc local persistent index dir: " << pk_path
                  << ", found tablet not in the worker, tablet_ids: " << JoinInts(not_in_worker_tablet_ids, ",")
                  << ", data_dir changed tablet_ids: " << JoinInts(dir_changed_tablet_ids, ",")
                  << ", and removed dir successfully, tablet_ids: " << JoinInts(removed_dir_tablet_ids, ",")
                  << ", cost:" << t_end - t_start << "ms";
    }
}

void LocalPkIndexManager::evict(UpdateManager* update_manager,
                                std::unordered_map<DataDir*, std::set<std::string>>& store_to_tablet_ids) {
    for (const auto& [data_dir, tablet_ids] : store_to_tablet_ids) {
        bool need_evict = false;
        auto space_info_or = FileSystem::Default()->space(data_dir->path());
        if (space_info_or.ok()) {
            auto space_info = *space_info_or;
            need_evict = (double)space_info.free < (double)space_info.capacity * config::starlet_cache_evict_low_water;
        }
        if (!need_evict) {
            continue;
        }

        auto pk_path = data_dir->get_persistent_index_path();
        LOG(INFO) << "start to evict local persistent index dir:" << pk_path;
        int64_t t_start = MonotonicMillis();

        std::vector<int64_t> tablet_ids_to_be_evicted;
        std::vector<int64_t> removed_dir_tablet_ids;

        for (const auto& tablet_id : tablet_ids) {
            auto tablet_pk_path = pk_path + "/" + tablet_id;
            int64_t id = std::stoll(tablet_id);
            std::set<std::string> index_files;
            Status ret = fs::list_dirs_files(tablet_pk_path, nullptr, &index_files);
            if (!ret.ok()) {
                LOG(WARNING) << "fail to walk dir. path=[" + tablet_pk_path << "] error[" << ret.to_string() << "]";
                continue;
            }

            auto now = time(nullptr);
            auto mtime = (uint64_t)now;
            for (const auto& index_file : index_files) {
                auto index_file_path = tablet_pk_path + "/" + index_file;
                auto mtime_or = FileSystem::Default()->get_file_modified_time(index_file_path);
                if (!mtime_or.ok()) {
                    continue;
                }
                mtime = std::min(mtime, *mtime_or);
            }

            if (now - mtime > config::lake_local_pk_index_unused_threshold) {
                tablet_ids_to_be_evicted.emplace_back(id);
                if (update_manager->try_lock_pk_index_shard(id)) {
                    if (update_manager->try_remove_primary_index_cache(id)) {
                        if (StorageEngine::instance()->clear_persistent_index(data_dir, id, tablet_pk_path).ok()) {
                            removed_dir_tablet_ids.push_back(id);
                        }
                    }
                    update_manager->unlock_pk_index_shard(id);
                }
            }
        }

        int64_t t_end = MonotonicMillis();
        LOG(INFO) << "finish evict local persistent index dir: " << pk_path
                  << ", found tablet_ids to be evicted: " << JoinInts(tablet_ids_to_be_evicted, ",")
                  << ", and removed dir successfully, tablet_ids: " << JoinInts(removed_dir_tablet_ids, ",")
                  << ", cost:" << t_end - t_start << "ms";
    }
}

} // namespace starrocks::lake
