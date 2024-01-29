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

#include "exec/spill/query_spill_manager.h"

#include <cstdint>
#include <memory>

#include "exec/spill/dir_manager.h"
#include "exec/spill/file_block_manager.h"
#include "exec/spill/hybird_block_manager.h"
#include "exec/spill/log_block_manager.h"
#include "runtime/exec_env.h"

namespace starrocks::spill {

Status QuerySpillManager::init_block_manager(const TQueryOptions& query_options) {
    bool enable_spill_to_remote_storage =
            query_options.__isset.enable_spill_to_remote_storage && query_options.enable_spill_to_remote_storage;
    if (!enable_spill_to_remote_storage) {
        _block_manager = std::make_unique<LogBlockManager>(_uid, ExecEnv::GetInstance()->spill_dir_mgr());
        return Status::OK();
    }
    if (!query_options.__isset.spill_remote_storage_paths || !query_options.__isset.spill_remote_storage_conf) {
        DCHECK(false) << "enable spill_to_remote_storage but spill_remote_storage_paths or spill_remote_storage_conf "
                         "is not set";
        _block_manager = std::make_unique<LogBlockManager>(_uid, ExecEnv::GetInstance()->spill_dir_mgr());
        return Status::OK();
    }
    const auto& remote_storage_paths = query_options.spill_remote_storage_paths;
    auto remote_storage_conf = std::make_shared<TCloudConfiguration>(query_options.spill_remote_storage_conf);
    // init remote block manager
    std::vector<std::shared_ptr<Dir>> remote_dirs;
    for (const auto& path : remote_storage_paths) {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(path, FSOptions(remote_storage_conf.get())));
        RETURN_IF_ERROR(fs->create_dir_if_missing(path));
        auto dir = std::make_shared<RemoteDir>(path, std::move(fs), remote_storage_conf, INT64_MAX);
        remote_dirs.emplace_back(dir);
    }
    _remote_dir_manager = std::make_unique<DirManager>(remote_dirs);

    bool disable_spill_to_local_disk =
            query_options.__isset.disable_spill_to_local_disk && query_options.disable_spill_to_local_disk;
    if (disable_spill_to_local_disk) {
        _block_manager = std::make_unique<FileBlockManager>(_uid, _remote_dir_manager.get());
        return Status::OK();
    }

    // init block manager
    auto local_block_manager = std::make_unique<LogBlockManager>(_uid, ExecEnv::GetInstance()->spill_dir_mgr());
    auto remote_block_manager = std::make_unique<FileBlockManager>(_uid, _remote_dir_manager.get());
    _block_manager =
            std::make_unique<HyBirdBlockManager>(_uid, std::move(local_block_manager), std::move(remote_block_manager));

    return Status::OK();
}
} // namespace starrocks::spill