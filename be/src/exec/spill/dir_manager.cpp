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

#include "exec/spill/dir_manager.h"
#include "storage/options.h"
#include "common/config.h"

namespace starrocks {
namespace spill {

Status DirManager::init() {
    LOG(INFO) << "init DirManager";
    std::vector<starrocks::StorePath> storage_paths;
    RETURN_IF_ERROR(parse_conf_store_paths(config::storage_root_path, &storage_paths));
    if (storage_paths.empty()) {
        return Status::InvalidArgument("cannot find storage_root_path");
    }

    for (const auto& path : storage_paths) {
        std::string spill_dir_path = path.path + "/" + config::spill_local_storage_dir;
        LOG(INFO) << "create fs for spill dir: " << spill_dir_path;
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(spill_dir_path));
        LOG(INFO) << "create dir if missing";
        RETURN_IF_ERROR(fs->create_dir_if_missing(spill_dir_path));
        LOG(INFO) << "iterator dir";
        RETURN_IF_ERROR(fs->iterate_dir(spill_dir_path, [fs, &spill_dir_path] (std::string_view sub_dir) {
            LOG(INFO) <<"sub dir: " << sub_dir; 
            std::string dir = spill_dir_path + "/" + std::string(sub_dir.begin(), sub_dir.end());
            fs->delete_dir_recursive(dir);
            // fs->delete_dir_recursive(std::string(sub_dir.begin(), sub_dir.end()));
            return true;
        }));
        LOG(INFO) << "create spill dir path: " << spill_dir_path;
        _dirs.emplace_back(std::make_shared<Dir>(spill_dir_path, std::move(fs)));
    }
    return Status::OK();
}

StatusOr<Dir*> DirManager::acquire_writable_dir(const AcquireDirOptions& opts) {
    // put data to _dirs
    return _dirs[0].get();
}

}
}