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

#include "common/config.h"
#include "storage/options.h"

namespace starrocks::spill {

Status DirManager::init() {
    std::vector<starrocks::StorePath> storage_paths;
    RETURN_IF_ERROR(parse_conf_store_paths(config::storage_root_path, &storage_paths));
    if (storage_paths.empty()) {
        return Status::InvalidArgument("cannot find storage_root_path");
    }

    for (const auto& path : storage_paths) {
        std::string spill_dir_path = path.path + "/" + config::spill_local_storage_dir;
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(spill_dir_path));
        RETURN_IF_ERROR(fs->create_dir_if_missing(spill_dir_path));
        RETURN_IF_ERROR(fs->iterate_dir(spill_dir_path, [fs, &spill_dir_path](std::string_view sub_dir) {
            std::string dir = spill_dir_path + "/" + std::string(sub_dir.begin(), sub_dir.end());
            fs->delete_dir_recursive(dir);
            return true;
        }));
        _dirs.emplace_back(std::make_shared<Dir>(spill_dir_path, std::move(fs)));
    }
    return Status::OK();
}

StatusOr<Dir*> DirManager::acquire_writable_dir(const AcquireDirOptions& opts) {
    // @TODO(silverbullet233): refine the strategy for dir selection
    size_t idx = _idx++ % _dirs.size();
    return _dirs[idx].get();
}

} // namespace starrocks::spill