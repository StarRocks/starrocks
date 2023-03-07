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

#include <mutex>
#include <vector>

#include "common/config.h"
#include "exec/spill/spiller_path_provider.h"
#include "gen_cpp/Types_types.h"
#include "storage/options.h"

namespace starrocks {

std::vector<std::string> QuerySpillManager::_spill_root_paths;

Status QuerySpillManager::init() {
    std::vector<starrocks::StorePath> paths;
    RETURN_IF_ERROR(parse_conf_store_paths(config::storage_root_path, &paths));
    for (const auto& path : paths) {
        _spill_root_paths.emplace_back(path.path + "/" + config::spill_local_storage_dir);
    }
    return Status::OK();
}

Status QuerySpillManager::init(const TUniqueId& uid) {
    if (_spill_root_paths.empty()) {
        return Status::InternalError("Not Found Spill Path");
    }

    // init path
    for (const auto& path : _spill_root_paths) {
        RETURN_IF_ERROR(_fs->create_dir_if_missing(path));
    }

    _uid = uid;
    return Status::OK();
}

std::vector<std::string> QuerySpillManager::_spill_paths(const TUniqueId& uid) const {
    std::vector<std::string> res;
    for (const auto& path : _spill_root_paths) {
        res.emplace_back(path + "/" + print_id(uid));
    }
    return res;
}

SpillPathProviderFactory QuerySpillManager::provider(const std::string& prefix) {
    auto paths = _spill_paths(_uid);
    std::lock_guard guard(_mutex);
    if (auto iter = _spill_provider_factorys.find(prefix); iter == _spill_provider_factorys.end()) {
        auto path_provider = std::make_shared<LocalPathProvider>(paths, prefix, _fs);
        auto factory = [provider = std::move(path_provider)]() -> StatusOr<std::shared_ptr<SpillerPathProvider>> {
            return provider;
        };
        _spill_provider_factorys.emplace(prefix, factory);
        return factory;
    } else {
        return iter->second;
    }
}

} // namespace starrocks