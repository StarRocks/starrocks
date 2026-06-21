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

#include "storage/storage_env.h"

#include <memory>

#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"

namespace starrocks {

StorageEnv* StorageEnv::GetInstance() {
    static StorageEnv s_storage_env;
    return &s_storage_env;
}

StorageEnv::StorageEnv() = default;

StorageEnv::~StorageEnv() = default;

Status StorageEnv::init(const StorageEnvOptions& options) {
    if (_parallel_compact_mgr != nullptr || options.lake_tablet_manager == nullptr) {
        return Status::OK();
    }

    auto parallel_compact_mgr =
            std::make_unique<lake::LakePersistentIndexParallelCompactMgr>(options.lake_tablet_manager);
    RETURN_IF_ERROR(parallel_compact_mgr->init());
    _parallel_compact_mgr = std::move(parallel_compact_mgr);
    return Status::OK();
}

void StorageEnv::stop() {
    if (_parallel_compact_mgr != nullptr) {
        _parallel_compact_mgr->shutdown();
    }
}

void StorageEnv::destroy() {
    _parallel_compact_mgr.reset();
}

} // namespace starrocks
