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

#include <cstdlib>
#include <memory>
#include <vector>

#include "gutil/strings/join.h"
#include "platform/store_path.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"

#ifdef USE_STAROS
#include <fslib/configuration.h>

#include "common/config_starlet_fwd.h"
#include "storage/lake/starlet_location_provider.h"
#endif

namespace starrocks {

StorageEnv* StorageEnv::GetInstance() {
    static StorageEnv s_storage_env;
    return &s_storage_env;
}

StorageEnv::StorageEnv() = default;

StorageEnv::~StorageEnv() = default;

Status StorageEnv::init(const StorageEnvOptions& options) {
    if (_lake_tablet_manager != nullptr || options.lake_location_provider_mode == LakeLocationProviderMode::kDisabled) {
        return Status::OK();
    }
    if (options.update_mem_tracker == nullptr) {
        return Status::InvalidArgument("StorageEnv lake update mem tracker is null");
    }
    if (options.store_path_registry == nullptr) {
        return Status::InvalidArgument("StorageEnv store path registry is null");
    }

    const auto& store_path_roots = options.store_path_registry->store_path_roots();

    std::shared_ptr<lake::LocationProvider> lake_location_provider;
    switch (options.lake_location_provider_mode) {
    case LakeLocationProviderMode::kStarlet:
#ifdef USE_STAROS
        lake_location_provider = std::make_shared<lake::StarletLocationProvider>();
        if (config::starlet_cache_dir.empty()) {
            std::vector<std::string> starlet_cache_paths;
            starlet_cache_paths.reserve(store_path_roots.size());
            for (const auto& store_path_root : store_path_roots) {
                starlet_cache_paths.emplace_back(store_path_root + "/starlet_cache");
            }
            config::starlet_cache_dir = JoinStrings(starlet_cache_paths, ":");
        }
        setenv(staros::starlet::fslib::kFslibCacheDir.c_str(), config::starlet_cache_dir.c_str(), 1);
        break;
#else
        return Status::NotSupported("StorageEnv Starlet lake location requires USE_STAROS");
#endif
    case LakeLocationProviderMode::kFixed:
        if (store_path_roots.empty()) {
            return Status::InvalidArgument("StorageEnv fixed lake location requires at least one store path");
        }
        lake_location_provider = std::make_shared<lake::FixedLocationProvider>(store_path_roots.front());
        break;
    case LakeLocationProviderMode::kDisabled:
        return Status::OK();
    }

    auto lake_update_manager =
            std::make_unique<lake::UpdateManager>(lake_location_provider, options.update_mem_tracker);
    auto lake_tablet_manager =
            std::make_unique<lake::TabletManager>(lake_location_provider, lake_update_manager.get(),
                                                  options.lake_metadata_cache_limit, options.store_path_registry);
    auto lake_replication_txn_manager = std::make_unique<lake::ReplicationTxnManager>(lake_tablet_manager.get());

    auto parallel_compact_mgr =
            std::make_unique<lake::LakePersistentIndexParallelCompactMgr>(lake_tablet_manager.get());
    RETURN_IF_ERROR(parallel_compact_mgr->init());
    lake_update_manager->set_parallel_compact_mgr(parallel_compact_mgr.get());

    _lake_location_provider = std::move(lake_location_provider);
    _lake_update_manager = std::move(lake_update_manager);
    _lake_tablet_manager = std::move(lake_tablet_manager);
    _lake_replication_txn_manager = std::move(lake_replication_txn_manager);
    _parallel_compact_mgr = std::move(parallel_compact_mgr);
    return Status::OK();
}

void StorageEnv::stop() {
    if (_parallel_compact_mgr != nullptr) {
        _parallel_compact_mgr->shutdown();
    }
}

void StorageEnv::stop_lake_tablet_manager() {
    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->stop();
    }
}

void StorageEnv::destroy() {
    _spill_dir_mgr = nullptr;
    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->prune_metacache();
    }
    _parallel_compact_mgr.reset();
    _lake_replication_txn_manager.reset();
    _lake_tablet_manager.reset();
    _lake_update_manager.reset();
    _lake_location_provider.reset();
}

} // namespace starrocks
