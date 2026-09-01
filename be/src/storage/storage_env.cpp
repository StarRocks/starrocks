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

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <vector>

#ifdef WITH_TENANN
#include "tenann/index/index_cache.h"
#endif

#ifdef USE_STAROS
#include <fslib/configuration.h>
#endif

#include "base/string/parse_util.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "gutil/strings/join.h"
#include "platform/store_path.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_metrics.h"

#ifdef USE_STAROS
#include "common/config_starlet_fwd.h"
#include "storage/lake/remote_starlet_location_provider.h"
#include "storage/lake/starlet_location_provider.h"
#endif

namespace starrocks {

namespace {

constexpr int kVectorIndexCacheAsyncLoadMaxQueueSize = 4096;

} // namespace

StorageEnv* StorageEnv::GetInstance() {
    static StorageEnv s_storage_env;
    return &s_storage_env;
}

StorageEnv::StorageEnv() = default;

StorageEnv::~StorageEnv() {
    _deregister_lake_compaction_hook();
    destroy_vector_index_cache();
    _parallel_compact_mgr.reset();
    _lake_replication_txn_manager.reset();
    _lake_tablet_manager.reset();
    _lake_update_manager.reset();
    _remote_starlet_location_provider.reset();
    _lake_location_provider.reset();
}

Status StorageEnv::init(const StorageEnvOptions& options) {
    if (options.vector_index_mem_tracker != nullptr) {
        RETURN_IF_ERROR(init_vector_index_cache(options.process_mem_limit, options.vector_index_mem_tracker));
    }

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
    std::shared_ptr<lake::RemoteStarletLocationProvider> remote_starlet_location_provider;
#ifdef USE_STAROS
    remote_starlet_location_provider = std::make_shared<lake::RemoteStarletLocationProvider>();
#endif
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

    _lake_tablet_manager = std::move(lake_tablet_manager);
    _lake_location_provider = std::move(lake_location_provider);
    _remote_starlet_location_provider = std::move(remote_starlet_location_provider);
    _lake_update_manager = std::move(lake_update_manager);
    _lake_replication_txn_manager = std::move(lake_replication_txn_manager);
    _parallel_compact_mgr = std::move(parallel_compact_mgr);
    _register_lake_compaction_hook();
    return Status::OK();
}

void StorageEnv::_register_lake_compaction_hook() {
    _lake_compaction_hook_registered =
            StorageMetrics::instance()->register_lake_compaction_hook(_lake_tablet_manager->compaction_scheduler());
}

void StorageEnv::_deregister_lake_compaction_hook() {
    if (!_lake_compaction_hook_registered) {
        return;
    }

    StorageMetrics::instance()->deregister_lake_compaction_hook();
    _lake_compaction_hook_registered = false;
}

Status StorageEnv::init_vector_index_cache(int64_t process_mem_limit, MemTracker* vector_index_mem_tracker) {
#ifdef WITH_TENANN
    if (_vector_index_cache != nullptr) {
        return Status::OK();
    }
    if (vector_index_mem_tracker == nullptr) {
        return Status::InvalidArgument("StorageEnv vector index mem tracker is null");
    }

    ASSIGN_OR_RETURN(int64_t vi_capacity,
                     ParseUtil::parse_mem_spec(config::vector_query_cache_capacity, process_mem_limit));
    if (vi_capacity <= 0) {
        LOG(WARNING) << "vector_query_cache_capacity resolved to " << vi_capacity
                     << " bytes (raw=" << config::vector_query_cache_capacity
                     << ", process_mem_limit=" << process_mem_limit
                     << "); async vector index loading is disabled, but queries still load indexes synchronously. "
                        "The cache capacity is a soft limit";
        vi_capacity = 0;
    }
    auto vector_index_cache =
            std::make_unique<VectorIndexCache>(static_cast<size_t>(vi_capacity), vector_index_mem_tracker);
    const int async_load_threads = std::max(1, config::vector_index_cache_async_load_threads);
    if (async_load_threads != config::vector_index_cache_async_load_threads) {
        LOG(WARNING) << "vector_index_cache_async_load_threads must be positive; use 1 instead of "
                     << config::vector_index_cache_async_load_threads;
    }
    RETURN_IF_ERROR(
            vector_index_cache->init_async_load_pool(async_load_threads, kVectorIndexCacheAsyncLoadMaxQueueSize));
    _vector_index_cache = std::move(vector_index_cache);
    tenann::SetGlobalIndexCache(_vector_index_cache.get());
#endif
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

void StorageEnv::destroy_vector_index_cache() {
#ifdef WITH_TENANN
    if (_vector_index_cache != nullptr && tenann::GetGlobalIndexCache() == _vector_index_cache.get()) {
        tenann::SetGlobalIndexCache(nullptr);
    }
    if (_vector_index_cache != nullptr) {
        _vector_index_cache->shutdown_async_load_pool();
    }
#endif
    _vector_index_cache.reset();
}

void StorageEnv::destroy() {
    _spill_dir_mgr = nullptr;
    _deregister_lake_compaction_hook();
    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->prune_metacache();
    }
    _parallel_compact_mgr.reset();
    _lake_replication_txn_manager.reset();
    _lake_tablet_manager.reset();
    _lake_update_manager.reset();
    _remote_starlet_location_provider.reset();
    _lake_location_provider.reset();
}

} // namespace starrocks
