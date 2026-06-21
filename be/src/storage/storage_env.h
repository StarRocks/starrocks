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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"

namespace starrocks {

class MemTracker;
class StorePathRegistry;

namespace lake {
class LakePersistentIndexParallelCompactMgr;
class LocationProvider;
class ReplicationTxnManager;
class TabletManager;
class UpdateManager;
} // namespace lake

enum class LakeLocationProviderMode {
    kDisabled,
    kStarlet,
    kFixed,
};

struct StorageEnvOptions {
    LakeLocationProviderMode lake_location_provider_mode = LakeLocationProviderMode::kDisabled;
    const StorePathRegistry* store_path_registry = nullptr;
    MemTracker* update_mem_tracker = nullptr;
    int64_t lake_metadata_cache_limit = 0;
};

// Process-scoped storage resource owner. Keep business logic dependencies
// explicitly injected instead of using StorageEnv as a broad service locator.
class StorageEnv {
public:
    static StorageEnv* GetInstance();

    StorageEnv();
    ~StorageEnv();

    StorageEnv(const StorageEnv&) = delete;
    StorageEnv& operator=(const StorageEnv&) = delete;

    Status init(const StorageEnvOptions& options);
    void stop();
    void stop_lake_tablet_manager();
    void destroy();

    std::shared_ptr<lake::LocationProvider> lake_location_provider() const { return _lake_location_provider; }
    lake::TabletManager* lake_tablet_manager() const { return _lake_tablet_manager.get(); }
    lake::UpdateManager* lake_update_manager() const { return _lake_update_manager.get(); }
    lake::ReplicationTxnManager* lake_replication_txn_manager() const { return _lake_replication_txn_manager.get(); }
    lake::LakePersistentIndexParallelCompactMgr* parallel_compact_mgr() const { return _parallel_compact_mgr.get(); }

private:
    std::shared_ptr<lake::LocationProvider> _lake_location_provider;
    std::unique_ptr<lake::UpdateManager> _lake_update_manager;
    std::unique_ptr<lake::TabletManager> _lake_tablet_manager;
    std::unique_ptr<lake::ReplicationTxnManager> _lake_replication_txn_manager;
    std::unique_ptr<lake::LakePersistentIndexParallelCompactMgr> _parallel_compact_mgr;
};

} // namespace starrocks
