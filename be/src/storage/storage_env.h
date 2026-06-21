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

#include <memory>

#include "common/status.h"

namespace starrocks {

namespace lake {
class LakePersistentIndexParallelCompactMgr;
class TabletManager;
} // namespace lake

struct StorageEnvOptions {
    lake::TabletManager* lake_tablet_manager = nullptr;
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
    void destroy();

    lake::LakePersistentIndexParallelCompactMgr* parallel_compact_mgr() const { return _parallel_compact_mgr.get(); }

private:
    std::unique_ptr<lake::LakePersistentIndexParallelCompactMgr> _parallel_compact_mgr;
};

} // namespace starrocks
