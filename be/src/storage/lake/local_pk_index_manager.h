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

#include <set>
#include <string>
#include <unordered_map>

#include "common/status.h"

namespace starrocks {

class DataDir;

namespace lake {

class UpdateManager;

#ifdef USE_STAROS
class LocalPkIndexManager {
public:
    static void gc(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids);

    static void evict(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids);

    // remove pk index meta first, and if success then remove dir.
    static Status clear_persistent_index(int64_t tablet_id);

private:
    static bool need_evict_tablet(const std::string& tablet_pk_path);
};
#else
class LocalPkIndexManager {
public:
    static void gc(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {}

    static void evict(UpdateManager* update_manager, DataDir* data_dir, std::set<std::string>& tablet_ids) {}

    // remove pk index meta first, and if success then remove dir.
    static Status clear_persistent_index(int64_t tablet_id) { return Status::OK(); }

private:
    static bool need_evict_tablet(const std::string& tablet_pk_path) { return false; }
};
#endif // USE_STAROS

} // namespace lake
} // namespace starrocks
