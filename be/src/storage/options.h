// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/options.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/olap_define.h"
#include "util/uid_util.h"

namespace starrocks {

class MemTracker;

struct StorePath {
    StorePath() = default;
    explicit StorePath(std::string path_) : path(std::move(path_)), storage_medium(TStorageMedium::HDD) {}
    std::string path;
    TStorageMedium::type storage_medium{TStorageMedium::HDD};
};

// parse a single root path of storage_root_path
Status parse_root_path(const std::string& root_path, StorePath* path);

Status parse_conf_store_paths(const std::string& config_path, std::vector<StorePath>* path);

Status parse_conf_block_cache_paths(const std::string& config_path, std::vector<std::string>* paths);

struct EngineOptions {
    // list paths that tablet will be put into.
    std::vector<StorePath> store_paths;
    // BE's UUID. It will be reset every time BE restarts.
    UniqueId backend_uid{0, 0};
    MemTracker* compaction_mem_tracker = nullptr;
    MemTracker* update_mem_tracker = nullptr;
    // config path to store cluster_id, used by DummyStorageEngine
    std::string conf_path;
};
} // namespace starrocks
