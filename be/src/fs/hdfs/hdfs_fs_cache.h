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

#include <hdfs/hdfs.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "fs/hdfs/fs_hdfs.h"
#include "util/random.h"

namespace starrocks {

class HdfsFsClient {
public:
    ~HdfsFsClient() {
        if (hdfs_fs != nullptr) {
            // hdfs_fs maybe a nullptr, if it create failed.
            hdfsDisconnect(hdfs_fs);
        }
    }
    std::string namenode;
    hdfsFS hdfs_fs;
};

// Cache for HDFS file system
class HdfsFsCache {
public:
    ~HdfsFsCache() = default;
    static HdfsFsCache* instance() {
        static HdfsFsCache s_instance;
        return &s_instance;
    }

    // This function is thread-safe
    Status get_connection(const std::string& namenode, std::shared_ptr<HdfsFsClient>& hdfs_client,
                          const FSOptions& options);

private:
    std::mutex _lock;
    std::unordered_map<std::string, std::shared_ptr<HdfsFsClient>> _cache_clients;
    std::vector<std::string> _cache_keys;
    Random _rand{(uint32_t)time(nullptr)};

    HdfsFsCache() = default;
    HdfsFsCache(const HdfsFsCache&) = delete;
    const HdfsFsCache& operator=(const HdfsFsCache&) = delete;
};

} // namespace starrocks
