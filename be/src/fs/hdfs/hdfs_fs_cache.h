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
        if (hdfs_fs != nullptr && _owns_hdfs_fs) {
            // Only disconnect if we own the hdfs_fs instance.
            // When hdfs_client_force_new_instance is false, the hdfs_fs is managed by
            // Hadoop's internal FileSystem cache, and we should not close it here
            // to avoid "FileSystem closed" errors for other users of the same cache entry.
            hdfsDisconnect(hdfs_fs);
        }
    }

    std::string namenode;
    hdfsFS hdfs_fs{nullptr};
    // Whether this client owns the hdfs_fs instance and should disconnect it on destruction.
    // When hdfs_client_force_new_instance=true, each HdfsFsClient has its own hdfs_fs instance.
    // When hdfs_client_force_new_instance=false, multiple HdfsFsClient may share the same
    // Hadoop-cached hdfs_fs, so we should not disconnect it.
    bool _owns_hdfs_fs{true};
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

// Helper function to determine whether to force new FileSystem instance.
// Extracted for testability.
// Priority: THdfsProperties.disable_cache > config::hdfs_client_force_new_instance
bool should_force_new_hdfs_instance(const THdfsProperties* properties);

} // namespace starrocks
