// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <hdfs/hdfs.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gutil/macros.h"

namespace starrocks {

// Cache for HDFS file system
class HdfsFsCache {
public:
    using HdfsFsMap = std::unordered_map<std::string, hdfsFS>;

    static HdfsFsCache* instance() {
        static HdfsFsCache s_instance;
        return &s_instance;
    }

    // This function is thread-safe
    Status get_connection(const std::string& path, hdfsFS* fs, HdfsFsMap* map = nullptr);

private:
    std::mutex _lock;
    HdfsFsMap _cache;

    HdfsFsCache() = default;
    HdfsFsCache(const HdfsFsCache&) = delete;
    const HdfsFsCache& operator=(const HdfsFsCache&) = delete;
};

} // namespace starrocks
