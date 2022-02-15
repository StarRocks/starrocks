// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "env/env_hdfs.h"
namespace starrocks {

// Cache for HDFS file system
class HdfsFsCache {
public:
    using FileOpenLimit = std::atomic<int32_t>;
    using FileOpenLimitPtr = FileOpenLimit*;
    using HdfsFsMap = std::unordered_map<std::string, std::pair<HdfsFsHandle, std::unique_ptr<FileOpenLimit>>>;

    static HdfsFsCache* instance() {
        static HdfsFsCache s_instance;
        return &s_instance;
    }

    // This function is thread-safe
    Status get_connection(const std::string& namenode, HdfsFsHandle* handle, FileOpenLimitPtr* semaphore);

private:
    std::mutex _lock;
    HdfsFsMap _cache;

    HdfsFsCache() = default;
    HdfsFsCache(const HdfsFsCache&) = delete;
    const HdfsFsCache& operator=(const HdfsFsCache&) = delete;
};

} // namespace starrocks
