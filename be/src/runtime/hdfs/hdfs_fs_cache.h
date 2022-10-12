// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <hdfs/hdfs.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "fs/fs_hdfs.h"

namespace starrocks {

class ObjectStore;

struct HdfsFsHandle {
    enum class Type { LOCAL, HDFS, S3 };
    Type type;
    std::string namenode;
    hdfsFS hdfs_fs;
    ObjectStore* object_store;
};

// Cache for HDFS file system
class HdfsFsCache {
public:
    using HdfsFsMap = std::unordered_map<std::string, HdfsFsHandle>;

    static HdfsFsCache* instance() {
        static HdfsFsCache s_instance;
        return &s_instance;
    }

    // This function is thread-safe
    Status get_connection(const std::string& namenode, HdfsFsHandle* handle, const FSOptions& options);

private:
    std::mutex _lock;
    HdfsFsMap _cache;

    HdfsFsCache() = default;
    HdfsFsCache(const HdfsFsCache&) = delete;
    const HdfsFsCache& operator=(const HdfsFsCache&) = delete;
};

} // namespace starrocks
