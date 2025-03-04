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

#include <sys/stat.h>

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "cache/block_cache/cache_options.h"
#include "common/status.h"
#include "fs/fs.h"
#include "util/disk_info.h"

namespace starrocks {

class BlockCache;

class DiskSpace {
public:
    // Wrap a new class to make it easy controlled in unittest.
    class FileSystemWrapper {
    public:
        virtual StatusOr<SpaceInfo> space(const std::string& path) { return FileSystem::Default()->space(path); }

        virtual StatusOr<size_t> directory_size(const std::string& dir);

        virtual dev_t device_id(const std::string& path);

        virtual ~FileSystemWrapper() {}
    };

    struct DiskStats {
        int64_t capacity_bytes = 0;
        int64_t available_bytes = 0;

        double used_rate() { return static_cast<double>(capacity_bytes - available_bytes) / capacity_bytes; }
    };

    struct AdjustContext {
        size_t total_cache_quota = 0;
        size_t total_cache_usage = 0;
    };

    DiskSpace(dev_t device_id, const std::string& path, std::shared_ptr<FileSystemWrapper> fs)
            : _device_id(device_id), _path(path), _fs(fs) {}

    Status init_spaces(const std::vector<DirSpace>& dir_spaces);

    bool adjust_spaces(const AdjustContext& ctx);

    std::vector<DirSpace>& dir_spaces() { return _dir_spaces; }

    size_t total_cache_quota();

    // The align unit when adjusting cache disk quota. We set it to 10G to keep consistent with underlying cache file size,
    // which can help reduce processing the specail tail files.
    const static size_t kQuotaAlignUnit;

    // The cache usage threshold for automatically increase disk quota, if the cache usage is under this level,
    // the disk expansion will be skipped.
    // NOTICE: When users update the disk quota manually, this threshold will be ignored.
    const static double kAutoIncreaseThreshold;

private:
    Status _update_disk_stats();

    void _revise_disk_stats_by_cache_dir();

    void _update_spaces_by_cache_usage(const AdjustContext& ctx);

    void _update_spaces_by_cache_quota(size_t cache_avalil_bytes);

    bool _allow_expansion(const AdjustContext& ctx);

    size_t _check_cache_low_limit(int64_t cache_quota);

    dev_t _device_id = 0;
    std::string _path;
    DiskStats _disk_stats;

    // The datacache can be configured to have multiple data directories on the same physical disk.
    std::vector<DirSpace> _dir_spaces;
    std::shared_ptr<FileSystemWrapper> _fs = nullptr;
    int64_t _disk_free_period = 0;
    bool _disabled = false;
};

class DiskSpaceMonitor {
public:
    DiskSpaceMonitor(BlockCache* cache);
    ~DiskSpaceMonitor();

    Status init(std::vector<DirSpace>* dir_spaces);

    void start();

    void stop();

    bool is_stopped();

    std::vector<DirSpace> all_dir_spaces();

    static std::string to_string(const std::vector<DirSpace>& dir_spaces);

private:
    void _adjust_datacache_callback();

    bool _adjust_spaces_by_disk_usage();

    void _update_cache_stats();

    Status _update_cache_quota(const std::vector<DirSpace>& dir_spaces);

    std::vector<DiskSpace> _disk_spaces;

    std::atomic<bool> _stopped = true;
    std::atomic<bool> _updating = false;
    std::thread _adjust_datacache_thread;
    std::mutex _mutex;

    size_t _total_cache_usage = 0;
    size_t _total_cache_quota = 0;
    BlockCache* _cache = nullptr;
    std::shared_ptr<DiskSpace::FileSystemWrapper> _fs = nullptr;
};

} // namespace starrocks
