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

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "cache/cache_options.h"
#include "cache/disk_cache/local_disk_cache_engine.h"
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

        int64_t used_bytes() { return capacity_bytes - available_bytes; }
    };

    struct DiskOptions {
        int64_t cache_lower_limit = 0;
        int64_t cache_upper_limit = 0;

        int64_t low_level_size = 0;
        int64_t safe_level_size = 0;
        int64_t high_level_size = 0;

        int64_t adjust_interval_s = 0;
        int64_t idle_for_expansion_s = 0;
    };

    struct AdjustContext {
        size_t total_cache_quota = 0;
        size_t total_cache_usage = 0;
    };

    DiskSpace(const std::string& path, std::shared_ptr<FileSystemWrapper> fs) : _path(path), _fs(fs) {}

    Status init_spaces(const std::vector<DirSpace>& dir_spaces);

    bool adjust_spaces(const AdjustContext& ctx);

    std::vector<DirSpace>& dir_spaces() { return _dir_spaces; }

    size_t cache_quota();

    // The alignment unit when adjusting cache disk quota.
    // We set it to 10G to keep consistent with underlying cache file size,
    // which can help reduce processing the special tail files.
    const static size_t kQuotaAlignUnit;

    // The cache usage threshold for automatically increase disk quota, if the cache usage is under this level,
    // the disk expansion will be skipped.
    // NOTICE: When users update the disk quota manually, this threshold will be ignored.
    const static double kAutoIncreaseThreshold;

private:
    Status _update_disk_stats();
    Status _update_disk_options();

    void _update_spaces_by_cache_quota(size_t cache_avalil_bytes);

    bool _allow_expansion(const AdjustContext& ctx);

    size_t _cache_usage(const AdjustContext& ctx);
    size_t _cache_file_space_usage();
    size_t _calc_new_cache_quota(size_t cur_cache_usage);

    size_t _check_cache_limit(int64_t cache_quota);

    size_t _check_cache_low_limit(int64_t cache_quota);

    size_t _check_cache_high_limit(int64_t cache_quota);

    std::string _path;
    DiskStats _disk_stats;
    DiskOptions _disk_opts;

    // The datacache can be configured to have multiple data directories on the same physical disk.
    std::vector<DirSpace> _dir_spaces;
    std::shared_ptr<FileSystemWrapper> _fs = nullptr;
    int64_t _disk_free_period = 0;
    bool _disabled = false;
};

class DiskSpaceMonitor {
public:
    DiskSpaceMonitor(LocalDiskCacheEngine* cache);
    DiskSpaceMonitor(LocalDiskCacheEngine* cache, std::shared_ptr<DiskSpace::FileSystemWrapper> fs);
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
    LocalDiskCacheEngine* _cache = nullptr;
    std::shared_ptr<DiskSpace::FileSystemWrapper> _fs = nullptr;
};

} // namespace starrocks
