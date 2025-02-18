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

#include "cache/block_cache/cache_options.h"
#include "common/status.h"
#include "fs/fs.h"
#include "util/disk_info.h"

namespace starrocks {

class BlockCache;
class DiskSpaceMonitor {
public:
    struct DiskStats {
        int disk_id = 0;
        std::string path;
        size_t capacity_bytes = 0;
        size_t available_bytes = 0;
    };

    // Wrap a new class to make it easy controlled in unittest.
    class FileSystemWrapper {
    public:
        virtual StatusOr<SpaceInfo> space(const std::string& path) { return FileSystem::Default()->space(path); }

        virtual StatusOr<size_t> directory_size(const std::string& dir);

        virtual int disk_id(const std::string& path) { return DiskInfo::disk_id(path.c_str()); }

        virtual ~FileSystemWrapper() {}
    };

    DiskSpaceMonitor(BlockCache* cache);
    ~DiskSpaceMonitor();

    void start();

    void stop();

    bool is_stopped();

    bool adjust_spaces(std::vector<DirSpace>* dir_spaces);

    Status check_spaces();

    Status adjust_cache_quota(const std::vector<DirSpace>& dir_spaces);

    // The align unit when adjusting cache disk quota. We set it to 10G to keep consistent with underlying cache file size,
    // which can help reduce processing the specail tail files.
    const static size_t QUOTA_ALIGN_UNIT;
    // The cache usage threshold for automatically increase disk quota, if the cache usage is under this level,
    // the disk expansion will be skipped.
    // NOTICE: When users update the disk quota manually, this threshold will be ignored.
    const static int64_t AUTO_INCREASE_THRESHOLD;

private:
    void _update_disk_stats();
    void _update_cache_stats();
    void _reset_spaces();
    void _init_spaces_by_cache_dir();
    void _update_spaces_by_cache_usage();
    bool _adjust_spaces_by_disk_usage(bool immediate);

    int64_t _max_disk_used_rate();

    void _adjust_datacache_callback();

    void _reset() {
        _dir_spaces.clear();
        _disk_stats.clear();
        _disk_to_dirs.clear();
    }

    std::vector<DirSpace> _dir_spaces;
    // <disk_id, DiskStats>
    std::unordered_map<int, DiskStats> _disk_stats;

    // The datacache can be configured to have multiple data directories on the same physical disk.
    // <disk_id, dir_space_index_list>
    std::unordered_map<int, std::vector<uint32_t>> _disk_to_dirs;
    // Max directory count in one disk
    size_t _max_disk_dirs = 0;
    // Minimum directory count in one disk
    size_t _min_disk_dirs = 0;

    size_t _total_cache_usage = 0;
    size_t _total_cache_quota = 0;
    int64_t _disk_free_period = 0;

    std::atomic<bool> _stopped = true;
    std::atomic<bool> _adjusting = false;
    std::thread _adjust_datacache_thread;
    std::mutex _mutex;

    std::unique_ptr<FileSystemWrapper> _fs = nullptr;
    BlockCache* _cache = nullptr;
};

} // namespace starrocks
