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

#include "cache/block_cache/disk_space_monitor.h"

#include "cache/block_cache/block_cache.h"
#include "common/config.h"
#include "util/await.h"
#include "util/thread.h"

namespace starrocks {

#ifndef BE_TEST
const size_t DiskSpaceMonitor::QUOTA_ALIGN_UNIT = 10uL * 1024 * 1024 * 1024;
#else
const size_t DiskSpaceMonitor::QUOTA_ALIGN_UNIT = 10uL * 1024 * 1024;
#endif

const int64_t DiskSpaceMonitor::AUTO_INCREASE_THRESHOLD = 90;

StatusOr<size_t> DiskSpaceMonitor::FileSystemWrapper::directory_size(const std::string& dir) {
    size_t capacity = 0;
    auto st = FileSystem::Default()->iterate_dir2(dir, [&](DirEntry entry) {
        capacity += entry.size.value();
        return true;
    });
    RETURN_IF_ERROR(st);
    return capacity;
}

DiskSpaceMonitor::DiskSpaceMonitor(BlockCache* cache) : _fs(std::make_unique<FileSystemWrapper>()), _cache(cache) {}

DiskSpaceMonitor::~DiskSpaceMonitor() {
    stop();
}

void DiskSpaceMonitor::start() {
    std::unique_lock<std::mutex> lck(_mutex);
    if (!_stopped.load(std::memory_order_acquire)) {
        return;
    }
    if (_dir_spaces.empty()) {
        return;
    }
    _stopped.store(false, std::memory_order_release);
    _adjust_datacache_thread = std::thread([this] { _adjust_datacache_callback(); });
    Thread::set_thread_name(_adjust_datacache_thread, "adjust_datacache");
}

void DiskSpaceMonitor::stop() {
    if (_stopped.load(std::memory_order_acquire)) {
        return;
    }
    _stopped.store(true, std::memory_order_release);
    if (_adjust_datacache_thread.joinable()) {
        _adjust_datacache_thread.join();
    }
}

bool DiskSpaceMonitor::is_stopped() {
    return _stopped.load(std::memory_order_acquire);
}

void DiskSpaceMonitor::_adjust_datacache_callback() {
    while (!is_stopped()) {
        std::unique_lock<std::mutex> lck(_mutex);
        if (config::datacache_enable && config::datacache_auto_adjust_enable &&
            !_adjusting.load(std::memory_order_acquire)) {
            _update_disk_stats();
            _update_cache_stats();
            if (_adjust_spaces_by_disk_usage(false)) {
                Status st = adjust_cache_quota(_dir_spaces);
                LOG_IF(WARNING, !st.ok()) << "fail to adjust datacache disk quota, reason: " << st.message();
            }
        }
        lck.unlock();

        static const int64_t kWaitTimeout = config::datacache_disk_adjust_interval_seconds * 1000 * 1000;
        static const int64_t kCheckInterval = 1000 * 1000;
        auto cond = [this]() { return is_stopped(); };
        auto ret = Awaitility().timeout(kWaitTimeout).interval(kCheckInterval).until(cond);
        if (ret) {
            break;
        }
    }
}

bool DiskSpaceMonitor::adjust_spaces(std::vector<DirSpace>* dir_spaces) {
    std::unique_lock<std::mutex> lck(_mutex);
    _reset();
    if (dir_spaces->empty()) {
        return false;
    }

    _dir_spaces = *dir_spaces;
    _min_disk_dirs = _dir_spaces.size();
    for (size_t dir_index = 0; dir_index < _dir_spaces.size(); ++dir_index) {
        auto& dir_space = _dir_spaces[dir_index];
        int disk_id = _fs->disk_id(dir_space.path);
        if (_disk_stats.find(disk_id) == _disk_stats.end()) {
            DiskStats disk;
            disk.disk_id = disk_id;
            disk.path = dir_space.path;
            _disk_stats[disk_id] = disk;
        }
        _disk_to_dirs[disk_id].push_back(dir_index);
    }
    for (const auto& pair : _disk_to_dirs) {
        const auto& dirs = pair.second;
        _max_disk_dirs = std::max(_max_disk_dirs, dirs.size());
        _min_disk_dirs = std::min(_min_disk_dirs, dirs.size());
    }
    _update_disk_stats();

    // We check this switch after some infomation are initialized, such as `_disk_stats`, because
    // even if it is off now, we still need these infomation once the switch is turn on online.
    if (!config::datacache_auto_adjust_enable) {
        return false;
    }

    _init_spaces_by_cache_dir();
    if (_adjust_spaces_by_disk_usage(true)) {
        *dir_spaces = _dir_spaces;
        return true;
    }
    return false;
}

void DiskSpaceMonitor::_update_disk_stats() {
    for (auto& pair : _disk_stats) {
        auto& disk = pair.second;
        std::string path = disk.path;
        auto ret = _fs->space(path);
        if (!ret.ok()) {
            LOG(WARNING) << ret.status().message();
            continue;
        }
        auto& space_info = ret.value();
        disk.capacity_bytes = space_info.capacity;
        disk.available_bytes = space_info.available;
        VLOG(2) << "Get disk statistics, capaticy: " << disk.capacity_bytes << ", available: " << disk.available_bytes
                << ", used_rate: " << (disk.capacity_bytes - disk.available_bytes) * 100 / disk.capacity_bytes << "%";
    }
}

int64_t DiskSpaceMonitor::_max_disk_used_rate() {
    int64_t max_used_rate = 0;
    for (const auto& pair : _disk_to_dirs) {
        const auto& disk_id = pair.first;
        const auto& disk = _disk_stats[disk_id];
        int64_t used_rate = (disk.capacity_bytes - disk.available_bytes) * 100 / disk.capacity_bytes;
        if (used_rate > max_used_rate) {
            max_used_rate = used_rate;
        }
    }
    return max_used_rate;
}

void DiskSpaceMonitor::_init_spaces_by_cache_dir() {
    _total_cache_usage = 0;
    for (auto& dir_space : _dir_spaces) {
        auto ret = _fs->directory_size(dir_space.path);
        if (ret.ok() && ret.value() > 0) {
            int disk_id = _fs->disk_id(dir_space.path);
            auto& disk = _disk_stats[disk_id];
            // The space under datacache directories can be reused, so ignore their usage.
            disk.available_bytes += ret.value();
            if (disk.available_bytes > disk.capacity_bytes) {
                disk.available_bytes = disk.capacity_bytes;
            }
        }
        _total_cache_quota += dir_space.size;
    }
}

void DiskSpaceMonitor::_update_spaces_by_cache_usage() {
    if (_total_cache_quota == 0) {
        return;
    }
    double cache_used_rate = static_cast<double>(_total_cache_usage) / _total_cache_quota;
    for (auto& dir_space : _dir_spaces) {
        dir_space.size = dir_space.size * cache_used_rate;
    }
}

void DiskSpaceMonitor::_update_cache_stats() {
    const auto metrics = _cache->cache_metrics();
    _total_cache_usage = metrics.disk_used_bytes;
    _total_cache_quota = metrics.disk_quota_bytes;
}

bool DiskSpaceMonitor::_adjust_spaces_by_disk_usage(bool immediate) {
    bool shrink = false;
    int64_t max_disk_used_rate = _max_disk_used_rate();
    if (max_disk_used_rate > config::datacache_disk_high_level) {
        shrink = true;
    } else if (max_disk_used_rate < config::datacache_disk_low_level) {
        _disk_free_period += config::datacache_disk_adjust_interval_seconds;
        if (!immediate && _disk_free_period < config::datacache_disk_idle_seconds_for_expansion) {
            return false;
        }
        if (_total_cache_quota > 0) {
            double cache_used_rate = 100.0 * _total_cache_usage / _total_cache_quota;
            if (cache_used_rate < AUTO_INCREASE_THRESHOLD) {
                return false;
            }
        }
    } else {
        return false;
    }

    int64_t delta_rate = 0;
    if (!shrink) {
        // Increase cache quota
        delta_rate = (config::datacache_disk_safe_level - max_disk_used_rate) / _max_disk_dirs;
        DCHECK_GT(delta_rate, 0);
    } else {
        // Decrease cache quota
        delta_rate = (config::datacache_disk_safe_level - max_disk_used_rate) / _min_disk_dirs;
        DCHECK_LT(delta_rate, 0);
    }
    _update_spaces_by_cache_usage();

    int64_t total_cache_quota = 0;
    for (const auto& pair : _disk_to_dirs) {
        const auto& disk_id = pair.first;
        const auto& dirs = pair.second;
        const auto& disk = _disk_stats[disk_id];
        int64_t delta_quota = disk.capacity_bytes / 100 * delta_rate;
        size_t disk_cache_quota = 0;
        for (uint32_t dir_index : dirs) {
            auto& dir_space = _dir_spaces[dir_index];
            dir_space.size += delta_quota;
            dir_space.size = dir_space.size / QUOTA_ALIGN_UNIT * QUOTA_ALIGN_UNIT;
            disk_cache_quota += dir_space.size;
        }
        total_cache_quota += disk_cache_quota;
    }

    _disk_free_period = 0;
    if (total_cache_quota < config::datacache_min_disk_quota_for_adjustment) {
        // If the current available disk space is too small, cache quota will be reset to zero to avoid overly frequent
        // population and eviction.
        _reset_spaces();
        if (_total_cache_quota == 0) {
            // If the cache quata is already zero, skip adjusting it repeatedly.
            VLOG(1) << "Skip updating the cache quota because the target quota is less than "
                    << "`datacache_min_disk_quota_for_adjustment`, target cache quota: " << total_cache_quota;
            total_cache_quota = 0;
            return false;
        } else {
            // This warning log only be printed when the cache disk quota is adjust from a non-zero integer to zero.
            LOG(WARNING) << "The current available disk space is too small, so disable the disk cache directly. If you "
                         << "still need it, you could reduce the value of `datacache_min_disk_quota_for_adjustment`";
            total_cache_quota = 0;
        }
    }
    LOG(INFO) << "Adjusting datacache disk quota from " << _total_cache_quota << " to " << total_cache_quota;
    return true;
}

void DiskSpaceMonitor::_reset_spaces() {
    for (auto& dir_space : _dir_spaces) {
        dir_space.size = 0;
    }
}

Status DiskSpaceMonitor::adjust_cache_quota(const std::vector<DirSpace>& dir_spaces) {
    _adjusting.store(true, std::memory_order_release);
    Status st = _cache->update_disk_spaces(dir_spaces);
    _adjusting.store(false, std::memory_order_release);
    return st;
}

} // namespace starrocks
