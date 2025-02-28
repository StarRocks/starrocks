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
const size_t DiskSpace::kQuotaAlignUnit = 10uL * 1024 * 1024 * 1024;
#else
const size_t DiskSpace::kQuotaAlignUnit = 10uL * 1024 * 1024;
#endif

const double DiskSpace::kAutoIncreaseThreshold = 0.9;

Status DiskSpace::init_spaces(const std::vector<DirSpace>& dir_spaces) {
    Status st = _update_disk_stats();
    if (!st.ok()) {
        LOG(ERROR) << "fail to init disk space, reason: " << st.message();
        return st;
    }
    _dir_spaces = dir_spaces;

    // Revise the original disk space state.
    // The disk space occupied by old datacache files should be excluded because these space can
    // be reused by datacache .
    _revise_disk_stats_by_cache_dir();

    // We check this switch after some infomation are initialized, because even if it is off now,
    // we still need these infomation once the switch is turn on online.
    if (!config::datacache_auto_adjust_enable) {
        return st;
    }

    double delta_rate = config::datacache_disk_safe_level * 0.01 - _disk_stats.used_rate();
    size_t cache_avail_bytes = 0;
    if (delta_rate > 0) {
        int64_t delta_size = _disk_stats.capacity_bytes * delta_rate;
        cache_avail_bytes = _check_cache_low_limit(delta_size);
    }
    _update_spaces_by_cache_quota(cache_avail_bytes);
    return st;
}

bool DiskSpace::adjust_spaces(const AdjustContext& ctx) {
    Status st = _update_disk_stats();
    if (!st.ok()) {
        LOG(ERROR) << "fail to check and adjust cache disk spaces, reason: " << st.message();
        return false;
    }

    double used_rate = _disk_stats.used_rate();
    int64_t cur_level = static_cast<int64_t>(used_rate * 100);
    if (cur_level < config::datacache_disk_low_level) {
        _disk_free_period += config::datacache_disk_adjust_interval_seconds;
        if (!_allow_expansion(ctx)) {
            return false;
        }
    } else if (cur_level <= config::datacache_disk_high_level) {
        return false;
    }

    double delta_rate = config::datacache_disk_safe_level * 0.01 - used_rate;
    int64_t delta_quota = _disk_stats.capacity_bytes * delta_rate;
    // TODO: Support obtaining the cache usage of each directory in starcache, to make it more accurate.
    _update_spaces_by_cache_usage(ctx);

    int64_t old_cache_quota = total_cache_quota();
    int64_t new_cache_quota = old_cache_quota + delta_quota;

    new_cache_quota = _check_cache_low_limit(new_cache_quota);
    _update_spaces_by_cache_quota(new_cache_quota);
    _disk_free_period = 0;

    return new_cache_quota != old_cache_quota;
}

size_t DiskSpace::total_cache_quota() {
    size_t cache_quota = 0;
    for (auto& dir : _dir_spaces) {
        cache_quota += dir.size;
    }
    return cache_quota;
}

Status DiskSpace::_update_disk_stats() {
    auto ret = _fs->space(_path);
    if (!ret.ok()) {
        LOG(WARNING) << "fail to get disk space for path: " << _path << ", reason: " << ret.status().message();
        return ret.status();
    }
    auto& space_info = ret.value();
    _disk_stats.capacity_bytes = space_info.capacity;
    _disk_stats.available_bytes = space_info.available;
    VLOG(2) << "Get disk statistics, capaticy: " << _disk_stats.capacity_bytes
            << ", available: " << _disk_stats.available_bytes << ", used_rate: " << _disk_stats.used_rate();

    return Status::OK();
}

void DiskSpace::_revise_disk_stats_by_cache_dir() {
    for (auto& dir : _dir_spaces) {
        auto ret = _fs->directory_size(dir.path);
        if (ret.ok() && ret.value() > 0) {
            // The space under datacache directories can be reused, so ignore their usage.
            _disk_stats.available_bytes += ret.value();
            if (_disk_stats.available_bytes > _disk_stats.capacity_bytes) {
                _disk_stats.available_bytes = _disk_stats.capacity_bytes;
            }
        }
    }
}

void DiskSpace::_update_spaces_by_cache_quota(size_t cache_avail_bytes) {
    size_t avg_dir_size = cache_avail_bytes / _dir_spaces.size() / kQuotaAlignUnit * kQuotaAlignUnit;
    for (auto& dir : _dir_spaces) {
        dir.size = avg_dir_size;
    }
}

void DiskSpace::_update_spaces_by_cache_usage(const AdjustContext& ctx) {
    if (ctx.total_cache_quota > 0) {
        double cache_used_rate = static_cast<double>(ctx.total_cache_usage) / ctx.total_cache_quota;
        for (auto& dir : _dir_spaces) {
            dir.size = dir.size * cache_used_rate;
        }
    }
}

bool DiskSpace::_allow_expansion(const AdjustContext& ctx) {
    if (_disk_free_period < config::datacache_disk_idle_seconds_for_expansion) {
        return false;
    }
    if (ctx.total_cache_quota > 0) {
        double cache_used_rate = static_cast<double>(ctx.total_cache_usage / ctx.total_cache_quota);
        if (cache_used_rate < kAutoIncreaseThreshold) {
            return false;
        }
    }
    return true;
}

size_t DiskSpace::_check_cache_low_limit(int64_t cache_quota) {
    if (cache_quota < config::datacache_min_disk_quota_for_adjustment) {
        if (_disabled) {
            // If the cache quata is already disabled, skip adjusting it repeatedly.
            VLOG(1) << "Skip updating the disk cache quota because the target quota is less than"
                    << " `datacache_min_disk_quota_for_adjustment`, path: " << _path;
        } else {
            // This warning log only be printed when the cache disk quota is adjust from a non-zero integer to zero.
            LOG(WARNING) << "The current available disk space is too small, so disable the disk cache directly."
                         << " If you still need it, you could reduce the value of"
                         << " `datacache_min_disk_quota_for_adjustment`, path: " << _path;
            _disabled = true;
        }
        return 0;
    }
    _disabled = false;
    return cache_quota;
}

StatusOr<size_t> DiskSpace::FileSystemWrapper::directory_size(const std::string& dir) {
    size_t capacity = 0;
    auto st = FileSystem::Default()->iterate_dir2(dir, [&](DirEntry entry) {
        capacity += entry.size.value();
        return true;
    });
    RETURN_IF_ERROR(st);
    return capacity;
}

dev_t DiskSpace::FileSystemWrapper::device_id(const std::string& path) {
    struct stat s;
    if (stat(path.c_str(), &s) != 0) {
        return 0;
    }
    return s.st_dev;
}

DiskSpaceMonitor::DiskSpaceMonitor(BlockCache* cache)
        : _cache(cache), _fs(std::make_shared<DiskSpace::FileSystemWrapper>()) {}

DiskSpaceMonitor::~DiskSpaceMonitor() {
    stop();
}

Status DiskSpaceMonitor::init(std::vector<DirSpace>* dir_spaces) {
    if (dir_spaces->empty()) {
        return Status::OK();
    }

    std::map<dev_t, std::vector<DirSpace>> disk_to_dir_spaces;
    for (auto& dir : *dir_spaces) {
        dev_t device_id = _fs->device_id(dir.path);
        if (device_id > 0) {
            disk_to_dir_spaces[device_id].push_back(dir);
        } else {
            LOG(ERROR) << "fail to get device id for the path: " << dir.path;
            return Status::InvalidArgument("fail to get device id");
        }
    }

    _disk_spaces.clear();
    for (auto& disk2spaces : disk_to_dir_spaces) {
        auto& device_id = disk2spaces.first;
        auto& dirs = disk2spaces.second;
        _disk_spaces.emplace_back(device_id, dirs[0].path, _fs);
        auto& disk_space = _disk_spaces.back();
        RETURN_IF_ERROR(disk_space.init_spaces(dirs));
    }
    *dir_spaces = all_dir_spaces();

    return Status::OK();
}

void DiskSpaceMonitor::start() {
    std::unique_lock<std::mutex> lck(_mutex);
    if (!_stopped.load(std::memory_order_acquire)) {
        return;
    }
    if (_disk_spaces.empty()) {
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
            !_updating.load(std::memory_order_acquire)) {
            if (_adjust_spaces_by_disk_usage()) {
                auto dir_spaces = all_dir_spaces();
                Status st = _update_cache_quota(dir_spaces);
                if (st.ok()) {
                    LOG(INFO) << "success to adjust datacache disk spaces to: " << to_string(dir_spaces);
                } else {
                    LOG(WARNING) << "fail to adjust datacache disk spaces, reason: " << st.message();
                }
            }
        }
        lck.unlock();

        int64_t kWaitTimeout = config::datacache_disk_adjust_interval_seconds * 1000 * 1000;
        static const int64_t kCheckInterval = 1000 * 1000;
        auto cond = [this]() { return is_stopped(); };
        auto ret = Awaitility().timeout(kWaitTimeout).interval(kCheckInterval).until(cond);
        if (ret) {
            break;
        }
    }
}

bool DiskSpaceMonitor::_adjust_spaces_by_disk_usage() {
    _update_cache_stats();

    DiskSpace::AdjustContext ctx = {.total_cache_quota = _total_cache_quota, .total_cache_usage = _total_cache_usage};
    bool changed = false;
    for (auto& disk_space : _disk_spaces) {
        if (disk_space.adjust_spaces(ctx)) {
            changed = true;
        }
    }
    return changed;
}

std::vector<DirSpace> DiskSpaceMonitor::all_dir_spaces() {
    std::vector<DirSpace> result;
    for (auto& disk_space : _disk_spaces) {
        auto& dirs = disk_space.dir_spaces();
        result.insert(result.end(), dirs.begin(), dirs.end());
    }
    return result;
}

std::string DiskSpaceMonitor::to_string(const std::vector<DirSpace>& dir_spaces) {
    std::stringstream ss;
    ss << "[";
    for (size_t index = 0; index < dir_spaces.size(); ++index) {
        auto& dir = dir_spaces[index];
        ss << "{ path: " << dir.path << ", size: " << dir.size << " }";
        if (index + 1 < dir_spaces.size()) {
            ss << ", ";
        }
    }
    ss << "]";
    return ss.str();
}

void DiskSpaceMonitor::_update_cache_stats() {
    const auto metrics = _cache->cache_metrics();
    _total_cache_usage = metrics.disk_used_bytes;
    _total_cache_quota = metrics.disk_quota_bytes;
}

Status DiskSpaceMonitor::_update_cache_quota(const std::vector<DirSpace>& dir_spaces) {
    _updating.store(true, std::memory_order_release);
    Status st = _cache->update_disk_spaces(dir_spaces);
    _updating.store(false, std::memory_order_release);
    return st;
}

} // namespace starrocks
