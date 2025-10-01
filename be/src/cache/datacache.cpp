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

#include "cache/datacache.h"

#include "cache/datacache_utils.h"
#include "cache/disk_space_monitor.h"
#include "cache/lrucache_engine.h"
#include "cache/mem_space_monitor.h"
#include "cache/object_cache/page_cache.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "runtime/exec_env.h"
#include "storage/options.h"
#include "util/parse_util.h"

#ifdef WITH_STARCACHE
#include "cache/peer_cache_engine.h"
#include "cache/starcache_engine.h"
#endif

namespace starrocks {

DataCache* DataCache::GetInstance() {
    static DataCache s_cache_env;
    return &s_cache_env;
}

Status DataCache::init(const std::vector<StorePath>& store_paths) {
    _global_env = GlobalEnv::GetInstance();
    _store_paths = store_paths;
    _block_cache = std::make_shared<BlockCache>();
    _page_cache = std::make_shared<StoragePageCache>();

    if (!config::datacache_enable) {
        config::disable_storage_page_cache = true;
        config::block_cache_enable = false;
        return Status::OK();
    }

    ASSIGN_OR_RETURN(auto mem_cache_options, _init_mem_cache_options());

#if defined(WITH_STARCACHE)
    ASSIGN_OR_RETURN(auto disk_cache_options, _init_disk_cache_options());
    RETURN_IF_ERROR(_init_starcache_engine(&disk_cache_options));

    auto remote_cache_options = _init_remote_cache_options();
    RETURN_IF_ERROR(_init_peer_cache(remote_cache_options));

    if (config::block_cache_enable) {
        auto block_cache_options = _init_block_cache_options();
        RETURN_IF_ERROR(_block_cache->init(block_cache_options, _local_disk_cache, _remote_cache));
    }
#endif

    RETURN_IF_ERROR(_init_lrucache_engine(mem_cache_options));

    RETURN_IF_ERROR(_init_page_cache());

    _mem_space_monitor = std::make_shared<MemSpaceMonitor>(this);
    _mem_space_monitor->start();

    return Status::OK();
}

void DataCache::destroy() {
    if (_disk_space_monitor != nullptr) {
        _disk_space_monitor->stop();
        _disk_space_monitor.reset();
        LOG(INFO) << "disk space monitor stop successfully";
    }
    if (_mem_space_monitor != nullptr) {
        _mem_space_monitor->stop();
        _mem_space_monitor.reset();
        LOG(INFO) << "mem space monitor stop successfully";
    }

    _page_cache.reset();
    LOG(INFO) << "pagecache shutdown successfully";

    _block_cache.reset();
    _local_mem_cache.reset();
    _local_disk_cache.reset();
    _remote_cache.reset();
    LOG(INFO) << "datacache shutdown successfully";
}

bool DataCache::adjust_mem_capacity(int64_t delta, size_t min_capacity) {
    if (_local_mem_cache != nullptr) {
        Status st = _local_mem_cache->adjust_mem_quota(delta, min_capacity);
        if (st.ok()) {
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

size_t DataCache::get_mem_capacity() const {
    if (_local_mem_cache != nullptr) {
        return _local_mem_cache->mem_quota();
    } else {
        return 0;
    }
}

Status DataCache::_init_lrucache_engine(const MemCacheOptions& cache_options) {
    _local_mem_cache = std::make_shared<LRUCacheEngine>();
    RETURN_IF_ERROR(reinterpret_cast<LRUCacheEngine*>(_local_mem_cache.get())->init(cache_options));
    LOG(INFO) << "lrucache engine init successfully";
    return Status::OK();
}

Status DataCache::_init_page_cache() {
    _page_cache->init(_local_mem_cache.get());
    _page_cache->init_metrics();
    LOG(INFO) << "storage page cache init successfully";
    return Status::OK();
}

#if defined(WITH_STARCACHE)
Status DataCache::_init_starcache_engine(DiskCacheOptions* cache_options) {
    // init starcache & disk monitor
    // TODO: DiskSpaceMonitor needs to be decoupled from StarCacheEngine.
    _local_disk_cache = std::make_shared<StarCacheEngine>();
    _disk_space_monitor = std::make_shared<DiskSpaceMonitor>(_local_disk_cache.get());
    RETURN_IF_ERROR(_disk_space_monitor->init(&cache_options->dir_spaces));
    RETURN_IF_ERROR(reinterpret_cast<StarCacheEngine*>(_local_disk_cache.get())->init(*cache_options));
    _disk_space_monitor->start();
    return Status::OK();
}

Status DataCache::_init_peer_cache(const RemoteCacheOptions& cache_options) {
    _remote_cache = std::make_shared<PeerCacheEngine>();
    return _remote_cache->init(cache_options);
}
#endif

RemoteCacheOptions DataCache::_init_remote_cache_options() {
    RemoteCacheOptions cache_options{.skip_read_factor = config::datacache_skip_read_factor};
    return cache_options;
}

StatusOr<MemCacheOptions> DataCache::_init_mem_cache_options() {
    MemCacheOptions cache_options;
    RETURN_IF_ERROR(DataCacheUtils::parse_conf_datacache_mem_size(
            config::datacache_mem_size, _global_env->process_mem_limit(), &cache_options.mem_space_size));
    return cache_options;
}

BlockCacheOptions DataCache::_init_block_cache_options() {
    BlockCacheOptions cache_options;
    cache_options.block_size = config::datacache_block_size;
    return cache_options;
}

#if defined(WITH_STARCACHE)
StatusOr<DiskCacheOptions> DataCache::_init_disk_cache_options() {
    DiskCacheOptions cache_options;

#ifdef USE_STAROS
    std::vector<string> corresponding_starlet_dirs;
    if (config::datacache_unified_instance_enable && !config::starlet_cache_dir.empty()) {
        // in older versions, users might set `starlet_cache_dir` instead of `storage_root_path` for starlet cache,
        // we need to move starlet cache into storage_root_path/datacache
        auto s = DataCacheUtils::get_corresponding_starlet_cache_dir(_store_paths, config::starlet_cache_dir);
        if (!s.ok()) {
            LOG(WARNING) << s.status().message() << ", change config::datacache_unified_instance_enable to false";
            config::datacache_unified_instance_enable = false;
        } else {
            corresponding_starlet_dirs = *s;
        }
    }
    int idx = 0;
#endif

    for (auto& root_path : _store_paths) {
        // Because we have unified the datacache between datalake and starlet, we also need to unify the
        // cache path and quota.
        // To reuse the old cache data in `starlet_cache` directory, we try to rename it to the new `datacache`
        // directory if it exists. To avoid the risk of cross disk renaming of a large amount of cached data,
        // we do not automatically rename it when the source and destination directories are on different disks.
        // In this case, users should manually remount the directories and restart them.
        std::string datacache_path = root_path.path + "/datacache";
#ifdef USE_STAROS
        if (config::datacache_unified_instance_enable) {
            std::string starlet_cache_path;
            if (idx < corresponding_starlet_dirs.size()) {
                starlet_cache_path = corresponding_starlet_dirs[idx++];
            } else {
                starlet_cache_path = root_path.path + "/starlet_cache/star_cache";
            }
            RETURN_IF_ERROR(DataCacheUtils::change_disk_path(starlet_cache_path, datacache_path));
        }
#endif
        // Create it if not exist
        Status st = FileSystem::Default()->create_dir_if_missing(datacache_path);
        if (!st.ok()) {
            LOG(ERROR) << "Fail to create datacache directory: " << datacache_path << ", reason: " << st.message();
            return Status::InternalError("Fail to create datacache directory");
        }

        ASSIGN_OR_RETURN(int64_t disk_size, DataCacheUtils::parse_conf_datacache_disk_size(
                                                    datacache_path, config::datacache_disk_size, -1));
#ifdef USE_STAROS
        // If the `datacache_disk_size` is manually set a positive value, we will use the maximum cache quota between
        // dataleke and starlet cache as the quota of the unified cache. Otherwise, the cache quota will remain zero
        // and then automatically adjusted based on the current avalible disk space.
        if (config::datacache_unified_instance_enable &&
            (!config::enable_datacache_disk_auto_adjust || disk_size > 0)) {
            ASSIGN_OR_RETURN(
                    int64_t starlet_cache_size,
                    DataCacheUtils::parse_conf_datacache_disk_size(
                            datacache_path, fmt::format("{}%", config::starlet_star_cache_disk_size_percent), -1));
            disk_size = std::max(disk_size, starlet_cache_size);
        }
#endif
        cache_options.dir_spaces.push_back({.path = datacache_path, .size = static_cast<size_t>(disk_size)});
    }

    if (cache_options.dir_spaces.empty()) {
        config::enable_datacache_disk_auto_adjust = false;
    }

    cache_options.block_size = config::datacache_block_size;
    cache_options.max_flying_memory_mb = config::datacache_max_flying_memory_mb;
    cache_options.max_concurrent_inserts = config::datacache_max_concurrent_inserts;
    cache_options.enable_checksum = config::datacache_checksum_enable;
    cache_options.enable_direct_io = config::datacache_direct_io_enable;
    cache_options.enable_tiered_cache = config::datacache_tiered_cache_enable;
    cache_options.skip_read_factor = config::datacache_skip_read_factor;
    cache_options.scheduler_threads_per_cpu = config::datacache_scheduler_threads_per_cpu;
    cache_options.enable_datacache_persistence = config::datacache_persistence_enable;
    cache_options.inline_item_count_limit = config::datacache_inline_item_count_limit;
    cache_options.eviction_policy = config::datacache_eviction_policy;

    return cache_options;
}
#endif

static bool parse_resource_str(const string& str, string* value) {
    if (!str.empty()) {
        std::string tmp_str = str;
        StripLeadingWhiteSpace(&tmp_str);
        StripTrailingWhitespace(&tmp_str);
        if (tmp_str.empty()) {
            return false;
        } else {
            *value = tmp_str;
            std::transform(value->begin(), value->end(), value->begin(), [](char c) { return std::tolower(c); });
            return true;
        }
    } else {
        return false;
    }
}

void DataCache::try_release_resource_before_core_dump() {
    std::set<std::string> modules;
    bool release_all = false;
    if (config::try_release_resource_before_core_dump.value() == "*") {
        release_all = true;
    } else {
        SplitStringAndParseToContainer(StringPiece(config::try_release_resource_before_core_dump), ",",
                                       &parse_resource_str, &modules);
    }

    auto need_release = [&release_all, &modules](const std::string& name) {
        return release_all || modules.contains(name);
    };

    if (_local_mem_cache != nullptr && need_release("data_cache")) {
        (void)_local_mem_cache->update_mem_quota(0, false);
    }
}

StatusOr<int64_t> DataCache::get_datacache_limit() {
    return ParseUtil::parse_mem_spec(config::datacache_mem_size.value(), _global_env->process_mem_limit());
}

bool DataCache::page_cache_available() const {
    return !config::disable_storage_page_cache && _page_cache != nullptr && _page_cache->get_capacity() > 0;
}

int64_t DataCache::check_datacache_limit(int64_t datacache_limit) {
    if (datacache_limit > _global_env->process_mem_limit()) {
        LOG(WARNING) << "BE Config datacache_mem_size is greater process memory limit, config="
                     << config::datacache_mem_size.value() << ", memory=" << _global_env->process_mem_limit();
        datacache_limit = _global_env->process_mem_limit();
    }
    if (datacache_limit < kcacheMinSize) {
        LOG(WARNING) << "Data cache limit is too small, use default size.";
        datacache_limit = kcacheMinSize;
    }
    return datacache_limit;
}

} // namespace starrocks
