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

#if defined(WITH_STARCACHE)
    if (!config::datacache_enable) {
        config::block_cache_enable = false;
    }
#else
    config::datacache_enable = false;
    config::block_cache_enable = false;
#endif

    RETURN_IF_ERROR(_init_datacache());
    RETURN_IF_ERROR(_init_lrucache_engine());
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
    LOG(INFO) << "datacache shutdown successfully";
}

bool DataCache::adjust_mem_capacity(int64_t delta, size_t min_capacity) {
    if (_lru_cache != nullptr) {
        Status st = _lru_cache->update_mem_quota(delta, min_capacity);
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
    if (_lru_cache != nullptr) {
        return _lru_cache->mem_quota();
    } else {
        return 0;
    }
}

Status DataCache::_init_lrucache_engine() {
    ASSIGN_OR_RETURN(int64_t storage_cache_limit, get_storage_page_cache_limit());
    storage_cache_limit = check_storage_page_cache_limit(storage_cache_limit);

    CacheOptions cache_options{.mem_space_size = static_cast<size_t>(storage_cache_limit)};
    _lru_cache = std::make_shared<LRUCacheEngine>();
    RETURN_IF_ERROR(_lru_cache->init(cache_options));
    LOG(INFO) << "object cache init successfully";
    return Status::OK();
}

Status DataCache::_init_page_cache() {
    _page_cache = std::make_shared<StoragePageCache>(_lru_cache.get());
    _page_cache->init_metrics();
    LOG(INFO) << "storage page cache init successfully";
    return Status::OK();
}

Status DataCache::_init_datacache() {
    _block_cache = std::make_shared<BlockCache>();

    if (config::datacache_enable) {
#if defined(WITH_STARCACHE)
        ASSIGN_OR_RETURN(auto cache_options, _init_cache_options());

        // init starcache & disk monitor
        _local_cache = std::make_shared<StarCacheEngine>();
        _disk_space_monitor = std::make_shared<DiskSpaceMonitor>(_local_cache.get());
        RETURN_IF_ERROR(_disk_space_monitor->init(&cache_options.dir_spaces));
        RETURN_IF_ERROR(_local_cache->init(cache_options));
        _disk_space_monitor->start();

        // init remote cache
        _remote_cache = std::make_shared<PeerCacheEngine>();
        RETURN_IF_ERROR(_remote_cache->init(cache_options));

        if (config::block_cache_enable) {
            RETURN_IF_ERROR(_block_cache->init(cache_options, _local_cache, _remote_cache));
        }

        LOG(INFO) << "datacache init successfully";
#endif
    } else {
        LOG(INFO) << "starts by skipping the datacache initialization";
    }
    return Status::OK();
}

StatusOr<CacheOptions> DataCache::_init_cache_options() {
    CacheOptions cache_options;
    RETURN_IF_ERROR(DataCacheUtils::parse_conf_datacache_mem_size(
            config::datacache_mem_size, _global_env->process_mem_limit(), &cache_options.mem_space_size));

    for (auto& root_path : _store_paths) {
        // Because we have unified the datacache between datalake and starlet, we also need to unify the
        // cache path and quota.
        // To reuse the old cache data in `starlet_cache` directory, we try to rename it to the new `datacache`
        // directory if it exists. To avoid the risk of cross disk renaming of a large amount of cached data,
        // we do not automatically rename it when the source and destination directories are on different disks.
        // In this case, users should manually remount the directories and restart them.
        std::string datacache_path = root_path.path + "/datacache";
        std::string starlet_cache_path = root_path.path + "/starlet_cache/star_cache";
#ifdef USE_STAROS
        if (config::datacache_unified_instance_enable) {
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

    // Adjust the default engine based on build switches.
    if (config::datacache_engine == "") {
#if defined(WITH_STARCACHE)
        config::datacache_engine = "starcache";
#endif
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
    cache_options.engine = config::datacache_engine;
    cache_options.eviction_policy = config::datacache_eviction_policy;

    return cache_options;
}

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

    if (_page_cache != nullptr && need_release("data_cache")) {
        _page_cache->set_capacity(0);
    }
    if (_local_cache != nullptr && _local_cache->available() && need_release("data_cache")) {
        // TODO: Currently, block cache don't support shutdown now,
        //  so here will temporary use update_mem_quota instead to release memory.
        (void)_local_cache->update_mem_quota(0, false);
    }
}

StatusOr<int64_t> DataCache::get_storage_page_cache_limit() {
    return ParseUtil::parse_mem_spec(config::storage_page_cache_limit.value(), _global_env->process_mem_limit());
}

bool DataCache::page_cache_available() const {
    return !config::disable_storage_page_cache && _page_cache != nullptr && _page_cache->get_capacity() > 0;
}

int64_t DataCache::check_storage_page_cache_limit(int64_t storage_cache_limit) {
    if (storage_cache_limit > _global_env->process_mem_limit()) {
        LOG(WARNING) << "Config storage_page_cache_limit is greater process memory limit, config="
                     << config::storage_page_cache_limit.value() << ", memory=" << _global_env->process_mem_limit();
        storage_cache_limit = _global_env->process_mem_limit();
    }
    if (!config::disable_storage_page_cache) {
        if (storage_cache_limit < kcacheMinSize) {
            LOG(WARNING) << "Storage cache limit is too small, use default size.";
            storage_cache_limit = kcacheMinSize;
        }
        LOG(INFO) << "Set storage page cache size " << storage_cache_limit;
    }
    return storage_cache_limit;
}

} // namespace starrocks