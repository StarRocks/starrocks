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

#include "cache/block_cache/starcache_wrapper.h"

#include <filesystem>

#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status StarCacheWrapper::init(const CacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.disk_spaces) {
        opt.disk_dir_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    opt.block_size = options.block_size;
    opt.enable_disk_checksum = options.enable_checksum;
    opt.max_concurrent_writes = options.max_concurrent_inserts;
    opt.enable_os_page_cache = !options.enable_direct_io;
    opt.scheduler_thread_ratio_per_cpu = options.scheduler_threads_per_cpu;
    opt.max_flying_memory_mb = options.max_flying_memory_mb;
    opt.inline_cache_count_limit = options.inline_item_count_limit;
    opt.alloc_mem_threshold = 100;
    opt.evict_touch_mem_probalility = 10;
    opt.evict_touch_disk_probalility = 10;
    _cache_adaptor.reset(starcache::create_default_adaptor(options.skip_read_factor));
    opt.cache_adaptor = _cache_adaptor.get();
    if (options.enable_datacache_persistence) {
        opt.durability_type = starcache::DurabilityType::ROCKSDB;
    }
    opt.instance_name = "default_cache";

    if (options.eviction_policy == "slru") {
        opt.lru_segment_ratios = {35, 65};
    }
    opt.lru_segment_freq_bits = 0;

    _enable_tiered_cache = options.enable_tiered_cache;
    _enable_datacache_persistence = options.enable_datacache_persistence;
    _cache = std::make_unique<starcache::StarCache>();
    return to_status(_cache->init(opt));
}

Status StarCacheWrapper::write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) {
    if (!options) {
        return to_status(_cache->set(key, buffer.const_raw_buf(), nullptr));
    }

    starcache::WriteOptions opts;
    opts.priority = options->priority;
    opts.ttl_seconds = options->ttl_seconds;
    opts.overwrite = options->overwrite;
    opts.async = options->async;
    opts.keep_alive = options->allow_zero_copy;
    opts.callback = options->callback;
    if (!_enable_datacache_persistence && _enable_tiered_cache) {
        opts.mode = starcache::WriteOptions::WriteMode::WRITE_BACK;
    } else {
        opts.mode = starcache::WriteOptions::WriteMode::WRITE_THROUGH;
    }
    opts.evict_probability = options->evict_probability;
    Status st;
    {
        // The memory when writing starcache is no longer recorded to the query memory.
        // Because we free the memory in other threads in starcache library, which is hard to track.
        // It is safe because we limit the flying memory in starcache, also, this behavior
        // doesn't affect the process memory tracker.
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
        st = to_status(_cache->set(key, buffer.const_raw_buf(), &opts));
    }
    if (st.ok() && !opts.async) {
        options->stats.write_mem_bytes = opts.stats.write_mem_bytes;
        options->stats.write_disk_bytes = opts.stats.write_disk_bytes;
    }
    return st;
}

Status StarCacheWrapper::read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                              ReadCacheOptions* options) {
    if (!options) {
        return to_status(_cache->read(key, off, size, &buffer->raw_buf(), nullptr));
    }
    starcache::ReadOptions opts;
    opts.use_adaptor = options->use_adaptor;
    opts.mode = _enable_tiered_cache ? starcache::ReadOptions::ReadMode::READ_BACK
                                     : starcache::ReadOptions::ReadMode::READ_THROUGH;
    auto st = to_status(_cache->read(key, off, size, &buffer->raw_buf(), &opts));
    if (st.ok()) {
        options->stats.read_mem_bytes = opts.stats.read_mem_bytes;
        options->stats.read_disk_bytes = opts.stats.read_disk_bytes;
    }
    return st;
}

bool StarCacheWrapper::exist(const std::string& key) const {
    return _cache->exist(key);
}

Status StarCacheWrapper::remove(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

Status StarCacheWrapper::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    return to_status(_cache->update_mem_quota(quota_bytes, flush_to_disk));
}

Status StarCacheWrapper::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    std::vector<starcache::DirSpace> disk_spaces;
    disk_spaces.reserve(spaces.size());
    for (auto& dir : spaces) {
        disk_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    return to_status(_cache->update_disk_spaces(disk_spaces));
}

const DataCacheMetrics StarCacheWrapper::cache_metrics(int level) {
    auto metrics = _cache->metrics(level);
    // Now the EEXIST is treated as an failed status in starcache, which will cause the write_fail_count too large
    // in many cases because we write cache with `overwrite=false` now. It makes users confused.
    // As real writing failure rarely occursso currently, so we temporarily adjust it here.
    // Once the starcache library is update, the following lines will be removed.
    if (metrics.detail_l2) {
        metrics.detail_l2->write_success_count += metrics.detail_l2->write_fail_count;
        metrics.detail_l2->write_fail_count = 0;
    }
    return metrics;
}

void StarCacheWrapper::record_read_remote(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_remote(size, lateny_us);
    }
}

void StarCacheWrapper::record_read_cache(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_cache(size, lateny_us);
    }
}

Status StarCacheWrapper::shutdown() {
    // TODO: starcache implement shutdown to release memory
    return Status::OK();
}

} // namespace starrocks
