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

#include "cache/disk_cache/starcache_engine.h"

#include <filesystem>

#include "cache/cache_metrics.h"
#include "cache/disk_space_monitor.h"
#include "cache/status.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status StarCacheEngine::init(const DiskCacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.dir_spaces) {
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

    _cache = std::make_shared<starcache::StarCache>();
    RETURN_IF_ERROR(to_status(_cache->init(opt)));

    _refresh_quota();
    _initialized.store(true, std::memory_order_relaxed);

    LOG(INFO) << "init starcache engine, block_size: " << options.block_size
              << ", disk_spaces: " << DiskSpaceMonitor::to_string(options.dir_spaces);
    return Status::OK();
}

Status StarCacheEngine::write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) {
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
    opts.mode = starcache::WriteOptions::WriteMode::WRITE_THROUGH;
    opts.evict_probability = 100;
    opts.ignore_inline = true;
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

Status StarCacheEngine::read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                             DiskCacheReadOptions* options) {
    if (!options) {
        return to_status(_cache->read(key, off, size, &buffer->raw_buf(), nullptr));
    }
    starcache::ReadOptions opts;
    opts.use_adaptor = options->use_adaptor;
    opts.mode = starcache::ReadOptions::ReadMode::READ_THROUGH;
    auto st = to_status(_cache->read(key, off, size, &buffer->raw_buf(), &opts));
    if (st.ok()) {
        options->stats.read_mem_bytes = opts.stats.read_mem_bytes;
        options->stats.read_disk_bytes = opts.stats.read_disk_bytes;
    }
    return st;
}

bool StarCacheEngine::exist(const std::string& key) const {
    return _cache->exist(key);
}

Status StarCacheEngine::remove(const std::string& key) {
    return to_status(_cache->remove(key));
}

Status StarCacheEngine::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    std::vector<starcache::DirSpace> disk_spaces;
    disk_spaces.reserve(spaces.size());
    for (auto& dir : spaces) {
        disk_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    Status st = to_status(_cache->update_disk_spaces(disk_spaces));
    _refresh_quota();
    return st;
}

Status StarCacheEngine::update_inline_cache_count_limit(int32_t limit) {
    return to_status(_cache->update_inline_cache_count_limit(limit));
}

const StarCacheMetrics StarCacheEngine::starcache_metrics(int level) const {
    return _cache->metrics(level);
}

const DataCacheMetrics StarCacheEngine::cache_metrics() const {
    auto starcache_metrics = _cache->metrics(0);
    DataCacheMetrics metrics = {.status = static_cast<DataCacheStatus>(starcache_metrics.status),
                                .mem_quota_bytes = starcache_metrics.mem_quota_bytes,
                                .mem_used_bytes = starcache_metrics.mem_used_bytes,
                                .disk_quota_bytes = starcache_metrics.disk_quota_bytes,
                                .disk_used_bytes = starcache_metrics.disk_used_bytes,
                                .meta_used_bytes = starcache_metrics.meta_used_bytes};
    return metrics;
}

void StarCacheEngine::record_read_remote(size_t size, int64_t latency_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_remote(size, latency_us);
    }
}

void StarCacheEngine::record_read_cache(size_t size, int64_t latency_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_cache(size, latency_us);
    }
}

Status StarCacheEngine::shutdown() {
    // TODO: starcache implement shutdown to release memory
    return Status::OK();
}

void StarCacheEngine::_refresh_quota() {
    auto metrics = starcache_metrics(0);
    _disk_quota.store(metrics.disk_quota_bytes, std::memory_order_relaxed);
}

void StarCacheEngine::disk_spaces(std::vector<DirSpace>* spaces) const {
    spaces->clear();
    auto metrics = starcache_metrics(0);
    for (auto& dir : metrics.disk_dir_spaces) {
        spaces->push_back({.path = dir.path, .size = dir.quota_bytes});
    }
}

size_t StarCacheEngine::lookup_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count + metrics.detail_l1->miss_count;
}

size_t StarCacheEngine::hit_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count;
}

Status StarCacheEngine::prune() {
    return to_status(_cache->update_mem_quota(0, false));
}

} // namespace starrocks
