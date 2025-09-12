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

#include "cache/starcache_engine.h"

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

    _enable_tiered_cache = options.enable_tiered_cache;
    _enable_datacache_persistence = options.enable_datacache_persistence;
    _cache = std::make_shared<starcache::StarCache>();
    RETURN_IF_ERROR(to_status(_cache->init(opt)));

    _refresh_quota();
    _initialized.store(true, std::memory_order_relaxed);

    LOG(INFO) << "init starcache engine, block_size: " << options.block_size
              << ", disk_spaces: " << DiskSpaceMonitor::to_string(options.dir_spaces);
    return Status::OK();
}

Status StarCacheEngine::write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) {
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

Status StarCacheEngine::insert(const std::string& key, void* value, size_t size, ObjectCacheDeleter deleter,
                               ObjectCacheHandlePtr* handle, const ObjectCacheWriteOptions& options) {
    starcache::ObjectHandle* obj_hdl = new starcache::ObjectHandle;
    auto obj_deleter = [deleter, key, value] {
        // For temporary compatibility with old deleters.
        CacheKey cache_key(key);
        deleter(cache_key, value);
    };
    starcache::WriteOptions opts;
    opts.priority = options.priority;
    opts.ttl_seconds = options.ttl_seconds;
    opts.overwrite = options.overwrite;
    opts.evict_probability = options.evict_probability;
    Status st = to_status(_cache->set_object(key, value, size, obj_deleter, obj_hdl, &opts));
    if (!st.ok()) {
        delete obj_hdl;
    } else if (handle) {
        // Try release the old handle before fill it with a new one.
        _try_release_obj_handle(*handle);
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(obj_hdl);
    }
    return st;
}

Status StarCacheEngine::lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) {
    starcache::ObjectHandle* obj_hdl = new starcache::ObjectHandle;
    // Skip checking options temporarily because there is no valid members in `ObjectCacheReadOptions` now.
    Status st = to_status(_cache->get_object(key, obj_hdl, nullptr));
    if (!st.ok()) {
        delete obj_hdl;
    } else if (handle) {
        _try_release_obj_handle(*handle);
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(obj_hdl);
    }
    return st;
}

void StarCacheEngine::release(ObjectCacheHandlePtr handle) {
    _try_release_obj_handle(handle);
}

const void* StarCacheEngine::value(ObjectCacheHandlePtr handle) {
    auto obj_hdl = reinterpret_cast<starcache::ObjectHandle*>(handle);
    return obj_hdl->ptr();
}

bool StarCacheEngine::exist(const std::string& key) const {
    return _cache->exist(key);
}

Status StarCacheEngine::remove(const std::string& key) {
    return to_status(_cache->remove(key));
}

Status StarCacheEngine::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    Status st = to_status(_cache->update_mem_quota(quota_bytes, flush_to_disk));
    _refresh_quota();
    return st;
}

Status StarCacheEngine::adjust_mem_quota(int64_t delta, size_t min_capacity) {
    auto starcache_metrics = _cache->metrics();
    size_t capacity = starcache_metrics.mem_quota_bytes;
    int64_t new_capacity = capacity + delta;
    if (new_capacity < (int64_t)min_capacity) {
        return Status::InvalidArgument("target capacity is less than the minimum capacity");
    }
    return to_status(_cache->update_mem_quota(new_capacity, false));
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
    _mem_quota.store(metrics.mem_quota_bytes, std::memory_order_relaxed);
    _disk_quota.store(metrics.disk_quota_bytes, std::memory_order_relaxed);
}

void StarCacheEngine::_try_release_obj_handle(ObjectCacheHandlePtr handle) {
    if (handle) {
        auto obj_hdl = reinterpret_cast<starcache::ObjectHandle*>(handle);
        obj_hdl->release();
        delete obj_hdl;
    }
}

void StarCacheEngine::disk_spaces(std::vector<DirSpace>* spaces) const {
    spaces->clear();
    auto metrics = starcache_metrics(0);
    for (auto& dir : metrics.disk_dir_spaces) {
        spaces->push_back({.path = dir.path, .size = dir.quota_bytes});
    }
}

size_t StarCacheEngine::mem_quota() const {
    starcache::CacheMetrics metrics = _cache->metrics(0);
    // TODO: optimizer later
    return metrics.mem_quota_bytes;
}

size_t StarCacheEngine::mem_usage() const {
    // TODO: add meta size?
    starcache::CacheMetrics metrics = _cache->metrics(0);
    return metrics.mem_used_bytes;
}

size_t StarCacheEngine::lookup_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count + metrics.detail_l1->miss_count;
}

size_t StarCacheEngine::hit_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count;
}

const ObjectCacheMetrics StarCacheEngine::metrics() const {
    auto starcache_metrics = _cache->metrics(2);
    ObjectCacheMetrics m;
    m.capacity = starcache_metrics.mem_quota_bytes;
    m.usage = starcache_metrics.mem_used_bytes;
    m.lookup_count = starcache_metrics.detail_l1->hit_count + starcache_metrics.detail_l1->miss_count;
    m.hit_count = starcache_metrics.detail_l1->hit_count;
    m.object_item_count = starcache_metrics.detail_l2->object_item_count;
    return m;
}

Status StarCacheEngine::prune() {
    return to_status(_cache->update_mem_quota(0, false));
}

} // namespace starrocks
