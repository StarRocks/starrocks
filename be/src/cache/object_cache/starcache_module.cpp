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

#include "cache/object_cache/starcache_module.h"

#include "common/logging.h"
#include "cache/status.h"
#include "runtime/current_thread.h"

namespace starrocks {

Status StarCacheModule::init() {
    if (_cache) {
        return Status::OK();
    }
    starcache::CacheOptions opts;
    opts.mem_quota_bytes = _options.capacity;
    opts.scheduler_thread_ratio_per_cpu = 0;
    opts.alloc_mem_threshold = 100;
    opts.instance_name = "object_cache";
    _cache = std::make_shared<starcache::StarCache>();
    return to_status(_cache->init(opts));
}

Status StarCacheModule::insert(const std::string& key, void* value, size_t size, size_t charge,
                                ObjectCacheDeleter deleter, ObjectCacheHandlePtr* handle,
                                ObjectCacheWriteOptions* options) {
    starcache::ObjectHandle *obj_hdl = new starcache::ObjectHandle;
    auto obj_deleter = [deleter, key, value] {
        // For temporary compatibility with old deleters. 
        CacheKey cache_key(key);
        deleter(cache_key, value);
    };
    Status st;
    if (!options) {
        st = to_status(_cache->set_object(key, value, charge, obj_deleter, obj_hdl, nullptr));
    } else {
        starcache::WriteOptions opts;
        opts.priority = options->priority;
        opts.ttl_seconds = options->ttl_seconds;
        opts.overwrite = options->overwrite;
        opts.evict_probability = options->evict_probability;
        {
            // The memory when inserting starcache is no longer recorded to the query memory.
            // Because we free the memory in other threads in starcache library, which is hard to track.
            // It is safe because we limit the flying memory in starcache, also, this behavior
            // doesn't affect the process memory tracker.
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
            st = to_status(_cache->set_object(key, value, charge, obj_deleter, obj_hdl, &opts));
        }
    }
    if (!st.ok()) {
        delete obj_hdl;
    } else if (handle) {
        // Try release the old handle before fill it with a new one.
        _try_release_obj_handle(*handle);
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(obj_hdl);
    }
    return st;
}

Status StarCacheModule::lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) {
    starcache::ObjectHandle *obj_hdl = new starcache::ObjectHandle;
    // Skip checking options temporarily because there is no valid members in `ObjectCacheReadOptions` now.
    Status st = to_status(_cache->get_object(key, obj_hdl, nullptr));
    if (!st.ok()) {
        delete obj_hdl; 
    } else {
        _try_release_obj_handle(*handle);
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(obj_hdl);
    }
    return st;
}

Status StarCacheModule::remove(const std::string& key)  {
    return to_status(_cache->remove(key));
}

void StarCacheModule::release(ObjectCacheHandlePtr handle) {
    auto obj_hdl = reinterpret_cast<starcache::ObjectHandle*>(handle);
    obj_hdl->release();
    delete obj_hdl;
}

const void* StarCacheModule::value(ObjectCacheHandlePtr handle) {
    auto obj_hdl = reinterpret_cast<starcache::ObjectHandle*>(handle);
    return obj_hdl->ptr();
}

Slice StarCacheModule::value_slice(ObjectCacheHandlePtr handle) {
    auto obj_hdl = reinterpret_cast<starcache::ObjectHandle*>(handle);
    Slice slice((char*)obj_hdl->ptr(), obj_hdl->size());
    return slice;
}

Status StarCacheModule::adjust_capacity(int64_t delta, size_t min_capacity) {
    auto starcache_metrics = _cache->metrics();
    size_t capacity = starcache_metrics.mem_quota_bytes;
    int64_t new_capacity = capacity + delta;
    if (new_capacity < (int64_t)min_capacity) {
        return Status::InvalidArgument("target capcity is less than the minumum capacity");
    }
    return to_status(_cache->update_mem_quota(new_capacity, false));
}

Status StarCacheModule::set_capacity(size_t capacity) {
    return to_status(_cache->update_mem_quota(capacity, false));
}

size_t StarCacheModule::capacity() const {
    return 0;
    //return _cache->numeric_metric(starcache::CacheNumericMetricClass::MEM_QUOTA);
}

size_t StarCacheModule::usage() const {
    return 0;
    //return _cache->numeric_metric(starcache::CacheNumericMetricClass::MEM_USAGE);
}

size_t StarCacheModule::lookup_count() const {
    return 0;
    //return _cache->numeric_metric(starcache::CacheNumericMetricClass::READ_COUNT);
}

size_t StarCacheModule::hit_count() const {
    return 0;
    //return _cache->numeric_metric(starcache::CacheNumericMetricClass::HIT_COUNT);
}

const ObjectCacheMetrics StarCacheModule::metrics() const {
    auto starcache_metrics = _cache->metrics(2);
    ObjectCacheMetrics m;
    m.capacity = starcache_metrics.mem_quota_bytes;
    m.usage = starcache_metrics.mem_used_bytes;
    m.lookup_count = starcache_metrics.detail_l1->hit_count + starcache_metrics.detail_l1->miss_count;
    m.hit_count = starcache_metrics.detail_l1->hit_count;
    m.object_item_count = starcache_metrics.detail_l2->object_item_count;
    return m;
}

Status StarCacheModule::prune() {
    return to_status(_cache->update_mem_quota(0, false));
}

Status StarCacheModule::shutdown() {
    // TODO: Clean the all cache memory if the starcache instance is created by this class.
    // So before the process coredump, calling this function can help release the cache memory and
    // reduce core file size.
    if (_own_star_cache) {
        return to_status(_cache->update_mem_quota(0, false));
        return prune();
    }
    return Status::OK();
}

bool StarCacheModule::_try_release_obj_handle(ObjectCacheHandlePtr handle) {
    if (handle) {
        release(handle);
        return true;
    }
    return false;
}

} // namespace starrocks
