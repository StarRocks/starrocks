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

#include "cache/object_cache/cache_types.h"
#include "cache/status.h"
#include "common/logging.h"

namespace starrocks {

StarCacheModule::StarCacheModule(std::shared_ptr<starcache::StarCache> star_cache) : _cache(std::move(star_cache)) {
    _initialized.store(true, std::memory_order_release);
}

Status StarCacheModule::insert(const std::string& key, void* value, size_t size, size_t charge,
                               ObjectCacheDeleter deleter, ObjectCacheHandlePtr* handle,
                               ObjectCacheWriteOptions* options) {
    starcache::ObjectHandle* obj_hdl = new starcache::ObjectHandle;
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
        st = to_status(_cache->set_object(key, value, charge, obj_deleter, obj_hdl, &opts));
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

Status StarCacheModule::remove(const std::string& key) {
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
        return Status::InvalidArgument("target capacity is less than the minimum capacity");
    }
    return to_status(_cache->update_mem_quota(new_capacity, false));
}

Status StarCacheModule::set_capacity(size_t capacity) {
    return to_status(_cache->update_mem_quota(capacity, false));
}

size_t StarCacheModule::capacity() const {
    starcache::CacheMetrics metrics = _cache->metrics(0);
    // TODO: optimizer later
    return metrics.mem_quota_bytes;
}

size_t StarCacheModule::usage() const {
    // TODO: add meta size?
    starcache::CacheMetrics metrics = _cache->metrics(0);
    return metrics.mem_used_bytes;
}

size_t StarCacheModule::lookup_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count + metrics.detail_l1->miss_count;
}

size_t StarCacheModule::hit_count() const {
    starcache::CacheMetrics metrics = _cache->metrics(1);
    return metrics.detail_l1->hit_count;
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
    return prune();
}

bool StarCacheModule::_try_release_obj_handle(ObjectCacheHandlePtr handle) {
    if (handle) {
        release(handle);
        return true;
    }
    return false;
}

} // namespace starrocks