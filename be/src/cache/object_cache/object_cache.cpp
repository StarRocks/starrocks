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

#include "cache/object_cache/object_cache.h"

#include <fmt/format.h>

#include "cache/object_cache/lrucache_module.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "runtime/exec_env.h"

namespace starrocks {

ObjectCache::~ObjectCache() {
    (void)shutdown();
}

Status ObjectCache::init(const ObjectCacheOptions& options) {
    if (_initialized.load(std::memory_order_acquire)) {
        LOG(WARNING) << "Fail to initialize because it has already been initialized before";
        return Status::AlreadyExist("already initialized");
    }
    if (options.module == ObjectCacheModuleType::LRUCACHE) {
        _cache_module = std::make_shared<LRUCacheModule>(options);
        LOG(INFO) << "Init object cache with lrucache module";
    }
    if (!_cache_module) {
        LOG(ERROR) << "Unsupported cache module";
        return Status::NotSupported("unsupported block cache engine");
    }
    RETURN_IF_ERROR(_cache_module->init());
    _initialized.store(true, std::memory_order_release);
    return Status::OK();
}

Status ObjectCache::insert(const std::string& key, void* value, size_t size, size_t charge, ObjectCacheDeleter deleter,
                           ObjectCacheHandlePtr* handle, ObjectCacheWriteOptions* options) {
    return _cache_module->insert(key, value, size, charge, std::move(deleter), handle, options);
}

Status ObjectCache::lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) {
    return _cache_module->lookup(key, handle, options);
}

Status ObjectCache::remove(const std::string& key) {
    return _cache_module->remove(key);
}

void ObjectCache::release(ObjectCacheHandlePtr handle) {
    return _cache_module->release(handle);
}

const void* ObjectCache::value(ObjectCacheHandlePtr handle) {
    return _cache_module->value(handle);
}

Slice ObjectCache::value_slice(ObjectCacheHandlePtr handle) {
    return _cache_module->value_slice(handle);
}

Status ObjectCache::adjust_capacity(int64_t delta, size_t min_capacity) {
    return _cache_module->adjust_capacity(delta, min_capacity);
}

Status ObjectCache::set_capacity(size_t capacity) {
    return _cache_module->set_capacity(capacity);
}

size_t ObjectCache::capacity() const {
    return _cache_module->capacity();
}

size_t ObjectCache::usage() const {
    return _cache_module->usage();
}

size_t ObjectCache::lookup_count() const {
    return _cache_module->lookup_count();
}

size_t ObjectCache::hit_count() const {
    return _cache_module->hit_count();
}

const ObjectCacheMetrics ObjectCache::metrics() const {
    return _cache_module->metrics();
}

Status ObjectCache::prune() {
    return _cache_module->prune();
}

Status ObjectCache::shutdown() {
    return _cache_module->shutdown();
}

} // namespace starrocks