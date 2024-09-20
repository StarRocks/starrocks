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

#pragma once

#include "cache/object_cache/cache_module.h"
#include "starcache/star_cache.h"

namespace starrocks {

class StarCacheModule : public ObjectCacheModule {
public:
    StarCacheModule(const ObjectCacheOptions& options): _options(options), _own_star_cache(true) {}
    StarCacheModule(std::shared_ptr<starcache::StarCache> star_cache) : _cache(star_cache) {}

    ~StarCacheModule() = default;

    Status init() override;

    Status insert(const std::string& key, void* value, size_t size, size_t charge, ObjectCacheDeleter deleter,
                  ObjectCacheHandlePtr* handle, ObjectCacheWriteOptions* options) override;

    Status lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) override;

    Status remove(const std::string& key) override;

    void release(ObjectCacheHandlePtr handle) override;

    const void* value(ObjectCacheHandlePtr handle) override;

    Slice value_slice(ObjectCacheHandlePtr handle) override;

    Status adjust_capacity(int64_t delta, size_t min_capacity) override;

    Status set_capacity(size_t capacity) override;

    size_t capacity() const override;

    size_t usage() const override;

    size_t lookup_count() const override;

    size_t hit_count() const override;

    const ObjectCacheMetrics metrics() const override;

    Status prune() override;

    Status shutdown() override;

private:
    bool _try_release_obj_handle(ObjectCacheHandlePtr handle);

    ObjectCacheOptions _options;
    std::shared_ptr<starcache::StarCache> _cache;
    bool _own_star_cache = false;
};

} // namespace starrocks
