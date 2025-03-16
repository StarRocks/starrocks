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

#include "cache/object_cache/cache_types.h"
#include "common/status.h"
#include "util/slice.h"

namespace starrocks {

class ObjectCacheModule {
public:
    virtual ~ObjectCacheModule() = default;

    virtual Status insert(const std::string& key, void* value, size_t size, size_t charge, ObjectCacheDeleter deleter,
                          ObjectCacheHandlePtr* handle, ObjectCacheWriteOptions* options) = 0;

    virtual Status lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) = 0;

    virtual Status remove(const std::string& key) = 0;

    virtual void release(ObjectCacheHandlePtr handle) = 0;

    virtual const void* value(ObjectCacheHandlePtr handle) = 0;

    virtual Slice value_slice(ObjectCacheHandlePtr handle) = 0;

    virtual Status adjust_capacity(int64_t delta, size_t min_capacity) = 0;

    virtual Status set_capacity(size_t capacity) = 0;

    virtual size_t capacity() const = 0;

    virtual size_t usage() const = 0;

    virtual size_t lookup_count() const = 0;

    virtual size_t hit_count() const = 0;

    virtual const ObjectCacheMetrics metrics() const = 0;

    virtual Status prune() = 0;

    virtual Status shutdown() = 0;
};

} // namespace starrocks