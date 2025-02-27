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

#include <functional>
#include <ostream>
#include <string>

// Not a good way to import lru cache header here, just for temporary compatibility with old deleters.
#include "util/lru_cache.h"

namespace starrocks {

enum class ObjectCacheModuleType { LRUCACHE, STARCACHE };

struct ObjectCacheOptions {
    size_t capacity = 0;
    ObjectCacheModuleType module = ObjectCacheModuleType::LRUCACHE;
};

struct ObjectCacheWriteOptions {
    // The priority of the cache object, only support 0 and 1 now.
    int8_t priority = 0;
    // If ttl_seconds=0 (default), no ttl restriction will be set. If an old one exists, remove it.
    uint64_t ttl_seconds = 0;
    // If overwrite=true, the cache value will be replaced if it already exists.
    bool overwrite = false;
    // The probability to evict other items if the cache space is full, which can help avoid frequent cache replacement
    // and improve cache hit rate sometimes.
    // It is expressed as a percentage. If evict_probability is 10, it means the probability to evict other data is 10%.
    int32_t evict_probability = 100;
};

struct ObjectCacheReadOptions {};

struct ObjectCacheHandle {};

struct ObjectCacheMetrics {
    size_t capacity = 0;
    size_t usage = 0;
    size_t lookup_count = 0;
    size_t hit_count = 0;
    size_t object_item_count = 0;
};

using ObjectCacheHandlePtr = ObjectCacheHandle*;

// using CacheDeleter = std::function<void(const std::string&, void*)>;
//
// We only use the deleter function of the lru cache temporarily.
// Maybe a std::function object or a function pointer like `void (*)(std::string&, void*)` which
// independent on lru cache is more appropriate, but it is not easy to convert them to the lru
// cache deleter when using a lru cache module.
using ObjectCacheDeleter = void (*)(const CacheKey&, void*);

inline std::ostream& operator<<(std::ostream& os, const ObjectCacheModuleType& module) {
    switch (module) {
    case ObjectCacheModuleType::LRUCACHE:
        os << "lrucache";
        break;
    case ObjectCacheModuleType::STARCACHE:
        os << "starcache";
        break;
    }
    return os;
}

} // namespace starrocks