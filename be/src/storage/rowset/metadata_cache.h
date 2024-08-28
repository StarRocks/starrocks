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

#include <memory>
#include <mutex>
#include <string_view>
#include <variant>

#include "gutil/macros.h"

namespace starrocks {
class Cache;
class CacheKey;
class Rowset;

class MetadataCache {
public:
    explicit MetadataCache(size_t capacity);

    ~MetadataCache() {}

    DISALLOW_COPY_AND_MOVE(MetadataCache);

    // Create global instance of this class
    static void create_cache(size_t capacity);

    static MetadataCache* instance() { return _s_instance; }

    // will be called after rowset load metadata.
    void cache_rowset(Rowset* ptr);

    // evict this rowset manually, will be called before rowset destroy.
    void evict_rowset(Rowset* ptr);

    // Memory usage of lru cache
    size_t get_memory_usage() const;

private:
    void _insert(const std::string& key, Rowset* ptr, size_t size);
    void _erase(const std::string& key);
    static void _cache_value_deleter(const CacheKey& /*key*/, void* value);

    static MetadataCache* _s_instance;

    // LRU cache for metadata
    std::unique_ptr<Cache> _cache;
};

} // namespace starrocks