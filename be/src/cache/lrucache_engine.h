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

#include <atomic>

#include "cache/local_cache_engine.h"
#include "util/lru_cache.h"

namespace starrocks {
class LRUCacheEngine final : public LocalCacheEngine {
public:
    LRUCacheEngine() = default;
    ~LRUCacheEngine() override = default;

    Status init(const CacheOptions& options) override;
    bool is_initialized() const override { return _initialized.load(std::memory_order_relaxed); }

    Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) override;
    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer, ReadCacheOptions* options) override;

    Status insert(const std::string& key, void* value, size_t size, ObjectCacheDeleter deleter,
                  ObjectCacheHandlePtr* handle, const ObjectCacheWriteOptions& options) override;
    Status lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) override;

    bool exist(const std::string& key) const override;
    Status remove(const std::string& key) override;

    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) override;
    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override;
    Status update_inline_cache_count_limit(int32_t limit) override;

    const DataCacheMetrics cache_metrics() const override;
    void record_read_remote(size_t size, int64_t latency_us) override {}
    void record_read_cache(size_t size, int64_t latency_us) override {}

    Status shutdown() override;
    LocalCacheEngineType engine_type() override { return LocalCacheEngineType::LRUCACHE; }
    bool has_mem_cache() const override { return _cache->get_capacity() > 0; }
    bool has_disk_cache() const override { return false; }

    bool available() const override { return is_initialized() && has_mem_cache(); }
    bool mem_cache_available() const override { return is_initialized() && has_mem_cache(); }

    void disk_spaces(std::vector<DirSpace>* spaces) const override {}

    void release(ObjectCacheHandlePtr handle) override;
    const void* value(ObjectCacheHandlePtr handle) override;

    Status adjust_mem_quota(int64_t delta, size_t min_capacity) override;

    size_t mem_quota() const override;
    size_t mem_usage() const override;

    size_t lookup_count() const override;

    size_t hit_count() const override;

    const ObjectCacheMetrics metrics() const override;

    Status prune() override;

private:
    bool _check_write(size_t charge, const ObjectCacheWriteOptions& options) const;

    std::atomic<bool> _initialized = false;
    std::unique_ptr<ShardedLRUCache> _cache;
};
} // namespace starrocks