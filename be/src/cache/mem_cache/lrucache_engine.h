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

#include "cache/mem_cache/local_mem_cache_engine.h"
#include "util/lru_cache.h"

namespace starrocks {
class LRUCacheEngine final : public LocalMemCacheEngine {
public:
    LRUCacheEngine() = default;
    ~LRUCacheEngine() override = default;

    Status init(const MemCacheOptions& options);
    bool is_initialized() const override { return _initialized.load(std::memory_order_relaxed); }

    Status insert(const std::string& key, void* value, size_t size, MemCacheDeleter deleter, MemCacheHandlePtr* handle,
                  const MemCacheWriteOptions& options) override;
    Status lookup(const std::string& key, MemCacheHandlePtr* handle, MemCacheReadOptions* options) override;

    bool exist(const std::string& key) const override;
    Status remove(const std::string& key) override;

    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) override;

    const DataCacheMetrics cache_metrics() const override;

    Status shutdown() override;
    bool has_mem_cache() const override { return _cache->get_capacity() > 0; }

    bool available() const override { return is_initialized() && has_mem_cache(); }
    bool mem_cache_available() const override { return is_initialized() && has_mem_cache(); }

    void release(MemCacheHandlePtr handle) override;
    const void* value(MemCacheHandlePtr handle) override;

    Status adjust_mem_quota(int64_t delta, size_t min_capacity) override;

    size_t mem_quota() const override;
    size_t mem_usage() const override;

    size_t lookup_count() const override;

    size_t hit_count() const override;

    Status prune() override;

private:
    bool _check_write(size_t charge, const MemCacheWriteOptions& options) const;

    std::atomic<bool> _initialized = false;
    std::unique_ptr<ShardedLRUCache> _cache;
};
} // namespace starrocks
