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

#include "cache/local_disk_cache_engine.h"
#include "common/status.h"
#include "starcache/star_cache.h"
#include "starcache/time_based_cache_adaptor.h"

namespace starrocks {

class StarCacheEngine : public LocalDiskCacheEngine {
public:
    StarCacheEngine() = default;
    ~StarCacheEngine() override = default;

    Status init(const DiskCacheOptions& options);
    bool is_initialized() const override { return _initialized.load(std::memory_order_relaxed); }

    Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) override;
    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer, ReadCacheOptions* options) override;

    bool exist(const std::string& key) const override;

    Status remove(const std::string& key) override;

    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override;

    Status update_inline_cache_count_limit(int32_t limit) override;

    const StarCacheMetrics starcache_metrics(int level) const;

    const DataCacheMetrics cache_metrics() const override;

    void record_read_remote(size_t size, int64_t latency_us) override;

    void record_read_cache(size_t size, int64_t latency_us) override;

    Status shutdown() override;

    std::shared_ptr<starcache::StarCache> starcache_instance() { return _cache; }
    bool has_disk_cache() const override { return _disk_quota.load(std::memory_order_relaxed) > 0; }
    bool available() const override { return is_initialized() && has_disk_cache(); }

    void disk_spaces(std::vector<DirSpace>* spaces) const override;

    size_t lookup_count() const override;

    size_t hit_count() const override;

    const ObjectCacheMetrics metrics() const override;

    Status prune() override;

private:
    void _refresh_quota();

    std::shared_ptr<starcache::StarCache> _cache;
    std::unique_ptr<starcache::TimeBasedCacheAdaptor> _cache_adaptor;
    bool _enable_tiered_cache = false;
    bool _enable_datacache_persistence = false;
    std::atomic<bool> _initialized = false;

    std::atomic<size_t> _disk_quota = 0;
};
} // namespace starrocks
