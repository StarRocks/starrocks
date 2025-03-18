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

#include "cache/block_cache/cache_options.h"
#include "cache/block_cache/dummy_types.h"
#include "cache/block_cache/io_buffer.h"
#include "common/status.h"

#ifdef WITH_STARCACHE
#include "starcache/star_cache.h"
#endif

namespace starrocks {

// We use the `starcache::ObjectHandle` directly because implementing a new one seems unnecessary.
// Importing the starcache headers here is not graceful, but the `cachelib` doesn't support
// object cache and we'll deprecate it for some performance reasons. Now there is no need to
// pay too much attention to the compatibility and upper-level abstraction of the cachelib interface.
#ifdef WITH_STARCACHE
using DataCacheMetrics = starcache::CacheMetrics;
using DataCacheStatus = starcache::CacheStatus;
#else
using DataCacheMetrics = DummyCacheMetrics;
using DataCacheStatus = DummyCacheStatus;
#endif

enum class DataCacheEngineType { STARCACHE };

class KvCache {
public:
    virtual ~KvCache() = default;

    // Init KV cache
    virtual Status init(const CacheOptions& options) = 0;

    // Write data to cache
    virtual Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) = 0;

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                        ReadCacheOptions* options) = 0;

    virtual bool exist(const std::string& key) const = 0;

    // Remove data from cache. The offset must be aligned by block size
    virtual Status remove(const std::string& key) = 0;

    // Update the datacache memory quota.
    virtual Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) = 0;

    // Update the datacache disk space infomation, such as disk quota or disk path.
    virtual Status update_disk_spaces(const std::vector<DirSpace>& spaces) = 0;

    virtual const DataCacheMetrics cache_metrics(int level) = 0;

    virtual void record_read_remote(size_t size, int64_t lateny_us) = 0;

    virtual void record_read_cache(size_t size, int64_t lateny_us) = 0;

    virtual Status shutdown() = 0;

    virtual DataCacheEngineType engine_type() = 0;

#ifdef WITH_STARCACHE
    virtual std::shared_ptr<starcache::StarCache> starcache_instance() = 0;
#endif
};

} // namespace starrocks
