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

#include "block_cache/cache_options.h"
#include "block_cache/io_buffer.h"
#include "common/status.h"
#include "starcache/star_cache.h"

namespace starrocks {

using DataCacheMetrics = starcache::CacheMetrics;

class KvCache {
public:
    virtual ~KvCache() = default;

    // Init KV cache
    virtual Status init(const CacheOptions& options) = 0;

    // Write data to cache
    virtual Status write_cache(const std::string& key, const IOBuffer& buffer, size_t ttl_seconds, bool overwrite) = 0;

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual Status read_cache(const std::string& key, size_t off, size_t size, IOBuffer* buffer) = 0;

    // Remove data from cache. The offset must be aligned by block size
    virtual Status remove_cache(const std::string& key) = 0;

    virtual const DataCacheMetrics cache_metrics() = 0;

    virtual Status shutdown() = 0;
};

} // namespace starrocks
