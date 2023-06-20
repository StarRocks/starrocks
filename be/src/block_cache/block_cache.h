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

#include "block_cache/kv_cache.h"
#include "common/status.h"

namespace starrocks {

class BlockCache {
public:
    typedef std::string CacheKey;

    // Return a singleton block cache instance
    static BlockCache* instance();

    // Init the block cache instance
    Status init(const CacheOptions& options);

    // Write data to cache, the offset must be aligned by block size
    Status write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer, size_t ttl_seconds = 0,
                       bool overwrite = true);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset and size must be aligned by block size.
    StatusOr<size_t> read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer);

    // NOTICE: This function is not safe now, as the returned buffer may be evicted before visited
    // by users. We need to implement it more safe by cachelib item handle.
    Status read_cache_zero_copy(const CacheKey& cache_key, off_t offset, size_t size, const char** buf);

    // Remove data from cache. The offset and size must be aligned by block size
    Status remove_cache(const CacheKey& cache_key, off_t offset, size_t size);

    // Shutdown the cache instance to save some state meta
    Status shutdown();

    size_t block_size() const { return _block_size; }

    static const size_t MAX_BLOCK_SIZE;

private:
#ifndef BE_TEST
    BlockCache() = default;
#endif

    size_t _block_size = 0;
    std::unique_ptr<KvCache> _kv_cache;
};

} // namespace starrocks
