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
#include "cache/block_cache/io_buffer.h"
#include "common/status.h"

namespace starrocks {

class RemoteCache {
public:
    virtual ~RemoteCache() = default;

    // Init remote cache
    virtual Status init(const CacheOptions& options) = 0;

    // Write data to remote cache
    virtual Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) = 0;

    // Read data from remote cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                        ReadCacheOptions* options) = 0;

    // Remove data from cache.
    virtual Status remove(const std::string& key) = 0;

    virtual void record_read_remote(size_t size, int64_t lateny_us) = 0;

    virtual void record_read_cache(size_t size, int64_t lateny_us) = 0;

    virtual Status shutdown() = 0;
};

} // namespace starrocks
