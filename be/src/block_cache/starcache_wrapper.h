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
#include "starcache/star_cache.h"

namespace starrocks {

class StarCacheWrapper : public KvCache {
public:
    StarCacheWrapper() = default;
    ~StarCacheWrapper() override = default;

    Status init(const CacheOptions& options) override;

    Status write_cache(const std::string& key, const IOBuffer& buffer, size_t ttl_seconds, bool overwrite) override;

    Status read_cache(const std::string& key, size_t off, size_t size, IOBuffer* buffer) override;

    Status remove_cache(const std::string& key) override;

    std::unordered_map<std::string, double> cache_stats() override;

    Status shutdown() override;

private:
    std::unique_ptr<starcache::StarCache> _cache;
};

// In order to split the starcache library to a separate registry for other users such as the cloud team,
// we decouple it from starrocks as much as possible. So we use the butil::Status in starcache instead
// of starrocks::Status.
// This function is used to convert the butil::Status from starcache to starrocks::Status.
inline Status to_status(const butil::Status& st) {
    switch (st.error_code()) {
    case 0:
        return Status::OK();
    case ENOENT:
        return Status::NotFound(st.error_str());
    case EEXIST:
        return Status::AlreadyExist(st.error_str());
    case EINVAL:
        return Status::InvalidArgument(st.error_str());
    case EIO:
        return Status::IOError(st.error_str());
    case ENOMEM:
        return Status::MemoryLimitExceeded(st.error_str());
    default:
        return Status::InternalError(st.error_str());
    }
}

} // namespace starrocks
