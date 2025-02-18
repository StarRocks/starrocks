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

#include "cache/block_cache/kv_cache.h"
#include "common/status.h"
#include "starcache/star_cache.h"
#include "starcache/time_based_cache_adaptor.h"

namespace starrocks {

class StarCacheWrapper : public KvCache {
public:
    StarCacheWrapper() = default;
    ~StarCacheWrapper() override = default;

    Status init(const CacheOptions& options) override;

    Status write_buffer(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) override;

    Status write_object(const std::string& key, const void* ptr, size_t size, std::function<void()> deleter,
                        DataCacheHandle* handle, WriteCacheOptions* options) override;

    Status read_buffer(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                       ReadCacheOptions* options) override;

    Status read_object(const std::string& key, DataCacheHandle* handle, ReadCacheOptions* options) override;

    bool exist(const std::string& key) const override;

    Status remove(const std::string& key) override;

    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) override;

    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override;

    const DataCacheMetrics cache_metrics(int level) override;

    void record_read_remote(size_t size, int64_t lateny_us) override;

    void record_read_cache(size_t size, int64_t lateny_us) override;

    Status shutdown() override;

    DataCacheEngineType engine_type() override { return DataCacheEngineType::STARCACHE; }

    std::shared_ptr<starcache::StarCache> starcache_instance() override { return _cache; }

private:
    std::shared_ptr<starcache::StarCache> _cache;
    std::unique_ptr<starcache::TimeBasedCacheAdaptor> _cache_adaptor;
    bool _enable_tiered_cache = false;
    bool _enable_datacache_persistence = false;
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
    case ENOSPC:
        return Status::CapacityLimitExceed(st.error_str());
    case EBUSY:
        return Status::ResourceBusy(st.error_str());
    default:
        return Status::InternalError(st.error_str());
    }
}

} // namespace starrocks
