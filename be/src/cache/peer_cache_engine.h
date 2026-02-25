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

#include "cache/remote_cache_engine.h"
#include "starcache/time_based_cache_adaptor.h"

namespace starrocks {

class PeerCacheEngine : public RemoteCacheEngine {
public:
    PeerCacheEngine() = default;
    ~PeerCacheEngine() override = default;

    Status init(const RemoteCacheOptions& options) override;

    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                DiskCacheReadOptions* options) override;

    Status write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) override {
        return Status::NotSupported("write data to peer cache is unsupported");
    }

    Status remove(const std::string& key) override {
        return Status::NotSupported("remove data from peer cache is unsupported");
    }

    void record_read_remote(size_t size, int64_t lateny_us) override;

    void record_read_cache(size_t size, int64_t lateny_us) override;

    Status shutdown() override;

private:
    std::unique_ptr<starcache::TimeBasedCacheAdaptor> _cache_adaptor;
};

} // namespace starrocks
