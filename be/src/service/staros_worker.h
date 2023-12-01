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

#ifdef USE_STAROS

#include <starlet.h>

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common/status.h"
#include "fslib/configuration.h"
#include "fslib/file_system.h"

namespace starrocks {

class Cache;
class CacheKey;

// TODO: find a better place to put this function
// Convert absl::Status to starrocks::Status
Status to_status(const absl::Status& absl_status);

class StarOSWorker : public staros::starlet::Worker {
public:
    using ServiceId = staros::starlet::ServiceId;
    using WorkerId = staros::starlet::WorkerId;
    using ShardId = staros::starlet::ShardId;
    using ShardInfo = staros::starlet::ShardInfo;
    using WorkerInfo = staros::starlet::WorkerInfo;
    using FileSystem = staros::starlet::fslib::FileSystem;
    using Configuration = staros::starlet::fslib::Configuration;

    StarOSWorker();

    ~StarOSWorker() override;

    absl::Status add_shard(const ShardInfo& shard) override;

    absl::Status remove_shard(const ShardId shard) override;

    absl::StatusOr<WorkerInfo> worker_info() const override;

    absl::Status update_worker_info(const WorkerInfo& info) override;

    // get shard info directly from local cache, if the shard is assigned to this worker
    absl::StatusOr<ShardInfo> get_shard_info(ShardId id) const override;

    std::vector<ShardInfo> shards() const override;

    // `conf`: a k-v map, provides additional information about the filesystem configuration
    absl::StatusOr<std::shared_ptr<FileSystem>> get_shard_filesystem(ShardId id, const Configuration& conf);

    // retrieve shard info from the worker. Unlike `get_shard_info`, if the shard info is not there in local cache,
    // the worker will try to fetch it back from starmgr.
    absl::StatusOr<ShardInfo> retrieve_shard_info(ShardId id);

private:
    struct ShardInfoDetails {
        ShardInfo shard_info;
        std::shared_ptr<FileSystem> fs;

        ShardInfoDetails(const ShardInfo& info) : shard_info(info) {}
    };

    using CacheValue = std::weak_ptr<FileSystem>;

    // This function can be made static perfectly. The only reason to make it `virtual`
    // is, for unit test MOCK as it is the only interface to interact with g_starlet.
    virtual absl::StatusOr<ShardInfo> _fetch_shard_info_from_remote(ShardId id);

    static void cache_value_deleter(const CacheKey& /*key*/, void* value) { delete static_cast<CacheValue*>(value); }

    static std::string get_cache_key(std::string_view scheme, const Configuration& conf);

    static absl::StatusOr<staros::starlet::fslib::Configuration> build_conf_from_shard_info(const ShardInfo& info);

    static absl::StatusOr<std::string> build_scheme_from_shard_info(const ShardInfo& info);

    static bool need_enable_cache(const ShardInfo& info);

    absl::StatusOr<std::shared_ptr<FileSystem>> build_filesystem_on_demand(ShardId id, const Configuration& conf);
    absl::StatusOr<std::shared_ptr<FileSystem>> build_filesystem_from_shard_info(const ShardInfo& info,
                                                                                 const Configuration& conf);
    absl::StatusOr<std::shared_ptr<FileSystem>> new_shared_filesystem(std::string_view scheme,
                                                                      const Configuration& conf);
    absl::Status invalidate_fs(const ShardInfo& shard);

    mutable std::shared_mutex _mtx;
    std::unordered_map<ShardId, ShardInfoDetails> _shards;
    std::unique_ptr<Cache> _fs_cache;
};

extern std::shared_ptr<StarOSWorker> g_worker;
void init_staros_worker();
void shutdown_staros_worker();
void update_staros_starcache();

} // namespace starrocks
#endif // USE_STAROS
