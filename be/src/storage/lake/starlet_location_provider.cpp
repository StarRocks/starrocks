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

#ifdef USE_STAROS
#include "storage/lake/starlet_location_provider.h"

#include <fmt/format.h>

#include <unordered_map>

#include "common/logging.h"
#include "fs/fs_starlet.h"
#include "gutil/strings/util.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

std::string StarletLocationProvider::root_location(int64_t tablet_id) const {
    return build_starlet_uri(tablet_id, "");
}

Status StarletLocationProvider::list_root_locations(std::set<std::string>* roots) const {
    if (roots == nullptr) {
        return Status::InvalidArgument("roots set is NULL");
    }
    if (g_worker == nullptr) {
        return Status::OK();
    }
    auto shards = g_worker->shards();
    std::unordered_map<std::string, staros::starlet::ShardId> root_ids;
    for (const auto& shard : shards) {
        if (shard.path_info.fs_info().fs_type() != staros::FileStoreType::S3 &&
            shard.path_info.fs_info().fs_type() != staros::FileStoreType::HDFS) {
            return Status::NotSupported(
                    fmt::format("Unsupported object store type: {}", shard.path_info.fs_info().fs_type()));
        }
        root_ids[shard.path_info.full_path()] = shard.id;
    }
    for (const auto& [uri, id] : root_ids) {
        (void)uri;
        roots->insert(root_location(id));
    }
    return Status::OK();
}

std::set<int64_t> StarletLocationProvider::owned_tablets() const {
    std::set<int64_t> ret;
    if (g_worker != nullptr) {
        auto shards = g_worker->shards();
        for (const auto& s : shards) {
            ret.insert(s.id);
        }
    }
    return ret;
}

} // namespace starrocks::lake
#endif // USE_STAROS
