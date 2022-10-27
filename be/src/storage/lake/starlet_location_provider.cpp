// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    // TODO(jeff.ding):hdfs support
    for (const auto& shard : shards) {
        if (shard.path_info.fs_info().fs_type() != staros::FileStoreType::S3) {
            return Status::NotSupported(fmt::format("Unsupported object store type: {}", shard.path_info.fs_info().fs_type()));
        }
        // root_ids[shard.obj_store_info.s3_obj_store.uri] = shard.id;
        root_ids[shard.path_info.full_path()] = shard.id;
    }
    for (const auto& [uri, id] : root_ids) {
        (void)uri;
        roots->insert(root_location(id));
    }
    return Status::OK();
}

} // namespace starrocks::lake
#endif // USE_STAROS
