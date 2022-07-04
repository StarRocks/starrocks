// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/starlet_group_assigner.h"

#ifdef USE_STAROS

#include <fmt/format.h>

#include "fs/fs_starlet.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

static std::string normalize_group(const std::string& group) {
    if (group.back() != '/') {
        return group;
    }
    return group.substr(0, group.length() - 1);
}

std::string StarletGroupAssigner::get_fs_prefix() {
    return kStarletPrefix;
}

StatusOr<std::string> StarletGroupAssigner::get_group(int64_t tablet_id) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }
    ASSIGN_OR_RETURN(auto shard_info, g_worker->get_shard_info(tablet_id));
    return normalize_group(shard_info.obj_store_info.s3_obj_store.uri);
}

Status StarletGroupAssigner::list_group(std::set<std::string>* groups) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }

    std::vector<staros::starlet::ShardInfo> shards = g_worker->shards();
    for (const auto& shard : shards) {
        groups->emplace(std::move(normalize_group(shard.obj_store_info.s3_obj_store.uri)));
    }
    return Status::OK();
}

std::string StarletGroupAssigner::path_assemble(const std::string& path, int64_t tablet_id) {
    return fmt::format("{}{}?ShardId={}", kStarletPrefix, path, tablet_id);
}

} // namespace starrocks::lake
#endif // USE_STAROS
