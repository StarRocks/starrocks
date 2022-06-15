// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/starlet_group_assigner.h"

#ifdef USE_STAROS

#include <fmt/format.h>

#include "service/staros_worker.h"

namespace starrocks::lake {

static const char* const kStarletPrefix = "staros_";

StatusOr<std::string> StarletGroupAssigner::get_group(int64_t tablet_id) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }
    ASSIGN_OR_RETURN(auto shardinfo, g_worker->get_shard_info(tablet_id));
    auto group_id = 1001; // TODO get group id from shardinfo
    return fmt::format("{}{}/{}", kStarletPrefix, shardinfo.obj_store_info.uri, group_id);
}

Status StarletGroupAssigner::list_group(std::vector<std::string>* groups) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }

    std::vector<staros::starlet::ShardInfo> shards = g_worker->shards();
    auto group_id = 1001; // TODO get group id from shardinfo
    for (const auto& shard : shards) {
        std::string starlet_group = fmt::format("{}{}/{}", kStarletPrefix, shard.obj_store_info.uri, group_id);
        groups->emplace_back(std::move(starlet_group));
    }
    return Status::OK();
}

} // namespace starrocks::lake
#endif // USE_STAROS
