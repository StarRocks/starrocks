// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/starlet_group_assigner.h"

#ifdef USE_STAROS

#include <fmt/format.h>

#include "service/staros_worker.h"

namespace starrocks::lake {

static const char* const kStarletPrefix = "staros_";
static const char* const kGroupKey = "storageGroup";

StatusOr<std::string> StarletGroupAssigner::get_group(int64_t tablet_id) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }
    ASSIGN_OR_RETURN(auto shardinfo, g_worker->get_shard_info(tablet_id));
    auto iter = shardinfo.properties.find(kGroupKey);
    if (iter == shardinfo.properties.end()) {
        return Status::InternalError(fmt::format("Fail to find {} group path", tablet_id));
    }
    return fmt::format("{}{}", kStarletPrefix, iter->second);
}

Status StarletGroupAssigner::list_group(std::set<std::string>* groups) {
    if (g_worker == nullptr) {
        return Status::InternalError("init_staros_worker() must be called before get_shard_info()");
    }

    std::vector<staros::starlet::ShardInfo> shards = g_worker->shards();
    for (const auto& shard : shards) {
        auto iter = shard.properties.find(kGroupKey);
        if (iter == shard.properties.end()) {
            return Status::InternalError(fmt::format("Fail to find {} group path", shard.id));
        }
        std::string starlet_group = fmt::format("{}{}", kStarletPrefix, iter->second);
        groups->emplace(std::move(starlet_group));
    }
    return Status::OK();
}

} // namespace starrocks::lake
#endif // USE_STAROS
