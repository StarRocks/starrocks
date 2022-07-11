// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/starlet_location_provider.h"

#ifdef USE_STAROS

#include <fmt/format.h>

#include "common/logging.h"
#include "fs/fs_starlet.h"
#include "gutil/strings/util.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

std::string StarletLocationProvider::root_location(int64_t tablet_id) const {
    return fmt::format("{}?ShardId={}", kStarletPrefix, tablet_id);
}

std::string StarletLocationProvider::tablet_metadata_location(int64_t tablet_id, int64_t version) const {
    return fmt::format("{}/tbl_{:016X}_{:016X}?ShardId={}", kStarletPrefix, tablet_id, version, tablet_id);
}

std::string StarletLocationProvider::txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return fmt::format("{}/txn_{:016X}_{:016X}?ShardId={}", kStarletPrefix, tablet_id, txn_id, tablet_id);
}

std::string StarletLocationProvider::segment_location(int64_t tablet_id, std::string_view segment_name) const {
    return fmt::format("{}/{}?ShardId={}", kStarletPrefix, segment_name, tablet_id);
}

std::string StarletLocationProvider::join_path(std::string_view parent, std::std::string_view child) const {
    auto pos = parent.find("?ShardId=");
    CHECK(pos != std::string::npos);
    return fmt::format("{}/{}{}", parent.substr(0, pos), child, parent.substr(pos));
}

Status StarletLocationProvider::list_root_locations(std::set<std::string>* roots) const override {
    (void)roots;
    return Status::NotSupported("StarletLocationProvider::list_root_locations");
}

} // namespace starrocks::lake
#endif // USE_STAROS
