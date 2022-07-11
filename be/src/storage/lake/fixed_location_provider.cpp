// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/fixed_location_provider.h"

#include <fmt/format.h>

namespace starrocks::lake {

std::string FixedLocationProvider::tablet_metadata_location(int64_t tablet_id, int64_t version) const {
    return fmt::format("{}/tbl_{:016X}_{:016X}", _root, tablet_id, version);
}

std::string FixedLocationProvider::txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return fmt::format("{}/txn_{:016X}_{:016X}", _root, tablet_id, txn_id);
}

std::string FixedLocationProvider::segment_location(int64_t /*tablet_id*/, std::string_view segment_name) const {
    return fmt::format("{}/{}", _root, segment_name);
}

std::string FixedLocationProvider::join_path(std::string_view parent, std::string_view child) const {
    return fmt::format("{}/{}", parent, child);
}

Status FixedLocationProvider::list_root_locations(std::set<std::string>* roots) const {
    roots->insert(_root);
    return Status::OK();
}

} // namespace starrocks::lake