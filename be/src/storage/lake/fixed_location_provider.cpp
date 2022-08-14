// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/fixed_location_provider.h"

#include <fmt/format.h>

#include "storage/lake/filenames.h"

namespace starrocks::lake {

FixedLocationProvider::FixedLocationProvider(std::string root) : _root(std::move(root)) {
    while (!_root.empty() && _root.back() == '/') {
        _root.pop_back();
    }
}

std::string FixedLocationProvider::tablet_metadata_location(int64_t tablet_id, int64_t version) const {
    return fmt::format("{}/{}", _root, tablet_metadata_filename(tablet_id, version));
}

std::string FixedLocationProvider::txn_log_location(int64_t tablet_id, int64_t txn_id) const {
    return fmt::format("{}/{}", _root, txn_log_filename(tablet_id, txn_id));
}

std::string FixedLocationProvider::segment_location(int64_t /*tablet_id*/, std::string_view segment_name) const {
    return fmt::format("{}/{}", _root, segment_name);
}

Status FixedLocationProvider::list_root_locations(std::set<std::string>* roots) const {
    roots->insert(_root);
    return Status::OK();
}

} // namespace starrocks::lake
