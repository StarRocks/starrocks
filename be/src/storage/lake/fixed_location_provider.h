// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "gutil/macros.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class FixedLocationProvider : public LocationProvider {
public:
    explicit FixedLocationProvider(std::string root);

    ~FixedLocationProvider() override = default;

    // No usage now.
    DISALLOW_COPY_AND_MOVE(FixedLocationProvider);

    std::string root_location(int64_t tablet_id) const override { return _root; }

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const override;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const override;

    std::string txn_vlog_location(int64_t tablet_id, int64_t version) const override;

    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const override;

    std::string tablet_metadata_lock_location(int64_t tablet_id, int64_t version, int64_t expire_time) const override;

    Status list_root_locations(std::set<std::string>* roots) const override;

private:
    std::string _root;
};

} // namespace starrocks::lake
