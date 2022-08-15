// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#ifdef USE_STAROS

#include <set>

#include "common/statusor.h"
#include "gutil/macros.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class StarletLocationProvider : public LocationProvider {
public:
    StarletLocationProvider() = default;
    ~StarletLocationProvider() = default;

    DISALLOW_COPY_AND_MOVE(StarletLocationProvider);

    std::string root_location(int64_t tablet_id) const override;

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const override;

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const override;

    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const override;

    Status list_root_locations(std::set<std::string>* roots) const override;
};

} // namespace starrocks::lake

#endif // USE_STAROS
