// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <set>

#include "common/statusor.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class StarletLocationProvider : public LocationProvider {
public:
    // The result should be guaranteed to not end with "/"
    StatusOr<std::string> root_location(int64_t tablet_id) override;

    // The result should be guaranteed to not end with "/"
    Status list_root_locations(std::set<std::string>* groups) override;

    // 1. Add "staros://" prefix
    // 2. Add "?ShardId=tablet_id" suffix
    std::string location(const std::string& file_name, int64_t tablet_id) override;
};

} // namespace starrocks::lake
