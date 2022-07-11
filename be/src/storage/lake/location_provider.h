// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <set>
#include <string>

#include "common/statusor.h"

namespace starrocks::lake {

class LocationProvider {
public:
    virtual ~LocationProvider() = default;

    virtual StatusOr<std::string> root_location(int64_t tablet_id) = 0;

    virtual Status list_root_locations(std::set<std::string>* groups) = 0;

    virtual std::string location(const std::string& path, int64_t tablet_id) = 0;
};

} // namespace starrocks::lake
