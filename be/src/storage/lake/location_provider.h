// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <set>
#include <string>
#include <string_view>

#include "common/statusor.h"

namespace starrocks::lake {

class LocationProvider {
public:
    virtual ~LocationProvider() = default;

    // The result should be guaranteed to not end with "/"
    virtual std::string root_location(int64_t tablet_id) const = 0;

    virtual std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const = 0;

    virtual std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const = 0;

    virtual std::string segment_location(int64_t tablet_id, std::string_view segment_name) const = 0;

    virtual Status list_root_locations(std::set<std::string>* groups) const = 0;
};

} // namespace starrocks::lake
