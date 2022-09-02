// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "gutil/macros.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class FixedLocationProvider : public LocationProvider {
public:
    explicit FixedLocationProvider(std::string root) : _root(root) {
        while (!_root.empty() && _root.back() == '/') {
            _root.pop_back();
        }
    }

    ~FixedLocationProvider() override = default;

    // No usage now.
    DISALLOW_COPY_AND_MOVE(FixedLocationProvider);

    std::string root_location(int64_t tablet_id) const override { return _root; }

    Status list_root_locations(std::set<std::string>* roots) const override {
        roots->insert(_root);
        return Status::OK();
    }

private:
    std::string _root;
};

} // namespace starrocks::lake
