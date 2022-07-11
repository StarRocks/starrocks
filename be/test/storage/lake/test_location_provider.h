// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <set>
#include <string>

#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class TestGroupAssigner : public LocationProvider {
public:
    TestGroupAssigner(std::string path) : _path(std::move(path)) {}

    ~TestGroupAssigner() override = default;

    StatusOr<std::string> root_location(int64_t tablet_id) override { return _path; }

    Status list_root_locations(std::set<std::string>* groups) override {
        groups->emplace(_path);
        return Status::OK();
    }
    std::string location(const std::string& path, int64_t tablet_id) override { return path; }

private:
    std::string _path;
};

} // namespace starrocks::lake
