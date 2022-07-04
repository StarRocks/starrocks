// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <set>

#include "common/statusor.h"
#include "storage/lake/group_assigner.h"

namespace starrocks::lake {

class StarletGroupAssigner : public GroupAssigner {
public:
    std::string get_fs_prefix() override;
    StatusOr<std::string> get_group(int64_t tablet_id) override;
    Status list_group(std::set<std::string>* groups) override;
    std::string path_assemble(const std::string& path, int64_t tablet_id) override;
};

} // namespace starrocks::lake
