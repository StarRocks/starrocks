// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <set>

#include "common/statusor.h"
#include "storage/lake/group_assigner.h"

namespace starrocks::lake {

class StarletGroupAssigner : public GroupAssigner {
public:
    std::string get_fs_prefix() override;

    // The result should be guaranteed to not end with "/"
    StatusOr<std::string> get_group(int64_t tablet_id) override;

    // The result should be guaranteed to not end with "/"
    Status list_group(std::set<std::string>* groups) override;

    // 1. Add "staros://" prefix
    // 2. Add "?ShardId=tablet_id" suffix
    std::string path_assemble(const std::string& path, int64_t tablet_id) override;
};

} // namespace starrocks::lake
