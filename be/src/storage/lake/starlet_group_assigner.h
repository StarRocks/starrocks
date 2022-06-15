// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "common/statusor.h"
#include "storage/lake/group_assigner.h"

namespace starrocks::lake {

class StarletGroupAssigner : public GroupAssigner {
public:
    StatusOr<std::string> get_group(int64_t tablet_id) override;
    Status list_group(std::vector<std::string>* groups) override;
};

} // namespace starrocks::lake
