// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/status.h"

namespace starrocks::lake {

class CompactionTask {
public:
    virtual ~CompactionTask() = default;

    virtual Status execute() = 0;
};

} // namespace starrocks::lake