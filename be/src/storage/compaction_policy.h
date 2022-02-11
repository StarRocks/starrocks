// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "storage/compaction_task.h"

namespace starrocks {

// Compaction policy for tablet
class CompactionPolicy {
public:
    virtual ~CompactionPolicy() = default;

    // used to judge whether a tablet should do compaction or not
    virtual bool need_compaction() = 0;

    // to calculate the compaction score of a tablet
    // which is used to decide the compaction sequence of tablets
    virtual double compaction_score() = 0;

    // used to generate a CompactionTask for tablet
    virtual std::shared_ptr<CompactionTask> create_compaction() = 0;
};

} // namespace starrocks
