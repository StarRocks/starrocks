// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/compaction_task_factory.h"

#include "storage/compaction_task.h"

namespace starrocks {

std::shared_ptr<CompactionTask> CompactionTaskFactory::create_compaction_task() {
    return nullptr;
}

} // namespace starrocks
