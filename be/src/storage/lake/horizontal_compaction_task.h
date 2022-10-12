// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "storage/lake/compaction_task.h"

namespace starrocks::vectorized {
class Chunk;
class ChunkIterator;
} // namespace starrocks::vectorized

namespace starrocks::lake {

class Rowset;
class Tablet;
class TabletWriter;

class HorizontalCompactionTask : public CompactionTask {
public:
    explicit HorizontalCompactionTask(int64_t txn_id, int64_t version, std::shared_ptr<Tablet> tablet,
                                      std::vector<std::shared_ptr<Rowset>> input_rowsets)
            : _txn_id(txn_id),
              _version(version),
              _tablet(std::move(tablet)),
              _input_rowsets(std::move(input_rowsets)) {}

    ~HorizontalCompactionTask() override;

    Status execute() override;

private:
    int64_t _txn_id;
    int64_t _version;
    std::shared_ptr<Tablet> _tablet;
    std::vector<std::shared_ptr<Rowset>> _input_rowsets;
};

} // namespace starrocks::lake