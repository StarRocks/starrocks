// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "common/status.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/olap_common.h"

namespace starrocks {

class RowsetWriter;

namespace vectorized {
class TabletReader;
class Schema;
} // namespace vectorized
class HorizontalCompactionTask : public CompactionTask {
public:
    HorizontalCompactionTask() : CompactionTask(HORIZONTAL_COMPACTION) {}
    ~HorizontalCompactionTask() = default;
    Status run_impl() override;

private:
    Status _horizontal_compact_data(Statistics* statistics);
    StatusOr<size_t> _compact_data(int32_t chunk_size, vectorized::TabletReader& reader,
                                   const vectorized::Schema& schema, RowsetWriter* output_rs_writer);
    void _failure_callback();
    void _success_callback();
};

} // namespace starrocks
