// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "storage/compaction_task.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class RowsetWriter;
namespace vectorized {
class TabletReader;
class RowSourceMaskBuffer;
class RowSourceMask;
} // namespace vectorized

// need a factory of compaction task
class VerticalCompactionTask : public CompactionTask {
public:
    VerticalCompactionTask() : CompactionTask(VERTICAL_COMPACTION) {}
    ~VerticalCompactionTask() = default;

    Status run_impl() override;

private:
    Status _vertical_compaction_data(Statistics* statistics);

    Status _compact_column_group(bool is_key, int column_group_index, const std::vector<uint32_t>& column_group,
                                 RowsetWriter* output_rs_writer, vectorized::RowSourceMaskBuffer* mask_buffer,
                                 std::vector<vectorized::RowSourceMask>* source_masks, Statistics* statistics);

    StatusOr<size_t> _compact_data(bool is_key, int32_t chunk_size, const std::vector<uint32_t>& column_group,
                                   const vectorized::Schema& schema, vectorized::TabletReader* reader,
                                   RowsetWriter* output_rs_writer, vectorized::RowSourceMaskBuffer* mask_buffer,
                                   std::vector<vectorized::RowSourceMask>* source_masks);

    StatusOr<int32_t> _calculate_chunk_size_for_column_group(const std::vector<uint32_t>& column_group);
};

} // namespace starrocks
