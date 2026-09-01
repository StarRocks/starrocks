// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <vector>

#include "storage/lake/compaction_task.h"

namespace starrocks {
class Chunk;
class ChunkIterator;
class TabletSchema;
class RowSourceMaskBuffer;
struct RowSourceMask;
} // namespace starrocks

namespace starrocks::lake {

class TabletWriter;

class VerticalCompactionTask : public CompactionTask {
public:
    explicit VerticalCompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                                    CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema)
            : CompactionTask(std::move(tablet), std::move(input_rowsets), context, std::move(tablet_schema)) {}

    ~VerticalCompactionTask() override = default;

    Status execute(CancelFunc cancel_func, ThreadPool* flush_pool = nullptr) override;

private:
    StatusOr<int32_t> calculate_chunk_size_for_column_group(const std::vector<uint32_t>& column_group);

    // When `pk_in_key_group` is true (separate-sort-key PK table with eager index build), the key
    // column group has been widened to [sort-key columns, then PK columns]; the key-group schema is
    // built accordingly so the eager SST writer can read the PK columns.
    Status compact_column_group(bool is_key, int column_group_index, size_t num_column_groups,
                                const std::vector<uint32_t>& column_group, std::unique_ptr<TabletWriter>& writer,
                                RowSourceMaskBuffer* mask_buffer, std::vector<RowSourceMask>* source_masks,
                                const CancelFunc& cancel_func, bool pk_in_key_group);

    // For a separate-sort-key PK table with eager PK-index SST build, move the PK columns (tablet
    // column ids [0, num_key) not already in the sort key) out of the value groups and into the key
    // group (group 0), appended after the sort-key columns. Empty value groups are dropped. This lets
    // the eager SST writer, which only sees the key group, read the PK columns.
    void move_pk_columns_into_key_group(std::vector<std::vector<uint32_t>>* column_groups) const;
    int64_t _total_num_rows = 0;
    int64_t _total_data_size = 0;
    int64_t _total_input_segs = 0;
};

} // namespace starrocks::lake
