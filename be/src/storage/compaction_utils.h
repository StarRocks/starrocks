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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "storage/olap_common.h"
#include "tablet_schema.h"

namespace starrocks {

class CompactionPolicy;
class RowsetWriter;
class Tablet;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

enum CompactionAlgorithm {
    // compaction by all columns together.
    HORIZONTAL_COMPACTION = 0,
    // compaction by column group, for tablet with many columns.
    VERTICAL_COMPACTION = 1
};

struct Statistics {
    // number of rows written to the destination rowset after merge
    int64_t output_rows = 0;
    int64_t merged_rows = 0;
    int64_t filtered_rows = 0;
};

// need a factory of compaction task
class CompactionUtils {
public:
    static const char* compaction_algorithm_to_string(CompactionAlgorithm v);

    static int32_t get_read_chunk_size(int64_t mem_limit, int32_t config_chunk_size, int64_t total_num_rows,
                                       int64_t total_mem_footprint, size_t source_num);

    static Status construct_output_rowset_writer(Tablet* tablet, uint32_t max_rows_per_segment,
                                                 CompactionAlgorithm algorithm, Version version,
                                                 std::unique_ptr<RowsetWriter>* output_rowset_writer,
                                                 const TabletSchemaCSPtr& tablet_schema);

    static uint32_t get_segment_max_rows(int64_t max_segment_file_size, int64_t input_row_num, int64_t input_size);

    static void split_column_into_groups(size_t num_columns, const std::vector<ColumnId>& sort_key_idxes,
                                         int64_t max_columns_per_group,
                                         std::vector<std::vector<uint32_t>>* column_groups);

    // choose compaction algorithm according to tablet schema, max columns per group and segment iterator num.
    // 1. if the number of columns in the schema is less than or equal to max_columns_per_group, use HORIZONTAL_COMPACTION.
    // 2. if source_num is less than or equal to 1, or is more than MAX_SOURCES, use HORIZONTAL_COMPACTION.
    static CompactionAlgorithm choose_compaction_algorithm(size_t num_columns, int64_t max_columns_per_group,
                                                           size_t source_num);

    static RowsetSharedPtr& rowset_with_max_schema_version(std::vector<RowsetSharedPtr>& rowsets);
};

} // namespace starrocks
