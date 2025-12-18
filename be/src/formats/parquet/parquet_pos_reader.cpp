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

#include "formats/parquet/parquet_pos_reader.h"

#include "column/datum.h"
#include "storage/range.h"

namespace starrocks::parquet {

Status ParquetPosReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    if (filter == nullptr) {
        // No filter, generate positions for all rows in the range
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            // Generate position based on the row group first row and the current row index.
            int64_t pos = _row_group_first_row + i;
            dst->as_mutable_raw_ptr()->append_datum(Datum(pos));
        }
    } else {
        // Apply filter, only generate positions for selected rows
        DCHECK_EQ(filter->size(), range.span_size()) << "Filter size must match range size";
        for (uint64_t i = range.begin(); i < range.end(); ++i) {
            size_t filter_index = i - range.begin();
            if ((*filter)[filter_index]) {
                // Generate position based on the row group first row and the current row index.
                int64_t pos = _row_group_first_row + i;
                dst->as_mutable_raw_ptr()->append_datum(Datum(pos));
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks::parquet