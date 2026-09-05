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

#include "formats/parquet/read_range_planner.h"

#include <algorithm>

#include "formats/parquet/column_reader.h"
#include "formats/parquet/group_reader.h"

namespace starrocks::parquet {

ReadRangePlanner::ReadRangePlanner(const GroupReaderParam& param, ColumnReaderMap* column_readers)
        : _param(param), _column_readers(column_readers) {}

void ReadRangePlanner::collect_ranges(const std::vector<int>& column_indices, bool is_active, std::vector<IORange>* out,
                                      int64_t* end_offset, ColumnIOTypeFlags types) {
    for (const auto& index : column_indices) {
        const auto& column = _param.read_cols[index];
        (*_column_readers)[column.slot_id()]->collect_column_io_range(out, end_offset, types, is_active);
    }
}

void ReadRangePlanner::deduplicate(std::vector<IORange>* ranges) {
    if (ranges == nullptr || ranges->size() <= 1) {
        return;
    }
    std::sort(ranges->begin(), ranges->end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.offset != rhs.offset) {
            return lhs.offset < rhs.offset;
        }
        if (lhs.size != rhs.size) {
            return lhs.size < rhs.size;
        }
        return lhs.is_active < rhs.is_active;
    });
    size_t write_idx = 0;
    for (size_t read_idx = 1; read_idx < ranges->size(); ++read_idx) {
        auto& current = (*ranges)[write_idx];
        const auto& next = (*ranges)[read_idx];
        if (current.offset == next.offset && current.size == next.size) {
            current.is_active = current.is_active || next.is_active;
            continue;
        }
        ++write_idx;
        if (write_idx != read_idx) {
            (*ranges)[write_idx] = next;
        }
    }
    ranges->erase(ranges->begin() + static_cast<std::ptrdiff_t>(write_idx + 1), ranges->end());
}

bool ReadRangePlanner::should_coalesce_active_lazy() const {
    if (_param.lazy_column_coalesce_counter == nullptr) {
        return true;
    }
    return _param.lazy_column_coalesce_counter->load(std::memory_order_relaxed) >= 0;
}

void ReadRangePlanner::pre_plan_lazy_ranges(const std::vector<int>& lazy_column_indices, int64_t* end_offset,
                                            ColumnIOTypeFlags types) {
    _lazy_ranges.clear();
    collect_ranges(lazy_column_indices, false, &_lazy_ranges, end_offset, types);
}

} // namespace starrocks::parquet
