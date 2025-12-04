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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/types.h"

namespace starrocks {

class SlotDescriptor;

// RangeRouter is responsible for routing rows to tablets based on
// tablet ranges and range-distributed columns in a row-based manner.
class RangeRouter {
public:
    RangeRouter();

    ~RangeRouter() = default;

    Status init(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns);

    Status route_chunk_rows(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                            const std::vector<uint16_t>& row_indices,
                            const std::vector<int64_t>& candidate_dest, std::vector<int64_t>* target_dest);

private:
    Status _validate_range(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) const;

    bool _check_row_in_bound(const std::vector<ColumnPtr>& columns, uint16_t row_idx,
                             size_t range_idx, bool is_lower_bound) const;

    StatusOr<size_t> _find_tablet_index_for_row(const std::vector<ColumnPtr>& columns, uint16_t row_idx) const;

private:
    // For each range-distributed column, holds a column of lower/upper bound
    // values across all tablet ranges.
    std::vector<ColumnPtr> _lower_boundary;
    std::vector<ColumnPtr> _upper_boundary;

    // Per-range metadata derived from tablet_ranges during init():
    //   - std::nullopt: this range has no lower/upper bound (i.e. -inf/+inf)
    //   - bool value  : the bound exists and indicates whether it is inclusive.
    std::vector<std::optional<bool>> _lower_bound_inclusive;
    std::vector<std::optional<bool>> _upper_bound_inclusive;

    mutable bool _single_inf_range;
};

} // namespace starrocks