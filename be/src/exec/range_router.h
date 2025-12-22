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
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "runtime/types.h"

namespace starrocks {

class SlotDescriptor;

// RangeRouter is responsible for routing rows will ranges which represent the entire (-inf, +inf)
class RangeRouter {
public:
    RangeRouter() = default;

    ~RangeRouter() = default;

    Status init(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns);

    Status route_chunk_rows(Chunk* chunk, const std::vector<SlotDescriptor*>& slot_descs,
                            const std::vector<uint16_t>& row_indices, const std::vector<int64_t>& candidate_dest,
                            std::vector<int64_t>* target_dest);

private:
    Status _validate_range(const std::vector<TTabletRange>& tablet_ranges, size_t num_columns) const;

    size_t _find_tablet_index_for_row(const ChunkRow& check_row) const;

private:
    // Compact representation of the entire range (-inf, +inf)
    // For example, if there are 3 tablet ranges : (-inf, 100), [100, 200), [200, +inf)
    // we only save the valid upper boundaries: [100, 200]
    // lower bound must strictly less than upper bound
    MutableColumns _upper_boundaries; // size = range column count
    // row-wise view of the upper boundaries
    std::vector<ChunkRow> _upper_boundaries_slice; // size = range count
};

} // namespace starrocks
