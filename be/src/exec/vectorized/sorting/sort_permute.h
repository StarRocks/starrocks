// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "column/vectorized_fwd.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

struct PermutationItem {
    uint32_t chunk_index;
    uint32_t index_in_chunk;
    uint32_t permutation_index;
};

using Permutation = std::vector<PermutationItem>;
using Tie = std::vector<uint8_t>;

enum CompareStrategy {
    Default = 0,
    RowWise = 1,
    ColumnWise = 2,
    ColumnInc = 3,
};

} // namespace starrocks::vectorized