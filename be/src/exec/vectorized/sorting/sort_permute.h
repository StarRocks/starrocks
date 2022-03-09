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

struct SmallPermuteItem {
    uint32_t index_in_chunk;
};

// Inline data value into the permutation to optimize cache efficiency
template <class T>
struct InlinePermuteItem {
    // NOTE: do not inline a large value
    static_assert(sizeof(T) <= 16, "Do not inline a large value");
    
    T inline_value;
    uint32_t index_in_chunk;
};

using Tie = std::vector<uint8_t>;
using Permutation = std::vector<PermutationItem>;
using SmallPermutation = std::vector<SmallPermuteItem>;

template <class T>
using InlinePermutation = std::vector<InlinePermuteItem<T>>;

enum CompareStrategy {
    Default = 0,
    RowWise = 1,
    ColumnWise = 2,
    ColumnInc = 3,
};

} // namespace starrocks::vectorized