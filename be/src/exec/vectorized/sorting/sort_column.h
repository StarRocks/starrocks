// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/status.h"
#include "exec/vectorized/sorting/sort_permute.h"

namespace starrocks::vectorized {

// Sort this column incrementally, and build tie for the next column
// @param is_asc_order ascending order or descending order
// @param is_null_first null first or null last
// @param permutation input and output permutation
// @param tie input and output tie
// @param range sort range, {0, 0} means not build tie but sort data
Status sort_and_tie_column(const bool& cancel, const ColumnPtr column, bool is_asc_order, bool is_null_first,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, bool build_tie);

// Sort multiple columns using column-wise algorithm, output the order in permutation array
Status sort_and_tie_columns(const bool& cancel, const Columns& columns, const std::vector<int>& sort_orders,
                            const std::vector<int>& null_firsts, Permutation* permutation);

} // namespace starrocks::vectorized