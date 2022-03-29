// This file is licensed under the Elastic License 2.0. Copyright 2021 - present, StarRocks Limited.

#pragma once

#include "column/datum.h"
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

// Sort multiple columns, and stable
Status stable_sort_and_tie_columns(const bool& cancel, const Columns& columns, const std::vector<int>& sort_orders,
                                   const std::vector<int>& null_firsts, SmallPermutation* permutation);

// Sort multiple columns in vertical
Status sort_vertical_columns(const bool& cancel, const std::vector<ColumnPtr>& columns, bool is_asc_order,
                             bool is_null_first, Permutation& permutation, Tie& tie, std::pair<int, int> range,
                             bool build_tie, size_t limit = 0, size_t* limited = nullptr);

// Sort multiple chunks in column-wise style
Status sort_vertical_chunks(const bool& cancel, const std::vector<Columns>& vertical_chunks,
                            const std::vector<int>& sort_orders, const std::vector<int>& null_firsts, Permutation& perm,
                            size_t limit);

// Compare the column with the `rhs_value`, which must have the some type with column.
// @param cmp_result compare result is written into this array, value must within -1,0,1
// @param rhs_value the compare value
int compare_column(const ColumnPtr column, std::vector<int8_t>& cmp_result, Datum rhs_value, int sort_order,
                   int null_first);
void compare_columns(const Columns columns, std::vector<int8_t>& cmp_result, const std::vector<Datum>& rhs_values,
                     const std::vector<int>& sort_orders, const std::vector<int>& null_firsts);

// Build tie by comparison of adjacent rows in column.
// Tie(i) is set to 1 only if row(i-1) is equal to row(i), otherwise is set to 0.
void build_tie_for_column(const ColumnPtr column, Tie* tie);

} // namespace starrocks::vectorized