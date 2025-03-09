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

#include <span>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "exec/sorting/sort_permute.h"
#include "runtime/chunk_cursor.h"

namespace starrocks {

struct SortDesc;
struct SortDescs;

// Sort this column incrementally, and build tie for the next column
// @param is_asc_order ascending order or descending order
// @param is_null_first null first or null last
// @param permutation input and output permutation
// @param tie input and output tie
// @param range sort range, {0, 0} means not build tie but sort data
Status sort_and_tie_column(const std::atomic<bool>& cancel, ColumnPtr& column, const SortDesc& sort_desc,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, const bool build_tie);
Status sort_and_tie_column(const std::atomic<bool>& cancel, const ColumnPtr& column, const SortDesc& sort_desc,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, const bool build_tie);

// Sort multiple columns using column-wise algorithm, output the order in permutation array
Status sort_and_tie_columns(const std::atomic<bool>& cancel, const Columns& columns, const SortDescs& sort_desc,
                            Permutation* permutation);

/// Usually used to sort array columns.
///
/// Sort each part of key columns, and write the result to perm. perm corresponds to src_offsets. The range of the i-th
/// part is [offsets[i], offsets[i+1]), where offsets represents src_offsets or offsets_per_key[col_i].
/// Note that each part range of the src column and the key columns may be different.
/// eg.
///     src_column = (null, [1,2], [3,4], [5, 6]),
///     key_column0 = ([1,1], [2,2], [3, 3], [4,4]),
///     key_column1 = ([1,2], [3,4], null, [5,6])
///     so, src_offsets = (0, 0, 2, 4, 6), offsets_per_key = ((0, 0, 2, 4, 6, 8), (0, 2, 4, 4, 6))
Status sort_and_tie_columns(const std::atomic<bool>& cancel, const std::vector<const Column*>& columns,
                            const SortDescs& sort_desc, SmallPermutation& perm,
                            const std::span<const uint32_t> src_offsets,
                            const std::vector<std::span<const uint32_t>>& offsets_per_key);

// Sort multiple columns, and stable
Status stable_sort_and_tie_columns(const std::atomic<bool>& cancel, const Columns& columns, const SortDescs& sort_desc,
                                   SmallPermutation* permutation);

// Sort multiple columns in vertical
Status sort_vertical_columns(const std::atomic<bool>& cancel, const Columns& columns, const SortDesc& sort_desc,
                             Permutation& permutation, Tie& tie, std::pair<int, int> range, const bool build_tie,
                             const size_t limit = 0, size_t* limited = nullptr);

// Sort multiple chunks in column-wise style
Status sort_vertical_chunks(const std::atomic<bool>& cancel, const std::vector<Columns>& vertical_chunks,
                            const SortDescs& sort_desc, Permutation& perm, const size_t limit,
                            const bool is_limit_by_rank = false);

// Compare the column with the `rhs_value`, which must have the some type with column.
// @param cmp_result compare result is written into this array, value must within -1,0,1
// @param rhs_value the compare value
int compare_column(const ColumnPtr& column, Buffer<int8_t>& cmp_result, Datum rhs_value, const SortDesc& desc);
void compare_columns(const Columns& columns, Buffer<int8_t>& cmp_result, const Buffer<Datum>& rhs_values,
                     const SortDescs& sort_desc);

// Build tie by comparison of adjacent rows in column.
// Tie(i) is set to 1 only if row(i-1) is equal to row(i), otherwise is set to 0.
void build_tie_for_column(const ColumnPtr& column, Tie* tie, const NullColumnPtr& null_column = nullptr);

struct SortDesc {
    int sort_order;
    int null_first;

    SortDesc() = default;
    SortDesc(bool is_asc, bool inull_first) {
        sort_order = is_asc ? 1 : -1;
        null_first = (inull_first ? -1 : 1) * sort_order;
    }
    SortDesc(int order, int null) : sort_order(order), null_first(null) {}

    // Discard sort_order effect on the null_first
    int nan_direction() const { return null_first * sort_order; }
    bool is_null_first() const { return (null_first * sort_order) == -1; }
    bool asc_order() const { return sort_order == 1; }
};
struct SortDescs {
    std::vector<SortDesc> descs;

    SortDescs() = default;
    ~SortDescs() = default;

    SortDescs(const std::vector<bool>& orders, const std::vector<bool>& null_firsts) {
        descs.resize(orders.size());
        for (size_t i = 0; i < orders.size(); ++i) {
            descs[i] = SortDesc(orders.at(i), null_firsts.at(i));
        }
    }

    SortDescs(const std::vector<int>& orders, const std::vector<int>& nulls) {
        DCHECK_EQ(orders.size(), nulls.size());
        descs.reserve(orders.size());
        for (int i = 0; i < orders.size(); i++) {
            descs.emplace_back(orders[i], nulls[i]);
        }
    }

    // Create a default desc with asc order and null_first
    static SortDescs asc_null_first(int columns) {
        SortDescs res;
        for (int i = 0; i < columns; i++) {
            res.descs.emplace_back(1, -1);
        }
        return res;
    }

    size_t num_columns() const { return descs.size(); }

    SortDesc get_column_desc(int col) const { return descs[col]; }
};

} // namespace starrocks
