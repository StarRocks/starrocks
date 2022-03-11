// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column.h"
#include "column/json_column.h"
#include "exec/vectorized/sorting/sort_permute.h"

namespace starrocks::vectorized {

// Merge two sorted column, result is written into permutation
// @param lhs left column
// @param lhs_range range of left column
// @param rhs right column
// @param rhs_range range of right oclumn
static inline void merge_sorted(Column* lhs, Column* rhs, int limit, Permutation& result, Tie& tie,
                                std::pair<int, int> lhs_range, std::pair<int, int> rhs_range) {
    if (limit == 0) {
        limit = lhs->size() + rhs->size();
    }
    limit = std::min(limit, (lhs_range.second - lhs_range.first) + (rhs_range.second - rhs_range.first));
    result.resize(limit);
    tie.resize(limit);

    const int lhs_end = lhs_range.second;
    const int rhs_end = rhs_range.second;
    int lhs_idx = lhs_range.first, rhs_idx = rhs_range.first;
    std::array<Column*, 2> runs{lhs, rhs};
    std::array<int*, 2> indices{&lhs_idx, &rhs_idx};
    int result_idx = 0;

    for (; result_idx < limit && lhs_idx < lhs_end && rhs_idx < rhs_end; result_idx++) {
        // TODO(mofei) optimize the compare
        int x = lhs->compare_at(lhs_idx, rhs_idx, *rhs, 1);

        // If less/equal, greater is 0, so copy left
        // If greater, greater is 1, so copy right
        int target_column = x <= 0 ? 0 : 1;
        result[result_idx].chunk_index = target_column;
        result[result_idx].index_in_chunk = *indices[target_column];

#ifndef NDEBUG
        fmt::print("merge the {} th item in {} column({})\n", *indices[target_column], target_column,
                   runs[target_column]->debug_string());
#endif

        lhs_idx += 1 - target_column;
        rhs_idx += target_column;
    }
    while (lhs_idx < lhs_end && result_idx < limit) {
        result[result_idx].chunk_index = 0;
        result[result_idx].index_in_chunk = lhs_idx;
        lhs_idx++;
        result_idx++;
    }
    while (rhs_idx < rhs_end && result_idx < limit) {
        result[result_idx].chunk_index = 1;
        result[result_idx].index_in_chunk = rhs_idx;
        rhs_idx++;
        result_idx++;
    }
    // TODO(mofei) optimize it
    for (int i = 1; i < limit; i++) {
        int this_run = result[i].chunk_index;
        int this_idx = result[i].index_in_chunk;
        int prev_run = result[i - 1].chunk_index;
        int prev_idx = result[i - 1].index_in_chunk;
        tie[i] = runs[this_run]->compare_at(this_idx, prev_idx, *runs[prev_run], 1) == 0;
    }
}

static inline void merge_sorted(Column* lhs, Column* rhs, int limit, Permutation& result, Tie& tie) {
    merge_sorted(lhs, rhs, limit, result, tie, {0, lhs->size()}, {0, rhs->size()});
}

struct TieIterator {
    const Tie& tie;
    const int begin;
    const int end;

    // For outer access
    int range_first;
    int range_last;

    TieIterator(const Tie& tie) : TieIterator(tie, 0, tie.size()) {}

    TieIterator(const Tie& tie, int begin, int end) : tie(tie), begin(begin), end(end) {
        range_first = begin;
        range_last = end;
        _inner_range_first = begin;
        _inner_range_last = end;
    }

    // Iterate the tie
    // Return false means the loop should terminate
    bool next() {
        if (_inner_range_first >= end) {
            return false;
        }
        _inner_range_first = SIMD::find_nonzero(tie, _inner_range_first + 1);
        if (range_first >= end) {
            return false;
        }
        _inner_range_first--;
        _inner_range_last = SIMD::find_zero(tie, _inner_range_first + 1);
        if (_inner_range_last > end) {
            return false;
        }

        range_first = _inner_range_first;
        range_last = _inner_range_last;
        _inner_range_first = _inner_range_last;
        return true;
    }

private:
    int _inner_range_first;
    int _inner_range_last;
};

// Merge two sorted chunks
// 1. Merge the first column, reorder the permutation, and generate tie for it
// 2. Incremental merge the second column, according to tie
// 3. Repeat the 2 step until last column
static inline void merge_sorted_chunks(const Columns& lhs, const Columns& rhs, Permutation& result) {
    DCHECK_EQ(lhs.size(), rhs.size());
    if (lhs.empty()) {
        return;
    }

    int num_columns = lhs.size();
    int num_rows = lhs[0]->size();
    Tie tie(num_rows, 0);

    for (int col_idx = 0; col_idx < num_columns; col_idx++) {
        Column* lhs_column = lhs[col_idx].get();
        Column* rhs_column = rhs[col_idx].get();
        if (col_idx == 0) {
            merge_sorted(lhs_column, rhs_column, 0, result, tie);
        } else {
            TieIterator iterator(tie);
            while (iterator.next()) {
                int first = iterator.range_first;
                int last = iterator.range_last;
                // TODO(mofei) optimize the search procedure
                auto pred_first_run = [](const PermutationItem& item) { return item.chunk_index == 0; };
                auto left_end = std::partition_point(result.begin() + first, result.end() + last, pred_first_run) -
                                result.begin();
                std::pair<int, int> lhs_range{0, lhs_column->size()};
                std::pair<int, int> rhs_range{0, rhs_column->size()};

                if (result[first].chunk_index == 0 && left_end != last) {
                    // [LLLLLLL | RRRRRR]
                    DCHECK_EQ(result[left_end].chunk_index, 1);
                    DCHECK_EQ(result[left_end - 1].chunk_index, 0);
                    lhs_range.second = result[left_end - 1].index_in_chunk;
                    rhs_range.first = result[left_end].index_in_chunk;
                } else if (left_end == last) {
                    // [LLLLLLL]
                    rhs_range.first = rhs_column->size();
                } else {
                    // [RRRRRR]
                    lhs_range.second = 0;
                }

                merge_sorted(lhs_column, rhs_column, 0, result, tie, lhs_range, rhs_range);

#ifndef NDEBUG
                fmt::print("merge left[{}, {}), right[{}, {})\n", lhs_range.first, lhs_range.second, rhs_range.first,
                           rhs_range.second);
#endif
            }
        }
    }
}

} // namespace starrocks::vectorized