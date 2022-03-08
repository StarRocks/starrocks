// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized//sorting//sort_permute.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {

template <class T>
struct SorterComparator {
    static int compare(const T& lhs, const T& rhs) {
        if (lhs == rhs) {
            return 0;
        } else if (lhs < rhs) {
            return -1;
        } else {
            return 1;
        }
    }
};

template <bool null_first>
struct NullPredicate {
    bool operator()(uint8_t null_flag) {
        if constexpr (null_first) {
            return null_flag == 1;
        } else {
            return null_flag != 1;
        }
    }
};

// 1. Partition null and notnull values
// 2. Sort by not-null values
static inline void sort_and_tie_helper_nullable(NullableColumn* column, bool is_asc_order, bool is_null_first,
                                                Permutation& permutation, Tie& tie) {
    NullData& null_data = column->null_column_data();
    auto null_pred = [&](const PermutationItem& item) -> bool {
        if (is_null_first) {
            return null_data[item.index_in_chunk] == 1;
        } else {
            return null_data[item.index_in_chunk] != 1;
        }
    };

    fmt::print("nullable column tie before sort: {}\n", fmt::join(tie, ","));

    // sort ranges
    if (SIMD::count_nonzero(tie) > 0) {
        // TODO(mofei) is there any optimization opportunity
        int range_first = 0;
        while (range_first < tie.size()) {
            int range_last = SIMD::find_zero(tie, range_first + 1);

            if (range_last > tie.size()) {
                break;
            } else if (range_last - range_first > 1) {
                auto pivot_iter =
                        std::partition(permutation.begin() + range_first, permutation.begin() + range_last, null_pred);
                int pivot_start = range_first + pivot_iter - permutation.begin();
                tie[pivot_start] = 0;
                // TODO(mofei) reuse the global permutation
                Permutation partial_perm;
                Tie partial_tie;
                if (is_null_first) {
                    partial_perm.assign(pivot_iter, permutation.begin() + range_last);
                    partial_tie.assign(tie.begin() + pivot_start, tie.begin() + range_last);

                    column->data_column()->sort_and_tie(is_asc_order, false, partial_perm, partial_tie);
                    std::copy(partial_perm.begin(), partial_perm.end(), pivot_iter);
                    std::copy(partial_tie.begin(), partial_tie.end(), tie.begin() + pivot_start);
                    std::fill(tie.begin(), tie.begin() + pivot_start, 1);
                } else {
                    partial_perm.assign(permutation.begin() + range_first, pivot_iter);
                    partial_tie.assign(tie.begin() + range_first, tie.begin() + pivot_start);

                    column->data_column()->sort_and_tie(is_asc_order, false, partial_perm, partial_tie);
                    std::copy(partial_perm.begin(), partial_perm.end(), permutation.begin());
                    std::copy(partial_tie.begin(), partial_tie.end(), tie.begin());
                    std::fill(tie.begin() + pivot_start, tie.begin() + range_last, 1);
                }
            }
            range_first = range_last;
        }
    }
    fmt::print("nullable column tie after sort: {}\n", fmt::join(tie, ","));
}

template <class DataComparator>
static inline void sort_and_tie_helper(Column* column, bool is_asc_order, Permutation& permutation, Tie& tie,
                                       DataComparator cmp) {
    auto lesser = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };

    auto do_sort = [&](auto begin, auto end) {
        if (is_asc_order) {
            ::pdqsort(false, begin, end, lesser);
        } else {
            ::pdqsort(false, begin, end, greater);
        }
    }; // sort ranges
    fmt::print("tie before sort: {}\n", fmt::join(tie, ","));
    if (SIMD::count_nonzero(tie) > 0) {
        // TODO(mofei) is there any optimization opportunity
        int range_first = 0;
        while (range_first < tie.size()) {
            int range_last = SIMD::find_zero(tie, range_first + 1);
            if (range_last > tie.size()) {
                break;
            } else if (range_last - range_first > 1) {
                do_sort(permutation.begin() + range_first, permutation.begin() + range_last);
                for (int i = range_first + 1; i < range_last; i++) {
                    tie[i] = cmp(permutation[i - 1], permutation[i]) == 0;
                }
            }
            range_first = range_last;
        }
    }
    fmt::print("tie after sort: {}\n", fmt::join(tie, ","));

    // build tie
    // for (int i = 1; i < permutation.size(); i++) {
    // tie[i] = cmp(permutation[i - 1], permutation[i]) == 0;
    // }
}

} // namespace starrocks::vectorized