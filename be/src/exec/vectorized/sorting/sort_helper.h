// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

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

#ifndef NDEBUG
static std::string dubug_column(const Column* column, const SmallPermutation& permutation) {
    std::string res;
    for (auto p : permutation) {
        res += fmt::format("{:>5}, ", column->debug_item(p.index_in_chunk));
    }
    return res;
}
#endif

// 1. Partition null and notnull values
// 2. Sort by not-null values
static inline void sort_and_tie_helper_nullable(NullableColumn* column, bool is_asc_order, bool is_null_first,
                                                SmallPermutation& permutation, Tie& tie, bool build_tie = true) {
    NullData& null_data = column->null_column_data();
    auto null_pred = [&](const SmallPermuteItem& item) -> bool {
        if (is_null_first) {
            return null_data[item.index_in_chunk] == 1;
        } else {
            return null_data[item.index_in_chunk] != 1;
        }
    };

#ifndef NDEBUG
    fmt::print("nullable column tie before sort: {}\n", fmt::join(tie, ","));
    fmt::print("nullable column before sort: {}\n", dubug_column(column, permutation));
#endif

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
                int pivot_start = pivot_iter - permutation.begin();
                tie[pivot_start] = 0;
                // TODO(mofei) reuse the global permutation
                int notnull_start = is_null_first ? pivot_start : range_first;
                int notnull_end = is_null_first ? range_last : pivot_start;
                SmallPermutation partial_perm(permutation.begin() + notnull_start, permutation.begin() + notnull_end);
                Tie partial_tie(tie.begin() + notnull_start, tie.begin() + notnull_end);

                column->data_column()->sort_and_tie(is_asc_order, is_null_first, partial_perm, partial_tie, build_tie);
                std::copy(partial_perm.begin(), partial_perm.end(), permutation.begin() + notnull_start);
                std::copy(partial_tie.begin(), partial_tie.end(), tie.begin() + notnull_start);
            }

#ifndef NDEBUG
            fmt::print("column after iteration: [{}, {}): {}\n", range_first, range_last,
                       dubug_column(column, permutation));
            fmt::print("tie after iteration: [{}, {}] {}\n", range_first, range_last, fmt::join(tie, ",    "));
#endif

            range_first = range_last;
        }
    }

#ifndef NDEBUG
    fmt::print("nullable column tie after sort: {}\n", fmt::join(tie, ",    "));
    fmt::print("nullable column after sort: {}\n", dubug_column(column, permutation));
#endif
}

template <class DataComparator>
static inline void sort_and_tie_helper(Column* column, bool is_asc_order, SmallPermutation& permutation, Tie& tie,
                                       DataComparator cmp, bool build_tie = true) {
    auto lesser = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };

    auto do_sort = [&](auto begin, auto end) {
        if (is_asc_order) {
            ::pdqsort(false, begin, end, lesser);
        } else {
            ::pdqsort(false, begin, end, greater);
        }
    };
#ifndef NDEBUG
    fmt::print("tie before sort: {}\n", fmt::join(tie, ","));
    fmt::print("column before sort: {}\n", dubug_column(column, permutation));
#endif

    if (SIMD::count_nonzero(tie) > 0) {
        // TODO(mofei) is there any optimization opportunity
        int range_first = 0;
        while (range_first < tie.size()) {
            int range_last = SIMD::find_zero(tie, range_first + 1);
            if (range_last > tie.size()) {
                break;
            } else if (range_last - range_first > 1) {
                do_sort(permutation.begin() + range_first, permutation.begin() + range_last);
                if (build_tie) {
                    tie[range_first] = 0;
                    for (int i = range_first + 1; i < range_last; i++) {
                        tie[i] = cmp(permutation[i - 1], permutation[i]) == 0;
                    }
                }
            }
#ifndef NDEBUG
            fmt::print("column after iteration: [{}, {}) {}\n", range_first, range_last,
                       dubug_column(column, permutation));
            fmt::print("tie after iteration: {}\n", fmt::join(tie, ",   "));
#endif
            range_first = range_last;
        }
    }
#ifndef NDEBUG
    fmt::print("tie after sort: {}\n", fmt::join(tie, ",   "));
    fmt::print("nullable column after sort: {}\n", dubug_column(column, permutation));
#endif
}

} // namespace starrocks::vectorized