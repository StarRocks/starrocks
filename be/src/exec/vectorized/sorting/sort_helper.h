// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized//sorting//sort_permute.h"
#include "runtime/timestamp_value.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {

// Comparator for sort
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

template <>
struct SorterComparator<Slice> {
    static int compare(const Slice& lhs, const Slice& rhs) { return lhs.compare(rhs); }
};

template <>
struct SorterComparator<DateValue> {
    static int compare(const DateValue& lhs, const DateValue& rhs) {
        auto x = lhs.julian() - rhs.julian();
        if (x == 0) {
            return x;
        } else {
            return x > 0 ? 1 : -1;
        }
    }
};

template <>
struct SorterComparator<TimestampValue> {
    static int compare(TimestampValue lhs, TimestampValue rhs) {
        auto x = lhs.timestamp() - rhs.timestamp();
        if (x == 0) {
            return x;
        } else {
            return x > 0 ? 1 : -1;
        }
    }
};

#ifndef NDEBUG
template <class PermutationType>
static std::string dubug_column(const Column* column, const PermutationType& permutation) {
    std::string res;
    for (auto p : permutation) {
        res += fmt::format("{:>5}, ", column->debug_item(p.index_in_chunk));
    }
    return res;
}
#endif

// 1. Partition null and notnull values
// 2. Sort by not-null values
static inline void sort_and_tie_helper_nullable(const bool& cancel, NullableColumn* column, bool is_asc_order,
                                                bool is_null_first, SmallPermutation& permutation, Tie& tie,
                                                std::pair<int, int> range, bool build_tie) {
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

    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (range_last - range_first > 1) {
            auto pivot_iter =
                    std::partition(permutation.begin() + range_first, permutation.begin() + range_last, null_pred);
            int pivot_start = pivot_iter - permutation.begin();
            int notnull_start = is_null_first ? pivot_start : range_first;
            int notnull_end = is_null_first ? range_last : pivot_start;

            if (notnull_start < notnull_end) {
                tie[pivot_start] = 0;
                column->data_column()->sort_and_tie(cancel, is_asc_order, is_null_first, permutation, tie,
                                                    {notnull_start, notnull_end}, build_tie);
            }
        }

#ifndef NDEBUG
        fmt::print("column after iteration: [{}, {}): {}\n", range_first, range_last,
                   dubug_column(column, permutation));
        fmt::print("tie after iteration: [{}, {}] {}\n", range_first, range_last, fmt::join(tie, ",    "));
#endif
    }

#ifndef NDEBUG
    fmt::print("nullable column tie after sort: {}\n", fmt::join(tie, ",    "));
    fmt::print("nullable column after sort: {}\n", dubug_column(column, permutation));
#endif
}

template <class DataComparator, class PermutationType>
static inline void sort_and_tie_helper(const bool& cancel, Column* column, bool is_asc_order,
                                       PermutationType& permutation, Tie& tie, DataComparator cmp,
                                       std::pair<int, int> range, bool build_tie) {
    auto lesser = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };
    auto do_sort = [&](auto begin, auto end) {
        if (is_asc_order) {
            ::pdqsort(cancel, begin, end, lesser);
        } else {
            ::pdqsort(cancel, begin, end, greater);
        }
    };
#ifndef NDEBUG
    fmt::print("tie before sort: {}\n", fmt::join(tie, ","));
    fmt::print("column before sort: {}\n", dubug_column(column, permutation));
    int tie_count = 0;
#endif

    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (range_last - range_first > 1) {
            do_sort(permutation.begin() + range_first, permutation.begin() + range_last);
            if (build_tie) {
                tie[range_first] = 0;
                for (int i = range_first + 1; i < range_last; i++) {
                    tie[i] &= cmp(permutation[i - 1], permutation[i]) == 0;
                }
            }
        }
#ifndef NDEBUG
        tie_count++;
        fmt::print("column after iteration: [{}, {}) {}\n", range_first, range_last, dubug_column(column, permutation));
        fmt::print("tie after iteration: {}\n", fmt::join(tie, ",   "));
#endif
    }

#ifndef NDEBUG
    fmt::print("tie({}) after sort: {}\n", tie_count, fmt::join(tie, ",   "));
    fmt::print("nullable column after sort: {}\n", dubug_column(column, permutation));
#endif
}

} // namespace starrocks::vectorized