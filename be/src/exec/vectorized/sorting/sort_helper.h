// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized//sorting//sort_permute.h"
#include "exec/vectorized//sorting//sorting.h"
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
    static int compare(const Slice& lhs, const Slice& rhs) {
        int x = lhs.compare(rhs);
        if (x > 0) return 1;
        if (x < 0) return -1;
        return x;
    }
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

template <class PermutationType>
static std::string dubug_column(const Column* column, const PermutationType& permutation) {
    std::string res;
    for (auto p : permutation) {
        res += fmt::format("{:>5}, ", column->debug_item(p.index_in_chunk));
    }
    return res;
}

// 1. Partition null and notnull values
// 2. Sort by not-null values
static inline Status sort_and_tie_helper_nullable(const bool& cancel, const NullableColumn* column, bool is_asc_order,
                                                  bool is_null_first, SmallPermutation& permutation, Tie& tie,
                                                  std::pair<int, int> range, bool build_tie) {
    const NullData& null_data = column->immutable_null_column_data();
    auto null_pred = [&](const SmallPermuteItem& item) -> bool {
        if (is_null_first) {
            return null_data[item.index_in_chunk] == 1;
        } else {
            return null_data[item.index_in_chunk] != 1;
        }
    };

    VLOG(2) << fmt::format("nullable column tie before sort: {}\n", fmt::join(tie, ","));
    VLOG(2) << fmt::format("nullable column before sort: {}\n", dubug_column(column, permutation));

    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (range_last - range_first > 1) {
            auto pivot_iter =
                    std::partition(permutation.begin() + range_first, permutation.begin() + range_last, null_pred);
            int pivot_start = pivot_iter - permutation.begin();
            int notnull_start, notnull_end;
            int null_start, null_end;
            if (is_null_first) {
                null_start = range_first;
                null_end = pivot_start;
                notnull_start = pivot_start;
                notnull_end = range_last;
            } else {
                notnull_start = range_first;
                notnull_end = pivot_start;
                null_start = pivot_start;
                null_end = range_last;
            }

            if (notnull_start < notnull_end) {
                tie[notnull_start] = 0;
                RETURN_IF_ERROR(sort_and_tie_column(cancel, column->data_column(), is_asc_order, is_null_first,
                                                    permutation, tie, {notnull_start, notnull_end}, build_tie));
            }
            if (range_first <= null_start && null_start < range_last) {
                // Mark all null as 0, they don
                std::fill(tie.begin() + null_start, tie.begin() + null_end, 1);

                // Cut off null and non-null
                tie[null_start] = 0;
            }
        }

        VLOG(3) << fmt::format("column after iteration: [{}, {}): {}\n", range_first, range_last,
                               dubug_column(column, permutation));
        VLOG(3) << fmt::format("tie after iteration: [{}, {}] {}\n", range_first, range_last, fmt::join(tie, ",    "));
    }

    VLOG(2) << fmt::format("nullable column tie after sort: {}\n", fmt::join(tie, ",    "));
    VLOG(2) << fmt::format("nullable column after sort: {}\n", dubug_column(column, permutation));

    return Status::OK();
}

template <class DataComparator, class PermutationType>
static inline Status sort_and_tie_helper(const bool& cancel, const Column* column, bool is_asc_order,
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

    VLOG(2) << fmt::format("tie before sort: {}\n", fmt::join(tie, ","));
    VLOG(2) << fmt::format("column before sort: {}\n", dubug_column(column, permutation));

    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        if (UNLIKELY(cancel)) {
            return Status::Cancelled("Sort cancelled");
        }
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

        VLOG(3) << fmt::format("column after iteration: [{}, {}) {}\n", range_first, range_last,
                               dubug_column(column, permutation));
        VLOG(3) << fmt::format("tie after iteration: {}\n", fmt::join(tie, ",   "));
    }

    VLOG(2) << fmt::format("tie after sort: {}\n", fmt::join(tie, ",   "));
    VLOG(2) << fmt::format("nullable column after sort: {}\n", dubug_column(column, permutation));
    return Status::OK();
}

// Compare a column with datum, store result into a vector
// For column-wise compare, only consider rows that are equal at previous columns
// @return number of rows that are equal
template <class DataComparator>
inline int compare_row_helper(CompareVector& cmp_vector, DataComparator cmp) {
    // For sparse array, use SIMD to skip data
    // For dense array, just iterate all bytes

    int equal_count = 0;
    if (SIMD::count_zero(cmp_vector) > cmp_vector.size() / 8) {
        for (size_t i = 0; i < cmp_vector.size(); i++) {
            // Only consider rows that are queal at previous columns
            if (cmp_vector[i] == 0) {
                cmp_vector[i] = cmp(i);
                equal_count += (cmp_vector[i] == 0);
            }
        }
    } else {
        int idx = 0;
        while (true) {
            int pos = SIMD::find_zero(cmp_vector, idx);
            if (pos >= cmp_vector.size()) {
                break;
            }

            cmp_vector[pos] = cmp(pos);
            equal_count += (cmp_vector[pos] == 0);
            idx = pos + 1;
        }
    }

    return equal_count;
}

} // namespace starrocks::vectorized