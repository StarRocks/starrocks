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

#include <algorithm>
#include <concepts>

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "types/timestamp_value.h"
#include "util/json.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

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

template <>
struct SorterComparator<float> {
    static int compare(float lhs, float rhs) {
        lhs = std::isnan(lhs) ? 0 : lhs;
        rhs = std::isnan(rhs) ? 0 : rhs;
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
struct SorterComparator<double> {
    static int compare(double lhs, double rhs) {
        lhs = std::isnan(lhs) ? 0 : lhs;
        rhs = std::isnan(rhs) ? 0 : rhs;
        if (lhs == rhs) {
            return 0;
        } else if (lhs < rhs) {
            return -1;
        } else {
            return 1;
        }
    }
};

// TODO: reduce duplicate code
template <class NullPred>
static inline Status sort_and_tie_helper_nullable_vertical(const std::atomic<bool>& cancel,
                                                           const std::vector<ColumnPtr>& data_columns,
                                                           NullPred null_pred, const SortDesc& sort_desc,
                                                           Permutation& permutation, Tie& tie,
                                                           std::pair<int, int> range, bool build_tie, size_t limit,
                                                           size_t* limited) {
    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        if (UNLIKELY(cancel.load(std::memory_order_acquire))) {
            return Status::Cancelled("Sort cancelled");
        }
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (UNLIKELY(limit > 0 && range_first > limit)) {
            *limited = std::min(*limited, (size_t)range_first);
            break;
        }
        if (LIKELY(range_last - range_first > 1)) {
            auto pivot_iter =
                    std::partition(permutation.begin() + range_first, permutation.begin() + range_last, null_pred);
            int pivot_start = pivot_iter - permutation.begin();
            std::pair<size_t, size_t> null_range = {range_first, pivot_start};
            std::pair<size_t, size_t> notnull_range = {pivot_start, range_last};
            if (!sort_desc.is_null_first()) {
                std::swap(null_range, notnull_range);
            }

            // For null values, limit could only be pruned to end of null
            if (UNLIKELY(limit > 0 && null_range.first < limit && limit <= null_range.second)) {
                *limited = std::min(*limited, null_range.second);
            }

            if (notnull_range.first < notnull_range.second) {
                tie[notnull_range.first] = 0;
            }
            if (range_first <= null_range.first && null_range.first < range_last) {
                // Mark all null as equal
                std::fill(tie.begin() + null_range.first, tie.begin() + null_range.second, 1);

                // Cut off null and non-null
                tie[null_range.first] = 0;
            }
        }
    }

    // TODO(Murphy): avoid sort the null datums in the column
    RETURN_IF_ERROR(
            sort_vertical_columns(cancel, data_columns, sort_desc, permutation, tie, range, build_tie, limit, limited));

    return Status::OK();
}

// 1. Partition null and notnull values
// 2. Sort by not-null values
template <class NullPred>
static inline Status sort_and_tie_helper_nullable(const std::atomic<bool>& cancel, const NullableColumn* column,
                                                  const ColumnPtr& data_column, NullPred null_pred,
                                                  const SortDesc& sort_desc, SmallPermutation& permutation, Tie& tie,
                                                  std::pair<int, int> range, bool build_tie) {
    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        if (UNLIKELY(cancel.load(std::memory_order_acquire))) {
            return Status::Cancelled("Sort cancelled");
        }
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (LIKELY(range_last - range_first > 1)) {
            auto pivot_iter =
                    std::partition(permutation.begin() + range_first, permutation.begin() + range_last, null_pred);
            int pivot_start = pivot_iter - permutation.begin();
            std::pair<int, int> null_range = {range_first, pivot_start};
            std::pair<int, int> notnull_range = {pivot_start, range_last};
            if (!sort_desc.is_null_first()) {
                std::swap(null_range, notnull_range);
            }

            if (notnull_range.first < notnull_range.second) {
                tie[notnull_range.first] = 0;
            }
            if (range_first <= null_range.first && null_range.first < range_last) {
                // Mark all null datum as equal
                std::fill(tie.begin() + null_range.first, tie.begin() + null_range.second, 1);

                // Cut off null and non-null
                tie[null_range.first] = 0;
            }
        }
    }

    // TODO(Murphy): avoid sort the null datums in the column, eliminate the extra overhead
    // Some benchmark numbers:
    // --------------------------------------------------------------------------------------------------------------------------------------
    // Benchmark                                         Time             CPU   Iterations  data_size items_per_second  mem_usage rows_sorted
    // --------------------------------------------------------------------------------------------------------------------------------------
    // BM_fullsort_column_incr/64/4               44942516 ns     44904324 ns           13    4.1943M       5.83783M/s   10.5021M    3.40787M
    // BM_fullsort_column_incr/512/4             527808528 ns    527777351 ns            1   33.5544M       3.97355M/s   83.9025M    2.09715M
    // BM_fullsort_column_incr/4096/4           6197780685 ns   6197019209 ns            1   268.435M        2.7073M/s   671.105M    16.7772M
    // BM_fullsort_column_incr/32768/4          46811357585 ns   46799493931 ns            1   2.14748G       2.86793M/s   5.36873G    134.218M
    // BM_fullsort_column_incr_nullable/64/4      49548060 ns     49545725 ns           14   5.24288M       5.29095M/s   11.5507M    3.67002M
    // BM_fullsort_column_incr_nullable/512/4    568248214 ns    568200327 ns            1    41.943M       3.69087M/s   92.2911M    2.09715M
    // BM_fullsort_column_incr_nullable/4096/4  5816112918 ns   5815603071 ns            1   335.544M       2.88486M/s   738.214M    16.7772M
    // BM_fullsort_column_incr_nullable/32768/4 60430519397 ns   60424234298 ns            1   2.68435G       2.22126M/s    5.9056G    134.218M
    RETURN_IF_ERROR(sort_and_tie_column(cancel, data_column, sort_desc, permutation, tie, range, build_tie));

    return Status::OK();
}

template <class DataComparator, class PermutationType>
static inline Status sort_and_tie_helper(const std::atomic<bool>& cancel, const Column* column, bool is_asc_order,
                                         PermutationType& permutation, Tie& tie, DataComparator cmp,
                                         std::pair<int, int> range, bool build_tie, size_t limit = 0,
                                         size_t* limited = nullptr) {
    auto lesser = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };
    auto do_sort = [&](size_t first_iter, size_t last_iter) {
        auto begin = permutation.begin() + first_iter;
        auto end = permutation.begin() + last_iter;

        // If this range could be used to prune the limit, use partial sort and calculate the end of limit
        if (UNLIKELY(limit > 0 && first_iter < limit && limit <= last_iter)) {
            int n = limit - first_iter;
            if (is_asc_order) {
                std::partial_sort(begin, begin + n, end, lesser);
            } else {
                std::partial_sort(begin, begin + n, end, greater);
            }

            // Find if any element equal with the nth element, take it into account of this run
            auto nth = permutation[limit - 1];
            size_t equal_count = 0;
            for (auto iter = begin + n; iter < end; iter++) {
                if (cmp(*iter, nth) == 0) {
                    std::iter_swap(iter, begin + n + equal_count);
                    equal_count++;
                }
            }
            *limited = limit + equal_count;
        } else {
            if (is_asc_order) {
                ::pdqsort(begin, end, lesser);
            } else {
                ::pdqsort(begin, end, greater);
            }
        }
    };

    TieIterator iterator(tie, range.first, range.second);
    while (iterator.next()) {
        if (UNLIKELY(cancel.load(std::memory_order_acquire))) {
            return Status::Cancelled("Sort cancelled");
        }
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        if (UNLIKELY(limit > 0 && range_first > limit)) {
            break;
        }

        if (LIKELY(range_last - range_first > 1)) {
            do_sort(range_first, range_last);
            if (build_tie) {
                tie[range_first] = 0;
                for (int i = range_first + 1; i < range_last; i++) {
                    tie[i] &= cmp(permutation[i - 1], permutation[i]) == 0;
                }
            }
        }
    }

    return Status::OK();
}

static inline int compare_chunk_row(const SortDescs& desc, const Columns& lhs, const Columns& rhs, size_t lhs_row,
                                    size_t rhs_row) {
    DCHECK_EQ(lhs.size(), rhs.size());
    DCHECK_LE(desc.num_columns(), lhs.size());
    DCHECK_LE(desc.num_columns(), rhs.size());

    int num_columns = desc.num_columns();
    for (int i = 0; i < num_columns; i++) {
        auto& lhs_column = lhs[i];
        auto& rhs_column = rhs[i];
        int x = lhs_column->compare_at(lhs_row, rhs_row, *rhs_column, desc.get_column_desc(i).null_first);
        if (x != 0) {
            return x * desc.get_column_desc(i).sort_order;
        }
    }
    return 0;
}

} // namespace starrocks
