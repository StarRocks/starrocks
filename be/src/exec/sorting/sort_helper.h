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
#include <boost/mpl/size.hpp>
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

template <typename T>
concept FloatingPoint = std::is_floating_point_v<T>;

template <FloatingPoint T>
struct SorterComparator<T> {
    static int compare(T lhs, T rhs) {
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
                                                           size_t* limited, bool is_dense_rank_topn) {
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
    RETURN_IF_ERROR(sort_vertical_columns(cancel, data_columns, sort_desc, permutation, tie, range, build_tie, limit,
                                          limited, is_dense_rank_topn));

    return Status::OK();
}

/// Partition null and notnull values, and will update `permutation` and `tie`.
///
/// The sorting of nullable column is divided into two steps: at first partition the null elements and non-null element,
/// then sort them individually.
///
/// A prerequisite for sorting null element is that the datum of null element should be the same. Because when sorting
/// the next column, we need to ensure that the entire null part is still in the same `tie` range (that is, only the
/// beginning or end is 0, and the rest are 1). If the datum values of these nulls are different, then the `tie` of the
/// null part may have non-1 values, causing the next column to be sorted incorrectly.
/// For example, given two columns `(null(3), null(2), null(1)), (1, 2, 3)`, if the datum values of null elements are
/// not set to the same, the sorting result is `(null(1), null(2), null(3)), (3, 2, 1)`, but the expected result
/// should be (null(3), null(2), null(1)), (1, 2, 3).
template <class NullPred>
static inline Status partition_null_and_nonnull_helper(const std::atomic<bool>& cancel, NullPred null_pred,
                                                       const SortDesc& sort_desc, SmallPermutation& permutation,
                                                       Tie& tie, std::pair<int, int> range) {
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

    return Status::OK();
}

// 1. Partition null and notnull values
// 2. Sort by not-null values
template <class NullPred>
static inline Status sort_and_tie_helper_nullable(const std::atomic<bool>& cancel, const NullableColumn* column,
                                                  const ColumnPtr& data_column, NullPred null_pred,
                                                  const SortDesc& sort_desc, SmallPermutation& permutation, Tie& tie,
                                                  std::pair<int, int> range, bool build_tie) {
    RETURN_IF_ERROR(partition_null_and_nonnull_helper(cancel, null_pred, sort_desc, permutation, tie, range));

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

// for every equal range in permutation[range.begin,range.end), sort them and rebuild tie from [range.begin,range.end)
// if limit is not zero and  is in equal range, we can just get the top 'limit' element instead of sort the whole equal range
// note:tie's equal range is derived from the former sort result by previous column, so we only need sort the values that are equal on previous columns
template <class DataComparator, class PermutationType>
static inline Status sort_and_tie_helper_for_dense_rank(const std::atomic<bool>& cancel, const Column* column,
                                                        bool is_asc_order, PermutationType& permutation, Tie& tie,
                                                        DataComparator cmp, std::pair<int, int> range, size_t limit,
                                                        size_t* limited, size_t* result_distinct_top_n,
                                                        bool is_sorted) {
    DCHECK(result_distinct_top_n != nullptr);
    DCHECK(limited != nullptr);
    auto lesser = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) < 0; };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };

    // permutation[range_first,range_last) is current equal range, distinct_top_n is current distinct top n before this equal range
    // this function will try to find enough distinct top values so total distinct_top_n >= limit
    auto do_topn_for_dense_rank = [&](size_t first_iter, size_t last_iter, size_t distinct_top_n) -> size_t {
        auto begin = permutation.begin() + first_iter;
        auto end = permutation.begin() + last_iter;

        // only try 3 times to find enough distinct elements, otherwise fall back to full sort
        size_t begin_index = 0;
        // if we find 'limit' + 1 distinct topn, then it's safe to get 'limit' distinct topn
        // for example: 1 1 1 2 2 2 3 3 4, limit = 2, if we only see 1 1 1 2 2, we may not get enough number
        // but if we see 1 1 1 2 2 2 3, which means we get 'limit' + 1 distinct topn, then 1 1 1 2 2 2 is enough
        if (!is_sorted) {
            size_t number_to_look_up = limit - distinct_top_n + 1;
            for (size_t i = 0; i < 3; i++) {
                // in this case, no need to try, just full back to full sort
                if (end - begin < number_to_look_up) break;
                // only get top n
                if (is_asc_order) {
                    std::partial_sort(begin, begin + number_to_look_up, end, lesser);
                } else {
                    std::partial_sort(begin, begin + number_to_look_up, end, greater);
                }
                // build the tie and count distinct for [first_iter + begin_index, first_iter + begin_index + n)
                for (auto j = first_iter + begin_index; j < first_iter + begin_index + number_to_look_up; j++) {
                    // equal range's begin
                    if (j == first_iter) {
                        tie[j] = 0;
                        distinct_top_n++;
                        if (distinct_top_n == limit + 1) {
                            *limited = first_iter;
                            return distinct_top_n - 1;
                        }
                    } else {
                        if (cmp(permutation[j - 1], permutation[j]) != 0) {
                            tie[j] = 0;
                            distinct_top_n++;
                            if (distinct_top_n == limit + 1) {
                                *limited = first_iter + j;
                                return distinct_top_n - 1;
                            }
                        } else {
                            tie[j] = 1;
                        }
                    }
                }

                begin_index += number_to_look_up;
                // make sure begin = permutation.begin() + begin_indx
                begin += number_to_look_up;
            }
        }

        // fall back the left data to full sort
        if (first_iter + begin_index < last_iter) {
            if (!is_sorted) {
                if (is_asc_order) {
                    ::pdqsort(begin, end, lesser);
                } else {
                    ::pdqsort(begin, end, greater);
                }
            }

            // build the tie and count distinct
            for (auto j = first_iter + begin_index; j < last_iter; j++) {
                if (UNLIKELY(j == first_iter)) {
                    tie[j] = 0;
                    distinct_top_n++;
                    if (distinct_top_n == limit + 1) {
                        *limited = j;
                        return distinct_top_n - 1;
                    }
                } else {
                    if (cmp(permutation[j - 1], permutation[j]) != 0) {
                        tie[j] = 0;
                        distinct_top_n++;
                        if (distinct_top_n == limit + 1) {
                            *limited = j;
                            return distinct_top_n - 1;
                        }
                    } else {
                        tie[j] = 1;
                    }
                }
            }

            // at the end of this equal range, if distinct_top_n == limit, it's also safe to set limited
            if (distinct_top_n == limit) {
                *limited = last_iter;
            }
            DCHECK(distinct_top_n <= limit);
        }

        return distinct_top_n;
    };

    DCHECK(limit > 0);
    DCHECK(limited != nullptr);

    // iterate range's every equal range to find at least 'limit' distinct elements
    TieIterator iterator(tie, range.first, range.second);
    size_t distinct_top_n = 0;
    while (iterator.next()) {
        if (UNLIKELY(cancel.load(std::memory_order_acquire))) {
            return Status::Cancelled("Sort cancelled");
        }
        // current equal range is [range_first,range_last)
        int range_first = iterator.range_first;
        int range_last = iterator.range_last;

        // current equal range is not empty
        if (LIKELY(range_last - range_first >= 1)) {
            distinct_top_n = do_topn_for_dense_rank(range_first, range_last, distinct_top_n);
        }

        if (distinct_top_n >= limit) break;
    }

    // if only one row in tie, TieIterator will return false directly
    if (distinct_top_n == 0 && range.second > range.first) {
        distinct_top_n = 1;
    }

    *result_distinct_top_n = distinct_top_n;

    // if range is not empty, then distinct_top_n > 0
    DCHECK(range.second <= range.first || distinct_top_n > 0);
    DCHECK(distinct_top_n <= limit) << "distinct_top_n: " << distinct_top_n << ", limit: " << limit;
    return Status::OK();
}

// for every equal range in permutation[range.begin,range.end), sort them and rebuild tie from [range.begin,range.end)
// if limit is not zero and  is in equal range, we can only get the top limit element instread of sort the whole equal range
template <class DataComparator, class PermutationType>
static inline Status sort_and_tie_helper(const std::atomic<bool>& cancel, const Column* column, bool is_asc_order,
                                         PermutationType& permutation, Tie& tie, DataComparator cmp,
                                         std::pair<int, int> range, bool build_tie, size_t limit = 0,
                                         size_t* limited = nullptr, bool is_dense_rank_topn = false,
                                         size_t* distinct_top_n = nullptr, bool is_sorted = false) {
    if (is_dense_rank_topn)
        return sort_and_tie_helper_for_dense_rank<DataComparator, PermutationType>(
                cancel, column, is_asc_order, permutation, tie, cmp, range, limit, limited, distinct_top_n, is_sorted);

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
            if (!is_sorted) {
                do_sort(range_first, range_last);
            }
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
