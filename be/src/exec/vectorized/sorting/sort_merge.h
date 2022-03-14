// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/column.h"
#include "column/json_column.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

struct PermutatedChunk {
    ChunkPtr chunk;
    Permutation perm;
    bool sorted = false;

    PermutatedChunk() = default;
    PermutatedChunk(ChunkPtr in_chunk, const Permutation& in_perm) : chunk(in_chunk), perm(in_perm) {}
    PermutatedChunk(ChunkPtr in_chunk) : chunk(in_chunk) {
        int rows = in_chunk->num_rows();
        perm.resize(rows);
        for (int i = 0; i < rows; i++) {
            perm[i].index_in_chunk = i;
        }
    }
    PermutatedChunk(const PermutatedChunk& other) : chunk(other.chunk), perm(other.perm) {}
    PermutatedChunk(PermutatedChunk&& other) : chunk(std::move(other.chunk)), perm(std::move(other.perm)) {}

    size_t num_rows() const { return chunk->num_rows(); }
    size_t num_columns() const { return chunk->num_columns(); }
    const ColumnPtr get_column(int col) const { return chunk->get_column_by_index(col); }
    void resize(int rows) {
        chunk->set_num_rows(rows);
        perm.resize(rows);
    }

    void filter(const std::vector<uint8_t>& filter) {
        size_t new_rows = SIMD::count_nonzero(filter);
        chunk->filter(filter);
        perm.resize(new_rows);
        for (int i = 0; i < perm.size(); i++) {
            perm[i].index_in_chunk = i;
        }
    }

    PermutatedColumn get_permutated_column(int col) const { return PermutatedColumn(*get_column(col), perm); }
};

struct MergeResult {
    static ChunkPtr create(const PermutatedChunk& lhs, const PermutatedChunk& rhs, const Permutation& merged) {
        std::unique_ptr<Chunk> res = lhs.chunk->clone_empty();

        // TODO: optimize performance
        std::vector<ChunkPtr> chunks{lhs.chunk, rhs.chunk};
        res->append_permutation(chunks, merged);

        return ChunkPtr(res.release());
    }
};
// Combine multiple columns according to permutation
static inline ColumnPtr unpermute_column(const Columns& columns, const Permutation& perm, int limit) {
    if (columns.empty()) {
        return {};
    }
    auto res = columns[0]->clone_empty();
    for (int i = 0; i < limit; i++) {
        auto p = perm[i];
        DCHECK_LT(p.chunk_index, columns.size());
        res->append_datum(columns[p.chunk_index]->get(p.index_in_chunk));
    }
    return res;
}

// Combine multiple columns according to permutation
static inline ColumnPtr unpermute_column(const std::vector<const Column*>& columns, const Permutation& perm,
                                         int limit) {
    if (columns.empty()) {
        return {};
    }
    auto res = columns[0]->clone_empty();
    for (int i = 0; i < limit; i++) {
        auto p = perm[i];
        DCHECK_LT(p.chunk_index, columns.size());
        res->append_datum(columns[p.chunk_index]->get(p.index_in_chunk));
    }
    return res;
}

static inline void init_permutation(Permutation& perm, int rows) {
    perm.resize(rows);
    for (int i = 0; i < rows; i++) {
        perm[i].index_in_chunk = i;
    }
}

static inline std::string print_permutation(const Permutation& perm) {
    std::string res;
    res += "[";
    for (int i = 0; i < perm.size(); i++) {
        res += fmt::format("{}:{},", perm[i].chunk_index, perm[i].index_in_chunk);
    }
    res += "]";
    return res;
}

template <class Comparator>
static inline void merge_sorted_impl(const PermutatedColumn& lhs, const PermutatedColumn& rhs, Permutation& result,
                                     Tie& tie, std::pair<int, int> lhs_range, std::pair<int, int> rhs_range,
                                     Comparator cmp, int output_idx, int limit, int null_first) {
    DCHECK_GE(lhs_range.first, 0);
    DCHECK_GE(rhs_range.first, 0);
    DCHECK_LE(lhs_range.second, lhs.size());
    DCHECK_LE(rhs_range.second, rhs.size());

#ifndef NDEBUG
    fmt::print("start merge {} {}, output_idx= {}, result={}\n", lhs.debug_string(lhs_range),
               rhs.debug_string(rhs_range), output_idx, print_permutation(result));
#endif

    const int lhs_end = lhs_range.second;
    const int rhs_end = rhs_range.second;
    int lhs_idx = lhs_range.first, rhs_idx = rhs_range.first;
    std::array<int*, 2> indices{&lhs_idx, &rhs_idx};
    int result_idx = output_idx;
    int result_len = (lhs_range.second - lhs_range.first) + (rhs_range.second - rhs_range.first);
    if (output_idx == 0) {
        result.resize(output_idx + result_len);
        tie.resize(output_idx + result_len, 1);
    }

    for (; lhs_idx < lhs_end && rhs_idx < rhs_end; result_idx++) {
        // TODO(mofei) optimize the compare
        int x = cmp(0, lhs_idx, 1, rhs_idx);
        // int x = lhs.compare_at(lhs_idx, rhs_idx, rhs, sort_order, null_first);
        int target_column = x <= 0 ? 0 : 1;
        result[result_idx].chunk_index = target_column;
        result[result_idx].index_in_chunk = *indices[target_column];

        lhs_idx += 1 - target_column;
        rhs_idx += target_column;
    }
    while (lhs_idx < lhs_end) {
        result[result_idx].chunk_index = 0;
        result[result_idx].index_in_chunk = lhs_idx;
        lhs_idx++;
        result_idx++;
    }
    while (rhs_idx < rhs_end) {
        result[result_idx].chunk_index = 1;
        result[result_idx].index_in_chunk = rhs_idx;
        rhs_idx++;
        result_idx++;
    }

    // TODO(mofei) optimize it
    int total_end = output_idx + result_len;
    int limit_end = (limit > 0 && output_idx == 0) ? (output_idx + limit) : (output_idx + result_len);

    for (int i = output_idx + 1; i < total_end; i++) {
        int this_run = result[i].chunk_index;
        int this_idx = result[i].index_in_chunk;
        int prev_run = result[i - 1].chunk_index;
        int prev_idx = result[i - 1].index_in_chunk;
        int x = cmp(prev_run, prev_idx, this_run, this_idx);
        tie[i] &= x;

        // Consider limit
        if (i >= limit_end && x != 1) {
            result.resize(i);
            tie.resize(i);
            break;
        }
    }
}

// Merge two sorted column, result is written into permutation
// @param lhs left column
// @param lhs_range range of left column
// @param rhs right column
// @param rhs_range range of right oclumn
static inline void merge_sorted(const PermutatedColumn& lhs, const PermutatedColumn& rhs, Permutation& result, Tie& tie,
                                std::pair<int, int> lhs_range, std::pair<int, int> rhs_range, int sort_order,
                                int null_first, int limit, int output_idx) {
    DCHECK_GE(lhs_range.first, 0);
    DCHECK_GE(rhs_range.first, 0);
    DCHECK_LE(lhs_range.second, lhs.size());
    DCHECK_LE(rhs_range.second, rhs.size());

#ifndef NDEBUG
    fmt::print("start merge {} {}, output_idx= {}, result={}\n", lhs.debug_string(lhs_range),
               rhs.debug_string(rhs_range), output_idx, print_permutation(result));
#endif

    const int lhs_end = lhs_range.second;
    const int rhs_end = rhs_range.second;
    int lhs_idx = lhs_range.first, rhs_idx = rhs_range.first;
    std::array<const Column*, 2> runs{&lhs.column, &rhs.column};
    std::array<int*, 2> indices{&lhs_idx, &rhs_idx};
    int result_idx = output_idx;
    int result_len = (lhs_range.second - lhs_range.first) + (rhs_range.second - rhs_range.first);
    if (output_idx == 0) {
        result.resize(output_idx + result_len);
        tie.resize(output_idx + result_len, 1);
    }

    for (; lhs_idx < lhs_end && rhs_idx < rhs_end; result_idx++) {
        // TODO(mofei) optimize the compare
        int x = lhs.compare_at(lhs_idx, rhs_idx, rhs, sort_order, null_first);
        int target_column = x <= 0 ? 0 : 1;
        result[result_idx].chunk_index = target_column;
        result[result_idx].index_in_chunk = *indices[target_column];

        lhs_idx += 1 - target_column;
        rhs_idx += target_column;
    }
    while (lhs_idx < lhs_end) {
        result[result_idx].chunk_index = 0;
        result[result_idx].index_in_chunk = lhs_idx;
        lhs_idx++;
        result_idx++;
    }
    while (rhs_idx < rhs_end) {
        result[result_idx].chunk_index = 1;
        result[result_idx].index_in_chunk = rhs_idx;
        rhs_idx++;
        result_idx++;
    }

    // TODO(mofei) optimize it
    int total_end = output_idx + result_len;
    int limit_end = (limit > 0 && output_idx == 0) ? (output_idx + limit) : (output_idx + result_len);

    for (int i = output_idx + 1; i < total_end; i++) {
        int this_run = result[i].chunk_index;
        int this_idx = result[i].index_in_chunk;
        int prev_run = result[i - 1].chunk_index;
        int prev_idx = result[i - 1].index_in_chunk;
        int x = runs[this_run]->compare_at(this_idx, prev_idx, *runs[prev_run], null_first) == 0;
        tie[i] &= x;

        // Consider limit
        if (i >= limit_end && x != 1) {
            result.resize(i);
            tie.resize(i);
            break;
        }
    }
}

// Merge two sorted chunks
// 1. Merge the first column, reorder the permutation, and generate tie for it
// 2. Incremental merge the second column, according to tie
// 3. Repeat the 2 step until last column
// TODO: change result to MergeResult
static inline void merge_sorted_chunks(const PermutatedChunk& lhs_chunk, const PermutatedChunk& rhs_chunk,
                                       const std::vector<int>& sort_orders, const std::vector<int>& null_firsts,
                                       Permutation& result, int limit) {
    DCHECK_EQ(lhs_chunk.num_columns(), rhs_chunk.num_columns());
    DCHECK_GT(limit, 0);
    if (lhs_chunk.num_rows() == 0) {
        return;
    }

    int num_rows = lhs_chunk.num_rows() + rhs_chunk.num_rows();
    Tie tie(num_rows, 1);
    result.resize(num_rows);

    for (int col_idx = 0; col_idx < sort_orders.size(); col_idx++) {
        int sort_order = sort_orders[col_idx];
        int null_first = null_firsts[col_idx];
        PermutatedColumn lhs_column = lhs_chunk.get_permutated_column(col_idx);
        PermutatedColumn rhs_column = rhs_chunk.get_permutated_column(col_idx);
        DCHECK(sort_order == 1 || sort_order == -1);
        DCHECK(null_first == 1 || null_first == -1);

        if (col_idx == 0) {
            lhs_column.column.merge_and_tie(sort_order, null_first, lhs_column, rhs_column, {0, lhs_column.size()},
                                            {0, rhs_column.size()}, tie, result, 0, limit);
            // merge_sorted(lhs_column, rhs_column, result, tie, {0, lhs_column.size()}, {0, rhs_column.size()},
            //  sort_order, null_first, limit, 0);
        } else {
            TieIterator iterator(tie);
            while (iterator.next()) {
                int first = iterator.range_first;
                int last = iterator.range_last;

                // already partial merged
                if (first > num_rows) {
                    break;
                }
                // TODO(mofei) optimize the search procedure
                auto pred_first_run = [](const PermutationItem& item) { return item.chunk_index == 0; };
                auto left_end = std::partition_point(result.begin() + first, result.begin() + last, pred_first_run) -
                                result.begin();
                std::pair<int, int> lhs_range{first, last};
                std::pair<int, int> rhs_range{first, last};

                if (result[first].chunk_index == 0 && left_end != last) {
                    // [LLLLLLL | RRRRRR]
                    DCHECK_EQ(result[left_end].chunk_index, 1);
                    DCHECK_EQ(result[left_end - 1].chunk_index, 0);
                    lhs_range.first = result[first].index_in_chunk;
                    lhs_range.second = result[left_end - 1].index_in_chunk + 1;
                    rhs_range.first = result[left_end].index_in_chunk;
                    rhs_range.second = result[last - 1].index_in_chunk + 1;
                } else if (result[first].chunk_index == 0 && left_end == last) {
                    // [LLLLLLL]
                    lhs_range.first = result[first].index_in_chunk;
                    lhs_range.second = result[last - 1].index_in_chunk + 1;
                    rhs_range.first = 0;
                    rhs_range.second = 0;
                } else {
                    // [RRRRRR]
                    lhs_range.first = 0;
                    lhs_range.second = 0;
                    rhs_range.first = result[first].index_in_chunk;
                    rhs_range.second = result[last - 1].index_in_chunk + 1;
                }
                DCHECK_EQ(last - first, (lhs_range.second - lhs_range.first) + (rhs_range.second - rhs_range.first));

                lhs_column.column.merge_and_tie(sort_order, null_first, lhs_column, rhs_column, lhs_range, rhs_range,
                                                tie, result, first, limit);
                // merge_sorted(lhs_column, rhs_column, result, tie, lhs_range, rhs_range, sort_order, null_first, limit,
                //  first);
            }
        }

#ifndef NDEBUG
        auto merge_column =
                unpermute_column(std::vector<const Column*>({&lhs_column.column, &rhs_column.column}), result, limit);
        fmt::print("merge {} and {} into {}\n", lhs_column.debug_string(), rhs_column.debug_string(),
                   merge_column->debug_string());
        fmt::print("tie become: [{}]\n", fmt::join(tie, ","));
#endif
    }
}

inline ChunkPtr merge_sorted_chunks_and_copy(const PermutatedChunk& lhs_chunk, const PermutatedChunk& rhs_chunk,
                                             const std::vector<int>& sort_orders, const std::vector<int>& null_firsts,
                                             int limit) {
    Permutation result_perm;
    merge_sorted_chunks(lhs_chunk, rhs_chunk, sort_orders, null_firsts, result_perm, limit);
    result_perm.resize(limit);
    return MergeResult::create(lhs_chunk, rhs_chunk, result_perm);
}

inline Status sort_permutated_chunk(PermutatedChunk& chunk, const std::vector<int>& sort_orders,
                                    const std::vector<int>& null_firsts) {
    const Columns& columns = chunk.chunk->columns();
    if (columns.size() < 1) {
        return Status::OK();
    }
    DCHECK_EQ(sort_orders.size(), null_firsts.size());
    size_t num_rows = chunk.chunk->num_rows();
    Tie tie(num_rows, 1);
    std::pair<int, int> range{0, num_rows};
    SmallPermutation permutation(num_rows);
    for (int i = 0; i < num_rows; i++) {
        permutation[i].index_in_chunk = i;
    }

    for (int col_index = 0; col_index < columns.size(); col_index++) {
        Column* column = columns[col_index].get();
        bool is_asc_order = (sort_orders[col_index] == 1);
        bool is_null_first = is_asc_order ? (null_firsts[col_index] == -1) : (null_firsts[col_index] == 1);
        bool build_tie = col_index != columns.size() - 1;
        column->sort_and_tie(is_asc_order, is_null_first, permutation, tie, range, build_tie);
    }
    for (int i = 0; i < num_rows; i++) {
        chunk.perm[i].index_in_chunk = permutation[i].index_in_chunk;
    }

    return Status::OK();
}
} // namespace starrocks::vectorized