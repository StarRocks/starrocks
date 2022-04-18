// This file is licensed under the Elastic License 2.0. Copyright 2021 - present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/datum.h"
#include "common/status.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "runtime/vectorized/chunk_cursor.h"

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

// Append rows from permutation
void append_by_permutation(Column* dst, const Columns& columns, const Permutation& perm);
void append_by_permutation(Chunk* dst, const std::vector<ChunkPtr>& chunks, const Permutation& perm);
void append_by_permutation(Chunk* dst, const std::vector<ChunkPtr>& chunks, const Permutation& perm, size_t start,
                           size_t end);
void append_by_permutation(Chunk* dst, const std::vector<const Chunk*>& chunks, const Permutation& perm);

struct SortDesc {
    int sort_order;
    int null_first;

    SortDesc() = default;
    SortDesc(int order, int null) : sort_order(order), null_first(null) {}
};

struct SortDescs {
    std::vector<SortDesc> descs;

    SortDescs() = default;
    ~SortDescs() = default;
    SortDescs(const std::vector<int>& orders, const std::vector<int>& nulls) {
        DCHECK_EQ(orders.size(), nulls.size());
        descs.reserve(orders.size());
        for (int i = 0; i < orders.size(); i++) {
            descs.push_back(SortDesc(orders[i], nulls[i]));
        }
    }

    size_t num_columns() const { return descs.size(); }

    SortDesc get_column_desc(int col) const {
        DCHECK_LT(col, descs.size());
        return descs[col];
    }
};

// SortedRun represents part of sorted chunk, specified by the range
// The chunk is sorted based on `orderby` columns
struct SortedRun {
    ChunkPtr chunk;
    Columns orderby;
    std::pair<uint32_t, uint32_t> range;

    SortedRun() = default;
    ~SortedRun() = default;
    explicit SortedRun(ChunkPtr ichunk) : chunk(ichunk), orderby(ichunk->columns()), range(0, ichunk->num_rows()) {}
    SortedRun(ChunkPtr ichunk, const Columns& columns)
            : chunk(ichunk), orderby(columns), range(0, ichunk->num_rows()) {}
    SortedRun(ChunkPtr ichunk, size_t start, size_t end)
            : chunk(ichunk), orderby(ichunk->columns()), range(start, end) {}
    SortedRun(const SortedRun& rhs) : chunk(rhs.chunk), orderby(rhs.orderby), range(rhs.range) {}
    SortedRun& operator=(const SortedRun& rhs) {
        if (&rhs == this) return *this;
        chunk = rhs.chunk;
        orderby = rhs.orderby;
        range = rhs.range;
        return *this;
    }

    size_t num_columns() const { return orderby.size(); }
    size_t num_rows() const { return range.second - range.first; }
    const Column* get_column(int index) const { return orderby[index].get(); }
    bool empty() const { return range.second == range.first; }
    void reset() {
        chunk->reset();
        orderby.clear();
        range = {};
    }
    ChunkUniquePtr clone_chunk() {
        if (range.first == 0) {
            return chunk->clone_unique();
        } else {
            ChunkUniquePtr cloned = chunk->clone_empty(num_rows() - range.first);
            cloned->append(*chunk, range.first, num_rows());
            return cloned;
        }
    }

    int compare_row(const SortDescs& desc, const SortedRun& rhs, size_t lhs_row, size_t rhs_row) const {
        for (int i = 0; i < orderby.size(); i++) {
            int x = get_column(i)->compare_at(lhs_row, rhs_row, *rhs.get_column(i), desc.get_column_desc(i).null_first);
            if (x != 0) {
                return x;
            }
        }
        return 0;
    }
};

// Multiple sorted chunks kept the order
struct SortedRuns {
    std::vector<SortedRun> chunks;

    SortedRuns() = default;
    ~SortedRuns() = default;
    SortedRuns(ChunkPtr chunk) : chunks{SortedRun(chunk)} {}
    SortedRuns(SortedRun run) : chunks{run} {}

    size_t num_chunks() const { return chunks.size(); }
};

// Merge algorithms
Status merge_sorted_chunks_two_way(const SortDescs& descs, const ChunkPtr left, const ChunkPtr right,
                                   Permutation* output);
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output);
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRuns& left, const SortedRuns& right,
                                   SortedRuns* output);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ChunkPtr>& chunks, ChunkPtr* output);

// Merge in rowwise
Status merge_sorted_chunks_two_way_rowwise(const SortDescs& descs, const ChunkPtr left, const ChunkPtr right,
                                           Permutation* output, size_t limit);

} // namespace starrocks::vectorized