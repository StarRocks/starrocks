// This file is licensed under the Elastic License 2.0. Copyright 2021 - present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/datum.h"
#include "exec/vectorized/sorting/sorting.h"
#include "runtime/chunk_cursor.h"

namespace starrocks::vectorized {

// SortedRun represents part of sorted chunk, specified by the range
// The chunk is sorted based on `orderby` columns
struct SortedRun {
    ChunkPtr chunk;
    Columns orderby;
    std::pair<uint32_t, uint32_t> range;

    SortedRun() = default;
    ~SortedRun() = default;

    SortedRun(ChunkPtr ichunk, const Columns& columns)
            : chunk(ichunk), orderby(columns), range(0, ichunk->num_rows()) {}

    SortedRun(SortedRun rhs, size_t start, size_t end) : chunk(rhs.chunk), orderby(rhs.orderby), range(start, end) {
        DCHECK_LE(start, end);
        DCHECK_LT(end, Column::MAX_CAPACITY_LIMIT);
    }

    SortedRun(const SortedRun& rhs) : chunk(rhs.chunk), orderby(rhs.orderby), range(rhs.range) {}

    SortedRun(ChunkPtr ichunk, const std::vector<ExprContext*>* exprs) : chunk(ichunk), range(0, ichunk->num_rows()) {
        for (auto& expr : *exprs) {
            auto maybe_column = expr->evaluate(ichunk.get());
            CHECK(maybe_column.ok());
            orderby.push_back(maybe_column.value());
        }
    }

    SortedRun& operator=(const SortedRun& rhs) {
        if (&rhs == this) return *this;
        chunk = rhs.chunk;
        orderby = rhs.orderby;
        range = rhs.range;
        return *this;
    }

    size_t num_columns() const { return orderby.size(); }
    size_t start_index() const { return range.first; }
    size_t end_index() const { return range.second; }
    size_t num_rows() const {
        DCHECK_LE(range.first, range.second);
        DCHECK_LT(range.second - range.first, Column::MAX_CAPACITY_LIMIT);
        return range.second - range.first;
    }
    const Column* get_column(int index) const { return orderby[index].get(); }
    bool empty() const { return range.second == range.first; }
    void reset();
    void resize(size_t size);
    int64_t mem_usage() const { return chunk->memory_usage(); }

    // Clone this SortedRun, could be the entire chunk or slice of chunk
    ChunkUniquePtr clone_slice() const {
        if (range.first == 0 && range.second == chunk->num_rows()) {
            return chunk->clone_unique();
        } else {
            size_t slice_rows = num_rows();
            DCHECK_LT(slice_rows, Column::MAX_CAPACITY_LIMIT);
            ChunkUniquePtr cloned = chunk->clone_empty(slice_rows);
            cloned->append(*chunk, range.first, slice_rows);
            return cloned;
        }
    }

    int compare_row(const SortDescs& desc, const SortedRun& rhs, size_t lhs_row, size_t rhs_row) const {
        DCHECK_LT(lhs_row, range.second);
        DCHECK_LT(rhs_row, rhs.range.second);
        for (int i = 0; i < orderby.size(); i++) {
            int x = get_column(i)->compare_at(lhs_row, rhs_row, *rhs.get_column(i), desc.get_column_desc(i).null_first);
            if (x != 0) {
                return x;
            }
        }
        return 0;
    }
};

// Multiple sorted chunks kept the order, without any intersection
struct SortedRuns {
    std::deque<SortedRun> chunks;

    SortedRuns() = default;
    ~SortedRuns() = default;
    SortedRuns(SortedRun run) : chunks{run} {}

    SortedRun& get_run(int i) { return chunks[i]; }
    ChunkPtr get_chunk(int i) const { return chunks[i].chunk; }
    size_t num_chunks() const { return chunks.size(); }
    size_t num_rows() const;
    void resize(size_t size);
    SortedRun& front() { return chunks.front(); }
    void pop_front() { chunks.pop_front(); }
    int64_t mem_usage() const {
        int64_t res = 0;
        for (auto& run : chunks) {
            res += run.mem_usage();
        }
        return res;
    }

    ChunkPtr assemble() const {
        if (chunks.empty()) {
            return {};
        }
        ChunkPtr result(chunks.front().clone_slice().release());
        for (int i = 1; i < chunks.size(); i++) {
            auto& run = chunks[i];
            result->append(*run.chunk, run.range.first, run.num_rows());
        }
        return result;
    }
};

// Merge two sorted cusor
class MergeTwoCursor {
public:
    MergeTwoCursor(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor>&& left_cursor,
                   std::unique_ptr<SimpleChunkSortCursor>&& right_cursor);

    bool is_data_ready();
    bool is_eos();

    // Use it as iterator
    // Return nullptr if no output
    StatusOr<ChunkUniquePtr> next();

    Status consume_all(ChunkConsumer output);
    std::unique_ptr<SimpleChunkSortCursor> as_chunk_cursor();

private:
    ChunkProvider& as_provider() { return _chunk_provider; }
    StatusOr<ChunkUniquePtr> merge_sorted_cursor_two_way();
    bool move_cursor();

    SortDescs _sort_desc;
    SortedRun _left_run;
    SortedRun _right_run;
    std::unique_ptr<SimpleChunkSortCursor> _left_cursor;
    std::unique_ptr<SimpleChunkSortCursor> _right_cursor;
    ChunkProvider _chunk_provider;
};

// Merge multiple cursors in cascade way
class MergeCursorsCascade {
public:
    MergeCursorsCascade() = default;
    ~MergeCursorsCascade() = default;

    Status init(const SortDescs& sort_desc, std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors);
    bool is_data_ready();
    bool is_eos();
    ChunkUniquePtr try_get_next();
    Status consume_all(ChunkConsumer consumer);

private:
    std::vector<std::unique_ptr<MergeTwoCursor>> _mergers;
    std::unique_ptr<SimpleChunkSortCursor> _root_cursor;
};

class SimpleChunkSortCursor;

// ColumnWise Merge algorithms
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output);
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRuns& left, const SortedRuns& right,
                                   SortedRuns* output);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs,
                           const std::vector<ChunkPtr>& chunks, ChunkPtr* output, size_t limit);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs,
                           const std::vector<ChunkPtr>& chunks, SortedRuns* output, size_t limit);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs,
                           const std::vector<SortedRuns>& chunks, SortedRuns* output, size_t limit);

// ColumnWise merge streaming merge
Status merge_sorted_cursor_two_way(const SortDescs& sort_desc, std::unique_ptr<SimpleChunkSortCursor> left_cursor,
                                   std::unique_ptr<SimpleChunkSortCursor> right_cursor, ChunkConsumer output);
Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors,
                                   ChunkConsumer consumer);

// Merge in rowwise, which is slow and used only in benchmark
Status merge_sorted_chunks_two_way_rowwise(const SortDescs& descs, const Columns& left, const Columns& right,
                                           Permutation* output, size_t limit);

} // namespace starrocks::vectorized