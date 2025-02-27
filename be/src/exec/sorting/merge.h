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

#include <deque>
#include <memory>
#include <optional>
#include <utility>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "runtime/chunk_cursor.h"

namespace starrocks {
// SortedRun represents part of sorted chunk, specified by the range
// The chunk is sorted based on `orderby` columns
struct SortedRun {
    ChunkPtr chunk;
    Columns orderby;
    std::pair<uint32_t, uint32_t> range;

    SortedRun() = default;
    ~SortedRun() = default;
    SortedRun(const SortedRun& rhs) = default;
    SortedRun(SortedRun&& rhs) = default;
    SortedRun& operator=(const SortedRun& rhs) = default;

    SortedRun(const ChunkPtr& ichunk, Columns columns)
            : chunk(ichunk), orderby(std::move(columns)), range(0, ichunk->num_rows()) {}

    SortedRun(const SortedRun& rhs, size_t start, size_t end)
            : chunk(rhs.chunk), orderby(rhs.orderby), range(start, end) {
        DCHECK_LE(start, end);
        DCHECK_LT(end, Column::MAX_CAPACITY_LIMIT);
    }

    SortedRun(const ChunkPtr& ichunk, const std::vector<ExprContext*>* exprs);

    void set_range(size_t start, size_t end) {
        DCHECK_LE(range.first, range.second);
        DCHECK_LT(range.second - range.first, Column::MAX_CAPACITY_LIMIT);
        range.first = start;
        range.second = end;
    }
    void reset_range() {
        DCHECK_EQ(chunk->num_rows(), orderby[0]->size());
        range.first = 0;
        range.second = chunk->num_rows();
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
    bool empty() const { return range.second == range.first || chunk == nullptr; }
    void reset();
    void resize(size_t size);
    int64_t mem_usage() const { return chunk->memory_usage(); }
    int debug_dump() const;

    // Check if two run has intersect, if not we don't need to merge them row by row
    // @return 0 if two run has intersect, -1 if left is less than right, 1 if left is greater than right
    int intersect(const SortDescs& sort_desc, const SortedRun& right_run) const;

    // Clone this SortedRun, could be the entire chunk or slice of chunk
    ChunkUniquePtr clone_slice() const;

    // Steal part of chunk, skip the first `skipped_rows` rows and take the next `size` rows, avoid copy if possible
    // After steal out, this run will not reference the chunk anymore
    ChunkPtr steal_chunk(size_t size, size_t skipped_rows = 0) { return steal(false, size, skipped_rows).first; }
    std::pair<ChunkPtr, Columns> steal(bool steal_orderby, size_t size, size_t skipped_rows);

    int compare_row(const SortDescs& desc, const SortedRun& rhs, size_t lhs_row, size_t rhs_row) const;

    bool is_sorted(const SortDescs& desc) const;
};

// Multiple sorted chunks kept the order, without any intersection
struct SortedRuns {
    std::deque<SortedRun> chunks;

    SortedRuns() = default;
    ~SortedRuns() = default;
    SortedRuns(const SortedRuns& run) = default;
    SortedRuns(SortedRuns&& run) = default;
    SortedRuns(const SortedRun& run) : chunks{run} {}
    SortedRuns& operator=(SortedRuns&& run) = default;

    void merge_runs(SortedRuns& runs) {
        for (auto& run : runs.chunks) {
            chunks.push_back(std::move(run));
        }
    }
    SortedRun& get_run(size_t i) { return chunks[i]; }
    const SortedRun& get_run(size_t i) const { return chunks[i]; }
    std::optional<std::pair<size_t, size_t>> get_run_idx(const size_t row) const {
        size_t offset = row;
        for (size_t idx = 0; idx < chunks.size(); idx++) {
            auto& chunk = chunks[idx];
            if (offset < chunk.num_rows()) {
                return std::make_pair(idx, chunk.start_index() + offset);
            }
            offset -= chunk.num_rows();
        }
        return {};
    }
    ChunkPtr get_chunk(size_t i) const { return chunks[i].chunk; }
    size_t num_chunks() const { return chunks.size(); }
    size_t num_rows() const;
    void resize(size_t size);
    SortedRun& front() { return chunks.front(); }
    const SortedRun& front() const { return chunks.front(); }
    const SortedRun& back() const { return chunks.back(); }
    void pop_front() { chunks.pop_front(); }
    int64_t mem_usage() const {
        int64_t res = 0;
        for (auto& run : chunks) {
            res += run.mem_usage();
        }
        return res;
    }
    void clear();

    bool is_sorted(const SortDescs& sort_desc) const;
    ChunkPtr assemble() const;
    int debug_dump() const;
};

// Similar to SortedRun. The difference is that it holds a unique_ptr.
struct MergedRun {
    ChunkUniquePtr chunk;
    Columns orderby;
    size_t start_index() const { return range.first; }
    size_t end_index() const { return range.second; }

    void set_range(size_t start, size_t end) {
        DCHECK_LE(range.first, range.second);
        DCHECK_LT(range.second - range.first, Column::MAX_CAPACITY_LIMIT);
        range.first = start;
        range.second = end;
    }
    size_t num_rows() const {
        DCHECK_LE(range.first, range.second);
        DCHECK_LT(range.second - range.first, Column::MAX_CAPACITY_LIMIT);
        return range.second - range.first;
    }
    bool empty() const { return range.second == range.first || chunk == nullptr; }

    static StatusOr<MergedRun> build(ChunkUniquePtr&& chunk, const std::vector<ExprContext*>& exprs);
    ChunkPtr steal_chunk(size_t size);

private:
    std::pair<uint32_t, uint32_t> range;
};

// Similar to SortedRuns.
class MergedRuns {
public:
    MergedRuns& operator=(MergedRuns&& run) = default;

    MergedRun& front() {
        _num_rows.reset();
        return _runs.front();
    }
    MergedRun& back() {
        _num_rows.reset();
        return _runs.back();
    }
    MergedRun& at(size_t i) {
        _num_rows.reset();
        return _runs[i];
    }
    const MergedRun& front() const { return _runs.front(); }
    const MergedRun& back() const { return _runs.back(); }
    const MergedRun& at(size_t i) const { return _runs[i]; }

    size_t num_chunks() const { return _runs.size(); }
    size_t num_rows() const;
    int64_t mem_usage() const;
    size_t empty() const { return num_rows() == 0; }

    void push_back(MergedRun&& run) {
        _num_rows.reset();
        _runs.emplace_back(std::move(run));
    }

    void pop_front() {
        _num_rows.reset();
        _runs.pop_front();
    }
    void pop_back() {
        _num_rows.reset();
        _runs.pop_back();
    }

private:
    std::deque<MergedRun> _runs;
    mutable std::optional<size_t> _num_rows;
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

    Status consume_all(const ChunkConsumer& output);
    std::unique_ptr<SimpleChunkSortCursor> as_chunk_cursor();

private:
    ChunkProvider& as_provider() { return _chunk_provider; }
    StatusOr<ChunkUniquePtr> merge_sorted_cursor_two_way();
    // merge two runs
    StatusOr<ChunkUniquePtr> merge_sorted_intersected_cursor(SortedRun& run1, SortedRun& run2);

    bool move_cursor();

    SortDescs _sort_desc;
    SortedRun _left_run;
    SortedRun _right_run;
    std::unique_ptr<SimpleChunkSortCursor> _left_cursor;
    std::unique_ptr<SimpleChunkSortCursor> _right_cursor;
    ChunkProvider _chunk_provider;

#ifndef NDEBUG
    bool _left_is_empty = false;
    bool _right_is_empty = false;
#endif
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
    Status consume_all(const ChunkConsumer& consumer);
    Status consume_all_with_limit(const ChunkConsumer& consumer, size_t limit);

private:
    std::vector<std::unique_ptr<MergeTwoCursor>> _mergers;
    std::unique_ptr<SimpleChunkSortCursor> _root_cursor;
};

class SimpleChunkSortCursor;

// Merge implementations
// Underlying algorithm is multi-level cascade-merge, which could be streaming and short-circuit
Status merge_sorted_chunks_two_way(const SortDescs& sort_desc, const SortedRun& left, const SortedRun& right,
                                   Permutation* output);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs,
                           std::vector<ChunkUniquePtr>& chunks, SortedRuns* output);
Status merge_sorted_chunks(const SortDescs& descs, const std::vector<ExprContext*>* sort_exprs, MergedRuns& left,
                           ChunkUniquePtr&& right, size_t limit, MergedRuns* output);
Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors,
                                   const ChunkConsumer& consumer);
Status merge_sorted_cursor_cascade(const SortDescs& sort_desc,
                                   std::vector<std::unique_ptr<SimpleChunkSortCursor>>&& cursors,
                                   const ChunkConsumer& consumer, size_t limit);

// Merge in rowwise, which is slow and used only in benchmark
Status merge_sorted_chunks_two_way_rowwise(const SortDescs& descs, const Columns& left, const Columns& right,
                                           Permutation* output, size_t limit);

} // namespace starrocks
