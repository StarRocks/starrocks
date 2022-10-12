// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <queue>

#include "runtime/chunk_cursor.h"
#include "util/runtime_profile.h"

namespace starrocks {

class SortExecExprs;

namespace vectorized {

// Merge a group of sorted Chunks to one Chunk in order.
class SortedChunksMerger {
public:
    SortedChunksMerger(RuntimeState* state, bool is_pipeline);
    ~SortedChunksMerger();

    Status init(const ChunkSuppliers& chunk_suppliers, const ChunkProbeSuppliers& chunk_probe_suppliers,
                const ChunkHasSuppliers& chunk_has_suppliers, const std::vector<ExprContext*>* sort_exprs,
                const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first);
    Status init_for_pipeline(const ChunkSuppliers& chunk_suppliers, const ChunkProbeSuppliers& chunk_probe_suppliers,
                             const ChunkHasSuppliers& chunk_has_suppliers, const std::vector<ExprContext*>* sort_exprs,
                             const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first);
    bool is_data_ready();
    void init_for_min_heap();

    void set_profile(RuntimeProfile* profile);

    // Return the next sorted chunk from this merger.
    Status get_next(ChunkPtr* chunk, bool* eos);
    Status get_next_for_pipeline(ChunkPtr* chunk, std::atomic<bool>* eos, bool* should_exit);

private:
    RuntimeState* _state;
    void collect_merged_chunks(ChunkPtr* chunk);
    void move_cursor_and_adjust_min_heap(std::atomic<bool>* eos);

    ChunkSupplier _single_supplier;
    ChunkProbeSupplier _single_probe_supplier;
    ChunkHasSupplier _single_has_supplier;

    bool _is_pipeline;

public:
    std::vector<std::unique_ptr<ChunkCursor>> _cursors;

private:
    struct CursorCmpGreater {
        // if a is greater than b.
        inline bool operator()(const ChunkCursor* a, const ChunkCursor* b) { return b->operator<(*a); }
    };
    CursorCmpGreater _cursor_cmp_greater;

public:
    std::vector<ChunkCursor*> _min_heap;

private:
    RuntimeProfile::Counter* _total_timer = nullptr;

    // for multiple suppliers.
    bool _after_min_heap = false;

    /* this is for pipeline.
     * _row_number: is initial 0, and record the number of rows between calls, after return datas, set _row_number back to 0.
     * _cursor: will record top element of min_heap.
     * _current_chunk: record currently used chunk.
     * _result_chunk: copy rows from every _current_chunk. 
     * _selective_values: used to record index in _current_chunk.
     * _wait_for_data: record is it need to blocking or non-blocking.
     */
    size_t _row_number = 0;
    ChunkCursor* _cursor;
    ChunkPtr _current_chunk;
    ChunkPtr _result_chunk;
    std::vector<uint32_t> _selective_values;

    // Because chunks is transfer from network in default, so we should wait for it,
    // and compute thread will not blocking for chunks.
    // Actually _wait_for_data is used to distinguish whether to exit
    // because the result is sufficient or the data is insufficient.
    bool _wait_for_data = false;
};

} // namespace vectorized

} // namespace starrocks
