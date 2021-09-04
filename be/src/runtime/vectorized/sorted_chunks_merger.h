// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <queue>

#include "runtime/vectorized/chunk_cursor.h"
#include "util/runtime_profile.h"

namespace starrocks {

class SortExecExprs;

namespace vectorized {

// Merge a group of sorted Chunks to one Chunk in order.
class SortedChunksMerger {
public:
    SortedChunksMerger();
    ~SortedChunksMerger();

    Status init(const ChunkSuppliers& suppliers, const std::vector<ExprContext*>* sort_exprs,
                const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first);

    void set_profile(RuntimeProfile* profile);

    // Return the next sorted chunk from this merger.
    Status get_next(ChunkPtr* chunk, bool* eos);

private:
    ChunkSupplier _single_supplier;
    std::vector<std::unique_ptr<ChunkCursor>> _cursors;

    struct CursorCmpGreater {
        // if a is greater than b.
        inline bool operator()(const ChunkCursor* a, const ChunkCursor* b) { return b->operator<(*a); }
    };
    CursorCmpGreater _cursor_cmp_greater;
    std::vector<ChunkCursor*> _min_heap;

    RuntimeProfile::Counter* _total_timer = nullptr;
};

} // namespace vectorized

} // namespace starrocks
