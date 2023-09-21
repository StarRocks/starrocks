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

#include <functional>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr_context.h"

namespace starrocks {

class SortExecExprs;

// A chunk supplier signal EOS by outputting a NULL Chunk.
typedef std::function<Status(Chunk**)> ChunkSupplier;
typedef std::vector<ChunkSupplier> ChunkSuppliers;

typedef std::function<Status(ChunkUniquePtr)> ChunkConsumer;

// A chunk supplier signal EOS by outputting a NULL Chunk.
typedef std::function<bool(Chunk**)> ChunkProbeSupplier;
typedef std::vector<ChunkProbeSupplier> ChunkProbeSuppliers;

// A chunk supplier signal EOS by outputting a NULL Chunk.
typedef std::function<bool()> ChunkHasSupplier;
typedef std::vector<ChunkHasSupplier> ChunkHasSuppliers;

typedef std::function<Status(ChunkUniquePtr)> ChunkConsumer;
using ChunkProvider = std::function<bool(ChunkUniquePtr*, bool*)>;

// A cursor refers to a record in a Chunk, and can compare to a cursor referring a record in another Chunk.
class ChunkCursor {
public:
    ChunkCursor() = default;
    ChunkCursor(const ChunkCursor&) = delete;
    ChunkCursor(ChunkSupplier chunk_supplier, ChunkProbeSupplier chunk_probe_supplier,
                ChunkHasSupplier chunk_has_supplier, const std::vector<ExprContext*>* sort_exprs,
                const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first, bool is_pipeline);
    ~ChunkCursor();

    // Whether the record referenced by this cursor is before the one referenced by cursor.
    bool operator<(const ChunkCursor& cursor) const;

    // Move to next row.
    void next();
    // Return if there is new chunk.
    bool has_next();
    // Move to next row for pipeline.
    void next_for_pipeline();
    void next_chunk_for_pipeline();
    // Is current row valid? A new Cursor without any next() has an invalid row.
    bool is_valid() const;
    // Copy current row to the dest Chunk whose structure is as same as the source Chunk.
    bool copy_current_row_to(Chunk* dest) const;

    ChunkPtr get_current_chunk() { return _current_chunk; };
    int32_t get_current_position_in_chunk() const { return _current_pos; };

    [[nodiscard]] ChunkPtr clone_empty_chunk(size_t reserved_row_number) const;

    [[nodiscard]] Status chunk_supplier(Chunk**);
    bool chunk_probe_supplier(Chunk**);
    bool chunk_has_supplier();

private:
    void _reset_with_next_chunk();

private:
    ChunkSupplier _chunk_supplier;
    // Probe for chunks, because _chunk_queue maybe empty when data hasn't come yet.
    // So compute thread should do other works.
    ChunkProbeSupplier _chunk_probe_supplier;
    // check if data has come, work with _chunk_probe_supplier.
    ChunkHasSupplier _chunk_has_supplier;

    ChunkPtr _current_chunk;
    int32_t _current_pos;
    Columns _current_order_by_columns;

    const std::vector<ExprContext*>* _sort_exprs;
    std::vector<int> _sort_order_flag; // 1 for ascending, -1 for descending.
    std::vector<int> _null_first_flag; // 1 for greatest, -1 for least.
    bool _is_pipeline;
};

// SimpleChunkCursor a simple cursor over the SenderQueue, avoid copy the chunk
//
// Example:
// while (!cursor.is_eos()) {
//      ChunkUniquePtr chunk = cursor.try_get_next();
//      if (!!chunk)) {
//          usage(chunk);
//      }
// }
class SimpleChunkSortCursor {
public:
    SimpleChunkSortCursor() = delete;
    SimpleChunkSortCursor(const SimpleChunkSortCursor& rhs) = delete;
    SimpleChunkSortCursor(ChunkProvider chunk_provider, const std::vector<ExprContext*>* sort_exprs);
    ~SimpleChunkSortCursor() = default;

    // Check has any data
    bool is_data_ready();

    // Try to get next chunk, return a Chunk a available,
    // return a nullptr if data temporarily not avaiable or end of stream
    std::pair<ChunkUniquePtr, Columns> try_get_next();

    // Check if is the end of stream
    bool is_eos();

    const std::vector<ExprContext*>* get_sort_exprs() const { return _sort_exprs; }

private:
    bool _data_ready = false;
    bool _eos = false;

    ChunkProvider _chunk_provider;
    const std::vector<ExprContext*>* _sort_exprs;
};

} // namespace starrocks
