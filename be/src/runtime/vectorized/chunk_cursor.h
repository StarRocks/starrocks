// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <functional>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr_context.h"

namespace starrocks {

class SortExecExprs;

namespace vectorized {

// A chunk supplier signal EOS by outputting a NULL Chunk.
typedef std::function<Status(Chunk**)> ChunkSupplier;
typedef std::vector<ChunkSupplier> ChunkSuppliers;

// A cursor refers to a record in a Chunk, and can compare to a cursor referring a record in another Chunk.
class ChunkCursor {
public:
    ChunkCursor(const ChunkSupplier& supplier, const std::vector<ExprContext*>* sort_exprs,
                const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first);
    ~ChunkCursor();

    // Whether the record referenced by this cursor is before the one referenced by cursor.
    bool operator<(const ChunkCursor& cursor) const;

    // Move to next row.
    void next();
    // Is current row valid? A new Cursor without any next() has an invalid row.
    bool is_valid() const;
    // Copy current row to the dest Chunk whose structure is as same as the source Chunk.
    bool copy_current_row_to(Chunk* dest) const;

    const ChunkPtr& get_current_chunk() const { return _current_chunk; };
    int32_t get_current_position_in_chunk() const { return _current_pos; };

    [[nodiscard]] ChunkPtr clone_empty_chunk(size_t reserved_row_number) const;

private:
    void _reset_with_next_chunk();

private:
    ChunkSupplier _chunk_supplier;
    ChunkPtr _current_chunk;
    int32_t _current_pos;
    Columns _current_order_by_columns;

    const std::vector<ExprContext*>* _sort_exprs;
    std::vector<int> _sort_order_flag; // 1 for ascending, -1 for descending.
    std::vector<int> _null_first_flag; // 1 for greatest, -1 for least.
};

} // namespace vectorized

} // namespace starrocks
