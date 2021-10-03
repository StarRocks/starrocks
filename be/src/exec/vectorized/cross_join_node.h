// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "exec/exec_node.h"

namespace starrocks::vectorized {
class CrossJoinNode : public ExecNode {
public:
    CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~CrossJoinNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    Status get_next_internal(RuntimeState* state, ChunkPtr* chunk, bool* eos,
                             ScopedTimer<MonotonicStopWatch>& probe_timer);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

private:
    Status _build(RuntimeState* state);
    Status _get_next_probe_chunk(RuntimeState* state);

    // append cross-joined rows into chunk

    void _copy_joined_rows_with_index_base_build(ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);
    void _copy_joined_rows_with_index_base_probe(ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);

    void _copy_probe_rows_with_index_base_build(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t copy_number);
    void _copy_probe_rows_with_index_base_probe(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t copy_number);

    void _copy_build_rows_with_index_base_build(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t row_count);
    void _copy_build_rows_with_index_base_probe(ColumnPtr& dest_col, ColumnPtr& src_col, size_t start_row,
                                                size_t row_count);

    void _init_row_desc();
    void _init_chunk(ChunkPtr* chunk);

    // previsou saved chunk.
    ChunkPtr _pre_output_chunk = nullptr;
    // used as right table's chunk.
    // _build_chunk include all rows of right table.
    ChunkPtr _build_chunk = nullptr;
    // total rows of right table.
    size_t _number_of_build_rows = 0;
    // total rows of right table by chunk_size.
    size_t _build_chunks_size = 0;
    // used when scan rows in [0, _build_chunks_size) rows.
    size_t _build_chunks_index = 0;
    // used when scan rows in [_build_chunks_size, _number_of_built_rows) rows.
    size_t _build_rows_index = 0;

    // used as left table's chunk.
    // _probe_chunk about one chunk_size(maybe 4096) of left table.
    ChunkPtr _probe_chunk = nullptr;
    // used when scan chunks in right table based on one row of left table.
    //  _probe_chunk_index is a local index in _probe_chunk.
    size_t _probe_chunk_index = 0;
    // used when scan rows in right table.
    // _probe_rows_index is a local index in _probe_chunk.
    // And is used when _probe_chunk_index == _probe_chunk->num_rows().
    size_t _probe_rows_index = 0;

    bool _eos = false;

    Buffer<SlotDescriptor*> _col_types;
    Buffer<TupleId> _output_build_tuple_ids;
    Buffer<TupleId> _output_probe_tuple_ids;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _probe_timer = nullptr;
    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _probe_rows_counter = nullptr;

    std::vector<uint32_t> _buf_selective;
};
} // namespace starrocks::vectorized
