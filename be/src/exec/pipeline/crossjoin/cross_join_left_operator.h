// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "runtime/descriptors.h"

namespace starrocks {
class ExprContext;
namespace pipeline {
class CrossJoinLeftOperator final : public OperatorWithDependency {
public:
    CrossJoinLeftOperator(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                          const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                          const std::vector<ExprContext*>& conjunct_ctxs, vectorized::ChunkPtr* build_chunk_ptr,
                          std::atomic<bool>* right_table_complete_ptr)
            : OperatorWithDependency(id, "cross_join_left", plan_node_id),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _conjunct_ctxs(conjunct_ctxs),
              _build_chunk_ptr(build_chunk_ptr),
              _right_table_complete_ptr(right_table_complete_ptr) {}

    ~CrossJoinLeftOperator() override = default;

    void _copy_joined_rows_with_index_base_build(vectorized::ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);
    void _copy_joined_rows_with_index_base_probe(vectorized::ChunkPtr& chunk, size_t row_count, size_t probe_index,
                                                 size_t build_index);

    void _copy_probe_rows_with_index_base_build(vectorized::ColumnPtr& dest_col, vectorized::ColumnPtr& src_col,
                                                size_t start_row, size_t copy_number);
    void _copy_probe_rows_with_index_base_probe(vectorized::ColumnPtr& dest_col, vectorized::ColumnPtr& src_col,
                                                size_t start_row, size_t copy_number);

    void _copy_build_rows_with_index_base_build(vectorized::ColumnPtr& dest_col, vectorized::ColumnPtr& src_col,
                                                size_t start_row, size_t row_count);
    void _copy_build_rows_with_index_base_probe(vectorized::ColumnPtr& dest_col, vectorized::ColumnPtr& src_col,
                                                size_t start_row, size_t row_count);

    void _init_row_desc();
    void _init_chunk(vectorized::ChunkPtr* chunk);
    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return _probe_chunk != nullptr; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished && _probe_chunk == nullptr; }

    void finish(RuntimeState* state) override { _is_finished = true; }

    bool is_ready() override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    // previsou saved chunk.
    vectorized::ChunkPtr _pre_output_chunk = nullptr;

    // used when scan rows in [0, _build_chunks_size) rows.
    size_t _build_chunks_index = 0;
    // used when scan rows in [_build_chunks_size, _number_of_built_rows) rows.
    size_t _build_rows_index = 0;

    // used as left table's chunk.
    // _probe_chunk about one chunk_size(maybe 4096) of left table.
    vectorized::ChunkPtr _probe_chunk = nullptr;
    // used when scan chunks in right table based on one row of left table.
    //  _probe_chunk_index is a local index in _probe_chunk.
    size_t _probe_chunk_index = 0;
    // used when scan rows in right table.
    // _probe_rows_index is a local index in _probe_chunk.
    // And is used when _probe_chunk_index == _probe_chunk->num_rows().
    size_t _probe_rows_index = 0;

    vectorized::Buffer<SlotDescriptor*> _col_types;
    vectorized::Buffer<TupleId> _output_build_tuple_ids;
    vectorized::Buffer<TupleId> _output_probe_tuple_ids;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    // Reference right table's data
    vectorized::ChunkPtr* _build_chunk_ptr = nullptr;
    // Used to mark that the right table has been constructed
    std::atomic<bool>* _right_table_complete_ptr = nullptr;

    size_t _number_of_build_rows = 0;
    size_t _build_chunks_size = 0;

    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;

    std::vector<uint32_t> _buf_selective;
};

class CrossJoinLeftOperatorFactory final : public OperatorWithDependencyFactory {
public:
    CrossJoinLeftOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                                 const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                                 const std::vector<ExprContext*>& conjunct_ctxs, vectorized::ChunkPtr* build_chunk_ptr,
                                 std::atomic<bool>* right_table_complete_ptr)
            : OperatorWithDependencyFactory(id, "cross_join_left", plan_node_id),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _conjunct_ctxs(conjunct_ctxs),
              _build_chunk_ptr(build_chunk_ptr),
              _right_table_complete_ptr(right_table_complete_ptr) {}

    ~CrossJoinLeftOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<CrossJoinLeftOperator>(_id, _plan_node_id, _row_descriptor, _left_row_desc,
                                                       _right_row_desc, _conjunct_ctxs, _build_chunk_ptr,
                                                       _right_table_complete_ptr);
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    // Reference right table's data
    vectorized::ChunkPtr* _build_chunk_ptr = nullptr;
    // Used to mark that the right table has been constructed
    std::atomic<bool>* _right_table_complete_ptr = nullptr;
};

} // namespace pipeline
} // namespace starrocks
