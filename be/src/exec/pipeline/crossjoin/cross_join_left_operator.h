// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "runtime/descriptors.h"

namespace starrocks {
class ExprContext;
namespace pipeline {
class CrossJoinContext;
class CrossJoinLeftOperator final : public OperatorWithDependency {
public:
    CrossJoinLeftOperator(int32_t id, int32_t plan_node_id, const std::vector<ExprContext*>& conjunct_ctxs,
                          const vectorized::Buffer<SlotDescriptor*>& col_types,
                          const vectorized::Buffer<TupleId>& output_build_tuple_ids,
                          const vectorized::Buffer<TupleId>& output_probe_tuple_ids, const size_t& probe_column_count,
                          const size_t& build_column_count, const std::shared_ptr<CrossJoinContext>& cross_join_context)
            : OperatorWithDependency(id, "cross_join_left", plan_node_id),
              _col_types(col_types),
              _output_build_tuple_ids(output_build_tuple_ids),
              _output_probe_tuple_ids(output_probe_tuple_ids),
              _probe_column_count(probe_column_count),
              _build_column_count(build_column_count),
              _conjunct_ctxs(conjunct_ctxs),
              _cross_join_context(cross_join_context) {}

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

    void _init_chunk(vectorized::ChunkPtr* chunk);
    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return _probe_chunk != nullptr; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished && _probe_chunk == nullptr; }

    void finish(RuntimeState* state) override { _is_finished = true; }

    bool is_ready() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // previsou saved chunk.
    vectorized::ChunkPtr _pre_output_chunk = nullptr;

    // used when scan rows in [0, _build_rows_threshold) rows.
    size_t _within_threshold_build_rows_index = 0;
    // used when scan rows in [_build_rows_threshold, _number_of_built_rows) rows.
    size_t _beyond_threshold_build_rows_index = 0;

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

    const vectorized::Buffer<SlotDescriptor*>& _col_types;
    const vectorized::Buffer<TupleId>& _output_build_tuple_ids;
    const vectorized::Buffer<TupleId>& _output_probe_tuple_ids;
    const size_t& _probe_column_count;
    const size_t& _build_column_count;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    // Decompose all rows into multiples of 4096 rows and remainder of 4096 rows.
    size_t mutable _total_build_rows = 0;
    // multiples of 4096
    size_t mutable _build_rows_threshold = 0;
    // remainder of 4096
    size_t mutable _build_rows_remainder = 0;
    // means right table is constructed completely.
    bool mutable _is_right_complete = false;

    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;

    std::vector<uint32_t> _buf_selective;

    const std::shared_ptr<CrossJoinContext>& _cross_join_context;
};

class CrossJoinLeftOperatorFactory final : public OperatorWithDependencyFactory {
public:
    CrossJoinLeftOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                                 const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                                 std::vector<ExprContext*>&& conjunct_ctxs,
                                 std::shared_ptr<CrossJoinContext> cross_join_context)
            : OperatorWithDependencyFactory(id, "cross_join_left", plan_node_id),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _cross_join_context(cross_join_context) {}

    ~CrossJoinLeftOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<CrossJoinLeftOperator>(_id, _plan_node_id, _conjunct_ctxs, _col_types,
                                                       _output_build_tuple_ids, _output_probe_tuple_ids,
                                                       _probe_column_count, _build_column_count, _cross_join_context);
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    void _init_row_desc();
    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    vectorized::Buffer<SlotDescriptor*> _col_types;
    vectorized::Buffer<TupleId> _output_build_tuple_ids;
    vectorized::Buffer<TupleId> _output_probe_tuple_ids;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    std::vector<ExprContext*> _conjunct_ctxs;

    std::shared_ptr<CrossJoinContext> _cross_join_context;
};

} // namespace pipeline
} // namespace starrocks
