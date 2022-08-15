// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/crossjoin/cross_join_context.h"
#include "exec/pipeline/operator_with_dependency.h"
#include "runtime/descriptors.h"

namespace starrocks {
class ExprContext;

namespace pipeline {

class CrossJoinLeftOperator final : public OperatorWithDependency {
public:
    CrossJoinLeftOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          const std::vector<ExprContext*>& conjunct_ctxs,
                          const vectorized::Buffer<SlotDescriptor*>& col_types, const size_t& probe_column_count,
                          const size_t& build_column_count, const std::shared_ptr<CrossJoinContext>& cross_join_context)
            : OperatorWithDependency(factory, id, "cross_join_left", plan_node_id, driver_sequence),
              _col_types(col_types),
              _probe_column_count(probe_column_count),
              _build_column_count(build_column_count),
              _conjunct_ctxs(conjunct_ctxs),
              _cross_join_context(cross_join_context) {
        _cross_join_context->ref();
    }

    ~CrossJoinLeftOperator() override = default;

    void close(RuntimeState* state) override {
        _cross_join_context->unref(state);
        Operator::close(state);
    }

    bool is_ready() const override { return _cross_join_context->is_right_finished(); }

    bool has_output() const override {
        // The probe chunk has been pushed to this operator,
        // and isn't finished crossing join with build chunks.
        return _probe_chunk != nullptr && !_is_curr_probe_chunk_finished();
    }

    bool need_input() const override {
        if (!is_ready()) {
            return false;
        }

        // If build is finished and build chunk is empty, the cross join result must be empty.
        // Therefore, CrossJoinLeftOperator directly comes to end without the chunk from the prev operator.
        if (_cross_join_context->is_build_chunk_empty()) {
            return false;
        }

        return _is_curr_probe_chunk_finished();
    }

    bool is_finished() const override {
        // If build is finished and build chunk is empty, the cross join result must be empty.
        // Therefore, CrossJoinLeftOperator directly comes to end without the chunk from the prev operator.
        if (is_ready() && _cross_join_context->is_build_chunk_empty()) {
            return true;
        }

        return _is_finished && _is_curr_probe_chunk_finished();
    }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    Status set_finished(RuntimeState* state) override {
        _cross_join_context->set_finished();
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // Whether the pushed probe chunk is finished crossing join with build chunks.
    bool _is_curr_probe_chunk_finished() const {
        // The init value of _curr_build_index is -1. After calling push_chunk and pull_chunk,
        // the range of _curr_build_index will be always in [0, num_build_chunks].
        return _curr_build_index < 0 || _curr_build_index >= _cross_join_context->num_build_chunks();
    }

    void _select_build_chunk(int32_t build_index, RuntimeState* state);

    void _init_chunk(vectorized::ChunkPtr* chunk, RuntimeState* state);

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

    const vectorized::Buffer<SlotDescriptor*>& _col_types;
    const size_t& _probe_column_count;
    const size_t& _build_column_count;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    // The init value of _curr_build_index is -1. After calling push_chunk and pull_chunk,
    // the range of _curr_build_index will be always in [0, num_build_chunks].
    int32_t _curr_build_index = -1;
    vectorized::Chunk* _curr_build_chunk = nullptr;
    // Decompose right table rows into multiples of 4096 rows and remainder of 4096 rows.
    // _curr_total_build_rows = _curr_build_rows_threshold * 4096 + _curr_build_rows_remainder.
    size_t _curr_total_build_rows = 0;
    // multiples of 4096.
    size_t _curr_build_rows_threshold = 0;
    // remainder of 4096.
    size_t _curr_build_rows_remainder = 0;

    // used when scan rows in [0, _curr_build_rows_threshold) rows.
    size_t _within_threshold_build_rows_index = 0;
    // used when scan rows in [_curr_build_rows_threshold, _number_of_built_rows) rows.
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

    bool _is_finished = false;

    std::vector<uint32_t> _buf_selective;

    const std::shared_ptr<CrossJoinContext>& _cross_join_context;
};

class CrossJoinLeftOperatorFactory final : public OperatorWithDependencyFactory {
public:
    CrossJoinLeftOperatorFactory(int32_t id, int32_t plan_node_id, const RowDescriptor& row_descriptor,
                                 const RowDescriptor& left_row_desc, const RowDescriptor& right_row_desc,
                                 std::vector<ExprContext*>&& conjunct_ctxs,
                                 std::shared_ptr<CrossJoinContext>&& cross_join_context)
            : OperatorWithDependencyFactory(id, "cross_join_left", plan_node_id),
              _row_descriptor(row_descriptor),
              _left_row_desc(left_row_desc),
              _right_row_desc(right_row_desc),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _cross_join_context(std::move(cross_join_context)) {}

    ~CrossJoinLeftOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<CrossJoinLeftOperator>(this, _id, _plan_node_id, driver_sequence, _conjunct_ctxs,
                                                       _col_types, _probe_column_count, _build_column_count,
                                                       _cross_join_context);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    void _init_row_desc();

    const RowDescriptor& _row_descriptor;
    const RowDescriptor& _left_row_desc;
    const RowDescriptor& _right_row_desc;

    vectorized::Buffer<SlotDescriptor*> _col_types;
    size_t _probe_column_count = 0;
    size_t _build_column_count = 0;

    std::vector<ExprContext*> _conjunct_ctxs;

    std::shared_ptr<CrossJoinContext> _cross_join_context;
};

} // namespace pipeline
} // namespace starrocks
