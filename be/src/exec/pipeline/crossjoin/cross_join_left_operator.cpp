// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/crossjoin/cross_join_left_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/exec_node.h"
#include "exec/pipeline/crossjoin/cross_join_context.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status CrossJoinLeftOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status CrossJoinLeftOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

void CrossJoinLeftOperator::_init_chunk(vectorized::ChunkPtr* chunk) {
    vectorized::ChunkPtr new_chunk = std::make_shared<vectorized::Chunk>();

    // init columns for the new chunk from _probe_chunk and _cross_join_context->_build_chunk
    for (size_t i = 0; i < _probe_column_count; ++i) {
        SlotDescriptor* slot = _col_types[i];
        vectorized::ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
        auto new_col = vectorized::ColumnHelper::create_column(slot->type(), src_col->is_nullable());
        new_chunk->append_column(std::move(new_col), slot->id());
    }
    for (size_t i = 0; i < _build_column_count; ++i) {
        SlotDescriptor* slot = _col_types[_probe_column_count + i];
        vectorized::ColumnPtr& src_col = _cross_join_context->_build_chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), src_col->is_nullable());
        new_chunk->append_column(std::move(new_col), slot->id());
    }

    for (int tuple_id : _output_probe_tuple_ids) {
        if (_probe_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr dest_col = vectorized::BooleanColumn::create();
            new_chunk->append_tuple_column(dest_col, tuple_id);
        }
    }
    for (int tuple_id : _output_build_tuple_ids) {
        if (_cross_join_context->_build_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr dest_col = vectorized::BooleanColumn::create();
            new_chunk->append_tuple_column(dest_col, tuple_id);
        }
    }

    *chunk = std::move(new_chunk);
    (*chunk)->reserve(config::vector_chunk_size);
}

void CrossJoinLeftOperator::_copy_joined_rows_with_index_base_probe(vectorized::ChunkPtr& chunk, size_t row_count,
                                                                    size_t probe_index, size_t build_index) {
    for (size_t i = 0; i < _probe_column_count; i++) {
        SlotDescriptor* slot = _col_types[i];
        vectorized::ColumnPtr& dest_col = chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
        _copy_probe_rows_with_index_base_probe(dest_col, src_col, probe_index, row_count);
    }

    for (size_t i = 0; i < _build_column_count; i++) {
        SlotDescriptor* slot = _col_types[i + _probe_column_count];
        vectorized::ColumnPtr& dest_col = chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr& src_col = _cross_join_context->_build_chunk->get_column_by_slot_id(slot->id());
        _copy_build_rows_with_index_base_probe(dest_col, src_col, build_index, row_count);
    }

    for (int tuple_id : _output_probe_tuple_ids) {
        if (_probe_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr& src_col = _probe_chunk->get_tuple_column_by_id(tuple_id);
            auto& src_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_col)->get_data();
            vectorized::ColumnPtr& dest_col = chunk->get_tuple_column_by_id(tuple_id);
            auto& dest_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_col)->get_data();
            for (size_t i = 0; i < row_count; i++) {
                dest_data.emplace_back(src_data[probe_index]);
            }
        }
    }

    for (int tuple_id : _output_build_tuple_ids) {
        if (_cross_join_context->_build_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr& src_col = _cross_join_context->_build_chunk->get_tuple_column_by_id(tuple_id);
            auto& src_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_col)->get_data();
            vectorized::ColumnPtr& dest_col = chunk->get_tuple_column_by_id(tuple_id);
            auto& dest_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_col)->get_data();

            for (size_t i = 0; i < row_count; i++) {
                dest_data.emplace_back(src_data[build_index + i]);
            }
        }
    }
}

void CrossJoinLeftOperator::_copy_joined_rows_with_index_base_build(vectorized::ChunkPtr& chunk, size_t row_count,
                                                                    size_t probe_index, size_t build_index) {
    for (size_t i = 0; i < _probe_column_count; i++) {
        SlotDescriptor* slot = _col_types[i];
        vectorized::ColumnPtr& dest_col = chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
        _copy_probe_rows_with_index_base_build(dest_col, src_col, probe_index, row_count);
    }

    for (size_t i = 0; i < _build_column_count; i++) {
        SlotDescriptor* slot = _col_types[i + _probe_column_count];
        vectorized::ColumnPtr& dest_col = chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr& src_col = _cross_join_context->_build_chunk->get_column_by_slot_id(slot->id());
        _copy_build_rows_with_index_base_build(dest_col, src_col, build_index, row_count);
    }

    for (int tuple_id : _output_probe_tuple_ids) {
        if (_probe_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr& src_col = _probe_chunk->get_tuple_column_by_id(tuple_id);
            auto& src_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_col)->get_data();
            vectorized::ColumnPtr& dest_col = chunk->get_tuple_column_by_id(tuple_id);
            auto& dest_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_col)->get_data();
            for (size_t i = 0; i < row_count; i++) {
                dest_data.emplace_back(src_data[probe_index + i]);
            }
        }
    }

    for (int tuple_id : _output_build_tuple_ids) {
        if (_cross_join_context->_build_chunk->is_tuple_exist(tuple_id)) {
            vectorized::ColumnPtr& src_col = _cross_join_context->_build_chunk->get_tuple_column_by_id(tuple_id);
            auto& src_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(src_col)->get_data();
            vectorized::ColumnPtr& dest_col = chunk->get_tuple_column_by_id(tuple_id);
            auto& dest_data = vectorized::ColumnHelper::as_raw_column<vectorized::BooleanColumn>(dest_col)->get_data();

            for (size_t i = 0; i < row_count; i++) {
                dest_data.emplace_back(src_data[build_index]);
            }
        }
    }
}

void CrossJoinLeftOperator::_copy_probe_rows_with_index_base_probe(vectorized::ColumnPtr& dest_col,
                                                                   vectorized::ColumnPtr& src_col, size_t start_row,
                                                                   size_t copy_number) {
    if (src_col->is_nullable()) {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(copy_number);
        } else {
            // repeat the value from probe table for copy_number times
            dest_col->append_value_multiple_times(*src_col.get(), start_row, copy_number);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            auto* const_col = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(src_col);
            // repeat the constant value from probe table for copy_number times
            _buf_selective.assign(copy_number, 0);
            dest_col->append_selective(*const_col->data_column(), &_buf_selective[0], 0, copy_number);
        } else {
            // repeat the value from probe table for copy_number times
            dest_col->append_value_multiple_times(*src_col.get(), start_row, copy_number);
        }
    }
}

void CrossJoinLeftOperator::_copy_probe_rows_with_index_base_build(vectorized::ColumnPtr& dest_col,
                                                                   vectorized::ColumnPtr& src_col, size_t start_row,
                                                                   size_t copy_number) {
    if (src_col->is_nullable()) {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(copy_number);
        } else {
            // repeat the value from probe table for copy_number times
            dest_col->append(*src_col.get(), start_row, copy_number);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            auto* const_col = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(src_col);
            // repeat the constant value from probe table for copy_number times
            _buf_selective.assign(copy_number, 0);
            dest_col->append_selective(*const_col->data_column(), &_buf_selective[0], 0, copy_number);
        } else {
            // repeat the value from probe table for copy_number times
            dest_col->append(*src_col.get(), start_row, copy_number);
        }
    }
}

void CrossJoinLeftOperator::_copy_build_rows_with_index_base_probe(vectorized::ColumnPtr& dest_col,
                                                                   vectorized::ColumnPtr& src_col, size_t start_row,
                                                                   size_t row_count) {
    if (!src_col->is_nullable()) {
        if (src_col->is_constant()) {
            // current can't reach here
            auto* const_col = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(src_col);
            // repeat the constant value for copy_number times
            _buf_selective.assign(row_count, 0);
            dest_col->append_selective(*const_col->data_column(), &_buf_selective[0], 0, row_count);
        } else {
            dest_col->append(*src_col.get(), start_row, row_count);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(row_count);
        } else {
            dest_col->append(*src_col.get(), start_row, row_count);
        }
    }
}

void CrossJoinLeftOperator::_copy_build_rows_with_index_base_build(vectorized::ColumnPtr& dest_col,
                                                                   vectorized::ColumnPtr& src_col, size_t start_row,
                                                                   size_t row_count) {
    if (!src_col->is_nullable()) {
        if (src_col->is_constant()) {
            // current can't reach here
            auto* const_col = vectorized::ColumnHelper::as_raw_column<vectorized::ConstColumn>(src_col);
            // repeat the constant value for copy_number times
            _buf_selective.assign(row_count, 0);
            dest_col->append_selective(*const_col->data_column(), &_buf_selective[0], 0, row_count);
        } else {
            dest_col->append_value_multiple_times(*src_col.get(), start_row, row_count);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(row_count);
        } else {
            dest_col->append_value_multiple_times(*src_col.get(), start_row, row_count);
        }
    }
}

/* 
 * This algorithm is the same as that CrossJoinNode, 
 * and pull_chunk, need_input, push_chunk is splited from CrossJoinNode's get_next.
 */
StatusOr<vectorized::ChunkPtr> CrossJoinLeftOperator::pull_chunk(RuntimeState* state) {
    vectorized::ChunkPtr chunk = nullptr;
    // If right table is empty, so we just return empty chunk.
    if (_total_build_rows > 0) {
        for (;;) {
            if (chunk == nullptr) {
                // we need a valid probe chunk to initialize the new chunk.
                _init_chunk(&chunk);
            }

            // need row_count to fill in chunk.
            size_t row_count = config::vector_chunk_size - chunk->num_rows();

            // means we have scan all chunks of right tables.
            // we should scan all remain rows of right table.
            // once _probe_chunk_index == _probe_chunk->num_rows() is true,
            // this condition will always true for this _probe_chunk,
            // Until _probe_chunk be done.
            if (_probe_chunk_index == _probe_chunk->num_rows()) {
                // step 2:
                // if left chunk is bigger than right, we shuld scan left based on right.
                if (_probe_chunk_index > _build_rows_remainder) {
                    if (row_count > _probe_chunk_index - _probe_rows_index) {
                        row_count = _probe_chunk_index - _probe_rows_index;
                    }

                    _copy_joined_rows_with_index_base_build(chunk, row_count, _probe_rows_index,
                                                            _beyond_threshold_build_rows_index);
                    _probe_rows_index += row_count;

                    if (_probe_rows_index == _probe_chunk_index) {
                        ++_beyond_threshold_build_rows_index;
                        _probe_rows_index = 0;
                    }

                    // _probe_chunk is done with _build_chunk.
                    if (_beyond_threshold_build_rows_index >= _total_build_rows) {
                        _probe_chunk = nullptr;
                    }
                } else {
                    // if remain rows of right is bigger than left, we should scan right based on left.
                    if (row_count > _total_build_rows - _beyond_threshold_build_rows_index) {
                        row_count = _total_build_rows - _beyond_threshold_build_rows_index;
                    }

                    _copy_joined_rows_with_index_base_probe(chunk, row_count, _probe_rows_index,
                                                            _beyond_threshold_build_rows_index);
                    _beyond_threshold_build_rows_index += row_count;

                    if (_beyond_threshold_build_rows_index == _total_build_rows) {
                        ++_probe_rows_index;
                        _beyond_threshold_build_rows_index = _build_rows_threshold;
                    }

                    // _probe_chunk is done with _build_chunk.
                    if (_probe_rows_index >= _probe_chunk_index) {
                        _probe_chunk = nullptr;
                    }
                }
            } else if (_within_threshold_build_rows_index < _build_rows_threshold) {
                // step 1:
                // we scan all chunks of right table.
                if (row_count > _build_rows_threshold - _within_threshold_build_rows_index) {
                    row_count = _build_rows_threshold - _within_threshold_build_rows_index;
                }

                _copy_joined_rows_with_index_base_probe(chunk, row_count, _probe_chunk_index,
                                                        _within_threshold_build_rows_index);
                _within_threshold_build_rows_index += row_count;
            } else {
                // step policy decision:
                DCHECK_EQ(_within_threshold_build_rows_index, _build_rows_threshold);

                if (_build_rows_threshold != 0) {
                    // scan right chunk_size rows for next row of left chunk.
                    ++_probe_chunk_index;
                    if (_probe_chunk_index < _probe_chunk->num_rows()) {
                        _within_threshold_build_rows_index = 0;
                    } else {
                        // if right table is all about chunks, means _probe_chunk is done.
                        if (_build_rows_threshold == _total_build_rows) {
                            _probe_chunk = nullptr;
                        } else {
                            _beyond_threshold_build_rows_index = _build_rows_threshold;
                            _probe_rows_index = 0;
                        }
                    }
                } else {
                    // optimized for smaller right table < 4096 rows.
                    _probe_chunk_index = _probe_chunk->num_rows();
                    _beyond_threshold_build_rows_index = _build_rows_threshold;
                    _probe_rows_index = 0;
                }
                continue;
            }

            if (chunk->num_rows() < config::vector_chunk_size && _probe_chunk != nullptr) {
                continue;
            }

            ExecNode::eval_conjuncts(_conjunct_ctxs, chunk.get());

            // we get result chunk.
            break;
        }
    } else {
        chunk = std::make_shared<vectorized::Chunk>();
        _probe_chunk = nullptr;
    }

    return chunk;
}

// (_probe_chunk == nullptr || _probe_chunk->num_rows() == 0) means that
// need take next chunk from left table.
bool CrossJoinLeftOperator::need_input() const {
    return _is_right_complete && (_total_build_rows == 0 || (_probe_chunk == nullptr || _probe_chunk->num_rows() == 0));
}

bool CrossJoinLeftOperator::is_ready() const {
    // woke from blocking througth cross join right sink operator by shared _right_table_complete_ptr.
    bool is_complete = _cross_join_context->_right_table_complete;
    if (is_complete) {
        _is_right_complete = true;
        if (_cross_join_context->_build_chunk->num_rows() > 0) {
            // Set fields for left table.
            _total_build_rows = _cross_join_context->_build_chunk->num_rows();
            _build_rows_threshold = (_total_build_rows / config::vector_chunk_size) * config::vector_chunk_size;
            _build_rows_remainder = _total_build_rows - _build_rows_threshold;
        }
    }

    return is_complete;
}

Status CrossJoinLeftOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _probe_chunk = chunk;
    _within_threshold_build_rows_index = 0;
    _probe_chunk_index = 0;
    return Status::OK();
}

void CrossJoinLeftOperatorFactory::_init_row_desc() {
    for (auto& tuple_desc : _left_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _probe_column_count++;
        }
        if (_row_descriptor.get_tuple_idx(tuple_desc->id()) != RowDescriptor::INVALID_IDX) {
            _output_probe_tuple_ids.emplace_back(tuple_desc->id());
        }
    }
    for (auto& tuple_desc : _right_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _build_column_count++;
        }
        if (_row_descriptor.get_tuple_idx(tuple_desc->id()) != RowDescriptor::INVALID_IDX) {
            _output_build_tuple_ids.emplace_back(tuple_desc->id());
        }
    }
}

Status CrossJoinLeftOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    _init_row_desc();
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc, mem_tracker));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void CrossJoinLeftOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
}

} // namespace starrocks::pipeline
