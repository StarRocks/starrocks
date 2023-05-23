// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/crossjoin/cross_join_left_operator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

void CrossJoinLeftOperator::_init_chunk(vectorized::ChunkPtr* chunk, RuntimeState* state) {
    vectorized::ChunkPtr new_chunk = std::make_shared<vectorized::Chunk>();

    // init columns for the new chunk from _probe_chunk and _curr_build_chunk
    for (size_t i = 0; i < _probe_column_count; ++i) {
        SlotDescriptor* slot = _col_types[i];
        vectorized::ColumnPtr& src_col = _probe_chunk->get_column_by_slot_id(slot->id());
        auto new_col = vectorized::ColumnHelper::create_column(slot->type(), src_col->is_nullable());
        new_chunk->append_column(std::move(new_col), slot->id());
    }
    for (size_t i = 0; i < _build_column_count; ++i) {
        SlotDescriptor* slot = _col_types[_probe_column_count + i];
        vectorized::ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot->id());
        vectorized::ColumnPtr new_col = vectorized::ColumnHelper::create_column(slot->type(), src_col->is_nullable());
        new_chunk->append_column(std::move(new_col), slot->id());
    }

    *chunk = std::move(new_chunk);
    (*chunk)->reserve(state->chunk_size());
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
        vectorized::ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot->id());
        _copy_build_rows_with_index_base_probe(dest_col, src_col, build_index, row_count);
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
        DCHECK(_curr_build_chunk != nullptr);
        vectorized::ColumnPtr& src_col = _curr_build_chunk->get_column_by_slot_id(slot->id());
        _copy_build_rows_with_index_base_build(dest_col, src_col, build_index, row_count);
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
            dest_col->append_value_multiple_times(*src_col.get(), start_row, copy_number, false);
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
            dest_col->append_value_multiple_times(*src_col.get(), start_row, copy_number, false);
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
            dest_col->append_shallow_copy(*src_col.get(), start_row, row_count);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(row_count);
        } else {
            dest_col->append_shallow_copy(*src_col.get(), start_row, row_count);
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
            dest_col->append_value_multiple_times(*src_col.get(), start_row, row_count, false);
        }
    } else {
        if (src_col->is_constant()) {
            // current can't reach here
            dest_col->append_nulls(row_count);
        } else {
            dest_col->append_value_multiple_times(*src_col.get(), start_row, row_count, false);
        }
    }
}

void CrossJoinLeftOperator::_select_build_chunk(const int32_t unprocessed_build_index, RuntimeState* state) {
    const int32 num_build_chunks = _cross_join_context->num_build_chunks();

    // Find the first build chunk which contains data from unprocessed_build_index.
    for (_curr_build_index = unprocessed_build_index; _curr_build_index < num_build_chunks; ++_curr_build_index) {
        _curr_build_chunk = _cross_join_context->get_build_chunk(_curr_build_index);
        if (_curr_build_chunk != nullptr && _curr_build_chunk->num_rows() > 0) {
            break;
        }
    }

    // Reset the indexes for traversing on _curr_build_chunk and _probe_chunk.
    if (_curr_build_index < num_build_chunks) {
        DCHECK(_curr_build_chunk != nullptr && _curr_build_chunk->num_rows() > 0);

        _curr_total_build_rows = _curr_build_chunk->num_rows();
        _curr_build_rows_threshold = (_curr_total_build_rows / state->chunk_size()) * state->chunk_size();
        _curr_build_rows_remainder = _curr_total_build_rows - _curr_build_rows_threshold;

        _within_threshold_build_rows_index = 0;
        _beyond_threshold_build_rows_index = 0;
        _probe_chunk_index = 0;
        _probe_rows_index = 0;
    }
}

/*
 * This algorithm is the same as that CrossJoinNode,
 * and pull_chunk, need_input, push_chunk is splited from CrossJoinNode's get_next.
 */
StatusOr<vectorized::ChunkPtr> CrossJoinLeftOperator::pull_chunk(RuntimeState* state) {
    vectorized::ChunkPtr chunk = nullptr;
    // we need a valid probe chunk to initialize the new chunk.
    _init_chunk(&chunk, state);

    for (;;) {
        // need row_count to fill in chunk.
        size_t row_count = state->chunk_size() - chunk->num_rows();

        // means we have scan all chunks of right tables.
        // we should scan all remain rows of right table.
        // once _probe_chunk_index == _probe_chunk->num_rows() is true,
        // this condition will always true for this _probe_chunk,
        // Until _probe_chunk be done.
        if (_probe_chunk_index == _probe_chunk->num_rows()) {
            // get left chunk's size.
            size_t probe_chunk_size = _probe_chunk_index;
            // step 2:
            // if left chunk is bigger than right, we shuld scan left based on right.
            if (probe_chunk_size > _curr_build_rows_remainder) {
                if (row_count > probe_chunk_size - _probe_rows_index) {
                    row_count = probe_chunk_size - _probe_rows_index;
                }

                _copy_joined_rows_with_index_base_build(chunk, row_count, _probe_rows_index,
                                                        _beyond_threshold_build_rows_index);
                _probe_rows_index += row_count;
                // if _probe_rows_index is equal with probe_chunk_size,
                // means left chunk is done with the right row, so we get next right row.
                if (_probe_rows_index == probe_chunk_size) {
                    ++_beyond_threshold_build_rows_index;
                    _probe_rows_index = 0;
                }

                // _probe_chunk is done with _build_chunk.
                if (_beyond_threshold_build_rows_index >= _curr_total_build_rows) {
                    _select_build_chunk(_curr_build_index + 1, state);
                }
            } else {
                // if remain rows of right is bigger than left, we should scan right based on left.
                if (row_count > _curr_total_build_rows - _beyond_threshold_build_rows_index) {
                    row_count = _curr_total_build_rows - _beyond_threshold_build_rows_index;
                }

                _copy_joined_rows_with_index_base_probe(chunk, row_count, _probe_rows_index,
                                                        _beyond_threshold_build_rows_index);
                _beyond_threshold_build_rows_index += row_count;

                if (_beyond_threshold_build_rows_index == _curr_total_build_rows) {
                    ++_probe_rows_index;
                    _beyond_threshold_build_rows_index = _curr_build_rows_threshold;
                }

                // _probe_chunk is done with _build_chunk.
                if (_probe_rows_index >= probe_chunk_size) {
                    _select_build_chunk(_curr_build_index + 1, state);
                }
            }
        } else if (_within_threshold_build_rows_index < _curr_build_rows_threshold) {
            // step 1:
            // we scan all chunks of right table.
            if (row_count > _curr_build_rows_threshold - _within_threshold_build_rows_index) {
                row_count = _curr_build_rows_threshold - _within_threshold_build_rows_index;
            }

            _copy_joined_rows_with_index_base_probe(chunk, row_count, _probe_chunk_index,
                                                    _within_threshold_build_rows_index);
            _within_threshold_build_rows_index += row_count;
        } else {
            // step policy decision:
            DCHECK_EQ(_within_threshold_build_rows_index, _curr_build_rows_threshold);

            bool has_build_chunk = true;
            if (_curr_build_rows_threshold != 0) {
                // scan right chunk_size rows for next row of left chunk.
                ++_probe_chunk_index;
                if (_probe_chunk_index < _probe_chunk->num_rows()) {
                    _within_threshold_build_rows_index = 0;
                } else {
                    // if right table is all about chunks, means _probe_chunk is done.
                    if (_curr_build_rows_threshold == _curr_total_build_rows) {
                        _select_build_chunk(_curr_build_index + 1, state);
                        has_build_chunk = !_is_curr_probe_chunk_finished();
                    } else {
                        _beyond_threshold_build_rows_index = _curr_build_rows_threshold;
                        _probe_rows_index = 0;
                    }
                }
            } else {
                // optimized for smaller right table < 4096 rows.
                _probe_chunk_index = _probe_chunk->num_rows();
                _beyond_threshold_build_rows_index = _curr_build_rows_threshold;
                _probe_rows_index = 0;
            }

            if (has_build_chunk) {
                continue;
            }
        }

        // When the chunk is full or the current probe chunk is finished
        // crossing join with build chunks, we can output this chunk.
        if (chunk->num_rows() >= state->chunk_size() || _is_curr_probe_chunk_finished()) {
            RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, chunk.get()));
            break;
        }
    }

    return chunk;
}

Status CrossJoinLeftOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _probe_chunk = chunk;
    _select_build_chunk(0, state);

    return Status::OK();
}

Status CrossJoinLeftOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _cross_join_context->incr_prober();

    return Status::OK();
}

Status CrossJoinLeftOperator::set_finished(RuntimeState* state) {
    return _cross_join_context->finish_one_left_prober(state);
}

void CrossJoinLeftOperatorFactory::_init_row_desc() {
    for (auto& tuple_desc : _left_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _probe_column_count++;
        }
    }
    for (auto& tuple_desc : _right_row_desc.tuple_descriptors()) {
        for (auto& slot : tuple_desc->slots()) {
            _col_types.emplace_back(slot);
            _build_column_count++;
        }
    }
}

Status CrossJoinLeftOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependencyFactory::prepare(state));

    _init_row_desc();
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    return Status::OK();
}

void CrossJoinLeftOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);

    OperatorWithDependencyFactory::close(state);
}

} // namespace starrocks::pipeline
