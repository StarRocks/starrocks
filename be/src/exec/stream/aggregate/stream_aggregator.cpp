// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/stream/aggregate/stream_aggregator.h"

#include "column/column_helper.h"
#include "exec/stream/state/mem_state_table.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks::stream {

namespace {

void append_prev_result(ChunkPtr result_chunk, const Columns& group_by_columns, size_t row_idx, ChunkPtr prev_result,
                        size_t prev_result_idx) {
    DCHECK_EQ(result_chunk->num_columns(), group_by_columns.size() + prev_result->num_columns());
    auto columns = result_chunk->columns();
    for (size_t i = 0; i < group_by_columns.size(); i++) {
        columns[i]->append(*group_by_columns[i], row_idx, 1);
    }
    auto agg_columns = prev_result->columns();
    for (size_t i = group_by_columns.size(); i < result_chunk->num_columns(); i++) {
        columns[i]->append(*agg_columns[i - group_by_columns.size()], prev_result_idx, 1);
    }
}

} // namespace

StreamAggregator::StreamAggregator(AggregatorParamsPtr params) : Aggregator(std::move(params)) {
    _count_agg_idx = _params->count_agg_idx;
}

Status StreamAggregator::prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile) {
    RETURN_IF_ERROR(Aggregator::prepare(state, pool, runtime_profile));
    return Status::OK();
}

Status StreamAggregator::_prepare_state_tables(RuntimeState* state) {
    auto agg_size = _agg_fn_ctxs.size();

    DCHECK_EQ(_agg_functions.size(), agg_size);
    int32_t result_idx = 0;
    int32_t intermediate_idx = 0;
    int32_t detail_idx = 0;
    std::vector<AggStateDataUPtr> agg_func_states;
    for (int32_t i = 0; i < agg_size; i++) {
        auto agg_state_kind = _agg_functions[i]->agg_state_table_kind(_params->is_append_only);
        switch (agg_state_kind) {
        case AggStateTableKind::RESULT: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, result_idx});
            agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::INTERMEDIATE: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, intermediate_idx, intermediate_idx});
            agg_func_states.emplace_back(std::move(agg_state));
            intermediate_idx++;
            break;
        }
        case AggStateTableKind::DETAIL_RESULT: {
            // For detail agg funcs, just use intermediate agg_state when no need generate retracts.
            DCHECK(!_params->is_append_only);
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, detail_idx++});
            agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::DETAIL_INTERMEDIATE: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, intermediate_idx++, detail_idx++});
            agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        default:
            LOG(WARNING) << "unsupported state kind, agg function id=" << i;
            return Status::NotSupported("unsupported state kind");
        }
        result_idx++;
    }

    // Prepare agg group state
    _agg_group_state = std::make_unique<AggGroupState>(std::move(agg_func_states), _params, _output_tuple_desc,
                                                       _intermediate_tuple_desc);
    RETURN_IF_ERROR(_agg_group_state->prepare(state));

    return Status::OK();
}

Status StreamAggregator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Aggregator::open(state));
    RETURN_IF_ERROR(_prepare_state_tables(state));
    return _agg_group_state->open(state);
}

Status StreamAggregator::process_chunk(StreamChunk* chunk) {
    size_t chunk_size = chunk->num_rows();
    _reset_exprs();
    RETURN_IF_ERROR(_evaluate_group_by_exprs(chunk));
    RETURN_IF_ERROR(evaluate_agg_fn_exprs(chunk));

    {
        SCOPED_TIMER(agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(build_hash_map_with_selection_and_allocation(chunk_size));
    }
    DCHECK_EQ(_streaming_selection.size(), chunk_size);

    auto ops = StreamChunkConverter::ops(chunk);
    RETURN_IF_ERROR(_agg_group_state->process_chunk(chunk_size, _group_by_columns, _streaming_selection, ops,
                                                    _agg_input_columns, _agg_input_raw_columns, _tmp_agg_states));
    return Status::OK();
}

Status StreamAggregator::output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk) {
    ChunkPtr intermediate_chunk = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RETURN_IF_ERROR(output_changes_internal(chunk_size, result_chunk, &intermediate_chunk, detail_chunks));
    return Status::OK();
}

Status StreamAggregator::output_changes_internal(int32_t chunk_size, StreamChunkPtr* result_chunk,
                                                 ChunkPtr* intermediate_chunk, std::vector<ChunkPtr>& detail_chunks) {
    SCOPED_TIMER(_agg_stat->get_results_timer);
    RETURN_IF_ERROR(hash_map_variant().visit([&](auto& variant_value) {
        auto& hash_map_with_key = *variant_value;
        using HashMapWithKey = std::remove_reference_t<decltype(hash_map_with_key)>;

        // initialize _it_hash
        if (!_it_hash.has_value()) {
            _it_hash = _state_allocator.begin();
        }

        auto it = std::any_cast<RawHashTableIterator>(_it_hash);
        auto end = _state_allocator.end();

        const auto hash_map_size = _hash_map_variant.size();
        auto num_rows = std::min<size_t>(hash_map_size - _num_rows_processed, chunk_size);
        Columns group_by_columns = _create_group_by_columns(num_rows);
        int32_t read_index = 0;
        {
            SCOPED_TIMER(_agg_stat->iter_timer);
            hash_map_with_key.results.resize(num_rows);
            // get key/value from hashtable
            while ((it != end) & (read_index < num_rows)) {
                auto* value = it.value();
                hash_map_with_key.results[read_index] = *reinterpret_cast<typename HashMapWithKey::KeyType*>(value);
                // Reuse _tmp_agg_states to store state pointer address
                _tmp_agg_states[read_index] = value;
                ++read_index;
                it.next();
            }
        }

        { hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns, read_index); }

        {
            // output intermediate and detail tables' change.
            RETURN_IF_ERROR(_agg_group_state->output_changes(read_index, group_by_columns, _tmp_agg_states,
                                                             intermediate_chunk, &detail_chunks));
            // output result state table changes
            RETURN_IF_ERROR(_output_result_changes(read_index, group_by_columns, result_chunk));
        }

        // NOTE: StreamAggregate do not support output NULL keys which is different from OLAP Engine.
        _is_ht_eos = (it == end);
        _it_hash = it;

        _num_rows_returned += read_index;
        _num_rows_processed += read_index;

        return Status::OK();
    }));

    // update incremental state into state table
    RETURN_IF_ERROR(_agg_group_state->write(_state, result_chunk, intermediate_chunk, detail_chunks));
    return Status::OK();
}

Status StreamAggregator::reset_state(RuntimeState* state) {
    return _reset_state(state, true);
}

Status StreamAggregator::reset_epoch(RuntimeState* state) {
    return _agg_group_state->reset_epoch(state);
}

Status StreamAggregator::_output_result_changes(int32_t chunk_size, const Columns& group_by_columns,
                                                StreamChunkPtr* result_chunk) {
    if (_params->is_generate_retract) {
        RETURN_IF_ERROR(_output_result_changes_with_retract(chunk_size, group_by_columns, result_chunk));
    } else {
        RETURN_IF_ERROR(_output_result_changes_without_retract(chunk_size, group_by_columns, result_chunk));
    }

    return Status::OK();
}

Status StreamAggregator::_output_final_result_with_retract(size_t chunk_size, const Columns& group_by_columns,
                                                           ChunkPtr* post_chunk_result) {
    Columns post_agg_result_columns = _create_agg_result_columns(chunk_size, false);
    RETURN_IF_ERROR(
            _agg_group_state->output_results(chunk_size, group_by_columns, _tmp_agg_states, post_agg_result_columns));
    *post_chunk_result = _build_output_chunk(group_by_columns, post_agg_result_columns, false);
    return Status::OK();
}

Status StreamAggregator::_output_result_changes_with_retract(size_t chunk_size, const Columns& group_by_columns,
                                                             StreamChunkPtr* result_chunk_with_ops) {
    // 1. compute final result (without considering previous result)
    ChunkPtr final_result_chunk;
    RETURN_IF_ERROR(_output_final_result_with_retract(chunk_size, group_by_columns, &final_result_chunk));
    DCHECK_EQ(chunk_size, final_result_chunk->num_rows());
    // compute agg count to decide whehter to generate retract info.
    auto agg_count_column = down_cast<const Int64Column*>(
            final_result_chunk->get_column_by_index(_group_by_columns.size() + _count_agg_idx).get());
    auto agg_count_column_data = agg_count_column->get_data();

    // 2. seek previous results from result state table.
    StateTableResult prev_state_result;
    RETURN_IF_ERROR(_agg_group_state->output_prev_state_results(group_by_columns, prev_state_result));

    auto& found_in_prev = prev_state_result.found;
    auto& prev_result = prev_state_result.result_chunk;
    DCHECK_EQ(found_in_prev.size(), chunk_size);
    DCHECK_LE(_agg_functions.size(), prev_result->num_columns());

    // 3. generate result chunks
    Int8ColumnPtr ops = Int8Column::create();
    ChunkPtr result_chunk = final_result_chunk->clone_empty();
    size_t j = 0;
    for (size_t i = 0; i < chunk_size; i++) {
        if (!found_in_prev[i]) {
            // append new row only if count > 0
            if (agg_count_column_data[i] > 0) {
                result_chunk->append(*final_result_chunk, i, 1);
                ops->append(INSERT_OP);
            }
        } else {
            if (agg_count_column_data[i] > 0) {
                // update before
                append_prev_result(result_chunk, group_by_columns, i, prev_result, j++);
                ops->append(UPDATE_BEFORE_OP);
                // update after
                result_chunk->append(*final_result_chunk, i, 1);
                ops->append(UPDATE_AFTER_OP);
            } else {
                // retract old
                append_prev_result(result_chunk, group_by_columns, i, prev_result, j++);
                ops->append(DELETE_OP);
            }
        }
    }
    DCHECK_EQ(prev_result->num_rows(), j);
    *result_chunk_with_ops = StreamChunkConverter::make_stream_chunk(result_chunk, ops);
    return Status::OK();
}

Status StreamAggregator::_output_result_changes_without_retract(size_t chunk_size, const Columns& group_by_columns,
                                                                StreamChunkPtr* result_chunk) {
    // agg result
    Columns agg_result_columns = _create_agg_result_columns(chunk_size, false);
    RETURN_IF_ERROR(
            _agg_group_state->output_results(chunk_size, group_by_columns, _tmp_agg_states, agg_result_columns));

    // op col
    Int8ColumnPtr ops = Int8Column::create();
    ops->append_value_multiple_times(&INSERT_OP, chunk_size);

    auto final_result_chunk = _build_output_chunk(group_by_columns, agg_result_columns, false);
    *result_chunk = StreamChunkConverter::make_stream_chunk(std::move(final_result_chunk), std::move(ops));

    return Status::OK();
}

Status StreamAggregator::commit_epoch(RuntimeState* state) {
    return _agg_group_state->commit_epoch(state);
}

} // namespace starrocks::stream
