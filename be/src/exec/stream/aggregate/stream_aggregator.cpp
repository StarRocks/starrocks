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

StreamAggregator::StreamAggregator(AggregatorParamsPtr&& params) : Aggregator(std::move(params)) {
    _count_agg_idx = _params->count_agg_idx;
}

Status StreamAggregator::_prepare_state_tables(RuntimeState* state) {
    auto key_size = _group_by_expr_ctxs.size();
    auto agg_size = _agg_fn_ctxs.size();

    DCHECK_EQ(_agg_functions.size(), agg_size);
    // TODO: state table's index should be deduced from PB.
    int32_t result_idx = 0;
    int32_t intermediate_idx = 0;
    int32_t detail_idx = 0;
    std::vector<AggStateData*> result_agg_states;
    std::vector<AggStateData*> intermediate_agg_states;
    std::vector<AggStateData*> detail_agg_states;
    for (int32_t i = 0; i < agg_size; i++) {
        auto agg_state_kind = _agg_functions[i]->agg_state_table_kind(_params->is_append_only);
        switch (agg_state_kind) {
        case AggStateTableKind::RESULT: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, result_idx});
            result_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::INTERMEDIATE: {
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            agg_state_kind, _agg_states_offsets[i],
                                                            AggStateDataParams{i, intermediate_idx, intermediate_idx});
            intermediate_agg_states.emplace_back(agg_state.get());
            _intermediate_agg_func_ids.emplace_back(i);
            _agg_func_states.emplace_back(std::move(agg_state));
            intermediate_idx++;
            break;
        }
        case AggStateTableKind::DETAIL_RESULT: {
            // For detail agg funcs, just use intermediate agg_state when no need generate retracts.
            DCHECK(!_params->is_append_only);
            auto agg_state = std::make_unique<AggStateData>(_agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i],
                                                            AggStateTableKind::DETAIL_RESULT, _agg_states_offsets[i],
                                                            AggStateDataParams{i, result_idx, detail_idx++});
            detail_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        case AggStateTableKind::DETAIL_INTERMEDIATE: {
            auto agg_state = std::make_unique<AggStateData>(
                    _agg_functions[i], _agg_fn_ctxs[i], _agg_fn_types[i], AggStateTableKind::DETAIL_INTERMEDIATE,
                    _agg_states_offsets[i], AggStateDataParams{i, intermediate_idx++, detail_idx++});
            _intermediate_agg_func_ids.emplace_back(i);
            intermediate_agg_states.emplace_back(agg_state.get());
            _agg_func_states.emplace_back(std::move(agg_state));
            break;
        }
        default:
            return Status::NotSupported("unsupported state kind");
        }
        result_idx++;
    }
    // initialize state tables
    if (_params->is_testing) {
        // result state table must be made!
        auto output_slots = _output_tuple_desc->slots();
        _result_state_table = std::make_unique<MemStateTable>(output_slots, key_size);

        // intermediate agg_state is created when intermediate/detail agg states are not empty.
        if (!_intermediate_agg_func_ids.empty()) {
            std::vector<SlotDescriptor*> intermediate_slots;
            for (int32_t i = 0; i < key_size; i++) {
                intermediate_slots.push_back(_intermediate_tuple_desc->slots()[i]);
            }
            for (auto& agg_func_id : _intermediate_agg_func_ids) {
                DCHECK_LT(agg_func_id + key_size, _intermediate_tuple_desc->slots().size());
                intermediate_slots.push_back(_intermediate_tuple_desc->slots()[agg_func_id + key_size]);
            }
            _intermediate_state_table = std::make_unique<MemStateTable>(intermediate_slots, key_size);
        }

        if (!detail_agg_states.empty()) {
            auto input_desc = state->desc_tbl().get_tuple_descriptor(0);
            auto input_slots = input_desc->slots();
            for (auto& agg_state : detail_agg_states) {
                // detail state table schema:
                // group_by_keys + agg_key -> count
                std::vector<SlotDescriptor*> detail_table_slots;
                for (auto i = 0; i < key_size; i++) {
                    detail_table_slots.push_back(input_slots[i]);
                }
                auto agg_func_idx = agg_state->agg_func_id();
                detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + agg_func_idx]);
                detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + _count_agg_idx]);
                DCHECK_EQ(detail_table_slots.size(), key_size + 2);
                auto detail_state_table = std::make_unique<MemStateTable>(detail_table_slots, key_size + 1);
                _detail_state_tables.emplace_back(std::move(detail_state_table));
            }
        }
    } else {
        // TODO: make imt state tables
        throw std::runtime_error("IMT MemStateTable is only supported for now.");
    }
    DCHECK(_result_state_table);
    if (!result_agg_states.empty()) {
        _result_agg_group =
                std::make_unique<IntermediateAggGroupState>(std::move(result_agg_states), _result_state_table.get());
    }
    if (!intermediate_agg_states.empty()) {
        DCHECK(_intermediate_state_table);
        _intermediate_agg_group = std::make_unique<IntermediateAggGroupState>(std::move(intermediate_agg_states),
                                                                              _intermediate_state_table.get());
    }
    if (!detail_agg_states.empty()) {
        // DetailAggGroup relies on both intermediate and detail state tables.
        DCHECK(!_detail_state_tables.empty());
        _detail_agg_group =
                std::make_unique<DetailAggGroupState>(std::move(detail_agg_states), _result_state_table.get(),
                                                      _intermediate_state_table.get(), _detail_state_tables);
    }
    return Status::OK();
}

Status StreamAggregator::process_chunk(StreamChunk* chunk) {
    size_t chunk_size = chunk->num_rows();
    RETURN_IF_ERROR(_evaluate_group_by_exprs(chunk));
    RETURN_IF_ERROR(evaluate_agg_fn_exprs(chunk));

    {
        SCOPED_TIMER(agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(build_hash_map_with_selection_and_allocation(chunk_size));
    }
    DCHECK_EQ(_streaming_selection.size(), chunk_size);

    // Deduce non found keys
    auto non_found_size = SIMD::count_nonzero(_streaming_selection.data(), chunk_size);
    // Reuse _non_found_keys and only resize when the vector is not equal to the real size.
    if (_non_found_keys.size() != non_found_size) {
        _non_found_keys.resize(non_found_size);
    }
    for (size_t i = 0; i < _streaming_selection.size(); i++) {
        if (_streaming_selection[i]) {
            _non_found_keys[i] = _convert_to_datum_row(_group_by_columns, i);
        }
    }
    DCHECK_EQ(_non_found_keys.size(), non_found_size);

    // batch fetch intermediate results from imt table
    auto ops = StreamChunkConverter::ops(chunk);
    if (_result_agg_group) {
        _result_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops, _agg_input_raw_columns,
                                         _tmp_agg_states);
    }
    if (_intermediate_agg_group) {
        _intermediate_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops,
                                               _agg_input_raw_columns, _tmp_agg_states);
    }
    if (_detail_agg_group) {
        _detail_agg_group->process_chunk(chunk_size, _non_found_keys, _streaming_selection, ops, _agg_input_raw_columns,
                                         _tmp_agg_states);
    }

    return Status::OK();
}

DatumRow StreamAggregator::_convert_to_datum_row(const Columns& columns, size_t row_idx) {
    DatumRow keys_row;
    keys_row.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); i++) {
        keys_row.emplace_back(columns[i]->get(row_idx));
    }
    return keys_row;
}

Status StreamAggregator::output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk) {
    ChunkPtr intermediate_chunk = std::make_shared<Chunk>();
    std::vector<ChunkPtr> detail_chunks;
    RETURN_IF_ERROR(output_changes(chunk_size, result_chunk, &intermediate_chunk, detail_chunks));
    return Status::OK();
}

Status StreamAggregator::output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                                        std::vector<ChunkPtr>& detail_chunks) {
    SCOPED_TIMER(agg_compute_timer());
    Status status;
    TRY_CATCH_BAD_ALLOC(hash_map_variant().visit([&](auto& hash_map_with_key) {
        status = _output_changes(*hash_map_with_key, chunk_size, result_chunk, intermediate_chunk, &detail_chunks);
    }));
    RETURN_IF_ERROR(status);

    // update intermediate table
    if (_intermediate_state_table) {
        _intermediate_state_table->flush(_state, (*intermediate_chunk).get());
    }
    // update result table
    DCHECK(_result_state_table);
    _result_state_table->flush(_state, (*result_chunk).get());
    if (_detail_agg_group) {
        DCHECK_EQ(_detail_agg_group->agg_states().size(), detail_chunks.size());
        for (size_t i = 0; i < _detail_agg_group->detail_state_tables().size(); i++) {
            DCHECK_LT(i, detail_chunks.size());
            auto& detail_state_table = _detail_agg_group->detail_state_tables()[i];
            detail_state_table->flush(_state, detail_chunks[i].get());
        }
    }

    return status;
}

Status StreamAggregator::reset_state(RuntimeState* state) {
    RETURN_IF_ERROR(_reset_state(state));
    return Status::OK();
}

template <typename HashMapWithKey>
Status StreamAggregator::_output_changes(HashMapWithKey& hash_map_with_key, int32_t chunk_size,
                                         StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                                         std::vector<ChunkPtr>* detail_chunks) {
    SCOPED_TIMER(_agg_stat->get_results_timer);

    // initialize _it_hash
    if (!_it_hash.has_value()) {
        hash_map_variant().visit([&](auto& hash_map_with_key) { _it_hash = _state_allocator.begin(); });
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
        SCOPED_TIMER(_agg_stat->agg_append_timer);
        // only output intermediate result if intermediate agg group is not empty
        if (intermediate_chunk != nullptr && !_intermediate_agg_func_ids.empty()) {
            RETURN_IF_ERROR(_output_intermediate_changes(read_index, group_by_columns, intermediate_chunk));
        }
        // always output result
        RETURN_IF_ERROR(_output_result_changes(read_index, group_by_columns, result_chunk));
        // only output detail result if detail agg group is not empty
        if (detail_chunks != nullptr && !_detail_state_tables.empty()) {
            RETURN_IF_ERROR(
                    _detail_agg_group->output_changes(read_index, group_by_columns, _tmp_agg_states, *detail_chunks));
        }
    }

    // NOTE: StreamAggregate do not support output NULL keys which is different from OLAP Engine.
    _is_ht_eos = (it == end);
    _it_hash = it;

    _num_rows_returned += read_index;
    _num_rows_processed += read_index;

    return Status::OK();
}

Status StreamAggregator::_output_intermediate_changes(int32_t chunk_size, const Columns& group_by_columns,
                                                      ChunkPtr* intermediate_chunk) {
    Columns agg_intermediate_columns;
    _intermediate_agg_group->output_changes(chunk_size, group_by_columns, _tmp_agg_states, agg_intermediate_columns);
    *intermediate_chunk = _build_output_chunk(group_by_columns, agg_intermediate_columns, true);
    return Status::OK();
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

    // TODO: use `batch_finalize_with_selection` to filter count=0 rows.
    for (auto& agg_state : _agg_func_states) {
        auto* to = post_agg_result_columns[agg_state->agg_func_id()].get();
        StateTable* state_table = nullptr;
        if (agg_state->is_detail_agg_state()) {
            state_table = _detail_agg_group->detail_state_tables()[agg_state->detail_table_column_idx()].get();
        }
        RETURN_IF_ERROR(agg_state->output_result(chunk_size, group_by_columns, _tmp_agg_states, state_table, to));
    }
    *post_chunk_result = _build_output_chunk(group_by_columns, post_agg_result_columns, false);
    return Status::OK();
}

namespace {

void append_prev_result(ChunkPtr result_chunk, const Columns& group_by_columns, size_t row_idx, ChunkPtr prev_result) {
    DCHECK_EQ(result_chunk->num_columns(), group_by_columns.size() + prev_result->num_columns());
    auto columns = result_chunk->columns();
    for (size_t i = 0; i < group_by_columns.size(); i++) {
        columns[i]->append(*group_by_columns[i], row_idx, 1);
    }
    auto agg_columns = prev_result->columns();
    for (size_t i = group_by_columns.size(); i < result_chunk->num_columns(); i++) {
        columns[i]->append(*agg_columns[i - group_by_columns.size()], 0, 1);
    }
}

} // namespace

Status StreamAggregator::_output_result_changes_with_retract(size_t chunk_size, const Columns& group_by_columns,
                                                             StreamChunkPtr* result_chunk_with_ops) {
    // 1. compute final result (without considering previous result)
    ChunkPtr final_result_chunk;
    RETURN_IF_ERROR(_output_final_result_with_retract(chunk_size, group_by_columns, &final_result_chunk));
    DCHECK_EQ(chunk_size, final_result_chunk->num_rows());
    // compute agg count to decide whehter to generate retract info.
    auto agg_count_column = down_cast<const Int64Column*>(
            final_result_chunk->get_column_by_index(_group_by_columns.size() + _count_agg_idx).get());
    ;
    auto agg_count_column_data = agg_count_column->get_data();

    // 2. seek previous results from result state table.
    std::vector<DatumRow> keys;
    keys.reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; i++) {
        keys.emplace_back(_convert_to_datum_row(group_by_columns, i));
    }
    auto result_ors = _result_state_table->seek(keys);
    DCHECK_EQ(result_ors.size(), chunk_size);

    // 3. generate result chunks
    Int8ColumnPtr ops = Int8Column::create();
    ChunkPtr result_chunk = final_result_chunk->clone_empty();
    for (size_t i = 0; i < chunk_size; i++) {
        auto result = result_ors[i];

        if (!result.ok()) {
            // there are no old results before.
            if (!result.status().is_end_of_file()) {
                return result.status();
            }

            // append new row only if count > 0
            if (agg_count_column_data[i] > 0) {
                result_chunk->append(*final_result_chunk, i, 1);
                ops->append(INSERT_OP);
            }
        } else {
            auto prev_result = result.value();
            DCHECK_EQ(prev_result->num_rows(), 1);
            DCHECK_LE(_agg_functions.size(), prev_result->num_columns());

            if (agg_count_column_data[i] > 0) {
                // update before
                append_prev_result(result_chunk, group_by_columns, i, prev_result);
                ops->append(UPDATE_BEFORE_OP);
                // update after
                result_chunk->append(*final_result_chunk, i, 1);
                ops->append(UPDATE_AFTER_OP);
            } else {
                // retract old
                append_prev_result(result_chunk, group_by_columns, i, prev_result);
                ops->append(DELETE_OP);
            }
        }
    }
    *result_chunk_with_ops = StreamChunkConverter::make_stream_chunk(result_chunk, ops);
    return Status::OK();
}

Status StreamAggregator::_output_result_changes_without_retract(size_t chunk_size, const Columns& group_by_columns,
                                                                StreamChunkPtr* result_chunk) {
    // agg result
    Columns agg_result_columns = _create_agg_result_columns(chunk_size, false);
    for (auto& agg_state : _agg_func_states) {
        auto* to = agg_result_columns[agg_state->agg_func_id()].get();
        RETURN_IF_ERROR(agg_state->output_result(chunk_size, group_by_columns, _tmp_agg_states, nullptr, to));
    }

    // op col
    Int8ColumnPtr ops = Int8Column::create();
    ops->append_value_multiple_times(&INSERT_OP, chunk_size);

    auto final_result_chunk = _build_output_chunk(group_by_columns, agg_result_columns, false);
    *result_chunk = StreamChunkConverter::make_stream_chunk(std::move(final_result_chunk), std::move(ops));

    return Status::OK();
}

void StreamAggregator::close(RuntimeState* state) {
    Aggregator::close(state);
}

} // namespace starrocks::stream
