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

#include "exec/stream/aggregate/agg_group_state.h"

#include "exprs/agg/stream/stream_detail_state.h"
#include "fmt/format.h"

namespace starrocks::stream {

AggGroupState::AggGroupState(std::vector<AggStateDataUPtr>&& agg_states, const AggregatorParamsPtr& params,
                             const TupleDescriptor* output_tuple_desc, const TupleDescriptor* intermediate_tuple_desc)
        : _agg_states(std::move(agg_states)),
          _params(params),
          _output_tuple_desc(output_tuple_desc),
          _intermediate_tuple_desc(intermediate_tuple_desc) {}

// initialize state tables
Status AggGroupState::prepare(RuntimeState* state) {
    std::vector<AggStateData*> intermediate_agg_states;
    std::vector<AggStateData*> detail_agg_states;

    for (int32_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
        auto agg_state_kind = agg_state->state_table_kind();
        switch (agg_state_kind) {
        case AggStateTableKind::RESULT:
            break;
        case AggStateTableKind::INTERMEDIATE:
            intermediate_agg_states.emplace_back(agg_state.get());
            break;
        case AggStateTableKind::DETAIL_RESULT:
            detail_agg_states.emplace_back(agg_state.get());
            break;
        case AggStateTableKind::DETAIL_INTERMEDIATE:
            intermediate_agg_states.emplace_back(agg_state.get());
            detail_agg_states.emplace_back(agg_state.get());
            break;
        default:
            return Status::NotSupported("unsupported state kind");
        }
    }

    if (_params->is_testing) {
        RETURN_IF_ERROR(_prepare_mem_state_tables(state, intermediate_agg_states, detail_agg_states));
    } else {
        RETURN_IF_ERROR(_prepare_imt_state_tables(state, intermediate_agg_states, detail_agg_states));
    }

    return Status::OK();
}

Status AggGroupState::_prepare_mem_state_tables(RuntimeState* state,
                                                const std::vector<AggStateData*>& intermediate_agg_states,
                                                const std::vector<AggStateData*>& detail_agg_states) {
    auto key_size = _params->grouping_exprs.size();
    // result state table must be made!
    auto output_slots = _output_tuple_desc->slots();
    _result_state_table = std::make_unique<MemStateTable>(output_slots, key_size);

    // intermediate agg_state is created when intermediate/detail agg states are not empty.
    if (!intermediate_agg_states.empty()) {
        std::vector<SlotDescriptor*> intermediate_slots;
        for (int32_t i = 0; i < key_size; i++) {
            intermediate_slots.push_back(_intermediate_tuple_desc->slots()[i]);
        }
        for (auto& agg_state : intermediate_agg_states) {
            auto agg_func_id = agg_state->agg_func_id();
            DCHECK_LT(agg_func_id + key_size, _intermediate_tuple_desc->slots().size());
            intermediate_slots.push_back(_intermediate_tuple_desc->slots()[agg_func_id + key_size]);
        }
        _intermediate_state_table = std::make_unique<MemStateTable>(intermediate_slots, key_size);
    }

    if (!detail_agg_states.empty()) {
        auto input_desc = state->desc_tbl().get_tuple_descriptor(0);
        auto input_slots = input_desc->slots();
        auto count_agg_idx = _params->count_agg_idx;
        for (auto& agg_state : detail_agg_states) {
            // detail state table schema:
            // group_by_keys + agg_key -> count
            std::vector<SlotDescriptor*> detail_table_slots;
            for (auto i = 0; i < key_size; i++) {
                detail_table_slots.push_back(input_slots[i]);
            }
            auto agg_func_idx = agg_state->agg_func_id();
            detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + agg_func_idx]);
            detail_table_slots.push_back(_output_tuple_desc->slots()[key_size + count_agg_idx]);
            DCHECK_EQ(detail_table_slots.size(), key_size + 2);
            auto detail_state_table = std::make_unique<MemStateTable>(detail_table_slots, key_size + 1);
            _detail_state_tables.emplace_back(std::move(detail_state_table));
        }
    }
    return Status::OK();
}

Status AggGroupState::_prepare_imt_state_tables(RuntimeState* state,
                                                const std::vector<AggStateData*>& intermediate_agg_states,
                                                const std::vector<AggStateData*>& detail_agg_states) {
    if (!_params->agg_result_imt) {
        return Status::InternalError("Create imt state table failed: result imt should exist");
    }
    if (!intermediate_agg_states.empty() && !_params->agg_intermediate_imt) {
        return Status::InternalError("Create imt state table failed: intermediate imt should exist");
    }
    if (!detail_agg_states.empty() && !_params->agg_detail_imt) {
        return Status::InternalError("Create imt state table failed: detail imt should exist");
    }
    _result_state_table = std::make_unique<IMTStateTable>(*_params->agg_result_imt);
    if (_params->agg_intermediate_imt) {
        _intermediate_state_table = std::make_unique<IMTStateTable>(*_params->agg_intermediate_imt);
    }
    // TODO: Make one detail state table works.
    if (_params->agg_detail_imt) {
        return Status::InternalError("Detail state table is not supported yet.");
    }
    return Status::OK();
}

Status AggGroupState::process_chunk(size_t chunk_size, const Columns& group_by_columns,
                                    const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                    const std::vector<std::vector<ColumnPtr>>& agg_columns,
                                    std::vector<std::vector<const Column*>>& raw_columns,
                                    const Buffer<AggDataPtr>& agg_group_state) const {
    DCHECK(!_agg_states.empty());
    std::unique_ptr<StateTableResult> intermediate_result_chunks = nullptr;
    std::unique_ptr<StateTableResult> result_chunks = nullptr;

    for (size_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
        auto state_table_kind = agg_state->state_table_kind();
        // TODO: support agg multi columns.
        if (agg_columns[i].size() != 1 || raw_columns[i].size() != 1) {
            return Status::InternalError(
                    fmt::format("Multi columns for one agg is not supported: {}", agg_columns[i].size()));
        }

        // Allocate state by using intermediate states.
        StateTableResult* tmp_result_chunks;
        switch (state_table_kind) {
        case AggStateTableKind::RESULT:
        case AggStateTableKind::DETAIL_RESULT: {
            // Restore agg intermediate states.
            DCHECK(_result_state_table);
            if (!result_chunks) {
                result_chunks = std::make_unique<StateTableResult>();
                RETURN_IF_ERROR(_result_state_table->seek(group_by_columns, keys_not_in_map, *result_chunks));
            }
            tmp_result_chunks = result_chunks.get();
            break;
        }
        case AggStateTableKind::INTERMEDIATE:
        case AggStateTableKind::DETAIL_INTERMEDIATE: {
            // Restore agg intermediate states.
            DCHECK(_intermediate_state_table);
            if (!intermediate_result_chunks) {
                intermediate_result_chunks = std::make_unique<StateTableResult>();
                RETURN_IF_ERROR(_intermediate_state_table->seek(group_by_columns, keys_not_in_map,
                                                                *intermediate_result_chunks));
            }
            tmp_result_chunks = intermediate_result_chunks.get();
            break;
        }
        default:
            return Status::NotSupported("unsupported state kind");
        }

        // Allocate intermediate state for incremental compute
        RETURN_IF_ERROR(agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map, tmp_result_chunks,
                                                               agg_group_state));

        // Restore previous detail state for the specific agg_key
        if (!_detail_state_tables.empty() && agg_state->is_detail_agg_state()) {
            auto* detail_state_table = _find_detail_state_table(agg_state);

            // Construct seek keys for detail state table
            auto detail_seek_keys = group_by_columns;
            detail_seek_keys.push_back(agg_columns[i][0]);
            StateTableResult detail_result_chunk;

            // Restore retract state from detail table, find the details for the specific group_by_keys and agg_values.
            RETURN_IF_ERROR(detail_state_table->seek(detail_seek_keys, keys_not_in_map, detail_result_chunk));

            RETURN_IF_ERROR(agg_state->allocate_detail_state(chunk_size, raw_columns[i][0], &detail_result_chunk,
                                                             agg_group_state));
        }
        // Process input chunks.
        RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, ops, raw_columns, agg_group_state));
    }
    return Status::OK();
}

StateTable* AggGroupState::_find_detail_state_table(const AggStateDataUPtr& agg_state) const {
    auto detail_table_column_idx = agg_state->detail_table_column_idx();
    return _detail_state_tables[detail_table_column_idx].get();
}

Status AggGroupState::output_results(size_t chunk_size, const Columns& group_by_columns,
                                     const Buffer<AggDataPtr>& agg_group_data, Columns output_columns) const {
    // TODO: use `batch_finalize_with_selection` to filter count=0 rows.
    for (auto& agg_state : _agg_states) {
        auto* to = output_columns[agg_state->agg_func_id()].get();
        StateTable* state_table = nullptr;
        if (!_detail_state_tables.empty() && agg_state->is_detail_agg_state()) {
            state_table = _find_detail_state_table(agg_state);
        }
        RETURN_IF_ERROR(agg_state->output_result(chunk_size, group_by_columns, agg_group_data, state_table, to));
    }
    return Status::OK();
}

Status AggGroupState::output_prev_state_results(const Columns& group_by_columns, StateTableResult& prev_state_result) {
    // output previous result.
    DCHECK(_result_state_table);
    RETURN_IF_ERROR(_result_state_table->seek(group_by_columns, prev_state_result));
    return Status::OK();
}

Status AggGroupState::output_changes(size_t chunk_size, const Columns& group_by_columns,
                                     const Buffer<AggDataPtr>& agg_group_state, ChunkPtr* agg_intermediate_chunk,
                                     std::vector<ChunkPtr>* detail_chunks) const {
    std::vector<AggStateData*> agg_intermediate_states;
    for (size_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
        // only output detail result if detail agg group is not empty
        if (!_detail_state_tables.empty() && agg_state->is_detail_agg_state()) {
            // detail table's output columns
            auto& agg_func_type = agg_state->agg_fn_type();
            auto agg_col = ColumnHelper::create_column(agg_func_type.result_type,
                                                       agg_func_type.has_nullable_child & agg_func_type.is_nullable);
            auto count_col = Int64Column::create();
            Columns detail_cols{agg_col, count_col};

            // record each column's map count which is used to expand group by columns.
            auto result_count = Int64Column::create();
            agg_state->output_detail(chunk_size, agg_group_state, detail_cols, result_count.get());

            auto result_count_data = reinterpret_cast<Int64Column*>(result_count.get())->get_data();
            std::vector<uint32_t> replicate_offsets;
            replicate_offsets.reserve(result_count_data.size() + 1);
            int offset = 0;
            for (auto count : result_count_data) {
                replicate_offsets.push_back(offset);
                offset += count;
            }
            replicate_offsets.push_back(offset);

            auto detail_result_chunk = std::make_shared<Chunk>();
            SlotId slot_id = 0;
            for (size_t j = 0; j < group_by_columns.size(); j++) {
                auto replicated_col = group_by_columns[j]->replicate(replicate_offsets);
                detail_result_chunk->append_column(replicated_col, slot_id++);
            }
            // TODO: take care slot_ids.
            detail_result_chunk->append_column(std::move(agg_col), slot_id++);
            detail_result_chunk->append_column(std::move(count_col), slot_id++);
            detail_chunks->emplace_back(std::move(detail_result_chunk));
        }
        // only output intermediate result if intermediate agg group is not empty
        if (agg_state->is_intermediate_agg_state()) {
            agg_intermediate_states.push_back(agg_state.get());
        }
    }
    // Output intermediate chunk
    if (!agg_intermediate_states.empty()) {
        Columns agg_intermediate_columns;
        for (auto& agg_state : agg_intermediate_states) {
            auto& agg_fn_type = agg_state->agg_fn_type();
            auto* agg_func = agg_state->agg_function();
            auto agg_col = ColumnHelper::create_column(agg_fn_type.serde_type, agg_fn_type.has_nullable_child);
            agg_col->reserve(chunk_size);
            agg_func->batch_serialize(agg_state->agg_fn_ctx(), chunk_size, agg_group_state,
                                      agg_state->agg_state_offset(), agg_col.get());
            agg_intermediate_columns.emplace_back(std::move(agg_col));
        }
        *agg_intermediate_chunk = _build_intermediate_chunk(group_by_columns, agg_intermediate_columns);
    }
    return Status::OK();
}

ChunkPtr AggGroupState::_build_intermediate_chunk(const Columns& group_by_columns,
                                                  const Columns& agg_intermediate_columns) const {
    ChunkPtr result_chunk = std::make_shared<Chunk>();

    for (size_t i = 0; i < group_by_columns.size(); i++) {
        result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
    }
    for (size_t i = 0; i < agg_intermediate_columns.size(); i++) {
        size_t id = group_by_columns.size() + i;
        result_chunk->append_column(agg_intermediate_columns[i], _intermediate_tuple_desc->slots()[id]->id());
    }
    return result_chunk;
}

Status AggGroupState::write(RuntimeState* state, StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                            std::vector<ChunkPtr>& detail_chunks) {
    // Update result table
    DCHECK(_result_state_table);
    RETURN_IF_ERROR(_result_state_table->write(state, (*result_chunk).get()));

    // Update intermediate table
    if (_intermediate_state_table) {
        RETURN_IF_ERROR(_intermediate_state_table->write(state, (*intermediate_chunk).get()));
    }

    // Update detail tables
    DCHECK_EQ(_detail_state_tables.size(), detail_chunks.size());
    for (auto i = 0; i < _detail_state_tables.size(); i++) {
        auto& detail_state_table = _detail_state_tables[i];
        RETURN_IF_ERROR(detail_state_table->write(state, detail_chunks[i].get()));
    }
    return Status::OK();
}

} // namespace starrocks::stream