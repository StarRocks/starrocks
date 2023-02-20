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

#include "exec/stream/aggregate/agg_state_data.h"

#include "exprs/agg/stream/stream_detail_state.h"

namespace starrocks::stream {

Status AggStateData::allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                                 const std::vector<ChunkPtrOr>* result_chunks,
                                                 const Buffer<AggGroupStatePtr>& agg_group_state) const {
    int32_t j = 0;
    auto table_column_idx = intermediate_table_column_idx();
    for (auto i = 0; i < chunk_size; i++) {
        // skip if keys are already existed in map(cache)
        if (keys_not_in_map[i]) {
            DCHECK_LT(j, chunk_size);
            // replacement new
            _agg_function->create(_agg_fn_ctx, agg_group_state[i] + _agg_state_offset);
            auto& result_chunk_or = (*result_chunks)[j++];
            if (result_chunk_or.ok()) {
                // deserialize result row and allocate it to agg_state
                auto result_chunk = result_chunk_or.value();
                DCHECK_LT(table_column_idx, result_chunk->num_columns());
                auto column = result_chunk->get_column_by_index(table_column_idx);
                DCHECK_EQ(1, column->size());
                _agg_function->merge(_agg_fn_ctx, column.get(), agg_group_state[i] + _agg_state_offset, 0);
            } else {
                auto status = result_chunk_or.status();
                DCHECK(status.is_end_of_file());
                if (!status.is_end_of_file()) {
                    return status;
                }
            }
        }
    }
    DCHECK(j == (*result_chunks).size());
    return Status::OK();
}

// Restore the specific group_by_keys + agg_values's detail into the state.
Status AggStateData::allocate_detail_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                           const Column* raw_column, const std::vector<ChunkPtrOr>* result_chunks,
                                           const Buffer<AggGroupStatePtr>& agg_group_state) const {
    int32_t j = 0;
    for (auto i = 0; i < chunk_size; i++) {
        DCHECK_LT(j, chunk_size);
        if (keys_not_in_map[i]) {
            auto& result_chunk_or = (*result_chunks)[j++];
            if (result_chunk_or.ok()) {
                auto result_chunk = result_chunk_or.value();
                auto& result_columns = result_chunk->columns();
                DCHECK_EQ(result_columns.size(), 1);
                std::vector<const Column*> columns = {raw_column, result_columns[0].get()};
                _agg_function->restore_detail(_agg_fn_ctx, i, columns, agg_group_state[i] + _agg_state_offset);
            } else {
                auto status = result_chunk_or.status();
                DCHECK(status.is_end_of_file());
                if (!status.is_end_of_file()) {
                    return status;
                }
            }
        }
    }
    DCHECK(j == (*result_chunks).size());
    return Status::OK();
}

Status AggStateData::process_chunk(size_t chunk_size, const StreamRowOp* ops,
                                   std::vector<std::vector<const Column*>>& raw_columns,
                                   const Buffer<AggDataPtr>& agg_group_state) const {
    auto* columns = (raw_columns[agg_func_id()]).data();
    for (int i = 0; i < chunk_size; i++) {
        auto op = ops[i];
        switch (op) {
        case StreamRowOp::OP_INSERT:
        case StreamRowOp::OP_UPDATE_BEFORE:
            _agg_function->update(_agg_fn_ctx, columns, agg_group_state[i] + _agg_state_offset, i);
            break;
        case StreamRowOp::OP_DELETE:
        case StreamRowOp::OP_UPDATE_AFTER:
            _agg_function->retract(_agg_fn_ctx, columns, agg_group_state[i] + _agg_state_offset, i);
            break;
        }
    }
    return Status::OK();
}

Status AggStateData::output_result(size_t chunk_size, const Columns& group_by_columns,
                                   const Buffer<AggDataPtr>& agg_group_data, const StateTable* detail_state_table,
                                   Column* to) const {
    if (detail_state_table && is_detail_agg_state()) {
        DCHECK(detail_state_table);
        UInt8ColumnPtr is_sync_col = UInt8Column::create();
        for (size_t i = 0; i < chunk_size; i++) {
            _agg_function->output_is_sync(_agg_fn_ctx, chunk_size, is_sync_col.get(),
                                          agg_group_data[i] + _agg_state_offset);
        }

        uint8_t* is_sync_data = is_sync_col->mutable_raw_data();
        // if need sync, query data from detail table.
        for (size_t i = 0; i < chunk_size; i++) {
            if (!is_sync_data[i]) {
                continue;
            }
            DatumRow keys;
            for (auto j = 0; j < group_by_columns.size(); j++) {
                keys.push_back(group_by_columns[j]->get(i));
            }
            auto chunk_iter_or = detail_state_table->prefix_scan(keys);
            if (chunk_iter_or.ok()) {
                auto& chunk_iter = chunk_iter_or.value();
                while (true) {
                    auto t_chunk = ChunkHelper::new_chunk(chunk_iter->schema(), 64);
                    auto status = chunk_iter->get_next(t_chunk.get());
                    if (status.is_end_of_file()) {
                        break;
                    }
                    if (!status.ok()) {
                        return status;
                    }
                    const auto& columns = t_chunk->columns();
                    _agg_function->restore_all_details(_agg_fn_ctx, agg_group_data[i] + _agg_state_offset,
                                                       t_chunk->num_rows(), columns);
                }
                chunk_iter->close();
            } else {
                if (!chunk_iter_or.status().is_end_of_file()) {
                    return chunk_iter_or.status();
                }
            }
        }
    }

    _agg_function->batch_finalize(_agg_fn_ctx, chunk_size, agg_group_data, _agg_state_offset, to);
    return Status::OK();
}

Status AggStateData::output_detail(size_t chunk_size, const Buffer<AggDataPtr>& agg_group_data, const Columns& to,
                                   Column* count) const {
    for (size_t i = 0; i < chunk_size; i++) {
        _agg_function->output_detail(_agg_fn_ctx, agg_group_data[i] + _agg_state_offset, to, count);
    }
    return Status::OK();
}

Status IntermediateAggGroupState::process_chunk(size_t chunk_size, const Buffer<DatumRow>& non_found_keys,
                                                const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                                std::vector<std::vector<const Column*>>& raw_columns,
                                                const Buffer<AggDataPtr>& agg_group_state) const {
    DCHECK(!_agg_states.empty());
    DCHECK(_state_table);
    auto result_chunks = _state_table->seek(non_found_keys);
    for (auto& agg_state : _agg_states) {
        // Allocate state by using intermediate states.
        RETURN_IF_ERROR(
                agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map, &result_chunks, agg_group_state));
        // Allocate state by using intermediate states.
        RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, ops, raw_columns, agg_group_state));
    }
    return Status::OK();
}

Status IntermediateAggGroupState::output_changes(size_t chunk_size, const Columns& group_by_columns,
                                                 const Buffer<AggDataPtr>& agg_group_state,
                                                 Columns& agg_intermediate_columns) const {
    for (auto& agg_state : _agg_states) {
        auto& agg_fn_type = agg_state->agg_fn_type();
        auto* agg_func = agg_state->agg_function();
        auto agg_col = ColumnHelper::create_column(agg_fn_type.serde_type, agg_fn_type.has_nullable_child);
        agg_col->reserve(chunk_size);
        agg_func->batch_serialize(agg_state->agg_fn_ctx(), chunk_size, agg_group_state, agg_state->agg_state_offset(),
                                  agg_col.get());
        agg_intermediate_columns.emplace_back(std::move(agg_col));
    }
    return Status::OK();
}

// TODO: Support const raw_columns
Status DetailAggGroupState::process_chunk(size_t chunk_size, const Buffer<DatumRow>& non_found_keys,
                                          const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                                          std::vector<std::vector<const Column*>>& raw_columns,
                                          const Buffer<AggDataPtr>& agg_group_state) const {
    DCHECK(!_agg_states.empty());
    std::vector<ChunkPtrOr> intermediate_result_chunks;
    std::vector<ChunkPtrOr> result_chunks;
    for (size_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
        auto& detail_state_table = _detail_state_tables[i];
        auto detail_seek_keys =
                _construct_non_found_detail_keys(chunk_size, keys_not_in_map, non_found_keys, raw_columns[i]);

        // TODO: support agg multi columns.
        DCHECK_EQ(raw_columns[i].size(), 1);

        // Allocate state by using intermediate states.
        std::vector<ChunkPtrOr>* tmp_result_chunks;
        if (agg_state->state_table_kind() == AggStateTableKind::DETAIL_RESULT) {
            // Restore agg intermediate states.
            if (result_chunks.empty()) {
                result_chunks = _result_state_table->seek(non_found_keys);
            }
            tmp_result_chunks = &result_chunks;
        } else {
            DCHECK(agg_state->state_table_kind() == AggStateTableKind::DETAIL_INTERMEDIATE);
            // Restore agg intermediate states.
            DCHECK(_intermediate_state_table);
            if (intermediate_result_chunks.empty()) {
                intermediate_result_chunks = _intermediate_state_table->seek(non_found_keys);
            }
            tmp_result_chunks = &intermediate_result_chunks;
        }

        RETURN_IF_ERROR(agg_state->allocate_intermediate_state(chunk_size, keys_not_in_map, tmp_result_chunks,
                                                               agg_group_state));
        // Restore retract state from detail table, find the details for the specific group_by_keys and agg_values.
        auto detail_result_chunks = detail_state_table->seek(detail_seek_keys);
        RETURN_IF_ERROR(agg_state->allocate_detail_state(chunk_size, keys_not_in_map, raw_columns[i][0],
                                                         &detail_result_chunks, agg_group_state));
        // Process input chunks.
        RETURN_IF_ERROR(agg_state->process_chunk(chunk_size, ops, raw_columns, agg_group_state));
    }
    return Status::OK();
}

Status DetailAggGroupState::output_changes(size_t chunk_size, const Columns& group_by_columns,
                                           const Buffer<AggDataPtr>& agg_group_state,
                                           std::vector<ChunkPtr>& detail_chunks) const {
    for (size_t i = 0; i < _agg_states.size(); i++) {
        auto& agg_state = _agg_states[i];
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
        detail_result_chunk->append_column(std::move(agg_col), slot_id++);
        detail_result_chunk->append_column(std::move(count_col), slot_id++);
        detail_chunks.emplace_back(std::move(detail_result_chunk));
    }
    return Status::OK();
}

} // namespace starrocks::stream