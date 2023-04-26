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
#include "fmt/format.h"

namespace starrocks::stream {

Status AggStateData::allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                                 const StateTableResult* state_result,
                                                 const Buffer<AggGroupStatePtr>& agg_group_state) const {
    DCHECK(state_result);
    auto& found = state_result->found;
    auto& result_chunk = state_result->result_chunk;
    DCHECK_EQ(found.size(), chunk_size);

    auto table_column_idx = intermediate_table_column_idx();
    // deserialize result row and allocate it to agg_state
    DCHECK_LT(table_column_idx, result_chunk->num_columns());
    Column* column = result_chunk->get_column_by_index(table_column_idx).get();

    size_t j = 0;
    for (size_t i = 0; i < chunk_size; i++) {
        // skip if keys are already existed in map(cache)
        if (keys_not_in_map[i]) {
            _agg_function->create(_agg_fn_ctx, agg_group_state[i] + _agg_state_offset);
            if (found[i]) {
                _agg_function->merge(_agg_fn_ctx, column, agg_group_state[i] + _agg_state_offset, j++);
            }
        }
    }
    DCHECK_EQ(j, result_chunk->num_rows());
    return Status::OK();
}

// Restore the specific group_by_keys + agg_values's detail into the state.
Status AggStateData::allocate_detail_state(size_t chunk_size, const Column* raw_column,
                                           const StateTableResult* state_result,
                                           const Buffer<AggGroupStatePtr>& agg_group_state) const {
    auto& found = state_result->found;
    auto& result_chunk = state_result->result_chunk;
    DCHECK_EQ(found.size(), chunk_size);
    DCHECK_EQ(1, result_chunk->num_columns());

    size_t j = 0;
    for (auto i = 0; i < chunk_size; i++) {
        if (found[i]) {
            auto& result_columns = result_chunk->columns();
            _agg_function->restore_detail(_agg_fn_ctx, raw_column, i, result_columns[0].get(), j++,
                                          agg_group_state[i] + _agg_state_offset);
        }
    }
    DCHECK_EQ(result_chunk->num_rows(), j);

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
        UInt8Column::Ptr is_sync_col = UInt8Column::create();
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
            auto chunk_iter_or = detail_state_table->prefix_scan(group_by_columns, i);
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

} // namespace starrocks::stream