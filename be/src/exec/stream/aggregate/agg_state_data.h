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

#pragma once

#include <map>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exec/aggregator.h"
#include "exec/stream/state/state_table.h"
#include "exec/stream/stream_fdw.h"
#include "exprs/agg/aggregate.h"

namespace starrocks::stream {
using AggGroupStatePtr = uint8_t*;

struct AggStateDataParams {
    int32_t func_idx;
    int32_t intermediate_table_column_idx; // maybe result/intermediate table decided by AggStateTableKind
    int32_t detail_table_column_idx;       // detail table idx only used by Detail state kind.
};

class AggStateData {
public:
    AggStateData(const AggregateFunction* func, FunctionContext* agg_fn_ctx, const AggFunctionTypes& agg_fn_type,
                 AggStateTableKind agg_state_kind, size_t agg_state_offset, AggStateDataParams params)
            : _agg_function(func),
              _agg_fn_ctx(agg_fn_ctx),
              _agg_fn_type(agg_fn_type),
              _agg_state_kind(agg_state_kind),
              _agg_state_offset(agg_state_offset),
              _params(params) {
        VLOG_ROW << "agg_state_kind=" << agg_state_kind << " func_idx=" << _params.func_idx
                 << " intermediate_table_column_idx=" << _params.intermediate_table_column_idx
                 << " detail_table_column_idx=" << _params.detail_table_column_idx;
    }
    virtual ~AggStateData() = default;

    // Restore intermediate state if needed by using last run state table's results from
    // result/intermediate state table.
    Status allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                       const std::vector<ChunkPtrOr>* result_chunks,
                                       const Buffer<AggGroupStatePtr>& agg_group_state) const;

    // Restore detail state if needed by using last run state's result from
    // detail state table
    Status allocate_detail_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                 const Column* raw_column, const std::vector<ChunkPtrOr>* result_chunks,
                                 const Buffer<AggGroupStatePtr>& agg_group_state) const;

    Status process_chunk(size_t chunk_size, const StreamRowOp* ops,
                         std::vector<std::vector<const Column*>>& raw_columns,
                         const Buffer<AggDataPtr>& agg_group_state) const;

    Status output_result(size_t chunk_size, const Columns& group_by_columns, const Buffer<AggDataPtr>& agg_group_data,
                         const StateTable* detail_state_table, Column* to) const;

    Status output_detail(size_t chunk_size, const Buffer<AggDataPtr>& agg_group_state, const Columns& to,
                         Column* count) const;

    const AggregateFunction* agg_function() const { return _agg_function; }
    FunctionContext* agg_fn_ctx() const { return _agg_fn_ctx; }
    const int32_t agg_func_id() const { return _params.func_idx; }
    const int32_t intermediate_table_column_idx() const { return _params.intermediate_table_column_idx; }
    const int32_t detail_table_column_idx() const { return _params.detail_table_column_idx; }
    const AggStateTableKind state_table_kind() const { return _agg_state_kind; }
    const AggFunctionTypes& agg_fn_type() const { return _agg_fn_type; }
    const size_t agg_state_offset() const { return _agg_state_offset; }
    const bool is_detail_agg_state() const {
        return _agg_state_kind == AggStateTableKind::DETAIL_INTERMEDIATE ||
               _agg_state_kind == AggStateTableKind::DETAIL_RESULT;
    }

protected:
    const AggregateFunction* _agg_function;
    FunctionContext* _agg_fn_ctx;
    const AggFunctionTypes& _agg_fn_type;
    AggStateTableKind _agg_state_kind;
    // idx represents the offset in all aggregate functions, used for agg_group_state
    size_t _agg_state_offset;
    AggStateDataParams _params;
};

class IntermediateAggGroupState {
public:
    IntermediateAggGroupState(std::vector<AggStateData*> agg_states, StateTable* state_table)
            : _agg_states(std::move(agg_states)), _state_table(state_table) {
        DCHECK(state_table);
        for (auto& agg_state : _agg_states) {
            _agg_func_ids.push_back(agg_state->agg_func_id());
        }
    }

    ~IntermediateAggGroupState() = default;

    const std::vector<AggStateData*>& agg_states() const { return _agg_states; }

    const std::vector<int32_t>& agg_func_ids() const { return _agg_func_ids; }

    Status process_chunk(size_t chunk_size, const Buffer<DatumRow>& non_found_keys,
                         const Buffer<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                         std::vector<std::vector<const Column*>>& raw_columns,
                         const Buffer<AggDataPtr>& agg_group_state) const;

    Status output_changes(size_t chunk_size, const Columns& group_by_columns, const Buffer<AggDataPtr>& agg_group_state,
                          Columns& agg_intermediate_columns) const;

private:
    std::vector<AggStateData*> _agg_states;
    std::vector<int32_t> _agg_func_ids;
    StateTable* _state_table;
};

class DetailAggGroupState {
public:
    DetailAggGroupState(std::vector<AggStateData*> agg_states, StateTable* result_state_table,
                        StateTable* intermediate_state_table,
                        const std::vector<std::unique_ptr<StateTable>>& detail_state_tables)
            : _agg_states(std::move(agg_states)),
              _result_state_table(result_state_table),
              _intermediate_state_table(intermediate_state_table),
              _detail_state_tables(detail_state_tables) {
        DCHECK(_result_state_table);
        DCHECK(!_detail_state_tables.empty());
        DCHECK_EQ(_detail_state_tables.size(), _agg_states.size());
    }

    ~DetailAggGroupState() = default;

    const std::vector<AggStateData*>& agg_states() const { return _agg_states; }

    const std::vector<std::unique_ptr<StateTable>>& detail_state_tables() const { return _detail_state_tables; }

    Status process_chunk(size_t chunk_size, const std::vector<DatumRow>& non_found_keys,
                         const std::vector<uint8_t>& keys_not_in_map, const StreamRowOp* ops,
                         std::vector<std::vector<const Column*>>& raw_columns,
                         const Buffer<AggDataPtr>& agg_group_state) const;

    Status output_changes(size_t chunk_size, const Columns& group_by_columns, const Buffer<AggDataPtr>& agg_group_state,
                          std::vector<ChunkPtr>& detail_chunks) const;

private:
    std::vector<DatumRow> _construct_non_found_detail_keys(size_t chunk_size, const Buffer<uint8_t>& keys_not_in_map,
                                                           const Buffer<DatumRow>& non_found_keys,
                                                           std::vector<const Column*> agg_cols) const {
        DCHECK_LE(non_found_keys.size(), chunk_size);
        auto seek_keys = non_found_keys;
        int32_t agg_column_size = agg_cols.size();
        DCHECK_EQ(agg_column_size, 1);
        for (int i = 0; i < agg_column_size; i++) {
            auto agg_col = agg_cols[i];
            int k = 0;
            for (int j = 0; j < chunk_size; j++) {
                if (keys_not_in_map[j]) {
                    seek_keys[k++].push_back(agg_col->get(j));
                }
            }
            DCHECK_EQ(k, non_found_keys.size());
        }
        return seek_keys;
    }

private:
    std::vector<AggStateData*> _agg_states;
    StateTable* _result_state_table;
    StateTable* _intermediate_state_table;
    const std::vector<std::unique_ptr<StateTable>>& _detail_state_tables;
};

} // namespace starrocks::stream
