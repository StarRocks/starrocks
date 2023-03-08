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

#include "column/stream_chunk.h"
#include "exec/aggregator.h"
#include "exec/stream/state/state_table.h"
#include "exprs/agg/aggregate.h"

namespace starrocks::stream {
using AggGroupStatePtr = uint8_t*;

class AggStateData;
using AggStateDataUPtr = std::unique_ptr<AggStateData>;

struct AggStateDataParams {
    int32_t func_idx;
    int32_t intermediate_table_column_idx; // maybe result/intermediate table decided by AggStateTableKind
    int32_t detail_table_column_idx;       // detail table idx only used by Detail state kind.
};

// `AggStateData` represents one agg function's state data which has a agg_state_kind(RESULT/INTERMEDIATE/DETAIL).
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
    ~AggStateData() = default;

    // Restore intermediate state if needed by using last run state table's results from
    // result/intermediate state table.
    Status allocate_intermediate_state(size_t chunk_size, const std::vector<uint8_t>& keys_not_in_map,
                                       const StateTableResult* state_result,
                                       const Buffer<AggGroupStatePtr>& agg_group_state) const;

    // Restore detail state if needed by using last run state's result from
    // detail state table
    Status allocate_detail_state(size_t chunk_size, const Column* raw_column, const StateTableResult* state_result,
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
    const bool is_result_agg_state() const {
        return _agg_state_kind == AggStateTableKind::RESULT || _agg_state_kind == AggStateTableKind::DETAIL_RESULT;
    }
    const bool is_intermediate_agg_state() const {
        return _agg_state_kind == AggStateTableKind::INTERMEDIATE ||
               _agg_state_kind == AggStateTableKind::DETAIL_INTERMEDIATE;
    }
    const bool is_detail_agg_state() const {
        return _agg_state_kind == AggStateTableKind::DETAIL_INTERMEDIATE ||
               _agg_state_kind == AggStateTableKind::DETAIL_RESULT;
    }

protected:
    const AggregateFunction* _agg_function;
    FunctionContext* _agg_fn_ctx;
    const AggFunctionTypes& _agg_fn_type;
    AggStateTableKind _agg_state_kind;
    size_t _agg_state_offset;
    AggStateDataParams _params;
};

} // namespace starrocks::stream
