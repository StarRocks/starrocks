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

#include "exec/stream/aggregate/agg_state_data.h"
#include "exec/stream/state/mem_state_table.h"

namespace starrocks::stream {

// `AggGroupState` represents all agg functions's state in StreamAggregator. It bridges between the operator
// and the state table behind the single agg function.
class AggGroupState {
public:
    AggGroupState(std::vector<AggStateDataUPtr>&& agg_states, const AggregatorParamsPtr& params,
                  const TupleDescriptor* _output_tuple_desc, const TupleDescriptor* _intermediate_tuple_desc);
    ~AggGroupState() = default;

    const std::vector<AggStateDataUPtr>& agg_states() const { return _agg_states; }

    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);

    Status process_chunk(size_t chunk_size, const Columns& group_by_columns, const Buffer<uint8_t>& keys_not_in_map,
                         const StreamRowOp* ops, const std::vector<Columns>& agg_columns,
                         std::vector<std::vector<const Column*>>& raw_columns,
                         const Buffer<AggDataPtr>& agg_group_state) const;

    Status output_results(size_t chunk_size, const Columns& group_by_columns, const Buffer<AggDataPtr>& agg_group_data,
                          Columns output_columns) const;

    Status output_changes(size_t chunk_size, const Columns& group_by_columns, const Buffer<AggDataPtr>& agg_group_state,
                          ChunkPtr* agg_intermediate_chunk, std::vector<ChunkPtr>* detail_chunks) const;

    Status output_prev_state_results(const Columns& group_by_columns, StateTableResult& prev_state_result);

    Status write(RuntimeState* state, StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                 std::vector<ChunkPtr>& detail_chunk);
    Status commit_epoch(RuntimeState* state);
    Status reset_epoch(RuntimeState* state);

private:
    Status _prepare_mem_state_tables(RuntimeState* state, const std::vector<AggStateData*>& intermediate_agg_states,
                                     const std::vector<AggStateData*>& detail_agg_states);
    Status _prepare_imt_state_tables(RuntimeState* state, const std::vector<AggStateData*>& intermediate_agg_states,
                                     const std::vector<AggStateData*>& detail_agg_states);
    StateTable* _find_detail_state_table(const AggStateDataUPtr& agg_state) const;
    ChunkPtr _build_intermediate_chunk(const Columns& group_by_columns, const Columns& agg_intermediate_columns) const;

private:
    const std::vector<AggStateDataUPtr> _agg_states;
    const AggregatorParamsPtr& _params;
    const TupleDescriptor* _output_tuple_desc;
    const TupleDescriptor* _intermediate_tuple_desc;

    std::unique_ptr<StateTable> _result_state_table;
    std::unique_ptr<StateTable> _intermediate_state_table;
    // TODO: support merge into one detail table later.
    std::vector<std::unique_ptr<StateTable>> _detail_state_tables;
};

} // namespace starrocks::stream
