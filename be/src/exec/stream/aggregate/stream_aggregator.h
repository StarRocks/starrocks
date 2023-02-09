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

#include <algorithm>
#include <utility>

#include "exec/aggregator.h"
#include "exec/stream/aggregate/agg_state_data.h"
#include "exec/stream/state/mem_state_table.h"
#include "exec/stream/state/state_table.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks::stream {

/**
 * StreamAggregator is used for stream materialized view, it differs with Aggregator:
 * stream aggregator's state is not kept into mem_pool, rather in the StateCache which can be changed adaptively;
 * stream aggregator's processing is by row rather than by chunk which is better in stream materialized view;
 * stream aggregator handles input's chunk as below:
 *  - step1: compute the agg state:
 *      - iterate each row of input chunk,
 *      - 1.1 if its state is in StateCache, get the state's address
 *      - 1.2 query the aggregate StateTable:
 *          - 1.2.1 if it is in StateTable, deserialized the result and store in the StateCache, then redo 1.1
 *          - 1.2.2 insert a new state into StateCache, and get the state's address
 *  - step2: update the intermediate agg state:
 *      - iterate each row of input chunk, call aggregate functions' update functions to accumulate the state.
 *  - step3: update StateTable
 *      - iterate incremental input rows and write back into the StateTable
 *  - step4: output the incremental results
 *      - iterate incremental input rows and output to the next operator
 */
class StreamAggregator final : public Aggregator {
public:
    StreamAggregator(AggregatorParamsPtr&& params);

    static constexpr StreamRowOp INSERT_OP = StreamRowOp::OP_INSERT;
    static constexpr StreamRowOp DELETE_OP = StreamRowOp::OP_DELETE;
    static constexpr StreamRowOp UPDATE_BEFORE_OP = StreamRowOp::OP_UPDATE_BEFORE;
    static constexpr StreamRowOp UPDATE_AFTER_OP = StreamRowOp::OP_UPDATE_AFTER;

    ~StreamAggregator() override {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status open(RuntimeState* state) { return Aggregator::open(state); }

    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile) override {
        RETURN_IF_ERROR(Aggregator::prepare(state, pool, runtime_profile));
        RETURN_IF_ERROR(_prepare_state_tables(state));
        return Status::OK();
    }

    // Process input's chunks util `Epoch` chunk is received.
    Status process_chunk(StreamChunk* chunk);

    // Called when need to generate incremental outputs and Output agg_states for the next batch.
    Status output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk);

    // Used to check result/intermediate/detail result for testing.
    // Called when need to generate incremental outputs and Output agg_states for the next batch.
    Status output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                          std::vector<ChunkPtr>& detail_chunks);

    // Reset hashmap(like Cache's evict) when the transaction is over.
    Status reset_state(RuntimeState* state);

    void close(RuntimeState* state) override;

private:
    Status _prepare_state_tables(RuntimeState* state);

    DatumRow _convert_to_datum_row(const Columns& columns, size_t row_idx);

    template <typename HashMapWithKey>
    Status _output_changes(HashMapWithKey& hash_map_with_key, int32_t chunk_size, StreamChunkPtr* result_chunk,
                           ChunkPtr* intermediate_chunk, std::vector<ChunkPtr>* detail_chunks);

    // Output intermediate(same to OLAP's agg_state) chunks.
    Status _output_intermediate_changes(int32_t chunk_size, const Columns& group_by_columns,
                                        ChunkPtr* intermediate_chunk);

    // Output incremental result changes based on previous outputs and new incremental outputs.
    Status _output_result_changes(int32_t chunk_size, const Columns& group_by_columns, StreamChunkPtr* result_chunk);

    // Use history(prev) output and final count to generate UPDATE_BEFORE/DELETE Ops:
    // If prev output exists and final count > 0: generate UPDATE_BEFORE
    // If prev output exists and final count = 0: generate DELETE
    Status _output_result_changes_with_retract(size_t chunk_size, const Columns& group_by_columns,
                                               StreamChunkPtr* result_chunk_with_ops);

    // output results by calling retract functions
    Status _output_final_result_with_retract(size_t chunk_size, const Columns& group_by_columns,
                                             ChunkPtr* post_chunk_result);

    // Output results without generating retract messages.
    Status _output_result_changes_without_retract(size_t chunk_size, const Columns& group_by_columns,
                                                  StreamChunkPtr* result_chunk);

private:
    // Store buffers which can be reused in the incremental compute.
    std::unique_ptr<MemPool> _mem_pool;

    // Store group by keys to agg state map.
    std::unique_ptr<StateTable> _result_state_table;
    std::unique_ptr<StateTable> _intermediate_state_table;
    // TODO: support merge into one detail table later.
    std::vector<std::unique_ptr<StateTable>> _detail_state_tables;
    int32_t _count_agg_idx{0};

    // store all agg states
    std::vector<std::unique_ptr<AggStateData>> _agg_func_states;
    // output intermediate columns.
    std::vector<int32_t> _intermediate_agg_func_ids;

    std::unique_ptr<IntermediateAggGroupState> _result_agg_group;
    std::unique_ptr<IntermediateAggGroupState> _intermediate_agg_group;
    std::unique_ptr<DetailAggGroupState> _detail_agg_group;

    std::vector<DatumRow> _non_found_keys;
    // Changed group by keys to generate outputs
    SliceHashSet _changed_keys;
};

} // namespace starrocks::stream
