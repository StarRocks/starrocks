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

#include "exec/aggregator.h"
#include "exec/stream/aggregate/agg_group_state.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

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
    StreamAggregator(AggregatorParamsPtr params);

    static constexpr StreamRowOp INSERT_OP = StreamRowOp::OP_INSERT;
    static constexpr StreamRowOp DELETE_OP = StreamRowOp::OP_DELETE;
    static constexpr StreamRowOp UPDATE_BEFORE_OP = StreamRowOp::OP_UPDATE_BEFORE;
    static constexpr StreamRowOp UPDATE_AFTER_OP = StreamRowOp::OP_UPDATE_AFTER;

    ~StreamAggregator() override {
        if (_state != nullptr) {
            close(_state);
        }
    }

    Status prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile);

    Status open(RuntimeState* state) override;

    // Process input's chunks util `Epoch` chunk is received.
    Status process_chunk(StreamChunk* chunk);

    // Called when need to generate incremental outputs and Output agg_states for the next batch.
    Status output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk);

    // Called when need to generate incremental outputs and Output agg_states for the next batch.
    Status output_changes(int32_t chunk_size, StreamChunkPtr* result_chunk, ChunkPtr* intermediate_chunk,
                          std::vector<ChunkPtr>& detail_chunks);

    // Reset the aggregator's state to avoid hashmap too large.
    Status reset_state(RuntimeState* state);

    // When the epoch is finished, commit the state table.
    Status commit_epoch(RuntimeState* state);

    // When the epoch starts, reset stream aggreator's state in the new epoch.
    Status reset_epoch(RuntimeState* state);

private:
    Status _prepare_state_tables(RuntimeState* state);

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
    int32_t _count_agg_idx{0};
    // Store AggState group.
    std::unique_ptr<AggGroupState> _agg_group_state;
};

} // namespace starrocks::stream
