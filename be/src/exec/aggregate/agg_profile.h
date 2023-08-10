// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "util/runtime_profile.h"

namespace starrocks {
struct AggStatistics {
    AggStatistics(RuntimeProfile* runtime_profile) {
        get_results_timer = ADD_TIMER(runtime_profile, "GetResultsTime");
        iter_timer = ADD_TIMER(runtime_profile, "ResultIteratorTime");
        agg_append_timer = ADD_TIMER(runtime_profile, "ResultAggAppendTime");
        group_by_append_timer = ADD_TIMER(runtime_profile, "ResultGroupByAppendTime");
        agg_compute_timer = ADD_TIMER(runtime_profile, "AggComputeTime");
        agg_function_compute_timer = ADD_TIMER(runtime_profile, "AggFuncComputeTime");
        streaming_timer = ADD_TIMER(runtime_profile, "StreamingTime");
        expr_compute_timer = ADD_TIMER(runtime_profile, "ExprComputeTime");
        expr_release_timer = ADD_TIMER(runtime_profile, "ExprReleaseTime");
        input_row_count = ADD_COUNTER(runtime_profile, "InputRowCount", TUnit::UNIT);
        hash_table_size = ADD_COUNTER(runtime_profile, "HashTableSize", TUnit::UNIT);
        pass_through_row_count = ADD_COUNTER(runtime_profile, "PassThroughRowCount", TUnit::UNIT);
        rows_returned_counter = ADD_COUNTER(runtime_profile, "RowsReturned", TUnit::UNIT);
        state_destroy_timer = ADD_TIMER(runtime_profile, "StateDestroy");
        allocate_state_timer = ADD_TIMER(runtime_profile, "StateAllocate");

        chunk_buffer_peak_memory = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakMem", TUnit::BYTES);
        chunk_buffer_peak_size = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakSize", TUnit::UNIT);
    }

    // timer for build hash table and compute aggregate function
    RuntimeProfile::Counter* agg_compute_timer{};
    // timer for compute aggregate function
    RuntimeProfile::Counter* agg_function_compute_timer{};
    // timer for streaming aggregate (convert to serialize)
    RuntimeProfile::Counter* streaming_timer{};
    // input rows
    RuntimeProfile::Counter* input_row_count{};
    // rows returned
    RuntimeProfile::Counter* rows_returned_counter;
    // hash table elements size
    RuntimeProfile::Counter* hash_table_size{};
    // timer for iterator hash table
    RuntimeProfile::Counter* iter_timer{};
    // timer for get result from hash table
    RuntimeProfile::Counter* get_results_timer{};
    // get result from hash table (get result from agg states)
    RuntimeProfile::Counter* agg_append_timer{};
    // get result from hash table (get result from group bys)
    RuntimeProfile::Counter* group_by_append_timer{};
    // hash streaming aggregate pass through rows
    RuntimeProfile::Counter* pass_through_row_count{};
    // timer for get input from hash table
    RuntimeProfile::Counter* expr_compute_timer{};
    // timer for result input from hash table
    RuntimeProfile::Counter* expr_release_timer{};
    RuntimeProfile::Counter* state_destroy_timer{};
    RuntimeProfile::Counter* allocate_state_timer{};

    RuntimeProfile::HighWaterMarkCounter* chunk_buffer_peak_memory{};
    RuntimeProfile::HighWaterMarkCounter* chunk_buffer_peak_size{};
};
} // namespace starrocks