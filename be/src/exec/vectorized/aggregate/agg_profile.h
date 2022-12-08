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
};
} // namespace starrocks