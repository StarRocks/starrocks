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

#include "exec/pipeline/limit_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

StatusOr<ChunkPtr> LimitOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status LimitOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _cur_chunk = chunk;

    int64_t old_limit;
    int64_t num_consume_rows;
    do {
        old_limit = _limit.load(std::memory_order_relaxed);
        num_consume_rows = std::min(old_limit, static_cast<int64_t>(chunk->num_rows()));
    } while (num_consume_rows && !_limit.compare_exchange_strong(old_limit, old_limit - num_consume_rows));

    if (num_consume_rows != chunk->num_rows()) {
        // In case of multi cast exchange chunks could be used in multiple pipelines with different limits and should
        // not be updated in place.
        if (!_limit_chunk_in_place) {
            _cur_chunk = chunk->clone_unique();
        }
        _cur_chunk->set_num_rows(num_consume_rows);
    }

    return Status::OK();
}

OperatorExecStatsSnapshot LimitOperator::exec_stats_snapshot() const {
    if (_is_subordinate) {
        return OperatorExecStatsSnapshot::ignored();
    }

    OperatorExecStatsSnapshot snapshot;
    snapshot.plan_node_id = _plan_node_id;
    snapshot.update_pull_rows = true;
    snapshot.force_set_pull_rows = true;
    snapshot.pull_rows = COUNTER_VALUE(_pull_row_num_counter);
    if (_conjuncts_input_counter != nullptr && _conjuncts_output_counter != nullptr) {
        snapshot.update_pred_filter_rows = true;
        snapshot.pred_filter_rows = COUNTER_VALUE(_conjuncts_input_counter) - COUNTER_VALUE(_conjuncts_output_counter);
    }
    if (_bloom_filter_eval_context.join_runtime_filter_input_counter != nullptr) {
        snapshot.update_rf_filter_rows = true;
        int64_t input_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_input_counter);
        int64_t output_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_output_counter);
        snapshot.rf_filter_rows = input_rows - output_rows;
    }
    return snapshot;
}

} // namespace starrocks::pipeline
