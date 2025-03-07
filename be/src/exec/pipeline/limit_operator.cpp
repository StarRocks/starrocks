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
#include "exec/pipeline/query_context.h"
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
        _cur_chunk->set_num_rows(num_consume_rows);
    }

    return Status::OK();
}

void LimitOperator::update_exec_stats(RuntimeState* state) {
    auto ctx = state->query_ctx();
    if (!_is_subordinate && ctx != nullptr) {
        ctx->force_set_pull_rows_stats(_plan_node_id, _pull_row_num_counter->value());
        if (_conjuncts_input_counter != nullptr && _conjuncts_output_counter != nullptr) {
            ctx->update_pred_filter_stats(_plan_node_id,
                                          _conjuncts_input_counter->value() - _conjuncts_output_counter->value());
        }

        if (_bloom_filter_eval_context.join_runtime_filter_input_counter != nullptr) {
            int64_t input_rows = _bloom_filter_eval_context.join_runtime_filter_input_counter->value();
            int64_t output_rows = _bloom_filter_eval_context.join_runtime_filter_output_counter->value();
            ctx->update_rf_filter_stats(_plan_node_id, input_rows - output_rows);
        }
    }
}

} // namespace starrocks::pipeline
