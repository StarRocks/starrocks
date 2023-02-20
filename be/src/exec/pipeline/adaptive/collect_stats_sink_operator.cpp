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

#include "exec/pipeline/adaptive/collect_stats_sink_operator.h"

#include "exec/pipeline/adaptive/collect_stats_context.h"

namespace starrocks::pipeline {

/// CollectStatsSinkOperator.
CollectStatsSinkOperator::CollectStatsSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                   const int32_t driver_sequence, CollectStatsContextRawPtr ctx)
        : Operator(factory, id, "collect_stats_sink", plan_node_id, driver_sequence), _ctx(ctx) {}

Status CollectStatsSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _ctx->ref();

    return Status::OK();
}

void CollectStatsSinkOperator::close(RuntimeState* state) {
    _ctx->unref(state);
    Operator::close(state);
}

bool CollectStatsSinkOperator::need_input() const {
    return _ctx->need_input(_driver_sequence);
}
bool CollectStatsSinkOperator::has_output() const {
    return false;
}
bool CollectStatsSinkOperator::is_finished() const {
    return _is_finishing || _ctx->is_upstream_finished(_driver_sequence);
}

Status CollectStatsSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _ctx->push_chunk(_driver_sequence, chunk);
}
StatusOr<ChunkPtr> CollectStatsSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}
Status CollectStatsSinkOperator::set_finishing(RuntimeState* state) {
    _is_finishing = true;
    return _ctx->set_finishing(_driver_sequence);
}

/// CollectStatsSinkOperatorFactory.
CollectStatsSinkOperatorFactory::CollectStatsSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                 CollectStatsContextPtr ctx)
        : OperatorFactory(id, "collect_stats_sink", plan_node_id), _ctx(std::move(ctx)) {}

} // namespace starrocks::pipeline
