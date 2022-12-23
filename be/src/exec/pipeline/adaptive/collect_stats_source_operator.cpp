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

#include "exec/pipeline/adaptive/collect_stats_source_operator.h"

#include "exec/pipeline/adaptive/collect_stats_context.h"

namespace starrocks::pipeline {

/// CollectStatsSourceOperator.
CollectStatsSourceOperator::CollectStatsSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                       const int32_t driver_sequence, CollectStatsContextRawPtr ctx)
        : SourceOperator(factory, id, "collect_stats_source", plan_node_id, driver_sequence), _ctx(ctx) {}

bool CollectStatsSourceOperator::need_input() const {
    return false;
}
bool CollectStatsSourceOperator::has_output() const {
    return _ctx->has_output(_driver_sequence);
}
bool CollectStatsSourceOperator::is_finished() const {
    return _ctx->is_finished(_driver_sequence);
}

Status CollectStatsSourceOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return Status::InternalError("Not support");
}
StatusOr<ChunkPtr> CollectStatsSourceOperator::pull_chunk(RuntimeState* state) {
    return _ctx->pull_chunk(_driver_sequence);
}
Status CollectStatsSourceOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}

/// CollectStatsSourceOperatorFactory.
CollectStatsSourceOperatorFactory::CollectStatsSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                     CollectStatsContextPtr ctx)
        : SourceOperatorFactory(id, "collect_stats_source", plan_node_id), _ctx(std::move(ctx)) {}

Status CollectStatsSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    _ctx->ref();

    return Status::OK();
}

void CollectStatsSourceOperatorFactory::close(RuntimeState* state) {
    _ctx->unref(state);
    OperatorFactory::close(state);
}

SourceOperatorFactory::State CollectStatsSourceOperatorFactory::state() const {
    if (_ctx->is_source_ready()) {
        return SourceOperatorFactory::State::READY;
    }
    return SourceOperatorFactory::State::NOT_READY;
}

size_t CollectStatsSourceOperatorFactory::degree_of_parallelism() const {
    return _ctx->source_dop();
}

} // namespace starrocks::pipeline
