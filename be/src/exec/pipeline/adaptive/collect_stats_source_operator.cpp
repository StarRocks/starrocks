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
#include "exec/pipeline/adaptive/utils.h"
#include "exec/pipeline/pipeline.h"

namespace starrocks::pipeline {

/// CollectStatsSourceOperator.
CollectStatsSourceOperator::CollectStatsSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                       const int32_t driver_sequence, CollectStatsContextRawPtr ctx)
        : SourceOperator(factory, id, "collect_stats_source", plan_node_id, driver_sequence), _ctx(ctx) {}

void CollectStatsSourceOperator::close(RuntimeState* state) {
    Operator::close(state);

    _unique_metrics->add_info_string("State", _ctx->readable_state());
}

bool CollectStatsSourceOperator::need_input() const {
    return false;
}
bool CollectStatsSourceOperator::has_output() const {
    return _ctx->has_output(_driver_sequence);
}
bool CollectStatsSourceOperator::is_finished() const {
    return _is_finished || _ctx->is_downstream_finished(_driver_sequence);
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
Status CollectStatsSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return _ctx->set_finished(_driver_sequence);
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

OperatorPtr CollectStatsSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    // DOP will be recomputed when its group is ready, see CollectStatsSourceOperatorFactory::degree_of_parallelism.
    _ctx->set_downstream_dop(degree_of_parallelism);
    return std::make_shared<CollectStatsSourceOperator>(this, _id, _plan_node_id, driver_sequence, _ctx.get());
}

SourceOperatorFactory::AdaptiveState CollectStatsSourceOperatorFactory::adaptive_state() const {
    if (_ctx->is_downstream_ready()) {
        return SourceOperatorFactory::AdaptiveState::ACTIVE;
    }
    return SourceOperatorFactory::AdaptiveState::INACTIVE;
}

/// Recompute DOP once, only when its group is active.
/// - DOP should be multiplied by output_amplification_factor of dependent pipelines.
/// - Constraints:
///   - DOP *= output_amplification_factor.
///   - DOP <= CollectStatsSinkOperatorFactory::degree_of_parallelism.
///   - DOP >= DOP of dependent pipelines.
size_t CollectStatsSourceOperatorFactory::degree_of_parallelism() const {
    // Recompute DOP once, only when its group is active.
    if (!is_adaptive_group_active()) {
        return _ctx->downstream_dop();
    }

    if (_adjusted_dop != ABSENT_ADJUSTED_DOP) {
        return _adjusted_dop;
    }

    const size_t downstream_dop = _ctx->downstream_dop();
    const size_t upstream_dop = _ctx->upstream_dop();
    const size_t max_output_amplification_factor = _ctx->max_output_amplification_factor();
    const auto& dependent_pipelines = group_dependent_pipelines();

    // 1. Use the source dop from context, which is adjusted by CsSink.
    _adjusted_dop = downstream_dop;
    if (_adjusted_dop == upstream_dop) {
        return _adjusted_dop;
    }

    // 2. DOP should be >= max_dependent_dop.
    size_t max_dependent_dop = 1;
    for (const auto& pipeline : dependent_pipelines) {
        max_dependent_dop = std::max(max_dependent_dop, pipeline->degree_of_parallelism());
    }
    if (max_dependent_dop >= upstream_dop) {
        _adjusted_dop = upstream_dop;
        return _adjusted_dop;
    }

    // 3. DOP should be multiplied output_amplification_factor of dependent pipelines.
    size_t max_amp_factor = std::max<size_t>(1, upstream_dop / downstream_dop);
    if (max_output_amplification_factor != 0 && max_output_amplification_factor < max_amp_factor) {
        max_amp_factor = max_output_amplification_factor;
    }
    size_t amp_factor = 1;
    if (max_amp_factor != 1) {
        for (const auto& dependent_pipeline : dependent_pipelines) {
            amp_factor *= dependent_pipeline->output_amplification_factor();
            if (amp_factor >= max_amp_factor) {
                break;
            }
        }
        amp_factor = std::min(max_amp_factor, max_amp_factor);
        amp_factor = std::max<size_t>(1, amp_factor);
    }
    if (amp_factor != 1) {
        _adjusted_dop *= amp_factor;
        _adjusted_dop = compute_max_le_power2(_adjusted_dop);
    }

    _adjusted_dop = std::max<size_t>(1, _adjusted_dop);
    _adjusted_dop = std::max<size_t>(_adjusted_dop, max_dependent_dop);
    _adjusted_dop = std::min<size_t>(_adjusted_dop, upstream_dop);

    return _adjusted_dop;
}

} // namespace starrocks::pipeline
