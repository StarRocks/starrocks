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

#include "exec/pipeline/adaptive/lazy_instantiate_drivers_operator.h"

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/result_sink_operator.h"

namespace starrocks::pipeline {

/// LazyInstantiateDriversOperator.
LazyInstantiateDriversOperator::LazyInstantiateDriversOperator(OperatorFactory* factory, int32_t id,
                                                               int32_t plan_node_id, const int32_t driver_sequence,
                                                               const PipelineGroupMap& unready_pipeline_group_mapping)
        : SourceOperator(factory, id, "lazy_instantiate_drivers", plan_node_id, driver_sequence) {
    std::vector<PipelineGroup> unready_pipeline_groups;
    unready_pipeline_groups.reserve(unready_pipeline_groups.size());
    for (const auto& [leader_source_op, pipes] : unready_pipeline_group_mapping) {
        unready_pipeline_groups.emplace_back(leader_source_op, leader_source_op->degree_of_parallelism(), pipes);
    }
    std::sort(unready_pipeline_groups.begin(), unready_pipeline_groups.end(),
              [](const PipelineGroup& lhs, const PipelineGroup& rhs) {
                  auto lhs_id = lhs.pipelines[0]->get_id();
                  auto rhs_id = rhs.pipelines[0]->get_id();
                  return lhs_id < rhs_id;
              });

    _unready_pipeline_groups.insert(_unready_pipeline_groups.end(), std::move_iterator{unready_pipeline_groups.begin()},
                                    std::move_iterator{unready_pipeline_groups.end()});
}

bool LazyInstantiateDriversOperator::has_output() const {
    return std::any_of(
            _unready_pipeline_groups.begin(), _unready_pipeline_groups.end(),
            [](const auto& pipeline_group) { return pipeline_group.leader_source_op->is_adaptive_group_active(); });
}
bool LazyInstantiateDriversOperator::need_input() const {
    return false;
}
bool LazyInstantiateDriversOperator::is_finished() const {
    return _unready_pipeline_groups.empty();
}

Status LazyInstantiateDriversOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}
Status LazyInstantiateDriversOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return Status::InternalError("Not support");
}

StatusOr<ChunkPtr> LazyInstantiateDriversOperator::pull_chunk(RuntimeState* state) {
    auto* fragment_ctx = state->fragment_ctx();

    std::vector<PipelinePtr> ready_pipelines;
    auto pipe_group_it = _unready_pipeline_groups.begin();
    while (pipe_group_it != _unready_pipeline_groups.end()) {
        auto& [leader_source_op, original_leader_dop, pipelines] = *pipe_group_it;
        if (!leader_source_op->is_adaptive_group_active()) {
            pipe_group_it++;
            continue;
        }

        leader_source_op->adjust_dop();
        size_t leader_dop = leader_source_op->degree_of_parallelism();
        bool adjust_dop = leader_dop != original_leader_dop;

        for (auto& pipeline : pipelines) {
            if (adjust_dop) {
                pipeline->source_operator_factory()->adjust_max_dop(leader_dop);
            }
            pipeline->instantiate_drivers(state);
            ready_pipelines.emplace_back(std::move(pipeline));
        }

        _unready_pipeline_groups.erase(pipe_group_it++);
    }

    for (const auto& pipe : ready_pipelines) {
        for (const auto& driver : pipe->drivers()) {
            RETURN_IF_ERROR(driver->prepare(state));
        }
    }

    auto* executor = fragment_ctx->enable_resource_group() ? state->exec_env()->wg_driver_executor()
                                                           : state->exec_env()->driver_executor();
    for (const auto& pipe : ready_pipelines) {
        for (const auto& driver : pipe->drivers()) {
            DCHECK(!fragment_ctx->enable_resource_group() || driver->workgroup() != nullptr);
            executor->submit(driver.get());
        }
    }

    return nullptr;
}

void LazyInstantiateDriversOperator::close(RuntimeState* state) {
    auto* fragment_ctx = state->fragment_ctx();
    for (auto& pipeline_group : _unready_pipeline_groups) {
        for (auto& pipeline : pipeline_group.pipelines) {
            if (typeid(*pipeline->sink_operator_factory()) != typeid(ResultSinkOperatorFactory)) {
                fragment_ctx->count_down_pipeline(state);
            } else {
                // Closing ResultSinkOperator notifies FE not to wait fetch_data anymore.
                pipeline->source_operator_factory()->set_degree_of_parallelism(1);
                pipeline->instantiate_drivers(state);
                for (auto& driver : pipeline->drivers()) {
                    driver->prepare(state);
                }
                for (auto& driver : pipeline->drivers()) {
                    driver->finalize(state, DriverState::FINISH, 0, 0);
                }
            }
        }
    }
    _unready_pipeline_groups.clear();

    Operator::close(state);
}

/// LazyInstantiateDriversOperatorFactory.
LazyInstantiateDriversOperatorFactory::LazyInstantiateDriversOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                             PipelineGroupMap&& unready_pipeline_groups)
        : SourceOperatorFactory(id, "lazy_instantiate_drivers", plan_node_id),
          _unready_pipeline_groups(std::move(unready_pipeline_groups)) {}

} // namespace starrocks::pipeline
