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

#include "exec/pipeline/adaptive/lazy_create_drivers_operator.h"

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_executor.h"

namespace starrocks::pipeline {

/// LazyCreateDriversOperator.
LazyCreateDriversOperator::LazyCreateDriversOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                                     const int32_t driver_sequence,
                                                     const std::vector<Pipelines>& unready_pipeline_groups)
        : SourceOperator(factory, id, "lazy_create_drivers", plan_node_id, driver_sequence) {
    for (const auto& pipe_group : unready_pipeline_groups) {
        PipelineItems pipe_item_group;
        pipe_item_group.reserve(pipe_group.size());
        for (const auto& pipe : pipe_group) {
            pipe_item_group.emplace_back(pipe, pipe->source_operator_factory()->degree_of_parallelism());
        }
        _unready_pipeline_groups.emplace_back(std::move(pipe_item_group));
    }
}

bool LazyCreateDriversOperator::has_output() const {
    return std::any_of(_unready_pipeline_groups.begin(), _unready_pipeline_groups.end(),
                       [](const auto& pipeline_group) {
                           auto* source_op_of_first_pipeline = pipeline_group[0].pipeline->source_operator_factory();
                           return source_op_of_first_pipeline->state() == SourceOperatorFactory::State::READY;
                       });
}
bool LazyCreateDriversOperator::need_input() const {
    return false;
}
bool LazyCreateDriversOperator::is_finished() const {
    return _unready_pipeline_groups.empty();
}

Status LazyCreateDriversOperator::set_finishing(RuntimeState* state) {
    return Status::OK();
}
Status LazyCreateDriversOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return Status::InternalError("Not support");
}

StatusOr<ChunkPtr> LazyCreateDriversOperator::pull_chunk(RuntimeState* state) {
    auto* fragment_ctx = state->fragment_ctx();

    std::vector<PipelinePtr> ready_pipelines;
    auto pipe_group_it = _unready_pipeline_groups.begin();
    while (pipe_group_it != _unready_pipeline_groups.end()) {
        auto& pipe_item_group = *pipe_group_it;
        auto* source_op_of_first_pipeline = pipe_item_group[0].pipeline->source_operator_factory();
        if (source_op_of_first_pipeline->state() != SourceOperatorFactory::State::READY) {
            pipe_group_it++;
            continue;
        }

        size_t max_dop = source_op_of_first_pipeline->degree_of_parallelism();
        bool adjust_dop = max_dop != pipe_item_group[0].original_dop;

        for (auto& pipe_item : pipe_item_group) {
            auto& pipe = pipe_item.pipeline;
            if (adjust_dop) {
                pipe->source_operator_factory()->set_max_dop(max_dop);
            }
            pipe->create_drivers(state);
            ready_pipelines.emplace_back(std::move(pipe));
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

void LazyCreateDriversOperator::close(RuntimeState* state) {
    auto* fragment_ctx = state->fragment_ctx();
    for (auto& pipeline_group : _unready_pipeline_groups) {
        fragment_ctx->count_down_pipeline(pipeline_group.size());
    }
    _unready_pipeline_groups.clear();

    Operator::close(state);
}

/// LazyCreateDriversOperatorFactory.
LazyCreateDriversOperatorFactory::LazyCreateDriversOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                   std::vector<Pipelines>&& unready_pipeline_groups)
        : SourceOperatorFactory(id, "lazy_create_drivers", plan_node_id),
          _unready_pipeline_groups(std::move(unready_pipeline_groups)) {}

} // namespace starrocks::pipeline
