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

#include "exec/runtime/pipeline_builder_context.h"

#include <memory>

#include "exec/pipeline/primitives/event.h"
#include "exec/pipeline/scan/morsel_queue_factory_base.h"
#include "exec/pipeline/source_operator.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/group_execution/execution_group_builder.h"
#include "exec/runtime/pipeline.h"

namespace starrocks::pipeline {

PipelineBuilderContext::PipelineBuilderContext(FragmentContext* fragment_context, size_t degree_of_parallelism,
                                               size_t sink_dop)
        : _fragment_context(fragment_context),
          _degree_of_parallelism(degree_of_parallelism),
          _data_sink_dop(sink_dop),
          _enable_group_execution(fragment_context->enable_group_execution()) {
    // init the default execution group
    _execution_groups.emplace_back(ExecutionGroupBuilder::create_normal_exec_group());
    _normal_exec_group = _execution_groups.back().get();
    _current_execution_group = _execution_groups.back().get();
}

void PipelineBuilderContext::init_colocate_groups(std::unordered_map<int32_t, ExecutionGroupPtr>&& colocate_groups) {
    _group_id_to_colocate_groups = std::move(colocate_groups);
    for (auto& [group_id, group] : _group_id_to_colocate_groups) {
        _execution_groups.emplace_back(group);
        // Build reverse mapping for O(1) lookup in find_exec_group_by_plan_node_id.
        for (int32_t node_id : group->plan_node_ids()) {
            _plan_node_to_colocate_group[node_id] = group.get();
        }
    }
}

ExecutionGroupRawPtr PipelineBuilderContext::find_exec_group_by_plan_node_id(int32_t plan_node_id) {
    auto it = _plan_node_to_colocate_group.find(plan_node_id);
    if (it != _plan_node_to_colocate_group.end()) {
        return it->second;
    }
    return _normal_exec_group;
}

void PipelineBuilderContext::add_pipeline(const OpFactories& operators, ExecutionGroupRawPtr execution_group) {
    // TODO: refactor Pipelines to PipelineRawPtrs
    _pipelines.emplace_back(std::make_shared<Pipeline>(next_pipe_id(), operators, execution_group));
    execution_group->add_pipeline(_pipelines.back().get());
    _subscribe_pipeline_event(_pipelines.back().get());
}

void PipelineBuilderContext::add_pipeline(const OpFactories& operators) {
    add_pipeline(operators, _current_execution_group);
}

void PipelineBuilderContext::add_independent_pipeline(const OpFactories& operators) {
    add_pipeline(operators, _normal_exec_group);
}

bool PipelineBuilderContext::is_colocate_group() const {
    return _current_execution_group->type() == ExecutionGroupType::COLOCATE;
}

RuntimeState* PipelineBuilderContext::runtime_state() {
    return _fragment_context->runtime_state();
}

size_t PipelineBuilderContext::dop_of_source_operator(int source_node_id) {
    auto* morsel_queue_factory = morsel_queue_factory_of_source_operator(source_node_id);
    return morsel_queue_factory->size();
}

MorselQueueFactory* PipelineBuilderContext::morsel_queue_factory_of_source_operator(int source_node_id) {
    auto& morsel_queues = _fragment_context->morsel_queue_factories();
    DCHECK(morsel_queues.count(source_node_id));
    return morsel_queues[source_node_id].get();
}

MorselQueueFactory* PipelineBuilderContext::morsel_queue_factory_of_source_operator(
        const SourceOperatorFactory* source_op) {
    if (!source_op->with_morsels()) {
        return nullptr;
    }

    return morsel_queue_factory_of_source_operator(source_op->plan_node_id());
}

SourceOperatorFactory* PipelineBuilderContext::source_operator(const OpFactories& ops) {
    return down_cast<SourceOperatorFactory*>(ops[0].get());
}

bool PipelineBuilderContext::could_local_shuffle(const OpFactories& ops) const {
    return down_cast<SourceOperatorFactory*>(ops[0].get())->could_local_shuffle();
}

void PipelineBuilderContext::inherit_upstream_source_properties(SourceOperatorFactory* downstream_source,
                                                                SourceOperatorFactory* upstream_source) {
    downstream_source->set_degree_of_parallelism(upstream_source->degree_of_parallelism());
    downstream_source->set_could_local_shuffle(upstream_source->could_local_shuffle());
    downstream_source->set_partition_type(upstream_source->partition_type());
    if (!upstream_source->partition_exprs().empty() || !downstream_source->partition_exprs().empty()) {
        downstream_source->set_partition_exprs(upstream_source->partition_exprs());
    }

    if (downstream_source->adaptive_initial_state() == SourceOperatorFactory::AdaptiveState::NONE) {
        downstream_source->add_upstream_source(upstream_source);
    }
}

void PipelineBuilderContext::push_dependent_pipeline(const Pipeline* pipeline) {
    _dependent_pipelines.emplace_back(pipeline);
}

void PipelineBuilderContext::pop_dependent_pipeline() {
    _dependent_pipelines.pop_back();
}

void PipelineBuilderContext::_subscribe_pipeline_event(Pipeline* pipeline) {
    bool enable_wait_event = _fragment_context->runtime_state()->enable_wait_dependent_event();
    enable_wait_event &= !_current_execution_group->is_colocate_exec_group();
    if (enable_wait_event && !_dependent_pipelines.empty()) {
        pipeline->pipeline_event()->set_need_wait_dependencies_finished(true);
        pipeline->pipeline_event()->add_dependency(_dependent_pipelines.back()->pipeline_event());
    }
}

std::pair<ExecutionGroups, Pipelines> PipelineBuilder::build() {
    return {_context.execution_groups(), _context.pipelines()};
}

} // namespace starrocks::pipeline
