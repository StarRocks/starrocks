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

#include <list>
#include <unordered_map>

#include "common/logging.h"
#include "exec/runtime/group_execution/execution_group_fwd.h"
#include "exec_primitive/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks {
namespace pipeline {

class PipelineBuilderContext {
public:
    PipelineBuilderContext(FragmentContext* fragment_context, size_t degree_of_parallelism, size_t sink_dop);

    void init_colocate_groups(std::unordered_map<int32_t, ExecutionGroupPtr>&& colocate_groups);
    ExecutionGroupRawPtr find_exec_group_by_plan_node_id(int32_t plan_node_id);
    void set_current_execution_group(ExecutionGroupRawPtr exec_group) { _current_execution_group = exec_group; }
    ExecutionGroupRawPtr current_execution_group() { return _current_execution_group; }
    ExecutionGroupRawPtr normal_execution_group() { return _normal_exec_group; }

    void add_pipeline(const OpFactories& operators, ExecutionGroupRawPtr execution_group);

    void add_pipeline(const OpFactories& operators);

    void add_independent_pipeline(const OpFactories& operators);

    bool is_colocate_group() const;

    uint32_t next_pipe_id() { return _next_pipeline_id++; }

    uint32_t next_operator_id() { return _next_operator_id++; }

    size_t degree_of_parallelism() const { return _degree_of_parallelism; }
    size_t data_sink_dop() const { return _data_sink_dop; }

    const Pipeline* last_pipeline() const {
        DCHECK(!_pipelines.empty());
        return _pipelines[_pipelines.size() - 1].get();
    }

    /// Everything the builder has produced so far. Taken before a node builds a subtree outside its
    /// push_dependent_pipeline() scope, and handed back to bind_dependent_pipeline_between()
    /// afterwards.
    struct BuilderMark {
        size_t num_pipelines;
        size_t num_group_dependent_sources;
    };
    BuilderMark mark() const { return {_pipelines.size(), _group_dependent_sources.size()}; }

    /// Bind `dependency` to everything built between the two marks, as if
    /// push_dependent_pipeline(dependency) had been active while it was built. A node needs this when
    /// the shape of the pipeline others depend on is itself decided by those others, so it can only be
    /// added afterwards -- how to partition a set operation's build child, or a hash join's build
    /// side, depends on all the other children.
    ///
    /// TWO things read that scope, and both are handled here, in one call, because compensating for
    /// one and forgetting the other is silent:
    ///   1. _subscribe_pipeline_event() gives each pipeline added inside the scope an event
    ///      dependency on it.
    ///   2. maybe_interpolate_collect_stats() copies the whole scope into the CollectStatsSource it
    ///      creates, as that source's group-dependent pipelines. Those decide when the group's
    ///      drivers may be instantiated (FragmentExecutor makes the group-initialize event wait for
    ///      them) and clamp the group's DOP to at least theirs (adjust_dop()). For a hash join that is
    ///      exactly what upholds HashJoinerFactory's two rules -- builders created before probers, and
    ///      prober_dop a multiple of builder_dop -- so losing it crashes in get_builder() with
    ///      _builder_dop still 0, or silently leaves builders no prober ever visits.
    /// If a third reader of dependent_pipelines() appears, it has to be handled here too.
    ///
    /// The pipeline range must hold ONLY pipelines that consume `dependency`'s output. Anything that
    /// FEEDS it has to stay out: interpolating the local exchange in front of `dependency` registers
    /// the pipeline that ends in the matching LocalExchangeSink, and making that one wait for
    /// `dependency` to finish deadlocks the fragment, since `dependency` cannot finish before it has
    /// read what that pipeline sends. That is why the marks are taken around the other children only.
    ///
    /// `apply_events` is the caller's stand-in for the colocate-group half of the gate in
    /// _subscribe_pipeline_event: that gate reads the current execution group as each dependent
    /// pipeline is added, which cannot be recovered after the fact. It only decides event scheduling
    /// -- the operators still gate themselves on the build being ready -- so approximating it with
    /// the group the dependency itself lands in is safe. It does not cover (2), which is not optional.
    void bind_dependent_pipeline_between(const BuilderMark& begin, const BuilderMark& end, const Pipeline* dependency,
                                         bool apply_events);

    /// maybe_interpolate_collect_stats() records every CollectStatsSource it creates, so that
    /// bind_dependent_pipeline_between() can reach the ones built outside the scope.
    void record_group_dependent_source(SourceOperatorFactory* source_op) {
        _group_dependent_sources.emplace_back(source_op);
    }

    RuntimeState* runtime_state();
    FragmentContext* fragment_context() { return _fragment_context; }
    bool enable_group_execution() const { return _enable_group_execution; }

    size_t dop_of_source_operator(int source_node_id);
    MorselQueueFactory* morsel_queue_factory_of_source_operator(int source_node_id);
    MorselQueueFactory* morsel_queue_factory_of_source_operator(const SourceOperatorFactory* source_op);
    SourceOperatorFactory* source_operator(const OpFactories& ops);
    // Whether the building pipeline `ops` need local shuffle for the next operator.
    bool could_local_shuffle(const OpFactories& ops) const;

    // help to change some actions after aggregations, for example,
    // disable to ignore local data after aggregations with profile exchange speed.
    bool has_aggregation = false;

    void inherit_upstream_source_properties(SourceOperatorFactory* downstream_source,
                                            SourceOperatorFactory* upstream_source);

    void push_dependent_pipeline(const Pipeline* pipeline);
    void pop_dependent_pipeline();
    const std::list<const Pipeline*>& dependent_pipelines() const { return _dependent_pipelines; }

    ExecutionGroups execution_groups() { return std::move(_execution_groups); }
    Pipelines pipelines() { return std::move(_pipelines); }

private:
    void _subscribe_pipeline_event(Pipeline* pipeline);

    FragmentContext* _fragment_context;
    Pipelines _pipelines;
    ExecutionGroups _execution_groups;
    std::unordered_map<int32_t, ExecutionGroupPtr> _group_id_to_colocate_groups;
    // Reverse map from plan_node_id to colocate group for O(1) lookup.
    std::unordered_map<int32_t, ExecutionGroupRawPtr> _plan_node_to_colocate_group;
    ExecutionGroupRawPtr _normal_exec_group = nullptr;
    ExecutionGroupRawPtr _current_execution_group = nullptr;

    std::list<const Pipeline*> _dependent_pipelines;
    std::vector<SourceOperatorFactory*> _group_dependent_sources;

    uint32_t _next_pipeline_id = 0;
    uint32_t _next_operator_id = 0;

    const size_t _degree_of_parallelism;
    const size_t _data_sink_dop;

    const bool _enable_group_execution;
};

class PipelineBuilder {
public:
    explicit PipelineBuilder(PipelineBuilderContext& context) : _context(context) {}

    std::pair<ExecutionGroups, Pipelines> build();

private:
    PipelineBuilderContext& _context;
};
} // namespace pipeline
} // namespace starrocks
