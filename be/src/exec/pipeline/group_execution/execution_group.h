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

#include <atomic>
#include <cstddef>
#include <functional>
#include <unordered_set>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group_builder.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
// execution group is a collection of pipelines
// clang-format off
template <typename T>
concept PipelineCallable = std::invocable<T, Pipeline*> &&
        (std::same_as<std::invoke_result_t<T, Pipeline*>, void> ||
         std::same_as<std::invoke_result_t<T, Pipeline*>, Status>);
// clang-format on
class ExecutionGroup {
public:
    ExecutionGroup(ExecutionGroupType type) : _type(type) {}
    virtual ~ExecutionGroup() = default;
    virtual Status prepare_pipelines(RuntimeState* state) = 0;
    virtual Status prepare_drivers(RuntimeState* state) = 0;
    virtual void submit_active_drivers() = 0;
    template <PipelineCallable Callable>
    auto for_each_pipeline(Callable&& call) {
        using ReturnType = std::invoke_result_t<Callable, Pipeline*>;
        if constexpr (std::same_as<ReturnType, void>) {
            for (auto pipeline : _pipelines) {
                call(pipeline);
            }
        } else {
            for (auto pipeline : _pipelines) {
                RETURN_IF_ERROR(call(pipeline));
            }
            return Status::OK();
        }
    }

    virtual void add_pipeline(PipelineRawPtr pipeline) = 0;
    virtual void close(RuntimeState* state) = 0;
    virtual void submit_next_driver() = 0;
    virtual bool is_empty() const = 0;
    virtual std::string to_string() const = 0;
    void attach_driver_executor(DriverExecutor* executor) { _executor = executor; }

    void count_down_pipeline(RuntimeState* state) {
        if (++_num_finished_pipelines == _num_pipelines) {
            state->fragment_ctx()->count_down_execution_group();
        }
    }

    void count_down_epoch_pipeline(RuntimeState* state) {
        if (++_num_epoch_finished_pipelines == _num_pipelines) {
            state->fragment_ctx()->count_down_epoch_pipeline(state);
        }
    }

    size_t total_logical_dop() const { return _total_logical_dop; }

    ExecutionGroupType type() const { return _type; }
    bool is_colocate_exec_group() const { return type() == ExecutionGroupType::COLOCATE; }

    bool contains(int32_t plan_node_id) { return _plan_node_ids.contains(plan_node_id); }

protected:
    // only used in colocate groups
    std::unordered_set<int32_t> _plan_node_ids;
    ExecutionGroupType _type;
    // total logical degree of parallelism
    // will be inited in prepare_pipelines
    size_t _total_logical_dop{};
    std::atomic<size_t> _num_finished_pipelines{};
    std::atomic<size_t> _num_epoch_finished_pipelines{};
    size_t _num_pipelines{};
    DriverExecutor* _executor;
    PipelineRawPtrs _pipelines;
    void clear_all_drivers(Pipelines& pipelines);
};

class NormalExecutionGroup final : public ExecutionGroup {
public:
    NormalExecutionGroup() : ExecutionGroup(ExecutionGroupType::NORMAL) {}
    ~NormalExecutionGroup() override = default;

    Status prepare_pipelines(RuntimeState* state) override;
    Status prepare_drivers(RuntimeState* state) override;
    void submit_active_drivers() override;
    void add_pipeline(PipelineRawPtr pipeline) override;

    void close(RuntimeState* state) override;
    // nothing to do
    void submit_next_driver() override {}
    bool is_empty() const override { return _pipelines.empty(); }
    std::string to_string() const override;
};

// execution group for colocate pipelines
// all pipelines in this group should have the same dop
// There should be no dependencies between the operators of multiple dops
class ColocateExecutionGroup final : public ExecutionGroup {
public:
    ColocateExecutionGroup(size_t physical_dop)
            : ExecutionGroup(ExecutionGroupType::COLOCATE), _physical_dop(physical_dop) {}
    ~ColocateExecutionGroup() override = default;

    Status prepare_pipelines(RuntimeState* state) override;
    Status prepare_drivers(RuntimeState* state) override;
    void submit_active_drivers() override;
    void add_pipeline(PipelineRawPtr pipeline) override;

    void close(RuntimeState* state) override;
    void submit_next_driver() override;
    bool is_empty() const override { return _pipelines.empty(); }
    std::string to_string() const override;
    void add_plan_node_id(int32_t plan_node_id) { _plan_node_ids.insert(plan_node_id); }

private:
    size_t _physical_dop;
    // TODO: add Pad to fix false sharing problems
    std::unique_ptr<std::atomic<int>[]> _submit_drivers;
};

} // namespace starrocks::pipeline