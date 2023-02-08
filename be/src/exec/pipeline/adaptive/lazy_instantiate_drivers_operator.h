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

#include <unordered_map>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

using PipelineGroupMap = std::unordered_map<SourceOperatorFactory*, Pipelines>;

/// Lazily instantiate drivers of pipelines.
///
/// The pipelines of a fragment instance are organized by groups.
/// See the comment of SourceOperatorFactory::AdaptiveState for detail.
///
/// Instantiate the drivers and maybe adjust DOP, when a pipeline group is ready.
class LazyInstantiateDriversOperator final : public SourceOperator {
public:
    LazyInstantiateDriversOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                   const int32_t driver_sequence, const PipelineGroupMap& unready_pipeline_groups);
    ~LazyInstantiateDriversOperator() override = default;

    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    void close(RuntimeState* state) override;

private:
    struct PipelineGroup {
        SourceOperatorFactory* leader_source_op;
        size_t original_leader_dop;
        Pipelines pipelines;

        PipelineGroup(SourceOperatorFactory* leader_source_op, size_t original_leader_dop, const Pipelines& pipelines)
                : leader_source_op(leader_source_op), original_leader_dop(original_leader_dop), pipelines(pipelines) {}
    };
    std::list<PipelineGroup> _unready_pipeline_groups;
};

class LazyInstantiateDriversOperatorFactory final : public SourceOperatorFactory {
public:
    LazyInstantiateDriversOperatorFactory(int32_t id, int32_t plan_node_id, PipelineGroupMap&& unready_pipeline_groups);
    ~LazyInstantiateDriversOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LazyInstantiateDriversOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                _unready_pipeline_groups);
    }

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }

private:
    const PipelineGroupMap _unready_pipeline_groups;
};

} // namespace starrocks::pipeline
