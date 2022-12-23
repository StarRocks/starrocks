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

#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

/// Lazily create drivers of pipelines.
///
/// The pipelines of a fragment instance are organized by groups.
/// - The first pipeline of a group has a READY or NOT_READY source operator (leader source operator),
///   and the non-first pipeline of a group has a INHERIT operator.
/// - The pipelines of a group can create and execute drivers,
///   only when the leader source operator of the group becomes READY.
///
/// For example, the following fragment contains two pipeline groups: [pipe#1], [pipe#2, pipe#3].
///
///            ExchangeSink
///  pipe#3         |
///          BlockingAggSource(INHERIT)
///
///          BlockingAggSink
///  pipe#2         |
///          CsSource(NOT_READY)
///
///              CsSink
///  pipe#1        |
///          ScanNode(READY)
class LazyCreateDriversOperator final : public SourceOperator {
public:
    LazyCreateDriversOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                              const std::vector<Pipelines>& unready_pipeline_groups);
    ~LazyCreateDriversOperator() override = default;

    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    void close(RuntimeState* state) override;

private:
    struct PipelineItem {
        PipelinePtr pipeline;
        size_t original_dop;

        PipelineItem(const PipelinePtr& pipeline, size_t originalDop) : pipeline(pipeline), original_dop(originalDop) {}
    };
    using PipelineItems = std::vector<PipelineItem>;
    std::list<PipelineItems> _unready_pipeline_groups;
};

class LazyCreateDriversOperatorFactory final : public SourceOperatorFactory {
public:
    LazyCreateDriversOperatorFactory(int32_t id, int32_t plan_node_id,
                                     std::vector<Pipelines>&& unready_pipeline_groups);
    ~LazyCreateDriversOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<LazyCreateDriversOperator>(this, _id, _plan_node_id, driver_sequence,
                                                           _unready_pipeline_groups);
    }

private:
    const std::vector<Pipelines> _unready_pipeline_groups;
};

} // namespace starrocks::pipeline
