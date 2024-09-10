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

#include <memory>

#include "exec/aggregator.h"
#include "exec/pipeline/operator.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
// TODO: implements cache-relation method
class SortedAggregateStreamingSinkOperator : public Operator {
public:
    SortedAggregateStreamingSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence,
                                         std::shared_ptr<SortedStreamingAggregator> aggregator);
    ~SortedAggregateStreamingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    ChunkPipelineAccumulator _accumulator;
    std::shared_ptr<SortedStreamingAggregator> _aggregator;
};

class SortedAggregateStreamingSinkOperatorFactory final : public OperatorFactory {
public:
    SortedAggregateStreamingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                StreamingAggregatorFactoryPtr aggregator_factory,
                                                const SpillProcessChannelFactoryPtr& _)
            : SortedAggregateStreamingSinkOperatorFactory(id, plan_node_id, std::move(aggregator_factory)) {}

    SortedAggregateStreamingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                StreamingAggregatorFactoryPtr aggregator_factory)
            : OperatorFactory(id, "aggregate_streaming_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~SortedAggregateStreamingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    StreamingAggregatorFactoryPtr _aggregator_factory = nullptr;
};

} // namespace starrocks::pipeline
