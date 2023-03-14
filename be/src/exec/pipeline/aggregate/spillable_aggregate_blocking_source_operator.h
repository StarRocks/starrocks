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

#include <utility>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/sorted_streaming_aggregator.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillableAggregateBlockingSourceOperator final : public AggregateBlockingSourceOperator {
public:
    template <class... Args>
    SpillableAggregateBlockingSourceOperator(const SortedStreamingAggregatorPtr& aggregator, Args&&... args)
            : AggregateBlockingSourceOperator(aggregator, std::forward<Args>(args)...,
                                              "spillable_aggregate_source_operator"),
              _aggregator(aggregator) {}

    ~SpillableAggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    bool pending_finish() const override { return _aggregator->has_pending_restore(); }

private:
    StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    bool _is_finished = false;
    bool _has_last_chunk = true;
    ChunkPipelineAccumulator _accumulator;
    SortedStreamingAggregatorPtr _aggregator = nullptr;
};

class SpillableAggregateBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillableAggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                    StreamingAggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_aggregate_blocking_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~SpillableAggregateBlockingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SpillableAggregateBlockingSourceOperator>(
                _aggregator_factory->get_or_create(driver_sequence), this, _id, _plan_node_id, driver_sequence);
    }

private:
    StreamingAggregatorFactoryPtr _aggregator_factory;
};
} // namespace starrocks::pipeline