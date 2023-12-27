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
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::pipeline {
class SpillableAggregateBlockingSourceOperator final : public AggregateBlockingSourceOperator {
public:
    template <class... Args>
    SpillableAggregateBlockingSourceOperator(const AggregatorPtr& aggregator,
                                             SortedStreamingAggregatorPtr stream_aggregator, Args&&... args)
            : AggregateBlockingSourceOperator(aggregator, std::forward<Args>(args)...,
                                              "spillable_aggregate_blocking_source"),
              _stream_aggregator(std::move(stream_aggregator)) {}

    ~SpillableAggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    [[nodiscard]] Status set_finishing(RuntimeState* state) override;
    [[nodiscard]] Status set_finished(RuntimeState* state) override;

    [[nodiscard]] Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    [[nodiscard]] StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    [[nodiscard]] Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    [[nodiscard]] StatusOr<ChunkPtr> _pull_spilled_chunk(RuntimeState* state);

    bool _is_finished = false;
    bool _has_last_chunk = true;
    ChunkPipelineAccumulator _accumulator;
    SortedStreamingAggregatorPtr _stream_aggregator = nullptr;
};

class SpillableAggregateBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillableAggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                    AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "spillable_aggregate_blocking_source", plan_node_id),
              _hash_aggregator_factory(std::move(aggregator_factory)) {}

    ~SpillableAggregateBlockingSourceOperatorFactory() override = default;

    [[nodiscard]] Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    AggregatorFactoryPtr _hash_aggregator_factory;
    // _stream_aggregatory_factory is only used when spilling happens
    StreamingAggregatorFactoryPtr _stream_aggregator_factory;
};
} // namespace starrocks::pipeline