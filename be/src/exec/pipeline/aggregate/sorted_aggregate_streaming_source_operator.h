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

#include "exec/aggregator.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class SortedAggregateStreamingSourceOperator : public SourceOperator {
public:
    SortedAggregateStreamingSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           int32_t driver_sequence, SortedStreamingAggregatorPtr aggregator);
    ~SortedAggregateStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    mutable bool _is_finished = false;
    SortedStreamingAggregatorPtr _aggregator;
};

class SortedAggregateStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SortedAggregateStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                  StreamingAggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_streaming_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~SortedAggregateStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    StreamingAggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
