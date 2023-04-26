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
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {
class AggregateBlockingSourceOperator : public SourceOperator {
public:
    AggregateBlockingSourceOperator(AggregatorPtr aggregator, OperatorFactory* factory, int32_t id,
                                    int32_t plan_node_id, int32_t driver_sequence,
                                    const char* name = "aggregate_blocking_source")
            : SourceOperator(factory, id, name, plan_node_id, driver_sequence), _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~AggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

protected:
    // It is used to perform aggregation algorithms shared by
    // AggregateBlockingSinkOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
};

class AggregateBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_blocking_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateBlockingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateBlockingSourceOperator>(_aggregator_factory->get_or_create(driver_sequence),
                                                                 this, _id, _plan_node_id, driver_sequence);
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
