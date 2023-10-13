// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <utility>

#include "exec/aggregator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/stream/aggregate/stream_aggregator.h"

namespace starrocks::stream {
using StreamAggregatorPtr = std::shared_ptr<StreamAggregator>;
using StreamAggregatorFactory = AggregatorFactoryBase<StreamAggregator>;
using StreamAggregatorFactoryPtr = std::shared_ptr<StreamAggregatorFactory>;

class StreamAggregateOperator : public pipeline::SourceOperator {
public:
    StreamAggregateOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            StreamAggregatorPtr aggregator)
            : pipeline::SourceOperator(factory, id, "stream_aggregate", plan_node_id, false, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~StreamAggregateOperator() override = default;

    bool has_output() const override;
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override;
    [[nodiscard]] Status set_finishing(RuntimeState* state) override;
    [[nodiscard]] Status set_finished(RuntimeState* state) override;

    bool is_epoch_finished() const override;
    [[nodiscard]] Status set_epoch_finishing(RuntimeState* state) override;
    [[nodiscard]] Status set_epoch_finished(RuntimeState* state) override;
    [[nodiscard]] Status reset_epoch(RuntimeState* state) override;

    [[nodiscard]] StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    [[nodiscard]] Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    [[nodiscard]] Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    StreamAggregatorPtr _aggregator = nullptr;
    ChunkPtr _epoch_chunk = nullptr;
    // Whether prev operator has no output
    bool _is_input_finished = false;
    // Mark whether aggregator is already epoch finished.
    bool _is_epoch_finished = false;
    // Mark whether aggregator has output or not.
    bool _has_output = true;
};

class StreamAggregateOperatorFactory final : public pipeline::SourceOperatorFactory {
public:
    StreamAggregateOperatorFactory(int32_t id, int32_t plan_node_id, StreamAggregatorFactoryPtr aggregator_factory)
            : pipeline::SourceOperatorFactory(id, "stream_aggregate", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    // used for testing
    StreamAggregateOperatorFactory(int32_t id, int32_t plan_node_id, StreamAggregatorPtr aggregator)
            : pipeline::SourceOperatorFactory(id, "stream_aggregate", plan_node_id),
              _aggregator(std::move(aggregator)) {}

    ~StreamAggregateOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        if (_aggregator) {
            return std::make_shared<StreamAggregateOperator>(this, _id, _plan_node_id, driver_sequence, _aggregator);
        } else {
            return std::make_shared<StreamAggregateOperator>(this, _id, _plan_node_id, driver_sequence,
                                                             _aggregator_factory->get_or_create(driver_sequence));
        }
    }

private:
    StreamAggregatorFactoryPtr _aggregator_factory = nullptr;
    StreamAggregatorPtr _aggregator = nullptr;
};
} // namespace starrocks::stream
