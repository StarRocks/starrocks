// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateBlockingSourceOperator : public SourceOperator {
public:
    AggregateBlockingSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                    AggregatorPtr aggregator)
            : SourceOperator(factory, id, "aggregate_blocking_source", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~AggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
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
        return std::make_shared<AggregateBlockingSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                 _aggregator_factory->get_or_create(driver_sequence));
    }

    bool need_local_shuffle() const override { return _need_local_shuffle; }
    void set_need_local_shuffle(bool need_local_shuffle) override { _need_local_shuffle = need_local_shuffle; }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
    // This flag will be inherited from the source operator of AggregateBlockingSinkOperator,
    // by calling `set_need_local_shuffle` when decomposing to pipeline.
    bool _need_local_shuffle = true;
};
} // namespace starrocks::pipeline
