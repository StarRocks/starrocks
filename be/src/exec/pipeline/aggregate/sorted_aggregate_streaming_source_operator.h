// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class SortedAggregateStreamingSourceOperator : public SourceOperator {
public:
    SortedAggregateStreamingSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           int32_t driver_sequence,
                                           std::shared_ptr<SortedStreamingAggregator> aggregator);
    ~SortedAggregateStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    mutable bool _is_finished = false;
    std::shared_ptr<SortedStreamingAggregator> _aggregator;
};

class SortedAggregateStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SortedAggregateStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                  StreamingAggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_streaming_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~SortedAggregateStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool need_local_shuffle() const override { return _need_local_shuffle; }
    void set_need_local_shuffle(bool need_local_shuffle) override { _need_local_shuffle = need_local_shuffle; }

private:
    StreamingAggregatorFactoryPtr _aggregator_factory = nullptr;
    bool _need_local_shuffle = true;
};

} // namespace starrocks::pipeline
