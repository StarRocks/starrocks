// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"
#include "exec/stream/imt_state_table.h"

namespace starrocks::pipeline {
class StreamingAggregateSinkOperator : public Operator {
public:
    StreamingAggregateSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                   AggregatorPtr aggregator, IMTStateTablePtr imt_detail, IMTStateTablePtr imt_agg_result)
            : Operator(factory, id, "streaming_aggregate_sink", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)),
              _imt_detail(imt_detail), _imt_agg_result(imt_agg_result) {
        _aggregator->set_aggr_phase(AggrPhase1);
        _aggregator->ref();
    }
    ~StreamingAggregateSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _aggregator->is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::FORCE_PREAGGREGATION
    Status _push_chunk_by_force_preaggregation(const size_t chunk_size);

    // It is used to perform aggregation algorithms shared by
    // StreamingAggregateSourceOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
    IMTStateTablePtr _imt_detail;
    IMTStateTablePtr _imt_agg_result;
};

class StreamingAggregateSinkOperatorFactory final : public OperatorFactory {
public:
    StreamingAggregateSinkOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
                                          IMTStateTablePtr imt_detail, IMTStateTablePtr imt_agg_result)
            : OperatorFactory(id, "streaming_aggregate_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _imt_detail(imt_detail), _imt_agg_result(imt_agg_result) {}

    ~StreamingAggregateSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<StreamingAggregateSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                _aggregator_factory->get_or_create(driver_sequence),
                                                                _imt_detail, _imt_agg_result);
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
    IMTStateTablePtr _imt_detail;
    IMTStateTablePtr _imt_agg_result;
};
} // namespace starrocks::pipeline
