// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/stream/imt_state_table.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class StreamingAggregateSourceOperator : public SourceOperator {
public:
    StreamingAggregateSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, AggregatorPtr aggregator, IMTStateTablePtr imt_detail,
                                     IMTStateTablePtr imt_agg_result)
            : SourceOperator(factory, id, "streaming_aggregate_source", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)),
              _imt_detail(imt_detail),
              _imt_agg_result(imt_agg_result) {
        _aggregator->ref();
        if (_imt_agg_result) {
            _imt_agg_result_sink = _imt_agg_result->olap_table_sink();
        }
    }

    ~StreamingAggregateSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk, RuntimeState* state);

    // It is used to perform aggregation algorithms shared by
    // StreamingAggregateSinkOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    IMTStateTablePtr _imt_detail;
    IMTStateTablePtr _imt_agg_result;
    stream_load::OlapTableSink* _imt_agg_result_sink;

    // Whether prev operator has no output
    mutable bool _is_finished = false;
    mutable bool _is_open_done = false;
};

class StreamingAggregateSourceOperatorFactory final : public SourceOperatorFactory {
public:
    StreamingAggregateSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory,
                                            IMTStateTablePtr imt_detail, IMTStateTablePtr imt_agg_result)
            : SourceOperatorFactory(id, "streaming_aggregate_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _imt_detail(imt_detail),
              _imt_agg_result(imt_agg_result) {}

    ~StreamingAggregateSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<StreamingAggregateSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  _aggregator_factory->get_or_create(driver_sequence),
                                                                  _imt_detail, _imt_agg_result);
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(OperatorFactory::prepare(state));
        if (_imt_agg_result) {
            RETURN_IF_ERROR(_imt_agg_result->prepare(state));
        }
        return Status::OK();
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
    IMTStateTablePtr _imt_detail;
    IMTStateTablePtr _imt_agg_result;
};
} // namespace starrocks::pipeline
