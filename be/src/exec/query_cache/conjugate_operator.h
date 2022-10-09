// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <memory>

#include "exec/pipeline/operator.h"

namespace starrocks {
namespace query_cache {
class ConjugateOperator;
using ConjugateOperatorRawPtr = ConjugateOperator*;
using ConjugateOperatorPtr = std::shared_ptr<ConjugateOperator>;
class ConjugateOperatorFactory;
using ConjugateOperatorFactoryRawPtr = ConjugateOperatorFactory*;
using ConjugateOperatorFactoryPtr = std::shared_ptr<ConjugateOperatorFactory>;

// ConjugateOperator is used to join the pair of AggregateSinkOperator and AggregateSourceOperator together into
// a compound pre-cache per-tablet operator. for examples:
// 1. AggregateBlockingNode:  ConjugateOperator(AggregateBlockingSinkOperator, AggregateBlockingSourceOperator);
// 2. AggregateStreamingNode:  ConjugateOperator(AggregateStreamingSinkOperator, AggregateStreamingSourceOperator);
// 3. DistinctBlockingNode:  ConjugateOperator(DistinctBlockingSinkOperator, DistinctBlockingSourceOperator);
// 4. DistinctStreamingNode:  ConjugateOperator(DistinctStreamingSinkOperator, DistinctStreamingSourceOperator);
//
// When cache enabled, pipeline OlapScanOperator->ProjectOperator->AggregateBlockingSinkOperator will be transformed
// into the pipeline as follows.
// OlapScanOperator->MultilaneOperator(ProjectOperator)->MultilaneOperator(
//  ConjugateOperator(AggregateBlockingSinkOperator, AggregateBlockingSourceOperator))->CacheOperator->
//  AggregateBlockSinkOperator.
class ConjugateOperator : public pipeline::Operator {
public:
    ConjugateOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, const pipeline::OperatorPtr& sink_op,
                      const pipeline::OperatorPtr& source_op);
    ~ConjugateOperator() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    Status set_finished(RuntimeState* state) override;
    Status set_finishing(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;
    void set_precondition_ready(RuntimeState* state) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    Status reset_state(std::vector<ChunkPtr>&& chunks) override;

private:
    pipeline::OperatorPtr _sink_op;
    pipeline::OperatorPtr _source_op;
};

class ConjugateOperatorFactory : public pipeline::OperatorFactory {
public:
    ConjugateOperatorFactory(pipeline::OpFactoryPtr sink_op_factory, pipeline::OpFactoryPtr source_op_factory);
    ~ConjugateOperatorFactory() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    pipeline::OpFactoryPtr _sink_op_factory;
    pipeline::OpFactoryPtr _source_op_factory;
};
} // namespace query_cache
} // namespace starrocks
