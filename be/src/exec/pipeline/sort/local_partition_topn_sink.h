// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/pipeline/sort/local_partition_topn_context.h"

namespace starrocks::pipeline {

// The purpose of LocalPartitionTopn{Sink/Source}Operator is to reduce the amount of data,
// so the output chunks are still remain unordered
//                                   ┌────► topn─────┐
//                                   │               │
// (unordered)                       │               │                  (unordered)
// inputChunks ───────► partitioner ─┼────► topn ────┼─► gather ─────► outputChunks
//                                   │               │
//                                   │               │
//                                   └────► topn ────┘
class LocalPartitionTopnSinkOperator : public Operator {
public:
    LocalPartitionTopnSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                   LocalPartitionTopnContext* partition_topn_ctx);

    ~LocalPartitionTopnSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

private:
    bool _is_finished = false;

    LocalPartitionTopnContext* _partition_topn_ctx;
};

class LocalPartitionTopnSinkOperatorFactory final : public OperatorFactory {
public:
    LocalPartitionTopnSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                          const LocalPartitionTopnContextFactoryPtr& partition_topn_ctx_factory);

    ~LocalPartitionTopnSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    LocalPartitionTopnContextFactoryPtr _partition_topn_ctx_factory;
};
} // namespace starrocks::pipeline
