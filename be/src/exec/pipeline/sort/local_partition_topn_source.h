// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/sort/local_partition_topn_context.h"
#include "exec/pipeline/source_operator.h"

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
class LocalPartitionTopnSourceOperator : public SourceOperator {
public:
    LocalPartitionTopnSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, LocalPartitionTopnContext* partition_topn_ctx);

    ~LocalPartitionTopnSourceOperator() override = default;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    LocalPartitionTopnContext* _partition_topn_ctx;
};

class LocalPartitionTopnSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalPartitionTopnSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                            const LocalPartitionTopnContextFactoryPtr& partition_topn_ctx_factory);

    ~LocalPartitionTopnSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    LocalPartitionTopnContextFactoryPtr _partition_topn_ctx_factory;
};
} // namespace starrocks::pipeline
