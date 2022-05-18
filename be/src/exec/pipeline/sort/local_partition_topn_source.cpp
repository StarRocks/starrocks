// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/local_partition_topn_source.h"

namespace starrocks::pipeline {

LocalPartitionTopnSourceOperator::LocalPartitionTopnSourceOperator(OperatorFactory* factory, int32_t id,
                                                                   int32_t plan_node_id, int32_t driver_sequence,
                                                                   LocalPartitionTopnContext* partition_topn_ctx)
        : SourceOperator(factory, id, "local_partition_topn_source", plan_node_id, driver_sequence),
          _partition_topn_ctx(partition_topn_ctx) {}

bool LocalPartitionTopnSourceOperator::has_output() const {
    return _partition_topn_ctx->has_output();
}

bool LocalPartitionTopnSourceOperator::is_finished() const {
    return _partition_topn_ctx->is_finished();
}

StatusOr<vectorized::ChunkPtr> LocalPartitionTopnSourceOperator::pull_chunk(RuntimeState* state) {
    return _partition_topn_ctx->pull_one_chunk_from_sorters();
}

LocalPartitionTopnSourceOperatorFactory::LocalPartitionTopnSourceOperatorFactory(
        int32_t id, int32_t plan_node_id, const LocalPartitionTopnContextFactoryPtr& partition_topn_ctx_factory)
        : SourceOperatorFactory(id, "local_partition_topn_source", plan_node_id),
          _partition_topn_ctx_factory(partition_topn_ctx_factory) {}
OperatorPtr LocalPartitionTopnSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<LocalPartitionTopnSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                              _partition_topn_ctx_factory->create(driver_sequence));
}
} // namespace starrocks::pipeline
