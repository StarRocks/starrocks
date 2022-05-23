// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/local_partition_topn_sink.h"

namespace starrocks::pipeline {

LocalPartitionTopnSinkOperator::LocalPartitionTopnSinkOperator(OperatorFactory* factory, int32_t id,
                                                               int32_t plan_node_id, int32_t driver_sequence,
                                                               LocalPartitionTopnContext* partition_topn_ctx)
        : Operator(factory, id, "local_partition_topn_sink", plan_node_id, driver_sequence),
          _partition_topn_ctx(partition_topn_ctx) {}

Status LocalPartitionTopnSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _partition_topn_ctx->prepare(state);
}

StatusOr<vectorized::ChunkPtr> LocalPartitionTopnSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from local partition topn sink operator.");
}

Status LocalPartitionTopnSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _partition_topn_ctx->push_one_chunk_to_partitioner(chunk);
}

Status LocalPartitionTopnSinkOperator::set_finishing(RuntimeState* state) {
    RETURN_IF_ERROR(_partition_topn_ctx->transfer_all_chunks_from_partitioner_to_sorters(state));
    _partition_topn_ctx->sink_complete();
    _is_finished = true;
    return Status::OK();
}

LocalPartitionTopnSinkOperatorFactory::LocalPartitionTopnSinkOperatorFactory(
        int32_t id, int32_t plan_node_id, const LocalPartitionTopnContextFactoryPtr& partition_topn_ctx_factory)
        : OperatorFactory(id, "local_partition_topn_sink", plan_node_id),
          _partition_topn_ctx_factory(partition_topn_ctx_factory) {}
OperatorPtr LocalPartitionTopnSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<LocalPartitionTopnSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                            _partition_topn_ctx_factory->create(driver_sequence));
}
}; // namespace starrocks::pipeline
