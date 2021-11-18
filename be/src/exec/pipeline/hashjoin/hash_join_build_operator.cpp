// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/hashjoin/hash_join_build_operator.h"

namespace starrocks {
namespace pipeline {

HashJoinBuildOperator::HashJoinBuildOperator(int32_t id, const string& name, int32_t plan_node_id,
                                             HashJoinerPtr hash_joiner)
        : Operator(id, name, plan_node_id), _hash_joiner(hash_joiner) {
    _hash_joiner->ref_no_barrier();
}

Status HashJoinBuildOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _hash_joiner->append_chunk_to_ht(state, chunk);
}

Status HashJoinBuildOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _hash_joiner->prepare(state);
}
Status HashJoinBuildOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(_hash_joiner->unref(state));
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> HashJoinBuildOperator::pull_chunk(RuntimeState* state) {
    const char* msg = "pull_chunk not supported in HashJoinBuildOperator";
    CHECK(false) << msg;
    return Status::NotSupported(msg);
}

void HashJoinBuildOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _hash_joiner->build_ht(state);
}

HashJoinBuildOperatorFactory::HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           HashJoinerFactoryPtr hash_joiner_factory)
        : OperatorFactory(id, "hash_join_build", plan_node_id), _hash_joiner_factory(hash_joiner_factory) {}

Status HashJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return _hash_joiner_factory->prepare(state);
}

void HashJoinBuildOperatorFactory::close(RuntimeState* state) {
    _hash_joiner_factory->close(state);
    OperatorFactory::close(state);
}

OperatorPtr HashJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashJoinBuildOperator>(_id, _name, _plan_node_id,
                                                   _hash_joiner_factory->create(driver_sequence));
}
} // namespace pipeline
} // namespace starrocks