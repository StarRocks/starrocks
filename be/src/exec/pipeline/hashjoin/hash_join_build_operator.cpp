// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/hashjoin/hash_join_build_operator.h"

namespace starrocks {
namespace pipeline {

HashJoinBuildOperator::HashJoinBuildOperator(int32_t id, const string& name, int32_t plan_node_id,
                                             HashJoiner* hash_joiner)
        : Operator(id, name, plan_node_id), _hash_joiner(hash_joiner) {}
Status HashJoinBuildOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _hash_joiner->append_chunk_to_ht(state, chunk);
}

StatusOr<vectorized::ChunkPtr> HashJoinBuildOperator::pull_chunk(RuntimeState* state) {
    const char* msg = "pull_chunk not supported in HashJoinBuildOperator";
    CHECK(false) << msg;
    return Status::NotSupported(msg);
}

void HashJoinBuildOperator::finish(RuntimeState* state) {
    if (!_is_finished) {
        _hash_joiner->build_ht(state);
        _is_finished = true;
    }
}

HashJoinBuildOperatorFactory::HashJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, HashJoiner* hash_joiner)
        : OperatorFactory(id, "hash_join_build", plan_node_id), _hash_joiner(hash_joiner) {}

Status HashJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return _hash_joiner->prepare(state);
}

void HashJoinBuildOperatorFactory::close(RuntimeState* state) {
    _hash_joiner->close(state);
    OperatorFactory::close(state);
}

OperatorPtr HashJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashJoinBuildOperator>(_id, _name, _plan_node_id, _hash_joiner);
}
} // namespace pipeline
} // namespace starrocks