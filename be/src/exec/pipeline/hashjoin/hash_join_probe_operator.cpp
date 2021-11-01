// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

namespace starrocks {
namespace pipeline {
HashJoinProbeOperator::HashJoinProbeOperator(int32_t id, const string& name, int32_t plan_node_id,
                                             HashJoiner* hash_joiner)
        : OperatorWithDependency(id, name, plan_node_id), _hash_joiner(hash_joiner) {}

bool HashJoinProbeOperator::has_output() const {
    return _hash_joiner->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    return _hash_joiner->need_input();
}

bool HashJoinProbeOperator::is_finished() const {
    return _hash_joiner->is_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _hash_joiner->push_chunk(state, std::move(const_cast<vectorized::ChunkPtr&>(chunk)));
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return _hash_joiner->pull_chunk(state);
}

void HashJoinProbeOperator::finish(RuntimeState* state) {
    if (!_is_finished) {
        _hash_joiner->enter_post_probe_phase();
        _is_finished = true;
    }
}

bool HashJoinProbeOperator::is_ready() const {
    return _hash_joiner->is_build_done();
}

HashJoinProbeOperatorFactory::HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           std::unique_ptr<HashJoiner>&& hash_joiner)
        : OperatorFactory(id, "hash_join_probe", plan_node_id), _hash_joiner(std::move(hash_joiner)) {}

Status HashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    return OperatorFactory::prepare(state);
}
void HashJoinProbeOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

OperatorPtr HashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashJoinProbeOperator>(_id, _name, _plan_node_id, _hash_joiner.get());
}

} // namespace pipeline
} // namespace starrocks