// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

namespace starrocks {
namespace pipeline {

HashJoinProbeOperator::HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, HashJoinerPtr probe_hash_joiner,
                                             HashJoinerPtr build_hash_joiner)
        : OperatorWithDependency(factory, id, name, plan_node_id),
          _probe_hash_joiner(std::move(probe_hash_joiner)),
          _build_hash_joiner(std::move(build_hash_joiner)) {}

Status HashJoinProbeOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(_probe_hash_joiner->unref(state));
    if (_build_hash_joiner != _probe_hash_joiner) {
        RETURN_IF_ERROR(_build_hash_joiner->unref(state));
    }

    return OperatorWithDependency::close(state);
}

Status HashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    if (_build_hash_joiner != _probe_hash_joiner) {
        _build_hash_joiner->ref();
    }
    _probe_hash_joiner->ref();

    // The buildable hash joiner is prepared in HashJoinBuildOperator::prepare,
    // while the read only hash joiner is prepared in HashJoinProbeOperator::prepare.
    if (!_probe_hash_joiner->is_buildable()) {
        RETURN_IF_ERROR(_probe_hash_joiner->prepare(state));
    }

    return Status::OK();
}

bool HashJoinProbeOperator::has_output() const {
    return _probe_hash_joiner->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    return _probe_hash_joiner->need_input();
}

bool HashJoinProbeOperator::is_finished() const {
    return _probe_hash_joiner->is_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _probe_hash_joiner->push_chunk(state, std::move(const_cast<vectorized::ChunkPtr&>(chunk)));
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return _probe_hash_joiner->pull_chunk(state);
}

void HashJoinProbeOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _probe_hash_joiner->enter_post_probe_phase();
}

void HashJoinProbeOperator::set_finished(RuntimeState* state) {
    _probe_hash_joiner->enter_eos_phase();
    _probe_hash_joiner->set_finished();
}

bool HashJoinProbeOperator::is_ready() const {
    return _probe_hash_joiner->is_build_done();
}

HashJoinProbeOperatorFactory::HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           HashJoinerFactoryPtr hash_joiner_factory)
        : OperatorFactory(id, "hash_join_probe", plan_node_id), _hash_joiner_factory(std::move(hash_joiner_factory)) {}

Status HashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    return OperatorFactory::prepare(state);
}
void HashJoinProbeOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

OperatorPtr HashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashJoinProbeOperator>(this, _id, _name, _plan_node_id,
                                                   _hash_joiner_factory->create_probe_hash_joiner(driver_sequence),
                                                   _hash_joiner_factory->create_build_hash_joiner(driver_sequence));
}

} // namespace pipeline
} // namespace starrocks
