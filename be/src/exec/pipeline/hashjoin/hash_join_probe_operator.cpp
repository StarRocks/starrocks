// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

namespace starrocks::pipeline {

HashJoinProbeOperator::HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, int32_t driver_sequence, HashJoinerPtr join_prober,
                                             HashJoinerPtr join_builder)
        : OperatorWithDependency(factory, id, name, plan_node_id, driver_sequence),
          _join_prober(std::move(join_prober)),
          _join_builder(std::move(join_builder)) {}

void HashJoinProbeOperator::close(RuntimeState* state) {
    _join_prober->unref(state);
    if (_join_builder != _join_prober) {
        _join_builder->unref(state);
    }

    OperatorWithDependency::close(state);
}

Status HashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    if (_join_builder != _join_prober) {
        _join_builder->ref();
    }
    _join_prober->ref();

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));

    return Status::OK();
}

bool HashJoinProbeOperator::has_output() const {
    return _join_prober->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    return _join_prober->need_input();
}

bool HashJoinProbeOperator::is_finished() const {
    return _join_prober->is_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _join_prober->push_chunk(state, std::move(const_cast<vectorized::ChunkPtr&>(chunk)));
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return _join_prober->pull_chunk(state);
}

Status HashJoinProbeOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _join_prober->enter_post_probe_phase();
    return Status::OK();
}

Status HashJoinProbeOperator::set_finished(RuntimeState* state) {
    _join_prober->enter_eos_phase();
    _join_builder->set_prober_finished();
    return Status::OK();
}

bool HashJoinProbeOperator::is_ready() const {
    return _join_prober->is_build_done();
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
    return std::make_shared<HashJoinProbeOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                   _hash_joiner_factory->create_prober(driver_sequence),
                                                   _hash_joiner_factory->create_builder(driver_sequence));
}

} // namespace starrocks::pipeline
