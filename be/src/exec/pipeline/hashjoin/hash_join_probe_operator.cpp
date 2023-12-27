// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

HashJoinProbeOperator::HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, int32_t driver_sequence, HashJoinerPtr join_prober,
                                             HashJoinerPtr join_builder)
        : OperatorWithDependency(factory, id, name, plan_node_id, false, driver_sequence),
          _join_prober(std::move(join_prober)),
          _join_builder(std::move(join_builder)) {}

void HashJoinProbeOperator::close(RuntimeState* state) {
    if (_join_prober != _join_builder) {
        _join_prober->unref(state);
    }

    _join_builder->decr_prober(state);

    OperatorWithDependency::close(state);
}

Status HashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    _join_builder->incr_prober();

    if (_join_builder != _join_prober) {
        _join_prober->ref();
    }

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));

    return Status::OK();
}

bool HashJoinProbeOperator::has_output() const {
    return _join_prober->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    if (_join_prober->need_input()) {
        return true;
    }

    if (_join_prober != _join_builder && is_ready()) {
        // If hasn't referenced hash table, return true to reference hash table in push_chunk.
        return !_join_prober->has_referenced_hash_table();
    }
    return false;
}

bool HashJoinProbeOperator::is_finished() const {
    return _join_prober->is_done();
}

bool HashJoinProbeOperator::is_ready() const {
    return _join_builder->is_build_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    RETURN_IF_ERROR(_join_prober->push_chunk(state, std::move(const_cast<ChunkPtr&>(chunk))));
    return Status::OK();
}

StatusOr<ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return _join_prober->pull_chunk(state);
}

Status HashJoinProbeOperator::set_finishing(RuntimeState* state) {
    _join_prober->enter_post_probe_phase();
    return Status::OK();
}

Status HashJoinProbeOperator::set_finished(RuntimeState* state) {
    _join_prober->enter_eos_phase();
    _join_builder->set_prober_finished();
    return Status::OK();
}

Status HashJoinProbeOperator::_reference_builder_hash_table_once() {
    // non-broadcast join directly return as _join_prober == _join_builder,
    // but broadcast should refer to the shared join builder
    if (_join_prober == _join_builder) {
        return Status::OK();
    }

    if (!is_ready()) {
        return Status::OK();
    }

    if (_join_prober->has_referenced_hash_table()) {
        return Status::OK();
    }

    TRY_CATCH_ALLOC_SCOPE_START()
    _join_prober->reference_hash_table(_join_builder.get());
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status HashJoinProbeOperator::reset_state(RuntimeState* state, const vector<ChunkPtr>& refill_chunks) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    // Reset probe state only when it has valid state after referencing the build hash table.
    if (_join_prober->has_referenced_hash_table()) {
        RETURN_IF_ERROR(_join_prober->reset_probe(state));
    }
    return Status::OK();
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

OperatorPtr HashJoinProbeOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<HashJoinProbeOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                   _hash_joiner_factory->create_prober(dop, driver_sequence),
                                                   _hash_joiner_factory->get_builder(dop, driver_sequence));
}

} // namespace starrocks::pipeline
