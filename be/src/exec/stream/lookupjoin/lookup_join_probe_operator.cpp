// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/stream/lookupjoin/lookup_join_probe_operator.h"

#include "column/chunk.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks::pipeline {

LookupJoinProbeOperator::LookupJoinProbeOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
        const std::shared_ptr<LookupJoinContext>& lookup_join_context)
        : Operator(factory, id, "lookup_join_probe", plan_node_id, driver_sequence),
          _lookup_join_context(lookup_join_context) {
    _lookup_join_context->ref();
}

Status LookupJoinProbeOperator::prepare(RuntimeState* state) {
    return Operator::prepare(state);
}

void LookupJoinProbeOperator::close(RuntimeState* state) {
    _lookup_join_context->unref(state);
    Operator::close(state);
}

Status LookupJoinProbeOperator::set_finishing(RuntimeState* state) {
    VLOG(1) << "probe set finished";
    _is_finished = true;
    _lookup_join_context->mark_finished(true);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> LookupJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from lookup join probe operator");
}

Status LookupJoinProbeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    VLOG(1) << "put the chunk:"<< chunk->num_rows();
    if (!_lookup_join_context->blocking_put(chunk)) {
        return Status::InternalError("Add chunk into lookup join queue failed.");
    }
    return Status::OK();
}

} // namespace starrocks::pipeline