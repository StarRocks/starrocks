// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_base_operator.h"

namespace starrocks::pipeline {

AggregateBaseOperator::AggregateBaseOperator(int32_t id, std::string name, int32_t plan_node_id, const TPlanNode& tnode)
        : Operator(id, name, plan_node_id), vectorized::AggregateBase(tnode), _tnode(tnode) {}

Status AggregateBaseOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    // TODO(hcf) which obj pool should I use
    _pool = state->obj_pool();
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);

    RETURN_IF_ERROR(AggregateBase::_init(_tnode, state));
    return AggregateBase::_prepare(state);

    // TODO(hcf) missing expr's prepre and open process
    // according to AggregateStreamingNode::open or AggregateBlockingNode::open
}

Status AggregateBaseOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBase::_close(state));
    return Operator::close(state);
}

} // namespace starrocks::pipeline