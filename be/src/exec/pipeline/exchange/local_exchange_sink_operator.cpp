// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/exchange/local_exchange_sink_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status LocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->increment_sink_number();
    return Status::OK();
}

bool LocalExchangeSinkOperator::need_input() const {
    return !_is_finished && _exchanger->need_input();
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from local exchange sink.");
}

Status LocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _exchanger->finish(state);
    return Status::OK();
}

Status LocalExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _exchanger->accept(chunk, _driver_sequence);
}

} // namespace starrocks::pipeline
