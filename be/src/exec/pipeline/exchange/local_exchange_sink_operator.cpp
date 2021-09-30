// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/local_exchange_sink_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status LocalExchangeSinkOperator::prepare(RuntimeState* state) {
    _exchanger->increment_sink_number();
    Operator::prepare(state);
    return Status::OK();
}

bool LocalExchangeSinkOperator::need_input() const {
    return !_is_finished && _exchanger->need_input();
}

StatusOr<vectorized::ChunkPtr> LocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from local exchange sink.");
}

void LocalExchangeSinkOperator::finish(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->finish(state);
    }
}

Status LocalExchangeSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _exchanger->accept(chunk);
    return Status::OK();
}

} // namespace starrocks::pipeline
