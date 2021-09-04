// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/exchange_source_operator.h"

#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status ExchangeSourceOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _row_desc, state->fragment_instance_id(), _plan_node_id, _num_sender,
            config::exchg_node_buffer_size_bytes, _runtime_profile, false, nullptr);
    return Status::OK();
}

Status ExchangeSourceOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

bool ExchangeSourceOperator::has_output() const {
    return _stream_recvr->has_output();
}

bool ExchangeSourceOperator::is_finished() const {
    return _stream_recvr->is_finished();
}

void ExchangeSourceOperator::finish(RuntimeState* state) {
    if (_is_finishing) {
        return;
    }
    _is_finishing = true;
    return _stream_recvr->close();
}

StatusOr<vectorized::ChunkPtr> ExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    std::unique_ptr<vectorized::Chunk> chunk = std::make_unique<vectorized::Chunk>();
    RETURN_IF_ERROR(_stream_recvr->get_chunk(&chunk));
    return std::move(chunk);
}

// Fixme(kks): The exchange seems don't need a abstract of morsel
void ExchangeSourceOperator::add_morsel(Morsel* morsel) {}

} // namespace starrocks::pipeline
