// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/olap_table_sink_operator.h"

#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/query_statistics.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status OlapTableSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);

    state->set_per_fragment_instance_idx(_sender_id);

    RETURN_IF_ERROR(_sink->prepare(state));

    RETURN_IF_ERROR(_sink->try_open(state));

    return Status::OK();
}

void OlapTableSinkOperator::close(RuntimeState* state) {
    _unique_metrics->copy_all_info_strings_from(_sink->profile());
    _unique_metrics->copy_all_counters_from(_sink->profile());

    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> OlapTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::NotSupported("Shouldn't pull chunk from olap table sink operator");
}

bool OlapTableSinkOperator::is_finished() const {
    return _is_finished;
}

bool OlapTableSinkOperator::pending_finish() const {
    // audit report not finish, we need check until finish
    if (!_is_audit_report_done) {
        return true;
    }

    // sink's open not finish, we need check util finish
    if (!_is_open_done) {
        if (!_sink->is_open_done()) {
            return true;
        }
        _is_open_done = true;
        // since is_open_done(), open_wait will not block
        auto st = _sink->open_wait();
        if (!st.ok()) {
            _fragment_ctx->cancel(st);
            return false;
        }
    }

    if (!_sink->is_close_done()) {
        auto st = _sink->try_close(_fragment_ctx->runtime_state());
        if (!st.ok()) {
            _fragment_ctx->cancel(st);
            return false;
        }
        return true;
    }

    auto st = _sink->close(_fragment_ctx->runtime_state(), Status::OK());
    if (!st.ok()) {
        _fragment_ctx->cancel(st);
    }

    return false;
}

Status OlapTableSinkOperator::set_cancelled(RuntimeState* state) {
    return _sink->close(state, Status::Cancelled("Cancelled by pipeline engine"));
}

Status OlapTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        _is_audit_report_done = false;
        auto* executor = state->fragment_ctx()->enable_resource_group() ? state->exec_env()->wg_driver_executor()
                                                                        : state->exec_env()->driver_executor();
        executor->report_audit_statistics(state->query_ctx(), state->fragment_ctx(), &_is_audit_report_done);
    }
    if (_is_open_done) {
        // sink's open already finish, we can try_close
        return _sink->try_close(state);
    } else {
        // sink's open not finish, we need check in pending_finish() before close
        return Status::OK();
    }
}

bool OlapTableSinkOperator::need_input() const {
    if (is_finished()) {
        return false;
    }

    if (!_is_open_done && !_sink->is_open_done()) {
        return false;
    }

    return !_sink->is_full();
}

Status OlapTableSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (!_is_open_done) {
        _is_open_done = true;
        // we can be here cause _sink->is_open_done() return true
        // so that open_wait() will not block
        RETURN_IF_ERROR(_sink->open_wait());
    }

    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    // send_chunk() use internal queue, we check is_full() before call send_chunk(), so it will not block
    return _sink->send_chunk(state, chunk.get());
}

OperatorPtr OlapTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    _increment_num_sinkers_no_barrier();
    if (driver_sequence == 0) {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sink0, _fragment_ctx, _num_sinkers);
    } else {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sinks[driver_sequence - 1].get(), _fragment_ctx, _num_sinkers);
    }
}

Status OlapTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    return Status::OK();
}

void OlapTableSinkOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline
