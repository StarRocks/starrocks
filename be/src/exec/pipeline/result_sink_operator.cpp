// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/result_sink_operator.h"

#include "column/chunk.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/mysql_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status ResultSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);

    // Create profile
    _profile = std::make_unique<RuntimeProfile>("result sink");

    // Create sender
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(), 1024, &_sender));

    // Create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer = std::make_shared<MysqlResultWriter>(_sender.get(), _output_expr_ctxs, _profile.get());
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status ResultSinkOperator::close(RuntimeState* state) {
    Status st;
    // Close the writer
    if (_writer != nullptr) {
        st = _writer->close();
    }

    // Close sender
    if (_sender != nullptr) {
        if (_writer != nullptr) {
            _sender->update_num_written_rows(_writer->get_written_rows());
        }
        _sender->close(st);
    }

    state->exec_env()->result_mgr()->cancel_at_time(time(nullptr) + config::result_buffer_cancelled_interval_time,
                                                    state->fragment_instance_id());

    Operator::close(state);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ResultSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

bool ResultSinkOperator::need_input() const {
    if (is_finished()) {
        return false;
    }
    if (!_fetch_data_result) {
        return true;
    }
    auto* mysql_writer = down_cast<MysqlResultWriter*>(_writer.get());
    auto status = mysql_writer->try_add_batch(_fetch_data_result);
    if (status.ok()) {
        return status.value();
    } else {
        _last_error = status.status();
        return true;
    }
}

Status ResultSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (!_last_error.ok()) {
        return _last_error;
    }
    DCHECK(!_fetch_data_result);
    auto* mysql_writer = down_cast<MysqlResultWriter*>(_writer.get());
    auto status = mysql_writer->process_chunk(chunk.get());
    if (status.ok()) {
        _fetch_data_result = std::move(status.value());
        return mysql_writer->try_add_batch(_fetch_data_result).status();
    } else {
        return status.status();
    }
}
Status ResultSinkOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, row_desc, mem_tracker));
    return Status::OK();
}

void ResultSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
}
} // namespace starrocks::pipeline
