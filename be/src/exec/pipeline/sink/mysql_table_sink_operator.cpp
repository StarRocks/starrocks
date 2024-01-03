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

#include "exec/pipeline/sink/mysql_table_sink_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/expr.h"
#include "runtime/mysql_table_writer.h"
#include "runtime/runtime_state.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"
#include "util/spinlock.h"

namespace starrocks::pipeline {

class MysqlTableSinkIOBuffer final : public SinkIOBuffer {
public:
    MysqlTableSinkIOBuffer(const TMysqlTableSink& t_mysql_table_sink, std::vector<ExprContext*>& output_expr_ctxs,
                           int32_t num_sinkers, FragmentContext* fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _t_mysql_table_sink(t_mysql_table_sink),
              _output_expr_ctxs(output_expr_ctxs),
              _fragment_ctx(fragment_ctx) {}

    ~MysqlTableSinkIOBuffer() override = default;

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override;

    Status _open_mysql_table_writer();

    TMysqlTableSink _t_mysql_table_sink;
    const std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<MysqlTableWriter> _writer;
    FragmentContext* _fragment_ctx;
};

Status MysqlTableSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }
    _state = state;

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
    int ret = bthread::execution_queue_start<ChunkPtr>(_exec_queue_id.get(), &options,
                                                       &MysqlTableSinkIOBuffer::execute_io_task, this);
    if (ret != 0) {
        _exec_queue_id.reset();
        return Status::InternalError("start execution queue error");
    }

    return Status::OK();
}

void MysqlTableSinkIOBuffer::close(RuntimeState* state) {
    _writer.reset();
    SinkIOBuffer::close(state);
}

void MysqlTableSinkIOBuffer::_process_chunk(bthread::TaskIterator<ChunkPtr>& iter) {
    DeferOp op([&]() {
        auto nc = _num_pending_chunks.fetch_sub(1);
        DCHECK_GE(nc, 1L);
    });

    if (_is_finished) {
        return;
    }

    if (_is_cancelled && !_is_finished) {
        if (_num_pending_chunks == 1) {
            close(_state);
        }
        return;
    }

    if (_writer == nullptr) {
        if (Status status = _open_mysql_table_writer(); !status.ok()) {
            LOG(WARNING) << "open mysql table writer failed, error: " << status.to_string();
            _fragment_ctx->cancel(status);
            return;
        }
    }

    const auto& chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        auto nc = _num_pending_chunks.load();
        DCHECK_EQ(nc, 1L);
        close(_state);
        return;
    }
    if (Status status = _writer->append(chunk.get()); !status.ok()) {
        LOG(WARNING) << "add chunk to mysql table writer failed, error: " << status.to_string();
        _fragment_ctx->cancel(status);
        return;
    }
}

Status MysqlTableSinkIOBuffer::_open_mysql_table_writer() {
    DCHECK(_writer == nullptr);
    _writer = std::make_unique<MysqlTableWriter>(_output_expr_ctxs, 1024);
    MysqlConnInfo conn_info;
    conn_info.host = _t_mysql_table_sink.host;
    conn_info.port = _t_mysql_table_sink.port;
    conn_info.user = _t_mysql_table_sink.user;
    conn_info.passwd = _t_mysql_table_sink.passwd;
    conn_info.db = _t_mysql_table_sink.db;

    return _writer->open(conn_info, _t_mysql_table_sink.table);
}

Status MysqlTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _mysql_table_sink_buffer->prepare(state, _unique_metrics.get());
}

void MysqlTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool MysqlTableSinkOperator::need_input() const {
    return _mysql_table_sink_buffer->need_input();
}

bool MysqlTableSinkOperator::is_finished() const {
    return _mysql_table_sink_buffer->is_finished();
}

Status MysqlTableSinkOperator::set_finishing(RuntimeState* state) {
    return _mysql_table_sink_buffer->set_finishing();
}

bool MysqlTableSinkOperator::pending_finish() const {
    return !_mysql_table_sink_buffer->is_finished();
}

Status MysqlTableSinkOperator::set_cancelled(RuntimeState* state) {
    _mysql_table_sink_buffer->cancel_one_sinker();
    return Status::OK();
}

StatusOr<ChunkPtr> MysqlTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from mysql table sink operator");
}

Status MysqlTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _mysql_table_sink_buffer->append_chunk(state, chunk);
}

MysqlTableSinkOperatorFactory::MysqlTableSinkOperatorFactory(int32_t id, const TMysqlTableSink& t_mysql_table_sink,
                                                             std::vector<TExpr> t_output_expr, int32_t num_sinkers,
                                                             FragmentContext* fragment_ctx)
        : OperatorFactory(id, "mysql_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _t_mysql_table_sink(t_mysql_table_sink),
          _num_sinkers(num_sinkers),
          _fragment_ctx(fragment_ctx) {}

Status MysqlTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    _mysql_table_sink_buffer = std::make_shared<MysqlTableSinkIOBuffer>(_t_mysql_table_sink, _output_expr_ctxs,
                                                                        _num_sinkers, _fragment_ctx);

    return Status::OK();
}

void MysqlTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
