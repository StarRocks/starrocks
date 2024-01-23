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

#include "exec/pipeline/sink/file_sink_operator.h"

#include <utility>

#include "column/chunk.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/query_statistics.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "udf/java/utils.h"

namespace starrocks::pipeline {

class FileSinkIOBuffer final : public SinkIOBuffer {
public:
    FileSinkIOBuffer(std::vector<ExprContext*>& output_expr_ctxs, std::shared_ptr<ResultFileOptions> file_opts,
                     int32_t num_sinkers, FragmentContext* const fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _output_expr_ctxs(output_expr_ctxs),
              _file_opts(std::move(file_opts)),
              _fragment_ctx(fragment_ctx) {}

    ~FileSinkIOBuffer() override = default;

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override;

    void close(RuntimeState* state) override;

private:
    void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override;

    std::vector<ExprContext*> _output_expr_ctxs;

    std::shared_ptr<ResultFileOptions> _file_opts;
    std::shared_ptr<FileResultWriter> _writer;
    std::shared_ptr<BufferControlBlock> _sender;

    bool _is_writer_opened = false;

    FragmentContext* const _fragment_ctx;
};

Status FileSinkIOBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }

    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(), 1024, &_sender));

    _state = state;
    _writer = std::make_shared<FileResultWriter>(_file_opts.get(), _output_expr_ctxs, parent_profile);
    RETURN_IF_ERROR(_writer->init(state));

    bthread::ExecutionQueueOptions options;
    options.executor = SinkIOExecutor::instance();
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
    int ret = bthread::execution_queue_start<ChunkPtr>(_exec_queue_id.get(), &options,
                                                       &FileSinkIOBuffer::execute_io_task, this);
    if (ret != 0) {
        _exec_queue_id.reset();
        return Status::InternalError("start execution queue error");
    }
    return Status::OK();
}

void FileSinkIOBuffer::close(RuntimeState* state) {
    int64_t num_written_rows = 0;
    if (_writer != nullptr) {
        if (Status status = _writer->close(); !status.ok()) {
            set_io_status(status);
        }
        num_written_rows = _writer->get_written_rows();
        _writer.reset();
    }

    if (_sender != nullptr) {
        auto query_statistic = std::make_shared<QueryStatistics>();
        QueryContext* query_ctx = state->query_ctx();
        query_statistic->add_scan_stats(query_ctx->cur_scan_rows_num(), query_ctx->get_scan_bytes());
        query_statistic->add_cpu_costs(query_ctx->cpu_cost());
        query_statistic->add_mem_costs(query_ctx->mem_cost_bytes());
        query_statistic->set_returned_rows(num_written_rows);
        _sender->set_query_statistics(query_statistic);
        Status final_status = _fragment_ctx->final_status();
        Status io_status = get_io_status();
        if (!io_status.ok() && final_status.ok()) {
            final_status = io_status;
        }
        _sender->close(final_status);
        _sender.reset();

        auto st = _state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time, state->fragment_instance_id());
        st.permit_unchecked_error();
    }
    SinkIOBuffer::close(state);
}

void FileSinkIOBuffer::_process_chunk(bthread::TaskIterator<ChunkPtr>& iter) {
    DeferOp op([&]() {
        auto nc = _num_pending_chunks.fetch_sub(1);
        DCHECK_GE(nc, 1L);
    });

    // close is already done, just skip
    if (_is_finished) {
        return;
    }

    // cancelling has happened but close is not invoked
    if (_is_cancelled && !_is_finished) {
        if (_num_pending_chunks == 1) {
            close(_state);
        }
        return;
    }

    if (!_is_writer_opened) {
        if (Status status = _writer->open(_state); !status.ok()) {
            status = status.clone_and_prepend("open file writer failed, error");
            LOG(WARNING) << status;
            _fragment_ctx->cancel(status);
            close(_state);
            return;
        }
        _is_writer_opened = true;
    }

    const auto& chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        auto nc = _num_pending_chunks.load();
        DCHECK_EQ(nc, 1L);
        close(_state);
        return;
    }

    if (Status status = _writer->append_chunk(chunk.get()); !status.ok()) {
        status = status.clone_and_prepend("add chunk to file writer failed, error");
        LOG(WARNING) << status;
        _fragment_ctx->cancel(status);
        close(_state);
        return;
    }
}

Status FileSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _file_sink_buffer->prepare(state, _unique_metrics.get());
}

void FileSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool FileSinkOperator::pending_finish() const {
    return !_file_sink_buffer->is_finished();
}

bool FileSinkOperator::is_finished() const {
    return _file_sink_buffer->is_finished();
}

bool FileSinkOperator::need_input() const {
    return _file_sink_buffer->need_input();
}

Status FileSinkOperator::set_finishing(RuntimeState* state) {
    return _file_sink_buffer->set_finishing();
}

Status FileSinkOperator::set_cancelled(RuntimeState* state) {
    _file_sink_buffer->cancel_one_sinker();
    return Status::OK();
}

StatusOr<ChunkPtr> FileSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from file sink operator");
}

Status FileSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _file_sink_buffer->append_chunk(state, chunk);
}

FileSinkOperatorFactory::FileSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr,
                                                 std::shared_ptr<ResultFileOptions> file_opts, int32_t _num_sinkers,
                                                 FragmentContext* const fragment_ctx)
        : OperatorFactory(id, "file_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _file_opts(std::move(file_opts)),
          _num_sinkers(_num_sinkers),
          _fragment_ctx(fragment_ctx) {}

Status FileSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    _file_sink_buffer = std::make_shared<FileSinkIOBuffer>(_output_expr_ctxs, _file_opts, _num_sinkers, _fragment_ctx);
    return Status::OK();
}

void FileSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
