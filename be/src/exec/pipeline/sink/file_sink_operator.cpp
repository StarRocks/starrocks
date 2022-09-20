// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/sink/file_sink_operator.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include "bthread/execution_queue.h"
DIAGNOSTIC_POP
#include "column/chunk.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/query_statistics.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/spinlock.h"

namespace starrocks::pipeline {

// FileSinkBuffer accepts input from all FileSinkOperators, it uses an execution queue to asynchronously write chunks to file one by one.
class FileSinkBuffer {
public:
    FileSinkBuffer(std::vector<ExprContext*>& output_expr_ctxs, std::shared_ptr<ResultFileOptions> file_opts,
                   int32_t num_sinkers, FragmentContext* const fragment_ctx)
            : _output_expr_ctxs(output_expr_ctxs),
              _file_opts(file_opts),
              _num_result_sinkers(num_sinkers),
              _fragment_ctx(fragment_ctx) {}

    ~FileSinkBuffer() {}

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile);

    Status append_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);

    bool need_input();

    Status set_finishing();

    bool is_finished();

    void cancel_one_sinker();

    void close(RuntimeState* state);

    inline void set_io_status(const Status& status) {
        std::lock_guard<SpinLock> l(_io_status_mutex);
        if (_io_status.ok()) {
            _io_status = status;
        }
    }

    inline Status get_io_status() const {
        std::lock_guard<SpinLock> l(_io_status_mutex);
        return _io_status;
    }

    static int execute_io_task(void* meta, bthread::TaskIterator<const vectorized::ChunkPtr>& iter);

private:
    void process_chunk(bthread::TaskIterator<const vectorized::ChunkPtr>& iter);

    std::vector<ExprContext*> _output_expr_ctxs;

    std::shared_ptr<ResultFileOptions> _file_opts;
    std::shared_ptr<FileResultWriter> _writer;

    std::shared_ptr<BufferControlBlock> _sender;

    std::unique_ptr<bthread::ExecutionQueueId<const vectorized::ChunkPtr>> _exec_queue_id;

    std::atomic_int32_t _num_result_sinkers = 0;
    std::atomic_int64_t _num_pending_chunks = 0;

    bool _is_writer_opened = false;
    std::atomic_bool _is_prepared = false;
    std::atomic_bool _is_cancelled = false;
    std::atomic_bool _is_finished = false;

    mutable SpinLock _io_status_mutex;
    Status _io_status;

    FragmentContext* const _fragment_ctx;
    RuntimeState* _state = nullptr;

    static const int32_t kExecutionQueueSizeLimit = 64;
};

int FileSinkBuffer::execute_io_task(void* meta, bthread::TaskIterator<const vectorized::ChunkPtr>& iter) {
    FileSinkBuffer* file_sink_buffer = static_cast<FileSinkBuffer*>(meta);
    for (; iter; ++iter) {
        file_sink_buffer->process_chunk(iter);
    }
    return 0;
}

void FileSinkBuffer::process_chunk(bthread::TaskIterator<const vectorized::ChunkPtr>& iter) {
    --_num_pending_chunks;
    // close is already done, just skip
    if (_is_finished) {
        return;
    }

    // cancelling has happened but close is not invoked
    if (_is_cancelled && !_is_finished) {
        close(_state);
        return;
    }

    if (!_is_writer_opened) {
        if (Status status = _writer->open(_state); !status.ok()) {
            set_io_status(status);
            close(_state);
            return;
        }
        _is_writer_opened = true;
    }
    auto chunk = *iter;
    if (chunk == nullptr) {
        // this is the last chunk
        close(_state);
        return;
    }
    if (Status status = _writer->append_chunk(chunk.get()); !status.ok()) {
        set_io_status(status);
        close(_state);
    }
}

Status FileSinkBuffer::prepare(RuntimeState* state, RuntimeProfile* parent_profile) {
    bool expected = false;
    if (!_is_prepared.compare_exchange_strong(expected, true)) {
        return Status::OK();
    }

    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(), 1024, &_sender));

    _state = state;
    _writer = std::make_shared<FileResultWriter>(_file_opts.get(), _output_expr_ctxs, parent_profile);
    RETURN_IF_ERROR(_writer->init(state));

    bthread::ExecutionQueueOptions options;
    _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<const vectorized::ChunkPtr>>();
    int ret = bthread::execution_queue_start<const vectorized::ChunkPtr>(_exec_queue_id.get(), &options,
                                                                         &FileSinkBuffer::execute_io_task, this);
    if (ret != 0) {
        return Status::InternalError("start execution queue error");
    }
    return Status::OK();
}

Status FileSinkBuffer::append_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    Status status = get_io_status();
    if (!status.ok()) {
        return status;
    }
    if (bthread::execution_queue_execute(*_exec_queue_id, chunk) != 0) {
        return Status::InternalError("submit io task failed");
    }
    ++_num_pending_chunks;
    return Status::OK();
}

bool FileSinkBuffer::need_input() {
    return _num_pending_chunks < kExecutionQueueSizeLimit;
}

Status FileSinkBuffer::set_finishing() {
    if (--_num_result_sinkers == 0) {
        if (bthread::execution_queue_execute(*_exec_queue_id, nullptr) != 0) {
            return Status::InternalError("submit task failed");
        }
        ++_num_pending_chunks;
    }
    return Status::OK();
}

bool FileSinkBuffer::is_finished() {
    return _is_finished;
}

void FileSinkBuffer::cancel_one_sinker() {
    _is_cancelled = true;
    bthread::execution_queue_stop(*_exec_queue_id);
}

void FileSinkBuffer::close(RuntimeState* state) {
    if (_writer != nullptr) {
        if (Status status = _writer->close(); !status.ok()) {
            set_io_status(status);
        }
        _writer.reset();
    }

    if (_sender != nullptr) {
        auto query_statistic = std::make_shared<QueryStatistics>();
        QueryContext* query_ctx = state->query_ctx();
        query_statistic->add_scan_stats(query_ctx->cur_scan_rows_num(), query_ctx->get_scan_bytes());
        query_statistic->add_cpu_costs(query_ctx->cpu_cost());
        query_statistic->add_mem_costs(query_ctx->mem_cost_bytes());
        _sender->set_query_statistics(query_statistic);
        Status final_status = _fragment_ctx->final_status();
        Status io_status = get_io_status();
        if (!io_status.ok() && final_status.ok()) {
            final_status = io_status;
        }
        _sender->close(final_status);
        _sender.reset();

        _state->exec_env()->result_mgr()->cancel_at_time(time(nullptr) + config::result_buffer_cancelled_interval_time,
                                                         state->fragment_instance_id());
    }
    _is_finished = true;
    bthread::execution_queue_stop(*_exec_queue_id);
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

StatusOr<vectorized::ChunkPtr> FileSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from file sink operator");
}

Status FileSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _file_sink_buffer->append_chunk(state, chunk);
}

FileSinkOperatorFactory::FileSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr,
                                                 std::shared_ptr<ResultFileOptions> file_opts, int32_t _num_sinkers,
                                                 FragmentContext* const fragment_ctx)
        : OperatorFactory(id, "file_sink", Operator::s_pseudo_plan_node_id_for_result_sink),
          _t_output_expr(std::move(t_output_expr)),
          _file_opts(file_opts),
          _num_sinkers(_num_sinkers),
          _fragment_ctx(fragment_ctx) {}

Status FileSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    _file_sink_buffer = std::make_shared<FileSinkBuffer>(_output_expr_ctxs, _file_opts, _num_sinkers, _fragment_ctx);
    return Status::OK();
}

void FileSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline