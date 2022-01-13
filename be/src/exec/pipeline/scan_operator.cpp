// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

Status ScanOperator::prepare(RuntimeState* state) {
    SourceOperator::prepare(state);
    DCHECK(_io_threads != nullptr);
    _state = state;
    auto num_scan_operators = 1 + state->exec_env()->increment_num_scan_operators(1);
    if (num_scan_operators > _io_threads->get_queue_capacity()) {
        state->exec_env()->decrement_num_scan_operators(1);
        return Status::TooManyTasks(
                strings::Substitute("num_scan_operators exceeds queue capacity($0) of pipeline_pool_thread",
                                    _io_threads->get_queue_capacity()));
    }

    // init filtered_ouput_columns
    for (const auto& col_name : _olap_scan_node.unused_output_column_name) {
        _unused_output_columns.emplace_back(col_name);
    }

    return Status::OK();
}

Status ScanOperator::close(RuntimeState* state) {
    // decrement global counter num_scan_operators.
    state->exec_env()->decrement_num_scan_operators(1);
    DCHECK(!_is_io_task_active.load(std::memory_order_acquire));
    if (_chunk_source) {
        _chunk_source->close(state);
        _chunk_source = nullptr;
    }
    return Operator::close(state);
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
    }

    // _chunk_source init at pull_chunk()
    // so the the initialization of the first chunk_source needs
    // to be driven by has_output() returning true
    if (!_chunk_source) {
        return true;
    }

    DCHECK(_chunk_source != nullptr);

    // Still have buffered chunks
    if (_chunk_source->has_output()) {
        return true;
    }

    // io task is busy reading chunks, so we just wait
    if (_is_io_task_active.load(std::memory_order_acquire)) {
        return false;
    }

    // Here are two situation
    // 1. Cache is empty out and morsel is not eof, _trigger_next_scan is required
    // 2. Cache is empty and morsel is eof, _pickup_morsel is required
    // Either of which needs to be triggered in pull_chunk, so we return true here
    return true;
}

bool ScanOperator::pending_finish() const {
    DCHECK(_is_finished);
    // If there is no next morsel, and io task is active
    // we just wait for the io thread to end
    return _is_io_task_active.load(std::memory_order_acquire);
}

bool ScanOperator::is_finished() const {
    return _is_finished;
}

void ScanOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    if (!_chunk_source) {
        RETURN_IF_ERROR(_pickup_morsel(state));
        return nullptr;
    }

    DCHECK(_chunk_source != nullptr);
    if (!_chunk_source->has_output()) {
        if (_chunk_source->has_next_chunk()) {
            RETURN_IF_ERROR(_trigger_next_scan(state));
        } else {
            RETURN_IF_ERROR(_pickup_morsel(state));
        }
        return nullptr;
    }

    auto&& chunk = _chunk_source->get_next_chunk_from_buffer();
    // If number of cached chunk is smaller than half of buffer_size,
    // we can start the next scan task ahead of time to obtain better continuity
    if (_chunk_source->get_buffer_size() < (_buffer_size >> 1) && !_is_io_task_active.load(std::memory_order_acquire) &&
        _chunk_source->has_next_chunk()) {
        RETURN_IF_ERROR(_trigger_next_scan(state));
    }

    eval_runtime_bloom_filters(chunk.value().get());

    return chunk;
}

Status ScanOperator::_trigger_next_scan(RuntimeState* state) {
    DCHECK(!_is_io_task_active.load(std::memory_order_acquire));

    PriorityThreadPool::Task task;
    _is_io_task_active.store(true, std::memory_order_release);
    task.work_function = [this, state]() {
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
            _chunk_source->buffer_next_batch_chunks_blocking(_buffer_size, _is_finished);
        }
        _is_io_task_active.store(false, std::memory_order_release);
    };
    // TODO(by satanson): set a proper priority
    task.priority = 20;
    if (_io_threads->try_offer(task)) {
        _io_task_retry_cnt = 0;
    } else {
        _is_io_task_active.store(false, std::memory_order_release);
        // TODO(hcf) set a proper retry times
        LOG(WARNING) << "ScanOperator failed to offer io task due to thread pool overload, retryCnt="
                     << _io_task_retry_cnt;
        if (++_io_task_retry_cnt > 100) {
            return Status::RuntimeError("ScanOperator failed to offer io task due to thread pool overload");
        }
    }
    return Status::OK();
}

Status ScanOperator::_pickup_morsel(RuntimeState* state) {
    DCHECK(_morsel_queue != nullptr);
    DCHECK(!_is_io_task_active.load(std::memory_order_acquire));
    if (_chunk_source) {
        _chunk_source->close(state);
        _chunk_source = nullptr;
    }
    auto maybe_morsel = _morsel_queue->try_get();
    if (!maybe_morsel.has_value()) {
        // release _chunk_source before _curr_morsel, because _chunk_source depends on _curr_morsel.
        _chunk_source = nullptr;
        _is_finished = true;
    } else {
        auto morsel = std::move(maybe_morsel.value());
        DCHECK(morsel);
        bool enable_column_expr_predicate = false;
        if (_olap_scan_node.__isset.enable_column_expr_predicate) {
            enable_column_expr_predicate = _olap_scan_node.enable_column_expr_predicate;
        }
        _chunk_source = std::make_shared<OlapChunkSource>(
                std::move(morsel), _olap_scan_node.tuple_id, _limit, enable_column_expr_predicate, _conjunct_ctxs,
                runtime_in_filters(), runtime_bloom_filters(), _olap_scan_node.key_column_name,
                _olap_scan_node.is_preaggregation, &_unused_output_columns, _runtime_profile.get());
        auto status = _chunk_source->prepare(state);
        if (!status.ok()) {
            _chunk_source = nullptr;
            _is_finished = true;
            return status;
        }
        RETURN_IF_ERROR(_trigger_next_scan(state));
    }
    return Status::OK();
}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, _row_desc));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_olap_scan_node.tuple_id);
    vectorized::DictOptimizeParser::rewrite_descriptor(state, _conjunct_ctxs, _olap_scan_node.dict_string_id_to_int_ids,
                                                       &(tuple_desc->decoded_slots()));
    return Status::OK();
}

void ScanOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
