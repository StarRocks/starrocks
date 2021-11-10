// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
    Operator::prepare(state);
    DCHECK(_io_threads != nullptr);
    _state = state;
    auto num_scan_operators = 1 + state->exec_env()->increment_num_scan_operators(1);
    if (num_scan_operators > _io_threads->get_queue_capacity()) {
        state->exec_env()->decrement_num_scan_operators(1);
        return Status::TooManyTasks(
                strings::Substitute("num_scan_operators exceeds queue capacity($0) of pipeline_pool_thread",
                                    _io_threads->get_queue_capacity()));
    }
    _pickup_morsel(state);
    return Status::OK();
}

Status ScanOperator::close(RuntimeState* state) {
    // decrement global counter num_scan_operators.
    state->exec_env()->decrement_num_scan_operators(1);
    if (!_is_io_task_active.load(std::memory_order_acquire)) {
        if (_chunk_source) {
            _chunk_source->close(state);
            _chunk_source = nullptr;
        }
    }
    Operator::close(state);
    return Status::OK();
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
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

bool ScanOperator::pending_finish() {
    DCHECK(_is_finished);
    // If there is no next morsel, and io task is active
    // we just wait for the io thread to end
    if (_is_io_task_active.load(std::memory_order_acquire)) {
        return true;
    } else {
        if (_chunk_source) {
            _chunk_source->close(_state);
            _chunk_source = nullptr;
        }
        return false;
    }
}

bool ScanOperator::is_finished() const {
    return _is_finished;
}

void ScanOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    DCHECK(_chunk_source != nullptr);
    if (!_chunk_source->has_output()) {
        if (_chunk_source->has_next_chunk()) {
            _trigger_next_scan(state);
        } else {
            _pickup_morsel(state);
        }
        return nullptr;
    }

    auto&& chunk = _chunk_source->get_next_chunk_from_buffer();
    // If buffer size is smaller than half of batch_size,
    // we can start the next scan task ahead of time to obtain better continuity
    if (_chunk_source->get_buffer_size() < (_batch_size >> 1) && !_is_io_task_active.load(std::memory_order_acquire) &&
        _chunk_source->has_next_chunk()) {
        _trigger_next_scan(state);
    }
    return chunk;
}

void ScanOperator::_trigger_next_scan(RuntimeState* state) {
    DCHECK(!_is_io_task_active.load(std::memory_order_acquire));

    PriorityThreadPool::Task task;
    _is_io_task_active.store(true, std::memory_order_release);
    task.work_function = [this, state]() {
        MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(state->instance_mem_tracker());
        DeferOp op([&] {
            tls_thread_status.set_mem_tracker(prev_tracker);
            _is_io_task_active.store(false, std::memory_order_release);
        });
        _chunk_source->buffer_next_batch_chunks_blocking(_batch_size, _is_finished);
    };
    // TODO(by satanson): set a proper priority
    task.priority = 20;
    // try to submit io task, always return true except that _io_threads is shutdown.
    _io_threads->try_offer(task);
}

void ScanOperator::_pickup_morsel(RuntimeState* state) {
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
        _chunk_source = std::make_shared<OlapChunkSource>(
                std::move(morsel), _olap_scan_node.tuple_id, _conjunct_ctxs, _runtime_profile.get(), _runtime_filters,
                _olap_scan_node.key_column_name, _olap_scan_node.is_preaggregation);
        _chunk_source->prepare(state);
        _trigger_next_scan(state);
    }
}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_olap_scan_node.tuple_id);
    vectorized::DictOptimizeParser::rewrite_descriptor(state, tuple_desc->slots(), _conjunct_ctxs,
                                                       _olap_scan_node.dict_string_id_to_int_ids);
    return Status::OK();
}

void ScanOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
}

} // namespace starrocks::pipeline
