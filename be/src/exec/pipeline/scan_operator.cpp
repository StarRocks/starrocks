// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
void ScanOperator::_pickup_morsel(RuntimeState* state) {
    DCHECK(_morsel_queue != nullptr);
    if (_chunk_source) {
        _chunk_source->close(state);
    }
    auto maybe_morsel = _morsel_queue->try_get();
    if (!maybe_morsel.has_value()) {
        // release _chunk_source before _curr_morsel, because _chunk_source depends on _curr_morsel.
        _chunk_source = nullptr;
        _is_finished = true;
    } else {
        auto morsel = std::move(maybe_morsel.value());
        DCHECK(morsel);
        _chunk_source = starrocks::make_exclusive<OlapChunkSource>(
                std::move(morsel), _olap_scan_node.tuple_id, _conjunct_ctxs, _runtime_filters,
                _olap_scan_node.key_column_name, _olap_scan_node.is_preaggregation);
        _chunk_source->prepare(state);
        _trigger_read_chunk();
    }
}
void ScanOperator::_trigger_read_chunk() {
    if (_io_threads == nullptr) {
        return;
    }
    DCHECK(!_pending_chunk.has_value());
    // no io task is pending, so create a pending io task.
    if (!_pending_chunk_source_future.has_value()) {
        DCHECK(!_pending_task.has_value());
        DCHECK(_chunk_source);
        auto chunk_source = _chunk_source;
        auto chunk_source_promise = starrocks::make_exclusive<ChunkSourcePromise>();
        _pending_chunk_source_future = chunk_source_promise->get_future();
        PriorityThreadPool::Task task;

        task.work_function = [chunk_source, chunk_source_promise]() {
            chunk_source->cache_next_chunk_blocking();
            chunk_source_promise->set_value(chunk_source);
        };
        // TODO(by satanson): set a proper priority
        task.priority = 20;
        _pending_task = task;
    }
    // pending task has already been submitted
    if (!_pending_task.has_value()) {
        return;
    }
    // try to submit io task.
    if (_io_threads->try_offer(_pending_task.value())) {
        _pending_task = {};
    }
    // io task is pending
    DCHECK(_pending_chunk_source_future.has_value());
}
Status ScanOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc, get_memtracker()));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    _pickup_morsel(state);
    return Status::OK();
}

Status ScanOperator::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    if (_chunk_source) {
        _chunk_source->close(state);
    }
    Operator::close(state);
    return Status::OK();
}

bool ScanOperator::_has_output_blocking() {
    DCHECK(_io_threads == nullptr);
    return _chunk_source->has_next_chunk();
}

bool ScanOperator::_has_output_nonblocking() {
    DCHECK(_io_threads != nullptr);
    // present chunk is not pulled
    if (_pending_chunk.has_value()) {
        return true;
    }
    // EOS has arrived
    if (_is_finished) {
        return false;
    }
    // no io task pending or a pending io task fails to be submitted in the previous invocation.
    // 1. if has_output method never trigger io tasks, then has_output always return false, so
    // pull_chunk has no chance to be invoked, so _trigger_read_chunk here.
    //
    // 2. in corner cases, pull_chunk method invokes _trigger_read_chunk create pending io task,
    // but fail to submit task, next time, has_output should re-submit this pending io tasks, so
    // here _trigger_read_chunk do so
    if (!_pending_chunk_source_future.has_value() || _pending_task.has_value()) {
        _trigger_read_chunk();
    }
    DCHECK(_pending_chunk_source_future.has_value());
    // fail to submit io task
    if (_pending_task.has_value()) {
        return false;
    }
    // submitted io task has not completed yet
    if (_pending_chunk_source_future.value().wait_for(std::chrono::seconds::zero()) != std::future_status::ready) {
        return false;
    }
    // submitted io task has already completed
    _chunk_source = _pending_chunk_source_future.value().get();
    _pending_chunk_source_future = {};
    _pending_chunk = _chunk_source->get_next_chunk_nonblocking();
    return true;
}

bool ScanOperator::has_output() {
    if (_io_threads != nullptr) {
        return _has_output_nonblocking();
    } else {
        return _has_output_blocking();
    }
}

bool ScanOperator::async_pending() {
    DCHECK(_is_finished);
    if (_io_threads == nullptr) {
        return false;
    }
    // pending io task is not submitted
    if (_pending_task.has_value()) {
        DCHECK(_pending_chunk_source_future.has_value());
        if (_io_threads->try_offer(_pending_task.value())) {
            _pending_task = {};
        } else {
            return true;
        }
    }
    // pending io task has been submitted, but not complete yet.
    if (_pending_chunk_source_future.has_value()) {
        DCHECK(!_pending_task.has_value());
        if (_pending_chunk_source_future.value().wait_for(std::chrono::seconds::zero()) == std::future_status::ready) {
            _chunk_source = _pending_chunk_source_future.value().get();
            _pending_chunk_source_future = {};
            return false;
        } else {
            return true;
        }
    }
    // no pending io task
    DCHECK(!_pending_chunk_source_future.has_value());
    return false;
}

bool ScanOperator::is_finished() const {
    return _is_finished;
}

void ScanOperator::finish(RuntimeState* state) {
    _is_finished = true;
    if (_chunk_source) {
        _chunk_source->close(state);
    }
}

StatusOr<vectorized::ChunkPtr> ScanOperator::_pull_chunk_blocking(RuntimeState* state) {
    DCHECK(_io_threads == nullptr);
    if (_is_finished) {
        return Status::EndOfFile("End-Of-Stream");
    }
    DCHECK(_chunk_source);
    auto chunk = _chunk_source->get_next_chunk();
    if (chunk.ok() || !chunk.status().is_end_of_file()) {
        return chunk;
    }
    _pickup_morsel(state);
    return nullptr;
}

StatusOr<vectorized::ChunkPtr> ScanOperator::_pull_chunk_nonblocking(RuntimeState* state) {
    DCHECK(_io_threads != nullptr);
    if (_is_finished) {
        return Status::EndOfFile("End-Of-Stream");
    }
    DCHECK(_pending_chunk.has_value());
    auto chunk = std::move(_pending_chunk.value());
    _pending_chunk = {};
    if (chunk.ok()) {
        _trigger_read_chunk();
        return chunk;
    }
    if (!chunk.status().is_end_of_file()) {
        return chunk;
    }
    // Now ScanOperator can process multiple morsels, when the non-last morsel is
    // processed and the EndOfFile is encountered, then ScanOperator has no chunk
    // to output and should pick up next morsel. so here return nullptr instead of
    // empty chunk.
    _pickup_morsel(state);
    return nullptr;
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    if (_io_threads == nullptr) {
        return _pull_chunk_blocking(state);
    } else {
        return _pull_chunk_nonblocking(state);
    }
}
} // namespace starrocks::pipeline
