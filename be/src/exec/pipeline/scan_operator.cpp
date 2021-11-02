// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status ScanOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    DCHECK(_io_threads != nullptr);
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
    state->exec_env()->decrement_num_scan_operators(1);
    if (_chunk_source) {
        _chunk_source->close(state);
    }
    Operator::close(state);
    return Status::OK();
}

bool ScanOperator::has_output() const {
    if (_is_finished) {
        return false;
    }

    if (_chunk_source->has_output()) {
        return true;
    }

    if (_chunk_source->has_next_chunk()) {
        return false;
    }

    // Current morsel is eof, so we need to try to get another morsel in pull_chunk
    return true;
}

bool ScanOperator::pending_finish() {
    // TODO(hcf) remove pending_finish next pull request
    DCHECK(_is_finished);
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

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    if (_is_finished) {
        return Status::EndOfFile("End-Of-Stream");
    }

    // Current morsel is eof, try to pickup another morsel
    if (!_chunk_source->has_output() && !_chunk_source->has_next_chunk()) {
        _pickup_morsel(state);
        return nullptr;
    }

    return _chunk_source->get_next_chunk();
}

void ScanOperator::_start_scan() {
    PriorityThreadPool::Task task;

    auto chunk_source = _chunk_source;
    task.work_function = [chunk_source]() {
        while (true) {
            auto status = chunk_source->cache_next_chunk_blocking();
            if (!status.ok()) {
                break;
            }
        }
    };
    // TODO(by satanson): set a proper priority
    task.priority = 20;
    // try to submit io task, always return true except that _io_threads is shutdown.
    _io_threads->try_offer(task);
}

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
        _chunk_source = std::make_shared<OlapChunkSource>(
                std::move(morsel), _olap_scan_node.tuple_id, _conjunct_ctxs, _runtime_profile.get(), _runtime_filters,
                _olap_scan_node.key_column_name, _olap_scan_node.is_preaggregation);
        _chunk_source->prepare(state);
        _start_scan();
    }
}

Status ScanOperatorFactory::prepare(RuntimeState* state) {
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void ScanOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
}

} // namespace starrocks::pipeline
