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

#include "exec/pipeline/sink/olap_table_sink_operator.h"

#include "base/time/time.h"
#include "common/thread/priority_thread_pool.hpp"
#include "compute_env/result/buffer_control_block.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/work_group.h"
#include "data_sink/tablet/olap_table_sink.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "exec_primitive/pipeline/primitives/driver_executor.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {
Status OlapTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    state->set_per_fragment_instance_idx(_sender_id);

    _automatic_partition_chunk.reset();

    _close_queue_timer = ADD_TIMER(_unique_metrics, "CloseQueueTime");
    _sink->set_profile(_unique_metrics.get());
    RETURN_IF_ERROR(_sink->prepare(state));

    RETURN_IF_ERROR(_sink->try_open(state));

    return Status::OK();
}

void OlapTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

StatusOr<ChunkPtr> OlapTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::NotSupported("Shouldn't pull chunk from olap table sink operator");
}

bool OlapTableSinkOperator::is_finished() const {
    return _is_finished;
}

bool OlapTableSinkOperator::pending_finish() const {
    if (_close_submitted) {
        return _pending_close();
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
            cancel_fragment_context(_fragment_ctx, st);
            return false;
        }
    }

    // last chunk trigger automatic partition creation
    // we need handle it before close sink
    if (!_is_cancelled && _automatic_partition_chunk) {
        if (_sink->is_full()) {
            return true;
        }
        auto st = _sink->send_chunk_nonblocking(_fragment_ctx->runtime_state(), _automatic_partition_chunk.get());
        _automatic_partition_chunk.reset();
        if (!st.ok()) {
            cancel_fragment_context(_fragment_ctx, st);
            return false;
        }
    }

    if (!_sink->is_close_done()) {
        auto st = _sink->try_close(_fragment_ctx->runtime_state());
        if (!st.ok()) {
            cancel_fragment_context(_fragment_ctx, st);
            return false;
        }
        return true;
    }

    // Every channel has processed its last response, so close() no longer waits on RPCs, but it still writes the
    // combined txn log to object storage, which can take seconds. pending_finish() runs on the pipeline poller, a
    // single thread that checks every blocked driver of the executor set, so run the close on the sink IO pool and
    // only check its result here.
    _submit_close();
    return _pending_close();
}

void OlapTableSinkOperator::_submit_close() const {
    _close_submitted = true;
    auto* state = _fragment_ctx->runtime_state();
    const int64_t submit_time_ns = MonotonicNanos();
    auto close_task = [this, state, submit_time_ns]() {
        COUNTER_UPDATE(_close_queue_timer, MonotonicNanos() - submit_time_ns);
        Status st;
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
            st = _sink->close(state, Status::OK());
        }
        _close_status = std::move(st);
        // The last access to this operator: once _close_done is set, the driver can finish and destroy it.
        _close_done.store(true, std::memory_order_release);
    };

    auto* pool = state->query_execution_services() != nullptr
                         ? state->query_execution_services()->execution->pipeline_sink_io_pool
                         : nullptr;
    if (pool != nullptr && pool->try_offer([close_task, query_id = state->query_id(),
                                            fragment_instance_id = state->fragment_instance_id()]() {
            SCOPED_SET_TRACE_INFO(0, query_id, fragment_instance_id);
            close_task();
        })) {
        return;
    }
    LOG_IF(WARNING, pool != nullptr) << "Sink IO pool is full, close the olap table sink inline, fragment_instance_id="
                                     << print_id(state->fragment_instance_id());
    close_task();
}

bool OlapTableSinkOperator::_pending_close() const {
    if (!_close_done.load(std::memory_order_acquire)) {
        return true;
    }
    if (!_close_status.ok()) {
        cancel_fragment_context(_fragment_ctx, _close_status);
    }
    return false;
}

Status OlapTableSinkOperator::set_cancelled(RuntimeState* state) {
    _is_cancelled = true;
    if (_close_submitted) {
        // The close task owns the sink until it finishes, and pending_finish() keeps the driver alive until then.
        // All channels were already closed when it was submitted, so there is nothing left to cancel.
        return Status::OK();
    }
    auto final_status = state->fragment_ctx()->final_status();
    std::string reason = "Cancelled by pipeline engine, reason: " + final_status.to_string();
    return _sink->close(state, Status::Cancelled(reason));
}

Status OlapTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        _fragment_ctx->workgroup()->executors()->driver_executor()->report_audit_statistics(state->query_ctx(),
                                                                                            state->fragment_ctx());
    }
    if (_is_open_done && !_automatic_partition_chunk) {
        // sink's open already finish, we can try_close
        return _sink->try_close(state);
    } else {
        // sink's open not finish or automatic partition in processing, we need check in pending_finish() before close
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

Status OlapTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (!_is_open_done) {
        _is_open_done = true;
        // we can be here cause _sink->is_open_done() return true
        // so that open_wait() will not block
        RETURN_IF_ERROR(_sink->open_wait());
    }

    // previous push_chunk() trigger automatic partition creation
    if (_automatic_partition_chunk) {
        // resend previous chunk before send new chunk
        auto st = _sink->send_chunk_nonblocking(state, _automatic_partition_chunk.get());
        _automatic_partition_chunk.reset();
        if (!st.ok()) {
            return st;
        }
    }

    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    // send_chunk_nonblocking() will return EAGAIN to avoid block
    auto st = _sink->send_chunk_nonblocking(state, chunk.get());
    if (st.is_eagain()) {
        // temporarily save the chunk, wait for the partition to be created and send again
        _automatic_partition_chunk = chunk;
        return Status::OK();
    } else {
        return st;
    }
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
