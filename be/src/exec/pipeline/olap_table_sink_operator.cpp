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

#include "exec/pipeline/olap_table_sink_operator.h"

#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/query_statistics.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status OlapTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    state->set_per_fragment_instance_idx(_sender_id);

    _sink->set_nonblocking_send_chunk(true);
    _automatic_partition_chunk.reset();

    RETURN_IF_ERROR(_sink->prepare(state));

    RETURN_IF_ERROR(_sink->try_open(state));

    return Status::OK();
}

void OlapTableSinkOperator::close(RuntimeState* state) {
    _unique_metrics->copy_all_info_strings_from(_sink->profile());
    _unique_metrics->copy_all_counters_from(_sink->profile());

    Operator::close(state);
}

StatusOr<ChunkPtr> OlapTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::NotSupported("Shouldn't pull chunk from olap table sink operator");
}

bool OlapTableSinkOperator::is_finished() const {
    return _is_finished;
}

bool OlapTableSinkOperator::pending_finish() const {
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

    // last chunk trigger automatic partition creation
    // we need handle it before close sink
    if (!_is_cancelled && _automatic_partition_chunk) {
        if (_sink->is_full()) {
            return true;
        }
        auto st = _sink->send_chunk(_fragment_ctx->runtime_state(), _automatic_partition_chunk.get());
        _automatic_partition_chunk.reset();
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

bool OlapTableSinkOperator::is_epoch_finishing() const {
    return pending_finish();
}

Status OlapTableSinkOperator::set_epoch_finishing(RuntimeState* state) {
    _is_epoch_finished = true;
    if (_is_open_done && !_automatic_partition_chunk) {
        // sink's open already finish, we can try_close
        return _sink->try_close(state);
    } else {
        // sink's open not finish, we need check in pending_finish() before close
        return Status::OK();
    }
}

Status OlapTableSinkOperator::reset_epoch(RuntimeState* state) {
    if (!_sink->is_close_done()) {
        RETURN_IF_ERROR(_sink->close(state, Status::OK()));
    }

    _is_epoch_finished = false;

    RETURN_IF_ERROR(_sink->reset_epoch(state));

    RETURN_IF_ERROR(_sink->prepare(state));

    RETURN_IF_ERROR(_sink->try_open(state));

    return Status::OK();
}

Status OlapTableSinkOperator::set_cancelled(RuntimeState* state) {
    _is_cancelled = true;
    return _sink->close(state, Status::Cancelled("Cancelled by pipeline engine"));
}

Status OlapTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

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
        auto st = _sink->send_chunk(state, _automatic_partition_chunk.get());
        _automatic_partition_chunk.reset();
        if (!st.ok()) {
            return st;
        }
    }

    uint16_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    // send_chunk() will return EAGAIN to avoid block
    auto st = _sink->send_chunk(state, chunk.get());
    if (st.is_eagain()) {
        // temporarily save the chunk, wait for the partition to be created and send again
        _automatic_partition_chunk = chunk;
        return Status::OK();
    } else {
        return st;
    }
}

OperatorPtr OlapTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    if (driver_sequence == 0) {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sink0, _fragment_ctx);
    } else {
        return std::make_shared<OlapTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _cur_sender_id++,
                                                       _sinks[driver_sequence - 1].get(), _fragment_ctx);
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
