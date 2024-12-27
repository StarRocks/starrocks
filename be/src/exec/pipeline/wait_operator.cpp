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

#include "exec/pipeline/wait_operator.h"

#include <memory>

#include "util/stopwatch.hpp"

namespace starrocks::pipeline {
Status WaitSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _mono_timer = state->obj_pool()->add(new MonotonicStopWatch());
    _mono_timer->start();
    return Status::OK();
}
bool WaitSourceOperator::has_output() const {
    if (!_reached_timeout) {
        if (_mono_timer->elapsed_time() > _wait_time_ns) {
            _reached_timeout = true;
        } else {
            return false;
        }
    }
    return !_wait_context->is_finished && !_wait_context->chunk_buffer->is_empty();
}

Status WaitSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _metrics = std::make_unique<BufferMetrics>(_unique_metrics.get());
    _wait_context->chunk_buffer = std::make_unique<LimitedPipelineChunkBuffer<BufferMetrics>>(
            _metrics.get(), 1, config::local_exchange_buffer_mem_limit_per_driver, state->chunk_size() * 16);
    return Status::OK();
}

bool WaitSinkOperator::need_input() const {
    return !_wait_context->chunk_buffer->is_full();
}

bool WaitSinkOperator::is_finished() const {
    return _wait_context->is_finished || (_wait_context->is_finishing && _wait_context->chunk_buffer->is_empty());
}

Status WaitSinkOperator::set_finishing(RuntimeState* state) {
    _wait_context->is_finishing = true;
    return Status::OK();
}

Status WaitSinkOperator::set_finished(RuntimeState* state) {
    _wait_context->chunk_buffer->clear();
    _wait_context->is_finished = true;
    return Status::OK();
}

} // namespace starrocks::pipeline
