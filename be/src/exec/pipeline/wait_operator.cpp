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

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/schedule/observer.h"
#include "exec/pipeline/schedule/timeout_tasks.h"
#include "util/stopwatch.hpp"

namespace starrocks::pipeline {
Status WaitSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _mono_timer = state->obj_pool()->add(new MonotonicStopWatch());
    _mono_timer->start();
    _wait_context->observable->attach_source_observer(state, observer());
    if (state->enable_event_scheduler()) {
        auto fragment_ctx = state->fragment_ctx();
        auto timer = std::make_unique<RFScanWaitTimeout>(fragment_ctx);
        timer->add_observer(state, observer());
        _wait_timer_task = std::move(timer);
        timespec abstime = butil::microseconds_to_timespec(butil::gettimeofday_us());
        abstime.tv_nsec += _wait_time_ns;
        butil::timespec_normalize(&abstime);
        RETURN_IF_ERROR(fragment_ctx->pipeline_timer()->schedule(_wait_timer_task.get(), abstime));
    }
    return Status::OK();
}

WaitSourceOperator::~WaitSourceOperator() {
    close(runtime_state());
}

void WaitSourceOperator::close(RuntimeState* state) {
    if (_wait_timer_task != nullptr) {
        state->fragment_ctx()->pipeline_timer()->unschedule(_wait_timer_task.get());
        _wait_timer_task = nullptr;
    }
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

StatusOr<ChunkPtr> WaitSourceOperator::pull_chunk(RuntimeState* state) {
    auto defer = _wait_context->observable->defer_notify_sink();
    return _wait_context->chunk_buffer->pull();
}

Status WaitSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _metrics = std::make_unique<BufferMetrics>(_unique_metrics.get());
    _wait_context->chunk_buffer = std::make_unique<LimitedPipelineChunkBuffer<BufferMetrics>>(
            _metrics.get(), 1, config::local_exchange_buffer_mem_limit_per_driver, state->chunk_size() * 16);
    _wait_context->observable = std::make_unique<PipeObservable>();
    _wait_context->observable->attach_sink_observer(state, observer());
    return Status::OK();
}

bool WaitSinkOperator::need_input() const {
    return !_wait_context->chunk_buffer->is_full();
}

bool WaitSinkOperator::is_finished() const {
    return _wait_context->is_finished || (_wait_context->is_finishing && _wait_context->chunk_buffer->is_empty());
}

Status WaitSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto defer = _wait_context->observable->defer_notify_source();
    _wait_context->chunk_buffer->push(chunk);
    return Status::OK();
}

Status WaitSinkOperator::set_finishing(RuntimeState* state) {
    auto defer = _wait_context->observable->defer_notify_source();
    _wait_context->is_finishing = true;
    return Status::OK();
}

Status WaitSinkOperator::set_finished(RuntimeState* state) {
    auto defer = _wait_context->observable->defer_notify_source();
    _wait_context->chunk_buffer->clear();
    _wait_context->is_finished = true;
    return Status::OK();
}

} // namespace starrocks::pipeline
