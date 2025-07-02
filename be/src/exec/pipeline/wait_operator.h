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

#pragma once

#include <memory>
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/limited_pipeline_chunk_buffer.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/schedule/observer.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {
struct BufferMetrics {
    BufferMetrics(RuntimeProfile* runtime_profile) {
        chunk_buffer_peak_memory = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakMem", TUnit::BYTES);
        chunk_buffer_peak_size = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakSize", TUnit::UNIT);
    }

    RuntimeProfile::HighWaterMarkCounter* chunk_buffer_peak_memory{};
    RuntimeProfile::HighWaterMarkCounter* chunk_buffer_peak_size{};
};

struct WaitContext {
    bool is_finished = false;
    bool is_finishing = false;
    std::unique_ptr<LimitedPipelineChunkBuffer<BufferMetrics>> chunk_buffer;
    std::unique_ptr<PipeObservable> observable;
};

class WaitSourceOperator final : public SourceOperator {
public:
    WaitSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       WaitContext* wait_context, int32_t wait_times_ms)
            : SourceOperator(factory, id, "wait_source", plan_node_id, true, driver_sequence),
              _wait_context(wait_context),
              _wait_time_ns(wait_times_ms * 1000L * 1000L) {}

    ~WaitSourceOperator() override;

    Status prepare(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override {
        return _wait_context->is_finished || (_wait_context->is_finishing && _wait_context->chunk_buffer->is_empty());
    }

    void close(RuntimeState* state) override;

    bool ignore_empty_eos() const override { return false; }

    Status set_finished(RuntimeState* state) override {
        _wait_context->is_finished = true;
        return Status::OK();
    }

private:
    mutable bool _reached_timeout = false;
    MonotonicStopWatch* _mono_timer = nullptr;
    WaitContext* _wait_context = nullptr;
    int64_t _wait_time_ns = 0;
    std::unique_ptr<PipelineTimerTask> _wait_timer_task;
};

class WaitSinkOperator final : public Operator {
public:
    WaitSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                     WaitContext* wait_context)
            : Operator(factory, id, "wait_sink", plan_node_id, true, driver_sequence), _wait_context(wait_context) {}

    ~WaitSinkOperator() override = default;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status prepare(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::NotSupported("unsupported push chunk in wait_source_operator");
    }

    bool has_output() const override { return false; }
    bool need_input() const override;
    bool is_finished() const override;

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

private:
    std::unique_ptr<BufferMetrics> _metrics;
    WaitContext* _wait_context = nullptr;
};

class WaitContextFactory {
public:
    size_t wait_times_ms() const { return _wait_time_ms; }
    void resize(int dop) { _buffer.resize(dop); }
    WaitContext* get(int driver_sequence) { return &_buffer[driver_sequence]; }

    WaitContextFactory(size_t wait_time_ms) : _wait_time_ms(wait_time_ms) {}

private:
    size_t _wait_time_ms = 0;
    std::vector<WaitContext> _buffer;
};

using WaitContextFactoryPtr = std::shared_ptr<WaitContextFactory>;

class WaitOperatorSourceFactory final : public SourceOperatorFactory {
public:
    WaitOperatorSourceFactory(int32_t id, int32_t plan_node_id, WaitContextFactoryPtr single_chunk_buffer)
            : SourceOperatorFactory(id, "wait_source", plan_node_id), _buffer_factory(std::move(single_chunk_buffer)) {}

    ~WaitOperatorSourceFactory() override = default;
    bool support_event_scheduler() const override { return true; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        _buffer_factory->resize(degree_of_parallelism);
        auto single_chunk_buffer = _buffer_factory->get(driver_sequence);
        return std::make_shared<WaitSourceOperator>(this, _id, _plan_node_id, driver_sequence, single_chunk_buffer,
                                                    _buffer_factory->wait_times_ms());
    }

private:
    WaitContextFactoryPtr _buffer_factory;
};

class WaitOperatorSinkFactory final : public OperatorFactory {
public:
    WaitOperatorSinkFactory(int32_t id, int32_t plan_node_id, WaitContextFactoryPtr wait_context_factory)
            : OperatorFactory(id, "wait_sink", plan_node_id), _wait_context_factory(std::move(wait_context_factory)) {}

    ~WaitOperatorSinkFactory() override = default;
    bool support_event_scheduler() const override { return true; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        _wait_context_factory->resize(degree_of_parallelism);
        auto wait_context = _wait_context_factory->get(driver_sequence);
        return std::make_shared<WaitSinkOperator>(this, _id, _plan_node_id, driver_sequence, wait_context);
    }

private:
    std::shared_ptr<WaitContextFactory> _wait_context_factory;
};

} // namespace pipeline
} // namespace starrocks
