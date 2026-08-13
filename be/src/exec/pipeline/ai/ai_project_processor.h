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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "base/statusor.h"
#include "column/chunk.h"
#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exprs/ai/ai_function_call_expr.h"
#include "platform/llm/ai_runtime.h"
#include "platform/llm/ai_task_dispatcher.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

struct AIProjectPreparedOutput {
    SlotId slot_id = 0;
    AIFunctionInputBatch input;
    // Production projections insert an empty nullable placeholder in the
    // projected column order. Test projections may leave this false and let
    // materialization append the AI column.
    bool replace_existing = false;
};

struct AIProjectPreparedSubchunk {
    ChunkPtr output_chunk;
    std::vector<AIProjectPreparedOutput> ai_outputs;
};

// Expression work stays on the source driver. Implementations evaluate common
// expressions, retain identity columns, and build stable per-row AI inputs.
class AIProjectProjection {
public:
    virtual ~AIProjectProjection() = default;

    // Factory-owned lifecycle. Expression-backed implementations use one
    // ExprContext clone per source driver. Defaults preserve lightweight test
    // projections that have no runtime resources.
    virtual Status prepare(RuntimeState*, int32_t) { return Status::OK(); }
    virtual void close(RuntimeState*) {}

    virtual StatusOr<AIProjectPreparedSubchunk> prepare_subchunk(RuntimeState* state, int32_t driver_sequence,
                                                                 const ChunkPtr& input) = 0;
};

struct AIProjectTaskRequest {
    uint64_t task_id = 0;
    // Borrowed from the active subchunk. submit() must synchronously copy or
    // consume these views before it returns.
    std::string_view model;
    std::string_view prompt;
    const AIProviderOptions* options = nullptr;
};

class AIProjectTaskHandle {
public:
    virtual ~AIProjectTaskHandle() = default;
    // Cancellation may synchronously publish the terminal callback. It must be
    // thread-safe, idempotent, and must not throw.
    virtual void cancel() noexcept = 0;
};

// The production adapter owns the provider/dispatcher configuration. This
// narrow seam also makes inline callback and cancellation races testable.
class AIProjectTaskSubmitter {
public:
    virtual ~AIProjectTaskSubmitter() = default;

    // The returned intrusive context is an allocation-free copy. Production
    // submitters use one cached context for the callback and all request-owned
    // async state; lightweight test submitters may leave it empty.
    virtual AIMemoryContext memory_context() const noexcept { return {}; }

    // An accepted task invokes callback exactly once, including after
    // cancellation or shutdown, and may do so before submit() returns.
    // Implementations must return a non-null handle for accepted work and must
    // consume the callback before returning, including on an error. Any retained
    // callback target must use memory_context() as its physical allocation scope.
    // Implementations must not throw; the processor contains exceptions
    // defensively to preserve teardown.
    virtual StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                                  AITaskCallback&& callback) = 0;
};

// Fragment-shared state connecting AISink and AISource operators.
//
// At most one subchunk is active in each lane. A callback only publishes one
// terminal row result and wakes the source; expression and column work remains
// on the source driver.
class AIProjectProcessor final {
public:
    static StatusOr<std::shared_ptr<AIProjectProcessor>> create(std::shared_ptr<AIChunkBuffer> input_buffer,
                                                                std::shared_ptr<AIProjectProjection> projection,
                                                                std::shared_ptr<AIProjectTaskSubmitter> submitter,
                                                                AIRuntimeConfig config);

    AIProjectProcessor(const AIProjectProcessor&) = delete;
    AIProjectProcessor& operator=(const AIProjectProcessor&) = delete;

    Status configure(int32_t dop);
    Status configuration_status() const;
    Status prepare(RuntimeState* state, int32_t dop);
    void close(RuntimeState* state);

    Status try_process(RuntimeState* state, int32_t driver_sequence);
    bool has_output(int32_t driver_sequence) const;
    bool can_process(int32_t driver_sequence) const;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t driver_sequence);

    Status status(int32_t driver_sequence) const;
    Status set_status(int32_t driver_sequence, const Status& status);
    StatusOr<bool> lane_finished(int32_t driver_sequence) const;
    bool pending_finish(int32_t driver_sequence) const;
    Status set_source_finished(int32_t driver_sequence);

    Status attach_source_observer(int32_t driver_sequence, RuntimeState* state, PipelineObserver* observer);

    const std::shared_ptr<AIChunkBuffer>& input_buffer() const { return _input_buffer; }

private:
    struct ResultCell;
    struct OutputState;
    struct ActiveSubchunk;
    struct Lane;
    struct Submission;
    enum class TerminalKind : uint8_t;

    AIProjectProcessor(std::shared_ptr<AIChunkBuffer> input_buffer, std::shared_ptr<AIProjectProjection> projection,
                       std::shared_ptr<AIProjectTaskSubmitter> submitter, AIRuntimeConfig config);

    StatusOr<std::shared_ptr<Lane>> _lane(int32_t driver_sequence) const;
    Status _prepare_and_submit(RuntimeState* state, int32_t driver_sequence, const std::shared_ptr<Lane>& lane,
                               ChunkPtr slice);
    static void _complete_task(const std::shared_ptr<Lane>& lane, bool ignore_row_failures, uint64_t task_id,
                               size_t output_index, size_t row_index, AITaskResult result) noexcept;
    static void _complete_submit_failure(const std::shared_ptr<Lane>& lane, bool ignore_row_failures, uint64_t task_id,
                                         size_t output_index, size_t row_index, const Status& status);
    static StatusOr<ChunkPtr> _materialize(const std::shared_ptr<ActiveSubchunk>& subchunk);

    static Status _terminal_status(const Status& driver_status, TerminalKind terminal_kind);
    static Status _row_failure_status();
    static void _dispose_subchunk_handles(const std::shared_ptr<Lane>& lane,
                                          const std::shared_ptr<ActiveSubchunk>& subchunk,
                                          bool cancel_unfinished) noexcept;
    static void _release_subchunk_results(const std::shared_ptr<Lane>& lane,
                                          const std::shared_ptr<ActiveSubchunk>& subchunk) noexcept;
    static void _notify_source(PipeObservable* observable);

    std::shared_ptr<AIChunkBuffer> _input_buffer;
    std::shared_ptr<AIProjectProjection> _projection;
    std::shared_ptr<AIProjectTaskSubmitter> _submitter;
    const AIMemoryContext _memory;
    const AIRuntimeConfig _config;

    mutable std::mutex _configure_mutex;
    Status _configuration_status;
    int32_t _configured_dop = 0;
    std::vector<std::shared_ptr<Lane>> _lanes;

    mutable std::mutex _lifecycle_mutex;
    Status _lifecycle_status;
    RuntimeState* _lifecycle_state = nullptr;
    int32_t _lifecycle_dop = 0;
    bool _prepare_attempted = false;
    bool _closed = false;

    std::atomic<uint64_t> _next_task_id{1};
};

} // namespace pipeline
} // namespace starrocks
