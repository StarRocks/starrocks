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

#include "exec/pipeline/ai/ai_project_processor.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/memory/column_allocator.h"
#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exprs/ai/ai_function_call_expr.h"
#include "platform/llm/ai_runtime.h"
#include "platform/llm/ai_task_dispatcher.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

constexpr SlotId kInputIdSlot = 1;
constexpr SlotId kPromptSlot = 2;
constexpr SlotId kOutputIdSlot = 11;
constexpr SlotId kAIOutputSlot = 12;
constexpr size_t kMiB = 1024UL * 1024;
constexpr std::string_view kSecretExceptionSentinel = "unit-test-secret-exception-must-not-leak";

ChunkPtr make_input_chunk(size_t begin, size_t rows) {
    auto ids = Int32Column::create();
    auto prompts = BinaryColumn::create();
    for (size_t offset = 0; offset < rows; ++offset) {
        const size_t row = begin + offset;
        ids->append(static_cast<int32_t>(row));
        prompts->append("prompt-" + std::to_string(row));
    }

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(ids), kInputIdSlot);
    chunk->append_column(std::move(prompts), kPromptSlot);
    return chunk;
}

std::shared_ptr<AIChunkBuffer> make_input_buffer() {
    auto buffer = AIChunkBuffer::create(12, 32 * kMiB);
    EXPECT_TRUE(buffer.ok()) << buffer.status();
    return buffer.ok() ? std::move(buffer).value() : nullptr;
}

AIRuntimeConfig runtime_config(std::string on_error = "ignore") {
    AIRuntimeConfig config;
    config.sub_chunk_size = 64;
    config.on_error = std::move(on_error);
    return config;
}

class RecordingProjection final : public AIProjectProjection {
public:
    enum class PrepareException : uint8_t { NONE, BAD_ALLOC, RUNTIME_ERROR };

    StatusOr<AIProjectPreparedSubchunk> prepare_subchunk(RuntimeState*, int32_t driver_sequence,
                                                         const ChunkPtr& input) override {
        const size_t prepare_call = prepare_calls++;
        observed_driver_sequences.emplace_back(driver_sequence);
        observed_rows.emplace_back(input->num_rows());
        observed_owner_ids.emplace_back(input->owner_info().owner_id());
        observed_last_markers.emplace_back(input->owner_info().is_last_chunk());
        observed_passthrough_markers.emplace_back(input->owner_info().is_passthrough());
        if (throw_on_prepare_call.has_value() && *throw_on_prepare_call == prepare_call) {
            if (prepare_exception == PrepareException::BAD_ALLOC) {
                throw std::bad_alloc();
            }
            if (prepare_exception == PrepareException::RUNTIME_ERROR) {
                throw std::runtime_error(std::string(kSecretExceptionSentinel));
            }
        }

        AIProjectPreparedSubchunk prepared;
        prepared.output_chunk = std::make_shared<Chunk>();
        if (output_chunk_rows.has_value()) {
            auto output_ids = Int32Column::create();
            for (size_t row = 0; row < *output_chunk_rows; ++row) {
                output_ids->append(static_cast<int32_t>(row));
            }
            prepared.output_chunk->append_column(std::move(output_ids), kOutputIdSlot);
        } else {
            prepared.output_chunk->append_column(input->get_column_by_slot_id(kInputIdSlot), kOutputIdSlot);
        }
        if (output_chunk_contains_ai_slot) {
            auto conflicting_output = BinaryColumn::create();
            for (size_t row = 0; row < input->num_rows(); ++row) {
                conflicting_output->append("existing-output");
            }
            prepared.output_chunk->append_column(std::move(conflicting_output), kAIOutputSlot);
        }

        AIProjectPreparedOutput output;
        output.slot_id = kAIOutputSlot;
        const auto& prompts = down_cast<const BinaryColumn&>(*input->get_column_by_slot_id(kPromptSlot));
        output.input.rows.reserve(input->num_rows());
        for (size_t row = 0; row < input->num_rows(); ++row) {
            AIFunctionRowInput input_row;
            input_row.action = row_actions.empty() ? AIFunctionRowAction::DISPATCH : row_actions.at(row);
            input_row.model = "model";
            input_row.prompt = prompts.get_slice(row).to_string();
            output.input.rows.emplace_back(std::move(input_row));
        }
        prepared.ai_outputs.emplace_back(std::move(output));
        last_output_chunk = prepared.output_chunk;
        return prepared;
    }

    std::optional<size_t> throw_on_prepare_call;
    PrepareException prepare_exception = PrepareException::NONE;
    std::optional<size_t> output_chunk_rows;
    bool output_chunk_contains_ai_slot = false;
    std::vector<AIFunctionRowAction> row_actions;
    size_t prepare_calls = 0;
    std::vector<int32_t> observed_driver_sequences;
    std::vector<size_t> observed_rows;
    std::vector<int64_t> observed_owner_ids;
    std::vector<bool> observed_last_markers;
    std::vector<bool> observed_passthrough_markers;
    std::weak_ptr<Chunk> last_output_chunk;
};

struct ManualHandleState {
    int cancel_calls = 0;
    int destroy_calls = 0;
    std::thread::id destroy_thread;
};

struct ResultMemoryState {
    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
};

class ResultMemoryContext {
public:
    explicit ResultMemoryContext(std::shared_ptr<ResultMemoryState> state) : _state(new State(std::move(state))) {
        _context = AIMemoryContext::create(_state, &reserve, &release, &run, &retain, &release_owner);
    }

    AIMemoryContext context() const { return _context; }

private:
    struct State {
        explicit State(std::shared_ptr<ResultMemoryState> result_memory) : result_memory(std::move(result_memory)) {}

        std::atomic<size_t> references{0};
        std::shared_ptr<ResultMemoryState> result_memory;
    };

    static bool reserve(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<State*>(opaque);
        ++state->result_memory->reserve_calls;
        state->result_memory->reserved_bytes += bytes;
        return true;
    }

    static void release(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<State*>(opaque);
        ++state->result_memory->release_calls;
        state->result_memory->released_bytes += bytes;
    }

    static void run(void*, AIMemoryContext::Action action, void* action_context) { action(action_context); }

    static void retain(void* opaque) noexcept {
        static_cast<State*>(opaque)->references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release_owner(void* opaque) noexcept {
        auto* state = static_cast<State*>(opaque);
        if (state->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete state;
        }
    }

    State* _state;
    AIMemoryContext _context;
};

class ManualTaskHandle final : public AIProjectTaskHandle {
public:
    explicit ManualTaskHandle(std::shared_ptr<ManualHandleState> state) : _state(std::move(state)) {}
    ~ManualTaskHandle() override {
        ++_state->destroy_calls;
        _state->destroy_thread = std::this_thread::get_id();
    }

    void cancel() noexcept override { ++_state->cancel_calls; }

private:
    std::shared_ptr<ManualHandleState> _state;
};

class ManualTaskSubmitter final : public AIProjectTaskSubmitter {
public:
    enum class SubmitException : uint8_t { NONE, BAD_ALLOC, RUNTIME_ERROR };

    struct Pending {
        std::string model;
        std::string prompt;
        AITaskCallback callback;
        std::shared_ptr<ManualHandleState> handle_state;
    };

    StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                          AITaskCallback&& callback) override {
        const size_t submit_call = submit_calls++;
        if (throw_on_submit_call.has_value() && *throw_on_submit_call == submit_call) {
            if (submit_exception == SubmitException::BAD_ALLOC) {
                throw std::bad_alloc();
            }
            if (submit_exception == SubmitException::RUNTIME_ERROR) {
                throw std::runtime_error(std::string(kSecretExceptionSentinel));
            }
        }
        if (submit_error.has_value()) {
            return *submit_error;
        }

        auto handle_state = std::make_shared<ManualHandleState>();
        handle_states.emplace_back(handle_state);
        submitted_prompts.emplace_back(request.prompt);

        {
            std::unique_lock lock(block_mutex);
            if (block_submit) {
                submit_entered = true;
                block_cv.notify_all();
                if (!block_cv.wait_for(lock, std::chrono::seconds(5), [&] { return release_submit; })) {
                    return Status::TimedOut("timed out waiting to release the blocked test submission");
                }
            }
        }

        if (inline_success) {
            callback(success_result("result-" + suffix(request.prompt)));
        } else if (inline_row_failure) {
            callback(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::PROVIDER_RESPONSE});
        } else {
            const auto [iterator, inserted] =
                    pending.emplace(request.task_id, Pending{std::string(request.model), std::string(request.prompt),
                                                             std::move(callback), handle_state});
            if (!inserted) {
                return Status::InternalError("duplicate test task id");
            }
        }
        return std::make_unique<ManualTaskHandle>(std::move(handle_state));
    }

    void succeed(std::string_view prompt) { complete(prompt, success_result("result-" + suffix(prompt))); }

    void succeed_with_memory_tracking(std::string_view prompt, const std::shared_ptr<ResultMemoryState>& memory_state) {
        ResultMemoryContext memory(memory_state);
        auto result = AITaskSuccess::create("result-" + suffix(prompt), memory.context());
        ASSERT_TRUE(result.ok()) << result.status();
        complete(prompt, std::move(result).value());
    }

    void fail_row(std::string_view prompt) {
        complete(prompt, AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::PROVIDER_RESPONSE});
    }

    void cancel_lifecycle(std::string_view prompt) {
        complete(prompt, AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
    }

    void complete_remaining_as_cancelled() {
        std::vector<uint64_t> task_ids;
        task_ids.reserve(pending.size());
        for (const auto& [task_id, unused] : pending) {
            task_ids.emplace_back(task_id);
        }
        for (uint64_t task_id : task_ids) {
            auto iterator = pending.find(task_id);
            ASSERT_NE(pending.end(), iterator);
            Pending task = std::move(iterator->second);
            pending.erase(iterator);
            task.callback(AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
        }
    }

    size_t pending_count() const { return pending.size(); }

    bool wait_until_submit_enters() {
        std::unique_lock lock(block_mutex);
        return block_cv.wait_for(lock, std::chrono::seconds(5), [&] { return submit_entered; });
    }

    void release_blocked_submit() {
        std::lock_guard lock(block_mutex);
        release_submit = true;
        block_cv.notify_all();
    }

    int cancel_calls(std::string_view prompt) const {
        for (const auto& [task_id, task] : pending) {
            if (task.prompt == prompt) {
                return task.handle_state->cancel_calls;
            }
        }
        return -1;
    }

    bool inline_success = false;
    bool inline_row_failure = false;
    bool block_submit = false;
    std::optional<Status> submit_error;
    std::optional<size_t> throw_on_submit_call;
    SubmitException submit_exception = SubmitException::NONE;
    size_t submit_calls = 0;
    std::vector<std::string> submitted_prompts;
    std::vector<std::shared_ptr<ManualHandleState>> handle_states;
    std::unordered_map<uint64_t, Pending> pending;

    std::mutex block_mutex;
    std::condition_variable block_cv;
    bool submit_entered = false;
    bool release_submit = false;

private:
    static std::string suffix(std::string_view prompt) {
        constexpr std::string_view kPrefix = "prompt-";
        EXPECT_TRUE(prompt.starts_with(kPrefix));
        return std::string(prompt.substr(kPrefix.size()));
    }

    static AITaskResult success_result(std::string content) {
        auto result = AITaskSuccess::create(std::move(content), {});
        EXPECT_TRUE(result.ok()) << result.status();
        if (!result.ok()) {
            return AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE};
        }
        return std::move(result).value();
    }

    void complete(std::string_view prompt, AITaskResult result) {
        auto iterator = std::find_if(pending.begin(), pending.end(),
                                     [&](const auto& entry) { return entry.second.prompt == prompt; });
        ASSERT_NE(pending.end(), iterator) << prompt;
        Pending task = std::move(iterator->second);
        pending.erase(iterator);
        task.callback(std::move(result));
    }
};

class FailNextColumnAllocation final : public Allocator {
public:
    explicit FailNextColumnAllocation(Allocator* delegate) : _delegate(delegate) {}

    void* alloc(size_t size) override { return _delegate->alloc(size); }
    void free(void* ptr) override { _delegate->free(ptr); }
    void* realloc(void* ptr, size_t size) override { return _delegate->realloc(ptr, size); }
    void* calloc(size_t n, size_t size) override { return _delegate->calloc(n, size); }
    void cfree(void* ptr) override { _delegate->cfree(ptr); }
    void* memalign(size_t align, size_t size) override { return _delegate->memalign(align, size); }
    void* aligned_alloc(size_t align, size_t size) override { return _delegate->aligned_alloc(align, size); }
    void* valloc(size_t size) override { return _delegate->valloc(size); }
    void* pvalloc(size_t size) override { return _delegate->pvalloc(size); }
    int posix_memalign(void** ptr, size_t align, size_t size) override {
        return _delegate->posix_memalign(ptr, align, size);
    }

    void* checked_alloc(size_t size) override {
        if (!_failed) {
            _failed = true;
            throw std::bad_alloc();
        }
        return _delegate->checked_alloc(size);
    }

private:
    Allocator* _delegate;
    bool _failed = false;
};

class BlockingSourceObserver final : public PipelineObserver {
public:
    void source_trigger() override {
        std::unique_lock lock(_mutex);
        _entered = true;
        _cv.notify_all();
        _cv.wait_for(lock, std::chrono::seconds(5), [&] { return _released; });
    }

    void sink_trigger() override {}
    void cancel_trigger() override {}
    void all_trigger() override {}
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "blocking-ai-source-observer"; }

    bool wait_until_entered() {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, std::chrono::seconds(5), [&] { return _entered; });
    }

    void release() {
        std::lock_guard lock(_mutex);
        _released = true;
        _cv.notify_all();
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _entered = false;
    bool _released = false;
};

std::shared_ptr<AIProjectProcessor> make_processor(const std::shared_ptr<AIChunkBuffer>& input_buffer,
                                                   const std::shared_ptr<RecordingProjection>& projection,
                                                   const std::shared_ptr<ManualTaskSubmitter>& submitter,
                                                   std::string on_error = "ignore", int32_t dop = 1) {
    auto processor =
            AIProjectProcessor::create(input_buffer, projection, submitter, runtime_config(std::move(on_error)));
    EXPECT_TRUE(processor.ok()) << processor.status();
    if (!processor.ok()) {
        return nullptr;
    }
    Status configure_status = processor.value()->configure(dop);
    EXPECT_TRUE(configure_status.ok()) << configure_status;
    if (!configure_status.ok()) {
        return nullptr;
    }
    return std::move(processor).value();
}

void put_and_finish(const std::shared_ptr<AIChunkBuffer>& buffer, const ChunkPtr& input, int32_t driver_sequence = 0) {
    auto admitted = buffer->try_put(driver_sequence, input);
    ASSERT_TRUE(admitted.ok()) << admitted.status();
    ASSERT_TRUE(admitted.value());
    ASSERT_OK(buffer->set_sink_eos(driver_sequence));
}

int32_t int_value(const ChunkPtr& chunk, SlotId slot_id, size_t row) {
    return down_cast<const Int32Column&>(*chunk->get_column_by_slot_id(slot_id)).get_data()[row];
}

const NullableColumn& ai_column(const ChunkPtr& chunk) {
    const ColumnPtr& column = chunk->get_column_by_slot_id(kAIOutputSlot);
    EXPECT_TRUE(column->is_nullable());
    return down_cast<const NullableColumn&>(*column);
}

std::string ai_value(const ChunkPtr& chunk, size_t row) {
    const auto& nullable = ai_column(chunk);
    EXPECT_FALSE(nullable.is_null(row));
    const auto& data = down_cast<const BinaryColumn&>(*nullable.data_column());
    return data.get_slice(row).to_string();
}

TEST(AIProjectProcessorTest, Splits65RowsIntoStable64And1SlicesAndPublishesInRowOrder) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);

    auto input = make_input_chunk(0, 65);
    input->owner_info().set_owner_id(37, true);
    input->owner_info().set_passthrough(true);
    put_and_finish(buffer, input);

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_EQ((std::vector<size_t>{64}), projection->observed_rows);
    EXPECT_EQ(64, submitter->pending_count());
    EXPECT_FALSE(processor->has_output(0));
    EXPECT_TRUE(processor->pending_finish(0));

    // One lane has only one active subchunk. Re-polling cannot dispatch row 64
    // while rows 0..63 are still outstanding.
    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_EQ(64, submitter->pending_count());
    EXPECT_EQ((std::vector<size_t>{64}), projection->observed_rows);

    for (size_t row = 64; row > 0; --row) {
        submitter->succeed("prompt-" + std::to_string(row - 1));
        EXPECT_EQ(row == 1, processor->has_output(0));
    }
    EXPECT_FALSE(processor->pending_finish(0));

    // A terminal but unconsumed subchunk still owns the lane.
    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_EQ(0, submitter->pending_count());
    EXPECT_EQ((std::vector<size_t>{64}), projection->observed_rows);

    auto first = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(first.ok()) << first.status();
    ASSERT_EQ(64, first.value()->num_rows());
    EXPECT_EQ(37, first.value()->owner_info().owner_id());
    EXPECT_TRUE(first.value()->owner_info().is_passthrough());
    EXPECT_FALSE(first.value()->owner_info().is_last_chunk());
    for (size_t row = 0; row < 64; ++row) {
        EXPECT_EQ(row, int_value(first.value(), kOutputIdSlot, row));
        EXPECT_EQ("result-" + std::to_string(row), ai_value(first.value(), row));
    }

    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_EQ((std::vector<size_t>{64, 1}), projection->observed_rows);
    ASSERT_EQ((std::vector<int64_t>{37, 37}), projection->observed_owner_ids);
    ASSERT_EQ((std::vector<bool>{false, true}), projection->observed_last_markers);
    ASSERT_EQ((std::vector<bool>{true, true}), projection->observed_passthrough_markers);
    ASSERT_EQ(1, submitter->pending_count());
    submitter->succeed("prompt-64");

    auto second = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(second.ok()) << second.status();
    ASSERT_EQ(1, second.value()->num_rows());
    EXPECT_EQ(64, int_value(second.value(), kOutputIdSlot, 0));
    EXPECT_EQ("result-64", ai_value(second.value(), 0));
    EXPECT_EQ(37, second.value()->owner_info().owner_id());
    EXPECT_TRUE(second.value()->owner_info().is_passthrough());
    EXPECT_TRUE(second.value()->owner_info().is_last_chunk());
}

TEST(AIProjectProcessorTest, EmptyLastChunkPreservesOwnerInfoWithoutSubmitting) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);

    auto input = make_input_chunk(0, 0);
    input->owner_info().set_owner_id(41, true);
    input->owner_info().set_passthrough(true);
    put_and_finish(buffer, input);

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_EQ((std::vector<size_t>{0}), projection->observed_rows);
    EXPECT_EQ(0, submitter->submit_calls);
    EXPECT_TRUE(processor->has_output(0));
    EXPECT_FALSE(processor->pending_finish(0));

    auto output = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_NE(nullptr, output.value());
    EXPECT_EQ(0, output.value()->num_rows());
    EXPECT_EQ(41, output.value()->owner_info().owner_id());
    EXPECT_TRUE(output.value()->owner_info().is_passthrough());
    EXPECT_TRUE(output.value()->owner_info().is_last_chunk());

    auto finished = processor->lane_finished(0);
    ASSERT_TRUE(finished.ok()) << finished.status();
    EXPECT_TRUE(finished.value());
}

TEST(AIProjectProcessorTest, IgnoreTurnsOnlySanitizedRowFailureIntoNull) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter, "ignore");
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 3));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    submitter->succeed("prompt-2");
    submitter->fail_row("prompt-1");
    submitter->succeed("prompt-0");

    ASSERT_TRUE(processor->has_output(0));
    auto output = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(3, output.value()->num_rows());
    EXPECT_EQ("result-0", ai_value(output.value(), 0));
    EXPECT_TRUE(ai_column(output.value()).is_null(1));
    EXPECT_EQ("result-2", ai_value(output.value(), 2));
    EXPECT_TRUE(processor->status(0).ok());
}

TEST(AIProjectProcessorTest, CompletionPublicationExceptionCannotStrandOutstandingBarrier) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_TRUE(processor->pending_finish(0));

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->SetCallBack("AIProjectProcessor::_complete_task:before_publish", [](void*) { throw std::bad_alloc(); });
    sync_point->EnableProcessing();
    EXPECT_NO_THROW(submitter->succeed("prompt-0"));
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();

    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_FALSE(processor->status(0).ok());
    EXPECT_TRUE(processor->has_output(0));
    auto output = processor->pull_chunk(&state, 0);
    EXPECT_FALSE(output.ok());
    EXPECT_OK(processor->set_source_finished(0));
}

TEST(AIProjectProcessorTest, LifecycleCancellationIsVisibleImmediatelyAndNeverBecomesNull) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter, "ignore");
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 3));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    submitter->cancel_lifecycle("prompt-1");

    EXPECT_TRUE(processor->status(0).is_cancelled());
    EXPECT_TRUE(processor->has_output(0));
    EXPECT_TRUE(processor->pending_finish(0));

    auto output = processor->pull_chunk(&state, 0);
    ASSERT_FALSE(output.ok());
    EXPECT_TRUE(output.status().is_cancelled()) << output.status();
    EXPECT_EQ(1, submitter->cancel_calls("prompt-0"));
    EXPECT_EQ(1, submitter->cancel_calls("prompt-2"));

    submitter->complete_remaining_as_cancelled();
    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_TRUE(processor->status(0).is_cancelled());
}

TEST(AIProjectProcessorTest, EarlyFinishCancelsExactlyOnceButWaitsForEveryCallbackToDrain) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 4));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_EQ(4, submitter->pending_count());
    ASSERT_TRUE(processor->pending_finish(0));
    ASSERT_FALSE(projection->last_output_chunk.expired());
    const std::thread::id driver_thread = std::this_thread::get_id();

    ASSERT_OK(processor->set_source_finished(0));
    ASSERT_OK(processor->set_source_finished(0));
    EXPECT_FALSE(processor->has_output(0));
    EXPECT_TRUE(processor->pending_finish(0));
    EXPECT_TRUE(projection->last_output_chunk.expired())
            << "source finish must release the active output while callbacks are still draining";
    for (size_t row = 0; row < 4; ++row) {
        EXPECT_EQ(1, submitter->cancel_calls("prompt-" + std::to_string(row)));
        EXPECT_EQ(1, submitter->handle_states[row]->destroy_calls);
        EXPECT_EQ(driver_thread, submitter->handle_states[row]->destroy_thread);
    }

    submitter->complete_remaining_as_cancelled();
    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_FALSE(processor->has_output(0));
    EXPECT_TRUE(processor->status(0).ok());
    EXPECT_TRUE(buffer->all_sources_finished());
}

TEST(AIProjectProcessorTest, InlineCallbacksCannotLoseHandlesOrOutstandingDrainState) {
    const std::thread::id driver_thread = std::this_thread::get_id();
    {
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        submitter->inline_success = true;
        auto processor = make_processor(buffer, projection, submitter);
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 2));

        RuntimeState state;
        ASSERT_OK(processor->try_process(&state, 0));
        EXPECT_TRUE(processor->has_output(0));
        EXPECT_FALSE(processor->pending_finish(0));
        ASSERT_EQ(2, submitter->handle_states.size());
        for (const auto& handle_state : submitter->handle_states) {
            EXPECT_EQ(0, handle_state->cancel_calls);
            EXPECT_EQ(0, handle_state->destroy_calls)
                    << "an inline-completed handle must remain driver-owned until the result is pulled";
        }

        auto output = processor->pull_chunk(&state, 0);
        ASSERT_TRUE(output.ok()) << output.status();
        ASSERT_EQ(2, output.value()->num_rows());
        EXPECT_EQ("result-0", ai_value(output.value(), 0));
        EXPECT_EQ("result-1", ai_value(output.value(), 1));
        for (const auto& handle_state : submitter->handle_states) {
            EXPECT_EQ(0, handle_state->cancel_calls);
            EXPECT_EQ(1, handle_state->destroy_calls);
            EXPECT_EQ(driver_thread, handle_state->destroy_thread);
        }
    }

    {
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        submitter->inline_row_failure = true;
        auto processor = make_processor(buffer, projection, submitter, "fail");
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 1));

        RuntimeState state;
        ASSERT_OK(processor->try_process(&state, 0));
        EXPECT_FALSE(processor->status(0).ok());
        EXPECT_TRUE(processor->has_output(0));
        EXPECT_FALSE(processor->pending_finish(0));
        ASSERT_EQ(1, submitter->handle_states.size());
        EXPECT_EQ(0, submitter->handle_states[0]->cancel_calls)
                << "a completed inline terminal task must not be cancelled";
        EXPECT_EQ(0, submitter->handle_states[0]->destroy_calls)
                << "a completed inline terminal handle must remain driver-owned until terminal consumption";

        auto output = processor->pull_chunk(&state, 0);
        EXPECT_FALSE(output.ok());
        EXPECT_EQ(0, submitter->handle_states[0]->cancel_calls);
        EXPECT_EQ(1, submitter->handle_states[0]->destroy_calls);
        EXPECT_EQ(driver_thread, submitter->handle_states[0]->destroy_thread);
    }
}

TEST(AIProjectProcessorTest, PendingFinishCoversTheCompleteObserverNotification) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    state.set_enable_event_scheduler(true);
    auto query_lifetime = std::make_shared<QueryContextLifetime>();
    state.set_query_ctx_lifetime(query_lifetime);
    BlockingSourceObserver observer;
    ASSERT_OK(processor->attach_source_observer(0, &state, &observer));
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_EQ(1, submitter->handle_states.size());
    const auto handle_state = submitter->handle_states.front();
    const std::thread::id driver_thread = std::this_thread::get_id();

    std::thread completion([&] { submitter->succeed("prompt-0"); });
    DeferOp completion_cleanup([&] {
        observer.release();
        if (completion.joinable()) {
            completion.join();
        }
    });
    ASSERT_TRUE(observer.wait_until_entered());
    EXPECT_TRUE(processor->pending_finish(0));
    EXPECT_EQ(0, handle_state->cancel_calls);
    EXPECT_EQ(0, handle_state->destroy_calls)
            << "the callback and observer must not destroy a driver-owned completed handle";
    observer.release();
    completion.join();

    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_TRUE(processor->has_output(0));
    EXPECT_EQ(0, handle_state->destroy_calls);
    auto output = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_EQ(0, handle_state->cancel_calls);
    EXPECT_EQ(1, handle_state->destroy_calls);
    EXPECT_EQ(driver_thread, handle_state->destroy_thread);
}

TEST(AIProjectProcessorTest, FinalDrainNotificationPinsQueryLifetimeAfterSourceFinish) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    std::weak_ptr<AIProjectProcessor> weak_processor = processor;
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    state.set_enable_event_scheduler(true);
    auto query_lifetime = std::make_shared<QueryContextLifetime>();
    std::weak_ptr<QueryContextLifetime> weak_query_lifetime = query_lifetime;
    state.set_query_ctx_lifetime(query_lifetime);
    BlockingSourceObserver observer;
    ASSERT_OK(processor->attach_source_observer(0, &state, &observer));
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_OK(processor->set_source_finished(0));
    ASSERT_TRUE(processor->pending_finish(0));

    std::thread completion([&] { submitter->cancel_lifecycle("prompt-0"); });
    DeferOp completion_cleanup([&] {
        observer.release();
        if (completion.joinable()) {
            completion.join();
        }
    });
    ASSERT_TRUE(observer.wait_until_entered());
    EXPECT_FALSE(processor->pending_finish(0))
            << "the final drain notification must observe the callback barrier already released";
    processor.reset();
    EXPECT_TRUE(weak_processor.expired()) << "the async completion lane must not retain the query-owned processor";

    query_lifetime.reset();
    EXPECT_FALSE(weak_query_lifetime.expired())
            << "the final callback must pin QueryContext while notifying its raw pipeline observer";

    observer.release();
    completion.join();
    EXPECT_TRUE(weak_query_lifetime.expired());
}

TEST(AIProjectProcessorTest, FinalSubmissionDrainNotificationPinsQueryLifetimeAfterSourceFinish) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    submitter->block_submit = true;
    submitter->inline_success = true;
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    state.set_enable_event_scheduler(true);
    auto query_lifetime = std::make_shared<QueryContextLifetime>();
    std::weak_ptr<QueryContextLifetime> weak_query_lifetime = query_lifetime;
    state.set_query_ctx_lifetime(query_lifetime);
    BlockingSourceObserver observer;
    ASSERT_OK(processor->attach_source_observer(0, &state, &observer));

    Status process_status;
    std::thread processing([&] { process_status = processor->try_process(&state, 0); });
    DeferOp processing_cleanup([&] {
        submitter->release_blocked_submit();
        observer.release();
        if (processing.joinable()) {
            processing.join();
        }
    });
    ASSERT_TRUE(submitter->wait_until_submit_enters());
    ASSERT_OK(processor->set_source_finished(0));
    ASSERT_TRUE(processor->pending_finish(0));

    submitter->release_blocked_submit();
    ASSERT_TRUE(observer.wait_until_entered());
    EXPECT_FALSE(processor->pending_finish(0))
            << "the final drain notification must observe the submission barrier already released";

    query_lifetime.reset();
    EXPECT_FALSE(weak_query_lifetime.expired())
            << "the final submit barrier must pin QueryContext while notifying its raw pipeline observer";

    observer.release();
    processing.join();
    EXPECT_OK(process_status);
    EXPECT_TRUE(weak_query_lifetime.expired());
    EXPECT_FALSE(processor->pending_finish(0));
    ASSERT_EQ(1, submitter->handle_states.size());
    EXPECT_EQ(1, submitter->handle_states[0]->cancel_calls);
}

TEST(AIProjectProcessorTest, ConflictingDopCannotPreventInflightCallbackDrain) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_EQ(1, submitter->pending_count());
    EXPECT_FALSE(processor->configure(2).ok());
    EXPECT_FALSE(processor->configuration_status().ok());

    submitter->succeed("prompt-0");
    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_TRUE(processor->status(0).ok());
    auto output = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_EQ("result-0", ai_value(output.value(), 0));
}

TEST(AIProjectProcessorTest, FinishWhileSubmitIsBlockedCancelsTheLateHandleAndStillDrains) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    submitter->block_submit = true;
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1));

    RuntimeState state;
    Status process_status;
    std::thread::id processing_thread;
    std::thread processing([&] {
        processing_thread = std::this_thread::get_id();
        process_status = processor->try_process(&state, 0);
    });
    DeferOp processing_cleanup([&] {
        submitter->release_blocked_submit();
        if (processing.joinable()) {
            processing.join();
        }
    });
    ASSERT_TRUE(submitter->wait_until_submit_enters());

    ASSERT_OK(processor->set_source_finished(0));
    EXPECT_TRUE(projection->last_output_chunk.expired())
            << "source finish must release the prepared output even while submit() remains blocked";
    EXPECT_TRUE(processor->pending_finish(0));
    submitter->release_blocked_submit();
    processing.join();

    ASSERT_OK(process_status);
    ASSERT_EQ(1, submitter->handle_states.size());
    EXPECT_EQ(1, submitter->handle_states[0]->cancel_calls);
    EXPECT_EQ(1, submitter->handle_states[0]->destroy_calls);
    EXPECT_EQ(processing_thread, submitter->handle_states[0]->destroy_thread);
    EXPECT_TRUE(processor->pending_finish(0));
    submitter->complete_remaining_as_cancelled();
    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_TRUE(processor->status(0).ok());
}

TEST(AIProjectProcessorTest, SubmitLifecycleErrorsAreNeverIgnored) {
    auto run = [](Status submit_error) {
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        submitter->submit_error.emplace(std::move(submit_error));
        auto processor = make_processor(buffer, projection, submitter, "ignore");
        EXPECT_NE(nullptr, processor);
        if (processor == nullptr) {
            return Status::InternalError("processor construction failed");
        }
        put_and_finish(buffer, make_input_chunk(0, 1));

        RuntimeState state;
        EXPECT_OK(processor->try_process(&state, 0));
        auto output = processor->pull_chunk(&state, 0);
        EXPECT_FALSE(output.ok());
        return output.ok() ? Status::InternalError("expected a terminal status") : output.status();
    };

    EXPECT_TRUE(run(Status::Cancelled("cancelled")).is_cancelled());
    EXPECT_TRUE(run(Status::TimedOut("deadline")).is_time_out());
    EXPECT_TRUE(run(Status::Shutdown("shutdown")).is_shutdown());
}

TEST(AIProjectProcessorTest, SqlNullRowsDoNotSubmitHttpTasks) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    projection->row_actions = {AIFunctionRowAction::SQL_NULL, AIFunctionRowAction::SQL_NULL};
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 2));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_TRUE(submitter->submitted_prompts.empty());
    EXPECT_FALSE(processor->pending_finish(0));

    auto output = processor->pull_chunk(&state, 0);
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_TRUE(ai_column(output.value()).is_null(0));
    EXPECT_TRUE(ai_column(output.value()).is_null(1));
}

TEST(AIProjectProcessorTest, ProjectionExceptionsAreTerminalAndCannotSkipToTheNextSlice) {
    const std::vector<RecordingProjection::PrepareException> exceptions = {
            RecordingProjection::PrepareException::BAD_ALLOC,
            RecordingProjection::PrepareException::RUNTIME_ERROR,
    };
    for (const auto exception : exceptions) {
        SCOPED_TRACE(exception == RecordingProjection::PrepareException::BAD_ALLOC ? "bad_alloc" : "runtime_error");
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        projection->throw_on_prepare_call = 0;
        projection->prepare_exception = exception;
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        auto processor = make_processor(buffer, projection, submitter);
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 65));

        RuntimeState state;
        Status process_status = processor->try_process(&state, 0);
        EXPECT_FALSE(process_status.ok());
        if (exception == RecordingProjection::PrepareException::BAD_ALLOC) {
            EXPECT_TRUE(process_status.is_mem_limit_exceeded()) << process_status;
        } else {
            EXPECT_EQ(TStatusCode::RUNTIME_ERROR, process_status.code()) << process_status;
            EXPECT_EQ(std::string::npos, process_status.to_string().find(kSecretExceptionSentinel));
        }

        Status lane_status = processor->status(0);
        EXPECT_FALSE(lane_status.ok()) << "a projection exception must become the lane terminal status";
        if (exception == RecordingProjection::PrepareException::BAD_ALLOC) {
            EXPECT_TRUE(lane_status.is_mem_limit_exceeded()) << lane_status;
        } else {
            EXPECT_EQ(TStatusCode::RUNTIME_ERROR, lane_status.code()) << lane_status;
            EXPECT_EQ(std::string::npos, lane_status.to_string().find(kSecretExceptionSentinel));
        }

        EXPECT_OK(processor->try_process(&state, 0));
        EXPECT_EQ((std::vector<size_t>{64}), projection->observed_rows)
                << "retrying a terminal lane must not skip the failed first slice and process row 64";
        EXPECT_EQ(0, submitter->submit_calls);

        EXPECT_OK(processor->set_source_finished(0));
        if (submitter->pending_count() > 0) {
            submitter->complete_remaining_as_cancelled();
        }
        EXPECT_FALSE(processor->pending_finish(0));
        EXPECT_TRUE(buffer->all_sources_finished());
    }
}

TEST(AIProjectProcessorTest, RejectsProjectionOutputRowCountMismatchBeforeSubmitting) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    projection->output_chunk_rows = 1;
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 2));

    RuntimeState state;
    Status process_status = processor->try_process(&state, 0);
    EXPECT_FALSE(process_status.ok());
    EXPECT_FALSE(processor->status(0).ok());
    EXPECT_EQ(0, submitter->submit_calls);

    EXPECT_OK(processor->set_source_finished(0));
    if (submitter->pending_count() > 0) {
        submitter->complete_remaining_as_cancelled();
    }
    EXPECT_FALSE(processor->pending_finish(0));
}

TEST(AIProjectProcessorTest, RejectsAIOutputSlotCollisionBeforeSubmitting) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    projection->output_chunk_contains_ai_slot = true;
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 2));

    RuntimeState state;
    Status process_status = processor->try_process(&state, 0);
    EXPECT_FALSE(process_status.ok());
    EXPECT_FALSE(processor->status(0).ok());
    EXPECT_EQ(0, submitter->submit_calls);

    EXPECT_OK(processor->set_source_finished(0));
    if (submitter->pending_count() > 0) {
        submitter->complete_remaining_as_cancelled();
    }
    EXPECT_FALSE(processor->pending_finish(0));
}

TEST(AIProjectProcessorTest, FailModePublishesAsyncRowFailureAsTerminal) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter, "fail");
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 3));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    submitter->fail_row("prompt-1");

    EXPECT_FALSE(processor->status(0).ok());
    EXPECT_TRUE(processor->has_output(0));
    ASSERT_EQ(3, submitter->handle_states.size());
    EXPECT_EQ(0, submitter->handle_states[1]->destroy_calls);
    auto output = processor->pull_chunk(&state, 0);
    EXPECT_FALSE(output.ok());
    EXPECT_EQ(1, submitter->cancel_calls("prompt-0"));
    EXPECT_EQ(0, submitter->handle_states[1]->cancel_calls);
    EXPECT_EQ(1, submitter->cancel_calls("prompt-2"));
    const std::thread::id driver_thread = std::this_thread::get_id();
    for (const auto& handle_state : submitter->handle_states) {
        EXPECT_EQ(1, handle_state->destroy_calls);
        EXPECT_EQ(driver_thread, handle_state->destroy_thread);
    }

    submitter->complete_remaining_as_cancelled();
    EXPECT_FALSE(processor->pending_finish(0));
    EXPECT_OK(processor->set_source_finished(0));
}

TEST(AIProjectProcessorTest, TerminalRowFailureHonorsIgnoreAndFailModesWithoutSubmitting) {
    {
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        projection->row_actions = {AIFunctionRowAction::TERMINAL_ROW_FAILURE};
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        auto processor = make_processor(buffer, projection, submitter, "ignore");
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 1));

        RuntimeState state;
        ASSERT_OK(processor->try_process(&state, 0));
        EXPECT_EQ(0, submitter->submit_calls);
        EXPECT_OK(processor->status(0));
        auto output = processor->pull_chunk(&state, 0);
        ASSERT_TRUE(output.ok()) << output.status();
        EXPECT_TRUE(ai_column(output.value()).is_null(0));
    }

    {
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        projection->row_actions = {AIFunctionRowAction::TERMINAL_ROW_FAILURE};
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        auto processor = make_processor(buffer, projection, submitter, "fail");
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 1));

        RuntimeState state;
        Status process_status = processor->try_process(&state, 0);
        EXPECT_FALSE(process_status.ok());
        EXPECT_FALSE(processor->status(0).ok());
        EXPECT_EQ(0, submitter->submit_calls);
        auto output = processor->pull_chunk(&state, 0);
        EXPECT_FALSE(output.ok());
        EXPECT_OK(processor->set_source_finished(0));
    }
}

TEST(AIProjectProcessorTest, DopLanesKeepDataAndTerminalStateIsolated) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter, "fail", 2);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 1), 0);
    put_and_finish(buffer, make_input_chunk(100, 1), 1);

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    ASSERT_OK(processor->try_process(&state, 1));
    ASSERT_EQ((std::vector<int32_t>{0, 1}), projection->observed_driver_sequences);
    ASSERT_EQ(2, submitter->pending_count());

    submitter->fail_row("prompt-0");
    submitter->succeed("prompt-100");
    EXPECT_FALSE(processor->status(0).ok());
    EXPECT_OK(processor->status(1));
    EXPECT_TRUE(processor->has_output(0));
    EXPECT_TRUE(processor->has_output(1));

    auto lane_one_output = processor->pull_chunk(&state, 1);
    ASSERT_TRUE(lane_one_output.ok()) << lane_one_output.status();
    ASSERT_EQ(1, lane_one_output.value()->num_rows());
    EXPECT_EQ(100, int_value(lane_one_output.value(), kOutputIdSlot, 0));
    EXPECT_EQ("result-100", ai_value(lane_one_output.value(), 0));
    auto lane_one_finished = processor->lane_finished(1);
    ASSERT_TRUE(lane_one_finished.ok()) << lane_one_finished.status();
    EXPECT_TRUE(lane_one_finished.value());

    auto lane_zero_output = processor->pull_chunk(&state, 0);
    EXPECT_FALSE(lane_zero_output.ok());
    EXPECT_OK(processor->set_source_finished(0));
    EXPECT_OK(processor->set_source_finished(1));
}

TEST(AIProjectProcessorTest, SourceFinishImmediatelyReleasesCompletedSuccessMemory) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 2));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    auto result_memory = std::make_shared<ResultMemoryState>();
    submitter->succeed_with_memory_tracking("prompt-0", result_memory);
    ASSERT_EQ(1, result_memory->reserve_calls);
    ASSERT_EQ(0, result_memory->release_calls);
    ASSERT_TRUE(processor->pending_finish(0));

    ASSERT_OK(processor->set_source_finished(0));
    EXPECT_EQ(1, submitter->cancel_calls("prompt-1"));
    EXPECT_EQ(1, result_memory->release_calls)
            << "a finished source must not retain an already completed result through its active subchunk";
    EXPECT_EQ(result_memory->reserved_bytes, result_memory->released_bytes);

    submitter->complete_remaining_as_cancelled();
    EXPECT_FALSE(processor->pending_finish(0));
}

TEST(AIProjectProcessorTest, MaterializeFailureIsTerminalAndKeepsTheActiveSliceUntilSourceCleanup) {
    auto buffer = make_input_buffer();
    auto projection = std::make_shared<RecordingProjection>();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    auto processor = make_processor(buffer, projection, submitter);
    ASSERT_NE(nullptr, processor);
    put_and_finish(buffer, make_input_chunk(0, 65));

    RuntimeState state;
    ASSERT_OK(processor->try_process(&state, 0));
    auto result_memory = std::make_shared<ResultMemoryState>();
    submitter->succeed_with_memory_tracking("prompt-0", result_memory);
    for (size_t row = 1; row < 64; ++row) {
        submitter->succeed("prompt-" + std::to_string(row));
    }
    ASSERT_TRUE(processor->has_output(0));
    ASSERT_FALSE(processor->pending_finish(0));
    ASSERT_EQ(0, result_memory->release_calls);

    auto output = [&] {
        FailNextColumnAllocation failing_allocator(tls_column_allocator);
        ThreadLocalColumnAllocatorSetter allocator_setter(&failing_allocator);
        return processor->pull_chunk(&state, 0);
    }();
    ASSERT_FALSE(output.ok());
    EXPECT_TRUE(output.status().is_mem_limit_exceeded()) << output.status();
    EXPECT_TRUE(processor->status(0).is_mem_limit_exceeded()) << processor->status(0);
    EXPECT_TRUE(processor->has_output(0));
    EXPECT_EQ(0, result_memory->release_calls)
            << "the active slice must survive materialization failure until source cleanup";

    ASSERT_OK(processor->try_process(&state, 0));
    EXPECT_EQ((std::vector<size_t>{64}), projection->observed_rows);
    EXPECT_EQ(64, submitter->submitted_prompts.size()) << "the second slice must not be dispatched after failure";

    ASSERT_OK(processor->set_source_finished(0));
    EXPECT_EQ(1, result_memory->release_calls);
}

TEST(AIProjectProcessorTest, SubmitExceptionsSettleUnsubmittedRowsAndAllowSubmittedCallbacksToDrain) {
    const std::vector<ManualTaskSubmitter::SubmitException> exceptions = {
            ManualTaskSubmitter::SubmitException::BAD_ALLOC,
            ManualTaskSubmitter::SubmitException::RUNTIME_ERROR,
    };
    for (const auto exception : exceptions) {
        SCOPED_TRACE(exception == ManualTaskSubmitter::SubmitException::BAD_ALLOC ? "bad_alloc" : "runtime_error");
        auto buffer = make_input_buffer();
        auto projection = std::make_shared<RecordingProjection>();
        auto submitter = std::make_shared<ManualTaskSubmitter>();
        submitter->throw_on_submit_call = 1;
        submitter->submit_exception = exception;
        auto processor = make_processor(buffer, projection, submitter);
        ASSERT_NE(nullptr, processor);
        put_and_finish(buffer, make_input_chunk(0, 3));

        RuntimeState state;
        Status process_status = processor->try_process(&state, 0);
        ASSERT_FALSE(process_status.ok());
        if (exception == ManualTaskSubmitter::SubmitException::BAD_ALLOC) {
            EXPECT_TRUE(process_status.is_mem_limit_exceeded()) << process_status;
        } else {
            EXPECT_EQ(TStatusCode::RUNTIME_ERROR, process_status.code()) << process_status;
            EXPECT_EQ(std::string::npos, process_status.to_string().find(kSecretExceptionSentinel));
        }

        ASSERT_EQ(1, submitter->pending_count());
        EXPECT_EQ(1, submitter->cancel_calls("prompt-0"))
                << "a handle submitted before the exception must be cancelled";
        ASSERT_EQ(1, submitter->handle_states.size());
        EXPECT_EQ(1, submitter->handle_states[0]->destroy_calls);
        EXPECT_EQ(std::this_thread::get_id(), submitter->handle_states[0]->destroy_thread);
        EXPECT_FALSE(processor->status(0).ok()) << "the submit exception must become the lane terminal status";
        EXPECT_TRUE(processor->pending_finish(0));

        submitter->complete_remaining_as_cancelled();
        EXPECT_FALSE(processor->pending_finish(0))
                << "unsubmitted result cells must be settled instead of leaking the callback barrier";
    }
}

} // namespace
} // namespace starrocks::pipeline
