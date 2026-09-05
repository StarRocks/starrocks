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

#include "exec/pipeline/ai/ai_project_operator.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "exec/pipeline/ai/ai_chunk_buffer.h"
#include "exec/pipeline/ai/ai_project_processor.h"
#include "exec_primitive/pipeline/primitives/pipeline_observer.h"
#include "exprs/ai/ai_function_call_expr.h"
#include "platform/llm/ai_runtime.h"
#include "platform/llm/ai_task_dispatcher.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

constexpr SlotId kIdSlot = 1;
constexpr SlotId kPromptSlot = 2;
constexpr SlotId kAIOutputSlot = 3;
constexpr size_t kMiB = 1024UL * 1024;

ChunkPtr make_chunk(int32_t begin, size_t rows) {
    auto ids = Int32Column::create();
    auto prompts = BinaryColumn::create();
    for (size_t offset = 0; offset < rows; ++offset) {
        const int32_t value = begin + static_cast<int32_t>(offset);
        ids->append(value);
        prompts->append("prompt-" + std::to_string(value));
    }

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(ids), kIdSlot);
    chunk->append_column(std::move(prompts), kPromptSlot);
    return chunk;
}

int32_t first_id(const ChunkPtr& chunk) {
    return down_cast<const Int32Column&>(*chunk->get_column_by_slot_id(kIdSlot)).get_data().front();
}

class TestProjection final : public AIProjectProjection {
public:
    explicit TestProjection(bool dispatch) : _dispatch(dispatch) {}

    StatusOr<AIProjectPreparedSubchunk> prepare_subchunk(RuntimeState*, int32_t, const ChunkPtr& input) override {
        AIProjectPreparedSubchunk prepared;
        prepared.output_chunk = std::make_shared<Chunk>();
        prepared.output_chunk->append_column(input->get_column_by_slot_id(kIdSlot), kIdSlot);

        AIProjectPreparedOutput output;
        output.slot_id = kAIOutputSlot;
        const auto& prompts = down_cast<const BinaryColumn&>(*input->get_column_by_slot_id(kPromptSlot));
        output.input.rows.reserve(input->num_rows());
        for (size_t row = 0; row < input->num_rows(); ++row) {
            AIFunctionRowInput input_row;
            input_row.action = _dispatch ? AIFunctionRowAction::DISPATCH : AIFunctionRowAction::SQL_NULL;
            input_row.model = "model";
            input_row.prompt = prompts.get_slice(row).to_string();
            output.input.rows.emplace_back(std::move(input_row));
        }
        prepared.ai_outputs.emplace_back(std::move(output));
        return prepared;
    }

private:
    const bool _dispatch;
};

class TestTaskHandle final : public AIProjectTaskHandle {
public:
    void cancel() noexcept override {}
};

class ManualTaskSubmitter final : public AIProjectTaskSubmitter {
public:
    StatusOr<std::unique_ptr<AIProjectTaskHandle>> submit(AIProjectTaskRequest request,
                                                          AITaskCallback&& callback) override {
        _pending.emplace_back(Pending{.prompt = std::string(request.prompt), .callback = std::move(callback)});
        ++_submitted_count;
        std::unique_ptr<AIProjectTaskHandle> handle = std::make_unique<TestTaskHandle>();
        return handle;
    }

    size_t pending_count() const { return _pending.size(); }
    size_t submitted_count() const { return _submitted_count; }

    void complete_all() {
        std::vector<Pending> pending;
        pending.swap(_pending);
        for (Pending& task : pending) {
            auto success = AITaskSuccess::create("result-" + task.prompt, {});
            ASSERT_TRUE(success.ok()) << success.status();
            task.callback(std::move(success).value());
        }
    }

private:
    struct Pending {
        std::string prompt;
        AITaskCallback callback;
    };

    std::vector<Pending> _pending;
    size_t _submitted_count = 0;
};

class CountingObserver final : public PipelineObserver {
public:
    void source_trigger() override { ++source_wakeups; }
    void sink_trigger() override { ++sink_wakeups; }
    void cancel_trigger() override {}
    void all_trigger() override {}
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "ai-project-operator-test-observer"; }

    size_t source_wakeups = 0;
    size_t sink_wakeups = 0;
};

std::shared_ptr<AIProjectProcessor> make_processor(size_t capacity, bool dispatch, int32_t sub_chunk_size = 64) {
    auto buffer = AIChunkBuffer::create(capacity, 32 * kMiB);
    EXPECT_TRUE(buffer.ok()) << buffer.status();
    if (!buffer.ok()) {
        return nullptr;
    }

    AIRuntimeConfig config;
    config.sub_chunk_size = sub_chunk_size;
    config.on_error = "ignore";
    auto processor = AIProjectProcessor::create(std::move(buffer).value(), std::make_shared<TestProjection>(dispatch),
                                                std::make_shared<ManualTaskSubmitter>(), std::move(config));
    EXPECT_TRUE(processor.ok()) << processor.status();
    return processor.ok() ? std::move(processor).value() : nullptr;
}

struct OperatorPair {
    OperatorPtr sink;
    OperatorPtr source;
    std::unique_ptr<CountingObserver> sink_observer;
    std::unique_ptr<CountingObserver> source_observer;
    std::shared_ptr<QueryContextLifetime> query_lifetime;
};

OperatorPair create_operator_pair(AISinkOperatorFactory* sink_factory, AISourceOperatorFactory* source_factory,
                                  RuntimeState* state) {
    OperatorPair pair;
    pair.sink = sink_factory->create(1, 0);
    pair.source = source_factory->create(1, 0);
    pair.sink_observer = std::make_unique<CountingObserver>();
    pair.source_observer = std::make_unique<CountingObserver>();
    if (state->enable_event_scheduler()) {
        pair.query_lifetime = std::make_shared<QueryContextLifetime>();
        state->set_query_ctx_lifetime(pair.query_lifetime);
    }
    pair.sink->set_observer(pair.sink_observer.get());
    pair.source->set_observer(pair.source_observer.get());
    EXPECT_OK(pair.sink->prepare(state));
    EXPECT_OK(pair.source->prepare(state));
    return pair;
}

TEST(AIProjectOperatorTest, PendingSinkChunkIsRetriedBeforeFinishingPublishesEos) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);
    EXPECT_TRUE(sink_factory.support_event_scheduler());
    EXPECT_TRUE(source_factory.support_event_scheduler());

    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);

    ASSERT_TRUE(operators.sink->need_input());
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(10, 1)));
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(20, 1)));
    EXPECT_FALSE(operators.sink->need_input());

    ASSERT_OK(operators.sink->set_finishing(&state));
    auto lane_finished = processor->input_buffer()->lane_finished(0);
    ASSERT_TRUE(lane_finished.ok()) << lane_finished.status();
    EXPECT_FALSE(lane_finished.value());

    ASSERT_TRUE(operators.source->has_output());
    auto first = operators.source->pull_chunk(&state);
    ASSERT_TRUE(first.ok()) << first.status();
    ASSERT_NE(nullptr, first.value());
    EXPECT_EQ(10, first_id(first.value()));
    EXPECT_EQ(0, processor->input_buffer()->size());

    // need_input() is the scheduling poll that retries the retained chunk.
    EXPECT_FALSE(operators.sink->need_input());
    EXPECT_EQ(1, processor->input_buffer()->size());
    lane_finished = processor->input_buffer()->lane_finished(0);
    ASSERT_TRUE(lane_finished.ok()) << lane_finished.status();
    EXPECT_FALSE(lane_finished.value());

    ASSERT_TRUE(operators.source->has_output());
    auto second = operators.source->pull_chunk(&state);
    ASSERT_TRUE(second.ok()) << second.status();
    ASSERT_NE(nullptr, second.value());
    EXPECT_EQ(20, first_id(second.value()));

    lane_finished = processor->input_buffer()->lane_finished(0);
    ASSERT_TRUE(lane_finished.ok()) << lane_finished.status();
    EXPECT_TRUE(lane_finished.value());
    EXPECT_TRUE(operators.sink->is_finished());
    EXPECT_TRUE(operators.source->is_finished());
}

TEST(AIProjectOperatorTest, SourceEarlyFinishDropsPendingSinkChunkAndUnblocksSink) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(10, 1)));

    auto pending = make_chunk(20, 1);
    std::weak_ptr<Chunk> pending_lifetime = pending;
    ASSERT_OK(operators.sink->push_chunk(&state, pending));
    pending.reset();
    ASSERT_FALSE(pending_lifetime.expired());
    ASSERT_FALSE(operators.sink->need_input());

    ASSERT_OK(operators.source->set_finished(&state));
    EXPECT_EQ(0, processor->input_buffer()->size());

    // The source-finished wakeup lets the sink observe rejection, release its
    // private pending reference, and terminate without waiting for upstream EOS.
    EXPECT_FALSE(operators.sink->need_input());
    EXPECT_TRUE(pending_lifetime.expired());
    EXPECT_TRUE(operators.sink->is_finished());
    EXPECT_TRUE(operators.source->is_finished());
    EXPECT_FALSE(operators.sink->pending_finish());
}

TEST(AIProjectOperatorTest, ForcedSinkFinishDropsPendingChunkWithoutReadmittingIt) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(10, 1)));

    auto pending = make_chunk(20, 1);
    std::weak_ptr<Chunk> pending_lifetime = pending;
    ASSERT_OK(operators.sink->push_chunk(&state, pending));
    pending.reset();
    ASSERT_FALSE(pending_lifetime.expired());

    // Free the buffer slot without polling need_input(). A forced close must
    // discard its private pending chunk instead of treating this as another
    // ordinary scheduling retry.
    ChunkPtr dequeued;
    auto got = processor->input_buffer()->try_get(0, &dequeued);
    ASSERT_TRUE(got.ok()) << got.status();
    ASSERT_TRUE(got.value());
    ASSERT_EQ(0, processor->input_buffer()->size());

    ASSERT_OK(operators.sink->set_finished(&state));
    EXPECT_EQ(0, processor->input_buffer()->size());
    EXPECT_TRUE(pending_lifetime.expired());
    EXPECT_TRUE(operators.sink->is_finished());
    auto lane_finished = processor->input_buffer()->lane_finished(0);
    ASSERT_TRUE(lane_finished.ok()) << lane_finished.status();
    EXPECT_TRUE(lane_finished.value());
}

TEST(AIProjectOperatorTest, ForcedSinkCancellationDropsPendingChunkWithoutReadmittingIt) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(10, 1)));

    auto pending = make_chunk(20, 1);
    std::weak_ptr<Chunk> pending_lifetime = pending;
    ASSERT_OK(operators.sink->push_chunk(&state, pending));
    pending.reset();
    ASSERT_FALSE(pending_lifetime.expired());

    ChunkPtr dequeued;
    auto got = processor->input_buffer()->try_get(0, &dequeued);
    ASSERT_TRUE(got.ok()) << got.status();
    ASSERT_TRUE(got.value());
    ASSERT_EQ(0, processor->input_buffer()->size());

    ASSERT_OK(operators.sink->set_cancelled(&state));
    EXPECT_EQ(0, processor->input_buffer()->size());
    EXPECT_TRUE(pending_lifetime.expired());
    EXPECT_TRUE(operators.sink->is_finished());
    auto lane_finished = processor->input_buffer()->lane_finished(0);
    ASSERT_TRUE(lane_finished.ok()) << lane_finished.status();
    EXPECT_TRUE(lane_finished.value());
}

TEST(AIProjectOperatorTest, SourceFinishPreservesCleanupFailureAcrossRetries) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);
    RuntimeState state;
    ASSERT_OK(processor->configure(1));

    // An out-of-range driver sequence deterministically makes processor
    // cleanup fail without test-only production hooks. This also models the
    // cleanup path after an operator failed validation during prepare().
    auto source = source_factory.create(1, 1);
    const Status first = source->set_finished(&state);
    ASSERT_TRUE(first.is_invalid_argument()) << first;
    const Status second = source->set_finished(&state);
    EXPECT_TRUE(second.is_invalid_argument()) << second;
    EXPECT_EQ(first.to_string(), second.to_string());
}

TEST(AIProjectOperatorTest, SourceFactoryPreservesBucketAndSkewProperties) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    TBucketProperty bucket;
    bucket.__set_bucket_func(TBucketFunction::MURMUR3_X86_32);
    bucket.__set_bucket_num(17);
    std::vector<TBucketProperty> bucket_properties{bucket};

    source_factory.set_bucket_properties(bucket_properties);
    source_factory.set_skewed(true);

    ASSERT_EQ(bucket_properties, source_factory.get_bucket_properties());
    EXPECT_TRUE(source_factory.is_skewed());
}

TEST(AIProjectOperatorTest, SourcePullAdvancesOnlyOneSubchunkAndBlocksForItsCallbacks) {
    auto buffer = AIChunkBuffer::create(4, 32 * kMiB);
    ASSERT_TRUE(buffer.ok()) << buffer.status();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    AIRuntimeConfig config;
    config.sub_chunk_size = 2;
    config.on_error = "ignore";
    auto processor_or = AIProjectProcessor::create(std::move(buffer).value(), std::make_shared<TestProjection>(true),
                                                   submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();

    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);
    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);

    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(0, 3)));
    ASSERT_OK(operators.sink->set_finishing(&state));
    ASSERT_TRUE(operators.source->has_output());

    auto waiting = operators.source->pull_chunk(&state);
    ASSERT_TRUE(waiting.ok()) << waiting.status();
    EXPECT_EQ(nullptr, waiting.value());
    EXPECT_EQ(2, submitter->pending_count());
    EXPECT_EQ(2, submitter->submitted_count());
    EXPECT_FALSE(operators.source->has_output());

    submitter->complete_all();
    EXPECT_TRUE(operators.source->has_output());
    auto first = operators.source->pull_chunk(&state);
    ASSERT_TRUE(first.ok()) << first.status();
    ASSERT_NE(nullptr, first.value());
    EXPECT_EQ(2, first.value()->num_rows());
    EXPECT_EQ(0, first_id(first.value()));

    // The remaining row is processable, but it is not submitted until the
    // next pull quantum.
    EXPECT_TRUE(operators.source->has_output());
    EXPECT_EQ(2, submitter->submitted_count());
    auto second_wait = operators.source->pull_chunk(&state);
    ASSERT_TRUE(second_wait.ok()) << second_wait.status();
    EXPECT_EQ(nullptr, second_wait.value());
    EXPECT_EQ(1, submitter->pending_count());
    EXPECT_EQ(3, submitter->submitted_count());
    EXPECT_FALSE(operators.source->has_output());

    submitter->complete_all();
    auto second = operators.source->pull_chunk(&state);
    ASSERT_TRUE(second.ok()) << second.status();
    ASSERT_NE(nullptr, second.value());
    EXPECT_EQ(1, second.value()->num_rows());
    EXPECT_EQ(2, first_id(second.value()));
}

TEST(AIProjectOperatorTest, PendingFinishIsAPureLifetimeCheckAndDoesNotRetryPendingInput) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    RuntimeState state;
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(10, 1)));
    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(20, 1)));
    ASSERT_OK(operators.sink->set_finishing(&state));
    ASSERT_EQ(1, processor->input_buffer()->size());

    for (int i = 0; i < 16; ++i) {
        (void)operators.sink->pending_finish();
        (void)operators.source->pending_finish();
    }
    EXPECT_EQ(1, processor->input_buffer()->size());

    auto first = operators.source->pull_chunk(&state);
    ASSERT_TRUE(first.ok()) << first.status();
    ASSERT_NE(nullptr, first.value());
    EXPECT_EQ(10, first_id(first.value()));
    EXPECT_EQ(0, processor->input_buffer()->size());

    // Only the ordinary scheduling predicate is allowed to retry the pending
    // chunk. pending_finish() must never do work or fire callbacks.
    EXPECT_FALSE(operators.sink->need_input());
    EXPECT_EQ(1, processor->input_buffer()->size());
}

TEST(AIProjectOperatorTest, PrepareWithoutEventSchedulerDoesNotRequireObservers) {
    auto processor = make_processor(1, false);
    ASSERT_NE(nullptr, processor);
    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);

    RuntimeState state;
    state.set_enable_event_scheduler(false);
    auto sink = sink_factory.create(1, 0);
    auto source = source_factory.create(1, 0);

    // Poller-mode drivers do not own PipelineObservers. prepare() must not try
    // to attach a null observer to either the buffer or callback observable.
    EXPECT_OK(sink->prepare(&state));
    EXPECT_OK(source->prepare(&state));
}

TEST(AIProjectOperatorTest, SourcePendingFinishBarrierCompletionWakesEventScheduler) {
    auto buffer = AIChunkBuffer::create(1, 32 * kMiB);
    ASSERT_TRUE(buffer.ok()) << buffer.status();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    AIRuntimeConfig config;
    config.sub_chunk_size = 64;
    config.on_error = "ignore";
    auto processor_or = AIProjectProcessor::create(std::move(buffer).value(), std::make_shared<TestProjection>(true),
                                                   submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();

    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);

    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(0, 1)));
    ASSERT_OK(operators.sink->set_finishing(&state));
    auto waiting = operators.source->pull_chunk(&state);
    ASSERT_TRUE(waiting.ok()) << waiting.status();
    EXPECT_EQ(nullptr, waiting.value());
    ASSERT_EQ(1, submitter->pending_count());

    ASSERT_OK(operators.source->set_finished(&state));
    ASSERT_TRUE(operators.source->pending_finish());
    const size_t wakeups_before_barrier_completion = operators.source_observer->source_wakeups;

    // Once the last callback leaves the lifetime barrier, the source driver
    // must be woken to re-check PENDING_FINISH. It is already locally
    // finished, so no ordinary output-ready notification can provide this.
    submitter->complete_all();
    EXPECT_FALSE(operators.source->pending_finish());
    EXPECT_GT(operators.source_observer->source_wakeups, wakeups_before_barrier_completion);
}

TEST(AIProjectOperatorTest, SinkPendingFinishDoesNotWaitForSourceCallbacks) {
    auto buffer = AIChunkBuffer::create(1, 32 * kMiB);
    ASSERT_TRUE(buffer.ok()) << buffer.status();
    auto submitter = std::make_shared<ManualTaskSubmitter>();
    AIRuntimeConfig config;
    config.sub_chunk_size = 64;
    config.on_error = "ignore";
    auto processor_or = AIProjectProcessor::create(std::move(buffer).value(), std::make_shared<TestProjection>(true),
                                                   submitter, std::move(config));
    ASSERT_TRUE(processor_or.ok()) << processor_or.status();
    auto processor = std::move(processor_or).value();

    AISinkOperatorFactory sink_factory(1, 7, processor);
    AISourceOperatorFactory source_factory(2, 7, processor);
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    auto operators = create_operator_pair(&sink_factory, &source_factory, &state);

    ASSERT_OK(operators.sink->push_chunk(&state, make_chunk(0, 1)));
    ASSERT_OK(operators.sink->set_finishing(&state));
    auto waiting = operators.source->pull_chunk(&state);
    ASSERT_TRUE(waiting.ok()) << waiting.status();
    EXPECT_EQ(nullptr, waiting.value());
    ASSERT_EQ(1, submitter->pending_count());
    ASSERT_TRUE(operators.source->pending_finish());

    // Async callbacks retain the shared processor and are guarded by the
    // source operator's lifetime barrier; they never retain the sink.
    EXPECT_FALSE(operators.sink->pending_finish());

    submitter->complete_all();
    auto output = operators.source->pull_chunk(&state);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_NE(nullptr, output.value());
    ASSERT_OK(operators.source->set_finished(&state));
}

} // namespace
} // namespace starrocks::pipeline
