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

#include "exec/pipeline/ai/ai_chunk_buffer.h"

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "exec_primitive/pipeline/primitives/pipeline_observer.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

constexpr size_t kMiB = 1024UL * 1024;

ChunkPtr make_chunk(int32_t value, size_t rows = 1) {
    auto column = Int32Column::create();
    for (size_t index = 0; index < rows; ++index) {
        column->append(value);
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(column), 0);
    return chunk;
}

int32_t chunk_value(const ChunkPtr& chunk) {
    return chunk->get_column_by_index(0)->get(0).get_int32();
}

std::shared_ptr<AIChunkBuffer> create_buffer(int64_t capacity, int64_t max_retained_bytes) {
    auto result = AIChunkBuffer::create(capacity, max_retained_bytes);
    EXPECT_TRUE(result.ok()) << result.status();
    if (!result.ok()) return nullptr;
    return std::move(result).value();
}

bool put(const std::shared_ptr<AIChunkBuffer>& buffer, int32_t lane, ChunkPtr chunk) {
    auto result = buffer->try_put(lane, std::move(chunk));
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() && result.value();
}

ChunkPtr get(const std::shared_ptr<AIChunkBuffer>& buffer, int32_t lane) {
    ChunkPtr chunk;
    auto result = buffer->try_get(lane, &chunk);
    EXPECT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result.ok() && result.value());
    return chunk;
}

class CountingObserver final : public PipelineObserver {
public:
    explicit CountingObserver(std::function<void()> on_trigger = {}) : _on_trigger(std::move(on_trigger)) {}

    void source_trigger() override {
        ++source_count;
        if (_on_trigger) _on_trigger();
    }
    void sink_trigger() override {
        ++sink_count;
        if (_on_trigger) _on_trigger();
    }
    void cancel_trigger() override { ++cancel_count; }
    void all_trigger() override {
        ++source_count;
        ++sink_count;
    }
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "AIChunkBufferTestObserver"; }

    std::atomic<int> source_count = 0;
    std::atomic<int> sink_count = 0;
    std::atomic<int> cancel_count = 0;

private:
    std::function<void()> _on_trigger;
};

TEST(AIChunkBufferTest, ComputesOverflowSafeCapacityAndMemoryDefaults) {
    auto capacity_one = AIChunkBuffer::capacity_for_dop(1);
    ASSERT_OK(capacity_one.status());
    EXPECT_EQ(12, capacity_one.value());
    auto capacity_two = AIChunkBuffer::capacity_for_dop(2);
    ASSERT_OK(capacity_two.status());
    EXPECT_EQ(12, capacity_two.value());
    auto capacity_three = AIChunkBuffer::capacity_for_dop(3);
    ASSERT_OK(capacity_three.status());
    EXPECT_EQ(18, capacity_three.value());
    EXPECT_FALSE(AIChunkBuffer::capacity_for_dop(0).ok());
    EXPECT_FALSE(AIChunkBuffer::capacity_for_dop(-1).ok());
    EXPECT_FALSE(AIChunkBuffer::capacity_for_dop(std::numeric_limits<int64_t>::max()).ok());

    auto unknown_limit = AIChunkBuffer::memory_limit_for_query(0);
    ASSERT_OK(unknown_limit.status());
    EXPECT_EQ(32 * kMiB, unknown_limit.value());
    auto unlimited_limit = AIChunkBuffer::memory_limit_for_query(-1);
    ASSERT_OK(unlimited_limit.status());
    EXPECT_EQ(32 * kMiB, unlimited_limit.value());
    auto minimum_sentinel_limit = AIChunkBuffer::memory_limit_for_query(std::numeric_limits<int64_t>::min());
    ASSERT_OK(minimum_sentinel_limit.status());
    EXPECT_EQ(32 * kMiB, minimum_sentinel_limit.value());
    auto minimum_limit = AIChunkBuffer::memory_limit_for_query(1);
    ASSERT_OK(minimum_limit.status());
    EXPECT_EQ(4 * kMiB, minimum_limit.value());
    auto proportional_limit = AIChunkBuffer::memory_limit_for_query(1'000'000'000);
    ASSERT_OK(proportional_limit.status());
    EXPECT_EQ(20'000'000, proportional_limit.value());
    auto maximum_limit = AIChunkBuffer::memory_limit_for_query(std::numeric_limits<int64_t>::max());
    ASSERT_OK(maximum_limit.status());
    EXPECT_EQ(256 * kMiB, maximum_limit.value());

    EXPECT_FALSE(AIChunkBuffer::create(0, 1).ok());
    EXPECT_FALSE(AIChunkBuffer::create(-1, 1).ok());
    EXPECT_FALSE(AIChunkBuffer::create(1, -1).ok());
    EXPECT_TRUE(AIChunkBuffer::create(1, 0).ok()) << "zero is a valid soft byte limit";
}

TEST(AIChunkBufferTest, ConfigureIsIdempotentAndRejectsConflictingOrInvalidLanes) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);

    auto unconfigured_put = buffer->try_put(0, make_chunk(1));
    EXPECT_FALSE(unconfigured_put.ok());
    EXPECT_FALSE(buffer->lane_has_chunk(0).ok());
    EXPECT_FALSE(buffer->lane_source_finished(0).ok());
    EXPECT_FALSE(buffer->configure(0).ok());
    EXPECT_FALSE(buffer->configure(-1).ok());
    ASSERT_OK(buffer->configure(2));
    EXPECT_OK(buffer->configure(2));
    EXPECT_FALSE(buffer->configure(3).ok());

    EXPECT_FALSE(buffer->try_put(-1, make_chunk(1)).ok());
    EXPECT_FALSE(buffer->try_put(2, make_chunk(1)).ok());
    EXPECT_FALSE(buffer->lane_has_chunk(-1).ok());
    EXPECT_FALSE(buffer->lane_has_chunk(2).ok());
    EXPECT_FALSE(buffer->lane_source_finished(-1).ok());
    EXPECT_FALSE(buffer->lane_source_finished(2).ok());
    ChunkPtr output;
    EXPECT_FALSE(buffer->try_get(-1, &output).ok());
    EXPECT_FALSE(buffer->try_get(2, &output).ok());
    EXPECT_FALSE(buffer->try_get(0, nullptr).ok());
    EXPECT_FALSE(buffer->attach_sink_observer(2, nullptr, nullptr).ok());
}

TEST(AIChunkBufferTest, ReportsLaneQueueAndSourceLifecycleWithoutMutation) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));

    auto lane_zero_has_chunk = buffer->lane_has_chunk(0);
    auto lane_zero_source_finished = buffer->lane_source_finished(0);
    ASSERT_OK(lane_zero_has_chunk.status());
    ASSERT_OK(lane_zero_source_finished.status());
    EXPECT_FALSE(lane_zero_has_chunk.value());
    EXPECT_FALSE(lane_zero_source_finished.value());

    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
    lane_zero_has_chunk = buffer->lane_has_chunk(0);
    ASSERT_OK(lane_zero_has_chunk.status());
    EXPECT_TRUE(lane_zero_has_chunk.value());
    EXPECT_EQ(1, buffer->size());

    ASSERT_OK(buffer->set_source_finished(0));
    lane_zero_has_chunk = buffer->lane_has_chunk(0);
    lane_zero_source_finished = buffer->lane_source_finished(0);
    ASSERT_OK(lane_zero_has_chunk.status());
    ASSERT_OK(lane_zero_source_finished.status());
    EXPECT_FALSE(lane_zero_has_chunk.value());
    EXPECT_TRUE(lane_zero_source_finished.value());
    EXPECT_EQ(0, buffer->size());

    buffer->close();
    auto lane_one_has_chunk = buffer->lane_has_chunk(1);
    auto lane_one_source_finished = buffer->lane_source_finished(1);
    ASSERT_OK(lane_one_has_chunk.status());
    ASSERT_OK(lane_one_source_finished.status());
    EXPECT_FALSE(lane_one_has_chunk.value());
    EXPECT_TRUE(lane_one_source_finished.value());
}

TEST(AIChunkBufferTest, PreservesStrictFifoAndNeverStealsAcrossLanes) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));

    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
    EXPECT_TRUE(put(buffer, 0, make_chunk(2)));
    EXPECT_TRUE(put(buffer, 1, make_chunk(10)));
    EXPECT_EQ(3, buffer->size());

    EXPECT_EQ(10, chunk_value(get(buffer, 1)));
    ChunkPtr no_chunk;
    auto empty_lane = buffer->try_get(1, &no_chunk);
    ASSERT_OK(empty_lane.status());
    EXPECT_FALSE(empty_lane.value()) << "lane 1 must not steal queued chunks from lane 0";

    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    EXPECT_EQ(2, chunk_value(get(buffer, 0)));
    EXPECT_EQ(0, buffer->size());
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, EnforcesGlobalChunkAndRetainedByteLimitsAcrossLanes) {
    auto capacity_buffer = create_buffer(2, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, capacity_buffer);
    ASSERT_OK(capacity_buffer->configure(2));
    EXPECT_TRUE(put(capacity_buffer, 0, make_chunk(1)));
    EXPECT_TRUE(put(capacity_buffer, 1, make_chunk(2)));
    ChunkPtr capacity_waiter = make_chunk(3);
    auto capacity_rejected = capacity_buffer->try_put(0, capacity_waiter);
    ASSERT_OK(capacity_rejected.status());
    EXPECT_FALSE(capacity_rejected.value());
    EXPECT_EQ(2, capacity_buffer->size());

    EXPECT_EQ(1, chunk_value(get(capacity_buffer, 0)));
    EXPECT_TRUE(put(capacity_buffer, 0, capacity_waiter));
    EXPECT_EQ(2, chunk_value(get(capacity_buffer, 1)));
    EXPECT_EQ(3, chunk_value(get(capacity_buffer, 0)));
    EXPECT_EQ(0, capacity_buffer->retained_bytes());

    ChunkPtr first = make_chunk(11, 16);
    ChunkPtr second = make_chunk(22, 16);
    const size_t first_bytes = first->memory_usage();
    ASSERT_GT(first_bytes, 0);
    auto memory_buffer = create_buffer(12, static_cast<int64_t>(first_bytes));
    ASSERT_NE(nullptr, memory_buffer);
    ASSERT_OK(memory_buffer->configure(2));
    EXPECT_TRUE(put(memory_buffer, 0, first));
    EXPECT_EQ(first_bytes, memory_buffer->retained_bytes());
    auto memory_rejected = memory_buffer->try_put(1, second);
    ASSERT_OK(memory_rejected.status());
    EXPECT_FALSE(memory_rejected.value());
    EXPECT_EQ(first_bytes, memory_buffer->retained_bytes());

    EXPECT_EQ(11, chunk_value(get(memory_buffer, 0)));
    EXPECT_TRUE(put(memory_buffer, 1, second));
    EXPECT_EQ(22, chunk_value(get(memory_buffer, 1)));
    EXPECT_EQ(0, memory_buffer->retained_bytes());
}

TEST(AIChunkBufferTest, BackpressureDoesNotConsumeCallerOwnershipEvenWhenMoved) {
    auto buffer = create_buffer(1, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(1));
    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));

    ChunkPtr pending = make_chunk(2);
    Chunk* pending_address = pending.get();
    auto rejected = buffer->try_put(0, std::move(pending));
    ASSERT_OK(rejected.status());
    EXPECT_FALSE(rejected.value());
    ASSERT_NE(nullptr, pending);
    EXPECT_EQ(pending_address, pending.get());
    EXPECT_EQ(2, chunk_value(pending));

    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    EXPECT_TRUE(put(buffer, 0, pending));
    EXPECT_EQ(2, chunk_value(get(buffer, 0)));
}

TEST(AIChunkBufferTest, ByteReservationCannotBeStolenWhenChunkSlotsRemain) {
    ChunkPtr initial = make_chunk(1, 32);
    ChunkPtr waiting_one = make_chunk(2, 32);
    ChunkPtr waiting_two = make_chunk(3, 32);
    const size_t chunk_bytes = initial->memory_usage();
    ASSERT_EQ(chunk_bytes, waiting_one->memory_usage());
    ASSERT_EQ(chunk_bytes, waiting_two->memory_usage());

    auto buffer = create_buffer(4, static_cast<int64_t>(chunk_bytes));
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(3));
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    CountingObserver sink_one;
    CountingObserver sink_two;
    ASSERT_OK(buffer->attach_sink_observer(1, &state, &sink_one));
    ASSERT_OK(buffer->attach_sink_observer(2, &state, &sink_two));

    EXPECT_TRUE(put(buffer, 0, initial));
    auto blocked_one = buffer->try_put(1, waiting_one);
    auto blocked_two = buffer->try_put(2, waiting_two);
    ASSERT_OK(blocked_one.status());
    ASSERT_OK(blocked_two.status());
    EXPECT_FALSE(blocked_one.value());
    EXPECT_FALSE(blocked_two.value());

    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    EXPECT_EQ(1, sink_one.sink_count);
    EXPECT_EQ(0, sink_two.sink_count);
    auto cannot_steal_bytes = buffer->try_put(2, waiting_two);
    ASSERT_OK(cannot_steal_bytes.status());
    ASSERT_FALSE(cannot_steal_bytes.value())
            << "free chunk slots must not bypass bytes reserved for the selected waiter";

    ASSERT_TRUE(put(buffer, 1, waiting_one));
    EXPECT_EQ(chunk_bytes, buffer->retained_bytes());
    EXPECT_EQ(2, chunk_value(get(buffer, 1)));
    EXPECT_EQ(1, sink_two.sink_count);
    ASSERT_TRUE(put(buffer, 2, waiting_two));
    EXPECT_EQ(3, chunk_value(get(buffer, 2)));
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, AllowsOneOversizedChunkOnlyWhenGloballyEmpty) {
    ChunkPtr wide = make_chunk(1, 4096);
    const size_t wide_bytes = wide->memory_usage();
    ASSERT_GT(wide_bytes, 0);
    auto buffer = create_buffer(12, 0);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));

    EXPECT_TRUE(put(buffer, 0, wide));
    EXPECT_EQ(wide_bytes, buffer->retained_bytes());
    ChunkPtr blocked_chunk = make_chunk(2);
    auto blocked = buffer->try_put(1, blocked_chunk);
    ASSERT_OK(blocked.status());
    EXPECT_FALSE(blocked.value());

    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    EXPECT_EQ(0, buffer->retained_bytes());
    EXPECT_TRUE(put(buffer, 1, blocked_chunk));
    EXPECT_EQ(2, chunk_value(get(buffer, 1)));
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, SinkEosIsLaneLocalAndDrainsBeforeSourceFinishes) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));
    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
    EXPECT_TRUE(put(buffer, 1, make_chunk(10)));

    ASSERT_OK(buffer->set_sink_eos(0));
    EXPECT_OK(buffer->set_sink_eos(0));
    auto lane_zero_finished = buffer->lane_finished(0);
    ASSERT_OK(lane_zero_finished.status());
    EXPECT_FALSE(lane_zero_finished.value());
    EXPECT_FALSE(buffer->try_put(0, make_chunk(2)).ok());
    EXPECT_TRUE(put(buffer, 1, make_chunk(11)));

    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    lane_zero_finished = buffer->lane_finished(0);
    ASSERT_OK(lane_zero_finished.status());
    EXPECT_TRUE(lane_zero_finished.value());
    auto lane_one_finished = buffer->lane_finished(1);
    ASSERT_OK(lane_one_finished.status());
    EXPECT_FALSE(lane_one_finished.value());
    EXPECT_EQ(10, chunk_value(get(buffer, 1)));
    EXPECT_EQ(11, chunk_value(get(buffer, 1)));
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, SinkEosDoesNotNotifyAnAlreadyFinishedSource) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(1));
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    CountingObserver source;
    ASSERT_OK(buffer->attach_source_observer(0, &state, &source));

    ASSERT_OK(buffer->set_source_finished(0));
    EXPECT_EQ(0, source.source_count);
    ASSERT_OK(buffer->set_sink_eos(0));
    EXPECT_OK(buffer->set_sink_eos(0));
    EXPECT_EQ(0, source.source_count);

    std::lock_guard lock(buffer->_mutex);
    EXPECT_TRUE(buffer->_lanes[0]->sink_eos);
    EXPECT_FALSE(buffer->_lanes[0]->sink_waiting);
    EXPECT_FALSE(buffer->_lanes[0]->wake_pending);
    EXPECT_EQ(0, buffer->_lanes[0]->waiting_bytes);
}

TEST(AIChunkBufferTest, SourceFinishDoesNotNotifyAnAlreadyFinishedSink) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(1));
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    CountingObserver sink;
    ASSERT_OK(buffer->attach_sink_observer(0, &state, &sink));

    ASSERT_OK(buffer->set_sink_eos(0));
    EXPECT_EQ(0, sink.sink_count);
    ASSERT_OK(buffer->set_source_finished(0));
    EXPECT_OK(buffer->set_source_finished(0));
    EXPECT_EQ(0, sink.sink_count);
}

TEST(AIChunkBufferTest, SourceEarlyFinishDropsOnlyItsLaneAndAllFinishedRejectsNewPuts) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));
    ChunkPtr lane_zero_first = make_chunk(1, 8);
    ChunkPtr lane_zero_second = make_chunk(2, 16);
    ChunkPtr lane_one = make_chunk(10, 32);
    const size_t lane_one_bytes = lane_one->memory_usage();
    EXPECT_TRUE(put(buffer, 0, lane_zero_first));
    EXPECT_TRUE(put(buffer, 0, lane_zero_second));
    EXPECT_TRUE(put(buffer, 1, lane_one));

    ASSERT_OK(buffer->set_source_finished(0));
    EXPECT_OK(buffer->set_source_finished(0));
    EXPECT_EQ(1, buffer->size());
    EXPECT_EQ(lane_one_bytes, buffer->retained_bytes());
    ChunkPtr output;
    auto lane_zero_empty = buffer->try_get(0, &output);
    ASSERT_OK(lane_zero_empty.status());
    EXPECT_FALSE(lane_zero_empty.value());
    auto lane_zero_put = buffer->try_put(0, make_chunk(3));
    ASSERT_OK(lane_zero_put.status());
    EXPECT_FALSE(lane_zero_put.value());

    EXPECT_TRUE(put(buffer, 1, make_chunk(11)));
    ASSERT_OK(buffer->set_source_finished(1));
    EXPECT_TRUE(buffer->all_sources_finished());
    EXPECT_EQ(0, buffer->size());
    EXPECT_EQ(0, buffer->retained_bytes());
    auto all_finished_put = buffer->try_put(1, make_chunk(12));
    ASSERT_OK(all_finished_put.status());
    EXPECT_FALSE(all_finished_put.value());
}

TEST(AIChunkBufferTest, NotifiesOnlyBlockedSinkLanesFairlyAndOutsideMutex) {
    auto buffer = create_buffer(1, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(3));
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    CountingObserver source_zero([&] { static_cast<void>(buffer->size()); });
    CountingObserver source_one([&] { static_cast<void>(buffer->size()); });
    CountingObserver source_two([&] { static_cast<void>(buffer->size()); });
    CountingObserver sink_zero([&] { static_cast<void>(buffer->retained_bytes()); });
    CountingObserver sink_one([&] { static_cast<void>(buffer->retained_bytes()); });
    CountingObserver sink_two([&] { static_cast<void>(buffer->retained_bytes()); });
    ASSERT_OK(buffer->attach_source_observer(0, &state, &source_zero));
    ASSERT_OK(buffer->attach_source_observer(1, &state, &source_one));
    ASSERT_OK(buffer->attach_source_observer(2, &state, &source_two));
    ASSERT_OK(buffer->attach_sink_observer(0, &state, &sink_zero));
    ASSERT_OK(buffer->attach_sink_observer(1, &state, &sink_one));
    ASSERT_OK(buffer->attach_sink_observer(2, &state, &sink_two));

    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
    EXPECT_EQ(1, source_zero.source_count);
    EXPECT_EQ(0, source_one.source_count);
    EXPECT_EQ(1, chunk_value(get(buffer, 0)));
    EXPECT_EQ(0, sink_zero.sink_count);
    EXPECT_EQ(0, sink_one.sink_count) << "a pop without blocked producers must not cause a wakeup herd";
    EXPECT_EQ(0, sink_two.sink_count);

    EXPECT_TRUE(put(buffer, 0, make_chunk(2)));
    ChunkPtr waiting_zero = make_chunk(3);
    ChunkPtr waiting_one = make_chunk(4);
    ChunkPtr waiting_two = make_chunk(5);
    auto zero_blocked = buffer->try_put(0, waiting_zero);
    auto one_blocked = buffer->try_put(1, waiting_one);
    ASSERT_OK(zero_blocked.status());
    ASSERT_OK(one_blocked.status());
    EXPECT_FALSE(zero_blocked.value());
    EXPECT_FALSE(one_blocked.value());

    EXPECT_EQ(2, chunk_value(get(buffer, 0)));
    EXPECT_EQ(1, sink_zero.sink_count);
    EXPECT_EQ(0, sink_one.sink_count) << "one release may wake only one blocked lane";
    EXPECT_EQ(0, sink_two.sink_count);
    auto one_cannot_steal = buffer->try_put(1, waiting_one);
    auto two_cannot_steal = buffer->try_put(2, waiting_two);
    ASSERT_OK(one_cannot_steal.status());
    ASSERT_OK(two_cannot_steal.status());
    ASSERT_FALSE(one_cannot_steal.value());
    ASSERT_FALSE(two_cannot_steal.value()) << "unreserved lanes must not steal the slot virtually reserved for lane 0";
    ASSERT_TRUE(put(buffer, 0, waiting_zero));

    EXPECT_EQ(3, chunk_value(get(buffer, 0)));
    EXPECT_EQ(1, sink_zero.sink_count);
    EXPECT_EQ(1, sink_one.sink_count) << "round-robin selection must advance to the next blocked lane";
    EXPECT_EQ(0, sink_two.sink_count);
    ASSERT_TRUE(put(buffer, 1, waiting_one));
    EXPECT_EQ(4, chunk_value(get(buffer, 1)));
    EXPECT_EQ(1, sink_zero.sink_count);
    EXPECT_EQ(1, sink_one.sink_count);
    EXPECT_EQ(1, sink_two.sink_count) << "round-robin selection must advance to the third blocked lane";
    ASSERT_TRUE(put(buffer, 2, waiting_two));
    EXPECT_EQ(5, chunk_value(get(buffer, 2)));
    EXPECT_EQ(1, sink_zero.sink_count);
    EXPECT_EQ(1, sink_one.sink_count);
    EXPECT_EQ(1, sink_two.sink_count) << "no waiter remains, so the pop must not notify any sink";

    EXPECT_TRUE(put(buffer, 0, make_chunk(6)));
    ChunkPtr early_finish_waiter = make_chunk(7);
    auto early_finish_blocked = buffer->try_put(1, early_finish_waiter);
    ASSERT_OK(early_finish_blocked.status());
    EXPECT_FALSE(early_finish_blocked.value());
    ASSERT_OK(buffer->set_source_finished(0));
    EXPECT_EQ(2, sink_zero.sink_count) << "source early-finish must wake its paired sink for terminal state";
    EXPECT_EQ(2, sink_one.sink_count) << "dropping a lane must wake only a real global-limit waiter";
    EXPECT_EQ(1, sink_two.sink_count);
    ASSERT_TRUE(put(buffer, 1, early_finish_waiter));

    ASSERT_OK(buffer->set_sink_eos(1));
    EXPECT_EQ(4, source_zero.source_count);
    EXPECT_EQ(3, source_one.source_count) << "sink EOS must wake only its source lane";

    buffer->close();
    EXPECT_EQ(5, source_zero.source_count);
    EXPECT_EQ(4, source_one.source_count);
    EXPECT_EQ(2, source_two.source_count);
    EXPECT_EQ(3, sink_zero.sink_count);
    EXPECT_EQ(3, sink_one.sink_count);
    EXPECT_EQ(2, sink_two.sink_count) << "close must wake every source and sink lane";
}

TEST(AIChunkBufferTest, BulkDropWakesEveryAdmissibleWaiterWithoutBroadcasting) {
    auto buffer = create_buffer(4, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(4));
    RuntimeState state;
    state.set_enable_event_scheduler(true);
    CountingObserver sink_zero;
    CountingObserver sink_one;
    CountingObserver sink_two;
    CountingObserver sink_three;
    ASSERT_OK(buffer->attach_sink_observer(0, &state, &sink_zero));
    ASSERT_OK(buffer->attach_sink_observer(1, &state, &sink_one));
    ASSERT_OK(buffer->attach_sink_observer(2, &state, &sink_two));
    ASSERT_OK(buffer->attach_sink_observer(3, &state, &sink_three));

    for (int32_t value = 0; value < 4; ++value) {
        EXPECT_TRUE(put(buffer, 0, make_chunk(value)));
    }
    ChunkPtr waiting_one = make_chunk(11);
    ChunkPtr waiting_two = make_chunk(22);
    ChunkPtr waiting_three = make_chunk(33);
    auto blocked_one = buffer->try_put(1, waiting_one);
    auto blocked_two = buffer->try_put(2, waiting_two);
    auto blocked_three = buffer->try_put(3, waiting_three);
    ASSERT_OK(blocked_one.status());
    ASSERT_OK(blocked_two.status());
    ASSERT_OK(blocked_three.status());
    EXPECT_FALSE(blocked_one.value());
    EXPECT_FALSE(blocked_two.value());
    EXPECT_FALSE(blocked_three.value());

    ASSERT_OK(buffer->set_source_finished(0));
    EXPECT_EQ(1, sink_zero.sink_count) << "the finished source must wake its paired sink for terminal state";
    EXPECT_EQ(1, sink_one.sink_count);
    EXPECT_EQ(1, sink_two.sink_count);
    EXPECT_EQ(1, sink_three.sink_count) << "all three released slots must remain work-conserving";

    EXPECT_TRUE(put(buffer, 1, waiting_one));
    EXPECT_TRUE(put(buffer, 2, waiting_two));
    EXPECT_TRUE(put(buffer, 3, waiting_three));
    EXPECT_EQ(11, chunk_value(get(buffer, 1)));
    EXPECT_EQ(22, chunk_value(get(buffer, 2)));
    EXPECT_EQ(33, chunk_value(get(buffer, 3)));
    EXPECT_EQ(0, buffer->size());
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, ClearsStaleWakeReservationsOnEosSourceFinishAndClose) {
    RuntimeState state;
    state.set_enable_event_scheduler(true);

    auto eos_buffer = create_buffer(1, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, eos_buffer);
    ASSERT_OK(eos_buffer->configure(2));
    CountingObserver eos_source_zero;
    CountingObserver eos_source_one;
    CountingObserver eos_sink_zero;
    CountingObserver eos_sink_one;
    ASSERT_OK(eos_buffer->attach_source_observer(0, &state, &eos_source_zero));
    ASSERT_OK(eos_buffer->attach_source_observer(1, &state, &eos_source_one));
    ASSERT_OK(eos_buffer->attach_sink_observer(0, &state, &eos_sink_zero));
    ASSERT_OK(eos_buffer->attach_sink_observer(1, &state, &eos_sink_one));
    EXPECT_TRUE(put(eos_buffer, 0, make_chunk(1)));
    ChunkPtr eos_waiting_zero = make_chunk(2);
    ChunkPtr eos_waiting_one = make_chunk(3);
    auto eos_blocked_zero = eos_buffer->try_put(0, eos_waiting_zero);
    auto eos_blocked_one = eos_buffer->try_put(1, eos_waiting_one);
    ASSERT_OK(eos_blocked_zero.status());
    ASSERT_OK(eos_blocked_one.status());
    EXPECT_FALSE(eos_blocked_zero.value());
    EXPECT_FALSE(eos_blocked_one.value());
    EXPECT_EQ(1, chunk_value(get(eos_buffer, 0)));
    EXPECT_EQ(1, eos_sink_zero.sink_count);
    EXPECT_EQ(0, eos_sink_one.sink_count);
    ASSERT_OK(eos_buffer->set_sink_eos(0));
    EXPECT_EQ(1, eos_sink_zero.sink_count);
    EXPECT_EQ(1, eos_sink_one.sink_count) << "EOS must hand a stale reservation to the next waiter";
    EXPECT_TRUE(put(eos_buffer, 1, eos_waiting_one));
    EXPECT_EQ(3, chunk_value(get(eos_buffer, 1)));

    auto finish_buffer = create_buffer(1, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, finish_buffer);
    ASSERT_OK(finish_buffer->configure(2));
    CountingObserver finish_sink_zero;
    CountingObserver finish_sink_one;
    ASSERT_OK(finish_buffer->attach_sink_observer(0, &state, &finish_sink_zero));
    ASSERT_OK(finish_buffer->attach_sink_observer(1, &state, &finish_sink_one));
    EXPECT_TRUE(put(finish_buffer, 0, make_chunk(10)));
    ChunkPtr finish_waiting_zero = make_chunk(20);
    ChunkPtr finish_waiting_one = make_chunk(30);
    auto finish_blocked_zero = finish_buffer->try_put(0, finish_waiting_zero);
    auto finish_blocked_one = finish_buffer->try_put(1, finish_waiting_one);
    ASSERT_OK(finish_blocked_zero.status());
    ASSERT_OK(finish_blocked_one.status());
    EXPECT_FALSE(finish_blocked_zero.value());
    EXPECT_FALSE(finish_blocked_one.value());
    EXPECT_EQ(10, chunk_value(get(finish_buffer, 0)));
    EXPECT_EQ(1, finish_sink_zero.sink_count);
    EXPECT_EQ(0, finish_sink_one.sink_count);
    ASSERT_OK(finish_buffer->set_source_finished(0));
    EXPECT_EQ(2, finish_sink_zero.sink_count) << "source early-finish must wake its paired sink";
    EXPECT_EQ(1, finish_sink_one.sink_count) << "source early-finish must hand a stale reservation to the next waiter";
    EXPECT_TRUE(put(finish_buffer, 1, finish_waiting_one));
    EXPECT_EQ(30, chunk_value(get(finish_buffer, 1)));

    auto close_buffer = create_buffer(1, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, close_buffer);
    ASSERT_OK(close_buffer->configure(2));
    CountingObserver close_source_zero;
    CountingObserver close_source_one;
    CountingObserver close_sink_zero;
    CountingObserver close_sink_one;
    ASSERT_OK(close_buffer->attach_source_observer(0, &state, &close_source_zero));
    ASSERT_OK(close_buffer->attach_source_observer(1, &state, &close_source_one));
    ASSERT_OK(close_buffer->attach_sink_observer(0, &state, &close_sink_zero));
    ASSERT_OK(close_buffer->attach_sink_observer(1, &state, &close_sink_one));
    EXPECT_TRUE(put(close_buffer, 0, make_chunk(100)));
    ChunkPtr close_waiting_zero = make_chunk(200);
    ChunkPtr close_waiting_one = make_chunk(300);
    auto close_blocked_zero = close_buffer->try_put(0, close_waiting_zero);
    auto close_blocked_one = close_buffer->try_put(1, close_waiting_one);
    ASSERT_OK(close_blocked_zero.status());
    ASSERT_OK(close_blocked_one.status());
    EXPECT_FALSE(close_blocked_zero.value());
    EXPECT_FALSE(close_blocked_one.value());
    EXPECT_EQ(100, chunk_value(get(close_buffer, 0)));
    EXPECT_EQ(1, close_sink_zero.sink_count);
    EXPECT_EQ(0, close_sink_one.sink_count);
    close_buffer->close();
    close_buffer->close();
    EXPECT_EQ(2, close_source_zero.source_count);
    EXPECT_EQ(1, close_source_one.source_count);
    EXPECT_EQ(2, close_sink_zero.sink_count);
    EXPECT_EQ(1, close_sink_one.sink_count);
    EXPECT_EQ(0, close_buffer->size());
    EXPECT_EQ(0, close_buffer->retained_bytes());
}

TEST(AIChunkBufferTest, ConcurrentMpmcMaintainsPerLaneFifoAndExactAccounting) {
    constexpr int32_t kLanes = 4;
    constexpr int32_t kChunksPerLane = 200;
    constexpr int64_t kCapacity = 4;
    auto buffer = create_buffer(kCapacity, std::numeric_limits<int64_t>::max());
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(kLanes));
    std::barrier start(kLanes * 2);
    std::atomic<bool> failed = false;
    std::atomic<bool> stop = false;
    std::atomic<bool> release_consumers = false;
    std::atomic<bool> observed_backpressure = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::vector<std::thread> threads;

    for (int32_t lane = 0; lane < kLanes; ++lane) {
        threads.emplace_back([&, lane] {
            start.arrive_and_wait();
            for (int32_t index = 0; index < kChunksPerLane; ++index) {
                ChunkPtr chunk = make_chunk(lane * 100'000 + index);
                while (!stop.load()) {
                    auto result = buffer->try_put(lane, chunk);
                    if (!result.ok()) {
                        failed.store(true);
                        stop.store(true);
                        release_consumers.store(true);
                        break;
                    }
                    if (result.value()) {
                        break;
                    }
                    observed_backpressure.store(true);
                    release_consumers.store(true);
                    if (std::chrono::steady_clock::now() >= deadline) {
                        failed.store(true);
                        stop.store(true);
                        break;
                    }
                    std::this_thread::yield();
                }
                if (stop.load()) {
                    break;
                }
            }
            if (!stop.load()) {
                Status status = buffer->set_sink_eos(lane);
                if (!status.ok()) {
                    failed.store(true);
                    stop.store(true);
                }
            }
            release_consumers.store(true);
        });
        threads.emplace_back([&, lane] {
            start.arrive_and_wait();
            while (!release_consumers.load() && !stop.load()) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    failed.store(true);
                    stop.store(true);
                    break;
                }
                std::this_thread::yield();
            }

            int32_t next = 0;
            while (next < kChunksPerLane && !stop.load()) {
                ChunkPtr chunk;
                auto result = buffer->try_get(lane, &chunk);
                if (!result.ok()) {
                    failed.store(true);
                    stop.store(true);
                    return;
                }
                if (!result.value()) {
                    auto finished = buffer->lane_finished(lane);
                    if (!finished.ok() || finished.value() || std::chrono::steady_clock::now() >= deadline) {
                        failed.store(true);
                        stop.store(true);
                        return;
                    }
                    std::this_thread::yield();
                    continue;
                }
                if (chunk_value(chunk) != lane * 100'000 + next) {
                    failed.store(true);
                    stop.store(true);
                    return;
                }
                ++next;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_TRUE(observed_backpressure);
    EXPECT_FALSE(failed);
    if (failed.load()) {
        buffer->close();
    }
    EXPECT_EQ(0, buffer->size());
    EXPECT_EQ(0, buffer->retained_bytes());
}

TEST(AIChunkBufferTest, PreconfiguresRetirementQueuesAndKeepsThemStableAcrossClose) {
    auto buffer = create_buffer(12, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    EXPECT_TRUE(buffer->_retirement_queues.empty());
    ASSERT_OK(buffer->configure(3));
    ASSERT_EQ(3, buffer->_retirement_queues.size());
    const auto* retirement_storage = buffer->_retirement_queues.data();
    for (const auto& queue : buffer->_retirement_queues) {
        EXPECT_TRUE(queue.empty());
    }

    EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
    EXPECT_TRUE(put(buffer, 1, make_chunk(2)));
    EXPECT_TRUE(put(buffer, 2, make_chunk(3)));
    buffer->close();
    EXPECT_EQ(retirement_storage, buffer->_retirement_queues.data());
    EXPECT_EQ(3, buffer->_retirement_queues.size());
    for (const auto& queue : buffer->_retirement_queues) {
        EXPECT_TRUE(queue.empty());
    }

    buffer->close();
    EXPECT_EQ(retirement_storage, buffer->_retirement_queues.data());
    EXPECT_EQ(3, buffer->_retirement_queues.size());
}

TEST(AIChunkBufferTest, ReserveWaitersUsesCallerPreallocatedNotificationStorage) {
    auto buffer = create_buffer(1, 32 * kMiB);
    ASSERT_NE(nullptr, buffer);
    ASSERT_OK(buffer->configure(2));
    AIChunkBuffer::NotificationList notifications;
    notifications.reserve(2);
    const auto* notification_storage = notifications.data();

    {
        std::lock_guard lock(buffer->_mutex);
        auto* waiter = buffer->_lanes[1].get();
        waiter->sink_waiting = true;
        waiter->waiting_bytes = 0;
        buffer->_reserve_waiters_locked(&notifications);
        EXPECT_TRUE(waiter->wake_pending);
    }

    ASSERT_EQ(1, notifications.size());
    EXPECT_EQ(notification_storage, notifications.data());
    EXPECT_GE(notifications.capacity(), buffer->_lanes.size());
}

TEST(AIChunkBufferTest, CloseAndDestructorReleaseEveryRetainedChunk) {
    std::weak_ptr<Chunk> destructor_chunk;
    {
        auto buffer = create_buffer(12, 32 * kMiB);
        ASSERT_NE(nullptr, buffer);
        ASSERT_OK(buffer->configure(3));
        EXPECT_TRUE(put(buffer, 0, make_chunk(1)));
        EXPECT_TRUE(put(buffer, 1, make_chunk(2)));
        EXPECT_TRUE(put(buffer, 2, make_chunk(3)));
        buffer->close();
        buffer->close();
        EXPECT_EQ(0, buffer->size());
        EXPECT_EQ(0, buffer->retained_bytes());
        EXPECT_TRUE(buffer->all_sources_finished());
        auto closed_put = buffer->try_put(0, make_chunk(4));
        ASSERT_OK(closed_put.status());
        EXPECT_FALSE(closed_put.value());
        ChunkPtr output;
        auto closed_get = buffer->try_get(0, &output);
        ASSERT_OK(closed_get.status());
        EXPECT_FALSE(closed_get.value());

        auto destructor_buffer = create_buffer(12, 32 * kMiB);
        ASSERT_NE(nullptr, destructor_buffer);
        ASSERT_OK(destructor_buffer->configure(1));
        ChunkPtr retained = make_chunk(5);
        destructor_chunk = retained;
        EXPECT_TRUE(put(destructor_buffer, 0, retained));
        retained.reset();
        EXPECT_FALSE(destructor_chunk.expired());
        destructor_buffer.reset();
    }
    EXPECT_TRUE(destructor_chunk.expired());
}

} // namespace
} // namespace starrocks::pipeline
