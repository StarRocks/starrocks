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

#include "exec/pipeline/spill_process_channel.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

namespace starrocks {

namespace {

// A task that counts how many times it runs, then reports EOF.
SpillProcessTask make_counting_task(std::shared_ptr<std::atomic_int32_t> runs) {
    return SpillProcessTask([runs]() -> StatusOr<ChunkPtr> {
        runs->fetch_add(1);
        return Status::EndOfFile("eos");
    });
}

} // namespace

// Cancel close: a cancelled sink calls set_finishing() without enqueuing a final task, so a data task (e.g. the
// sorter's chunk iterator, which reads sink-owned state that may already be freed) is left at the tail of the
// queue. close() must drop it without running it.
TEST(SpillProcessChannelTest, close_after_cancel_does_not_run_trailing_data_task) {
    SpillProcessChannel channel;

    auto data_runs = std::make_shared<std::atomic_int32_t>(0);
    channel.add_spill_task(make_counting_task(data_runs));
    channel.add_spill_task(make_counting_task(data_runs));
    channel.set_finishing();
    ASSERT_TRUE(channel.has_task());

    channel.close();

    ASSERT_EQ(data_runs->load(), 0);
    ASSERT_FALSE(channel.has_task());
    ASSERT_TRUE(channel.is_finished());
}

// Orderly close with tasks still queued: only the terminal handoff enqueued by add_last_task runs (it carries
// the sink's completion callback / context unref); the data tasks queued before it are dropped.
TEST(SpillProcessChannelTest, close_runs_only_final_task) {
    SpillProcessChannel channel;

    auto data_runs = std::make_shared<std::atomic_int32_t>(0);
    auto final_runs = std::make_shared<std::atomic_int32_t>(0);
    channel.add_spill_task(make_counting_task(data_runs));
    channel.add_last_task(make_counting_task(final_runs));
    ASSERT_TRUE(channel.is_finishing());

    channel.close();

    ASSERT_EQ(data_runs->load(), 0);
    ASSERT_EQ(final_runs->load(), 1);
    ASSERT_FALSE(channel.has_task());
    ASSERT_TRUE(channel.is_finished());
}

} // namespace starrocks
