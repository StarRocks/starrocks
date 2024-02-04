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

#include "exec/pipeline/sink/sink_io_buffer.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

// This is a mock test for synchronization between SinkIOBuffer and its underlying execution queue.
// Query-related context (including SinkIOBuffer) would only be destroyed after SinkIOBuffer becomes finished.
// Although we do not guarantee SinkIOBuffer outlives execution queue, we can still avoid use-after-free problem by
// skipping stop task in consumer thread.

namespace {

class MockSinkIOBuffer : public SinkIOBuffer {
public:
    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) {}

    void _process_chunk(ChunkPtr chunk) override {
        // handle this chunk
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
};

TEST(SinkIOBufferTest, test_basic) {
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    {
        ExecEnv* env = ExecEnv::GetInstance();
        RuntimeState state(env);

        ASSERT_OK(sink_buffer->prepare(&state, nullptr));

        auto chunk = std::make_shared<Chunk>();
        ASSERT_OK(sink_buffer->append_chunk(&state, chunk));
        ASSERT_OK(sink_buffer->append_chunk(&state, chunk));
        ASSERT_OK(sink_buffer->set_finishing());

        // wait until consumer thread finished all non-stop tasks
        while (!sink_buffer->is_finished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    sink_buffer.reset();
}
} // namespace

} // namespace starrocks::pipeline
