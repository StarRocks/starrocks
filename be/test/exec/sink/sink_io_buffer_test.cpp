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
bthread::ExecutionQueueId<ChunkPtr> _execq_id;
std::promise<void> _promise;

class MockSinkIOBuffer : public SinkIOBuffer {
public:
    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) {}

    static int execute_io_task(void* meta, bthread::TaskIterator<ChunkPtr>& iter) {
        if (iter.is_queue_stopped()) {
            // block until SinkIOBuffer is destroyed
            _promise.get_future().wait();
        }

        if (iter.is_queue_stopped()) { // skip stop task
            return 0;
        }

        auto* sink_io_buffer = static_cast<MockSinkIOBuffer*>(meta);
        // calling dummy() causes use-after-free if we do not skip stop task
        sink_io_buffer->dummy();
        for (; iter; ++iter) {
            sink_io_buffer->_process_chunk(iter);
        }
        return 0;
    }

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override {
        int ret =
                bthread::execution_queue_start<ChunkPtr>(&_execq_id, nullptr, &MockSinkIOBuffer::execute_io_task, this);
        _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>(_execq_id);
        EXPECT_TRUE(ret == 0);
        if (ret != 0) {
            return Status::InternalError("start execution queue error");
        }
        return Status::OK();
    }

    void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override {
        DeferOp op([&]() {
            --_num_pending_chunks;
            DCHECK(_num_pending_chunks >= 0);
        });

        // close is already done, just skip
        if (_is_finished) {
            return;
        }

        // cancelling has happened but close is not invoked
        if (_is_cancelled && !_is_finished) {
            if (_num_pending_chunks == 1) {
                close(_state);
            }
            return;
        }

        const auto& chunk = *iter;
        if (chunk == nullptr) {
            // this is the last chunk
            EXPECT_EQ(_num_pending_chunks, 1);
            close(_state);
            return;
        }

        // handle this chunk
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ALWAYS_NOINLINE void dummy() { std::cout << _num_pending_chunks << std::endl; }
};

TEST(SinkIOBufferTest, test_basic) {
    {
        auto sink_buffer = std::make_unique<MockSinkIOBuffer>(10);
        ASSERT_OK(sink_buffer->prepare(nullptr, nullptr));

        auto chunk = std::make_shared<Chunk>();
        ASSERT_OK(sink_buffer->append_chunk(nullptr, chunk));
        ASSERT_OK(sink_buffer->append_chunk(nullptr, chunk));
        ASSERT_OK(sink_buffer->append_chunk(nullptr, nullptr)); // append close marker

        // wait until consumer thread finished all non-stop tasks
        while (!sink_buffer->is_finished()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    {
        // after sink buffer is destroyed, signal the consumer thread to execute stop task
        _promise.set_value();
        // wait until execution queue is destroyed
        int r = bthread::execution_queue_join(_execq_id);
        ASSERT_EQ(r, 0);
    }
}
} // namespace

} // namespace starrocks::pipeline
