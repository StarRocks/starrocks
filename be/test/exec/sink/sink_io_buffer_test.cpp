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

#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "column/fixed_length_column.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

class MockSinkIOBuffer : public SinkIOBuffer {
public:
    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) {}

    void _add_chunk(const ChunkPtr& chunk) override { ++_chunk_added; }

    int num_chunks() const { return _chunk_added; }

private:
    int _chunk_added = 0;
};

class SinkIOBufferTest : public testing::Test {
protected:
    SinkIOBufferTest() = default;

    static void operator_thread(void* arg1, void* arg2) {
        auto* buf = reinterpret_cast<MockSinkIOBuffer*>(arg1);
        auto* runtime_state = reinterpret_cast<RuntimeState*>(arg2);

        auto first_chunk = gen_test_chunk(1);
        ASSERT_OK(buf->append_chunk(runtime_state, first_chunk));

        auto second_chunk = gen_test_chunk(2);
        ASSERT_OK(buf->append_chunk(runtime_state, second_chunk));

        // append a nullptr, won't cause the queue termination
        ASSERT_OK(buf->append_chunk(runtime_state, nullptr));

        // the forth chunk can be processed correctly
        auto forth_chunk = gen_test_chunk(4);
        ASSERT_OK(buf->append_chunk(runtime_state, forth_chunk));
    }

    static void poll_thread(void* arg1) {
        auto* buf = reinterpret_cast<MockSinkIOBuffer*>(arg1);
        (void)buf->set_finishing();
    }

    static bool wait(const std::function<bool()>& func) {
        int i = 0;
        while (!func()) {
            bthread_usleep(1000);
            i++;
            if (i > 10000) {
                // max wait 10s
                return false;
            }
        }
        return true;
    }

protected:
    static ChunkPtr gen_test_chunk(int value);
    static std::shared_ptr<RuntimeState> gen_test_runtime_state();
    std::atomic<int> _data_chunk = 0;
    std::atomic<int> _close_chunk = 0;
};

ChunkPtr SinkIOBufferTest::gen_test_chunk(int value) {
    auto col = Int32Column::create();
    col->resize(value);
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(col), 1);
    return chunk;
}

std::shared_ptr<RuntimeState> SinkIOBufferTest::gen_test_runtime_state() {
    auto runtime_state = std::make_shared<RuntimeState>();
    auto mem_tracker = std::make_shared<MemTracker>();
    runtime_state->set_query_mem_tracker(mem_tracker);
    return runtime_state;
}

// Execute sequentially one by one
TEST_F(SinkIOBufferTest, test_basic_1) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void*) {
        ASSERT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _data_chunk <= 0; });
        _data_chunk++;
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [](void*) {});
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void*) { _data_chunk--; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_apend_chunk_end_queue", [this](void*) { _close_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_process_chunk_end_queue", [this](void*) { _close_chunk--; });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _data_chunk == 0 && _close_chunk == 0; });
    EXPECT_PRED1(SinkIOBufferTest::wait, [&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    // MockSinkIOBuffer::_add_chunk() called 4 times
    EXPECT_EQ(4, sink_buffer->num_chunks());
    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Add all and run
TEST_F(SinkIOBufferTest, test_basic_2) {
    SyncPoint::GetInstance()->EnableProcessing();
    bool _need_process_chunk = false;
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void*) { _data_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [&_need_process_chunk](void* arg) {
        ASSERT_PRED1(SinkIOBufferTest::wait, [&_need_process_chunk]() -> bool { return _need_process_chunk; });
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void*) { _data_chunk--; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_apend_chunk_end_queue", [this](void*) { _close_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_process_chunk_end_queue", [this](void*) { _close_chunk--; });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _data_chunk == 4; });

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _close_chunk == 1; });
    _need_process_chunk = true;

    EXPECT_PRED1(SinkIOBufferTest::wait, [&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    // MockSinkIOBuffer::_add_chunk() called 4 times
    EXPECT_EQ(4, sink_buffer->num_chunks());
    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Cancel when there is no task
TEST_F(SinkIOBufferTest, test_cancel_1) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void*) { _data_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [](void*) {});
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void*) { _data_chunk--; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_apend_chunk_end_queue", [this](void*) { _close_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_process_chunk_end_queue", [this](void*) { _close_chunk--; });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _data_chunk == 0; });

    sink_buffer->cancel_one_sinker();
    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _close_chunk == 0; });
    EXPECT_PRED1(SinkIOBufferTest::wait, [&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    // MockSinkIOBuffer::_add_chunk() called 4 times
    EXPECT_EQ(4, sink_buffer->num_chunks());
    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Cancel (have tasks in queue)
TEST_F(SinkIOBufferTest, test_cancel_2) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void*) { _data_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [&sink_buffer](void* arg) {
        ASSERT_PRED1(SinkIOBufferTest::wait, [&sink_buffer]() -> bool { return sink_buffer->is_cancelled(); });
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void*) { _data_chunk--; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_apend_chunk_end_queue", [this](void*) { _close_chunk++; });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_process_chunk_end_queue", [this](void*) { _close_chunk--; });

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    sink_buffer->cancel_one_sinker();

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    EXPECT_PRED1(SinkIOBufferTest::wait, [this]() -> bool { return _data_chunk == 0 && _close_chunk == 0; });
    EXPECT_PRED1(SinkIOBufferTest::wait, [&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    // MockSinkIOBuffer::_add_chunk() can be called at most once (because the callback is waiting for the cancelling),
    // and possibly never done if the cancel is done before the processing. Rest of the chunks are all fast skipped.
    EXPECT_LE(sink_buffer->num_chunks(), 1);
    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

} // namespace starrocks::pipeline
