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
    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) { _value = std::make_unique<int>(); }

    static int execute_io_task(void* meta, bthread::TaskIterator<ChunkPtr>& iter) {
        return SinkIOBuffer::execute_io_task(meta, iter);
    }

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override {
        _state = state;
        _exec_queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
        int ret = bthread::execution_queue_start<ChunkPtr>(_exec_queue_id.get(), nullptr,
                                                           &MockSinkIOBuffer::execute_io_task, this);
        if (ret != 0) {
            _exec_queue_id.reset();
            return Status::InternalError("start execution queue failed");
        }
        return Status::OK();
    }

    void _add_chunk(const ChunkPtr& chunk) override { *_value = 10; }

private:
    std::unique_ptr<int> _value;
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
    }

    static void poll_thread(void* arg1) {
        auto* buf = reinterpret_cast<MockSinkIOBuffer*>(arg1);
        (void)buf->set_finishing();
    }

    void wait(const std::function<bool()>& func) {
        int i = 0;
        while (!func()) {
            bthread_usleep(1000);
            i++;
            if (i > 50000) {
                // max wait 50s
                ASSERT_TRUE(false);
            }
        }
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
    chunk->append_column(col, 1);
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
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void* arg) {
        wait([this]() -> bool { return _data_chunk <= 0; });

        if (arg == nullptr) {
            _close_chunk++;
        } else {
            _data_chunk++;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [](void* arg) {});
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk--;
        } else {
            _data_chunk--;
        }
    });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    wait([this]() -> bool { return _data_chunk == 0 && _close_chunk == 0; });
    wait([&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Add all and run
TEST_F(SinkIOBufferTest, test_basic_2) {
    SyncPoint::GetInstance()->EnableProcessing();
    bool _need_process_chunk = false;
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk++;
        } else {
            _data_chunk++;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk",
                                          [this, &_need_process_chunk](void* arg) {
                                              wait([&_need_process_chunk]() -> bool { return _need_process_chunk; });
                                          });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk--;
        } else {
            _data_chunk--;
        }
    });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    wait([this]() -> bool { return _data_chunk == 2; });

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    wait([this]() -> bool { return _close_chunk == 1; });
    _need_process_chunk = true;

    wait([&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Cancel when there is no task
TEST_F(SinkIOBufferTest, test_cancel_1) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk++;
        } else {
            _data_chunk++;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [](void* arg) {});
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk--;
        } else {
            _data_chunk--;
        }
    });

    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    wait([this]() -> bool { return _data_chunk == 0; });

    sink_buffer->cancel_one_sinker();
    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    wait([this]() -> bool { return _close_chunk == 0; });
    wait([&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

// Cancel (have tasks in queue)
TEST_F(SinkIOBufferTest, test_cancel_2) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_append_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk++;
        } else {
            _data_chunk++;
        }
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_before_process_chunk", [&sink_buffer, this](void* arg) {
        wait([&sink_buffer]() -> bool { return sink_buffer->is_cancelled(); });
    });
    SyncPoint::GetInstance()->SetCallBack("sink_io_buffer_after_process_chunk", [this](void* arg) {
        if (arg == nullptr) {
            _close_chunk--;
        } else {
            _data_chunk--;
        }
    });

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    thread1.join();

    sink_buffer->cancel_one_sinker();

    std::thread thread2(poll_thread, sink_buffer.get());
    thread2.join();

    wait([this]() -> bool { return _data_chunk == 0 && _close_chunk == 0; });
    wait([&sink_buffer]() -> bool { return sink_buffer->is_finished(); });

    sink_buffer.reset();

    SyncPoint::GetInstance()->DisableProcessing();
}

} // namespace starrocks::pipeline