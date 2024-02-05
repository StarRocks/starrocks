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
    enum PromiseType { FIRST_CHUNK, SECOND_CHUNK, CLOSE_CHUNK, FINISHING };

    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) { _value = std::make_unique<int>(); }

    ~MockSinkIOBuffer() { _value.reset(); }

    static int execute_io_task(void* meta, bthread::TaskIterator<ChunkPtr>& iter) {
        return SinkIOBuffer::execute_io_task(meta, iter);
    }

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override {
        _queue_id = std::make_unique<bthread::ExecutionQueueId<ChunkPtr>>();
        int ret = bthread::execution_queue_start<ChunkPtr>(_queue_id.get(), nullptr, &MockSinkIOBuffer::execute_io_task,
                                                           this);
        if (ret != 0) {
            _queue_id.reset();
            return Status::InternalError("start execution queue failed");
        }
        return Status::OK();
    }

    void _add_chunk(const ChunkPtr& chunk) override {
        if (chunk == nullptr) {
            _close_chunk_promise.get_future().wait();
        } else if (chunk->num_rows() == 1) {
            _first_chunk_promise.get_future().wait();
        } else if (chunk->num_rows() == 2) {
            _second_chunk_promise.get_future().wait();
        }
    }

    ALWAYS_NOINLINE void dummy() { *_value = 10; }

    void set_promise_value(PromiseType type) {
        if (type == FIRST_CHUNK) {
            _first_chunk_promise.set_value();
        } else if (type == SECOND_CHUNK) {
            _second_chunk_promise.set_value();
        } else if (type == CLOSE_CHUNK) {
            _close_chunk_promise.set_value();
        }
    }

    Status set_finishing() override {
        if (--_num_result_sinkers == 0) {
            // when all writes are over, we add a nullptr as a special mark to trigger close
            if (bthread::execution_queue_execute(*_exec_queue_id, nullptr) != 0) {
                return Status::InternalError("submit task failed");
            }
            _finishing_promise.get_future().wait();
            ++_num_pending_chunks;
        }
        return Status::OK();
    }

private:
    std::unique_ptr<bthread::ExecutionQueueId<ChunkPtr>> _queue_id;

    std::promise<void> _first_chunk_promise;
    std::promise<void> _second_chunk_promise;
    std::promise<void> _close_chunk_promise;
    std::promise<void> _finishing_promise;

    std::unique_ptr<int> _value;
};

class SinkIOBufferTest : public testing::Test {
protected:
    SinkIOBufferTest() = default;
    void SetUp() override {}

protected:
    static ChunkPtr gen_test_chunk(int value);
    static std::shared_ptr<RuntimeState> gen_test_runtime_state();
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

TEST_F(SinkIOBufferTest, test_basic_1) {
    auto runtime_state = gen_test_runtime_state();

    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(2);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    auto first_chunk = gen_test_chunk(1);
    ASSERT_OK(sink_buffer->append_chunk(runtime_state.get(), first_chunk));
    sink_buffer->set_promise_value(MockSinkIOBuffer::FIRST_CHUNK);

    auto second_chunk = gen_test_chunk(1);
    ASSERT_OK(sink_buffer->append_chunk(runtime_state.get(), second_chunk));
    sink_buffer->set_promise_value(MockSinkIOBuffer::SECOND_CHUNK);

    ASSERT_TRUE(sink_buffer->set_finishing().ok());
    ASSERT_TRUE(sink_buffer->set_finishing().ok());
    sink_buffer->set_promise_value(MockSinkIOBuffer::FINISHING);

    sink_buffer.reset();
}

} // namespace starrocks::pipeline
