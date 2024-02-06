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
    enum PromiseType {
        BEFORE_ADD_TO_QUEUE = 0,
        BEFORE_ADD_COUNTER = 1,
        AFTER_ADD_COUNTER = 2,
        BEFORE_EXECUTE = 3,
        BEFORE_CANCEL_CHECK_COUNTER = 4,
        AFTER_CANCEL_CHECK_COUNTER = 5,
        BEFORE_DEC_COUNTER = 6,
        AFTER_DEC_COUNTER = 7,
        END
    };

    enum ChunkType { FIRST_CHUNK = 0, SECOND_CHUNK = 1, CLOSE_CHUNK = 2 };

    enum ControlType { ALL = 0, SET = 1, WAIT = 2 };

    MockSinkIOBuffer(int num_sinkers) : SinkIOBuffer(num_sinkers) {
        _first_chunk_wait_promises.resize(PromiseType::END);
        _first_chunk_control_promises.resize(PromiseType::END);
        _second_chunk_wait_promises.resize(PromiseType::END);
        _second_chunk_control_promises.resize(PromiseType::END);
        _close_chunk_wait_promises.resize(PromiseType::END);
        _close_chunk_control_promises.resize(PromiseType::END);

        for (int i = 0; i < PromiseType::END; i++) {
            _first_chunk_wait_promises[i].second = _first_chunk_wait_promises[i].first.get_future();
            _first_chunk_control_promises[i].second = _first_chunk_control_promises[i].first.get_future();
            _second_chunk_wait_promises[i].second = _second_chunk_wait_promises[i].first.get_future();
            _second_chunk_control_promises[i].second = _second_chunk_control_promises[i].first.get_future();
            _close_chunk_wait_promises[i].second = _close_chunk_wait_promises[i].first.get_future();
            _close_chunk_control_promises[i].second = _close_chunk_control_promises[i].first.get_future();
        }

        _value = std::make_unique<int>();
    }

    ~MockSinkIOBuffer() { _value.reset(); }

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

    void _add_chunk(const ChunkPtr& chunk) override { dummy(); }

    void set_and_wait_promise(const ChunkPtr& chunk, PromiseType type) {
        if (chunk == nullptr) {
            LOG(INFO) << "SET_AND_WAIT: " << (int)CLOSE_CHUNK << ":" << (int)type << std::endl;
            _close_chunk_control_promises[type].first.set_value();
            _close_chunk_wait_promises[type].second.get();
        } else if (chunk->num_rows() == 1) {
            LOG(INFO) << "SET_AND_WAIT: " << (int)FIRST_CHUNK << ":" << (int)type << std::endl;
            _first_chunk_control_promises[type].first.set_value();
            _first_chunk_wait_promises[type].second.get();
        } else if (chunk->num_rows() == 2) {
            LOG(INFO) << "SET_AND_WAIT: " << (int)SECOND_CHUNK << ":" << (int)type << std::endl;
            _second_chunk_control_promises[type].first.set_value();
            _second_chunk_wait_promises[type].second.get();
        }
    }

    void set_wait_promise(ChunkType chunk_type, PromiseType type) {
        LOG(INFO) << "SET: " << (int)chunk_type << ":" << (int)type << std::endl;
        if (chunk_type == FIRST_CHUNK) {
            _first_chunk_wait_promises[type].first.set_value();
        } else if (chunk_type == SECOND_CHUNK) {
            _second_chunk_wait_promises[type].first.set_value();
        } else if (chunk_type == CLOSE_CHUNK) {
            _close_chunk_wait_promises[type].first.set_value();
        }
    }

    void wait_control_promise(ChunkType chunk_type, PromiseType type) {
        LOG(INFO) << "WAIT: " << (int)chunk_type << ":" << (int)type << std::endl;
        if (chunk_type == FIRST_CHUNK) {
            _first_chunk_control_promises[type].second.wait();
        } else if (chunk_type == SECOND_CHUNK) {
            _second_chunk_control_promises[type].second.wait();
        } else if (chunk_type == CLOSE_CHUNK) {
            _close_chunk_control_promises[type].second.wait();
        }
    }

    Status append_chunk(RuntimeState* state, const ChunkPtr& chunk) override {
        if (Status status = get_io_status(); !status.ok()) {
            return status;
        }
        set_and_wait_promise(chunk, BEFORE_ADD_TO_QUEUE);
        if (bthread::execution_queue_execute(*_exec_queue_id, chunk) != 0) {
            return Status::InternalError("submit io task failed");
        }
        set_and_wait_promise(chunk, BEFORE_ADD_COUNTER);
        ++_num_pending_chunks;
        set_and_wait_promise(chunk, AFTER_ADD_COUNTER);
        return Status::OK();
    }

    ALWAYS_NOINLINE void dummy() { *_value = 10; }

    Status set_finishing() override {
        if (--_num_result_sinkers == 0) {
            // when all writes are over, we add a nullptr as a special mark to trigger close
            if (_is_finishing_failed) {
                return Status::InternalError("set finishing failed");
            } else {
                set_and_wait_promise(nullptr, BEFORE_ADD_TO_QUEUE);
                int v = bthread::execution_queue_execute(*_exec_queue_id, nullptr);
                if (v != 0) {
                    return Status::InternalError("submit task failed");
                }
                set_and_wait_promise(nullptr, BEFORE_ADD_COUNTER);
                ++_num_pending_chunks;
                set_and_wait_promise(nullptr, AFTER_ADD_COUNTER);
            }
        }
        return Status::OK();
    }

    void set_is_finishing_failed(bool is_finishing_failed) { _is_finishing_failed = is_finishing_failed; }

protected:
    virtual void _process_chunk(bthread::TaskIterator<ChunkPtr>& iter) override {
        DeferOp op([&]() {
            set_and_wait_promise(*iter, BEFORE_DEC_COUNTER);
            _num_pending_chunks--;
            set_and_wait_promise(*iter, AFTER_DEC_COUNTER);
        });

        set_and_wait_promise(*iter, BEFORE_EXECUTE);

        if (_is_finished) {
            return;
        }

        const auto& chunk = *iter;
        if (chunk == nullptr) {
            close(_state);
            return;
        }

        if (_is_cancelled) {
            set_and_wait_promise(chunk, BEFORE_CANCEL_CHECK_COUNTER);
            if (_num_pending_chunks <= 1) {
                set_and_wait_promise(chunk, AFTER_CANCEL_CHECK_COUNTER);
                close(_state);
            }
            return;
        }

        _add_chunk(chunk);
    }

private:
    std::vector<std::pair<std::promise<void>, std::future<void>>> _first_chunk_wait_promises;
    std::vector<std::pair<std::promise<void>, std::future<void>>> _first_chunk_control_promises;

    std::vector<std::pair<std::promise<void>, std::future<void>>> _second_chunk_wait_promises;
    std::vector<std::pair<std::promise<void>, std::future<void>>> _second_chunk_control_promises;

    std::vector<std::pair<std::promise<void>, std::future<void>>> _close_chunk_wait_promises;
    std::vector<std::pair<std::promise<void>, std::future<void>>> _close_chunk_control_promises;

    std::promise<void> _finishing_promise;
    std::promise<void> _before_cancel_promise;

    std::unique_ptr<int> _value;
    bool _is_finishing_failed = false;
};

class MockSinkIOBuffer2 : public SinkIOBuffer {
public:
    MockSinkIOBuffer2(int num_sinkers) : SinkIOBuffer(num_sinkers) { _value = std::make_unique<int>(); }

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

    void _add_chunk(const ChunkPtr& chunk) override { dummy(); }

    ALWAYS_NOINLINE void dummy() { *_value = 10; }

private:
    std::unique_ptr<int> _value;
};

using ControlType = MockSinkIOBuffer::ControlType;
using PromiseType = MockSinkIOBuffer::PromiseType;
using ChunkType = MockSinkIOBuffer::ChunkType;

class SinkIOBufferTest : public testing::Test {
protected:
    struct Task {
        ControlType control_type;
        ChunkType chunk_type;
        PromiseType promise_type;
    };

    SinkIOBufferTest() = default;

    void SetUp() override {}

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

    static void run_check(MockSinkIOBuffer& buf, const std::vector<Task>& task) {
        for (size_t i = 0; i < task.size(); i++) {
            if (task[i].control_type == ControlType::WAIT) {
                buf.wait_control_promise(task[i].chunk_type, task[i].promise_type);
            } else if (task[i].control_type == ControlType::SET) {
                buf.set_wait_promise(task[i].chunk_type, task[i].promise_type);
            } else {
                buf.wait_control_promise(task[i].chunk_type, task[i].promise_type);
                buf.set_wait_promise(task[i].chunk_type, task[i].promise_type);
            }
        }
    }

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

// Execute sequentially one by one
TEST_F(SinkIOBufferTest, test_basic_1) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk and wait
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            // Append second chunk and wait
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks1);

    std::thread thread2(poll_thread, sink_buffer.get());
    std::vector<Task> tasks2 = {
            // Append close chunk
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks2);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

// Add all and run
TEST_F(SinkIOBufferTest, test_basic_2) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},

            // Append second chunk
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},
    };
    run_check(*sink_buffer, tasks1);

    std::thread thread2(poll_thread, sink_buffer.get());
    std::vector<Task> tasks2 = {
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_ADD_COUNTER},
    };
    run_check(*sink_buffer, tasks2);

    std::vector<Task> tasks3 = {
            // Append close chunk
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks3);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

// Cancel when there is no task
TEST_F(SinkIOBufferTest, test_cancel_1) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk and wait
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            // Append second chunk and wait
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks1);

    sink_buffer->cancel_one_sinker();
    std::thread thread2(poll_thread, sink_buffer.get());
    std::vector<Task> tasks2 = {
            // Append close chunk
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks2);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

// Cancel (_num_pending_chunks = 0)
TEST_F(SinkIOBufferTest, test_cancel_2) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk and wait
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            // Append second chunk and wait
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::WAIT, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
    };
    run_check(*sink_buffer, tasks1);

    sink_buffer->cancel_one_sinker();

    std::vector<Task> tasks2 = {
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::WAIT, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
    };
    run_check(*sink_buffer, tasks2);
    ASSERT_EQ(sink_buffer->num_pending_chunks(), 0);

    std::thread thread2(poll_thread, sink_buffer.get());

    std::vector<Task> tasks3 = {
            {ControlType::SET, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},
            {ControlType::SET, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},

            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
    };
    run_check(*sink_buffer, tasks3);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

// Cancel (_num_pending_chunks = 2)
TEST_F(SinkIOBufferTest, test_cancel_3) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk and wait
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            // Append second chunk and wait
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},
            {ControlType::WAIT, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
    };
    run_check(*sink_buffer, tasks1);

    sink_buffer->cancel_one_sinker();

    std::vector<Task> tasks2 = {
            {ControlType::SET, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::WAIT, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
    };
    run_check(*sink_buffer, tasks2);

    std::thread thread2(poll_thread, sink_buffer.get());
    std::vector<Task> tasks3 = {
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_ADD_COUNTER},
    };
    run_check(*sink_buffer, tasks3);

    ASSERT_EQ(sink_buffer->num_pending_chunks(), 2);

    std::vector<Task> tasks4 = {
            {ControlType::SET, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::CLOSE_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks4);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

TEST_F(SinkIOBufferTest, test_push_close_to_execute_queue_failed) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer>(1);
    sink_buffer->set_is_finishing_failed(true);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    std::thread thread1(operator_thread, sink_buffer.get(), runtime_state.get());
    std::vector<Task> tasks1 = {
            // Append first chunk
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_ADD_COUNTER},

            // Append second chunk
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_TO_QUEUE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_ADD_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_ADD_COUNTER},
    };
    run_check(*sink_buffer, tasks1);

    sink_buffer->cancel_one_sinker();

    std::thread thread2(poll_thread, sink_buffer.get());

    std::vector<Task> tasks3 = {
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::FIRST_CHUNK, PromiseType::AFTER_DEC_COUNTER},

            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_EXECUTE},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_CANCEL_CHECK_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::BEFORE_DEC_COUNTER},
            {ControlType::ALL, ChunkType::SECOND_CHUNK, PromiseType::AFTER_DEC_COUNTER},
    };
    run_check(*sink_buffer, tasks3);

    thread1.join();
    thread2.join();
    ASSERT_TRUE(sink_buffer->is_finished());

    sink_buffer.reset();
}

TEST_F(SinkIOBufferTest, test_base_class) {
    auto runtime_state = gen_test_runtime_state();
    auto sink_buffer = std::make_unique<MockSinkIOBuffer2>(1);
    ASSERT_OK(sink_buffer->prepare(runtime_state.get(), nullptr));

    auto first_chunk = gen_test_chunk(1);
    ASSERT_OK(sink_buffer->append_chunk(runtime_state.get(), first_chunk));

    auto second_chunk = gen_test_chunk(2);
    ASSERT_OK(sink_buffer->append_chunk(runtime_state.get(), second_chunk));

    ASSERT_OK(sink_buffer->set_finishing());

    int i = 0;
    while (!sink_buffer->is_finished()) {
        usleep(10000);
        i++;
        if (i > 1000) {
            ASSERT_FALSE(true);
        }
    }

    sink_buffer.reset();
}

} // namespace starrocks::pipeline