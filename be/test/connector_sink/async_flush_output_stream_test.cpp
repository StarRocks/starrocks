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

#include "io/async_flush_output_stream.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <thread>

#include "connector/async_flush_stream_poller.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/utils.h"
#include "fs/fs_memory.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::connector {
namespace {

using Stream = io::AsyncFlushOutputStream;

class AsyncFlushOutputStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
        _io_executor = _pool.add(new PriorityThreadPool("test", 1, 100));
        _file = _fs.new_writable_file("/test.out").value();
    }

    void TearDown() override {}

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
    PriorityThreadPool* _io_executor;
    MemoryFileSystem _fs;
    std::unique_ptr<WritableFile> _file;
};

TEST_F(AsyncFlushOutputStreamTest, test_empty) {
    Stream stream = Stream(std::move(_file), _io_executor, _runtime_state);
    EXPECT_FALSE(is_ready(stream.io_status()));
}

TEST_F(AsyncFlushOutputStreamTest, test_close_empty) {
    Stream stream = Stream(std::move(_file), _io_executor, _runtime_state);
    EXPECT_OK(stream.close());
    EXPECT_OK(stream.io_status().get());
}

TEST_F(AsyncFlushOutputStreamTest, test_append_one_shot) {
    Stream stream = Stream(std::move(_file), _io_executor, _runtime_state);
    const int N = 1000'000;
    std::vector<uint8> data(N, 'a');
    EXPECT_OK(stream.write(data.data(), data.size()));
    EXPECT_OK(stream.close());
    EXPECT_GE(stream.releasable_memory(), 0);
    EXPECT_OK(stream.io_status().get());
    EXPECT_EQ(stream.releasable_memory(), 0);
    EXPECT_EQ(stream.tell(), N);
}

TEST_F(AsyncFlushOutputStreamTest, test_append_repeated) {
    Stream stream = Stream(std::move(_file), _io_executor, _runtime_state);
    const int N = 1000'000;
    const int M = 100;
    std::vector<uint8> data(N, 'a');
    for (int i = 0; i < M; i++) {
        EXPECT_OK(stream.write(data.data(), data.size()));
    }
    EXPECT_OK(stream.close());
    EXPECT_GE(stream.releasable_memory(), 0);
    EXPECT_OK(stream.io_status().get());
    EXPECT_EQ(stream.releasable_memory(), 0);
    EXPECT_EQ(stream.tell(), N * M);
}

TEST_F(AsyncFlushOutputStreamTest, test_poller) {
    auto stream = std::make_unique<Stream>(std::move(_file), _io_executor, _runtime_state);
    auto ptr = stream.get();
    AsyncFlushStreamPoller poller;
    poller.enqueue(std::move(stream));
    EXPECT_EQ(poller.releasable_memory(), 0);
    {
        auto [s, done] = poller.poll();
        EXPECT_OK(s);
        EXPECT_FALSE(done);
    }

    {
        const int N = 1000'000;
        const int M = 100;
        std::vector<uint8> data(N, 'a');
        for (int i = 0; i < M; i++) {
            EXPECT_OK(ptr->write(data.data(), data.size()));
        }
        EXPECT_OK(ptr->close());
    }

    {
        using namespace std::chrono_literals;
        while (true) {
            auto [s, done] = poller.poll();
            if (done) {
                break;
            }
            std::this_thread::sleep_for(10ms);
        }
    }
}

} // namespace
} // namespace starrocks::connector
