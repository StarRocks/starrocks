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

#include "connector/async_flush_stream_poller.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/thread/priority_thread_pool.hpp"
#include "exec/pipeline/fragment_context.h"
#include "formats/io/async_flush_output_stream.h"
#include "fs/fs_memory.h"
#include "runtime/exec_env.h"

namespace starrocks::connector {
namespace {

class AsyncFlushStreamPollerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state->set_exec_env(exec_env);
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        _io_executor = _pool.add(new PriorityThreadPool("test", 1, 100));
        _file = _fs.new_writable_file("/test.out").value();
    }

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
    PriorityThreadPool* _io_executor;
    MemoryFileSystem _fs;
    std::unique_ptr<WritableFile> _file;
};

TEST_F(AsyncFlushStreamPollerTest, poll_until_async_status_ready) {
    auto stream = std::make_shared<formats::AsyncFlushOutputStream>(std::move(_file), _io_executor, _runtime_state);
    auto* raw_stream = stream.get();
    AsyncFlushStreamPoller poller;
    poller.enqueue(std::move(stream));
    EXPECT_EQ(poller.releasable_memory(), 0);

    auto [initial_status, initial_done] = poller.poll();
    EXPECT_OK(initial_status);
    EXPECT_FALSE(initial_done);

    const int N = 1000'000;
    const int M = 100;
    std::vector<uint8_t> data(N, 'a');
    for (int i = 0; i < M; i++) {
        EXPECT_OK(raw_stream->write(data.data(), data.size()));
    }
    EXPECT_OK(raw_stream->close());

    using namespace std::chrono_literals;
    while (true) {
        auto [status, done] = poller.poll();
        EXPECT_OK(status);
        if (done) {
            break;
        }
        std::this_thread::sleep_for(10ms);
    }
}

} // namespace
} // namespace starrocks::connector
