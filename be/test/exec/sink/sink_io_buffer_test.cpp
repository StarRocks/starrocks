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

#include <string>
#include <thread>

#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

// This is a mock test for synchronization between SinkIOBuffer and its underlying execution queue.
// Query-related context (including SinkIOBuffer) would only be destroyed after SinkIOBuffer becomes finished.
// Although we do not guarantee SinkIOBuffer outlives execution queue, we can still avoid use-after-free problem by
// skipping stop task in consumer thread.

namespace {

class DerivedSinkIOBuffer : public SinkIOBuffer {
public:
    DerivedSinkIOBuffer(int num_sinkers, bool inject_error = false)
            : SinkIOBuffer(num_sinkers, nullptr), _inject_error(inject_error) {}

    Status prepare(RuntimeState* state, RuntimeProfile* parent_profile) override {
        bool expected = false;
        if (!_is_prepared.compare_exchange_strong(expected, true)) {
            return Status::OK();
        }

        return SinkIOBuffer::prepare(state, parent_profile);
    }

    void close(RuntimeState* state) override { SinkIOBuffer::close(state); }

    Status _write_chunk(ChunkPtr chunk) override {
        if (_inject_error) {
            return Status::IOError("io error");
        }
        return Status::OK();
    }

private:
    const bool _inject_error{false};
};

class TestSinkIOBuffer : public ::testing::Test, public testing::WithParamInterface<std::tuple<int, bool>> {};

TEST_P(TestSinkIOBuffer, MultiThread) {
    auto [num_sinkers, inject_error] = GetParam();
    auto sink_buffer = std::make_unique<DerivedSinkIOBuffer>(num_sinkers, inject_error);

    // prepare
    std::vector<std::thread> threads;
    for (int i = 0; i < num_sinkers; i++) {
        threads.emplace_back([&]() { ASSERT_OK(sink_buffer->prepare(nullptr, nullptr)); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // running
    threads.clear();
    for (int i = 0; i < num_sinkers; i++) {
        threads.emplace_back([&]() {
            auto chunk = std::make_shared<Chunk>();
            ASSERT_OK(sink_buffer->append_chunk(nullptr, chunk));
            ASSERT_OK(sink_buffer->append_chunk(nullptr, chunk));
            ASSERT_OK(sink_buffer->set_finishing());
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // check closed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_TRUE(sink_buffer->is_finished());
}

INSTANTIATE_TEST_SUITE_P(TestSinkIOBuffer, TestSinkIOBuffer,
                         testing::Combine(testing::Values(1, 3, 10), testing::Values(true, false)),
                         [](const testing::TestParamInfo<TestSinkIOBuffer::ParamType>& info) {
                             std::stringstream ss;
                             ss << std::get<0>(info.param) << "_" << std::get<1>(info.param);
                             return ss.str();
                         });

} // namespace
} // namespace starrocks::pipeline
