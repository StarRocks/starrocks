// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/stream_load_pipe_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/stream_load/stream_load_pipe.h"

#include <gtest/gtest.h>

#include <thread>

#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/monotime.h"

namespace starrocks {

class StreamLoadPipeTest : public testing::Test {
public:
    StreamLoadPipeTest() {}
    virtual ~StreamLoadPipeTest() {}
    void SetUp() override {}
};

PARALLEL_TEST(StreamLoadPipeTest, stream) {
    auto pipe = std::make_shared<StreamLoadPipe>(66, 64);
    auto stream = new_pipe_read_stream(pipe);

    auto appender = [&pipe] {
        for (int i = 0; i < 128; ++i) {
            char buf = '0' + (i % 10);
            pipe->append(&buf, 1);
        }
        pipe->finish();
    };
    std::thread t1(appender);

    char buf[256];
    ASSERT_OK(stream->read_fully(buf, 128));
    for (int i = 0; i < 128; ++i) {
        ASSERT_EQ('0' + (i % 10), buf[i]);
    }
    ASSIGN_OR_ABORT(auto nread, stream->read(buf, 128));
    ASSERT_EQ(0, nread);
    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, cancel) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        int k = 0;
        for (int i = 0; i < 10; ++i) {
            char buf = '0' + (k++ % 10);
            pipe.append(&buf, 1);
        }
        SleepFor(MonoDelta::FromMilliseconds(100));
        pipe.cancel(Status::Cancelled("Cancelled"));
    };
    std::thread t1(appender);

    auto res = pipe.read();
    ASSERT_FALSE(res.ok());
    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, read) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        int k = 0;
        char buf[64];
        for (int j = 0; j < 64; ++j) {
            buf[j] = '0' + (k++ % 10);
        }
        pipe.append(buf, sizeof(buf));
        pipe.append_and_flush(buf, sizeof(buf));
        pipe.finish();
    };
    std::thread t1(appender);

    ASSIGN_OR_ABORT(auto buff, pipe.read());
    ASSERT_EQ(64, buff->remaining());
    for (int i = 0; i < 64; ++i) {
        ASSERT_EQ('0' + (i % 10), buff->ptr[i]);
    }

    ASSIGN_OR_ABORT(buff, pipe.read());
    ASSERT_EQ(64, buff->remaining());
    for (int i = 0; i < 64; ++i) {
        ASSERT_EQ('0' + (i % 10), buff->ptr[i]);
    }

    ASSIGN_OR_ABORT(buff, pipe.read());
    ASSERT_TRUE(buff == nullptr);

    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, append_large_chunk) {
    StreamLoadPipe pipe(/*max_buffered_bytes=*/6, /*min_chunk_size=*/4);

    auto producer = std::thread([&pipe]() {
        // append data with size larger than max_buffered_bytes
        ASSERT_OK(pipe.append("0123456789", 10));
        pipe.finish();
    });

    ASSIGN_OR_ABORT(auto buff, pipe.read());
    ASSERT_EQ(std::string_view("0123456789"), std::string_view(buff->ptr, buff->limit));
    ASSIGN_OR_ABORT(buff, pipe.read());
    ASSERT_TRUE(buff == nullptr);

    producer.join();
}

} // namespace starrocks
