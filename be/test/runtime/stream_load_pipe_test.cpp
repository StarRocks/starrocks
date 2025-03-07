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

#include <fstream>
#include <memory>
#include <thread>

#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/monotime.h"

namespace starrocks {

class StreamLoadPipeTest : public testing::Test {
public:
    StreamLoadPipeTest() = default;
    ~StreamLoadPipeTest() override = default;
    void SetUp() override {}
};

PARALLEL_TEST(StreamLoadPipeTest, append_bytes) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        for (int i = 0; i < 128; ++i) {
            char buf = '0' + (i % 10);
            pipe.append(&buf, 1);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[256];
    size_t buf_len = 256;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(128, buf_len);
    ASSERT_FALSE(eof);
    for (int i = 0; i < 128; ++i) {
        ASSERT_EQ('0' + (i % 10), buf[i]);
    }
    st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(0, buf_len);
    ASSERT_TRUE(eof);

    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, append_bytes2) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        for (int i = 0; i < 128; ++i) {
            char buf = '0' + (i % 10);
            pipe.append(&buf, 1);
        }
        pipe.finish();
    };
    std::thread t1(appender);

    char buf[128];
    size_t buf_len = 62;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(62, buf_len);
    ASSERT_FALSE(eof);
    for (int i = 0; i < 62; ++i) {
        ASSERT_EQ('0' + (i % 10), buf[i]);
    }
    for (int i = 62; i < 128; ++i) {
        char ch;
        buf_len = 1;
        auto st = pipe.read((uint8_t*)&ch, &buf_len, &eof);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(1, buf_len);
        ASSERT_FALSE(eof);
        ASSERT_EQ('0' + (i % 10), ch);
    }
    st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(0, buf_len);
    ASSERT_TRUE(eof);

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

    char buf[128];
    size_t buf_len = 128;
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_FALSE(st.ok());
    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, append_buffer) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        auto buf = ByteBuffer::allocate_with_tracker(64).value();
        int k = 0;
        for (int j = 0; j < 64; ++j) {
            char c = '0' + (k++ % 10);
            buf->put_bytes(&c, sizeof(c));
        }
        buf->flip();
        pipe.append(std::move(buf));
        pipe.finish();
    };
    std::thread t1(appender);

    // 1st read, the whole buffer is expected.
    char buf[64];
    size_t buf_len = sizeof(buf);
    bool eof = false;
    auto st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_FALSE(eof);
    ASSERT_EQ(buf_len, 64);
    for (int i = 0; i < sizeof(buf); ++i) {
        ASSERT_EQ('0' + (i % 10), buf[i]);
    }

    // 2nd read, eof is expected.
    st = pipe.read((uint8_t*)buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(0, buf_len);
    ASSERT_TRUE(eof);

    t1.join();
    ASSERT_EQ(1, pipe.num_append_buffers());
    ASSERT_EQ(64, pipe.append_buffer_bytes());
}

PARALLEL_TEST(StreamLoadPipeTest, append_and_read_buffer) {
    StreamLoadPipe pipe(66, 64);

    auto appender = [&pipe] {
        auto buf = ByteBuffer::allocate_with_tracker(64).value();
        int k = 0;
        for (int j = 0; j < 64; ++j) {
            char c = '0' + (k++ % 10);
            buf->put_bytes(&c, sizeof(c));
        }
        buf->flip();
        pipe.append(std::move(buf));
        pipe.finish();
    };
    std::thread t1(appender);

    // 1st read.
    auto st = pipe.read();
    ASSERT_TRUE(st.ok());
    auto buf = st.value();
    ASSERT_EQ(64, buf->limit);
    for (int i = 0; i < buf->pos; ++i) {
        ASSERT_EQ('0' + (i % 10), *(buf->ptr + i));
    }

    // 2nd read, eof is expected.
    st = pipe.read();
    ASSERT_TRUE(st.status().is_end_of_file());

    t1.join();
    ASSERT_EQ(1, pipe.num_append_buffers());
    ASSERT_EQ(64, pipe.append_buffer_bytes());
}

PARALLEL_TEST(StreamLoadPipeTest, append_large_chunk) {
    StreamLoadPipe pipe(/*max_buffered_bytes=*/6, /*min_chunk_size=*/4);

    auto producer = std::thread([&pipe]() {
        // append data with size larger than max_buffered_bytes
        ASSERT_OK(pipe.append("0123456789", 10));
        pipe.finish();
    });

    char buf[12];
    size_t buf_len = 12;
    bool eof = false;
    ASSERT_OK(pipe.read((uint8_t*)buf, &buf_len, &eof));
    ASSERT_EQ(10, buf_len);
    ASSERT_FALSE(eof);
    ASSERT_EQ(std::string_view("0123456789"), std::string_view(buf, buf_len));
    ASSERT_OK(pipe.read((uint8_t*)buf, &buf_len, &eof));
    ASSERT_EQ(0, buf_len);
    ASSERT_TRUE(eof);

    producer.join();
}

std::vector<char> readFileAsBytes(const std::string& filename) {
    // Open the file in binary mode
    std::ifstream file(filename, std::ios::binary);

    // Stop execution if file couldn't be opened
    if (!file) {
        throw std::runtime_error("Could not open file");
    }

    // Seek to the end of the file to find its size
    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    // Create a vector to hold the bytes of the file
    std::vector<char> buffer(size);

    // Read the file into the buffer
    if (!file.read(buffer.data(), size)) {
        throw std::runtime_error("Error reading file");
    }

    return buffer;
}

PARALLEL_TEST(StreamLoadPipeTest, compressed_reader) {
    auto pipe = std::make_shared<StreamLoadPipe>();

    auto producer = std::thread([&pipe]() {
        // append data with size larger than max_buffered_bytes
        auto buf = readFileAsBytes("./be/test/runtime/test_data/compressed_file/foo.json.lz4");
        EXPECT_OK(pipe->append(buf.data(), buf.size()));
        pipe->finish();
    });

    CompressedStreamLoadPipeReader reader(pipe, TCompressionType::LZ4_FRAME);

    auto res = reader.read();
    EXPECT_OK(res.status());
    auto buf = res.value();
    EXPECT_EQ(buf->remaining(), 42000021);
    EXPECT_EQ(std::string_view(R"({"foo": 1, "bar": 2})"), std::string_view(buf->ptr, 20));
    producer.join();
}

PARALLEL_TEST(StreamLoadPipeTest, append_after_finish) {
    StreamLoadPipe pipe(66, 64);

    auto buf1 = ByteBuffer::allocate_with_tracker(64).value();
    for (int j = 0; j < 64; ++j) {
        char c = '0' + j;
        buf1->put_bytes(&c, sizeof(c));
    }
    buf1->flip();
    ASSERT_OK(pipe.append(std::move(buf1)));

    auto appender = [&pipe] {
        while (pipe.num_waiting_append_buffer() == 0) {
            SleepFor(MonoDelta::FromMilliseconds(1));
        }
        EXPECT_OK(pipe.finish());
    };
    std::thread t1(appender);

    auto buf2 = ByteBuffer::allocate_with_tracker(64).value();
    for (int j = 0; j < 64; ++j) {
        char c = '0' + j;
        buf2->put_bytes(&c, sizeof(c));
    }
    buf2->flip();
    EXPECT_TRUE(pipe.append(std::move(buf2)).is_capacity_limit_exceeded());
    t1.join();
}

PARALLEL_TEST(StreamLoadPipeTest, non_blocking_read) {
    StreamLoadPipe pipe(true, 50, 1000, 1000);

    ASSERT_TRUE(pipe.read().status().is_time_out());

    auto buf = ByteBuffer::allocate_with_tracker(64).value();
    for (int j = 0; j < 64; ++j) {
        char c = '0' + j;
        buf->put_bytes(&c, sizeof(c));
    }
    buf->flip();
    ASSERT_OK(pipe.append(std::move(buf)));

    auto ret = pipe.read();
    ASSERT_TRUE(ret.ok());
    auto read_buf = ret.value();
    ASSERT_EQ(64, read_buf->limit);
    for (int i = 0; i < read_buf->limit; ++i) {
        ASSERT_EQ('0' + i, *(read_buf->ptr + i));
    }

    ASSERT_TRUE(pipe.read().status().is_time_out());
}

PARALLEL_TEST(StreamLoadPipeTest, non_blocking_cancel_with_ok_status) {
    StreamLoadPipe pipe(true, 50, 1000, 1000);
    for (int i = 0; i < 10; ++i) {
        char buf = '0' + i;
        pipe.append(&buf, 1);
    }
    pipe.cancel(Status::OK());
    auto st = pipe.read();
    ASSERT_TRUE(st.status().is_cancelled());

    char read_buf[128];
    size_t buf_len = 128;
    bool eof = false;
    ASSERT_TRUE(pipe.read((uint8_t*)read_buf, &buf_len, &eof).is_cancelled());
}

} // namespace starrocks
