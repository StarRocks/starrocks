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

#include "runtime/stream_load/time_bounded_stream_load_pipe.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class TimeBoundedStreamLoadPipeTest : public testing::Test {
public:
    TimeBoundedStreamLoadPipeTest() = default;
    ~TimeBoundedStreamLoadPipeTest() override = default;
    void SetUp() override {}
};

PARALLEL_TEST(TimeBoundedStreamLoadPipeTest, read_buffer_finish) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TimeBoundedStreamLoadPipe::get_current_ns");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 0; });
    TimeBoundedStreamLoadPipe pipe("p", 1000);

    auto buf1 = ByteBuffer::allocate_with_tracker(64).value();
    for (int j = 0; j < 64; ++j) {
        char c = '0' + j;
        buf1->put_bytes(&c, sizeof(c));
    }
    buf1->flip();
    ASSERT_OK(pipe.append(std::move(buf1)));

    auto ret = pipe.read();
    ASSERT_TRUE(ret.ok());
    auto read_buf = ret.value();
    ASSERT_EQ(64, read_buf->limit);
    for (int i = 0; i < read_buf->pos; ++i) {
        ASSERT_EQ('0' + i, *(read_buf->ptr + i));
    }

    ASSERT_TRUE(pipe.read().status().is_time_out());

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 2000000000; });
    ASSERT_TRUE(pipe.read().status().is_end_of_file());
}

PARALLEL_TEST(TimeBoundedStreamLoadPipeTest, read_data_finish) {
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TimeBoundedStreamLoadPipe::get_current_ns");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 0; });
    TimeBoundedStreamLoadPipe pipe("p", 1000);

    auto buf1 = ByteBuffer::allocate_with_tracker(64).value();
    for (int j = 0; j < 64; ++j) {
        char c = '0' + j;
        buf1->put_bytes(&c, sizeof(c));
    }
    buf1->flip();
    ASSERT_OK(pipe.append(std::move(buf1)));

    char read_buf[128];
    size_t buf_len = 128;
    bool eof = false;
    ASSERT_OK(pipe.read((uint8_t*)read_buf, &buf_len, &eof));
    ASSERT_FALSE(eof);
    ASSERT_EQ(buf_len, 64);
    for (int i = 0; i < buf_len; ++i) {
        ASSERT_EQ('0' + i, *(read_buf + i));
    }

    buf_len = 128;
    eof = false;
    ASSERT_TRUE(pipe.read((uint8_t*)read_buf, &buf_len, &eof).is_time_out());
    ASSERT_EQ(0, buf_len);
    ASSERT_FALSE(eof);
    SyncPoint::GetInstance()->SetCallBack("TimeBoundedStreamLoadPipe::get_current_ns",
                                          [&](void* arg) { *((int64_t*)arg) = 2000000000; });

    buf_len = 128;
    eof = false;
    ASSERT_OK(pipe.read((uint8_t*)read_buf, &buf_len, &eof));
    ASSERT_EQ(0, buf_len);
    ASSERT_TRUE(eof);
}

PARALLEL_TEST(TimeBoundedStreamLoadPipeTest, not_support_appending_char_array) {
    TimeBoundedStreamLoadPipe pipe("p", 50);
    char ch = '0';
    ASSERT_TRUE(pipe.append(&ch, 1).is_not_supported());
}

} // namespace starrocks
