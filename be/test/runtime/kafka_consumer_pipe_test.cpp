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
//   https://github.com/apache/incubator-doris/blob/master/be/test/runtime/kafka_consumer_pipe_test.cpp

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

#include "runtime/routine_load/kafka_consumer_pipe.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks {

class KafkaConsumerPipeTest : public testing::Test {
public:
    KafkaConsumerPipeTest() = default;
    ~KafkaConsumerPipeTest() override = default;

    void SetUp() override {}

    void TearDown() override {}

private:
};

TEST_F(KafkaConsumerPipeTest, append_read) {
    KafkaConsumerPipe k_pipe(1024 * 1024, 64 * 1024);

    std::string msg1 = "i have a dream";
    std::string msg2 = "This is from kafka";

    Status st;
    char row_delimiter = '\n';
    st = k_pipe.append_with_row_delimiter(msg1.c_str(), msg1.length(), row_delimiter);
    ASSERT_TRUE(st.ok());
    st = k_pipe.append_with_row_delimiter(msg2.c_str(), msg2.length(), row_delimiter, 1, 2);
    ASSERT_TRUE(st.ok());
    st = k_pipe.finish();
    ASSERT_TRUE(st.ok());

    char buf[1024];
    size_t data_size = 1024;
    bool eof = false;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, msg1.length() + msg2.length() + 2);
    ASSERT_EQ(eof, false);

    data_size = 1024;
    st = k_pipe.read((uint8_t*)buf, &data_size, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(data_size, 0);
    ASSERT_EQ(eof, true);
}

TEST_F(KafkaConsumerPipeTest, append_read_json) {
    KafkaConsumerPipe k_pipe(1024 * 1024, 64 * 1024);

    std::string msg1 = R"({"k1":1}")";
    std::string msg2 = R"({"k2":2}")";

    Status st;
    char row_delimiter = '\n';
    st = k_pipe.append_json(msg1.c_str(), msg1.length(), row_delimiter);
    ASSERT_TRUE(st.ok());
    st = k_pipe.append_json(msg2.c_str(), msg2.length(), row_delimiter, 1, 2);
    ASSERT_TRUE(st.ok());
    st = k_pipe.finish();
    ASSERT_TRUE(st.ok());

    StatusOr<ByteBufferPtr> buf_st1 = k_pipe.read();
    ASSERT_OK(buf_st1.status());
    auto buf1 = buf_st1.value();
    ASSERT_EQ(NoneByteBufferMeta::instance(), buf1->meta());

    StatusOr<ByteBufferPtr> buf_st2 = k_pipe.read();
    ASSERT_OK(buf_st2.status());
    auto buf2 = buf_st2.value();
    ByteBufferMeta* meta = buf2->meta();
    ASSERT_EQ(ByteBufferMetaType::KAFKA, meta->type());
    KafkaByteBufferMeta* kafka_meta = static_cast<KafkaByteBufferMeta*>(meta);
    ASSERT_EQ(1, kafka_meta->partition());
    ASSERT_EQ(2, kafka_meta->offset());
}

} // namespace starrocks
