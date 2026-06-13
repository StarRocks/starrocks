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

#include "util/byte_buffer.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/logging.h"

namespace starrocks {

class ByteBufferTest : public testing::Test {
public:
    ByteBufferTest() = default;
    ~ByteBufferTest() override = default;

protected:
    char _write_data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    char _read_data[10];
};

TEST_F(ByteBufferTest, normal) {
    auto buf = ByteBuffer::allocate_with_tracker(4).value();
    ASSERT_EQ(0, buf->pos);
    ASSERT_EQ(4, buf->limit);
    ASSERT_EQ(4, buf->capacity);

    char test[] = {1, 2, 3};
    buf->put_bytes(test, 3);

    ASSERT_EQ(3, buf->pos);
    ASSERT_EQ(4, buf->limit);
    ASSERT_EQ(4, buf->capacity);

    ASSERT_EQ(1, buf->remaining());
    buf->flip_to_read();
    ASSERT_EQ(0, buf->pos);
    ASSERT_EQ(3, buf->limit);
    ASSERT_EQ(4, buf->capacity);
    ASSERT_EQ(3, buf->remaining());
}

TEST_F(ByteBufferTest, test_meta) {
    auto none_meta_st = ByteBufferMeta::create(ByteBufferMetaType::NONE);
    ASSERT_OK(none_meta_st.status());
    NoneByteBufferMeta* none_meta = dynamic_cast<NoneByteBufferMeta*>(none_meta_st.value());
    ASSERT_TRUE(none_meta != nullptr);
    ASSERT_EQ(NoneByteBufferMeta::instance(), none_meta);
    ASSERT_EQ(ByteBufferMetaType::NONE, none_meta->type());
    ASSERT_EQ("none", none_meta->to_string());
    ASSERT_OK(none_meta->copy_from(none_meta));

    auto kafka_meta_st = ByteBufferMeta::create(ByteBufferMetaType::KAFKA);
    ASSERT_OK(kafka_meta_st.status());
    StreamMessageMeta* kafka_meta = dynamic_cast<StreamMessageMeta*>(kafka_meta_st.value());
    DeferOp defer([&] { delete kafka_meta; });
    ASSERT_TRUE(kafka_meta != nullptr);
    ASSERT_EQ(ByteBufferMetaType::KAFKA, kafka_meta->type());
    ASSERT_EQ(-1, kafka_meta->partition());
    ASSERT_EQ(-1, kafka_meta->offset());
    kafka_meta->set_topic("t");
    kafka_meta->set_partition(1);
    kafka_meta->set_offset(2);
    kafka_meta->set_timestamp(123);
    kafka_meta->set_key("k");
    kafka_meta->add_header("h1", "v1");
    ASSERT_EQ(1, kafka_meta->partition());
    ASSERT_EQ(2, kafka_meta->offset());
    ASSERT_EQ(123, kafka_meta->timestamp());
    ASSERT_TRUE(kafka_meta->has_key());
    ASSERT_EQ("k", kafka_meta->key());
    ASSERT_EQ(1, kafka_meta->headers().size());
    ASSERT_EQ("kafka topic: t, partition: 1, offset: 2", kafka_meta->to_string());

    // Without a topic (the job references no metadata column) only partition/offset are rendered.
    StreamMessageMeta bare_kafka_meta(ByteBufferMetaType::KAFKA);
    bare_kafka_meta.set_partition(4);
    bare_kafka_meta.set_offset(5);
    ASSERT_EQ("kafka partition: 4, offset: 5", bare_kafka_meta.to_string());

    ASSERT_TRUE(none_meta->copy_from(kafka_meta).is_not_supported());
    ASSERT_TRUE(kafka_meta->copy_from(none_meta).is_not_supported());

    // A Pulsar-typed meta is incompatible with a Kafka-typed one.
    StreamMessageMeta pulsar_meta(ByteBufferMetaType::PULSAR);
    ASSERT_TRUE(kafka_meta->copy_from(&pulsar_meta).is_not_supported());

    // copy_from fully overwrites and clears absent fields (no stale key/headers leak).
    StreamMessageMeta kafka_meta1(ByteBufferMetaType::KAFKA);
    kafka_meta1.set_topic("t2");
    kafka_meta1.set_partition(2);
    kafka_meta1.set_offset(3);
    ASSERT_OK(kafka_meta->copy_from(&kafka_meta1));
    ASSERT_EQ(2, kafka_meta->partition());
    ASSERT_EQ(3, kafka_meta->offset());
    ASSERT_FALSE(kafka_meta->has_key());
    ASSERT_TRUE(kafka_meta->key().empty());
    ASSERT_TRUE(kafka_meta->headers().empty());
}

TEST_F(ByteBufferTest, test_allocate_with_meta) {
    auto buf1 = ByteBuffer::allocate_with_tracker(4).value();
    ASSERT_EQ(NoneByteBufferMeta::instance(), buf1->meta());

    auto buf2 = ByteBuffer::allocate_with_tracker(4, 0, ByteBufferMetaType::KAFKA).value();
    StreamMessageMeta* meta2 = dynamic_cast<StreamMessageMeta*>(buf2->meta());
    ASSERT_TRUE(meta2 != nullptr);
    ASSERT_EQ(-1, meta2->partition());
    ASSERT_EQ(-1, meta2->offset());
    meta2->set_partition(2);
    meta2->set_offset(4);
    meta2->add_header("h", "v");
    ASSERT_EQ(2, meta2->partition());
    ASSERT_EQ(4, meta2->offset());

    // reallocate copies the meta forward, including headers.
    auto buf3 = ByteBuffer::reallocate_with_tracker(buf2, 8).value();
    StreamMessageMeta* meta3 = dynamic_cast<StreamMessageMeta*>(buf3->meta());
    ASSERT_TRUE(meta3 != nullptr);
    ASSERT_TRUE(meta2 != meta3);
    ASSERT_EQ(2, meta3->partition());
    ASSERT_EQ(4, meta3->offset());
    ASSERT_EQ(1, meta3->headers().size());
}

TEST_F(ByteBufferTest, test_flip_to_write_partial_read) {
    auto buf = ByteBuffer::allocate_with_tracker(16).value();

    // write [1, 2, 3, 4, 5]
    buf->put_bytes(_write_data, 5);

    // read [1, 2, 3]
    buf->flip_to_read();
    buf->get_bytes(_read_data, 3);

    // write [1, 2, 3, 4]
    buf->flip_to_write();
    buf->put_bytes(_write_data, 4);

    // read all bytes
    buf->flip_to_read();
    ASSERT_EQ(6, buf->remaining());
    buf->get_bytes(_read_data, 6);
    char check_data[] = {4, 5, 1, 2, 3, 4};
    ASSERT_EQ(0, memcmp(check_data, _read_data, 6));
    ASSERT_EQ(16, buf->capacity);
    ASSERT_EQ(6, buf->pos);
    ASSERT_EQ(6, buf->limit);
}

TEST_F(ByteBufferTest, test_flip_to_write_read_all) {
    auto buf = ByteBuffer::allocate_with_tracker(16).value();

    // write [1, 2, 3, 4, 5]
    buf->put_bytes(_write_data, 5);

    // read [1, 2, 3, 4, 5]
    buf->flip_to_read();
    buf->get_bytes(_read_data, 5);

    // write [1, 2, 3, 4]
    buf->flip_to_write();
    buf->put_bytes(_write_data, 4);

    // read all bytes
    buf->flip_to_read();
    ASSERT_EQ(4, buf->remaining());
    buf->get_bytes(_read_data, 4);
    char check_data[] = {1, 2, 3, 4};
    ASSERT_EQ(0, memcmp(check_data, _read_data, 4));
    ASSERT_EQ(16, buf->capacity);
    ASSERT_EQ(4, buf->pos);
    ASSERT_EQ(4, buf->limit);
}

TEST_F(ByteBufferTest, test_flip_to_write_read_nothing) {
    auto buf = ByteBuffer::allocate_with_tracker(16).value();

    // write [1, 2, 3, 4, 5]
    buf->put_bytes(_write_data, 5);

    // read [1, 2, 3, 4, 5]
    buf->flip_to_read();

    // write [1, 2, 3, 4]
    buf->flip_to_write();
    buf->put_bytes(_write_data, 4);

    // read all bytes
    buf->flip_to_read();
    ASSERT_EQ(9, buf->remaining());
    buf->get_bytes(_read_data, 9);
    char check_data[] = {1, 2, 3, 4, 5, 1, 2, 3, 4};
    ASSERT_EQ(0, memcmp(check_data, _read_data, 9));
    ASSERT_EQ(16, buf->capacity);
    ASSERT_EQ(9, buf->pos);
    ASSERT_EQ(9, buf->limit);
}

} // namespace starrocks
