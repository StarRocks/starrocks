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

#include "runtime/routine_load/data_consumer.h"

#include <gtest/gtest.h>

<<<<<<< HEAD:be/test/runtime/routine_load/data_consumer_test.cpp
#include "runtime/exec_env.h"
#include "testutil/assert.h"
=======
#include "base/testutil/assert.h"
#include "data_workflows/load/routine_load/data_consumer_group.h"
#include "exec/exec_env.h"
#include "runtime/byte_buffer.h"

#ifndef __APPLE__
#include "pulsar/MessageBuilder.h"
#endif
>>>>>>> 0796ea6077 ([Enhancement] Expose Kafka/Pulsar message metadata via an INCLUDE METADATA clause in Routine Load (#73840)):be/test/data_workflows/load/routine_load/data_consumer_test.cpp

namespace starrocks {

class KafkaDataConsumerTest : public testing::Test {};

// A minimal RdKafka::Message stand-in for build_kafka_message_meta: only the fields the builder reads
// (partition/offset/timestamp/key/headers) are backed; the rest of the librdkafka interface returns
// harmless defaults. Lets the message->meta mapping be exercised without a live broker.
class MockKafkaMessage : public RdKafka::Message {
public:
    MockKafkaMessage(int32_t partition, int64_t offset) : _partition(partition), _offset(offset) {
        _ts.type = RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE;
        _ts.timestamp = -1;
    }
    ~MockKafkaMessage() override { delete _headers; }

    void set_timestamp(RdKafka::MessageTimestamp::MessageTimestampType type, int64_t ts) {
        _ts.type = type;
        _ts.timestamp = ts;
    }
    void set_key(const std::string& key) {
        _key = key;
        _has_key = true;
    }
    // Takes ownership of a Headers built via RdKafka::Headers::create({...}).
    void set_headers(RdKafka::Headers* headers) {
        delete _headers;
        _headers = headers;
    }

    int32_t partition() const override { return _partition; }
    int64_t offset() const override { return _offset; }
    RdKafka::MessageTimestamp timestamp() const override { return _ts; }
    const void* key_pointer() const override { return _has_key ? _key.data() : nullptr; }
    size_t key_len() const override { return _has_key ? _key.size() : 0; }
    RdKafka::Headers* headers() override { return _headers; }
    RdKafka::Headers* headers(RdKafka::ErrorCode* err) override {
        if (err != nullptr) {
            *err = RdKafka::ERR_NO_ERROR;
        }
        return _headers;
    }

    // Unused by the builder; return defaults.
    std::string errstr() const override { return ""; }
    RdKafka::ErrorCode err() const override { return RdKafka::ERR_NO_ERROR; }
    RdKafka::Topic* topic() const override { return nullptr; }
    std::string topic_name() const override { return ""; }
    void* payload() const override { return nullptr; }
    size_t len() const override { return 0; }
    const std::string* key() const override { return nullptr; }
    void* msg_opaque() const override { return nullptr; }
    int64_t latency() const override { return 0; }
    struct rd_kafka_message_s* c_ptr() override {
        return nullptr;
    }
    RdKafka::Message::Status status() const override { return RdKafka::Message::MSG_STATUS_PERSISTED; }
    int32_t broker_id() const override { return 0; }
    int32_t leader_epoch() const override { return 0; }
    RdKafka::Error* offset_store() override { return nullptr; }

private:
    int32_t _partition;
    int64_t _offset;
    RdKafka::MessageTimestamp _ts{};
    std::string _key;
    bool _has_key = false;
    RdKafka::Headers* _headers = nullptr;
};

// need_meta=false: only partition/offset are stamped (for error logs); topic/timestamp/key/headers stay
// at their sentinels even though the message carries a key.
TEST_F(KafkaDataConsumerTest, build_kafka_message_meta_no_metadata) {
    MockKafkaMessage msg(3, 100);
    msg.set_key("k");
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    build_kafka_message_meta(msg, "orders", /*need_meta=*/false, /*need_key=*/true, /*need_headers=*/true, &meta);
    EXPECT_EQ(3, meta.partition());
    EXPECT_EQ(100, meta.offset());
    EXPECT_TRUE(meta.topic().empty());
    EXPECT_EQ(-1, meta.timestamp());
    EXPECT_FALSE(meta.has_key());
    EXPECT_TRUE(meta.headers().empty());
}

// need_meta=true with a create-time timestamp, key, and headers (including a null-valued header that
// renders as an empty string).
TEST_F(KafkaDataConsumerTest, build_kafka_message_meta_full) {
    MockKafkaMessage msg(7, 200);
    msg.set_timestamp(RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME, 1700000000000L);
    msg.set_key("mykey");
    msg.set_headers(RdKafka::Headers::create(
            {RdKafka::Headers::Header("h1", "v1", 2), RdKafka::Headers::Header("h2", nullptr, 0)}));
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    build_kafka_message_meta(msg, "orders", true, true, true, &meta);
    EXPECT_EQ(7, meta.partition());
    EXPECT_EQ(200, meta.offset());
    EXPECT_EQ("orders", meta.topic());
    EXPECT_EQ(1700000000000L, meta.timestamp());
    EXPECT_TRUE(meta.has_key());
    EXPECT_EQ("mykey", meta.key());
    ASSERT_EQ(2, meta.headers().size());
    EXPECT_EQ("h1", meta.headers()[0].first);
    EXPECT_EQ("v1", meta.headers()[0].second);
    EXPECT_EQ("h2", meta.headers()[1].first);
    EXPECT_EQ("", meta.headers()[1].second);
}

// need_meta=true but the KEY/HEADERS columns are not selected: the key/headers the message carries are
// not copied, and a not-available timestamp stays at the -1 sentinel.
TEST_F(KafkaDataConsumerTest, build_kafka_message_meta_gated) {
    MockKafkaMessage msg(1, 50);
    msg.set_key("secret");
    msg.set_headers(RdKafka::Headers::create({RdKafka::Headers::Header("h", "v", 1)}));
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    build_kafka_message_meta(msg, "t", true, /*need_key=*/false, /*need_headers=*/false, &meta);
    EXPECT_EQ("t", meta.topic());
    EXPECT_EQ(-1, meta.timestamp());
    EXPECT_FALSE(meta.has_key());
    EXPECT_TRUE(meta.headers().empty());
}

// need_meta=true with the columns selected but the message carries neither a key (key_pointer()==null)
// nor a headers wrapper (headers()==null).
TEST_F(KafkaDataConsumerTest, build_kafka_message_meta_absent_key_headers) {
    MockKafkaMessage msg(2, 9);
    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
    build_kafka_message_meta(msg, "t", true, true, true, &meta);
    EXPECT_EQ("t", meta.topic());
    EXPECT_FALSE(meta.has_key());
    EXPECT_TRUE(meta.headers().empty());
}

#ifndef __APPLE__
// build_pulsar_message_meta over a locally-built message: topic is the configured logical topic, the
// PARTITION index is parsed out of the per-message name, key/properties are surfaced, and to_string()
// exercises the PULSAR rendering branch. The message topic name is passed in rather than read via
// msg.getTopicName(), which dereferences a null impl on a MessageBuilder-built message.
TEST_F(KafkaDataConsumerTest, build_pulsar_message_meta_full) {
    pulsar::Message msg = pulsar::MessageBuilder()
                                  .setContent("payload")
                                  .setPartitionKey("pk")
                                  .setProperty("p1", "v1")
                                  .setEventTimestamp(1700000000000UL)
                                  .build();
    StreamMessageMeta meta(ByteBufferMetaType::PULSAR);
    build_pulsar_message_meta(msg, "my-topic", "persistent://public/default/my-topic-partition-3",
                              /*need_key=*/true, /*need_headers=*/true, &meta);
    EXPECT_EQ("my-topic", meta.topic());
    EXPECT_EQ(3, meta.partition());
    EXPECT_EQ(1700000000000L, meta.event_timestamp());
    EXPECT_TRUE(meta.has_key());
    EXPECT_EQ("pk", meta.key());
    ASSERT_EQ(1, meta.headers().size());
    EXPECT_EQ("p1", meta.headers()[0].first);
    EXPECT_EQ("v1", meta.headers()[0].second);
    EXPECT_NE(std::string::npos, meta.to_string().find("pulsar"));
}

// Pulsar KEY/HEADERS columns not selected: the partition key and properties are not copied. A
// non-partitioned message topic leaves PARTITION at its NULL sentinel.
TEST_F(KafkaDataConsumerTest, build_pulsar_message_meta_gated) {
    pulsar::Message msg = pulsar::MessageBuilder().setContent("x").setPartitionKey("pk").setProperty("p", "v").build();
    StreamMessageMeta meta(ByteBufferMetaType::PULSAR);
    build_pulsar_message_meta(msg, "my-topic", "my-topic", /*need_key=*/false, /*need_headers=*/false, &meta);
    EXPECT_EQ("my-topic", meta.topic());
    EXPECT_EQ(-1, meta.partition());
    EXPECT_FALSE(meta.has_key());
    EXPECT_TRUE(meta.headers().empty());
}
#endif

// PulsarLoadInfo maps the INCLUDE METADATA consumer gates out of the thrift struct.
TEST_F(KafkaDataConsumerTest, pulsar_load_info_metadata_flags) {
    TPulsarLoadInfo t;
    t.service_url = "pulsar://localhost:6650";
    t.topic = "my-topic";
    t.subscription = "sub";
    t.partitions = {"my-topic-partition-0"};
    t.__set_need_source_metadata(true);
    t.__set_need_message_key(true);
    t.__set_need_message_headers(true);
    PulsarLoadInfo info(t);
    EXPECT_EQ("my-topic", info.topic);
    EXPECT_TRUE(info.need_source_metadata);
    EXPECT_TRUE(info.need_message_key);
    EXPECT_TRUE(info.need_message_headers);

    // Unset flags default to false.
    TPulsarLoadInfo t2;
    t2.service_url = "pulsar://localhost:6650";
    t2.topic = "my-topic";
    t2.subscription = "sub";
    PulsarLoadInfo info2(t2);
    EXPECT_FALSE(info2.need_source_metadata);
    EXPECT_FALSE(info2.need_message_key);
    EXPECT_FALSE(info2.need_message_headers);
}

TEST(PulsarTopicTest, parse_partition_index_anchored_on_configured_topic) {
    // Partitioned topic: the "-partition-N" suffix sits right after the configured logical topic.
    EXPECT_EQ(3, parse_pulsar_partition_index("persistent://public/default/my-topic",
                                              "persistent://public/default/my-topic-partition-3"));

    // Partition 0 is a real partition, not "absent".
    EXPECT_EQ(0, parse_pulsar_partition_index("my-topic", "my-topic-partition-0"));

    // Configured topic in short form still anchors against the canonical per-partition name: the match
    // lands at the leading "/" boundary.
    EXPECT_EQ(7, parse_pulsar_partition_index("my-topic", "persistent://public/default/my-topic-partition-7"));

    // Non-partitioned topic: message name equals the configured topic, no suffix -> -1 (SQL NULL).
    EXPECT_EQ(-1, parse_pulsar_partition_index("persistent://public/default/my-topic",
                                               "persistent://public/default/my-topic"));
}

TEST(PulsarTopicTest, standalone_partition_suffix_topic_is_not_a_partition) {
    // A standalone non-partitioned topic literally named "<x>-partition-<n>" is configured as the
    // logical topic verbatim, so the trailing suffix must NOT be read as a partition index.
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic-partition-5", "my-topic-partition-5"));
    EXPECT_EQ(-1, parse_pulsar_partition_index("persistent://public/default/my-topic-partition-5",
                                               "persistent://public/default/my-topic-partition-5"));

    // A real partition of such a (pathologically named) partitioned topic still resolves correctly:
    // its partitions are "<x>-partition-5-partition-N".
    EXPECT_EQ(2, parse_pulsar_partition_index("my-topic-partition-5", "my-topic-partition-5-partition-2"));
}

TEST(PulsarTopicTest, malformed_or_foreign_suffix_is_not_treated_as_partition) {
    // Empty index, non-numeric index, and a trailing-dash suffix all yield -1.
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic", "my-topic-partition-"));
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic", "my-topic-partition-x"));
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic", "my-topic-partition-1a"));

    // A value past INT32_MAX is rejected (overflow guard) rather than wrapping.
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic", "my-topic-partition-99999999999"));

    // The prefix before "-partition-N" must be exactly the configured topic, not merely end with it:
    // "other-my-topic" is a different topic and yields -1 rather than a fabricated partition.
    EXPECT_EQ(-1, parse_pulsar_partition_index("my-topic", "other-my-topic-partition-2"));

    // Only the last "-partition-N" is considered; an earlier occurrence belongs to the topic name.
    EXPECT_EQ(2, parse_pulsar_partition_index("a-partition-1-b", "a-partition-1-b-partition-2"));
}

TEST_F(KafkaDataConsumerTest, test_get_partition_offset_broker_down) {
    TKafkaLoadInfo tKafkaLoadInfo;
    tKafkaLoadInfo.brokers = "localhost:19092";
    tKafkaLoadInfo.topic = "test_topic";
    tKafkaLoadInfo.partition_begin_offset = {{0, 100}};
    auto kafka_info = std::make_unique<KafkaLoadInfo>(tKafkaLoadInfo);
    StreamLoadContext context(ExecEnv::GetInstance());
    context.kafka_info = std::move(kafka_info);

    KafkaDataConsumer consumer(&context);
    ASSERT_OK(consumer.init(&context));

    std::vector<int32_t> partition_ids{0};
    std::vector<int64_t> beginning_offsets;
    std::vector<int64_t> latest_offsets;
    int timeout = 10;
    auto st = consumer.get_partition_offset(&partition_ids, &beginning_offsets, &latest_offsets, timeout);
    std::cout << "get partition offset st: " << st << std::endl;
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("Local: All broker connections are down") != string::npos);
    ASSERT_OK(consumer.reset());
}

TEST_F(KafkaDataConsumerTest, test_get_partition_meta_broker_down) {
    TKafkaLoadInfo tKafkaLoadInfo;
    tKafkaLoadInfo.brokers = "localhost:19092";
    tKafkaLoadInfo.topic = "test_topic";
    tKafkaLoadInfo.partition_begin_offset = {{0, 100}};
    auto kafka_info = std::make_unique<KafkaLoadInfo>(tKafkaLoadInfo);
    StreamLoadContext context(ExecEnv::GetInstance());
    context.kafka_info = std::move(kafka_info);

    KafkaDataConsumer consumer(&context);
    ASSERT_OK(consumer.init(&context));

    std::vector<int32_t> partition_ids{0};
    int timeout = 10;
    auto st = consumer.get_partition_meta(&partition_ids, timeout);
    std::cout << "get partition meta st: " << st << std::endl;
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.message().find("Local: Broker transport failure") != string::npos);
    ASSERT_OK(consumer.reset());
}

} // namespace starrocks
