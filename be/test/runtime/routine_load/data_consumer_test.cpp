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

#include "base/testutil/assert.h"
#include "runtime/exec_env.h"
#include "runtime/routine_load/data_consumer_group.h"

namespace starrocks {

class KafkaDataConsumerTest : public testing::Test {};

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
    ASSERT_TRUE(st.message().find("Local: All broker connections are down") != std::string::npos);
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
    ASSERT_TRUE(st.message().find("Local: Broker transport failure") != std::string::npos);
    ASSERT_OK(consumer.reset());
}

} // namespace starrocks
