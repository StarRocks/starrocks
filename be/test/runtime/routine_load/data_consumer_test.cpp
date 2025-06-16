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

#include "runtime/exec_env.h"
#include "testutil/assert.h"

namespace starrocks {

class KafkaDataConsumerTest : public testing::Test {};

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
