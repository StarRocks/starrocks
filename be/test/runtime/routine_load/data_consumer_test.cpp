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

#include <chrono>
#include <map>
#include <thread>

#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafka_mock.h"

#include "runtime/exec_env.h"
#include "testutil/assert.h"

namespace starrocks {

class KafkaDataConsumerTest : public testing::Test {};

// Exercise the public consumer API against librdkafka's in-process mock broker.
// A small receiving queue hands off message ownership to the test immediately.
class KafkaDataConsumerEofTest : public testing::Test {
protected:
    void SetUp() override {
        std::string error;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("test.mock.num.brokers", "1", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("log_level", "0", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("linger.ms", "0", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("batch.num.messages", "1", error));
        _producer.reset(RdKafka::Producer::create(conf.get(), error));
        ASSERT_NE(nullptr, _producer) << error;
        auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
        ASSERT_NE(nullptr, cluster);
        ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_topic_create(cluster, _topic.c_str(), 3, 1));

        TKafkaLoadInfo info;
        info.brokers = rd_kafka_mock_cluster_bootstraps(cluster);
        info.topic = _topic;
        info.properties = {{"group.id", "routine-load-eof-test"}, {"enable.auto.commit", "false"},
                           {"fetch.wait.max.ms", "10"},           {"fetch.message.max.bytes", "1024"},
                           {"queued.min.messages", "100000"},     {"queued.max.messages.kbytes", "64"},
                           {"fetch.queue.backoff.ms", "10"}};
        _context = std::make_unique<StreamLoadContext>(ExecEnv::GetInstance());
        _context->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        _consumer = std::make_unique<KafkaDataConsumer>(_context.get());
        ASSERT_OK(_consumer->init(_context.get()));
    }

    RdKafka::ErrorCode produce(int32_t partition, int count) {
        std::string payload(1024, 'x');
        for (int i = 0; i < count; ++i) {
            auto error = _producer->produce(_topic, partition, RdKafka::Producer::RK_MSG_COPY, payload.data(),
                                            payload.size(), nullptr, 0, 0, nullptr);
            if (error != RdKafka::ERR_NO_ERROR) {
                return error;
            }
        }
        return _producer->flush(10000);
    }

    struct BatchResult {
        Status status;
        std::map<int32_t, int> data_rows;
        std::map<int32_t, int> eof_events;
        bool data_after_eof = false;
        RdKafka::ErrorCode produce_error = RdKafka::ERR_NO_ERROR;
    };

    BatchResult consume_batch(const std::map<int32_t, int64_t>& offsets, int records_after_first_eof = 0) {
        BatchResult result;
        result.status = _consumer->assign_topic_partitions(offsets, _topic, _context.get());
        if (!result.status.ok()) {
            return result;
        }
        TimedBlockingQueue<RdKafka::Message*> queue(1);
        std::thread worker([&] {
            result.status = _consumer->group_consume(&queue, 30000);
            queue.shutdown();
        });
        RdKafka::Message* raw_message;
        while (queue.blocking_get(&raw_message)) {
            std::unique_ptr<RdKafka::Message> message(raw_message);
            const int32_t partition = message->partition();
            if (message->err() == RdKafka::ERR_NO_ERROR) {
                ++result.data_rows[partition];
                if (partition == 0 && result.eof_events[0] > 0) {
                    result.data_after_eof = true;
                }
                // Keep the other partition backlogged while the caught-up
                // partition produces EOF -> data -> EOF through the real SDK.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } else if (message->err() == RdKafka::ERR__PARTITION_EOF) {
                ++result.eof_events[partition];
                if (records_after_first_eof > 0 && partition == 0 && result.eof_events[0] == 1) {
                    result.produce_error = produce(0, records_after_first_eof);
                    if (result.produce_error != RdKafka::ERR_NO_ERROR) {
                        queue.shutdown();
                    }
                }
            }
        }
        worker.join();
        return result;
    }

    const std::string _topic = "routine_load_eof_test";
    std::unique_ptr<RdKafka::Producer> _producer;
    std::unique_ptr<StreamLoadContext> _context;
    std::unique_ptr<KafkaDataConsumer> _consumer;
};

TEST_F(KafkaDataConsumerEofTest, repeated_eof_does_not_stop_another_partition) {
    constexpr int backlog = 2000;
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(1, backlog));
    auto result = consume_batch({{0, 0}, {1, 0}}, 1);
    ASSERT_OK(result.status);
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, result.produce_error);
    ASSERT_TRUE(result.data_after_eof);
    ASSERT_GE(result.eof_events[0], 2);
    EXPECT_EQ(backlog, result.data_rows[1]);
    EXPECT_EQ(1, result.eof_events[1]);
}

TEST_F(KafkaDataConsumerEofTest, new_data_clears_a_partitions_previous_eof) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(1, 1000));
    auto result = consume_batch({{0, 0}, {1, 0}}, 2000);
    ASSERT_OK(result.status);
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, result.produce_error);
    ASSERT_TRUE(result.data_after_eof);
    EXPECT_EQ(2000, result.data_rows[0]);
    EXPECT_EQ(1000, result.data_rows[1]);
    EXPECT_GE(result.eof_events[0], 2);
    EXPECT_EQ(1, result.eof_events[1]);
}

TEST_F(KafkaDataConsumerEofTest, reassign_starts_with_clean_eof_state) {
    auto first = consume_batch({{0, 0}});
    ASSERT_OK(first.status);
    ASSERT_EQ(1, first.eof_events[0]);

    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(2, 2000));
    auto second = consume_batch({{1, 0}, {2, 0}});
    ASSERT_OK(second.status);
    EXPECT_EQ(2000, second.data_rows[2]);
    EXPECT_EQ(1, second.eof_events[1]);
    EXPECT_EQ(1, second.eof_events[2]);
}

TEST_F(KafkaDataConsumerEofTest, reset_allows_reuse_after_cancel_and_eof) {
    auto first = consume_batch({{0, 0}});
    ASSERT_OK(first.status);
    ASSERT_EQ(1, first.eof_events[0]);
    ASSERT_OK(_consumer->cancel(_context.get()));
    ASSERT_OK(_consumer->reset());

    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(1, 10));
    auto second = consume_batch({{1, 0}});
    ASSERT_OK(second.status);
    EXPECT_EQ(10, second.data_rows[1]);
    EXPECT_EQ(1, second.eof_events[1]);
}

TEST_F(KafkaDataConsumerEofTest, cancelled_batch_does_not_enqueue_and_reset_resumes_consumption) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, 1));
    ASSERT_OK(_consumer->assign_topic_partitions({{0, 0}}, _topic, _context.get()));
    ASSERT_OK(_consumer->cancel(_context.get()));

    // Allow the data and EOF to fit if cancellation regresses, so the
    // assertion reports unexpected messages instead of blocking the test.
    TimedBlockingQueue<RdKafka::Message*> queue(2);
    auto status = _consumer->group_consume(&queue, 30000);
    queue.shutdown();
    int queued_messages = 0;
    RdKafka::Message* raw_message;
    while (queue.blocking_get(&raw_message)) {
        std::unique_ptr<RdKafka::Message> message(raw_message);
        ++queued_messages;
    }
    ASSERT_OK(status);
    EXPECT_EQ(0, queued_messages);

    ASSERT_OK(_consumer->reset());
    auto resumed = consume_batch({{0, 0}});
    ASSERT_OK(resumed.status);
    EXPECT_EQ(1, resumed.data_rows[0]);
    EXPECT_EQ(1, resumed.eof_events[0]);
}

TEST_F(KafkaDataConsumerEofTest, topic_authorization_failure_returns_error_without_eof) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, 1));
    auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
    ASSERT_NE(nullptr, cluster);
    // Inject before assign so the error cannot race the first Fetch request.
    rd_kafka_mock_push_request_errors(cluster, 1 /* Fetch */, 1, RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED);

    auto result = consume_batch({{0, 0}});
    ASSERT_FALSE(result.status.ok());
    EXPECT_NE(std::string::npos, result.status.to_string().find("READ permission"));
    EXPECT_TRUE(result.data_rows.empty());
    EXPECT_TRUE(result.eof_events.empty());
}

TEST_F(KafkaDataConsumerEofTest, closed_queue_keeps_message_ownership_with_consumer) {
    ASSERT_OK(_consumer->assign_topic_partitions({{0, 0}}, _topic, _context.get()));
    TimedBlockingQueue<RdKafka::Message*> queue(1);
    queue.shutdown();
    ASSERT_OK(_consumer->group_consume(&queue, 30000));
    RdKafka::Message* message = nullptr;
    EXPECT_FALSE(queue.blocking_get(&message));
    EXPECT_EQ(nullptr, message);
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
