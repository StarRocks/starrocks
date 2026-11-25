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

#include "data_workflows/load/routine_load/data_consumer_group.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/concurrency/stopwatch.hpp"
#include "base/testutil/assert.h"
#include "data_workflows/load/routine_load/data_consumer.h"
#include "data_workflows/load/routine_load/kafka_consumer_pipe.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafka_mock.h"
#include "librdkafka/rdkafkacpp.h"

namespace starrocks {

// Consumes a topic of librdkafka's in-process mock cluster the way a Kafka scan range of the coordinated routine
// load mode does: into a non-blocking pipe, with the window ending once every partition has been at its end for
// kafka_idle_end_ms. Each of the two partitions gets its own consumer of the group.
class KafkaScanRangeConsumeTest : public testing::Test {
protected:
    void SetUp() override {
        std::string error;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("test.mock.num.brokers", "1", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("log_level", "0", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("linger.ms", "0", error));
        _producer.reset(RdKafka::Producer::create(conf.get(), error));
        ASSERT_NE(nullptr, _producer) << error;
        auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
        ASSERT_NE(nullptr, cluster);
        ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_topic_create(cluster, _topic.c_str(), 2, 1));
        _brokers = rd_kafka_mock_cluster_bootstraps(cluster);
    }

    RdKafka::ErrorCode produce(int32_t partition, const std::vector<std::string>& payloads) {
        for (const auto& payload : payloads) {
            auto error = _producer->produce(_topic, partition, RdKafka::Producer::RK_MSG_COPY,
                                            const_cast<char*>(payload.data()), payload.size(), nullptr, 0, 0, nullptr);
            if (error != RdKafka::ERR_NO_ERROR) {
                return error;
            }
        }
        return _producer->flush(10000);
    }

    std::unique_ptr<StreamLoadContext> make_context(bool scan_range_mode) {
        TKafkaLoadInfo info;
        info.brokers = _brokers;
        info.topic = _topic;
        info.partition_begin_offset = {{0, 0}, {1, 0}};
        info.__set_properties({{"group.id", "routine-load-scan-range-test"}, {"fetch.wait.max.ms", "10"}});
        auto ctx = std::make_unique<StreamLoadContext>(nullptr);
        ctx->load_type = TLoadType::ROUTINE_LOAD;
        ctx->load_src_type = TLoadSourceType::KAFKA;
        ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
        // an upper bound only: the window ends once both partitions have been at their end for kafka_idle_end_ms
        ctx->max_interval_ms = 30000;
        ctx->kafka_scan_range_mode = scan_range_mode;
        if (scan_range_mode) {
            ctx->kafka_row_delimiter = '\n';
            ctx->kafka_idle_end_ms = 200;
        }
        _pipe = std::make_shared<KafkaConsumerPipe>(true, DEFAULT_STREAM_LOAD_PIPE_NON_BLOCKING_WAIT_US,
                                                    DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES,
                                                    DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE);
        ctx->body_sink = _pipe;
        return ctx;
    }

    // Builds the group the way DataConsumerPool::get_consumer_grp does for two partitions: one new consumer each.
    Status consume(StreamLoadContext* ctx) {
        const size_t partition_num = ctx->kafka_info->begin_offset.size();
        auto grp = std::make_shared<KafkaDataConsumerGroup>(partition_num);
        for (size_t i = 0; i < partition_num; ++i) {
            auto consumer = std::make_shared<KafkaDataConsumer>(ctx);
            RETURN_IF_ERROR(consumer->init(ctx));
            grp->add_consumer(consumer);
        }
        RETURN_IF_ERROR(grp->assign_topic_partitions(ctx));
        return grp->start_all(ctx);
    }

    // Reads the finished pipe up to its end, as a scanner would.
    StatusOr<std::string> read_all() {
        std::string data;
        char buf[1024];
        while (true) {
            size_t size = sizeof(buf);
            bool eof = false;
            RETURN_IF_ERROR(_pipe->read(reinterpret_cast<uint8_t*>(buf), &size, &eof));
            if (eof) {
                return data;
            }
            data.append(buf, size);
        }
    }

    const std::string _topic = "routine_load_scan_range_test";
    std::string _brokers;
    std::unique_ptr<RdKafka::Producer> _producer;
    std::shared_ptr<KafkaConsumerPipe> _pipe;
};

// A round may hand a scan range partitions without new data. Unlike a classic task, the consumption then ends the
// scan range normally: it reports the offsets unchanged and finishes the pipe instead of cancelling it.
TEST_F(KafkaScanRangeConsumeTest, nothing_to_consume_finishes_the_pipe) {
    auto ctx = make_context(true);
    int callbacks = 0;
    bool finished_at_callback = true;
    std::map<int32_t, int64_t> offsets_at_callback;
    ctx->kafka_consumed_callback = [&](StreamLoadContext* consumed_ctx) {
        ++callbacks;
        finished_at_callback = _pipe->finished();
        offsets_at_callback = consumed_ctx->kafka_info->cmt_offset;
    };

    ASSERT_OK(consume(ctx.get()));
    EXPECT_EQ(1, callbacks);
    EXPECT_FALSE(finished_at_callback);
    std::map<int32_t, int64_t> unchanged{{0, -1}, {1, -1}};
    EXPECT_EQ(unchanged, offsets_at_callback);
    EXPECT_EQ(unchanged, ctx->kafka_info->cmt_offset);
    EXPECT_EQ(0, ctx->receive_bytes);
    EXPECT_EQ(0, ctx->receive_rows);
    EXPECT_TRUE(ctx->kafka_reached_end);
    EXPECT_TRUE(_pipe->finished());
    auto data = read_all();
    ASSERT_OK(data.status());
    EXPECT_TRUE(data.value().empty());
}

// The consumed offsets reach the callback before the pipe finishes, so they are published before the scanner can
// see the end of the pipe.
TEST_F(KafkaScanRangeConsumeTest, consumed_offsets_are_published_before_the_pipe_finishes) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2", "c,3"}));
    auto ctx = make_context(true);
    int callbacks = 0;
    bool finished_at_callback = true;
    std::map<int32_t, int64_t> offsets_at_callback;
    int64_t rows_at_callback = 0;
    size_t bytes_at_callback = 0;
    ctx->kafka_consumed_callback = [&](StreamLoadContext* consumed_ctx) {
        ++callbacks;
        finished_at_callback = _pipe->finished();
        offsets_at_callback = consumed_ctx->kafka_info->cmt_offset;
        rows_at_callback = consumed_ctx->receive_rows;
        bytes_at_callback = consumed_ctx->receive_bytes;
    };

    ASSERT_OK(consume(ctx.get()));
    EXPECT_EQ(1, callbacks);
    EXPECT_FALSE(finished_at_callback);
    std::map<int32_t, int64_t> expected_offsets{{0, 2}, {1, -1}};
    EXPECT_EQ(expected_offsets, offsets_at_callback);
    EXPECT_EQ(3, rows_at_callback);
    EXPECT_EQ(9, bytes_at_callback);
    EXPECT_TRUE(ctx->kafka_reached_end);
    EXPECT_TRUE(_pipe->finished());
    auto data = read_all();
    ASSERT_OK(data.status());
    EXPECT_EQ("a,1\nb,2\nc,3\n", data.value());
}

// A consumption that stops at its size limit with data left has not reached the end, so that the FE starts the next
// round at once instead of after the load interval.
TEST_F(KafkaScanRangeConsumeTest, stopping_at_the_size_limit_does_not_reach_the_end) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, std::vector<std::string>(100, "a,1")));
    auto ctx = make_context(true);
    ctx->max_batch_size = 6;
    ctx->kafka_consumed_callback = [](StreamLoadContext*) {};

    ASSERT_OK(consume(ctx.get()));
    EXPECT_LT(ctx->receive_rows, 100);
    EXPECT_FALSE(ctx->kafka_reached_end);
}

// A consumer whose partitions have all reached their end keeps consuming them while the scan range goes on: partition
// 1 gets its messages only after both partitions were at their end, and the window still consumes them.
TEST_F(KafkaScanRangeConsumeTest, a_partition_that_caught_up_keeps_being_consumed) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1"}));
    auto ctx = make_context(true);
    ctx->kafka_idle_end_ms = 1000;
    ctx->kafka_consumed_callback = [](StreamLoadContext*) {};
    // one message every 200ms, well within the idle time, so the window lasts until the last one
    std::thread trickle([this] {
        for (int i = 0; i < 5; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            EXPECT_EQ(RdKafka::ERR_NO_ERROR, produce(1, {"b,2"}));
        }
    });
    auto st = consume(ctx.get());
    trickle.join();
    ASSERT_OK(st);
    EXPECT_EQ(6, ctx->receive_rows);
    std::map<int32_t, int64_t> expected_offsets{{0, 0}, {1, 4}};
    EXPECT_EQ(expected_offsets, ctx->kafka_info->cmt_offset);
    EXPECT_TRUE(ctx->kafka_reached_end);
}

// Once every partition has been at its end, with no new message, for kafka_idle_end_ms, the window ends long before its
// consume time, having reached the end.
TEST_F(KafkaScanRangeConsumeTest, the_window_ends_once_its_partitions_stay_at_their_end) {
    ASSERT_EQ(RdKafka::ERR_NO_ERROR, produce(0, {"a,1", "b,2"}));
    auto ctx = make_context(true);
    ctx->kafka_idle_end_ms = 300;
    ctx->kafka_consumed_callback = [](StreamLoadContext*) {};
    MonotonicStopWatch watch;
    watch.start();
    ASSERT_OK(consume(ctx.get()));
    const int64_t elapsed_ms = watch.elapsed_time() / 1000 / 1000;
    EXPECT_EQ(2, ctx->receive_rows);
    EXPECT_TRUE(ctx->kafka_reached_end);
    EXPECT_GE(elapsed_ms, 300);
    EXPECT_LT(elapsed_ms, 10000);
}

// A classic task keeps cancelling the pipe of a window that consumed nothing, and has no callback to call.
TEST_F(KafkaScanRangeConsumeTest, classic_task_cancels_the_pipe_when_nothing_to_consume) {
    auto ctx = make_context(false);
    auto st = consume(ctx.get());
    EXPECT_TRUE(st.is_cancelled()) << st;
    EXPECT_FALSE(_pipe->finished());
    auto data = read_all();
    EXPECT_TRUE(data.status().is_cancelled()) << data.status();
}

} // namespace starrocks
