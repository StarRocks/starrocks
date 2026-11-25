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

#include "data_workflows/load/routine_load/data_consumer_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "common/config_ingest_fwd.h"
#include "data_workflows/load/routine_load/data_consumer_group.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafka_mock.h"
#include "librdkafka/rdkafkacpp.h"

namespace starrocks {

// The consumers point at librdkafka's in-process mock cluster, so creating and closing them needs no broker.
class DataConsumerPoolTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_pool_size = config::routine_load_kafka_consumer_pool_size;

        std::string error;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("test.mock.num.brokers", "1", error));
        ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->set("log_level", "0", error));
        _producer.reset(RdKafka::Producer::create(conf.get(), error));
        ASSERT_NE(nullptr, _producer) << error;
        auto* cluster = rd_kafka_handle_mock_cluster(_producer->c_ptr());
        ASSERT_NE(nullptr, cluster);
        ASSERT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_mock_topic_create(cluster, _topic.c_str(), 1, 1));

        TKafkaLoadInfo info;
        info.brokers = rd_kafka_mock_cluster_bootstraps(cluster);
        info.topic = _topic;
        info.partition_begin_offset = {{0, 0}};
        info.__set_properties({{"group.id", "routine-load-pool-test"}});
        _context = std::make_unique<StreamLoadContext>(nullptr);
        _context->load_type = TLoadType::ROUTINE_LOAD;
        _context->load_src_type = TLoadSourceType::KAFKA;
        _context->kafka_info = std::make_unique<KafkaLoadInfo>(info);
    }

    void TearDown() override { config::routine_load_kafka_consumer_pool_size = _saved_pool_size; }

    std::vector<std::shared_ptr<DataConsumer>> get_consumers(DataConsumerPool* pool, int num) {
        std::vector<std::shared_ptr<DataConsumer>> consumers;
        for (int i = 0; i < num; ++i) {
            std::shared_ptr<DataConsumer> consumer;
            EXPECT_OK(pool->get_consumer(_context.get(), &consumer));
            consumers.push_back(consumer);
        }
        return consumers;
    }

    static std::vector<UniqueId> pooled_ids(DataConsumerPool* pool) {
        std::vector<UniqueId> ids;
        for (const auto& consumer : pool->_pool) {
            ids.push_back(consumer->id());
        }
        return ids;
    }

    static std::vector<UniqueId> committing_ids(DataConsumerPool* pool) {
        std::vector<UniqueId> ids;
        for (const auto& consumer : pool->_committing) {
            ids.push_back(consumer->id());
        }
        return ids;
    }

    const std::string _topic = "routine_load_pool_test";
    int32_t _saved_pool_size = 0;
    std::unique_ptr<RdKafka::Producer> _producer;
    std::unique_ptr<StreamLoadContext> _context;
};

TEST_F(DataConsumerPoolTest, return_marks_the_consumer_visited) {
    config::routine_load_kafka_consumer_pool_size = 4;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 1);
    consumers[0]->set_last_visit_time(0);

    const time_t before = time(nullptr);
    (void)pool.return_consumer(consumers[0]);
    EXPECT_GE(consumers[0]->last_visit_time(), before);
}

// A full pool makes room by evicting the consumer idle for the longest, whatever the order it was returned in.
TEST_F(DataConsumerPoolTest, full_pool_evicts_the_consumer_idle_for_the_longest) {
    config::routine_load_kafka_consumer_pool_size = 2;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 3);
    (void)pool.return_consumer(consumers[0]);
    (void)pool.return_consumer(consumers[1]);
    const time_t now = time(nullptr);
    consumers[0]->set_last_visit_time(now - 10);
    consumers[1]->set_last_visit_time(now - 100);

    (void)pool.return_consumer(consumers[2]);
    std::vector<UniqueId> expected{consumers[0]->id(), consumers[2]->id()};
    EXPECT_EQ(expected, pooled_ids(&pool));
}

TEST_F(DataConsumerPoolTest, lowered_pool_size_shrinks_the_pool) {
    config::routine_load_kafka_consumer_pool_size = 3;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 4);
    for (int i = 0; i < 3; ++i) {
        (void)pool.return_consumer(consumers[i]);
    }
    ASSERT_EQ(3, pooled_ids(&pool).size());

    config::routine_load_kafka_consumer_pool_size = 1;
    (void)pool.return_consumer(consumers[3]);
    std::vector<UniqueId> expected{consumers[3]->id()};
    EXPECT_EQ(expected, pooled_ids(&pool));
}

TEST_F(DataConsumerPoolTest, returning_a_group_transfers_all_evicted_consumers_to_the_caller) {
    config::routine_load_kafka_consumer_pool_size = 2;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 4);
    (void)pool.return_consumer(consumers[0]);
    (void)pool.return_consumer(consumers[1]);
    std::weak_ptr<DataConsumer> first_evicted = consumers[0];
    std::weak_ptr<DataConsumer> second_evicted = consumers[1];

    auto group = std::make_unique<DataConsumerGroup>(2);
    group->add_consumer(consumers[2]);
    group->add_consumer(consumers[3]);
    // Leave the idle consumers owned only by the pool, as after an earlier task has finished.
    consumers.clear();

    auto evicted = pool.return_consumers(group.get());
    ASSERT_EQ(2, evicted.size());
    group.reset();
    EXPECT_FALSE(first_evicted.expired());
    EXPECT_FALSE(second_evicted.expired());
    EXPECT_EQ(2, pooled_ids(&pool).size());

    // The task handles its commit or rollback while holding these references. Only releasing them closes
    // the old consumers; returning the group must not wait for their potentially slow broker close.
    evicted.clear();
    EXPECT_TRUE(first_evicted.expired());
    EXPECT_TRUE(second_evicted.expired());
}

TEST_F(DataConsumerPoolTest, shrinking_or_disabling_the_pool_transfers_every_unpooled_consumer) {
    for (int32_t new_pool_size : {1, 0}) {
        SCOPED_TRACE(new_pool_size);
        config::routine_load_kafka_consumer_pool_size = 3;
        DataConsumerPool pool;
        auto consumers = get_consumers(&pool, 5);
        std::vector<std::weak_ptr<DataConsumer>> tracked(consumers.begin(), consumers.end());
        for (int i = 0; i < 3; ++i) {
            (void)pool.return_consumer(consumers[i]);
        }
        auto group = std::make_unique<DataConsumerGroup>(2);
        group->add_consumer(consumers[3]);
        group->add_consumer(consumers[4]);
        consumers.clear();

        config::routine_load_kafka_consumer_pool_size = new_pool_size;
        auto evicted = pool.return_consumers(group.get());
        group.reset();
        ASSERT_EQ(5 - new_pool_size, evicted.size());
        EXPECT_EQ(new_pool_size, pooled_ids(&pool).size());
        for (const auto& consumer : tracked) {
            EXPECT_FALSE(consumer.expired());
        }

        evicted.clear();
        // A pool of size one keeps only the last returned group member. A disabled pool keeps none.
        for (size_t i = 0; i < tracked.size(); ++i) {
            EXPECT_EQ(new_pool_size == 0 || i + 1 < tracked.size(), tracked[i].expired());
        }
    }
}

TEST_F(DataConsumerPoolTest, zero_pool_size_keeps_nothing) {
    config::routine_load_kafka_consumer_pool_size = 0;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 1);
    (void)pool.return_consumer(consumers[0]);
    EXPECT_TRUE(pooled_ids(&pool).empty());
}

TEST_F(DataConsumerPoolTest, pooled_consumer_is_reused) {
    config::routine_load_kafka_consumer_pool_size = 4;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 1);
    (void)pool.return_consumer(consumers[0]);

    auto reused = get_consumers(&pool, 1);
    EXPECT_EQ(consumers[0]->id(), reused[0]->id());
    EXPECT_TRUE(pooled_ids(&pool).empty());
}

// Reports whether the pool lock is free while the consumer is reset.
class LockProbingConsumer : public DataConsumer {
public:
    explicit LockProbingConsumer(DataConsumerPool* pool) : _pool(pool) {}
    Status init(StreamLoadContext*) override { return Status::OK(); }
    Status consume(StreamLoadContext*) override { return Status::OK(); }
    Status cancel(StreamLoadContext*) override { return Status::OK(); }
    Status reset() override {
        // Probe from another thread: try_lock() is undefined for the thread that owns the mutex.
        std::thread([this] {
            pool_lock_was_free = _pool->_lock.try_lock();
            if (pool_lock_was_free) {
                _pool->_lock.unlock();
            }
        }).join();
        return Status::OK();
    }
    bool match(StreamLoadContext*) override { return false; }

    bool pool_lock_was_free = false;

private:
    DataConsumerPool* _pool;
};

// A reset can take a while: a Kafka consumer drops the messages it has fetched and a Pulsar consumer closes its
// subscription with the broker. Meanwhile the other tasks must still get to the pool.
TEST_F(DataConsumerPoolTest, returned_consumer_is_reset_before_taking_the_pool_lock) {
    config::routine_load_kafka_consumer_pool_size = 4;
    DataConsumerPool pool;
    auto consumer = std::make_shared<LockProbingConsumer>(&pool);
    (void)pool.return_consumer(consumer);
    EXPECT_TRUE(consumer->pool_lock_was_free);
    std::vector<UniqueId> expected{consumer->id()};
    EXPECT_EQ(expected, pooled_ids(&pool));
}

TEST_F(DataConsumerPoolTest, clean_idle_consumers) {
    config::routine_load_kafka_consumer_pool_size = 4;
    DataConsumerPool pool;
    auto consumers = get_consumers(&pool, 2);
    (void)pool.return_consumer(consumers[0]);
    (void)pool.return_consumer(consumers[1]);
    consumers[0]->set_last_visit_time(time(nullptr) - 601);

    pool._clean_idle_consumer_bg();
    std::vector<UniqueId> expected{consumers[1]->id()};
    EXPECT_EQ(expected, pooled_ids(&pool));
}

// A consumer whose commit is on its way while the test says so. It matches every request.
class CommitInFlightConsumer : public DataConsumer {
public:
    Status init(StreamLoadContext*) override { return Status::OK(); }
    Status consume(StreamLoadContext*) override { return Status::OK(); }
    Status cancel(StreamLoadContext*) override { return Status::OK(); }
    Status reset() override { return Status::OK(); }
    bool match(StreamLoadContext*) override { return true; }
    bool has_commit_in_flight() override { return commit_in_flight.load(); }

    std::atomic<bool> commit_in_flight{true};
};

// A consumer whose commit is still on its way would fetch nothing for a new assignment until the commit completes, and
// closing it would wait for the commit. The pool keeps it apart and evicts nothing for it, a disabled pool too: the
// rounds get other consumers, and the later commits of its group share it.
TEST_F(DataConsumerPoolTest, consumer_with_a_commit_in_flight_is_kept_apart) {
    for (int32_t pool_size : {4, 0}) {
        config::routine_load_kafka_consumer_pool_size = pool_size;
        DataConsumerPool pool;
        auto committing = std::make_shared<CommitInFlightConsumer>();
        EXPECT_TRUE(pool.return_consumer(committing).empty());
        EXPECT_TRUE(pooled_ids(&pool).empty());
        std::vector<UniqueId> expected{committing->id()};
        EXPECT_EQ(expected, committing_ids(&pool));

        std::shared_ptr<DataConsumer> consumer;
        ASSERT_OK(pool.get_consumer(_context.get(), &consumer));
        EXPECT_NE(committing->id(), consumer->id());
        ASSERT_OK(pool.get_consumer_for_commit(_context.get(), &consumer));
        EXPECT_EQ(committing->id(), consumer->id());
        EXPECT_EQ(expected, committing_ids(&pool));
        // returned again by a commit that shared it, it stays apart once
        EXPECT_TRUE(pool.return_consumer(consumer).empty());
        EXPECT_EQ(expected, committing_ids(&pool));
    }
}

// A consumer kept apart is reused once its commits have completed and no commit uses it any more.
TEST_F(DataConsumerPoolTest, consumer_kept_apart_is_reused_once_its_commits_completed) {
    config::routine_load_kafka_consumer_pool_size = 4;
    DataConsumerPool pool;
    auto committing = std::make_shared<CommitInFlightConsumer>();
    const UniqueId id = committing->id();
    (void)pool.return_consumer(committing);
    committing->commit_in_flight = false;

    // a commit still uses it
    std::shared_ptr<DataConsumer> consumer;
    ASSERT_OK(pool.get_consumer(_context.get(), &consumer));
    EXPECT_NE(id, consumer->id());

    committing.reset();
    ASSERT_OK(pool.get_consumer(_context.get(), &consumer));
    EXPECT_EQ(id, consumer->id());
    EXPECT_TRUE(committing_ids(&pool).empty());
}

// The background worker pools a consumer kept apart once its commits have completed and no commit uses it, or closes it
// when the pool is full. Like a pooled consumer, one idle for too long is closed, whether its commits have completed or
// not.
TEST_F(DataConsumerPoolTest, background_worker_pools_or_closes_the_consumers_kept_apart) {
    config::routine_load_kafka_consumer_pool_size = 1;
    DataConsumerPool pool;
    auto in_flight = std::make_shared<CommitInFlightConsumer>();
    auto idle = std::make_shared<CommitInFlightConsumer>();
    auto in_use = std::make_shared<CommitInFlightConsumer>();
    auto completed = std::make_shared<CommitInFlightConsumer>();
    auto no_room = std::make_shared<CommitInFlightConsumer>();
    for (const auto& consumer : {in_flight, idle, in_use, completed, no_room}) {
        EXPECT_TRUE(pool.return_consumer(consumer).empty());
    }
    idle->set_last_visit_time(time(nullptr) - 601);
    in_use->commit_in_flight = false;
    completed->commit_in_flight = false;
    no_room->commit_in_flight = false;
    std::vector<UniqueId> kept_apart{in_flight->id(), in_use->id()};
    std::vector<UniqueId> pooled{completed->id()};
    std::weak_ptr<DataConsumer> idle_ref = idle;
    std::weak_ptr<DataConsumer> no_room_ref = no_room;
    in_flight.reset();
    idle.reset();
    completed.reset();
    no_room.reset();

    pool._clean_idle_consumer_bg();
    EXPECT_EQ(pooled, pooled_ids(&pool));
    EXPECT_EQ(kept_apart, committing_ids(&pool));
    EXPECT_TRUE(idle_ref.expired());
    EXPECT_TRUE(no_room_ref.expired());
}

// The background worker takes the pool lock to clean idle consumers, so stopping must not hold it while joining.
TEST_F(DataConsumerPoolTest, stop_joins_the_background_worker) {
    DataConsumerPool pool;
    pool.start_bg_worker();
    pool.stop();
    EXPECT_FALSE(pool._clean_idle_consumer_thread.joinable());
    // stopping again, as the destructor does, is a no-op
    pool.stop();
}

} // namespace starrocks
