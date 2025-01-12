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

#include "runtime/batch_write/txn_state_cache.h"

#include "testutil/assert.h"
#include "util/await.h"

namespace starrocks {

class TxnStateCacheTest : public testing::Test {
public:
    TxnStateCacheTest() = default;
    ~TxnStateCacheTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

void assert_txn_state_eq(const TxnState& expected, const TxnState& actual) {
    ASSERT_EQ(expected.txn_status, actual.txn_status);
    ASSERT_EQ(expected.reason, actual.reason);
}

TEST(TxnStateCacheTest, handler_update_state) {
    // PREPARE -> COMMITTED -> VISIBLE
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());

        handler.update_state(TTransactionStatus::COMMITTED, "");
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());

        handler.update_state(TTransactionStatus::VISIBLE, "");
        assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, handler.txn_state());
    }

    // PREPARE -> ABORTED
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());

        handler.update_state(TTransactionStatus::ABORTED, "manual failure");
        assert_txn_state_eq({TTransactionStatus::ABORTED, "manual failure"}, handler.txn_state());
    }

    // PREPARE -> UNKNOWN
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());

        handler.update_state(TTransactionStatus::UNKNOWN, "");
        assert_txn_state_eq({TTransactionStatus::UNKNOWN, ""}, handler.txn_state());
    }
}

TEST(TxnStateCacheTest, handler_wait_finished_state) {
    TxnStateHandler handler;
    StatusOr<TxnState> expected_status;
    auto wait_func = [&](const std::string& name, int64_t timeout_us) {
        handler.acquire_subscriber();
        auto st = handler.wait_finished_state(name, timeout_us);
        ASSERT_EQ(expected_status.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected_status.value(), st.value());
        }
    };

    // wait timeout
    expected_status = Status::TimedOut("Wait txn state timeout 10000 us");
    auto t0 = std::thread([&]() { wait_func("t0", 10000); });
    t0.join();
    ASSERT_EQ(0, handler.num_waiting_subscriber());

    // wait until final state
    auto t1 = std::thread([&]() { wait_func("t1", 60000000); });
    auto t2 = std::thread([&]() { wait_func("t2", 60000000); });
    ASSERT_TRUE(Awaitility().timeout(5000000).until([&] { return handler.num_waiting_subscriber() == 2; }));
    expected_status = {TTransactionStatus::VISIBLE, ""};
    handler.update_state(TTransactionStatus::VISIBLE, "");
    t1.join();
    t2.join();
    ASSERT_EQ(0, handler.num_waiting_subscriber());

    // already in final state
    auto t3 = std::thread([&]() { wait_func("t3", 60000000); });
    t3.join();
    ASSERT_EQ(0, handler.num_waiting_subscriber());
}

TEST(TxnStateCacheTest, handler_stop) {
    TxnStateHandler handler;
    StatusOr<TxnState> expected_status;
    auto wait_func = [&](const std::string& name, int64_t timeout_us) {
        handler.acquire_subscriber();
        auto st = handler.wait_finished_state(name, timeout_us);
        ASSERT_EQ(expected_status.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected_status.value(), st.value());
        }
    };

    // wait when stopped
    auto t1 = std::thread([&]() { wait_func("t1", 60000000); });
    auto t2 = std::thread([&]() { wait_func("t2", 60000000); });
    ASSERT_TRUE(Awaitility().timeout(60000000).until([&] { return handler.num_waiting_subscriber() == 2; }));
    expected_status = Status::ServiceUnavailable("Transaction state handler is stopped");
    handler.stop();
    t1.join();
    t2.join();

    // already stopped
    auto t3 = std::thread([&]() { wait_func("t3", 60000000); });
    t3.join();
}

TEST(TxnStateCacheTest, cache_update_state) {
    TxnStateCache cache(2048);
    DeferOp defer([&] { cache.stop(); });

    ASSERT_TRUE(cache.get_state(1).status().is_not_found());
    ASSERT_OK(cache.update_state(1, TTransactionStatus::PREPARE, ""));
    auto st_1 = cache.get_state(1);
    ASSERT_OK(st_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, st_1.value());
    ASSERT_OK(cache.update_state(1, TTransactionStatus::VISIBLE, ""));
    st_1 = cache.get_state(1);
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, st_1.value());

    ASSERT_TRUE(cache.get_state(2).status().is_not_found());
    ASSERT_OK(cache.update_state(2, TTransactionStatus::ABORTED, "artificial failure"));
    auto st_2 = cache.get_state(2);
    ASSERT_OK(st_2.status());
    assert_txn_state_eq({TTransactionStatus::ABORTED, "artificial failure"}, st_2.value());

    ASSERT_TRUE(cache.get_state(3).status().is_not_found());
    ASSERT_OK(cache.update_state(3, TTransactionStatus::UNKNOWN, ""));
    auto st_3 = cache.get_state(3);
    ASSERT_OK(st_3.status());
    assert_txn_state_eq({TTransactionStatus::UNKNOWN, ""}, st_3.value());
}

TEST(TxnStateCacheTest, cache_subscriber) {
    TxnStateCache cache(2048);
    DeferOp defer([&] { cache.stop(); });

    auto wait_func = [&](TxnStateSubscriber* subscriber, int64_t timeout_us, StatusOr<TxnState> expected) {
        auto st = subscriber->wait_finished_state(timeout_us);
        ASSERT_EQ(expected.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected.value(), st.value());
        }
    };

    ASSERT_TRUE(cache.get_state(1).status().is_not_found());
    auto s1_1 = cache.subscribe_state(1, "s1_1");
    ASSERT_OK(s1_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_1.value()->current_state());
    auto s1_2 = cache.subscribe_state(1, "s1_2");
    ASSERT_OK(s1_2.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_2.value()->current_state());

    ASSERT_TRUE(cache.get_state(2).status().is_not_found());
    ASSERT_OK(cache.update_state(2, TTransactionStatus::ABORTED, "artificial failure"));
    auto s2_1 = cache.subscribe_state(2, "s2_1");
    ASSERT_OK(s2_1.status());
    assert_txn_state_eq({TTransactionStatus::ABORTED, "artificial failure"}, s2_1.value()->current_state());

    auto t1_1 = std::thread([&]() {
        wait_func(s1_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    ASSERT_OK(cache.update_state(1, TTransactionStatus::VISIBLE, ""));
    t1_1.join();

    auto t1_2 = std::thread([&]() {
        wait_func(s1_2.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    t1_2.join();

    auto t2_1 = std::thread([&]() {
        wait_func(s2_1.value().get(), 60000000,
                  StatusOr<TxnState>({TTransactionStatus::ABORTED, "artificial failure"}));
    });
    t2_1.join();

    ASSERT_TRUE(cache.get_state(3).status().is_not_found());
    auto s3_1 = cache.subscribe_state(3, "s3_1");
    ASSERT_OK(s3_1.status());
    auto t3_1 = std::thread([&]() {
        wait_func(s3_1.value().get(), 10000, StatusOr<TxnState>(Status::TimedOut("Wait txn state timeout 10000 us")));
    });
    t3_1.join();
}

TEST(TxnStateCacheTest, cache_eviction) {
    int32_t numShards = 1 << 5;
    TxnStateCache cache(numShards * 3);
    DeferOp defer_stop([&] { cache.stop(); });

    int64_t evict_txn_id = -1;
    int32_t num_evict = 0;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TxnStateHandler::destruct");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("TxnStateHandler::destruct", [&](void* arg) {
        TxnStateHandler* handler = (TxnStateHandler*)arg;
        evict_txn_id = handler->txn_id();
        num_evict += 1;
    });
    ASSERT_OK(cache.update_state(1 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(1, cache.size());
    ASSERT_EQ(0, num_evict);
    auto s1 = cache.subscribe_state(2 << 5, "s1");
    ASSERT_OK(s1.status());
    ASSERT_EQ(2, cache.size());
    ASSERT_EQ(0, num_evict);
    ASSERT_OK(cache.update_state(3 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache.size());
    ASSERT_EQ(0, num_evict);

    ASSERT_OK(cache.update_state(4 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache.size());
    ASSERT_EQ(1 << 5, evict_txn_id);
    ASSERT_EQ(1, num_evict);

    auto s2 = cache.subscribe_state(5 << 5, "s2");
    ASSERT_OK(s2.status());
    ASSERT_EQ(3, cache.size());
    ASSERT_EQ(3 << 5, evict_txn_id);
    ASSERT_EQ(2, num_evict);

    auto s3 = cache.subscribe_state(6 << 5, "s3");
    ASSERT_OK(s3.status());
    ASSERT_EQ(3, cache.size());
    ASSERT_EQ(4 << 5, evict_txn_id);
    ASSERT_EQ(3, num_evict);

    s1.value().reset();
    ASSERT_OK(cache.update_state(7 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache.size());
    ASSERT_EQ(2 << 5, evict_txn_id);
    ASSERT_EQ(4, num_evict);
}

TEST(TxnStateCacheTest, cache_set_capacity) {
    TxnStateCache cache(2048);
    DeferOp defer([&] { cache.stop(); });
    auto shards = cache.get_cache_shards();
    for (auto shard : shards) {
        ASSERT_EQ(2048 / 32, shard->capacity());
    }
    cache.set_capacity(4096);
    for (auto shard : shards) {
        ASSERT_EQ(4096 / 32, shard->capacity());
    }
}

TEST(TxnStateCacheTest, cache_stop) {
    TxnStateCache cache(2048);

    Status expected_status = Status::ServiceUnavailable("Transaction state handler is stopped");
    auto wait_func = [&](TxnStateSubscriber* subscriber, int64_t timeout_us, StatusOr<TxnState> expected) {
        auto st = subscriber->wait_finished_state(timeout_us);
        ASSERT_EQ(expected.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected.value(), st.value());
        }
    };

    auto s1 = cache.subscribe_state(1, "s1");
    ASSERT_OK(s1.status());
    auto t1 = std::thread([&]() { wait_func(s1.value().get(), 60000000, StatusOr<TxnState>(expected_status)); });
    auto s2 = cache.subscribe_state(1, "s2");
    ASSERT_OK(s2.status());
    auto s3 = cache.subscribe_state(2, "s3");
    ASSERT_OK(s3.status());
    auto t3 = std::thread([&]() { wait_func(s3.value().get(), 60000000, StatusOr<TxnState>(expected_status)); });

    ASSERT_TRUE(Awaitility().timeout(5000000).until(
            [&] { return s1.value()->entry()->value().num_waiting_subscriber() == 1; }));
    ASSERT_TRUE(Awaitility().timeout(5000000).until(
            [&] { return s3.value()->entry()->value().num_waiting_subscriber() == 1; }));
    cache.stop();
    t1.join();
    t3.join();
    auto wait_st = s2.value()->wait_finished_state(10000);
    ASSERT_EQ(expected_status.to_string(), wait_st.status().to_string());

    expected_status = Status::ServiceUnavailable("Transaction state cache is stopped");
    ASSERT_EQ(expected_status.to_string(), cache.update_state(3, TTransactionStatus::VISIBLE, "").to_string(false));
    ASSERT_EQ(expected_status.to_string(), cache.subscribe_state(3, "s4").status().to_string(false));
}

} // namespace starrocks
