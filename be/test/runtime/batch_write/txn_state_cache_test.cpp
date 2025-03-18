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

    void SetUp() override {
        config::merge_commit_trace_log_enable = true;
        _db = "test_db";
        _tbl = "test_tbl";
        _auth = {"test_user", "test_password"};
        ASSERT_OK(ThreadPoolBuilder("IsomorphicBatchWriteTest")
                          .set_min_threads(0)
                          .set_max_threads(1)
                          .set_max_queue_size(2048)
                          .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                          .build(&_thread_pool));
    }

    void TearDown() override {
        if (_thread_pool) {
            _thread_pool->shutdown();
        }
    }

    std::unique_ptr<TxnStateCache> create_cache(int32_t capacity) {
        // use serial mode to run poll task in the same order of their execution time, this is just used for testing
        std::unique_ptr<ThreadPoolToken> token = _thread_pool->new_token(ThreadPool::ExecutionMode::SERIAL);
        std::unique_ptr<TxnStateCache> cache = std::make_unique<TxnStateCache>(capacity, std::move(token));
        EXPECT_OK(cache->init());
        return cache;
    }

protected:
    std::string _db;
    std::string _tbl;
    AuthInfo _auth;

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};

void assert_txn_state_eq(const TxnState& expected, const TxnState& actual) {
    ASSERT_EQ(expected.txn_status, actual.txn_status);
    ASSERT_EQ(expected.reason, actual.reason);
}

TEST_F(TxnStateCacheTest, handler_push_state) {
    // PREPARE -> COMMITTED -> VISIBLE
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        handler.push_state(TTransactionStatus::COMMITTED, "");
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_TRUE(handler.committed_status_from_fe());
        handler.push_state(TTransactionStatus::VISIBLE, "");
        assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, handler.txn_state());
    }

    // PREPARE -> ABORTED
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        handler.push_state(TTransactionStatus::ABORTED, "manual failure");
        assert_txn_state_eq({TTransactionStatus::ABORTED, "manual failure"}, handler.txn_state());
    }

    // PREPARE -> UNKNOWN
    {
        TxnStateHandler handler;
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        handler.push_state(TTransactionStatus::UNKNOWN, "");
        assert_txn_state_eq({TTransactionStatus::UNKNOWN, ""}, handler.txn_state());
    }
}

TEST_F(TxnStateCacheTest, handler_poll_state) {
    bool trigger_poll = false;
    // PREPARE -> PREPARED -> COMMITTED -> VISIBLE
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(TxnState{TTransactionStatus::PREPARED, ""}));
        assert_txn_state_eq({TTransactionStatus::PREPARED, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(TxnState{TTransactionStatus::COMMITTED, ""}));
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_FALSE(handler.committed_status_from_fe());
        ASSERT_FALSE(handler.poll_state(TxnState{TTransactionStatus::VISIBLE, ""}));
        assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, handler.txn_state());
    }

    // PREPARE -> ABORTED
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_FALSE(handler.poll_state(TxnState{TTransactionStatus::ABORTED, "manual failure"}));
        assert_txn_state_eq({TTransactionStatus::ABORTED, "manual failure"}, handler.txn_state());
    }

    // PREPARE -> UNKNOWN
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_FALSE(handler.poll_state(TxnState{TTransactionStatus::UNKNOWN, ""}));
        assert_txn_state_eq({TTransactionStatus::UNKNOWN, ""}, handler.txn_state());
    }

    // max failure (merge_commit_txn_state_poll_max_fail_times) = 2, and only one failure
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        ASSERT_EQ(0, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(Status::InternalError("artificial failure")));
        ASSERT_EQ(1, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(TxnState{TTransactionStatus::COMMITTED, ""}));
        ASSERT_EQ(0, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_FALSE(handler.committed_status_from_fe());
        ASSERT_TRUE(handler.poll_state(Status::InternalError("artificial failure")));
        ASSERT_EQ(1, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_FALSE(handler.poll_state(TxnState{TTransactionStatus::VISIBLE, ""}));
        ASSERT_EQ(0, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, handler.txn_state());
    }

    // max failure (merge_commit_txn_state_poll_max_fail_times) = 2, and reach max failure
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        ASSERT_EQ(0, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(TxnState{TTransactionStatus::COMMITTED, ""}));
        ASSERT_EQ(0, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_FALSE(handler.committed_status_from_fe());
        ASSERT_TRUE(handler.poll_state(Status::InternalError("artificial failure")));
        ASSERT_EQ(1, handler.num_poll_failure());
        assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, handler.txn_state());
        ASSERT_FALSE(handler.poll_state(Status::InternalError("artificial failure")));
        ASSERT_EQ(2, handler.num_poll_failure());
        assert_txn_state_eq(
                {TTransactionStatus::UNKNOWN, "poll txn state failure exceeds max times 2, last error: " +
                                                      Status::InternalError("artificial failure").to_string(false)},
                handler.txn_state());
    }

    // no subscriber
    {
        TxnStateHandler handler;
        handler.subscribe(trigger_poll);
        assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, handler.txn_state());
        ASSERT_TRUE(handler.poll_state(TxnState{TTransactionStatus::PREPARED, ""}));
        assert_txn_state_eq({TTransactionStatus::PREPARED, ""}, handler.txn_state());
        handler.unsubscribe();
        ASSERT_FALSE(handler.poll_state(TxnState{TTransactionStatus::COMMITTED, ""}));
    }
}

TEST_F(TxnStateCacheTest, handler_subscriber) {
    TxnStateHandler handler;
    bool trigger_poll = false;
    handler.subscribe(trigger_poll);
    ASSERT_EQ(1, handler.TEST_num_subscriber());
    ASSERT_TRUE(trigger_poll);
    handler.subscribe(trigger_poll);
    ASSERT_EQ(2, handler.TEST_num_subscriber());
    ASSERT_FALSE(trigger_poll);
    handler.push_state(TTransactionStatus::VISIBLE, "");
    handler.unsubscribe();
    ASSERT_EQ(1, handler.TEST_num_subscriber());
    handler.unsubscribe();
    ASSERT_EQ(0, handler.TEST_num_subscriber());
    handler.subscribe(trigger_poll);
    ASSERT_EQ(1, handler.TEST_num_subscriber());
    ASSERT_FALSE(trigger_poll);
}

TEST_F(TxnStateCacheTest, handler_push_state_notify_subscriber) {
    TxnStateHandler handler;
    StatusOr<TxnState> expected_status;
    auto wait_func = [&](const std::string& name, int64_t timeout_us) {
        bool trigger_poll = false;
        handler.subscribe(trigger_poll);
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
    ASSERT_EQ(0, handler.num_waiting_finished_state());

    // wait until final state
    auto t1 = std::thread([&]() { wait_func("t1", 60000000); });
    auto t2 = std::thread([&]() { wait_func("t2", 60000000); });
    ASSERT_TRUE(Awaitility().timeout(5000000).until([&] { return handler.num_waiting_finished_state() == 2; }));
    expected_status = {TTransactionStatus::VISIBLE, ""};
    handler.push_state(TTransactionStatus::VISIBLE, "");
    t1.join();
    t2.join();
    ASSERT_EQ(0, handler.num_waiting_finished_state());

    // already in final state
    auto t3 = std::thread([&]() { wait_func("t3", 60000000); });
    t3.join();
    ASSERT_EQ(0, handler.num_waiting_finished_state());
}

TEST_F(TxnStateCacheTest, handler_poll_state_notify_subscriber) {
    TxnStateHandler handler;
    StatusOr<TxnState> expected_status;
    auto wait_func = [&](const std::string& name, int64_t timeout_us) {
        bool trigger_poll = false;
        handler.subscribe(trigger_poll);
        auto st = handler.wait_finished_state(name, timeout_us);
        ASSERT_EQ(expected_status.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected_status.value(), st.value());
        }
    };

    auto t1 = std::thread([&]() { wait_func("t1", 60000000); });
    auto t2 = std::thread([&]() { wait_func("t2", 60000000); });
    ASSERT_TRUE(Awaitility().timeout(5000000).until([&] { return handler.num_waiting_finished_state() == 2; }));
    expected_status = {TTransactionStatus::VISIBLE, ""};
    handler.poll_state(TxnState{TTransactionStatus::VISIBLE, ""});
    t1.join();
    t2.join();
    ASSERT_EQ(0, handler.num_waiting_finished_state());
}

TEST_F(TxnStateCacheTest, handler_stop) {
    TxnStateHandler handler;
    StatusOr<TxnState> expected_status;
    auto wait_func = [&](const std::string& name, int64_t timeout_us) {
        bool trigger_poll = false;
        handler.subscribe(trigger_poll);
        auto st = handler.wait_finished_state(name, timeout_us);
        ASSERT_EQ(expected_status.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected_status.value(), st.value());
        }
    };

    // wait when stopped
    auto t1 = std::thread([&]() { wait_func("t1", 60000000); });
    auto t2 = std::thread([&]() { wait_func("t2", 60000000); });
    ASSERT_TRUE(Awaitility().timeout(60000000).until([&] { return handler.num_waiting_finished_state() == 2; }));
    expected_status = Status::ServiceUnavailable("Transaction state handler is stopped");
    handler.stop();
    t1.join();
    t2.join();

    // already stopped
    auto t3 = std::thread([&]() { wait_func("t3", 60000000); });
    t3.join();
}

TEST_F(TxnStateCacheTest, poller_skip_schedule) {
    auto cache = create_cache(2048);
    auto old_poll_interval_ms = config::merge_commit_txn_state_poll_interval_ms;
    config::merge_commit_txn_state_poll_interval_ms = 100;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::get_current_ms");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::request");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::status");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::response");
        SyncPoint::GetInstance()->DisableProcessing();
        cache->stop();
        config::merge_commit_txn_state_poll_interval_ms = old_poll_interval_ms;
    });
    std::atomic<int32_t> num_rpc = 0;
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::request",
                                          [&](void* arg) { num_rpc.fetch_add(1); });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::response", [&](void* arg) {
        TGetLoadTxnStatusResult* result = (TGetLoadTxnStatusResult*)arg;
        result->__set_status(TTransactionStatus::VISIBLE);
        result->__set_reason("");
    });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 0; });

    TxnStatePoller* poller = cache->txn_state_poller();
    ASSERT_TRUE(cache->get_state(1).status().is_not_found());
    // create a subscriber to trigger the poll scheduling
    auto s1 = cache->subscribe_state(1, "s1", _db, _tbl, _auth);
    ASSERT_OK(s1.status());
    ASSERT_TRUE(poller->TEST_is_txn_pending(1));

    // transit the state to the final before scheduling the poll, and the poll should be skipped
    ASSERT_OK(cache->push_state(1, TTransactionStatus::VISIBLE, ""));
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, cache->get_state(1).value());
    // advance the time to schedule the poll for txn 1
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 200; });
    // create subscriber for txn 2 to trigger the poll scheduling
    auto s2 = cache->subscribe_state(2, "s2", _db, _tbl, _auth);
    ASSERT_OK(s2.status());
    ASSERT_TRUE(poller->TEST_is_txn_pending(2));
    // advance the time to schedule the poll for txn 2
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 400; });
    // when state for txn 2 becomes visible, it means poll task for txn 2 has been finished. The thread pool token
    // is serial, so the poll task (if not skipped) for txn 1 also should be finished
    ASSERT_TRUE(Awaitility().timeout(5000000).until(
            [&] { return s2.value()->current_state().txn_status == TTransactionStatus::VISIBLE; }));
    ASSERT_EQ(1, num_rpc.load());
}

TEST_F(TxnStateCacheTest, cache_push_state) {
    auto cache = create_cache(2048);
    DeferOp defer([&] { cache->stop(); });

    ASSERT_TRUE(cache->get_state(1).status().is_not_found());
    ASSERT_OK(cache->push_state(1, TTransactionStatus::PREPARE, ""));
    auto st_1 = cache->get_state(1);
    ASSERT_OK(st_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, st_1.value());
    ASSERT_OK(cache->push_state(1, TTransactionStatus::VISIBLE, ""));
    st_1 = cache->get_state(1);
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, st_1.value());

    ASSERT_TRUE(cache->get_state(2).status().is_not_found());
    ASSERT_OK(cache->push_state(2, TTransactionStatus::ABORTED, "artificial failure"));
    auto st_2 = cache->get_state(2);
    ASSERT_OK(st_2.status());
    assert_txn_state_eq({TTransactionStatus::ABORTED, "artificial failure"}, st_2.value());

    ASSERT_TRUE(cache->get_state(3).status().is_not_found());
    ASSERT_OK(cache->push_state(3, TTransactionStatus::UNKNOWN, ""));
    auto st_3 = cache->get_state(3);
    ASSERT_OK(st_3.status());
    assert_txn_state_eq({TTransactionStatus::UNKNOWN, ""}, st_3.value());
}

TEST_F(TxnStateCacheTest, cache_push_state_notify_subscriber) {
    auto cache = create_cache(2048);
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::get_current_ms");
        SyncPoint::GetInstance()->DisableProcessing();
        cache->stop();
    });
    // disable poller to avoid unexpected txn state update
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 0; });

    auto wait_func = [&](TxnStateSubscriber* subscriber, int64_t timeout_us, StatusOr<TxnState> expected) {
        auto st = subscriber->wait_finished_state(timeout_us);
        ASSERT_EQ(expected.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected.value(), st.value());
        }
    };

    ASSERT_TRUE(cache->get_state(1).status().is_not_found());
    auto s1_1 = cache->subscribe_state(1, "s1_1", _db, _tbl, _auth);
    ASSERT_OK(s1_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_1.value()->current_state());
    auto s1_2 = cache->subscribe_state(1, "s1_2", _db, _tbl, _auth);
    ASSERT_OK(s1_2.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_2.value()->current_state());

    ASSERT_TRUE(cache->get_state(2).status().is_not_found());
    ASSERT_OK(cache->push_state(2, TTransactionStatus::ABORTED, "artificial failure"));
    auto s2_1 = cache->subscribe_state(2, "s2_1", _db, _tbl, _auth);
    ASSERT_OK(s2_1.status());
    assert_txn_state_eq({TTransactionStatus::ABORTED, "artificial failure"}, s2_1.value()->current_state());

    auto t1_1 = std::thread([&]() {
        wait_func(s1_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    ASSERT_OK(cache->push_state(1, TTransactionStatus::VISIBLE, ""));
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

    ASSERT_TRUE(cache->get_state(3).status().is_not_found());
    auto s3_1 = cache->subscribe_state(3, "s3_1", _db, _tbl, _auth);
    ASSERT_OK(s3_1.status());
    auto t3_1 = std::thread([&]() {
        wait_func(s3_1.value().get(), 10000, StatusOr<TxnState>(Status::TimedOut("Wait txn state timeout 10000 us")));
    });
    t3_1.join();
}

TEST_F(TxnStateCacheTest, cache_poll_state_notify_subscriber) {
    auto cache = create_cache(2048);
    auto old_poll_interval_ms = config::merge_commit_txn_state_poll_interval_ms;
    config::merge_commit_txn_state_poll_interval_ms = 100;
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::get_current_ms");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::request");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::status");
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::_execute_poll::response");
        SyncPoint::GetInstance()->DisableProcessing();
        cache->stop();
        config::merge_commit_txn_state_poll_interval_ms = old_poll_interval_ms;
    });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 0; });

    auto wait_func = [&](TxnStateSubscriber* subscriber, int64_t timeout_us, StatusOr<TxnState> expected) {
        auto st = subscriber->wait_finished_state(timeout_us);
        ASSERT_EQ(expected.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected.value(), st.value());
        }
    };

    std::atomic<int32_t> num_rpc = 0;
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::request",
                                          [&](void* arg) { num_rpc.fetch_add(1); });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::status",
                                          [&](void* arg) { *((Status*)arg) = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::response", [&](void* arg) {
        TGetLoadTxnStatusResult* result = (TGetLoadTxnStatusResult*)arg;
        result->__set_status(TTransactionStatus::VISIBLE);
        result->__set_reason("");
    });

    // txn 1 and 2 should be scheduled at time 100, current is 0
    TxnStatePoller* poller = cache->txn_state_poller();
    ASSERT_TRUE(cache->get_state(1).status().is_not_found());
    auto s1_1 = cache->subscribe_state(1, "s1_1", _db, _tbl, _auth);
    ASSERT_OK(s1_1.status());
    ASSERT_TRUE(poller->TEST_is_txn_pending(1));
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_1.value()->current_state());
    auto s1_2 = cache->subscribe_state(1, "s1_2", _db, _tbl, _auth);
    ASSERT_OK(s1_2.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s1_2.value()->current_state());

    ASSERT_TRUE(cache->get_state(2).status().is_not_found());
    auto s2_1 = cache->subscribe_state(2, "s2_1", _db, _tbl, _auth);
    ASSERT_OK(s2_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s2_1.value()->current_state());
    ASSERT_TRUE(poller->TEST_is_txn_pending(2));

    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 50; });
    // txn 3 should be scheduled at time 150, current is 50
    ASSERT_TRUE(cache->get_state(3).status().is_not_found());
    auto s3_1 = cache->subscribe_state(3, "s3_1", _db, _tbl, _auth);
    ASSERT_OK(s3_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s3_1.value()->current_state());
    ASSERT_TRUE(poller->TEST_is_txn_pending(1));
    ASSERT_TRUE(poller->TEST_is_txn_pending(2));
    ASSERT_TRUE(poller->TEST_is_txn_pending(3));

    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 80; });
    // txn 4 should be scheduled at time 180, current is 80
    ASSERT_TRUE(cache->get_state(4).status().is_not_found());
    auto s4_1 = cache->subscribe_state(4, "s4_1", _db, _tbl, _auth);
    ASSERT_OK(s4_1.status());
    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s4_1.value()->current_state());
    ASSERT_TRUE(poller->TEST_is_txn_pending(1));
    ASSERT_TRUE(poller->TEST_is_txn_pending(2));
    ASSERT_TRUE(poller->TEST_is_txn_pending(3));
    ASSERT_TRUE(poller->TEST_is_txn_pending(4));

    auto t1_1 = std::thread([&]() {
        wait_func(s1_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    auto t1_2 = std::thread([&]() {
        wait_func(s1_2.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    auto t2_1 = std::thread([&]() {
        wait_func(s2_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    auto t3_1 = std::thread([&]() {
        wait_func(s3_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });

    // advance time and should trigger txn 1, 2 and 3 to poll
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 160; });
    t1_1.join();
    t1_2.join();
    t2_1.join();
    t3_1.join();

    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, s1_1.value()->current_state());
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, s2_1.value()->current_state());
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, s3_1.value()->current_state());
    ASSERT_FALSE(poller->TEST_is_txn_pending(1));
    ASSERT_FALSE(poller->TEST_is_txn_pending(2));
    ASSERT_FALSE(poller->TEST_is_txn_pending(3));
    ASSERT_TRUE(poller->TEST_is_txn_pending(4));
    ASSERT_EQ(3, num_rpc.load());

    assert_txn_state_eq({TTransactionStatus::PREPARE, ""}, s4_1.value()->current_state());
    auto t4_1 = std::thread([&]() {
        wait_func(s4_1.value().get(), 60000000, StatusOr<TxnState>({TTransactionStatus::VISIBLE, ""}));
    });
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::response", [&](void* arg) {
        TGetLoadTxnStatusResult* result = (TGetLoadTxnStatusResult*)arg;
        result->__set_status(TTransactionStatus::COMMITTED);
        result->__set_reason("");
    });
    // advance time and trigger txn 4 to poll. it should continue polling because
    // the state is COMMITTED which is not final. next poll time is 300
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 200; });
    ASSERT_TRUE(Awaitility().timeout(5000000).until([&] {
        auto st = poller->TEST_pending_execution_time(4);
        return st.ok() && st.value() == 300;
    }));
    assert_txn_state_eq({TTransactionStatus::COMMITTED, ""}, s4_1.value()->current_state());
    ASSERT_EQ(4, num_rpc.load());

    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::_execute_poll::response", [&](void* arg) {
        TGetLoadTxnStatusResult* result = (TGetLoadTxnStatusResult*)arg;
        result->__set_status(TTransactionStatus::VISIBLE);
        result->__set_reason("");
    });
    // advance time and trigger txn 4 to poll again. This time it should reach the finished state
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 400; });
    t4_1.join();
    ASSERT_EQ(5, num_rpc.load());
    assert_txn_state_eq({TTransactionStatus::VISIBLE, ""}, s4_1.value()->current_state());
}

TEST_F(TxnStateCacheTest, cache_eviction) {
    int32_t numShards = 1 << 5;
    auto cache = create_cache(numShards * 3);
    DeferOp defer_stop([&] { cache->stop(); });

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
    ASSERT_OK(cache->push_state(1 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(1, cache->size());
    ASSERT_EQ(0, num_evict);
    auto s1 = cache->subscribe_state(2 << 5, "s1", _db, _tbl, _auth);
    ASSERT_OK(s1.status());
    ASSERT_EQ(2, cache->size());
    ASSERT_EQ(0, num_evict);
    ASSERT_OK(cache->push_state(3 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache->size());
    ASSERT_EQ(0, num_evict);

    ASSERT_OK(cache->push_state(4 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache->size());
    ASSERT_EQ(1 << 5, evict_txn_id);
    ASSERT_EQ(1, num_evict);

    auto s2 = cache->subscribe_state(5 << 5, "s2", _db, _tbl, _auth);
    ASSERT_OK(s2.status());
    ASSERT_EQ(3, cache->size());
    ASSERT_EQ(3 << 5, evict_txn_id);
    ASSERT_EQ(2, num_evict);

    auto s3 = cache->subscribe_state(6 << 5, "s3", _db, _tbl, _auth);
    ASSERT_OK(s3.status());
    ASSERT_EQ(3, cache->size());
    ASSERT_EQ(4 << 5, evict_txn_id);
    ASSERT_EQ(3, num_evict);

    s1.value().reset();
    ASSERT_OK(cache->push_state(7 << 5, TTransactionStatus::VISIBLE, ""));
    ASSERT_EQ(3, cache->size());
    ASSERT_EQ(2 << 5, evict_txn_id);
    ASSERT_EQ(4, num_evict);
}

TEST_F(TxnStateCacheTest, cache_set_capacity) {
    auto cache = create_cache(2048);
    DeferOp defer([&] { cache->stop(); });
    auto shards = cache->get_cache_shards();
    for (auto shard : shards) {
        ASSERT_EQ(2048 / 32, shard->capacity());
    }
    cache->set_capacity(4096);
    for (auto shard : shards) {
        ASSERT_EQ(4096 / 32, shard->capacity());
    }
}

TEST_F(TxnStateCacheTest, cache_clean_txn_state) {
    auto old_clean_interval_sec = config::merge_commit_txn_state_clean_interval_sec;
    auto old_expire_time_sec = config::merge_commit_txn_state_expire_time_sec;
    config::merge_commit_txn_state_clean_interval_sec = 1;
    config::merge_commit_txn_state_expire_time_sec = 1;
    auto cache = create_cache(2048);
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("TxnStateHandler::destruct");
        SyncPoint::GetInstance()->DisableProcessing();
        cache->stop();
        config::merge_commit_txn_state_clean_interval_sec = old_clean_interval_sec;
        config::merge_commit_txn_state_expire_time_sec = old_expire_time_sec;
    });
    std::atomic<int32_t> num_evict = 0;
    SyncPoint::GetInstance()->SetCallBack("TxnStateHandler::destruct", [&](void* arg) { num_evict += 1; });
    ASSERT_OK(cache->push_state(1, TTransactionStatus::VISIBLE, ""));
    ASSERT_OK(cache->push_state(2, TTransactionStatus::VISIBLE, ""));
    ASSERT_OK(cache->push_state(3, TTransactionStatus::VISIBLE, ""));
    ASSERT_TRUE(Awaitility().timeout(10000000).until([&] { return num_evict == 3; }));
    for (auto shard : cache->get_cache_shards()) {
        ASSERT_EQ(0, shard->object_size());
    }
}

TEST_F(TxnStateCacheTest, cache_stop) {
    auto cache = create_cache(2048);
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&] {
        SyncPoint::GetInstance()->ClearCallBack("TxnStatePoller::get_current_ms");
        SyncPoint::GetInstance()->DisableProcessing();
        cache->stop();
    });
    // disable poller to avoid unexpected txn state update
    SyncPoint::GetInstance()->SetCallBack("TxnStatePoller::get_current_ms", [&](void* arg) { *((int64_t*)arg) = 0; });

    Status expected_status = Status::ServiceUnavailable("Transaction state handler is stopped");
    auto wait_func = [&](TxnStateSubscriber* subscriber, int64_t timeout_us, StatusOr<TxnState> expected) {
        auto st = subscriber->wait_finished_state(timeout_us);
        ASSERT_EQ(expected.status().to_string(), st.status().to_string());
        if (st.ok()) {
            assert_txn_state_eq(expected.value(), st.value());
        }
    };

    auto s1 = cache->subscribe_state(1, "s1", _db, _tbl, _auth);
    ASSERT_OK(s1.status());
    auto t1 = std::thread([&]() { wait_func(s1.value().get(), 60000000, StatusOr<TxnState>(expected_status)); });
    auto s2 = cache->subscribe_state(1, "s2", _db, _tbl, _auth);
    ASSERT_OK(s2.status());
    auto s3 = cache->subscribe_state(2, "s3", _db, _tbl, _auth);
    ASSERT_OK(s3.status());
    auto t3 = std::thread([&]() { wait_func(s3.value().get(), 60000000, StatusOr<TxnState>(expected_status)); });

    ASSERT_TRUE(Awaitility().timeout(5000000).until(
            [&] { return s1.value()->entry()->value().num_waiting_finished_state() == 1; }));
    ASSERT_TRUE(Awaitility().timeout(5000000).until(
            [&] { return s3.value()->entry()->value().num_waiting_finished_state() == 1; }));
    cache->stop();
    t1.join();
    t3.join();
    auto wait_st = s2.value()->wait_finished_state(10000);
    ASSERT_EQ(expected_status.to_string(), wait_st.status().to_string());

    expected_status = Status::ServiceUnavailable("Transaction state cache is stopped");
    ASSERT_EQ(expected_status.to_string(), cache->push_state(3, TTransactionStatus::VISIBLE, "").to_string(false));
    ASSERT_EQ(expected_status.to_string(), cache->subscribe_state(3, "s4", _db, _tbl, _auth).status().to_string(false));
    ASSERT_FALSE(cache->txn_state_poller()->TEST_is_scheduling());
}

} // namespace starrocks
