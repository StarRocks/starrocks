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

#include "base/failpoint/fail_point.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

namespace starrocks {

TEST(FailPointTest, enable_disable_mode) {
    failpoint::FailPoint fp("test");

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
    fp.setMode(trigger_mode);
    ASSERT_FALSE(fp.shouldFail());

    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp.setMode(trigger_mode);
    ASSERT_TRUE(fp.shouldFail());
}

TEST(FailPointTest, n_times_mode) {
    failpoint::FailPoint fp("test");

    int32_t n_times = 10;
    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE_N_TIMES);
    trigger_mode.set_n_times(n_times);
    fp.setMode(trigger_mode);

    for (int i = 0; i < n_times; i++) {
        ASSERT_TRUE(fp.shouldFail());
    }
    ASSERT_FALSE(fp.shouldFail());
}

TEST(FailPointTest, probability_mode) {
    failpoint::FailPoint fp("test");

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::PROBABILITY_ENABLE);
    trigger_mode.set_probability(0);
    fp.setMode(trigger_mode);
    ASSERT_FALSE(fp.shouldFail());

    trigger_mode.set_probability(1);
    fp.setMode(trigger_mode);
    ASSERT_TRUE(fp.shouldFail());
}

TEST(FailPointTest, scoped_fail_point) {
    failpoint::ScopedFailPoint sfp("test");
    failpoint::FailPointRegisterer sfpr(&sfp);

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    sfp.setMode(trigger_mode);

    ASSERT_FALSE(sfp.shouldFail());

    {
        failpoint::ScopedFailPointGuard g("test");
        ASSERT_TRUE(sfp.shouldFail());
    }

    ASSERT_FALSE(sfp.shouldFail());
}

TEST(FailPointTest, fp_demo) {
    DEFINE_FAIL_POINT(fp_test);

    auto test_func = [&]() {
        FAIL_POINT_TRIGGER_RETURN(fp_test, false);
        return true;
    };

    ASSERT_TRUE(test_func());

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp_fp_test.setMode(trigger_mode);

    ASSERT_FALSE(test_func());
}

TEST(FailPointTest, sfp_demo) {
    DEFINE_SCOPED_FAIL_POINT(sfp_test);

    auto test_func = [&]() {
        FAIL_POINT_TRIGGER_EXECUTE(sfp_test, { return false; });
        return true;
    };

    ASSERT_TRUE(test_func());

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    sfp_sfp_test.setMode(trigger_mode);

    ASSERT_TRUE(test_func());

    {
        FAIL_POINT_SCOPE(sfp_test);
        ASSERT_FALSE(test_func());
    }

    ASSERT_TRUE(test_func());
}

namespace {

// The STORED form of a pause, i.e. what update_fail_point_status hands to setMode() after lifting
// the request-level discriminator. mode stays DISABLE so an old backend degrades safely on the wire.
PFailPointTriggerMode pause_mode(int32_t timeout_second) {
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::DISABLE);
    mode.set_pause(true);
    if (timeout_second > 0) {
        mode.set_pause_timeout_second(timeout_second);
    }
    return mode;
}

PFailPointTriggerMode simple_mode(FailPointTriggerModeType type) {
    PFailPointTriggerMode mode;
    mode.set_mode(type);
    return mode;
}

// Spin until |fp| reports |expected| parked threads, so the pause tests synchronise on real state
// instead of a fixed sleep. Returns false if the state is not reached within the budget.
bool wait_for_parked(failpoint::FailPoint& fp, int64_t expected, int budget_ms = 10000) {
    for (int waited = 0; waited < budget_ms; waited += 5) {
        if (fp.to_pb().paused_thread_count() == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}

// Always disables |fp| and joins |threads| on scope exit, so a failed ASSERT_* cannot leave a
// joinable std::thread behind -- that would call std::terminate and take the whole suite down.
class PauseTestCleanup {
public:
    PauseTestCleanup(failpoint::FailPoint& fp, std::vector<std::thread>& threads) : _fp(fp), _threads(threads) {}
    ~PauseTestCleanup() {
        _fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
        for (auto& t : _threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
    PauseTestCleanup(const PauseTestCleanup&) = delete;
    PauseTestCleanup& operator=(const PauseTestCleanup&) = delete;

private:
    failpoint::FailPoint& _fp;
    std::vector<std::thread>& _threads;
};

} // namespace

TEST(FailPointTest, pause_released_by_disable) {
    failpoint::FailPoint fp("test_pause_disable");
    fp.setMode(pause_mode(0));

    std::atomic<bool> returned{false};
    std::atomic<bool> result{true};
    std::vector<std::thread> threads;
    PauseTestCleanup cleanup(fp, threads);
    threads.emplace_back([&] {
        result = fp.shouldFail();
        returned = true;
    });

    ASSERT_TRUE(wait_for_parked(fp, 1));
    ASSERT_FALSE(returned.load());
    ASSERT_EQ(1, fp.to_pb().trigger_count());

    fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    threads[0].join();

    ASSERT_TRUE(returned.load());
    // A released pause continues normally and never injects.
    ASSERT_FALSE(result.load());
    ASSERT_EQ(0, fp.to_pb().paused_thread_count());
}

TEST(FailPointTest, pause_released_by_rearm) {
    failpoint::FailPoint fp("test_pause_rearm");
    fp.setMode(pause_mode(0));

    std::atomic<bool> result{true};
    std::vector<std::thread> threads;
    PauseTestCleanup cleanup(fp, threads);
    threads.emplace_back([&] { result = fp.shouldFail(); });
    ASSERT_TRUE(wait_for_parked(fp, 1));

    // Any mode change releases, not just DISABLE.
    fp.setMode(simple_mode(FailPointTriggerModeType::ENABLE));
    threads[0].join();
    ASSERT_FALSE(result.load());
}

TEST(FailPointTest, pause_releases_all_waiters) {
    failpoint::FailPoint fp("test_pause_all");
    fp.setMode(pause_mode(0));

    std::atomic<int> injected{0};
    std::vector<std::thread> threads;
    PauseTestCleanup cleanup(fp, threads);
    for (int i = 0; i < 4; i++) {
        threads.emplace_back([&] {
            if (fp.shouldFail()) {
                injected++;
            }
        });
    }
    ASSERT_TRUE(wait_for_parked(fp, 4));
    ASSERT_EQ(4, fp.to_pb().trigger_count());

    fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_EQ(0, injected.load());
    ASSERT_EQ(0, fp.to_pb().paused_thread_count());
}

TEST(FailPointTest, pause_times_out) {
    failpoint::FailPoint fp("test_pause_timeout");
    fp.setMode(pause_mode(1));

    // shouldFail() runs on a worker and is bounded by a future deadline. Calling it synchronously
    // and asserting on elapsed time afterwards cannot catch a hang -- the assertion would never be
    // reached -- and run-be-ut.sh imposes no per-test limit.
    std::promise<bool> done;
    auto future = done.get_future();
    auto start = std::chrono::steady_clock::now();
    std::thread worker([&] { done.set_value(fp.shouldFail()); });

    const bool finished = future.wait_for(std::chrono::seconds(20)) == std::future_status::ready;
    if (!finished) {
        // Unblock the worker through the other code path so the test can fail instead of hanging.
        fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    }
    worker.join();
    auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    ASSERT_TRUE(finished) << "pause_timeout_second was ignored: shouldFail() never returned on its own";
    ASSERT_FALSE(future.get());
    ASSERT_GE(elapsed_ms, 900);
    ASSERT_EQ(0, fp.to_pb().paused_thread_count());
    ASSERT_EQ(1, fp.to_pb().trigger_count());
}

TEST(FailPointTest, pause_does_not_block_set_mode) {
    failpoint::FailPoint fp("test_pause_no_deadlock");
    // The short self-release keeps a regression from hanging the whole suite: if the pause wrongly
    // held the shared _mu, setMode() would merely be delayed until this timeout instead of
    // deadlocking forever, and the elapsed-time assertion below still catches it.
    fp.setMode(pause_mode(5));

    std::vector<std::thread> threads;
    PauseTestCleanup cleanup(fp, threads);
    threads.emplace_back([&] { (void)fp.shouldFail(); });
    ASSERT_TRUE(wait_for_parked(fp, 1));

    auto start = std::chrono::steady_clock::now();
    fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    threads[0].join();

    ASSERT_LT(elapsed_ms, 2000) << "setMode() blocked while a thread was paused: the pause is holding _mu";
}

TEST(FailPointTest, pause_default_timeout_applies_when_unset) {
    failpoint::FailPoint fp("test_pause_default_timeout");
    // pause_timeout_second unset -> kDefaultPauseTimeoutSecond (300s), so the thread must still be
    // parked well after a short observation window rather than resuming immediately.
    fp.setMode(pause_mode(0));

    std::vector<std::thread> threads;
    PauseTestCleanup cleanup(fp, threads);
    threads.emplace_back([&] { (void)fp.shouldFail(); });
    ASSERT_TRUE(wait_for_parked(fp, 1));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_EQ(1, fp.to_pb().paused_thread_count());
}

TEST(FailPointTest, trigger_count_counts_fires_not_calls) {
    failpoint::FailPoint fp("test_trigger_count");

    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE_N_TIMES);
    mode.set_n_times(3);
    fp.setMode(mode);

    int fired = 0;
    for (int i = 0; i < 5; i++) {
        if (fp.shouldFail()) {
            fired++;
        }
    }
    ASSERT_EQ(3, fired);
    // 5 calls, 3 fires.
    ASSERT_EQ(3, fp.to_pb().trigger_count());
    ASSERT_EQ(0, fp.to_pb().paused_thread_count());
}

TEST(FailPointTest, trigger_mode_from_request_lifts_pause) {
    // The discriminator arrives on the REQUEST so an old BE cannot echo it back; a new BE lifts it
    // into the mode it stores, which is what to_pb() then reports truthfully.
    PUpdateFailPointStatusRequest request;
    request.set_fail_point_name("fp");
    request.mutable_trigger_mode()->set_mode(FailPointTriggerModeType::DISABLE);
    request.set_pause(true);
    request.set_pause_timeout_second(7);

    auto stored = failpoint::trigger_mode_from_request(request);
    ASSERT_TRUE(stored.pause());
    ASSERT_EQ(7, stored.pause_timeout_second());
    ASSERT_EQ(FailPointTriggerModeType::DISABLE, stored.mode());

    // A non-pause request passes through untouched.
    PUpdateFailPointStatusRequest plain;
    plain.mutable_trigger_mode()->set_mode(FailPointTriggerModeType::ENABLE);
    auto passthrough = failpoint::trigger_mode_from_request(plain);
    ASSERT_FALSE(passthrough.pause());
    ASSERT_EQ(FailPointTriggerModeType::ENABLE, passthrough.mode());
}

} // namespace starrocks
