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
#include <cstdio>
#include <fstream>
#include <future>
#include <thread>
#include <vector>

#include "base/concurrency/await.h"
#include "base/utility/defer_op.h"

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

// Wait until |fp| reports |expected| parked threads, so the pause tests synchronise on real state
// instead of a fixed sleep. Returns false if the state is not reached within the budget.
bool wait_for_parked(failpoint::FailPoint& fp, int64_t expected, int64_t budget_us = 10 * 1000 * 1000) {
    return Awaitility().timeout(budget_us).until([&] { return fp.to_pb().paused_thread_count() == expected; });
}

// Always disables |fp| and joins |threads| on scope exit, so a failed ASSERT_* cannot leave a
// joinable std::thread behind -- that would call std::terminate and take the whole suite down.
auto pause_test_cleanup(failpoint::FailPoint& fp, std::vector<std::thread>& threads) {
    return DeferOp([&fp, &threads] {
        fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    });
}

struct BoundedRun {
    bool finished;
    bool injected;
    int64_t elapsed_ms;
};

// Runs fp.shouldFail() on a worker bounded by |budget_seconds|, so a regression in the timeout path
// fails the test instead of hanging the suite (run-be-ut.sh imposes no per-test limit). On timeout it
// disables the failpoint through the other code path to unblock the worker.
BoundedRun should_fail_bounded(failpoint::FailPoint& fp, int budget_seconds = 20) {
    std::promise<bool> done;
    auto future = done.get_future();
    auto start = std::chrono::steady_clock::now();
    std::thread worker([&] { done.set_value(fp.shouldFail()); });

    const bool finished = future.wait_for(std::chrono::seconds(budget_seconds)) == std::future_status::ready;
    if (!finished) {
        fp.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    }
    worker.join();
    const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    return {finished, finished && future.get(), elapsed_ms};
}

} // namespace

TEST(FailPointTest, pause_released_by_disable) {
    failpoint::FailPoint fp("test_pause_disable");
    fp.setMode(pause_mode(0));

    std::atomic<bool> returned{false};
    std::atomic<bool> result{true};
    std::vector<std::thread> threads;
    auto cleanup = pause_test_cleanup(fp, threads);
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
    auto cleanup = pause_test_cleanup(fp, threads);
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
    auto cleanup = pause_test_cleanup(fp, threads);
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

    const auto run = should_fail_bounded(fp);
    ASSERT_TRUE(run.finished) << "pause_timeout_second was ignored: shouldFail() never returned on its own";
    ASSERT_FALSE(run.injected);
    ASSERT_GE(run.elapsed_ms, 900);
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
    auto cleanup = pause_test_cleanup(fp, threads);
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
    auto cleanup = pause_test_cleanup(fp, threads);
    threads.emplace_back([&] { (void)fp.shouldFail(); });
    ASSERT_TRUE(wait_for_parked(fp, 1));

    // A short observation window is enough: with the 300s default armed, a broken fallback would
    // resume the thread immediately. Kept small to match this file's no-long-fixed-sleeps convention.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
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

TEST(FailPointTest, init_from_conf_pause_mode) {
    failpoint::FailPoint fp("test_conf_pause");
    ASSERT_TRUE(failpoint::FailPointRegistry::GetInstance()->add(&fp).ok());

    const std::string conf_path = "./fail_point_conf_pause.json";
    {
        std::ofstream out(conf_path);
        out << R"({"test_conf_pause": {"mode": "pause", "pause_timeout_second": 1}})";
    }

    ASSERT_TRUE(failpoint::init_failpoint_from_conf(conf_path));
    ASSERT_TRUE(fp.to_pb().trigger_mode().pause());
    ASSERT_EQ(1, fp.to_pb().trigger_mode().pause_timeout_second());

    // The armed timeout is honoured: the call returns on its own without any setMode().
    const auto run = should_fail_bounded(fp);
    ASSERT_TRUE(run.finished) << "conf-file pause_timeout_second was ignored";
    ASSERT_FALSE(run.injected);
    ASSERT_GE(run.elapsed_ms, 900);

    std::remove(conf_path.c_str());
}

// FailPointRegistry stores a RAW pointer and has no removal API, so a failpoint handed to it must
// outlive the test. File-scope here, so the registration also happens exactly once under
// --gtest_repeat.
static failpoint::FailPoint s_conf_unknown_mode_fp("test_conf_unknown_mode");
static const bool s_conf_unknown_mode_registered =
        failpoint::FailPointRegistry::GetInstance()->add(&s_conf_unknown_mode_fp).ok();

// A typo in the conf file's "mode" must be rejected. Before this was fixed the unmatched string left
// PFailPointTriggerMode.mode unset, and proto2 reports an unset optional enum as its FIRST value --
// ENABLE -- so a typo silently ARMED the failpoint instead of reporting an error. Assert both halves:
// the parse fails, and the failpoint is left untouched.
TEST(FailPointTest, init_from_conf_rejects_unknown_mode) {
    ASSERT_TRUE(s_conf_unknown_mode_registered);

    const std::string conf_path = "./fail_point_conf_unknown_mode.json";
    {
        std::ofstream out(conf_path);
        out << R"({"test_conf_unknown_mode": {"mode": "nonsense"}})";
    }
    DeferOp remove_conf([&] { std::remove(conf_path.c_str()); });

    ASSERT_FALSE(failpoint::init_failpoint_from_conf(conf_path));
    // A freshly constructed FailPoint is DISABLE, so DISABLE here proves setMode() was never reached
    // -- the parse bailed instead of arming.
    ASSERT_EQ(FailPointTriggerModeType::DISABLE, s_conf_unknown_mode_fp.to_pb().trigger_mode().mode());
    ASSERT_FALSE(s_conf_unknown_mode_fp.to_pb().trigger_mode().pause());
    ASSERT_FALSE(s_conf_unknown_mode_fp.shouldFail());
}

// Every other pause test calls fp.shouldFail() directly, which bypasses libfiu entirely. In
// production the only caller is libfiu's external callback, invoked from inside fiu_fail() while it
// holds a __thread recursion counter and a pthread_rwlock read lock. This test drives the real macro
// so that a pause which corrupts that per-thread state -- by yielding to another pthread, say -- is
// caught here rather than silently disabling every failpoint on a worker in production.
TEST(FailPointTest, pause_through_the_fiu_trigger_macro) {
    DEFINE_FAIL_POINT(fp_pause_via_fiu);

    auto test_func = [&]() {
        FAIL_POINT_TRIGGER_RETURN(fp_pause_via_fiu, false);
        return true;
    };

    ASSERT_TRUE(test_func());

    fp_fp_pause_via_fiu.setMode(pause_mode(1));
    // Parks inside fiu_fail(), then times out, disarms and resumes without injecting.
    ASSERT_TRUE(test_func());

    // fiu's recursion guard must still be balanced: a subsequent ENABLE has to fire on this same
    // thread. If the pause left rec_count elevated, fiu_fail() would take its early return and this
    // would wrongly report success.
    fp_fp_pause_via_fiu.setMode(simple_mode(FailPointTriggerModeType::ENABLE));
    ASSERT_FALSE(test_func());

    fp_fp_pause_via_fiu.setMode(simple_mode(FailPointTriggerModeType::DISABLE));
    ASSERT_TRUE(test_func());
}

} // namespace starrocks
