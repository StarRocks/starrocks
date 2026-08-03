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

#include "exec/pipeline/schedule/pipeline_timer.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>
#include <vector>

#include "butil/time.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/query_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/brpc_stub_cache.h"

namespace starrocks::pipeline {

namespace {

constexpr int TIMER_TASK_REMOVED = 0;
constexpr int TIMER_TASK_RUNNING = 1;

timespec past_abstime() {
    timespec ts = butil::microseconds_to_timespec(butil::gettimeofday_us());
    ts.tv_sec -= 10;
    return ts;
}

timespec future_abstime(int seconds) {
    return butil::seconds_from_now(seconds);
}

// Minimal PipelineTimerTask that records Run() invocations and lets the test
// choreograph when Run() returns. Unlike the lambda tasks in observer_test, this
// exposes enough hooks for testing the _finished / _has_consumer protocol.
class ProbeTask final : public PipelineTimerTask {
public:
    void Run() override {
        run_enter.release();
        ran.store(true, std::memory_order_release);
        if (block_until_released) {
            run_gate.acquire();
        }
    }

    std::atomic<bool> ran{false};
    bool block_until_released{false};
    std::counting_semaphore<> run_enter{0};
    std::counting_semaphore<> run_gate{0};
};

class LightProbe final : public LightTimerTask {
public:
    void Run() override { ran.store(true, std::memory_order_release); }
    std::atomic<bool> ran{false};
};

// Reaches PipelineDriver's protected default constructor and its protected global-RF-timer members
// so a test can drive the destructor cleanup path without standing up a full fragment. On this
// branch the driver reaches the timer through _fragment_ctx->pipeline_timer(), so the test supplies
// a FragmentContext bound to the shared test timer (see TimerBoundFragment).
class TimerTestPipelineDriver final : public PipelineDriver {
public:
    TimerTestPipelineDriver() = default;

    void register_global_rf_timer(FragmentContext* fragment_ctx, std::shared_ptr<PipelineTimerTask> task) {
        _fragment_ctx = fragment_ctx;
        _global_rf_timer = std::move(task);
    }
};

} // namespace

PipelineTimer timer;

std::once_flag timer_init_flag;

class PipelineTimerTaskTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::call_once(timer_init_flag, []() { ASSERT_OK(timer.start()); });
    }
};

// Scheduled task with past abstime must run, and unschedule after completion
// must return promptly without wedging in waitUtilFinished.
TEST_F(PipelineTimerTaskTest, runs_when_due_and_unschedule_after_done_does_not_block) {
    auto task = std::make_shared<ProbeTask>();
    ASSERT_OK(timer.schedule(task.get(), past_abstime()));
    task->run_enter.acquire();
    // Give doRun a moment to finish its post-Run bookkeeping.
    while (!task->ran.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // task.unschedule drives waitUtilFinished only when rc == 1 (running).
    // Either way, the call must not deadlock.
    auto start = std::chrono::steady_clock::now();
    task->unschedule_and_wait(&timer);
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(5));
    EXPECT_TRUE(task->ran.load(std::memory_order_acquire));
}

// Unscheduling a not-yet-due task must remove it cleanly (rc == 0) without waiting.
TEST_F(PipelineTimerTaskTest, unschedule_pending_task_removes_without_wait) {
    auto task = std::make_shared<ProbeTask>();
    ASSERT_OK(timer.schedule(task.get(), future_abstime(3600)));

    int rc = timer.unschedule(task.get());
    EXPECT_EQ(rc, TIMER_TASK_REMOVED);

    // Give bthread a beat; Run should never be invoked.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(task->ran.load(std::memory_order_acquire));
}

// When Run is in progress, unschedule reports TIMER_TASK_RUNNING and
// PipelineTimerTask::unschedule blocks inside waitUtilFinished until Run returns.
TEST_F(PipelineTimerTaskTest, wait_util_finished_blocks_until_run_returns) {
    auto task = std::make_shared<ProbeTask>();
    task->block_until_released = true;

    ASSERT_OK(timer.schedule(task.get(), past_abstime()));
    // Wait until Run has started on the timer thread.
    task->run_enter.acquire();

    // bthread has consumed the task from its heap -> unschedule must say "running".
    int rc = timer.unschedule(task.get());
    EXPECT_EQ(rc, TIMER_TASK_RUNNING);

    // Let another thread call waitUtilFinished; unblock Run a bit later and verify
    // the waiter returns.
    std::atomic<bool> waiter_returned{false};
    std::thread waiter([&] {
        task->waitUtilFinished();
        waiter_returned.store(true, std::memory_order_release);
    });

    // Confirm the waiter is actually blocked, i.e., Run hasn't returned yet.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(waiter_returned.load(std::memory_order_acquire));

    // Release Run; the waiter must observe _finished and return.
    task->run_gate.release();
    waiter.join();
    EXPECT_TRUE(waiter_returned.load(std::memory_order_acquire));
    EXPECT_TRUE(task->ran.load(std::memory_order_acquire));
}

// waitUtilFinished must be a no-op once the task has already finished.
TEST_F(PipelineTimerTaskTest, wait_util_finished_returns_immediately_when_already_done) {
    auto task = std::make_shared<ProbeTask>();
    ASSERT_OK(timer.schedule(task.get(), past_abstime()));
    task->run_enter.acquire();

    // Drain post-Run state by asking unschedule to synchronize.
    task->unschedule_and_wait(&timer);

    auto start = std::chrono::steady_clock::now();
    task->waitUtilFinished();
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::milliseconds(50));
}

// Dekker / lost-wakeup stress. Each iteration creates a fresh task, schedules it
// with a past abstime and immediately calls PipelineTimerTask::unschedule. This
// races the waiter's store of _has_consumer against doRun()'s _finished store +
// _has_consumer load. A regression on the memory ordering (or on mutex + CV
// coordination) will manifest as a permanent hang here; the watchdog bounds
// the failure mode to a test timeout instead of a CI stall.
TEST_F(PipelineTimerTaskTest, dekker_synchronization_stress) {
    constexpr int kIterations = 2000;
    constexpr auto kWatchdog = std::chrono::seconds(30);

    std::atomic<int> completed{0};
    std::atomic<bool> done{false};

    std::thread watchdog([&] {
        auto deadline = std::chrono::steady_clock::now() + kWatchdog;
        while (!done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > deadline) {
                ADD_FAILURE() << "waitUtilFinished hang detected at iteration "
                              << completed.load(std::memory_order_acquire);
                std::_Exit(1);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    for (int i = 0; i < kIterations; ++i) {
        auto task = std::make_shared<ProbeTask>();
        ASSERT_OK(timer.schedule(task.get(), past_abstime()));
        // unschedule drives the race: may see TIMER_TASK_REMOVED (we got there
        // first), TIMER_TASK_RUNNING (bthread already popped it) or -1 (finished
        // before we looked). Only the "running" case exercises waitUtilFinished,
        // but all three must terminate quickly.
        task->unschedule_and_wait(&timer);
        completed.store(i + 1, std::memory_order_release);
    }

    done.store(true, std::memory_order_release);
    watchdog.join();
    EXPECT_EQ(completed.load(std::memory_order_acquire), kIterations);
}

TEST_F(PipelineTimerTaskTest, batched_tasks_all_unblock_eventually) {
    constexpr int kTasks = 16;
    std::vector<std::shared_ptr<ProbeTask>> tasks;
    tasks.reserve(kTasks);
    for (int i = 0; i < kTasks; ++i) {
        tasks.emplace_back(std::make_shared<ProbeTask>());
    }

    // The first task holds up the TimerThread so that the head-of-batch waiter
    // has to go through waitUtilFinished. This mirrors the production scenario
    // where a fragment finalize thread is stuck waiting for CheckFragmentTimeout
    // to return.
    tasks[0]->block_until_released = true;

    timespec when = past_abstime();
    for (auto& t : tasks) {
        ASSERT_OK(timer.schedule(t.get(), when));
    }

    // Wait for the first task to actually start running.
    tasks[0]->run_enter.acquire();

    // Fire off one waiter per task. Each one mimics clear_pipeline_timer.
    std::vector<std::thread> waiters;
    std::atomic<int> finished{0};
    waiters.reserve(kTasks);
    for (auto& t : tasks) {
        waiters.emplace_back([&, raw = t.get()] {
            raw->unschedule_and_wait(&timer);
            finished.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    // All tail waiters should return quickly (bthread cancels them); only the
    // head waiter should still be blocked on waitUtilFinished.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (finished.load(std::memory_order_acquire) < kTasks - 1) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
                << "tail waiters did not return while head-of-batch Run was still blocked";
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Head waiter must still be pending at this point.
    EXPECT_LT(finished.load(std::memory_order_acquire), kTasks);

    // Release the head; its waiter must now drain via the notify path.
    tasks[0]->run_gate.release();

    for (auto& w : waiters) {
        w.join();
    }
    EXPECT_EQ(finished.load(std::memory_order_acquire), kTasks);
    EXPECT_TRUE(tasks[0]->ran.load(std::memory_order_acquire));
}

// Builds a FragmentContext whose pipeline_timer() returns the shared test timer, so a
// TimerTestPipelineDriver exercises the real _fragment_ctx->pipeline_timer() unschedule path in the
// destructor. set_pipeline_timer() also schedules a fragment-timeout task 300s out; it never fires
// during the test and ~FragmentContext unschedules it on teardown.
struct TimerBoundFragment {
    std::shared_ptr<QueryContext> query_ctx = std::make_shared<QueryContext>();
    FragmentContext fragment_ctx;

    Status init() {
        auto runtime_state = std::make_shared<RuntimeState>();
        runtime_state->set_query_ctx(query_ctx.get());
        runtime_state->set_fragment_ctx(&fragment_ctx);
        fragment_ctx.set_runtime_state(std::move(runtime_state));
        return fragment_ctx.set_pipeline_timer(&timer);
    }
};

// A queued or blocked driver abandoned when the driver executor is closed during shutdown is
// destroyed without going through finalize(). The destructor must still unschedule the global
// runtime-filter timer, otherwise the timer thread runs a task whose owning shared_ptr is gone.
TEST_F(PipelineTimerTaskTest, pipeline_driver_destructor_unschedules_global_rf_timer) {
    TimerBoundFragment fragment;
    ASSERT_OK(fragment.init());

    auto task = std::make_shared<ProbeTask>();
    ASSERT_OK(timer.schedule(task.get(), future_abstime(3600)));
    ASSERT_NE(0, task->tid());

    auto driver = std::make_unique<TimerTestPipelineDriver>();
    driver->register_global_rf_timer(&fragment.fragment_ctx, task);

    // Intentionally skip finalize().
    driver.reset();

    // The destructor must already have removed the task; a second unschedule finds nothing.
    EXPECT_EQ(-1, timer.unschedule(task.get()));
}

// The destructor must block until an already-running global RF timer task returns, mirroring
// finalize()'s unschedule_and_wait, so the task is never freed while Run() is still executing.
TEST_F(PipelineTimerTaskTest, pipeline_driver_destructor_waits_for_running_global_rf_timer) {
    TimerBoundFragment fragment;
    ASSERT_OK(fragment.init());

    auto task = std::make_shared<ProbeTask>();
    task->block_until_released = true;
    ASSERT_OK(timer.schedule(task.get(), past_abstime()));
    task->run_enter.acquire();

    auto driver = std::make_unique<TimerTestPipelineDriver>();
    driver->register_global_rf_timer(&fragment.fragment_ctx, task);

    std::atomic<bool> destructor_returned{false};
    std::thread destructor([driver = std::move(driver), &destructor_returned]() mutable {
        driver.reset();
        destructor_returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(destructor_returned.load(std::memory_order_acquire));

    task->run_gate.release();
    destructor.join();
    EXPECT_TRUE(destructor_returned.load(std::memory_order_acquire));
}

// LightTimerTask goes through a separate schedule/unschedule pair and lacks the
// _finished / _has_consumer protocol. Cover it to prevent regressions in the
// other overload of PipelineTimer.
TEST_F(PipelineTimerTaskTest, light_timer_task_runs_and_unschedules) {
    {
        LightProbe expired;
        ASSERT_OK(timer.schedule(&expired, past_abstime()));
        for (int i = 0; i < 100 && !expired.ran.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_TRUE(expired.ran.load(std::memory_order_acquire));
    }
    {
        LightProbe pending;
        ASSERT_OK(timer.schedule(&pending, future_abstime(3600)));
        int rc = timer.unschedule(&pending);
        EXPECT_EQ(rc, TIMER_TASK_REMOVED);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        EXPECT_FALSE(pending.ran.load(std::memory_order_acquire));
    }
}

} // namespace starrocks::pipeline
