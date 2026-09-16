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

#include "platform/llm/ai_runtime.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"
#include "common/system/cpu_info.h"

namespace starrocks {
namespace {

using namespace std::chrono_literals;

class Event {
public:
    void notify() {
        {
            std::lock_guard lock(_mutex);
            _notified = true;
        }
        _cv.notify_all();
    }

    void wait() {
        std::unique_lock lock(_mutex);
        _cv.wait(lock, [this] { return _notified; });
    }

    bool wait_for(std::chrono::milliseconds timeout = 5s) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, timeout, [this] { return _notified; });
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _notified = false;
};

class StartGate {
public:
    void wait() {
        std::unique_lock lock(_mutex);
        ++_waiters;
        _all_waiting.notify_all();
        _start.wait(lock, [this] { return _started; });
    }

    bool wait_for_waiters(size_t count, std::chrono::milliseconds timeout = 5s) {
        std::unique_lock lock(_mutex);
        return _all_waiting.wait_for(lock, timeout, [this, count] { return _waiters >= count; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _started = true;
        }
        _start.notify_all();
    }

private:
    std::mutex _mutex;
    std::condition_variable _all_waiting;
    std::condition_variable _start;
    size_t _waiters = 0;
    bool _started = false;
};

bool same_runtime_config(const AIRuntimeConfig& lhs, const AIRuntimeConfig& rhs) {
    return lhs.request_timeout_ms == rhs.request_timeout_ms && lhs.connect_timeout_ms == rhs.connect_timeout_ms &&
           lhs.max_response_bytes == rhs.max_response_bytes && lhs.worker_thread_num == rhs.worker_thread_num &&
           lhs.sub_chunk_size == rhs.sub_chunk_size && lhs.max_retries == rhs.max_retries &&
           lhs.max_retries_on_throttle == rhs.max_retries_on_throttle && lhs.on_error == rhs.on_error &&
           lhs.rate_limit_qps_chat == rhs.rate_limit_qps_chat && lhs.max_inflight == rhs.max_inflight;
}

TEST(AIRuntimeConfigTest, DefaultsAndTimeoutBoundariesAreValid) {
    AIRuntimeConfig config;
    EXPECT_EQ(600000, config.request_timeout_ms);
    EXPECT_EQ(10000, config.connect_timeout_ms);
    EXPECT_EQ(8388608, config.max_response_bytes);
    EXPECT_EQ(16, config.worker_thread_num);
    EXPECT_EQ(64, config.sub_chunk_size);
    EXPECT_EQ(3, config.max_retries);
    EXPECT_EQ(5, config.max_retries_on_throttle);
    EXPECT_EQ("ignore", config.on_error);
    EXPECT_EQ(128, config.rate_limit_qps_chat);
    EXPECT_EQ(512, config.max_inflight);
    EXPECT_TRUE(config.validate().ok());

    config.request_timeout_ms = 0;
    config.connect_timeout_ms = 0;
    config.max_response_bytes = 1;
    config.worker_thread_num = 1;
    config.sub_chunk_size = 1;
    config.max_retries = 0;
    config.max_retries_on_throttle = 0;
    config.rate_limit_qps_chat = 1;
    config.max_inflight = 1;
    EXPECT_TRUE(config.validate().ok());
}

TEST(AIRuntimeConfigTest, RejectsEveryInvalidFieldWithoutEchoingValues) {
    std::vector<AIRuntimeConfig> invalid_configs;
    AIRuntimeConfig config;

    config.request_timeout_ms = -1;
    invalid_configs.emplace_back(config);
    config = {};
    config.connect_timeout_ms = -1;
    invalid_configs.emplace_back(config);
    config = {};
    config.max_response_bytes = 0;
    invalid_configs.emplace_back(config);
    config = {};
    config.worker_thread_num = 0;
    invalid_configs.emplace_back(config);
    config = {};
    config.sub_chunk_size = 0;
    invalid_configs.emplace_back(config);
    config = {};
    config.max_retries = -1;
    invalid_configs.emplace_back(config);
    config = {};
    config.max_retries_on_throttle = -1;
    invalid_configs.emplace_back(config);
    config = {};
    config.on_error = "do-not-echo-policy-value";
    invalid_configs.emplace_back(config);
    config = {};
    config.rate_limit_qps_chat = 0;
    invalid_configs.emplace_back(config);
    config = {};
    config.max_inflight = 0;
    invalid_configs.emplace_back(config);

    for (const AIRuntimeConfig& invalid : invalid_configs) {
        Status status = invalid.validate();
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(std::string_view::npos, status.message().find("do-not-echo-policy-value")) << status;
    }
}

TEST(AIRuntimeConfigSourceTest, InvalidUpdateRetainsThePreviousCompleteSnapshot) {
    auto source_or = AIRuntimeConfigSource::create();
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();
    const AIRuntimeConfig original = source->snapshot();

    AIRuntimeConfig invalid = original;
    invalid.worker_thread_num = 0;
    Status status = source->update(std::move(invalid));

    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_TRUE(same_runtime_config(original, source->snapshot()));
}

TEST(AIRuntimeConfigSourceTest, PreparedSnapshotIsInvisibleUntilNoFailPublish) {
    auto source_or = AIRuntimeConfigSource::create();
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();
    const AIRuntimeConfig original = source->snapshot();
    AIRuntimeConfig candidate = original;
    candidate.worker_thread_num = original.worker_thread_num + 1;
    candidate.max_inflight = original.max_inflight + 1;

    auto prepared = source->prepare(candidate);

    ASSERT_TRUE(prepared.ok()) << prepared.status();
    EXPECT_TRUE(same_runtime_config(original, source->snapshot()));
    source->publish(std::move(prepared).value());
    EXPECT_TRUE(same_runtime_config(candidate, source->snapshot()));
}

TEST(AIRuntimeConfigSourceTest, PreparationAllocationFailureCannotChangePublishedSnapshot) {
    auto source_or = AIRuntimeConfigSource::create();
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();
    const AIRuntimeConfig original = source->snapshot();
    AIRuntimeConfig candidate = original;
    candidate.worker_thread_num = original.worker_thread_num + 1;

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->SetCallBack("AIRuntimeConfigSource::prepare:before_snapshot_allocation",
                            [](void*) { throw std::bad_alloc(); });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->DisableProcessing();
        sync_point->ClearAllCallBacks();
    });

    auto prepared = source->prepare(std::move(candidate));

    ASSERT_FALSE(prepared.ok());
    EXPECT_TRUE(prepared.status().is_mem_limit_exceeded()) << prepared.status();
    EXPECT_TRUE(same_runtime_config(original, source->snapshot()));
}

TEST(AIRuntimeConfigSourceTest, InvalidInitialSnapshotIsRejected) {
    AIRuntimeConfig invalid;
    invalid.max_inflight = 0;

    auto source_or = AIRuntimeConfigSource::create(std::move(invalid));

    EXPECT_TRUE(source_or.status().is_invalid_argument()) << source_or.status();
}

TEST(AIRuntimeConfigSourceTest, LiveAdmissionLimitsComeFromThePublishedSnapshot) {
    auto source_or = AIRuntimeConfigSource::create();
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();
    AIRuntimeConfig updated = source->snapshot();
    updated.worker_thread_num = 123;
    updated.rate_limit_qps_chat = 321;
    updated.max_inflight = 654;

    ASSERT_TRUE(source->update(std::move(updated)).ok());

    EXPECT_EQ(123, source->worker_thread_num());
    EXPECT_EQ(321, source->qps(AICapability::CHAT));
    EXPECT_EQ(654, source->max_inflight());
}

TEST(AIRuntimeConfigSourceTest, UnknownCapabilityFailsClosedWithoutUsingChatQuota) {
    auto source_or = AIRuntimeConfigSource::create();
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();

    EXPECT_GT(source->qps(AICapability::CHAT), 0);
    EXPECT_EQ(0, source->qps(static_cast<AICapability>(std::numeric_limits<uint8_t>::max())));
}

TEST(AIRuntimeConfigSourceTest, ConcurrentReadersObserveOnlyCompletePublishedSnapshots) {
    AIRuntimeConfig old_config;
    old_config.request_timeout_ms = 101;
    old_config.connect_timeout_ms = 102;
    old_config.max_response_bytes = 103;
    old_config.worker_thread_num = 104;
    old_config.sub_chunk_size = 105;
    old_config.max_retries = 106;
    old_config.max_retries_on_throttle = 107;
    old_config.on_error = "ignore";
    old_config.rate_limit_qps_chat = 108;
    old_config.max_inflight = 109;

    AIRuntimeConfig new_config;
    new_config.request_timeout_ms = 201;
    new_config.connect_timeout_ms = 202;
    new_config.max_response_bytes = 203;
    new_config.worker_thread_num = 204;
    new_config.sub_chunk_size = 205;
    new_config.max_retries = 206;
    new_config.max_retries_on_throttle = 207;
    new_config.on_error = "fail";
    new_config.rate_limit_qps_chat = 208;
    new_config.max_inflight = 209;

    auto source_or = AIRuntimeConfigSource::create(old_config);
    ASSERT_TRUE(source_or.ok()) << source_or.status();
    auto source = std::move(source_or).value();
    constexpr size_t kReaderCount = 8;
    constexpr size_t kUpdateCount = 20'000;
    StartGate gate;
    std::atomic<bool> writer_done{false};
    std::atomic<bool> writer_failed{false};
    std::atomic<bool> saw_mixed_snapshot{false};
    std::atomic<bool> saw_new_snapshot{false};
    std::atomic<size_t> readers_observing{0};
    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);

    for (size_t i = 0; i < kReaderCount; ++i) {
        readers.emplace_back([&] {
            gate.wait();
            AIRuntimeConfig observed = source->snapshot();
            if (!same_runtime_config(observed, old_config) && !same_runtime_config(observed, new_config)) {
                saw_mixed_snapshot.store(true, std::memory_order_release);
            }
            readers_observing.fetch_add(1, std::memory_order_release);
            while (!writer_done.load(std::memory_order_acquire)) {
                observed = source->snapshot();
                if (same_runtime_config(observed, new_config)) {
                    saw_new_snapshot.store(true, std::memory_order_release);
                }
                if (!same_runtime_config(observed, old_config) && !same_runtime_config(observed, new_config)) {
                    saw_mixed_snapshot.store(true, std::memory_order_release);
                }
            }
        });
    }
    std::thread writer([&] {
        gate.wait();
        while (readers_observing.load(std::memory_order_acquire) != kReaderCount) {
            std::this_thread::yield();
        }
        if (!source->update(new_config).ok()) {
            writer_failed.store(true, std::memory_order_release);
        }
        const auto observation_deadline = std::chrono::steady_clock::now() + 5s;
        while (!writer_failed.load(std::memory_order_acquire) && !saw_new_snapshot.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < observation_deadline) {
            std::this_thread::yield();
        }
        if (!saw_new_snapshot.load(std::memory_order_acquire)) {
            writer_failed.store(true, std::memory_order_release);
        }
        for (size_t i = 1; i < kUpdateCount && !writer_failed.load(std::memory_order_acquire); ++i) {
            if (!source->update(i % 2 == 0 ? new_config : old_config).ok()) {
                writer_failed.store(true, std::memory_order_release);
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    const bool all_threads_ready = gate.wait_for_waiters(kReaderCount + 1);
    gate.release();
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }

    ASSERT_TRUE(all_threads_ready);
    EXPECT_FALSE(writer_failed.load(std::memory_order_acquire));
    EXPECT_TRUE(saw_new_snapshot.load(std::memory_order_acquire));
    EXPECT_FALSE(saw_mixed_snapshot.load(std::memory_order_acquire));
}

TEST(AIRuntimeTest, CompletionCapacityUsesWideSaturatingAddition) {
    EXPECT_EQ(528, ai_completion_capacity(512, 16));

    const int64_t int_max = std::numeric_limits<int>::max();
    EXPECT_EQ(std::numeric_limits<int>::max(), ai_completion_capacity(int_max, int_max));
}

TEST(AIControlThreadSchedulerTest, PostNeverRunsInlineAndTasksAreStrictlySerialized) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    const std::thread::id submitting_thread = std::this_thread::get_id();
    Event first_started;
    Event release_first;
    Event second_started;
    Event all_done;
    std::atomic<bool> ran_on_submitting_thread{false};
    std::atomic<int> active{0};
    std::atomic<int> maximum_active{0};
    std::atomic<int> completed{0};

    auto enter = [&] {
        const int current = active.fetch_add(1, std::memory_order_acq_rel) + 1;
        int observed = maximum_active.load(std::memory_order_acquire);
        while (current > observed && !maximum_active.compare_exchange_weak(observed, current)) {
        }
    };
    auto leave = [&] {
        active.fetch_sub(1, std::memory_order_acq_rel);
        if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == 2) {
            all_done.notify();
        }
    };
    SCOPED_CLEANUP({
        release_first.notify();
        scheduler->shutdown_and_drain();
    });

    ASSERT_TRUE(scheduler
                        ->post([&] {
                            ran_on_submitting_thread.store(std::this_thread::get_id() == submitting_thread,
                                                           std::memory_order_release);
                            enter();
                            first_started.notify();
                            release_first.wait();
                            leave();
                        })
                        .ok());
    ASSERT_TRUE(first_started.wait_for());

    ASSERT_TRUE(scheduler
                        ->post([&] {
                            enter();
                            second_started.notify();
                            leave();
                        })
                        .ok());

    const bool overlapped_first = second_started.wait_for(100ms);
    release_first.notify();
    ASSERT_TRUE(all_done.wait_for());
    scheduler->shutdown_and_drain();

    EXPECT_FALSE(ran_on_submitting_thread.load(std::memory_order_acquire));
    EXPECT_FALSE(overlapped_first);
    EXPECT_EQ(1, maximum_active.load(std::memory_order_acquire));
}

TEST(AIControlThreadSchedulerTest, CancelledTimerDoesNotRun) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    SystemAIClock clock;
    Event blocker_started;
    Event release_blocker;
    Event sentinel_ran;
    std::atomic<bool> cancelled_timer_ran{false};
    SCOPED_CLEANUP({
        release_blocker.notify();
        scheduler->shutdown_and_drain();
    });
    ASSERT_TRUE(scheduler
                        ->post([&] {
                            blocker_started.notify();
                            release_blocker.wait();
                        })
                        .ok());
    ASSERT_TRUE(blocker_started.wait_for());

    auto cancelled = scheduler->schedule_at(clock.monotonic_now_ns(),
                                            [&] { cancelled_timer_ran.store(true, std::memory_order_release); });
    ASSERT_TRUE(cancelled.ok()) << cancelled.status();
    scheduler->cancel(std::move(cancelled).value());
    ASSERT_TRUE(scheduler->post([&] { sentinel_ran.notify(); }).ok());

    release_blocker.notify();
    ASSERT_TRUE(sentinel_ran.wait_for(5s));
    scheduler->shutdown_and_drain();
    EXPECT_FALSE(cancelled_timer_ran.load(std::memory_order_acquire));
}

TEST(AIControlThreadSchedulerTest, ShutdownDrainsAcceptedImmediateWorkAndIsIdempotent) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    Event first_started;
    Event release_first;
    std::atomic<int> run_count{0};
    SCOPED_CLEANUP({
        release_first.notify();
        scheduler->shutdown_and_drain();
    });
    ASSERT_TRUE(scheduler
                        ->post([&] {
                            first_started.notify();
                            release_first.wait();
                            run_count.fetch_add(1, std::memory_order_relaxed);
                        })
                        .ok());
    ASSERT_TRUE(first_started.wait_for());
    ASSERT_TRUE(scheduler->post([&] { run_count.fetch_add(1, std::memory_order_relaxed); }).ok());

    std::thread shutdown_thread([&] { scheduler->shutdown_and_drain(); });
    release_first.notify();
    shutdown_thread.join();
    EXPECT_EQ(2, run_count.load(std::memory_order_relaxed));

    scheduler->shutdown_and_drain();
    EXPECT_EQ(2, run_count.load(std::memory_order_relaxed));
}

TEST(AIControlThreadSchedulerTest, ThrowingTaskDoesNotStopLaterControlWork) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    Event later_work_ran;
    SCOPED_CLEANUP({ scheduler->shutdown_and_drain(); });
    ASSERT_TRUE(scheduler->post([] { throw std::runtime_error("test control failure"); }).ok());
    ASSERT_TRUE(scheduler->post([&] { later_work_ran.notify(); }).ok());

    ASSERT_TRUE(later_work_ran.wait_for());
}

TEST(AIControlThreadSchedulerTest, PostAllocationFailureRejectsWithoutRetainingTask) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
        scheduler->shutdown_and_drain();
    });

    std::atomic<int> run_count{0};
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> weak_owner = owner;
    sync_point->SetCallBack("AIControlThreadScheduler::post:before_ready_emplace",
                            [](void*) { throw std::bad_alloc(); });
    Status rejected =
            scheduler->post([&, owner = std::move(owner)] { run_count.fetch_add(*owner, std::memory_order_relaxed); });
    EXPECT_TRUE(rejected.is_mem_limit_exceeded()) << rejected;
    EXPECT_TRUE(weak_owner.expired());
    sync_point->ClearCallBack("AIControlThreadScheduler::post:before_ready_emplace");

    Event accepted_ran;
    Status accepted = scheduler->post([&] {
        run_count.fetch_add(1, std::memory_order_relaxed);
        accepted_ran.notify();
    });
    ASSERT_TRUE(accepted.ok()) << accepted;
    ASSERT_TRUE(accepted_ran.wait_for());
    EXPECT_EQ(1, run_count.load(std::memory_order_relaxed));
}

TEST(AIControlThreadSchedulerTest, TimerPrimaryAllocationFailureRejectsWithoutRetainingTask) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
        scheduler->shutdown_and_drain();
    });

    SystemAIClock clock;
    std::atomic<int> rejected_run_count{0};
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> weak_owner = owner;
    sync_point->SetCallBack("AIControlThreadScheduler::schedule_at:before_timer_emplace",
                            [](void*) { throw std::bad_alloc(); });
    auto rejected = scheduler->schedule_at(clock.monotonic_now_ns(), [&, owner = std::move(owner)] {
        rejected_run_count.fetch_add(*owner, std::memory_order_relaxed);
    });
    ASSERT_FALSE(rejected.ok());
    EXPECT_TRUE(rejected.status().is_mem_limit_exceeded()) << rejected.status();
    EXPECT_TRUE(weak_owner.expired());
    sync_point->ClearCallBack("AIControlThreadScheduler::schedule_at:before_timer_emplace");

    Event accepted_ran;
    auto accepted = scheduler->schedule_at(clock.monotonic_now_ns(), [&] { accepted_ran.notify(); });
    ASSERT_TRUE(accepted.ok()) << accepted.status();
    ASSERT_TRUE(accepted_ran.wait_for());
    EXPECT_EQ(0, rejected_run_count.load(std::memory_order_relaxed));
}

TEST(AIControlThreadSchedulerTest, TimerReverseIndexAllocationFailureRollsBackPrimaryTimer) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    Event blocker_started;
    Event release_blocker;
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        release_blocker.notify();
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
        scheduler->shutdown_and_drain();
    });

    ASSERT_TRUE(scheduler
                        ->post([&] {
                            blocker_started.notify();
                            release_blocker.wait();
                        })
                        .ok());
    ASSERT_TRUE(blocker_started.wait_for());

    SystemAIClock clock;
    std::atomic<int> rejected_run_count{0};
    auto owner = std::make_shared<int>(1);
    std::weak_ptr<int> weak_owner = owner;
    sync_point->SetCallBack("AIControlThreadScheduler::schedule_at:before_timer_key_emplace",
                            [](void*) { throw std::bad_alloc(); });
    auto rejected = scheduler->schedule_at(clock.monotonic_now_ns(), [&, owner = std::move(owner)] {
        rejected_run_count.fetch_add(*owner, std::memory_order_relaxed);
    });
    ASSERT_FALSE(rejected.ok());
    EXPECT_TRUE(rejected.status().is_mem_limit_exceeded()) << rejected.status();
    EXPECT_TRUE(weak_owner.expired());
    sync_point->ClearCallBack("AIControlThreadScheduler::schedule_at:before_timer_key_emplace");

    Event accepted_timer_ran;
    auto accepted = scheduler->schedule_at(clock.monotonic_now_ns(), [&] { accepted_timer_ran.notify(); });
    ASSERT_TRUE(accepted.ok()) << accepted.status();
    release_blocker.notify();
    ASSERT_TRUE(accepted_timer_ran.wait_for());
    EXPECT_EQ(0, rejected_run_count.load(std::memory_order_relaxed));
}

TEST(AIControlThreadSchedulerTest, ShutdownRejectsNewImmediateAndTimerWork) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();
    scheduler->shutdown_and_drain();

    std::atomic<int> run_count{0};
    Status post_status = scheduler->post([&] { run_count.fetch_add(1, std::memory_order_relaxed); });
    EXPECT_TRUE(post_status.is_shutdown()) << post_status;

    SystemAIClock clock;
    auto timer = scheduler->schedule_at(clock.monotonic_now_ns(),
                                        [&] { run_count.fetch_add(1, std::memory_order_relaxed); });
    ASSERT_FALSE(timer.ok());
    EXPECT_TRUE(timer.status().is_shutdown()) << timer.status();
    EXPECT_EQ(0, run_count.load(std::memory_order_relaxed));
}

TEST(AIControlThreadSchedulerTest, DueTimerReadyAllocationFailureRunsExactlyOnceAndKeepsControlThreadAlive) {
    auto scheduler_or = AIControlThreadScheduler::create();
    ASSERT_TRUE(scheduler_or.ok()) << scheduler_or.status();
    auto scheduler = std::move(scheduler_or).value();

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
        scheduler->shutdown_and_drain();
    });

    sync_point->SetCallBack("AIControlThreadScheduler::run:before_ready_emplace",
                            [](void*) { throw std::bad_alloc(); });
    const std::thread::id submitting_thread = std::this_thread::get_id();
    std::atomic<int> timer_run_count{0};
    std::atomic<bool> ran_on_submitting_thread{false};
    std::atomic<bool> nested_post_succeeded{false};
    Event nested_task_ran;
    SystemAIClock clock;
    auto timer = scheduler->schedule_at(clock.monotonic_now_ns(), [&] {
        timer_run_count.fetch_add(1, std::memory_order_relaxed);
        ran_on_submitting_thread.store(std::this_thread::get_id() == submitting_thread, std::memory_order_release);
        Status nested = scheduler->post([&] { nested_task_ran.notify(); });
        nested_post_succeeded.store(nested.ok(), std::memory_order_release);
    });
    ASSERT_TRUE(timer.ok()) << timer.status();
    ASSERT_TRUE(nested_task_ran.wait_for());
    sync_point->ClearCallBack("AIControlThreadScheduler::run:before_ready_emplace");

    Event later_task_ran;
    Status later = scheduler->post([&] { later_task_ran.notify(); });
    ASSERT_TRUE(later.ok()) << later;
    ASSERT_TRUE(later_task_ran.wait_for());
    EXPECT_EQ(1, timer_run_count.load(std::memory_order_relaxed));
    EXPECT_FALSE(ran_on_submitting_thread.load(std::memory_order_acquire));
    EXPECT_TRUE(nested_post_succeeded.load(std::memory_order_acquire));
}

class AIThreadPoolCompletionExecutorTest : public ::testing::Test {
public:
    static void SetUpTestSuite() { CpuInfo::init(); }
};

TEST_F(AIThreadPoolCompletionExecutorTest, AcceptedWorkNeverRunsInline) {
    std::atomic<int> worker_thread_creations{0};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->EnableProcessing();
    sync_point->SetCallBack("ThreadPool::create_thread",
                            [&](void*) { worker_thread_creations.fetch_add(1, std::memory_order_relaxed); });
    SCOPED_CLEANUP({
        sync_point->ClearCallBack("ThreadPool::create_thread");
        sync_point->DisableProcessing();
    });

    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 1);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();
    EXPECT_EQ(0, worker_thread_creations.load(std::memory_order_relaxed))
            << "an unused completion executor must not create a worker";

    const std::thread::id submitting_thread = std::this_thread::get_id();
    Event done;
    std::atomic<bool> ran_on_submitting_thread{false};
    std::atomic<int> cancel_count{0};
    SCOPED_CLEANUP({ executor->shutdown(); });
    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 ran_on_submitting_thread.store(
                                                         std::this_thread::get_id() == submitting_thread,
                                                         std::memory_order_release);
                                                 done.notify();
                                             },
                                             [&] { cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());

    ASSERT_TRUE(done.wait_for());
    EXPECT_GE(worker_thread_creations.load(std::memory_order_relaxed), 1);
    executor->shutdown();
    EXPECT_FALSE(ran_on_submitting_thread.load(std::memory_order_acquire));
    EXPECT_EQ(0, cancel_count.load(std::memory_order_relaxed));
}

TEST_F(AIThreadPoolCompletionExecutorTest, ThrowingCompletionDoesNotEscapeOrStopLaterWork) {
    static_assert(noexcept(std::declval<AICompletionWork&>().run()));
    static_assert(noexcept(std::declval<AICompletionWork&>().cancel()));

    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 2);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();
    Event throwing_started;
    Event later_work_ran;
    SCOPED_CLEANUP({ executor->shutdown(); });

    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 throwing_started.notify();
                                                 throw std::runtime_error("test completion failure");
                                             },
                                             [] {}))
                        .ok());
    ASSERT_TRUE(executor->try_submit(AICompletionWork([&] { later_work_ran.notify(); }, [] {})).ok());

    EXPECT_TRUE(throwing_started.wait_for());
    EXPECT_TRUE(later_work_ran.wait_for());
}

TEST_F(AIThreadPoolCompletionExecutorTest, ThrowingDirectCancelIsContainedAndResolvedOnce) {
    std::atomic<int> cancel_count{0};
    AICompletionWork work([] {},
                          [&] {
                              cancel_count.fetch_add(1, std::memory_order_relaxed);
                              throw std::runtime_error("test cancellation failure");
                          });

    EXPECT_NO_THROW(work.cancel());
    EXPECT_NO_THROW(work.cancel());
    EXPECT_EQ(1, cancel_count.load(std::memory_order_relaxed));
}

TEST_F(AIThreadPoolCompletionExecutorTest, QueueFullIsResourceBusyAndRejectedCallbacksDoNotRun) {
    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 1);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();

    Event running_started;
    Event release_running;
    std::atomic<int> accepted_run_count{0};
    std::atomic<int> accepted_cancel_count{0};
    Event accepted_queue_done;
    std::atomic<int> rejected_run_count{0};
    std::atomic<int> rejected_cancel_count{0};
    SCOPED_CLEANUP({
        release_running.notify();
        executor->shutdown();
    });
    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 running_started.notify();
                                                 release_running.wait();
                                                 accepted_run_count.fetch_add(1, std::memory_order_relaxed);
                                             },
                                             [&] { accepted_cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());
    ASSERT_TRUE(running_started.wait_for());
    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 accepted_run_count.fetch_add(1, std::memory_order_relaxed);
                                                 accepted_queue_done.notify();
                                             },
                                             [&] { accepted_cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());

    Status status = executor->try_submit(
            AICompletionWork([&] { rejected_run_count.fetch_add(1, std::memory_order_relaxed); },
                             [&] { rejected_cancel_count.fetch_add(1, std::memory_order_relaxed); }));
    EXPECT_TRUE(status.is_resource_busy()) << status;
    EXPECT_FALSE(status.is_service_unavailable()) << status;
    EXPECT_EQ(0, rejected_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, rejected_cancel_count.load(std::memory_order_relaxed));

    release_running.notify();
    ASSERT_TRUE(accepted_queue_done.wait_for());
    executor->shutdown();
    EXPECT_EQ(2, accepted_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, accepted_cancel_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, rejected_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, rejected_cancel_count.load(std::memory_order_relaxed));
}

TEST_F(AIThreadPoolCompletionExecutorTest, ShutdownFinishesRunningAndCancelsQueuedWorkExactlyOnce) {
    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 2);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();

    Event running_started;
    Event release_running;
    Event queued_cancelled;
    std::atomic<int> running_run_count{0};
    std::atomic<int> running_cancel_count{0};
    std::atomic<int> queued_run_count{0};
    std::atomic<int> queued_cancel_count{0};
    SCOPED_CLEANUP({
        release_running.notify();
        executor->shutdown();
    });

    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 running_started.notify();
                                                 release_running.wait();
                                                 running_run_count.fetch_add(1, std::memory_order_relaxed);
                                             },
                                             [&] { running_cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());
    ASSERT_TRUE(running_started.wait_for());
    ASSERT_TRUE(executor->try_submit(AICompletionWork([&] { queued_run_count.fetch_add(1, std::memory_order_relaxed); },
                                                      [&] {
                                                          queued_cancel_count.fetch_add(1, std::memory_order_relaxed);
                                                          queued_cancelled.notify();
                                                      }))
                        .ok());

    std::thread shutdown_thread([&] { executor->shutdown(); });
    Status stopped_status;
    const auto observation_deadline = std::chrono::steady_clock::now() + 5s;
    do {
        stopped_status = executor->try_submit(AICompletionWork([] {}, [] {}));
        if (stopped_status.is_shutdown()) {
            break;
        }
        if (!stopped_status.ok() && !stopped_status.is_resource_busy()) {
            break;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < observation_deadline);
    release_running.notify();
    shutdown_thread.join();

    ASSERT_TRUE(stopped_status.is_shutdown()) << stopped_status;
    EXPECT_TRUE(queued_cancelled.wait_for());
    EXPECT_EQ(1, running_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, running_cancel_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, queued_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(1, queued_cancel_count.load(std::memory_order_relaxed));

    executor->shutdown();
    EXPECT_EQ(1, running_run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(1, queued_cancel_count.load(std::memory_order_relaxed));
}

TEST_F(AIThreadPoolCompletionExecutorTest, SubmitShutdownRaceResolvesEveryAcceptedWorkExactlyOnce) {
    constexpr size_t kWorkCount = 128;
    auto executor_or = AIThreadPoolCompletionExecutor::create(4, static_cast<int>(kWorkCount));
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();

    StartGate gate;
    std::array<std::atomic<int>, kWorkCount> run_counts{};
    std::array<std::atomic<int>, kWorkCount> cancel_counts{};
    std::vector<Status> statuses(kWorkCount);

    statuses[0] =
            executor->try_submit(AICompletionWork([&] { run_counts[0].fetch_add(1, std::memory_order_relaxed); },
                                                  [&] { cancel_counts[0].fetch_add(1, std::memory_order_relaxed); }));
    ASSERT_TRUE(statuses[0].ok()) << statuses[0];

    std::thread submitter([&] {
        gate.wait();
        for (size_t i = 1; i < kWorkCount; ++i) {
            statuses[i] = executor->try_submit(
                    AICompletionWork([&, i] { run_counts[i].fetch_add(1, std::memory_order_relaxed); },
                                     [&, i] { cancel_counts[i].fetch_add(1, std::memory_order_relaxed); }));
        }
    });
    std::thread stopper([&] {
        gate.wait();
        executor->shutdown();
    });
    const bool both_threads_ready = gate.wait_for_waiters(2);
    gate.release();
    submitter.join();
    stopper.join();
    ASSERT_TRUE(both_threads_ready);

    for (size_t i = 0; i < kWorkCount; ++i) {
        const int completions =
                run_counts[i].load(std::memory_order_relaxed) + cancel_counts[i].load(std::memory_order_relaxed);
        if (statuses[i].ok()) {
            EXPECT_EQ(1, completions) << "accepted work " << i;
        } else {
            EXPECT_TRUE(statuses[i].is_shutdown()) << statuses[i];
            EXPECT_EQ(0, completions) << "rejected work " << i;
        }
    }
}

TEST_F(AIThreadPoolCompletionExecutorTest, ResizeUpAndDownDoesNotCancelAcceptedWork) {
    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 8);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();

    Event release_blockers;
    Event first_started;
    Event second_started;
    Event queued_done;
    std::atomic<int> run_count{0};
    std::atomic<int> cancel_count{0};
    auto blocker = [&](Event* started) {
        return AICompletionWork(
                [&, started] {
                    started->notify();
                    release_blockers.wait();
                    run_count.fetch_add(1, std::memory_order_relaxed);
                },
                [&] { cancel_count.fetch_add(1, std::memory_order_relaxed); });
    };
    SCOPED_CLEANUP({
        release_blockers.notify();
        executor->shutdown();
    });

    ASSERT_TRUE(executor->try_submit(blocker(&first_started)).ok());
    ASSERT_TRUE(first_started.wait_for());

    ASSERT_TRUE(executor->update_worker_threads(2).ok());
    ASSERT_TRUE(executor->try_submit(blocker(&second_started)).ok());
    ASSERT_TRUE(second_started.wait_for());
    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 run_count.fetch_add(1, std::memory_order_relaxed);
                                                 queued_done.notify();
                                             },
                                             [&] { cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());

    ASSERT_TRUE(executor->update_worker_threads(1).ok());
    release_blockers.notify();
    ASSERT_TRUE(queued_done.wait_for());
    executor->shutdown();

    EXPECT_EQ(3, run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, cancel_count.load(std::memory_order_relaxed));
}

TEST_F(AIThreadPoolCompletionExecutorTest, ResizeAndShutdownRaceHasAStableLifecycleResult) {
    auto executor_or = AIThreadPoolCompletionExecutor::create(1, 8);
    ASSERT_TRUE(executor_or.ok()) << executor_or.status();
    auto executor = std::move(executor_or).value();

    Event running_started;
    Event release_running;
    StartGate gate;
    std::atomic<int> run_count{0};
    std::atomic<int> cancel_count{0};
    Status resize_status;
    SCOPED_CLEANUP({
        release_running.notify();
        executor->shutdown();
    });
    ASSERT_TRUE(executor->try_submit(AICompletionWork(
                                             [&] {
                                                 running_started.notify();
                                                 release_running.wait();
                                                 run_count.fetch_add(1, std::memory_order_relaxed);
                                             },
                                             [&] { cancel_count.fetch_add(1, std::memory_order_relaxed); }))
                        .ok());
    ASSERT_TRUE(running_started.wait_for());

    std::thread updater([&] {
        gate.wait();
        resize_status = executor->update_worker_threads(2);
    });
    std::thread stopper([&] {
        gate.wait();
        executor->shutdown();
    });
    const bool both_threads_ready = gate.wait_for_waiters(2);
    gate.release();
    release_running.notify();
    updater.join();
    stopper.join();

    ASSERT_TRUE(both_threads_ready);
    EXPECT_TRUE(resize_status.ok() || resize_status.is_shutdown()) << resize_status;
    EXPECT_EQ(1, run_count.load(std::memory_order_relaxed));
    EXPECT_EQ(0, cancel_count.load(std::memory_order_relaxed));
}

TEST(SystemAIRandomTest, ConcurrentCallsStayWithinRequestedRange) {
    constexpr uint32_t kExclusiveUpper = 2501;
    constexpr size_t kThreadCount = 8;
    constexpr size_t kCallsPerThread = 10'000;
    SystemAIRandom random;
    StartGate gate;
    std::atomic<bool> all_in_range{true};
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&] {
            gate.wait();
            for (size_t call = 0; call < kCallsPerThread; ++call) {
                if (random.uniform(kExclusiveUpper) >= kExclusiveUpper) {
                    all_in_range.store(false, std::memory_order_release);
                }
            }
        });
    }
    const bool all_threads_ready = gate.wait_for_waiters(kThreadCount);
    gate.release();
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_TRUE(all_threads_ready);
    EXPECT_TRUE(all_in_range.load(std::memory_order_acquire));
}

} // namespace
} // namespace starrocks
