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

#include "compute_env/ai/ai_executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/scoped_cleanup.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_http_client.h"
#include "platform/llm/ai_runtime.h"

namespace starrocks {

class AIAdmissionControllerTestPeer {
public:
    static uint64_t attempt_count(const AIAdmissionController& controller) {
        return controller._attempt_count_for_test();
    }
};

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

AIRuntimeConfig small_config() {
    AIRuntimeConfig config;
    config.worker_thread_num = 1;
    config.max_inflight = 2;
    return config;
}

std::unique_ptr<AIExecutor> create_executor(AIRuntimeConfig config = small_config()) {
    auto executor_or = AIExecutor::create(std::move(config));
    EXPECT_TRUE(executor_or.ok()) << executor_or.status();
    if (!executor_or.ok()) return nullptr;
    return std::move(executor_or).value();
}

AIQueryLifecycleProbe live_query_until(int64_t deadline_ns) {
    return [deadline_ns] { return AIQueryLifecycleSnapshot{.cancelled = false, .monotonic_deadline_ns = deadline_ns}; };
}

TEST(AIExecutorTest, OwnsOneDistinctProcessRuntimeGraph) {
    auto first = create_executor();
    auto second = create_executor();
    ASSERT_NE(nullptr, first);
    ASSERT_NE(nullptr, second);

    EXPECT_NE(nullptr, first->clock());
    EXPECT_NE(nullptr, first->config_source());
    EXPECT_NE(nullptr, first->random());
    EXPECT_NE(nullptr, first->control_scheduler());
    EXPECT_NE(nullptr, first->completion_executor());
    EXPECT_NE(nullptr, first->http_client());
    EXPECT_NE(nullptr, first->admission_controller());

    EXPECT_NE(first->clock(), second->clock());
    EXPECT_NE(first->config_source(), second->config_source());
    EXPECT_NE(first->random(), second->random());
    EXPECT_NE(first->control_scheduler(), second->control_scheduler());
    EXPECT_NE(first->completion_executor(), second->completion_executor());
    EXPECT_NE(first->http_client(), second->http_client());
    EXPECT_NE(first->admission_controller(), second->admission_controller());
}

TEST(AIExecutorTest, UsesOneFixedStartupCapacityForCompletionAndAdmission) {
    auto defaults = create_executor(AIRuntimeConfig{});
    ASSERT_NE(nullptr, defaults);
    EXPECT_EQ(528, defaults->completion_capacity());

    auto fixed = create_executor();
    ASSERT_NE(nullptr, fixed);
    EXPECT_EQ(3, fixed->completion_capacity());
    EXPECT_EQ(std::numeric_limits<int>::max(),
              ai_completion_capacity(std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max()));

    AIRuntimeConfig overflowing = small_config();
    overflowing.max_inflight = std::numeric_limits<int32_t>::max();
    auto saturated = create_executor(std::move(overflowing));
    ASSERT_NE(nullptr, saturated);
    EXPECT_EQ(std::numeric_limits<int>::max(), saturated->completion_capacity());

    ASSERT_TRUE(fixed->update_max_inflight(100).ok());
    ASSERT_TRUE(fixed->update_worker_thread_num(2).ok());
    EXPECT_EQ(3, fixed->completion_capacity());
}

TEST(AIExecutorTest, RejectsInvalidInitialConfigBeforePublishingRuntime) {
    AIRuntimeConfig invalid = small_config();
    invalid.max_inflight = 0;

    auto executor_or = AIExecutor::create(std::move(invalid));

    EXPECT_TRUE(executor_or.status().is_invalid_argument()) << executor_or.status();
}

TEST(AIExecutorTest, ConcurrentTypedUpdatesPreserveBothFields) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);
    StartGate gate;
    Status request_status;
    Status retry_status;

    std::thread request_update([&] {
        gate.wait();
        request_status = executor->update_request_timeout_ms(1234);
    });
    std::thread retry_update([&] {
        gate.wait();
        retry_status = executor->update_max_retries(9);
    });
    const bool both_updates_ready = gate.wait_for_waiters(2);
    gate.release();
    request_update.join();
    retry_update.join();

    ASSERT_TRUE(both_updates_ready);
    ASSERT_TRUE(request_status.ok()) << request_status;
    ASSERT_TRUE(retry_status.ok()) << retry_status;
    AIRuntimeConfig snapshot = executor->config_snapshot();
    EXPECT_EQ(1234, snapshot.request_timeout_ms);
    EXPECT_EQ(9, snapshot.max_retries);
}

TEST(AIExecutorTest, InvalidTypedUpdateDoesNotChangeConfigOrWorkers) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);
    const AIRuntimeConfig before = executor->config_snapshot();

    Status status = executor->update_worker_thread_num(0);

    EXPECT_TRUE(status.is_invalid_argument()) << status;
    AIRuntimeConfig after = executor->config_snapshot();
    EXPECT_EQ(before.worker_thread_num, after.worker_thread_num);
    EXPECT_EQ(before.max_inflight, after.max_inflight);

    Event running;
    Event release;
    Event second_started;
    SCOPED_CLEANUP({
        release.notify();
        executor->shutdown();
    });
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork(
                                [&] {
                                    running.notify();
                                    release.wait();
                                },
                                [] {}))
                        .ok());
    ASSERT_TRUE(running.wait_for());
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork([&] { second_started.notify(); }, [] {}))
                        .ok());
    EXPECT_FALSE(second_started.wait_for(50ms));
    release.notify();
    EXPECT_TRUE(second_started.wait_for());
}

TEST(AIExecutorTest, SnapshotPreparationFailureDoesNotResizeWorkersOrPublishConfig) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);
    const AIRuntimeConfig before = executor->config_snapshot();
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->SetCallBack("AIRuntimeConfigSource::prepare:before_snapshot_allocation",
                            [](void*) { throw std::bad_alloc(); });
    sync_point->EnableProcessing();

    Status status = executor->update_worker_thread_num(2);

    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    ASSERT_TRUE(status.is_mem_limit_exceeded()) << status;
    EXPECT_EQ(before.worker_thread_num, executor->config_snapshot().worker_thread_num);

    Event first_started;
    Event release_first;
    Event second_started;
    SCOPED_CLEANUP({
        release_first.notify();
        executor->shutdown();
    });
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork(
                                [&] {
                                    first_started.notify();
                                    release_first.wait();
                                },
                                [] {}))
                        .ok());
    ASSERT_TRUE(first_started.wait_for());
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork([&] { second_started.notify(); }, [] {}))
                        .ok());
    EXPECT_FALSE(second_started.wait_for(50ms));
    release_first.notify();
    EXPECT_TRUE(second_started.wait_for());
}

TEST(AIExecutorTest, WorkerUpdateResizesLivePoolWithoutChangingFixedCapacity) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);
    Event first_started;
    Event release_first;
    Event second_started;
    SCOPED_CLEANUP({
        release_first.notify();
        executor->shutdown();
    });

    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork(
                                [&] {
                                    first_started.notify();
                                    release_first.wait();
                                },
                                [] {}))
                        .ok());
    ASSERT_TRUE(first_started.wait_for());
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork([&] { second_started.notify(); }, [] {}))
                        .ok());

    ASSERT_TRUE(executor->update_worker_thread_num(2).ok());
    ASSERT_TRUE(executor->completion_executor()->try_submit(AICompletionWork([] {}, [] {})).ok());
    EXPECT_TRUE(second_started.wait_for());
    EXPECT_EQ(2, executor->config_snapshot().worker_thread_num);
    EXPECT_EQ(3, executor->completion_capacity());
    release_first.notify();
}

TEST(AIExecutorTest, ShutdownIsIdempotentBarrierAndRejectsNewWorkAndUpdates) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);
    Event running;
    Event release;
    Event running_finished;
    Event queued_resolved;
    std::atomic<int> queued_runs = 0;
    std::atomic<int> queued_cancels = 0;
    std::thread stopper;
    SCOPED_CLEANUP({
        release.notify();
        if (stopper.joinable()) stopper.join();
        executor->shutdown();
    });

    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork(
                                [&] {
                                    running.notify();
                                    release.wait();
                                    running_finished.notify();
                                },
                                [] {}))
                        .ok());
    ASSERT_TRUE(running.wait_for());
    ASSERT_TRUE(executor->completion_executor()
                        ->try_submit(AICompletionWork(
                                [&] {
                                    ++queued_runs;
                                    queued_resolved.notify();
                                },
                                [&] {
                                    ++queued_cancels;
                                    queued_resolved.notify();
                                }))
                        .ok());

    stopper = std::thread([&] { executor->shutdown(); });
    Status stopping_status;
    const auto stopping_deadline = std::chrono::steady_clock::now() + 5s;
    do {
        stopping_status = executor->update_sub_chunk_size(32);
        if (!stopping_status.ok()) break;
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < stopping_deadline);
    release.notify();
    stopper.join();
    executor->shutdown();

    EXPECT_TRUE(stopping_status.is_shutdown()) << stopping_status;
    EXPECT_TRUE(running_finished.wait_for());
    EXPECT_TRUE(queued_resolved.wait_for());
    EXPECT_EQ(1, queued_runs.load() + queued_cancels.load());
    EXPECT_TRUE(executor->update_sub_chunk_size(16).is_shutdown());
    EXPECT_TRUE(executor->completion_executor()->try_submit(AICompletionWork([] {}, [] {})).is_shutdown());

    AIAdmissionRequest admission;
    admission.workgroup_key = UniqueId{0, 1};
    admission.query_id = UniqueId(2, 3);
    admission.attempt_id = 4;
    admission.rate_limit_key = AIRateLimitKey::create("provider", "credential", AICapability::CHAT);
    admission.request_deadline_ns = MonotonicNanos() + 1'000'000'000;
    admission.lifecycle = live_query_until(admission.request_deadline_ns);
    auto ticket = executor->admission_controller()->enqueue(admission, [](AIAdmissionResult) {});
    EXPECT_FALSE(ticket.ok());

    AIHttpRequest request;
    request.url = "http://127.0.0.1/";
    request.request_deadline_ns = MonotonicNanos() + 1'000'000'000;
    request.lifecycle = live_query_until(request.request_deadline_ns);
    request.connect_timeout_ms = 100;
    request.max_response_bytes = 1024;
    EXPECT_TRUE(executor->http_client()->submit(std::move(request), [](AIHttpResult) {}).is_shutdown());
}

TEST(AIExecutorTest, ShutdownAndUpdateHaveOneLinearizationOrder) {
    for (int iteration = 0; iteration < 20; ++iteration) {
        auto executor = create_executor();
        ASSERT_NE(nullptr, executor);
        StartGate gate;
        Status update_status;

        std::thread updater([&] {
            gate.wait();
            update_status = executor->update_rate_limit_qps_chat(321);
        });
        std::thread stopper([&] {
            gate.wait();
            executor->shutdown();
        });
        const bool both_operations_ready = gate.wait_for_waiters(2);
        gate.release();
        updater.join();
        stopper.join();

        ASSERT_TRUE(both_operations_ready);
        EXPECT_TRUE(update_status.ok() || update_status.is_shutdown()) << update_status;
        EXPECT_EQ(update_status.ok() ? 321 : 128, executor->config_snapshot().rate_limit_qps_chat);
        EXPECT_TRUE(executor->update_rate_limit_qps_chat(111).is_shutdown());
    }
}

TEST(AIExecutorTest, MaxInflightUpdateNotifiesAWaitingAdmission) {
    AIRuntimeConfig config = small_config();
    config.max_inflight = 1;
    auto executor = create_executor(std::move(config));
    ASSERT_NE(nullptr, executor);
    Event first_admitted;
    Event second_admitted;
    std::optional<AIAdmissionGrant> first_grant;
    std::optional<AIAdmissionGrant> second_grant;
    const AIRateLimitKey key = AIRateLimitKey::create("provider", "credential", AICapability::CHAT);
    SCOPED_CLEANUP({
        executor->shutdown();
        second_grant.reset();
        first_grant.reset();
    });

    AIAdmissionRequest first_request;
    first_request.workgroup_key = UniqueId{0, 1};
    first_request.query_id = UniqueId(1, 1);
    first_request.attempt_id = 1;
    first_request.rate_limit_key = key;
    first_request.request_deadline_ns = MonotonicNanos() + 5'000'000'000;
    first_request.lifecycle = live_query_until(first_request.request_deadline_ns);
    auto first_ticket = executor->admission_controller()->enqueue(first_request, [&](AIAdmissionResult result) {
        if (std::holds_alternative<AIAdmissionGrant>(result)) {
            first_grant.emplace(std::get<AIAdmissionGrant>(std::move(result)));
        }
        first_admitted.notify();
    });
    ASSERT_TRUE(first_ticket.ok()) << first_ticket.status();
    ASSERT_TRUE(first_admitted.wait_for());
    ASSERT_TRUE(first_grant.has_value());

    AIAdmissionRequest second_request = first_request;
    second_request.workgroup_key = UniqueId{0, 2};
    second_request.query_id = UniqueId(2, 2);
    second_request.attempt_id = 2;
    auto second_ticket = executor->admission_controller()->enqueue(second_request, [&](AIAdmissionResult result) {
        if (std::holds_alternative<AIAdmissionGrant>(result)) {
            second_grant.emplace(std::get<AIAdmissionGrant>(std::move(result)));
        }
        second_admitted.notify();
    });
    ASSERT_TRUE(second_ticket.ok()) << second_ticket.status();
    EXPECT_FALSE(second_admitted.wait_for(50ms));

    ASSERT_TRUE(executor->update_max_inflight(2).ok());
    EXPECT_TRUE(second_admitted.wait_for());
    EXPECT_TRUE(second_grant.has_value());

    second_grant.reset();
    first_grant.reset();
}

TEST(AIExecutorTest, LimitFailureCallbackCanReenterUpdateWithoutLifecycleMutexDeadlock) {
    AIRuntimeConfig config = small_config();
    config.max_inflight = 1;
    auto executor = create_executor(std::move(config));
    ASSERT_NE(nullptr, executor);
    Event first_admitted;
    Event second_pump_entered;
    Event reentrant_update_finished;
    std::optional<AIAdmissionGrant> first_grant;
    std::optional<AIAdmissionFailureReason> second_failure;
    std::thread reentrant_update_thread;
    Status reentrant_update_status;
    bool reentrant_update_finished_inside_callback = false;
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
        if (reentrant_update_thread.joinable()) reentrant_update_thread.join();
        first_grant.reset();
        executor->shutdown();
    });

    const AIRateLimitKey key = AIRateLimitKey::create("provider", "credential", AICapability::CHAT);
    AIAdmissionRequest first_request;
    first_request.workgroup_key = UniqueId{0, 1};
    first_request.query_id = UniqueId{1, 1};
    first_request.attempt_id = 1;
    first_request.rate_limit_key = key;
    first_request.request_deadline_ns = MonotonicNanos() + 5'000'000'000;
    first_request.lifecycle = live_query_until(first_request.request_deadline_ns);
    auto first_ticket = executor->admission_controller()->enqueue(first_request, [&](AIAdmissionResult result) {
        if (std::holds_alternative<AIAdmissionGrant>(result)) {
            first_grant.emplace(std::get<AIAdmissionGrant>(std::move(result)));
        }
        first_admitted.notify();
    });
    ASSERT_TRUE(first_ticket.ok()) << first_ticket.status();
    ASSERT_TRUE(first_admitted.wait_for());
    ASSERT_TRUE(first_grant.has_value());

    sync_point->SetCallBack("AIAdmissionController::_pump:before_body", [&](void*) { second_pump_entered.notify(); });
    AIAdmissionRequest second_request = first_request;
    second_request.workgroup_key = UniqueId{0, 2};
    second_request.query_id = UniqueId{2, 2};
    second_request.attempt_id = 2;
    auto second_ticket = executor->admission_controller()->enqueue(second_request, [&](AIAdmissionResult result) {
        auto* failure = std::get_if<AIAdmissionFailure>(&result);
        if (failure != nullptr) second_failure = failure->reason;
        reentrant_update_thread = std::thread([&] {
            reentrant_update_status = executor->update_rate_limit_qps_chat(321);
            reentrant_update_finished.notify();
        });
        reentrant_update_finished_inside_callback = reentrant_update_finished.wait_for(5s);
    });
    ASSERT_TRUE(second_ticket.ok()) << second_ticket.status();
    ASSERT_TRUE(second_pump_entered.wait_for());
    Event second_pump_finished;
    ASSERT_TRUE(executor->control_scheduler()->post([&] { second_pump_finished.notify(); }).ok());
    ASSERT_TRUE(second_pump_finished.wait_for());
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::attempt_count(*executor->admission_controller()))
            << "the saturated admission pump must finish before injecting the limit-update post failure";
    sync_point->ClearCallBack("AIAdmissionController::_pump:before_body");

    std::atomic<bool> fail_next_control_post = true;
    sync_point->SetCallBack("AIControlThreadScheduler::post:before_ready_emplace", [&](void*) {
        if (fail_next_control_post.exchange(false)) throw std::bad_alloc();
    });

    Status update_status = executor->update_max_inflight(2);
    if (reentrant_update_thread.joinable()) reentrant_update_thread.join();

    ASSERT_TRUE(update_status.ok()) << update_status;
    EXPECT_FALSE(fail_next_control_post.load()) << "the limit update must exercise the injected control-post failure";
    EXPECT_TRUE(reentrant_update_finished_inside_callback)
            << "admission failure callbacks must run after the executor releases its lifecycle mutex";
    ASSERT_TRUE(second_failure.has_value());
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, *second_failure);
    EXPECT_TRUE(reentrant_update_status.ok()) << reentrant_update_status;
    EXPECT_EQ(2, executor->config_snapshot().max_inflight);
    EXPECT_EQ(321, executor->config_snapshot().rate_limit_qps_chat);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(*executor->admission_controller()));
}

TEST(AIExecutorTest, RepeatedCreateDestroyDoesNotLeaveRuntimeThreads) {
    for (int iteration = 0; iteration < 20; ++iteration) {
        auto executor = create_executor();
        ASSERT_NE(nullptr, executor);
        ASSERT_TRUE(executor->update_connect_timeout_ms(iteration).ok());
    }
}

TEST(AIExecutorTest, AllFragmentSnapshotFieldsHaveTypedMutationPaths) {
    auto executor = create_executor();
    ASSERT_NE(nullptr, executor);

    ASSERT_TRUE(executor->update_request_timeout_ms(1).ok());
    ASSERT_TRUE(executor->update_connect_timeout_ms(2).ok());
    ASSERT_TRUE(executor->update_max_response_bytes(3).ok());
    ASSERT_TRUE(executor->update_sub_chunk_size(4).ok());
    ASSERT_TRUE(executor->update_max_retries(5).ok());
    ASSERT_TRUE(executor->update_max_retries_on_throttle(6).ok());
    ASSERT_TRUE(executor->update_on_error("fail").ok());
    ASSERT_TRUE(executor->update_rate_limit_qps_chat(7).ok());
    ASSERT_TRUE(executor->update_max_inflight(8).ok());

    AIRuntimeConfig snapshot = executor->config_snapshot();
    EXPECT_EQ(1, snapshot.request_timeout_ms);
    EXPECT_EQ(2, snapshot.connect_timeout_ms);
    EXPECT_EQ(3, snapshot.max_response_bytes);
    EXPECT_EQ(4, snapshot.sub_chunk_size);
    EXPECT_EQ(5, snapshot.max_retries);
    EXPECT_EQ(6, snapshot.max_retries_on_throttle);
    EXPECT_EQ("fail", snapshot.on_error);
    EXPECT_EQ(7, snapshot.rate_limit_qps_chat);
    EXPECT_EQ(8, snapshot.max_inflight);
}

} // namespace
} // namespace starrocks
