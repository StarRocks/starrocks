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

#include "platform/llm/ai_admission_controller.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/scoped_cleanup.h"
#include "platform/llm/ai_memory.h"

namespace starrocks {

class AIAdmissionControllerTestPeer {
public:
    static int64_t inflight(const AIAdmissionController& controller) { return controller._inflight_for_test(); }
    static int64_t completion_in_use(const AIAdmissionController& controller) {
        return controller._completion_in_use_for_test();
    }
    static uint64_t attempt_count(const AIAdmissionController& controller) {
        return controller._attempt_count_for_test();
    }
    static uint64_t bucket_state_count(const AIAdmissionController& controller) {
        return controller._bucket_state_count_for_test();
    }
    static int64_t bucket_registrations(const AIAdmissionController& controller, const AIRateLimitKey& key) {
        return controller._bucket_registrations_for_test(key);
    }
    static int64_t bucket_inflight(const AIAdmissionController& controller, const AIRateLimitKey& key) {
        return controller._bucket_inflight_for_test(key);
    }
    static int64_t bucket_owners(const AIAdmissionController& controller, const AIRateLimitKey& key) {
        return controller._bucket_owners_for_test(key);
    }
    static int64_t unresolved_completion_count(const AIAdmissionController& controller, const AIRateLimitKey& key) {
        return controller._unresolved_completion_count_for_test(key);
    }
    static int64_t rate_pins(const AIAdmissionController& controller, const AIRateLimitKey& key) {
        return controller._rate_pins_for_test(key);
    }
    static uint64_t scheduling_steps(const AIAdmissionController& controller) {
        return controller._scheduling_steps_for_test();
    }
    static uint64_t rate_waiter_count(const AIAdmissionController& controller) {
        return controller._rate_waiter_count_for_test();
    }
};

namespace {

constexpr int64_t kSecond = 1'000'000'000;
constexpr int64_t kHour = 3600 * kSecond;

struct FakeAdmissionMemoryContextState;
thread_local FakeAdmissionMemoryContextState* tls_admission_physical_scope = nullptr;

struct FakeAdmissionMemoryContextState {
    static void retain(void* opaque) noexcept {
        static_cast<FakeAdmissionMemoryContextState*>(opaque)->references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release_owner(void* opaque) noexcept {
        static_cast<FakeAdmissionMemoryContextState*>(opaque)->references.fetch_sub(1, std::memory_order_relaxed);
    }

    static bool reserve_bytes(void*, size_t) noexcept { return true; }
    static void release_bytes(void*, size_t) noexcept {}

    static void run(void* opaque, AIMemoryContext::Action action, void* action_context) {
        auto* state = static_cast<FakeAdmissionMemoryContextState*>(opaque);
        struct RestoreScope {
            explicit RestoreScope(FakeAdmissionMemoryContextState* next)
                    : state(next), previous(tls_admission_physical_scope) {
                tls_admission_physical_scope = next;
            }
            ~RestoreScope() {
                state->exits.fetch_add(1, std::memory_order_relaxed);
                tls_admission_physical_scope = previous;
            }
            FakeAdmissionMemoryContextState* state;
            FakeAdmissionMemoryContextState* previous;
        } restore(state);
        state->entries.fetch_add(1, std::memory_order_relaxed);
        action(action_context);
    }

    AIMemoryContext context() {
        return AIMemoryContext::create(this, &FakeAdmissionMemoryContextState::reserve_bytes,
                                       &FakeAdmissionMemoryContextState::release_bytes,
                                       &FakeAdmissionMemoryContextState::run, &FakeAdmissionMemoryContextState::retain,
                                       &FakeAdmissionMemoryContextState::release_owner);
    }

    std::atomic<int> references{0};
    std::atomic<int> entries{0};
    std::atomic<int> exits{0};
};

struct AdmissionCallbackOwner {
    AdmissionCallbackOwner(FakeAdmissionMemoryContextState* expected_scope, std::atomic<int>* destroyed_in_scope)
            : expected_scope(expected_scope), destroyed_in_scope(destroyed_in_scope) {}

    FakeAdmissionMemoryContextState* expected_scope;
    std::atomic<int>* destroyed_in_scope;

    ~AdmissionCallbackOwner() {
        if (tls_admission_physical_scope == expected_scope) {
            destroyed_in_scope->fetch_add(1, std::memory_order_relaxed);
        }
    }
};

class ManualAIClock final : public AIClock {
public:
    int64_t monotonic_now_ns() const noexcept override {
        ++_access_count;
        return _monotonic_ns;
    }
    int64_t unix_now_seconds() const noexcept override {
        ++_access_count;
        return _unix_seconds;
    }

    void advance_ns(int64_t delta_ns) {
        _monotonic_ns += delta_ns;
        _unix_seconds += delta_ns / kSecond;
    }
    uint64_t access_count() const { return _access_count; }

private:
    int64_t _monotonic_ns = kSecond;
    int64_t _unix_seconds = 1'700'000'000;
    mutable uint64_t _access_count = 0;
};

class ManualAIControlScheduler final : public AIControlScheduler {
public:
    explicit ManualAIControlScheduler(const AIClock* clock) : _clock(clock) {}

    Status post(Task task) override {
        if (!_post_statuses.empty()) {
            Status status = std::move(_post_statuses.front());
            _post_statuses.pop_front();
            if (!status.ok()) {
                return status;
            }
        }
        std::lock_guard lock(_ready_mutex);
        _ready.emplace_back(std::move(task));
        return Status::OK();
    }

    StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) override {
        if (!_timer_statuses.empty()) {
            Status status = std::move(_timer_statuses.front());
            _timer_statuses.pop_front();
            if (!status.ok()) {
                return status;
            }
        }
        const TaskId id = ++_next_id;
        _timers.push_back(Timer{id, monotonic_time_ns, std::move(task), false});
        return id;
    }

    void cancel(TaskId id) override {
        for (auto& timer : _timers) {
            if (timer.id == id) {
                timer.cancelled = true;
            }
        }
    }

    void shutdown_and_drain() override {
        _timers.clear();
        run_until_idle();
    }

    void run_until_idle() {
        for (;;) {
            std::stable_sort(_timers.begin(), _timers.end(), [](const Timer& lhs, const Timer& rhs) {
                return std::pair(lhs.when_ns, lhs.id) < std::pair(rhs.when_ns, rhs.id);
            });
            while (!_timers.empty() && _timers.front().when_ns <= _clock->monotonic_now_ns()) {
                Timer timer = std::move(_timers.front());
                _timers.erase(_timers.begin());
                if (!timer.cancelled) {
                    std::lock_guard lock(_ready_mutex);
                    _ready.emplace_back(std::move(timer.task));
                }
            }
            Task task;
            {
                std::lock_guard lock(_ready_mutex);
                if (_ready.empty()) {
                    return;
                }
                task = std::move(_ready.front());
                _ready.pop_front();
            }
            try {
                task();
            } catch (...) {
            }
        }
    }

    size_t ready_count() const {
        std::lock_guard lock(_ready_mutex);
        return _ready.size();
    }

    size_t active_timer_count() const {
        return std::count_if(_timers.begin(), _timers.end(), [](const Timer& timer) { return !timer.cancelled; });
    }

    std::optional<int64_t> next_active_timer_time_ns() const {
        std::optional<int64_t> result;
        for (const auto& timer : _timers) {
            if (!timer.cancelled && (!result.has_value() || timer.when_ns < *result)) {
                result = timer.when_ns;
            }
        }
        return result;
    }

    bool force_fire_first_cancelled_timer() {
        auto it = std::find_if(_timers.begin(), _timers.end(), [](const Timer& timer) { return timer.cancelled; });
        if (it == _timers.end()) {
            return false;
        }
        std::lock_guard lock(_ready_mutex);
        _ready.emplace_back(it->task);
        return true;
    }

    void fail_next_post(Status status) { _post_statuses.emplace_back(std::move(status)); }
    void fail_next_timer(Status status) { _timer_statuses.emplace_back(std::move(status)); }

private:
    struct Timer {
        TaskId id;
        int64_t when_ns;
        Task task;
        bool cancelled;
    };

    const AIClock* _clock;
    TaskId _next_id = 0;
    mutable std::mutex _ready_mutex;
    std::deque<Task> _ready;
    std::vector<Timer> _timers;
    std::deque<Status> _post_statuses;
    std::deque<Status> _timer_statuses;
};

class MutableAIAdmissionLimitSource final : public AIAdmissionLimitSource {
public:
    int64_t qps(AICapability capability) const noexcept override {
        ++access_count;
        EXPECT_EQ(AICapability::CHAT, capability);
        return chat_qps;
    }
    int64_t max_inflight() const override {
        ++access_count;
        return inflight_cap;
    }

    int64_t chat_qps = 100;
    int64_t inflight_cap = 32;
    mutable uint64_t access_count = 0;
};

struct AdmissionRecord {
    AIWorkGroupKey workgroup_key;
    UniqueId query_id;
    uint64_t attempt_id;

    AdmissionRecord(AIWorkGroupKey workgroup_key, UniqueId query_id, uint64_t attempt_id)
            : workgroup_key(workgroup_key), query_id(query_id), attempt_id(attempt_id) {}
    AdmissionRecord(int64_t workgroup_id, UniqueId query_id, uint64_t attempt_id)
            : AdmissionRecord(UniqueId{0, workgroup_id}, query_id, attempt_id) {}

    bool operator==(const AdmissionRecord&) const = default;
};

class AIAdmissionControllerTest : public ::testing::Test {
protected:
    explicit AIAdmissionControllerTest(int64_t completion_capacity = 64)
            : _scheduler(&_clock), _controller(&_clock, &_scheduler, &_limits, completion_capacity) {}

    void TearDown() override {
        _controller.shutdown();
        _scheduler.run_until_idle();
        _grants.clear();
        _scheduler.run_until_idle();
    }

    AIAdmissionRequest request(AIWorkGroupKey workgroup_key, UniqueId query_id, uint64_t attempt_id,
                               std::string endpoint = "https://model.invalid/v1/chat", std::string credential = "key-a",
                               int64_t eligible_at_ns = 0, int64_t deadline_ns = 0) {
        AIAdmissionRequest result;
        result.workgroup_key = workgroup_key;
        result.query_id = query_id;
        result.attempt_id = attempt_id;
        result.rate_limit_key = AIRateLimitKey::create(std::move(endpoint), credential, AICapability::CHAT);
        result.eligible_at_ns = eligible_at_ns == 0 ? _clock.monotonic_now_ns() : eligible_at_ns;
        result.request_deadline_ns = deadline_ns == 0 ? _clock.monotonic_now_ns() + kHour : deadline_ns;
        const int64_t query_deadline_ns = result.request_deadline_ns;
        result.lifecycle = [query_deadline_ns] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
        };
        return result;
    }

    AIAdmissionRequest request(int64_t workgroup_id, int64_t query_id_lo, uint64_t attempt_id,
                               std::string endpoint = "https://model.invalid/v1/chat", std::string credential = "key-a",
                               int64_t eligible_at_ns = 0, int64_t deadline_ns = 0) {
        return request(UniqueId{0, workgroup_id}, UniqueId{0, query_id_lo}, attempt_id, std::move(endpoint),
                       std::move(credential), eligible_at_ns, deadline_ns);
    }

    AIAdmissionRequest request(int64_t workgroup_id, UniqueId query_id, uint64_t attempt_id,
                               std::string endpoint = "https://model.invalid/v1/chat", std::string credential = "key-a",
                               int64_t eligible_at_ns = 0, int64_t deadline_ns = 0) {
        return request(UniqueId{0, workgroup_id}, query_id, attempt_id, std::move(endpoint), std::move(credential),
                       eligible_at_ns, deadline_ns);
    }

    AIAdmissionCallback capture(std::vector<AdmissionRecord>* trace,
                                std::vector<AIAdmissionFailure>* failures = nullptr,
                                std::function<void(const AdmissionRecord&)> after_grant = {}) {
        return [this, trace, failures, after_grant = std::move(after_grant)](AIAdmissionResult result) mutable {
            if (auto* failure = std::get_if<AIAdmissionFailure>(&result); failure != nullptr) {
                if (failures != nullptr) {
                    failures->emplace_back(*failure);
                }
                return;
            }
            auto grant = std::get<AIAdmissionGrant>(std::move(result));
            const AdmissionRecord record{grant.workgroup_key(), grant.query_id(), grant.attempt_id()};
            grant.commit_network_attempt();
            trace->emplace_back(record);
            _grants.emplace_back(std::move(grant));
            if (after_grant) {
                after_grant(record);
            }
        };
    }

    AIAdmissionCallback capture_tentative(std::vector<AdmissionRecord>* trace) {
        return [this, trace](AIAdmissionResult result) {
            ASSERT_TRUE(std::holds_alternative<AIAdmissionGrant>(result));
            AIAdmissionGrant grant = std::get<AIAdmissionGrant>(std::move(result));
            trace->emplace_back(grant.workgroup_key(), grant.query_id(), grant.attempt_id());
            _grants.emplace_back(std::move(grant));
        };
    }

    size_t enqueue(const AIAdmissionRequest& admission, AIAdmissionCallback callback) {
        auto ticket = _controller.enqueue(admission, std::move(callback));
        if (!ticket.ok()) {
            ADD_FAILURE() << ticket.status();
            return std::numeric_limits<size_t>::max();
        }
        _tickets.emplace_back(std::move(ticket).value());
        return _tickets.size() - 1;
    }

    void complete_without_cooldown(size_t grant_index) {
        AIBucketResolutionGuard resolution = _grants[grant_index].complete_transport();
        resolution.resolve_without_cooldown();
    }

    ManualAIClock _clock;
    ManualAIControlScheduler _scheduler;
    MutableAIAdmissionLimitSource _limits;
    AIAdmissionController _controller;
    std::vector<AIAdmissionTicket> _tickets;
    std::vector<AIAdmissionGrant> _grants;
};

class AIAdmissionControllerSingleCompletionTest : public AIAdmissionControllerTest {
protected:
    AIAdmissionControllerSingleCompletionTest() : AIAdmissionControllerTest(1) {}
};

struct RegistrationFailurePoint {
    const char* name;
    const char* sync_point;
    bool throw_non_bad_alloc = false;
};

class AIAdmissionRegistrationFailureTest : public AIAdmissionControllerTest,
                                           public ::testing::WithParamInterface<RegistrationFailurePoint> {};

struct GrantPreparationFailurePoint {
    const char* name;
    const char* sync_point;
};

class AIAdmissionGrantPreparationFailureTest : public AIAdmissionControllerTest,
                                               public ::testing::WithParamInterface<GrantPreparationFailurePoint> {};

UniqueId query_id(int64_t lo, int64_t hi = 0) {
    return UniqueId{hi, lo};
}

TEST_P(AIAdmissionRegistrationFailureTest, RollsBackOnlyTheCurrentRegistrationAndRemainsReusable) {
    _limits.inflight_cap = 0;
    auto trace = std::make_shared<std::vector<AdmissionRecord>>();
    auto accepted_failures = std::make_shared<std::vector<AIAdmissionFailure>>();
    auto capture_owned = [this, trace, accepted_failures](AIAdmissionResult result) {
        if (auto* failure = std::get_if<AIAdmissionFailure>(&result); failure != nullptr) {
            accepted_failures->emplace_back(*failure);
            return;
        }
        AIAdmissionGrant grant = std::get<AIAdmissionGrant>(std::move(result));
        grant.commit_network_attempt();
        trace->emplace_back(grant.workgroup_key(), grant.query_id(), grant.attempt_id());
        _grants.emplace_back(std::move(grant));
    };
    enqueue(request(10, 101, 1), capture_owned);
    _scheduler.run_until_idle();
    ASSERT_EQ(1, _scheduler.active_timer_count());

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    const bool throw_non_bad_alloc = GetParam().throw_non_bad_alloc;
    sync_point->SetCallBack(GetParam().sync_point, [&throw_once, throw_non_bad_alloc](void*) {
        if (std::exchange(throw_once, false)) {
            if (throw_non_bad_alloc) {
                throw std::runtime_error("callback-copy-sentinel");
            }
            throw std::bad_alloc();
        }
    });

    auto rejected_callback_count = std::make_shared<size_t>(0);
    FakeAdmissionMemoryContextState rejected_memory;
    std::atomic<int> rejected_callback_owner_destroyed_in_scope{0};
    auto rejected_callback_owner =
            std::make_shared<AdmissionCallbackOwner>(&rejected_memory, &rejected_callback_owner_destroyed_in_scope);
    std::weak_ptr<AdmissionCallbackOwner> rejected_callback_owner_weak = rejected_callback_owner;
    AIAdmissionRequest rejected_request = request(20, 201, 2, "https://rejected.invalid/v1/chat", "rejected-key");
    rejected_request.memory = rejected_memory.context();
    std::optional<StatusOr<AIAdmissionTicket>> rejected;
    EXPECT_NO_THROW(rejected.emplace(_controller.enqueue(
            rejected_request, [rejected_callback_count, owner = std::move(rejected_callback_owner)](AIAdmissionResult) {
                ++*rejected_callback_count;
            })));
    sync_point->ClearCallBack(GetParam().sync_point);
    if (!rejected.has_value()) {
        return;
    }
    rejected_request.memory = {};

    ASSERT_FALSE(rejected->ok());
    EXPECT_TRUE(rejected->status().is_mem_limit_exceeded()) << rejected->status();
    EXPECT_EQ(0, *rejected_callback_count);
    EXPECT_TRUE(rejected_callback_owner_weak.expired());
    EXPECT_EQ(1, rejected_callback_owner_destroyed_in_scope.load(std::memory_order_relaxed))
            << "enqueue must consume and destroy the transferred callback in its request physical scope";
    EXPECT_EQ(0, rejected_memory.references.load(std::memory_order_relaxed));
    EXPECT_EQ(rejected_memory.entries.load(std::memory_order_relaxed),
              rejected_memory.exits.load(std::memory_order_relaxed));
    EXPECT_TRUE(accepted_failures->empty());
    EXPECT_EQ(1, _scheduler.active_timer_count())
            << "a failed current registration must not drain or cancel existing pending work";

    const size_t first_new_grant = _grants.size();
    _limits.inflight_cap = 2;
    enqueue(request(20, 201, 3), capture_owned);
    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}, {20, query_id(201), 3}}), *trace);
    EXPECT_EQ(0, *rejected_callback_count);
    EXPECT_TRUE(accepted_failures->empty());
    ASSERT_EQ(first_new_grant + 2, _grants.size());
    EXPECT_EQ(2, AIAdmissionControllerTestPeer::inflight(_controller));

    complete_without_cooldown(first_new_grant);
    complete_without_cooldown(first_new_grant + 1);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

INSTANTIATE_TEST_SUITE_P(
        PartialRegistration, AIAdmissionRegistrationFailureTest,
        ::testing::Values(
                RegistrationFailurePoint{"AttemptCreate", "AIAdmissionController::enqueue:before_attempt_create"},
                RegistrationFailurePoint{"RequestCopy", "AIAdmissionController::enqueue:before_request_copy"},
                RegistrationFailurePoint{"CallbackTransferException",
                                         "AIAdmissionController::enqueue:before_callback_transfer", true},
                RegistrationFailurePoint{"BucketStateCreate",
                                         "AIAdmissionController::enqueue:before_bucket_state_create"},
                RegistrationFailurePoint{"BucketStateIndex",
                                         "AIAdmissionController::enqueue:before_bucket_state_emplace"},
                RegistrationFailurePoint{"AttemptIndex", "AIAdmissionController::enqueue:before_attempt_index_emplace"},
                RegistrationFailurePoint{"QueryIndexWorkgroup",
                                         "AIAdmissionController::enqueue:before_query_index_workgroup_emplace"},
                RegistrationFailurePoint{"QueryIndexQuery",
                                         "AIAdmissionController::enqueue:before_query_index_query_emplace"},
                RegistrationFailurePoint{"QueryIndexRegistration",
                                         "AIAdmissionController::enqueue:before_query_index_registration_emplace"},
                RegistrationFailurePoint{"DeadlineHeap", "AIAdmissionController::enqueue:before_deadline_emplace"},
                RegistrationFailurePoint{"WorkgroupQueue",
                                         "AIAdmissionController::_enqueue_attempt_locked:before_workgroup_emplace"},
                RegistrationFailurePoint{"QueryQueue",
                                         "AIAdmissionController::_enqueue_attempt_locked:before_query_emplace"},
                RegistrationFailurePoint{"AttemptQueue",
                                         "AIAdmissionController::_enqueue_attempt_locked:before_attempt_emplace"},
                RegistrationFailurePoint{"QueryRing",
                                         "AIAdmissionController::_add_query_to_ring_locked:before_query_ring_emplace"},
                RegistrationFailurePoint{
                        "WorkgroupRing",
                        "AIAdmissionController::_add_workgroup_to_ring_locked:before_workgroup_ring_emplace"}),
        [](const ::testing::TestParamInfo<RegistrationFailurePoint>& info) { return info.param.name; });

TEST_F(AIAdmissionControllerTest, UnexpectedPumpExceptionFailsClosedExactlyOnceAndRemainsReusable) {
    _limits.inflight_cap = 0;
    auto reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    auto callback = [reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
    };
    enqueue(request(10, 101, 1), callback);
    enqueue(request(20, 201, 2), callback);

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack("AIAdmissionController::_pump:before_body", [&throw_once](void*) {
        if (std::exchange(throw_once, false)) {
            throw std::runtime_error("injected unexpected pump failure");
        }
    });

    _scheduler.run_until_idle();

    ASSERT_EQ(2, reasons->size());
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, (*reasons)[0]);
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, (*reasons)[1]);
    sync_point->ClearCallBack("AIAdmissionController::_pump:before_body");

    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(30, 301, 3), capture(&trace));
    _scheduler.run_until_idle();
    EXPECT_EQ((std::vector<AdmissionRecord>{{30, query_id(301), 3}}), trace);
    ASSERT_EQ(1, _grants.size());
    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_P(AIAdmissionGrantPreparationFailureTest, FailsClosedWithoutGhostCapacityAndRemainsReusable) {
    _limits.inflight_cap = 1;
    auto failed_reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    enqueue(request(10, 101, 1), [failed_reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        failed_reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
    });

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack(GetParam().sync_point, [&throw_once](void*) {
        if (std::exchange(throw_once, false)) {
            throw std::bad_alloc();
        }
    });

    _scheduler.run_until_idle();
    sync_point->ClearCallBack(GetParam().sync_point);

    EXPECT_EQ((std::vector<AIAdmissionFailureReason>{AIAdmissionFailureReason::LOCAL_RESOURCE}), *failed_reasons);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    if (failed_reasons->size() != 1 || AIAdmissionControllerTestPeer::inflight(_controller) != 0) {
        return;
    }

    _limits.inflight_cap = 2;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 102, 2), capture(&trace));
    enqueue(request(20, 201, 3), capture(&trace));
    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(102), 2}, {20, query_id(201), 3}}), trace)
            << "a failed grant preparation must not leave active capacity or scheduler history";
    ASSERT_EQ(2, _grants.size());
    complete_without_cooldown(0);
    complete_without_cooldown(1);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

INSTANTIATE_TEST_SUITE_P(TerminalMutation, AIAdmissionGrantPreparationFailureTest,
                         ::testing::Values(GrantPreparationFailurePoint{
                                 "ActiveWorkgroupGrant",
                                 "AIAdmissionController::_try_admit_one_locked:before_active_workgroup_grant_emplace"}),
                         [](const ::testing::TestParamInfo<GrantPreparationFailurePoint>& info) {
                             return info.param.name;
                         });

TEST_F(AIAdmissionControllerTest, InitialPumpTaskMaterializationFailureReturnsStatusAndRollsBackRegistration) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack("AIAdmissionController::_request_pump_locked:before_task_materialization",
                            [&throw_once](void*) {
                                if (std::exchange(throw_once, false)) {
                                    throw std::bad_alloc();
                                }
                            });

    auto callback_count = std::make_shared<size_t>(0);
    std::optional<StatusOr<AIAdmissionTicket>> rejected;
    EXPECT_NO_THROW(rejected.emplace(
            _controller.enqueue(request(10, 101, 1), [callback_count](AIAdmissionResult) { ++*callback_count; })));
    sync_point->ClearCallBack("AIAdmissionController::_request_pump_locked:before_task_materialization");
    if (!rejected.has_value()) {
        return;
    }

    ASSERT_FALSE(rejected->ok());
    EXPECT_TRUE(rejected->status().is_mem_limit_exceeded()) << rejected->status();
    EXPECT_EQ("failed to materialize AI admission pump task", rejected->status().message());
    EXPECT_EQ(0, *callback_count);

    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 2), capture(&trace));
    _scheduler.run_until_idle();
    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 2}}), trace);
}

TEST_F(AIAdmissionControllerTest, TimerTaskMaterializationFailureFailsClosedAcceptedPendingWork) {
    _limits.inflight_cap = 0;
    auto reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    enqueue(request(10, 101, 1), [reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
    });

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack("AIAdmissionController::_arm_timer_locked:before_task_materialization",
                            [&throw_once](void*) {
                                if (std::exchange(throw_once, false)) {
                                    throw std::bad_alloc();
                                }
                            });

    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AIAdmissionFailureReason>{AIAdmissionFailureReason::LOCAL_RESOURCE}), *reasons);
    EXPECT_EQ(0, _scheduler.active_timer_count());
}

TEST_F(AIAdmissionControllerTest, CancelWakeTaskMaterializationFailureDoesNotEscapeAndFailsClosed) {
    _limits.inflight_cap = 0;
    auto reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    enqueue(request(10, 101, 1), [reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
    });
    _scheduler.run_until_idle();
    ASSERT_EQ(1, _scheduler.active_timer_count());

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool throw_once = true;
    sync_point->SetCallBack("AIAdmissionController::_request_pump_locked:before_task_materialization",
                            [&throw_once](void*) {
                                if (std::exchange(throw_once, false)) {
                                    throw std::bad_alloc();
                                }
                            });

    EXPECT_NO_THROW(_tickets.front().cancel());
    sync_point->ClearCallBack("AIAdmissionController::_request_pump_locked:before_task_materialization");

    EXPECT_EQ((std::vector<AIAdmissionFailureReason>{AIAdmissionFailureReason::LOCAL_RESOURCE}), *reasons);
    EXPECT_EQ(0, _scheduler.active_timer_count());
}

TEST_F(AIAdmissionControllerTest, InitialPostFailureRollsBackRegistrationWithoutInvokingItsCallback) {
    _scheduler.fail_next_post(Status::MemoryLimitExceeded("scheduler-post-sentinel"));
    auto rejected_callback_count = std::make_shared<size_t>(0);
    auto rejected = _controller.enqueue(request(10, 101, 1),
                                        [rejected_callback_count](AIAdmissionResult) { ++*rejected_callback_count; });

    ASSERT_FALSE(rejected.ok());
    EXPECT_TRUE(rejected.status().is_mem_limit_exceeded()) << rejected.status();
    EXPECT_EQ("scheduler-post-sentinel", rejected.status().message());
    EXPECT_EQ(0, *rejected_callback_count);
    EXPECT_EQ(0, _scheduler.ready_count());

    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 2), capture(&trace));
    _scheduler.run_until_idle();
    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 2}}), trace)
            << "the rolled-back query and workgroup rings must not retain the rejected attempt";
    EXPECT_EQ(0, *rejected_callback_count);
}

TEST_F(AIAdmissionControllerTest, TimerProgressFailureDrainsEveryPendingCallbackAndOneThrowDoesNotBlockTheRest) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> initial_trace;
    auto reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    enqueue(request(10, 101, 1), capture(&initial_trace));
    enqueue(request(10, 102, 2), [reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
        throw std::runtime_error("first terminal callback failure");
    });
    enqueue(request(20, 201, 3), [reasons](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        reasons->emplace_back(std::get<AIAdmissionFailure>(result).reason);
    });
    _scheduler.run_until_idle();
    ASSERT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}}), initial_trace);

    _limits.inflight_cap = 0;
    _scheduler.fail_next_timer(Status::MemoryLimitExceeded("scheduler-timer-sentinel"));
    complete_without_cooldown(0);
    _scheduler.run_until_idle();

    ASSERT_EQ(2, reasons->size());
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, (*reasons)[0]);
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, (*reasons)[1]);
    EXPECT_EQ(0, _scheduler.active_timer_count());

    _limits.inflight_cap = 2;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 4), capture(&trace));
    enqueue(request(20, 201, 5), capture(&trace));
    _scheduler.run_until_idle();
    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 4}, {20, query_id(201), 5}}), trace)
            << "a local progress drain must remove stale scheduler history and leave the controller reusable";
}

TEST_F(AIAdmissionControllerTest, FollowupPostFailureDrainsOnlyPendingAndPreservesAlreadyDeliveredGrant) {
    _limits.inflight_cap = 1;
    auto trace = std::make_shared<std::vector<AdmissionRecord>>();
    auto failures = std::make_shared<std::vector<AIAdmissionFailure>>();
    auto capture_owned = [this, trace, failures](AIAdmissionResult result) {
        if (auto* failure = std::get_if<AIAdmissionFailure>(&result); failure != nullptr) {
            failures->emplace_back(*failure);
            return;
        }
        AIAdmissionGrant grant = std::get<AIAdmissionGrant>(std::move(result));
        grant.commit_network_attempt();
        trace->emplace_back(grant.workgroup_key(), grant.query_id(), grant.attempt_id());
        _grants.emplace_back(std::move(grant));
    };
    enqueue(request(10, 101, 1), capture_owned);
    enqueue(request(20, 201, 2), capture_owned);
    _scheduler.fail_next_post(Status::MemoryLimitExceeded("scheduler-followup-sentinel"));

    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}}), *trace);
    ASSERT_EQ(1, failures->size());
    EXPECT_EQ(AIAdmissionFailureReason::LOCAL_RESOURCE, failures->front().reason);
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller))
            << "fail-closed pending drain must not reclaim an already delivered grant";

    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AIAdmissionControllerTest, SchedulerShutdownWhileArmingTimerDrainsPendingAsShutdownAndClosesAdmission) {
    _limits.inflight_cap = 0;
    auto failures = std::make_shared<std::vector<AIAdmissionFailure>>();
    auto callback = [failures](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
        failures->emplace_back(std::get<AIAdmissionFailure>(result));
    };
    enqueue(request(10, 101, 1), callback);
    _scheduler.fail_next_timer(Status::Shutdown("scheduler-stopped"));

    _scheduler.run_until_idle();

    ASSERT_EQ(1, failures->size());
    EXPECT_EQ(AIAdmissionFailureReason::SHUTDOWN, failures->front().reason);
    auto rejected = _controller.enqueue(request(20, 201, 2), callback);
    ASSERT_FALSE(rejected.ok());
    EXPECT_TRUE(rejected.status().is_service_unavailable()) << rejected.status();
}

TEST_F(AIAdmissionControllerTest, RoundRobinsAcrossWorkGroupsAndThenQueries) {
    const std::vector<AIAdmissionRequest> requests = {
            request(10, 101, 1),
            request(10, 101, 2),
            request(10, 102, 3),
            request(10, 102, 4),
            request(20, 201, 5),
            request(20, 201, 6),
            request(20, query_id(202, 7), 7),
            request(20, query_id(202, 7), 8),
    };
    std::vector<AdmissionRecord> trace;
    for (const auto& admission : requests) {
        enqueue(admission, capture(&trace));
    }
    _scheduler.run_until_idle();

    const std::vector<AdmissionRecord> expected = {
            {10, query_id(101), 1}, {20, query_id(201), 5}, {10, query_id(102), 3}, {20, query_id(202, 7), 7},
            {10, query_id(101), 2}, {20, query_id(201), 6}, {10, query_id(102), 4}, {20, query_id(202, 7), 8},
    };
    EXPECT_EQ(expected, trace);
}

TEST_F(AIAdmissionControllerTest, TreatsSameLowBitsWithDifferentHighBitsAsDistinctQueries) {
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, query_id(202, 7), 1), capture(&trace));
    enqueue(request(10, query_id(202, 7), 2), capture(&trace));
    enqueue(request(10, query_id(202, 8), 3), capture(&trace));
    enqueue(request(10, query_id(202, 8), 4), capture(&trace));
    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(202, 7), 1},
                                            {10, query_id(202, 8), 3},
                                            {10, query_id(202, 7), 2},
                                            {10, query_id(202, 8), 4}}),
              trace);
}

TEST_F(AIAdmissionControllerTest, SameNumericIdDifferentVersionsUseDistinctFairnessDomains) {
    const AIWorkGroupKey old_workgroup{1, 10};
    const AIWorkGroupKey new_workgroup{2, 10};
    std::vector<AdmissionRecord> trace;

    enqueue(request(old_workgroup, query_id(101), 1), capture(&trace));
    enqueue(request(old_workgroup, query_id(102), 2), capture(&trace));
    enqueue(request(new_workgroup, query_id(201), 3), capture(&trace));
    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{old_workgroup, query_id(101), 1},
                                            {new_workgroup, query_id(201), 3},
                                            {old_workgroup, query_id(102), 2}}),
              trace);
}

TEST_F(AIAdmissionControllerTest, CancelQueryOnlyCancelsExactWorkGroupVersion) {
    const AIWorkGroupKey old_workgroup{1, 10};
    const AIWorkGroupKey new_workgroup{2, 10};
    const UniqueId shared_query_id = query_id(101);
    _limits.inflight_cap = 0;
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;

    enqueue(request(old_workgroup, shared_query_id, 1), capture(&trace, &failures));
    enqueue(request(new_workgroup, shared_query_id, 2), capture(&trace, &failures));
    _scheduler.run_until_idle();
    ASSERT_TRUE(trace.empty());

    _controller.cancel_query(old_workgroup, shared_query_id);
    _scheduler.run_until_idle();
    ASSERT_EQ(1, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::CANCELLED, failures.front().reason);

    _limits.inflight_cap = 1;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    EXPECT_EQ((AdmissionRecord{new_workgroup, shared_query_id, 2}), trace.front());
}

TEST_F(AIAdmissionControllerTest, ReconsidersFairnessAfterEveryAttemptAndRemainsWorkConserving) {
    _limits.inflight_cap = 4;
    std::vector<AdmissionRecord> trace;
    bool small_enqueued = false;
    auto callback = capture(&trace, nullptr, [&](const AdmissionRecord& record) {
        if (record.attempt_id == 1 && !small_enqueued) {
            small_enqueued = true;
            enqueue(request(10, 102, 100), capture(&trace));
        }
    });
    for (uint64_t attempt = 1; attempt <= 8; ++attempt) {
        enqueue(request(10, 101, attempt), callback);
    }
    _scheduler.run_until_idle();

    ASSERT_EQ(4, trace.size());
    EXPECT_EQ(
            (std::vector<AdmissionRecord>{
                    {10, query_id(101), 1}, {10, query_id(102), 100}, {10, query_id(101), 2}, {10, query_id(101), 3}}),
            trace);
    EXPECT_EQ(4, AIAdmissionControllerTestPeer::inflight(_controller))
            << "one runnable query must rapidly fill all available capacity";
}

TEST_F(AIAdmissionControllerTest, NewlyArrivedSmallQueryGetsTheNextEligibleRelease) {
    _limits.inflight_cap = 2;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 4; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}, {10, query_id(101), 2}}), trace);

    enqueue(request(10, 102, 100), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());

    AIBucketResolutionGuard resolution = _grants.front().complete_transport();
    EXPECT_EQ(2, trace.size()) << "native completion only posts an O(1) controller wakeup";
    resolution.resolve_without_cooldown();
    _scheduler.run_until_idle();

    ASSERT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(102), 100}), trace.back());
}

TEST_F(AIAdmissionControllerTest, NewlyArrivedWorkGroupGetsTheNextEligibleRelease) {
    _limits.inflight_cap = 2;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 4; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());

    enqueue(request(20, query_id(201, 9), 100), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());

    complete_without_cooldown(0);
    EXPECT_EQ(2, trace.size());
    _scheduler.run_until_idle();

    ASSERT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201, 9), 100}), trace.back());
}

TEST_F(AIAdmissionControllerTest, NewlyArrivedSmallQueryDoesNotSkipAnotherRunnableWorkGroupTurn) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}}), trace);

    enqueue(request(20, 201, 2), capture(&trace));
    enqueue(request(10, 102, 3), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201), 2}), trace.back())
            << "query-level latency preference cannot bypass an already-runnable WorkGroup turn";
}

TEST_F(AIAdmissionControllerTest, DeferredRetryReentersTheTailOfItsQueryAwareQueue) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    enqueue(request(10, 101, 2), capture(&trace));
    enqueue(request(10, 101, 3), capture(&trace));
    enqueue(request(10, 102, 50), capture(&trace));
    enqueue(request(10, 101, 99, "https://model.invalid/v1/chat", "key-a", _clock.monotonic_now_ns() + kSecond),
            capture(&trace));

    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    for (size_t grant_index = 0; grant_index < 4; ++grant_index) {
        complete_without_cooldown(grant_index);
        _scheduler.run_until_idle();
    }

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1},
                                            {10, query_id(102), 50},
                                            {10, query_id(101), 2},
                                            {10, query_id(101), 3},
                                            {10, query_id(101), 99}}),
              trace);
}

TEST_F(AIAdmissionControllerTest, TokenAndCooldownBlockedBucketsDoNotCauseHeadOfLineBlocking) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 3;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1, "https://token.invalid/v1/chat", "token-key"), capture(&trace));
    enqueue(request(10, 101, 2, "https://token.invalid/v1/chat", "token-key"), capture(&trace));

    const auto cooldown_key =
            AIRateLimitKey::create("https://cooldown.invalid/v1/chat", "cooldown-key", AICapability::CHAT);
    _controller.extend_bucket_cooldown(cooldown_key, _clock.monotonic_now_ns() + 10 * kSecond);
    AIAdmissionRequest cooldown = request(20, 201, 3);
    cooldown.rate_limit_key = cooldown_key;
    enqueue(cooldown, capture(&trace));
    enqueue(request(30, 301, 4, "https://eligible.invalid/v1/chat", "eligible-key"), capture(&trace));
    _scheduler.run_until_idle();

    EXPECT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}, {30, query_id(301), 4}}), trace);
}

TEST_F(AIAdmissionControllerTest, UnresolvedCompletionBlocksOnlyItsBucketUntilCooldownIsInstalled) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1, "https://shared.invalid/v1/chat", "shared-key"), capture(&trace));
    enqueue(request(20, 201, 2, "https://shared.invalid/v1/chat", "shared-key"), capture(&trace));
    enqueue(request(30, 301, 3, "https://other.invalid/v1/chat", "other-key"), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ((std::vector<AdmissionRecord>{{10, query_id(101), 1}}), trace);

    AIBucketResolutionGuard shared_resolution = _grants[0].complete_transport();
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    EXPECT_EQ((AdmissionRecord{30, query_id(301), 3}), trace.back());

    complete_without_cooldown(1);
    _scheduler.run_until_idle();
    EXPECT_EQ(2, trace.size());
    shared_resolution.resolve_with_cooldown(_clock.monotonic_now_ns() + kSecond);
    _scheduler.run_until_idle();
    EXPECT_EQ(2, trace.size());

    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201), 2}), trace.back());
}

TEST_F(AIAdmissionControllerTest, ObtainsTokenAndInflightTogetherAndHonorsALoweredCap) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1, "https://a.invalid/v1/chat", "key-a"), capture(&trace));
    enqueue(request(20, 201, 2, "https://b.invalid/v1/chat", "key-b"), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size()) << "a full inflight cap must not pre-charge the second bucket token";

    _limits.inflight_cap = 2;
    enqueue(request(20, 201, 3, "https://b.invalid/v1/chat", "key-b"), capture(&trace));
    enqueue(request(30, 301, 4, "https://c.invalid/v1/chat", "key-c"), capture(&trace));
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    EXPECT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{30, query_id(301), 4}), trace.back());
    EXPECT_EQ(2, AIAdmissionControllerTestPeer::inflight(_controller))
            << "a token-starved waiter must not occupy an inflight slot";

    enqueue(request(40, 401, 5, "https://d.invalid/v1/chat", "key-d"), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(3, trace.size());
    _limits.inflight_cap = 1;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    EXPECT_EQ(2, AIAdmissionControllerTestPeer::inflight(_controller))
            << "lowering the cap cancels no accepted attempt";
    complete_without_cooldown(1);
    _scheduler.run_until_idle();
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(3, trace.size()) << "new admission remains blocked while count equals the lowered cap";
    complete_without_cooldown(2);
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());
    EXPECT_EQ((AdmissionRecord{40, query_id(401), 5}), trace.back());
}

TEST_F(AIAdmissionControllerSingleCompletionTest, CompletionCapacityWaitersDoNotPrechargeTokensBeforeJointAdmission) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    enqueue(request(10, 101, 1, "https://a.invalid/v1/chat", "key-a"), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    std::optional<AIBucketResolutionGuard> completion = _grants[0].complete_transport();
    completion->resolve_without_cooldown();
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, trace.size());

    const size_t cancelled_ticket =
            enqueue(request(20, 201, 2, "https://b.invalid/v1/chat", "key-b"), capture(&trace, &failures));
    enqueue(request(20, 201, 3, "https://b.invalid/v1/chat", "key-b"), capture(&trace, &failures));
    _scheduler.run_until_idle();
    EXPECT_EQ(1, trace.size()) << "released inflight is insufficient while completion capacity remains reserved";

    _tickets[cancelled_ticket].cancel();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::CANCELLED, failures.front().reason);
    EXPECT_EQ(1, trace.size());

    const int64_t release_time_ns = _clock.monotonic_now_ns();
    completion.reset();
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201), 3}), trace.back());
    EXPECT_EQ(release_time_ns, _clock.monotonic_now_ns())
            << "the cancelled waiter must not pre-charge the shared bucket token while completion capacity is full";
}

TEST_F(AIAdmissionControllerTest, OverlappingBucketResolutionsAreReferenceCountedAndKeepMaximumCooldown) {
    _limits.inflight_cap = 3;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 3; ++attempt) {
        enqueue(request(10 + attempt, 100 + attempt, attempt, "https://shared.invalid/v1/chat", "shared-key"),
                capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(3, trace.size());

    std::optional<AIBucketResolutionGuard> first = _grants[0].complete_transport();
    std::optional<AIBucketResolutionGuard> second = _grants[1].complete_transport();
    std::optional<AIBucketResolutionGuard> third = _grants[2].complete_transport();
    enqueue(request(20, 201, 20, "https://shared.invalid/v1/chat", "shared-key"), capture(&trace));

    first->resolve_with_cooldown(_clock.monotonic_now_ns() + 3 * kSecond);
    first.reset();
    second->resolve_with_cooldown(std::numeric_limits<int64_t>::max());
    second.reset();
    _scheduler.run_until_idle();
    EXPECT_EQ(3, trace.size())
            << "a saturated cooldown is resolved without blocking, while the unresolved sibling still owns the bucket";

    _clock.advance_ns(3 * kSecond);
    _scheduler.run_until_idle();
    EXPECT_EQ(3, trace.size()) << "the final unresolved sibling still blocks its bucket after the shorter cooldown";

    third->resolve_with_cooldown(_clock.monotonic_now_ns() + 2 * kSecond);
    third.reset();
    _scheduler.run_until_idle();
    EXPECT_EQ(3, trace.size()) << "the final resolution preserves the maximum five-second cooldown";
    _clock.advance_ns(2 * kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201), 20}), trace.back());
}

TEST_F(AIAdmissionControllerTest, CancellationDeadlineAndShutdownWakeQueuedOrDeferredAttemptsExactlyOnce) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    FakeAdmissionMemoryContextState memory;
    std::atomic<int> callbacks_in_scope{0};
    std::atomic<int> callback_owners_destroyed_in_scope{0};
    auto capture_with_memory_owner = [&](std::vector<AdmissionRecord>* capture_trace,
                                         std::vector<AIAdmissionFailure>* capture_failures) {
        auto owner = std::make_shared<AdmissionCallbackOwner>(&memory, &callback_owners_destroyed_in_scope);
        AIAdmissionCallback inner = capture(capture_trace, capture_failures);
        return [&, owner = std::move(owner), inner = std::move(inner)](AIAdmissionResult result) mutable {
            if (tls_admission_physical_scope == &memory) {
                callbacks_in_scope.fetch_add(1, std::memory_order_relaxed);
            }
            inner(std::move(result));
        };
    };
    enqueue(request(1, 1, 1), capture(&trace, &failures));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    auto cancelled_request = request(2, 2, 2);
    cancelled_request.memory = memory.context();
    auto deadline_request =
            request(3, 3, 3, "https://deadline.invalid", "deadline", 0, _clock.monotonic_now_ns() + kSecond);
    deadline_request.memory = memory.context();
    auto deferred_request =
            request(4, 4, 4, "https://deferred.invalid", "deferred", _clock.monotonic_now_ns() + 10 * kSecond);
    deferred_request.memory = memory.context();
    enqueue(cancelled_request, capture_with_memory_owner(&trace, &failures));
    enqueue(deadline_request, capture_with_memory_owner(&trace, &failures));
    enqueue(deferred_request, capture_with_memory_owner(&trace, &failures));

    _controller.cancel_query(UniqueId{0, 2}, query_id(2));
    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(2, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::CANCELLED, failures[0].reason);
    EXPECT_EQ(AIAdmissionFailureReason::DEADLINE_EXCEEDED, failures[1].reason);

    _scheduler.fail_next_post(Status::MemoryLimitExceeded("shutdown-drain-allocation"));
    _controller.shutdown();
    _scheduler.run_until_idle();
    ASSERT_EQ(3, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::SHUTDOWN, failures[2].reason);
    EXPECT_EQ(0, _scheduler.active_timer_count());

    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(3, failures.size());

    cancelled_request.memory = {};
    deadline_request.memory = {};
    deferred_request.memory = {};
    _tickets.clear();
    EXPECT_EQ(0, callbacks_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(3, callback_owners_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(0, memory.references.load(std::memory_order_relaxed));
    EXPECT_GT(memory.entries.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(memory.entries.load(std::memory_order_relaxed), memory.exits.load(std::memory_order_relaxed));
}

TEST_F(AIAdmissionControllerTest, AttemptTicketCancelsOnlyItsOwnAttemptWithinAQuery) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    std::vector<AdmissionRecord> cancelled_trace;
    std::vector<AdmissionRecord> survivor_trace;
    std::vector<AIAdmissionFailure> failures;
    enqueue(request(1, query_id(9, 8), 1), capture(&trace, &failures));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    const size_t cancelled_ticket = enqueue(request(1, query_id(9, 8), 2), capture(&cancelled_trace, &failures));
    enqueue(request(1, query_id(9, 8), 2), capture(&survivor_trace, &failures));
    _tickets[cancelled_ticket].cancel();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::CANCELLED, failures.front().reason);
    EXPECT_TRUE(cancelled_trace.empty());

    complete_without_cooldown(0);
    _scheduler.run_until_idle();
    EXPECT_TRUE(cancelled_trace.empty());
    ASSERT_EQ(1, survivor_trace.size());
    EXPECT_EQ((AdmissionRecord{1, query_id(9, 8), 2}), survivor_trace.front());
}

TEST_F(AIAdmissionControllerTest, GrantRetainsItsCoreAfterTheControllerFacadeIsDestroyed) {
    ManualAIClock clock;
    ManualAIControlScheduler scheduler(&clock);
    MutableAIAdmissionLimitSource limits;
    std::optional<AIAdmissionGrant> retained_grant;
    size_t callback_count = 0;

    {
        AIAdmissionController controller(&clock, &scheduler, &limits, 1);
        AIAdmissionRequest admission;
        admission.workgroup_key = UniqueId{0, 10};
        admission.query_id = UniqueId{7, 11};
        admission.attempt_id = 1;
        admission.rate_limit_key =
                AIRateLimitKey::create("https://lifetime.invalid/v1/chat", "lifetime-key", AICapability::CHAT);
        admission.eligible_at_ns = clock.monotonic_now_ns();
        admission.request_deadline_ns = clock.monotonic_now_ns() + kHour;
        const int64_t query_deadline_ns = admission.request_deadline_ns;
        admission.lifecycle = [query_deadline_ns] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
        };

        auto ticket_or = controller.enqueue(admission, [&](AIAdmissionResult result) {
            ASSERT_TRUE(std::holds_alternative<AIAdmissionGrant>(result));
            AIAdmissionGrant grant = std::get<AIAdmissionGrant>(std::move(result));
            grant.commit_network_attempt();
            retained_grant.emplace(std::move(grant));
            ++callback_count;
        });
        ASSERT_TRUE(ticket_or.ok()) << ticket_or.status();
        AIAdmissionTicket ticket = std::move(ticket_or).value();
        scheduler.run_until_idle();
        ASSERT_TRUE(retained_grant.has_value());
        EXPECT_EQ(1, callback_count);
    }

    scheduler.run_until_idle();
    AIBucketResolutionGuard resolution = retained_grant->complete_transport();
    resolution.resolve_without_cooldown();
    scheduler.run_until_idle();
    EXPECT_EQ(1, callback_count);
}

TEST_F(AIAdmissionControllerTest, GrantAndPermitAreMoveOnlyAndRetainControllerCore) {
    static_assert(!std::is_copy_constructible_v<AIAdmissionGrant>);
    static_assert(!std::is_copy_assignable_v<AIAdmissionGrant>);
    static_assert(std::is_nothrow_move_constructible_v<AIAdmissionGrant>);
    static_assert(std::is_nothrow_move_assignable_v<AIAdmissionGrant>);
    static_assert(std::is_nothrow_destructible_v<AIAdmissionGrant>);
    static_assert(noexcept(std::declval<AIAdmissionGrant&>().commit_network_attempt()));
    static_assert(noexcept(std::declval<AIAdmissionGrant&>().complete_transport()));
    static_assert(!std::is_copy_constructible_v<AIBucketResolutionGuard>);
    static_assert(!std::is_copy_assignable_v<AIBucketResolutionGuard>);
    static_assert(std::is_nothrow_move_constructible_v<AIBucketResolutionGuard>);
    static_assert(std::is_nothrow_move_assignable_v<AIBucketResolutionGuard>);
    static_assert(std::is_nothrow_destructible_v<AIBucketResolutionGuard>);
    static_assert(noexcept(std::declval<AIBucketResolutionGuard&>().resolve_without_cooldown()));
    static_assert(noexcept(std::declval<AIBucketResolutionGuard&>().resolve_with_cooldown(0)));

    FakeAdmissionMemoryContextState memory;
    std::atomic<int> callbacks_in_scope{0};
    std::atomic<int> callback_owners_destroyed_in_scope{0};
    std::atomic<int> token_moves_in_scope{0};
    std::atomic<int> token_moves_outside_scope{0};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AIAdmissionController:request_owned_token_move", [&](void*) {
        if (tls_admission_physical_scope == &memory) {
            token_moves_in_scope.fetch_add(1, std::memory_order_relaxed);
        } else {
            token_moves_outside_scope.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::optional<AIAdmissionGrant> first;
    std::optional<AIAdmissionGrant> second;
    auto first_request = request(1, 1, 1, "https://first.invalid/v1/chat", "first-key");
    auto second_request = request(2, 2, 2, "https://second.invalid/v1/chat", "second-key");
    first_request.memory = memory.context();
    second_request.memory = memory.context();
    const AIRateLimitKey first_key = first_request.rate_limit_key;
    const AIRateLimitKey second_key = second_request.rate_limit_key;
    auto capture_grant = [&](std::optional<AIAdmissionGrant>* target, bool throw_after_delivery = false) {
        auto owner = std::make_shared<AdmissionCallbackOwner>(&memory, &callback_owners_destroyed_in_scope);
        return [&, target, owner = std::move(owner), throw_after_delivery](AIAdmissionResult result) {
            if (tls_admission_physical_scope == &memory) {
                callbacks_in_scope.fetch_add(1, std::memory_order_relaxed);
            }
            ASSERT_TRUE(std::holds_alternative<AIAdmissionGrant>(result));
            target->emplace(std::get<AIAdmissionGrant>(std::move(result)));
            if (throw_after_delivery) {
                throw std::runtime_error("grant-callback-sentinel");
            }
        };
    };
    enqueue(first_request, capture_grant(&first, true));
    enqueue(second_request, capture_grant(&second));
    _scheduler.run_until_idle();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(2, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(2, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::bucket_owners(_controller, first_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::bucket_owners(_controller, second_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_pins(_controller, first_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_pins(_controller, second_key));

    *first = std::move(*second);
    second.reset();
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, first_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, first_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::completion_in_use(_controller));

    first->commit_network_attempt();
    AIBucketResolutionGuard second_guard = first->complete_transport();
    first.reset();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, second_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::bucket_owners(_controller, second_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, second_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_pins(_controller, second_key));

    std::optional<AIAdmissionGrant> third;
    auto third_request = request(3, 3, 3, "https://third.invalid/v1/chat", "third-key");
    third_request.memory = memory.context();
    const AIRateLimitKey third_key = third_request.rate_limit_key;
    enqueue(third_request, capture_grant(&third));
    _scheduler.run_until_idle();
    ASSERT_TRUE(third.has_value());
    third->commit_network_attempt();
    AIBucketResolutionGuard third_guard = third->complete_transport();
    third.reset();

    second_guard = std::move(third_guard);
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, second_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, second_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, second_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::bucket_owners(_controller, third_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, third_key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_pins(_controller, third_key));

    second_guard = AIBucketResolutionGuard{};
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, third_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, third_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, third_key));

    std::optional<AIAdmissionGrant> abandoned;
    auto abandoned_request = request(4, 4, 4, "https://abandoned.invalid/v1/chat", "abandoned-key");
    abandoned_request.memory = memory.context();
    const AIRateLimitKey abandoned_key = abandoned_request.rate_limit_key;
    enqueue(abandoned_request, capture_grant(&abandoned));
    _scheduler.run_until_idle();
    ASSERT_TRUE(abandoned.has_value());
    abandoned->commit_network_attempt();
    abandoned.reset();
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, abandoned_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, abandoned_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, abandoned_key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));

    first_request.memory = {};
    second_request.memory = {};
    third_request.memory = {};
    abandoned_request.memory = {};
    _tickets.clear();
    EXPECT_EQ(0, callbacks_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(4, callback_owners_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_GT(token_moves_in_scope.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(0, token_moves_outside_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(0, memory.references.load(std::memory_order_relaxed));
    EXPECT_GT(memory.entries.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(memory.entries.load(std::memory_order_relaxed), memory.exits.load(std::memory_order_relaxed));
}

TEST_F(AIAdmissionControllerTest, MovedFromTentativeGrantRemainsSafeToInspectAndDestroy) {
    std::optional<AIAdmissionGrant> source;
    enqueue(request(1, 1, 1), [&](AIAdmissionResult result) {
        ASSERT_TRUE(std::holds_alternative<AIAdmissionGrant>(result));
        source.emplace(std::get<AIAdmissionGrant>(std::move(result)));
    });
    _scheduler.run_until_idle();
    ASSERT_TRUE(source.has_value());

    std::optional<AIAdmissionGrant> destination(std::move(*source));
    source->commit_network_attempt();
    AIBucketResolutionGuard empty = source->complete_transport();
    source.reset();
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));

    destination.reset();
    _scheduler.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AIAdmissionControllerTest, FacadeDestructionRetainsQueuedShutdownDeliveryUntilSchedulerDrain) {
    ManualAIClock clock;
    ManualAIControlScheduler scheduler(&clock);
    MutableAIAdmissionLimitSource limits;
    size_t callback_count = 0;
    std::optional<AIAdmissionFailureReason> reason;

    {
        AIAdmissionController controller(&clock, &scheduler, &limits, 1);
        AIAdmissionRequest admission = request(10, 101, 1, "https://deferred.invalid", "deferred-key",
                                               clock.monotonic_now_ns() + 10 * kSecond);
        auto ticket_or = controller.enqueue(admission, [&](AIAdmissionResult result) {
            ASSERT_TRUE(std::holds_alternative<AIAdmissionFailure>(result));
            reason = std::get<AIAdmissionFailure>(result).reason;
            ++callback_count;
        });
        ASSERT_TRUE(ticket_or.ok()) << ticket_or.status();
        AIAdmissionTicket ticket = std::move(ticket_or).value();
    }

    EXPECT_EQ(0, callback_count);
    scheduler.run_until_idle();
    EXPECT_EQ(1, callback_count);
    EXPECT_EQ(AIAdmissionFailureReason::SHUTDOWN, reason);
    scheduler.run_until_idle();
    EXPECT_EQ(1, callback_count);
}

TEST_F(AIAdmissionControllerTest, ResourceReleaseAfterShutdownDrainDoesNotScheduleControlWork) {
    ManualAIClock clock;
    ManualAIControlScheduler scheduler(&clock);
    MutableAIAdmissionLimitSource limits;
    limits.inflight_cap = 2;
    std::optional<AIAdmissionGrant> tentative;
    std::optional<AIAdmissionGrant> committed;
    int64_t cooldown_until_ns = 0;

    {
        AIAdmissionController controller(&clock, &scheduler, &limits, 2);
        auto retain = [&](bool commit, std::optional<AIAdmissionGrant>* target) {
            return [commit, target](AIAdmissionResult result) {
                ASSERT_TRUE(std::holds_alternative<AIAdmissionGrant>(result));
                AIAdmissionGrant grant = std::get<AIAdmissionGrant>(std::move(result));
                if (commit) {
                    grant.commit_network_attempt();
                }
                target->emplace(std::move(grant));
            };
        };
        ASSERT_TRUE(controller
                            .enqueue(request(10, 101, 1, "https://tentative.invalid", "tentative-key"),
                                     retain(false, &tentative))
                            .ok());
        ASSERT_TRUE(controller
                            .enqueue(request(20, 201, 2, "https://committed.invalid", "committed-key"),
                                     retain(true, &committed))
                            .ok());
        scheduler.run_until_idle();
        ASSERT_TRUE(tentative.has_value());
        ASSERT_TRUE(committed.has_value());
        cooldown_until_ns = clock.monotonic_now_ns() + kSecond;
        controller.shutdown();
        scheduler.run_until_idle();
    }

    ASSERT_EQ(0, scheduler.ready_count());
    ASSERT_EQ(0, scheduler.active_timer_count());
    const uint64_t clock_accesses_after_drain = clock.access_count();
    const uint64_t limit_accesses_after_drain = limits.access_count;
    tentative->commit_network_attempt();
    EXPECT_EQ(0, scheduler.ready_count());
    EXPECT_EQ(0, scheduler.active_timer_count());
    EXPECT_EQ(clock_accesses_after_drain, clock.access_count());
    EXPECT_EQ(limit_accesses_after_drain, limits.access_count);
    tentative.reset();
    EXPECT_EQ(0, scheduler.ready_count());
    EXPECT_EQ(0, scheduler.active_timer_count());

    std::optional<AIBucketResolutionGuard> resolution = committed->complete_transport();
    resolution->resolve_with_cooldown(cooldown_until_ns);
    resolution.reset();
    committed.reset();
    EXPECT_EQ(0, scheduler.ready_count());
    EXPECT_EQ(0, scheduler.active_timer_count());
    EXPECT_EQ(clock_accesses_after_drain, clock.access_count());
    EXPECT_EQ(limit_accesses_after_drain, limits.access_count);
}

TEST_F(AIAdmissionControllerTest, StaleCancelledTimerCannotClearTheCurrentTimerGeneration) {
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1, "https://later.invalid", "later", _clock.monotonic_now_ns() + 10 * kSecond),
            capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, _scheduler.active_timer_count());

    enqueue(request(20, 201, 2, "https://earlier.invalid", "earlier", _clock.monotonic_now_ns() + 5 * kSecond),
            capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, _scheduler.active_timer_count());
    ASSERT_TRUE(_scheduler.force_fire_first_cancelled_timer());
    _scheduler.run_until_idle();

    EXPECT_TRUE(trace.empty());
    EXPECT_EQ(1, _scheduler.active_timer_count());
    _clock.advance_ns(5 * kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    EXPECT_EQ((AdmissionRecord{20, query_id(201), 2}), trace.front());
}

TEST_F(AIAdmissionControllerTest, FullCapacityWakeupsDoNotRescanTheQueuedBacklog) {
    _limits.inflight_cap = 1;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 10'000; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);

    for (int i = 0; i < 100; ++i) {
        _controller.notify_limits_changed();
        _scheduler.run_until_idle();
    }

    EXPECT_LE(AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before, 1)
            << "a full process cap must be an O(1) wakeup, independent of queued row count";
}

TEST_F(AIAdmissionControllerTest, UnresolvedBucketSuspendsOnlyTheFrontWithoutControlTaskSpin) {
    _limits.inflight_cap = 1;
    _limits.chat_qps = 20'000;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 10'000; ++attempt) {
        enqueue(request(10, 101, attempt, "https://shared.invalid", "shared-key"), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());

    _limits.inflight_cap = 64;
    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);
    std::optional<AIBucketResolutionGuard> resolution = _grants.front().complete_transport();
    for (uint64_t attempt = 1; attempt <= 50; ++attempt) {
        enqueue(request(10, 102, attempt, "https://ready-" + std::to_string(attempt) + ".invalid", "ready-key"),
                capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(51, trace.size()) << "an unresolved query must not block another query in the same WorkGroup";
    EXPECT_LE(AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before, 51)
            << "peer grants must not detach more blockers from an unresolved query";
    EXPECT_EQ(0, _scheduler.ready_count()) << "an unresolved bucket must not spin through the hidden row backlog";

    resolution->resolve_without_cooldown();
    resolution.reset();
    _scheduler.run_until_idle();
    ASSERT_GT(trace.size(), 51);
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace[51])
            << "bucket suspension must preserve the query's attempt order";
    _controller.cancel_query(UniqueId{0, 10}, query_id(101));
    _scheduler.run_until_idle();
}

TEST_F(AIAdmissionControllerTest, QpsExhaustionDefersOneFrontWithoutDrainingTheQueuedBacklog) {
    _limits.inflight_cap = 100;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);
    for (uint64_t attempt = 1; attempt <= 10'000; ++attempt) {
        enqueue(request(10, 101, attempt, "https://shared.invalid", "shared-key"), capture(&trace));
    }
    _scheduler.run_until_idle();

    ASSERT_EQ(1, trace.size());
    EXPECT_LE(AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before, 2)
            << "one admitted front and one QPS-blocked front are sufficient to arm the next token wakeup";
    EXPECT_EQ(0, _scheduler.ready_count());
    EXPECT_EQ(1, _scheduler.active_timer_count());
}

TEST_F(AIAdmissionControllerTest, RateBlockedLargeQueryKeepsOneBlockerWhileOtherQueryMakesProgress) {
    _limits.inflight_cap = 1'000;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 10'000; ++attempt) {
        enqueue(request(10, 101, attempt, "https://blocked.invalid", "blocked-key"), capture(&trace));
    }
    for (uint64_t attempt = 1; attempt <= 50; ++attempt) {
        enqueue(request(10, 102, attempt, "https://ready-" + std::to_string(attempt) + ".invalid", "ready-key"),
                capture(&trace));
    }

    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);
    _scheduler.run_until_idle();

    EXPECT_EQ(51, trace.size()) << "the same-WorkGroup small query must remain work-conserving";
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller))
            << "a suspended query owns exactly one rate blocker regardless of its hidden row count";
    EXPECT_LE(AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before, 52)
            << "successful peer queries must not detach hidden rows from a suspended query";
    EXPECT_EQ(1, _scheduler.active_timer_count());
    _controller.cancel_query(UniqueId{0, 10}, query_id(101));
    _scheduler.run_until_idle();
}

TEST_F(AIAdmissionControllerTest, CancellingDeferredQueryBlockerExposesItsNextAttempt) {
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    const size_t blocker_ticket = enqueue(
            request(10, 101, 1, "https://deferred.invalid", "deferred-key", _clock.monotonic_now_ns() + 10 * kSecond),
            capture(&trace, &failures));
    enqueue(request(10, 101, 2, "https://ready.invalid", "ready-key"), capture(&trace, &failures));
    _scheduler.run_until_idle();
    EXPECT_TRUE(trace.empty()) << "a deferred query front must hide later attempts";

    _tickets[blocker_ticket].cancel();
    _scheduler.run_until_idle();

    ASSERT_EQ(1, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::CANCELLED, failures.front().reason);
    ASSERT_EQ(1, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace.front());
}

TEST_F(AIAdmissionControllerTest, ExpiringDeferredQueryBlockerExposesItsNextAttempt) {
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    const int64_t initial_query_deadline_ns = _clock.monotonic_now_ns() + kSecond;
    auto live_query_deadline_ns = std::make_shared<std::atomic<int64_t>>(initial_query_deadline_ns);
    auto blocker = request(10, 101, 1, "https://deferred.invalid", "deferred-key",
                           _clock.monotonic_now_ns() + 10 * kSecond, _clock.monotonic_now_ns() + 20 * kSecond);
    blocker.lifecycle = [live_query_deadline_ns] {
        return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = live_query_deadline_ns->load()};
    };
    enqueue(blocker, capture(&trace, &failures));
    enqueue(request(10, 101, 2, "https://ready.invalid", "ready-key"), capture(&trace, &failures));
    _scheduler.run_until_idle();
    EXPECT_TRUE(trace.empty()) << "a deferred query front must hide later attempts";

    _clock.advance_ns(kSecond);
    live_query_deadline_ns->store(_clock.monotonic_now_ns() + 5 * kSecond);
    _scheduler.run_until_idle();
    EXPECT_TRUE(failures.empty()) << "an old Query deadline timer must re-probe and observe a lifetime extension";
    EXPECT_TRUE(trace.empty());
    ASSERT_EQ(_clock.monotonic_now_ns() + 5 * kSecond, *_scheduler.next_active_timer_time_ns());

    _clock.advance_ns(5 * kSecond);
    _scheduler.run_until_idle();

    ASSERT_EQ(1, failures.size());
    EXPECT_EQ(AIAdmissionFailureReason::DEADLINE_EXCEEDED, failures.front().reason);
    ASSERT_EQ(1, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace.front());
}

TEST_F(AIAdmissionControllerTest, QueryCancellationDrainsSuspendedBlockerAndHiddenAttemptsWithoutAdmission) {
    _limits.chat_qps = 0;
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    for (uint64_t attempt = 1; attempt <= 3; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace, &failures));
    }
    _scheduler.run_until_idle();
    ASSERT_TRUE(trace.empty());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));

    _controller.cancel_query(UniqueId{0, 10}, query_id(101));
    _limits.chat_qps = 100;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();

    EXPECT_TRUE(trace.empty());
    ASSERT_EQ(3, failures.size());
    EXPECT_TRUE(std::all_of(failures.begin(), failures.end(), [](const AIAdmissionFailure& failure) {
        return failure.reason == AIAdmissionFailureReason::CANCELLED;
    }));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
}

TEST_F(AIAdmissionControllerTest, ShutdownDrainsSuspendedBlockerAndHiddenAttemptsWithoutRearming) {
    _limits.chat_qps = 0;
    std::vector<AdmissionRecord> trace;
    std::vector<AIAdmissionFailure> failures;
    for (uint64_t attempt = 1; attempt <= 3; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace, &failures));
    }
    _scheduler.run_until_idle();
    ASSERT_TRUE(trace.empty());
    ASSERT_EQ(1, _scheduler.active_timer_count());

    _controller.shutdown();
    _scheduler.run_until_idle();

    EXPECT_TRUE(trace.empty());
    ASSERT_EQ(3, failures.size());
    EXPECT_TRUE(std::all_of(failures.begin(), failures.end(), [](const AIAdmissionFailure& failure) {
        return failure.reason == AIAdmissionFailureReason::SHUTDOWN;
    }));
    EXPECT_EQ(0, _scheduler.ready_count());
    EXPECT_EQ(0, _scheduler.active_timer_count());
}

TEST_F(AIAdmissionControllerTest, EnablingAZeroQpsBucketReactivatesItsRateWaiterImmediately) {
    _limits.inflight_cap = 4;
    _limits.chat_qps = 0;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1), capture(&trace));
    _scheduler.run_until_idle();
    EXPECT_TRUE(trace.empty());

    _limits.chat_qps = 1;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 1}), trace.front());
}

TEST_F(AIAdmissionControllerTest, RaisingQpsInvalidatesTheOldTimerAndRecomputesAnEarlierWakeup) {
    _limits.inflight_cap = 4;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1), capture(&trace));
    enqueue(request(10, 101, 2), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    const int64_t old_wakeup_ns = *_scheduler.next_active_timer_time_ns();

    _clock.advance_ns(kSecond / 2);
    _limits.chat_qps = 4;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    ASSERT_TRUE(_scheduler.next_active_timer_time_ns().has_value());
    const int64_t new_wakeup_ns = *_scheduler.next_active_timer_time_ns();
    EXPECT_LT(new_wakeup_ns, old_wakeup_ns);

    ASSERT_TRUE(_scheduler.force_fire_first_cancelled_timer());
    _scheduler.run_until_idle();
    EXPECT_EQ(1, trace.size()) << "the stale timer generation must not admit or clear the earlier replacement";
    EXPECT_EQ(new_wakeup_ns, *_scheduler.next_active_timer_time_ns());

    _clock.advance_ns(new_wakeup_ns - _clock.monotonic_now_ns());
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace.back());
}

TEST_F(AIAdmissionControllerTest, LoweringQpsCannotReuseTheOldEarlierWakeup) {
    _limits.inflight_cap = 8;
    _limits.chat_qps = 4;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 5; ++attempt) {
        enqueue(request(10, 101, attempt), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());
    const int64_t old_wakeup_ns = *_scheduler.next_active_timer_time_ns();

    _limits.chat_qps = 1;
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());
    ASSERT_TRUE(_scheduler.next_active_timer_time_ns().has_value());
    EXPECT_GT(*_scheduler.next_active_timer_time_ns(), old_wakeup_ns);

    _clock.advance_ns(old_wakeup_ns - _clock.monotonic_now_ns());
    _scheduler.run_until_idle();
    EXPECT_EQ(4, trace.size()) << "the cancelled high-QPS timer must not admit under the lowered rate";
}

TEST_F(AIAdmissionControllerTest, LateTentativeCommitRecomputesAnInfiniteRateWaitIntoAFiniteTimer) {
    _limits.inflight_cap = 3;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    AIAdmissionCallback retain_tentative = capture_tentative(&trace);
    enqueue(request(10, 101, 1), retain_tentative);
    enqueue(request(10, 101, 2), retain_tentative);
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());

    _limits.chat_qps = 1;
    enqueue(request(10, 101, 3), capture(&trace));
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    ASSERT_TRUE(_scheduler.next_active_timer_time_ns().has_value());
    EXPECT_GT(*_scheduler.next_active_timer_time_ns(), _clock.monotonic_now_ns() + kSecond);

    const int64_t commit_time_ns = _clock.monotonic_now_ns();
    _grants[0].commit_network_attempt();
    _scheduler.run_until_idle();

    ASSERT_TRUE(_scheduler.next_active_timer_time_ns().has_value());
    EXPECT_EQ(commit_time_ns + kSecond, *_scheduler.next_active_timer_time_ns())
            << "a tentative commit must actively re-evaluate waiters that previously had infinite eligibility";
    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 3}), trace.back());
}

TEST_F(AIAdmissionControllerTest, TentativeRollbackReactivatesAnInfiniteRateWaiter) {
    _limits.inflight_cap = 3;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    AIAdmissionCallback retain_tentative = capture_tentative(&trace);
    enqueue(request(10, 101, 1), retain_tentative);
    enqueue(request(10, 101, 2), retain_tentative);
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());

    _limits.chat_qps = 1;
    enqueue(request(10, 101, 3), capture(&trace));
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));

    _grants[0] = AIAdmissionGrant{};
    _scheduler.run_until_idle();

    ASSERT_EQ(3, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 3}), trace.back());
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
}

TEST_F(AIAdmissionControllerTest, TentativeRollbackImmediatelyReactivatesAFiniteSameBucketWaiter) {
    _limits.inflight_cap = 3;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    AIAdmissionCallback retain_tentative = capture_tentative(&trace);
    enqueue(request(10, 101, 1), retain_tentative);
    enqueue(request(10, 102, 2), retain_tentative);
    enqueue(request(10, 103, 3), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    ASSERT_EQ(_clock.monotonic_now_ns() + kSecond / 2, *_scheduler.next_active_timer_time_ns());

    _grants[0] = AIAdmissionGrant{};
    _scheduler.run_until_idle();

    ASSERT_EQ(3, trace.size()) << "a synchronous submit failure refunds a token and must wake the suspended query";
    EXPECT_EQ((AdmissionRecord{10, query_id(103), 3}), trace.back());
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    EXPECT_EQ(0, _scheduler.active_timer_count()) << "the refunded token invalidates the old finite wakeup";
}

TEST_F(AIAdmissionControllerTest, TentativeRollbackReactivatesOnlyItsExactBucketFiniteWaiters) {
    _limits.inflight_cap = 6;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    AIAdmissionCallback retain_tentative = capture_tentative(&trace);
    for (uint64_t attempt = 1; attempt <= 2; ++attempt) {
        enqueue(request(10, 101, attempt, "https://a.invalid", "a-key"), retain_tentative);
        enqueue(request(10, 201, attempt, "https://b.invalid", "b-key"), retain_tentative);
    }
    enqueue(request(10, 101, 3, "https://a.invalid", "a-key"), capture(&trace));
    enqueue(request(10, 201, 3, "https://b.invalid", "b-key"), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());
    ASSERT_EQ(2, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);

    _grants[0] = AIAdmissionGrant{};
    _scheduler.run_until_idle();

    ASSERT_EQ(5, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 3}), trace.back());
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before)
            << "a rollback must not scan or retry another bucket's finite waiter";
    ASSERT_EQ(_clock.monotonic_now_ns() + kSecond / 2, *_scheduler.next_active_timer_time_ns());
    _controller.cancel_query(UniqueId{0, 10}, query_id(201));
    _scheduler.run_until_idle();
}

TEST_F(AIAdmissionControllerTest, LateTentativeCommitReactivatesOnlyItsBucketIndefiniteWaiters) {
    _limits.inflight_cap = 6;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    AIAdmissionCallback retain_tentative = capture_tentative(&trace);
    for (uint64_t attempt = 1; attempt <= 2; ++attempt) {
        enqueue(request(10, 101, attempt, "https://a.invalid", "a-key"), retain_tentative);
        enqueue(request(10, 201, attempt, "https://b.invalid", "b-key"), retain_tentative);
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(4, trace.size());

    _limits.chat_qps = 1;
    enqueue(request(10, 101, 3, "https://a.invalid", "a-key"), capture(&trace));
    enqueue(request(10, 201, 3, "https://b.invalid", "b-key"), capture(&trace));
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();
    ASSERT_EQ(2, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    const uint64_t before = AIAdmissionControllerTestPeer::scheduling_steps(_controller);

    _grants[0].commit_network_attempt();
    _scheduler.run_until_idle();

    EXPECT_EQ(2, AIAdmissionControllerTestPeer::rate_waiter_count(_controller))
            << "the committed bucket moves to a finite wait while the other bucket remains indefinite";
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::scheduling_steps(_controller) - before)
            << "a bucket commit must not scan or retry another bucket's indefinite waiter";
    ASSERT_EQ(_clock.monotonic_now_ns() + kSecond, *_scheduler.next_active_timer_time_ns());
    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(5, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 3}), trace.back());
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    _controller.cancel_query(UniqueId{0, 10}, query_id(201));
    _scheduler.run_until_idle();
}

TEST_F(AIAdmissionControllerTest, TentativeCommitDoesNotDisturbFiniteSameBucketWaiter) {
    _limits.inflight_cap = 3;
    _limits.chat_qps = 2;
    std::vector<AdmissionRecord> trace;
    enqueue(request(10, 101, 1), capture_tentative(&trace));
    enqueue(request(10, 101, 2), capture(&trace));
    enqueue(request(10, 101, 3), capture(&trace));
    _scheduler.run_until_idle();
    ASSERT_EQ(2, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    const int64_t wakeup_ns = *_scheduler.next_active_timer_time_ns();
    const uint64_t steps = AIAdmissionControllerTestPeer::scheduling_steps(_controller);

    _grants[0].commit_network_attempt();

    EXPECT_EQ(0, _scheduler.ready_count()) << "finite rate waiters need no commit-event re-evaluation";
    EXPECT_EQ(wakeup_ns, *_scheduler.next_active_timer_time_ns());
    EXPECT_EQ(steps, AIAdmissionControllerTestPeer::scheduling_steps(_controller));
}

TEST_F(AIAdmissionControllerTest, LimitChangePreservesFifoForSuspendedQueries) {
    _limits.inflight_cap = 16;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 3; ++attempt) {
        enqueue(request(10, 101, attempt, "https://shared.invalid", "shared-key"), capture(&trace));
    }
    enqueue(request(10, 201, 11, "https://b1.invalid", "b1-key"), capture(&trace));
    enqueue(request(10, 201, 12, "https://b2.invalid", "b2-key"), capture(&trace));
    enqueue(request(10, 301, 21, "https://c.invalid", "c-key"), capture(&trace));
    _scheduler.run_until_idle();

    ASSERT_EQ(4, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    _controller.notify_limits_changed();
    _scheduler.run_until_idle();

    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(5, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace.back())
            << "reactivating suspended queries must preserve registration order";
}

TEST_F(AIAdmissionControllerTest, NormalRateTimerPreservesFifoForSuspendedQueries) {
    _limits.inflight_cap = 16;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 3; ++attempt) {
        enqueue(request(10, 101, attempt, "https://shared.invalid", "shared-key"), capture(&trace));
    }
    enqueue(request(10, 201, 11, "https://b1.invalid", "b1-key"), capture(&trace));
    enqueue(request(10, 201, 12, "https://b2.invalid", "b2-key"), capture(&trace));
    enqueue(request(10, 301, 21, "https://c.invalid", "c-key"), capture(&trace));
    _scheduler.run_until_idle();

    ASSERT_EQ(4, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
    _clock.advance_ns(kSecond);
    _scheduler.run_until_idle();
    ASSERT_EQ(5, trace.size());
    EXPECT_EQ((AdmissionRecord{10, query_id(101), 2}), trace.back())
            << "due suspended queries must preserve registration order";
}

TEST_F(AIAdmissionControllerTest, NormalRateTimersDoNotRetainCompletedWaiterIndexEntries) {
    _limits.inflight_cap = 32;
    _limits.chat_qps = 1;
    std::vector<AdmissionRecord> trace;
    for (uint64_t attempt = 1; attempt <= 32; ++attempt) {
        enqueue(request(10, 101, attempt, "https://shared.invalid", "shared-key"), capture(&trace));
    }
    _scheduler.run_until_idle();
    ASSERT_EQ(1, trace.size());
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));

    while (trace.size() < 32) {
        _clock.advance_ns(kSecond);
        _scheduler.run_until_idle();
        EXPECT_LE(AIAdmissionControllerTestPeer::rate_waiter_count(_controller), 1);
    }
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
}

} // namespace
} // namespace starrocks
