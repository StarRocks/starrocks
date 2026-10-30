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

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <queue>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/status.h"
#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"
#include "common/logging.h"

namespace starrocks {

struct AIAdmissionBucketState {
    explicit AIAdmissionBucketState(AIRateLimitKey stable_key) : key(std::move(stable_key)) {}

    AIRateLimitKey key;
    int64_t registrations = 0;
    int64_t inflight = 0;
    int64_t owners = 0;
    int64_t unresolved = 0;
    std::map<uint64_t, uint64_t> rate_waiters;
    std::map<uint64_t, uint64_t> indefinite_rate_waiters;
    std::deque<uint64_t> completion_waiters;
    bool reactivate_rate_waiters = false;
    bool reactivate_indefinite_rate_waiters = false;
    bool reactivate_completion_waiters = false;
};

struct AIAdmissionAttempt {
    uint64_t registration_id = 0;
    uint64_t deferred_generation = 0;
    uint64_t deadline_generation = 0;
    int64_t armed_deadline_ns = 0;
    AIAdmissionRequest request;
    AIAdmissionCallback callback;
    std::weak_ptr<AIAdmissionControllerCore> core;
    std::shared_ptr<AIAdmissionBucketState> bucket_state;
    std::shared_ptr<AIAdmissionAttempt> cancel_next;
    std::shared_ptr<AIAdmissionAttempt> delivery_next;
    AIAdmissionFailureReason failure_reason = AIAdmissionFailureReason::LOCAL_RESOURCE;
    bool registration_counted = false;
    bool cancelled = false;
    bool done = false;
};

namespace {

template <typename Function>
void run_in_physical_scope(const AIMemoryContext& memory, Function&& function) {
    using StoredFunction = std::remove_reference_t<Function>;
    memory.run_in_physical_scope([](void* opaque) { (*static_cast<StoredFunction*>(opaque))(); },
                                 std::addressof(function));
}

struct StatusSnapshot {
    static constexpr size_t kMaxMessageSize = 4096;

    // Scheduler statuses can own heap memory created inside a request scope. Keep only a bounded stack snapshot
    // across that boundary, then materialize the public Status after the request scope has been restored.
    void capture(const Status& status) noexcept {
        code = status.code();
        const std::string_view source = status.message();
        message_size = std::min(source.size(), message.size());
        if (message_size != 0) {
            std::memcpy(message.data(), source.data(), message_size);
        }
    }

    bool ok() const noexcept { return code == TStatusCode::OK; }

    Status materialize() const {
        if (ok()) {
            return Status::OK();
        }
        return Status(code, std::string_view(message.data(), message_size));
    }

    TStatusCode::type code = TStatusCode::OK;
    std::array<char, kMaxMessageSize> message{};
    size_t message_size = 0;
};

struct QueryQueue {
    std::deque<std::shared_ptr<AIAdmissionAttempt>> attempts;
    std::shared_ptr<AIAdmissionAttempt> blocker;
    bool in_ring = false;
};

struct WorkGroupQueue {
    std::deque<UniqueId> query_ring;
    std::unordered_map<UniqueId, QueryQueue> queries;
    bool in_ring = false;
};

template <std::shared_ptr<AIAdmissionAttempt> AIAdmissionAttempt::*Next>
class AttemptChain {
public:
    AttemptChain() = default;
    ~AttemptChain() { clear(); }

    AttemptChain(const AttemptChain&) = delete;
    AttemptChain& operator=(const AttemptChain&) = delete;

    bool empty() const noexcept { return _head == nullptr; }

    void append(std::shared_ptr<AIAdmissionAttempt> attempt) noexcept {
        DCHECK(attempt != nullptr);
        DCHECK((attempt.get()->*Next) == nullptr);
        AIAdmissionAttempt* tail = attempt.get();
        if (_tail == nullptr) {
            _head = std::move(attempt);
        } else {
            _tail->*Next = std::move(attempt);
        }
        _tail = tail;
    }

    std::shared_ptr<AIAdmissionAttempt> pop_front() noexcept {
        DCHECK(_head != nullptr);
        auto attempt = std::move(_head);
        _head = std::move(attempt.get()->*Next);
        if (_head == nullptr) {
            _tail = nullptr;
        }
        return attempt;
    }

    void clear() noexcept {
        while (!empty()) {
            pop_front();
        }
    }

private:
    std::shared_ptr<AIAdmissionAttempt> _head;
    AIAdmissionAttempt* _tail = nullptr;
};

using CancelAttemptChain = AttemptChain<&AIAdmissionAttempt::cancel_next>;
using FailureDeliveryChain = AttemptChain<&AIAdmissionAttempt::delivery_next>;

struct TimedAttempt {
    int64_t when_ns;
    uint64_t registration_id;
    uint64_t generation;
    bool rate_wait;
};

struct LaterTimedAttempt {
    bool operator()(const TimedAttempt& lhs, const TimedAttempt& rhs) const {
        return std::pair(lhs.when_ns, lhs.registration_id) > std::pair(rhs.when_ns, rhs.registration_id);
    }
};

using TimedAttemptHeap = std::priority_queue<TimedAttempt, std::vector<TimedAttempt>, LaterTimedAttempt>;

void clear_callback_in_physical_scope(const AIMemoryContext& memory, AIAdmissionCallback* callback) noexcept {
    run_in_physical_scope(memory, [&] { AIAdmissionCallback().swap(*callback); });
}

void deliver_failures_noexcept(FailureDeliveryChain* deliveries) noexcept {
    while (!deliveries->empty()) {
        auto attempt = deliveries->pop_front();
        const AIMemoryContext memory = attempt->request.memory;
        AIAdmissionCallback callback;
        run_in_physical_scope(memory, [&] { callback = std::move(attempt->callback); });
        try {
            callback(AIAdmissionResult{AIAdmissionFailure{attempt->failure_reason}});
        } catch (...) {
            LOG(WARNING) << "AI admission failure callback threw an exception";
        }
        clear_callback_in_physical_scope(memory, &callback);
        attempt.reset();
    }
}

struct GrantDelivery {
    AIAdmissionCallback callback;
    AIAdmissionGrant grant;
    AIMemoryContext memory;
};

void deliver_grant_noexcept(std::optional<GrantDelivery>* grant_delivery) noexcept {
    if (!grant_delivery->has_value()) {
        return;
    }
    try {
        grant_delivery->value().callback(AIAdmissionResult{std::move(grant_delivery->value().grant)});
    } catch (...) {
        LOG(WARNING) << "AI admission grant callback threw an exception";
    }
    const AIMemoryContext memory = grant_delivery->value().memory;
    run_in_physical_scope(memory, [&] { grant_delivery->reset(); });
}

} // namespace

class AIAdmissionControllerCore : public std::enable_shared_from_this<AIAdmissionControllerCore> {
public:
    AIAdmissionControllerCore(const AIClock* clock, AIControlScheduler* scheduler, const AIAdmissionLimitSource* limits,
                              int64_t completion_capacity)
            : _clock(clock),
              _scheduler(scheduler),
              _limits(limits),
              _completion_capacity(std::max<int64_t>(0, completion_capacity)),
              _rate_limiter(clock, limits) {}

    StatusOr<AIAdmissionTicket> enqueue(const AIAdmissionRequest& request, AIAdmissionCallback&& callback) {
        if (!callback) {
            return Status::InvalidArgument("AI admission callback is empty");
        }
        const AIMemoryContext memory = request.memory;
        SCOPED_CLEANUP({ clear_callback_in_physical_scope(memory, &callback); });
        const AILifecycleObservation lifecycle =
                observe_ai_lifecycle(request.lifecycle, request.request_deadline_ns, _clock->monotonic_now_ns());
        if (lifecycle.state == AILifecycleState::CANCELLED) {
            return Status::Cancelled("AI admission Query lifecycle is unavailable or cancelled");
        }
        if (lifecycle.state == AILifecycleState::DEADLINE_EXCEEDED) {
            return Status::TimedOut("AI admission request deadline exceeded");
        }
        std::shared_ptr<AIAdmissionAttempt> attempt;
        try {
            run_in_physical_scope(memory, [&] {
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_attempt_create");
                attempt = ai_allocate_shared<AIAdmissionAttempt>(memory);
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_request_copy");
                attempt->request = request;
                attempt->core = weak_from_this();
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_callback_transfer");
                attempt->callback = std::move(callback);
                AIAdmissionCallback().swap(callback);
            });
        } catch (...) {
            attempt.reset();
            return Status::MemoryLimitExceeded("failed to allocate AI admission registration");
        }

        StatusSnapshot progress_status;
        FailureDeliveryChain failures;
        try {
            run_in_physical_scope(memory, [&] {
                std::lock_guard lock(_mutex);
                if (_shutdown) {
                    const Status status = Status::ServiceUnavailable("AI admission controller is shut down");
                    progress_status.capture(status);
                    return;
                }
                attempt->registration_id = ++_next_registration_id;
                bool rollback_registration = true;
                SCOPED_CLEANUP({
                    if (rollback_registration) {
                        _rollback_registration_locked(attempt);
                    }
                });
                auto bucket_it = _bucket_states.find(request.rate_limit_key);
                if (bucket_it == _bucket_states.end()) {
                    TEST_SYNC_POINT("AIAdmissionController::enqueue:before_bucket_state_create");
                    auto bucket_state = std::make_shared<AIAdmissionBucketState>(request.rate_limit_key);
                    TEST_SYNC_POINT("AIAdmissionController::enqueue:before_bucket_state_emplace");
                    bucket_it = _bucket_states.emplace(bucket_state->key, bucket_state).first;
                }
                attempt->bucket_state = bucket_it->second;
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_attempt_index_emplace");
                _attempts.emplace(attempt->registration_id, attempt);
                ++attempt->bucket_state->registrations;
                attempt->registration_counted = true;
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_query_index_workgroup_emplace");
                auto [workgroup_it, inserted_workgroup] = _query_attempts.try_emplace(request.workgroup_key);
                (void)inserted_workgroup;
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_query_index_query_emplace");
                auto [query_it, inserted_query] = workgroup_it->second.try_emplace(request.query_id);
                (void)inserted_query;
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_query_index_registration_emplace");
                query_it->second.emplace(attempt->registration_id);
                TEST_SYNC_POINT("AIAdmissionController::enqueue:before_deadline_emplace");
                attempt->armed_deadline_ns = lifecycle.effective_deadline_ns;
                attempt->deadline_generation = 1;
                _deadline_heap.push(TimedAttempt{attempt->armed_deadline_ns, attempt->registration_id,
                                                 attempt->deadline_generation, false});
                _enqueue_attempt_locked(attempt, true);
                {
                    const Status status = _request_pump_locked();
                    progress_status.capture(status);
                    if (!status.ok()) {
                        _rollback_registration_locked(attempt);
                        rollback_registration = false;
                        _handle_progress_failure_locked(status, &failures);
                    }
                }
                rollback_registration = false;
            });
        } catch (const std::bad_alloc&) {
            attempt.reset();
            return Status::MemoryLimitExceeded("failed to allocate AI admission registration");
        }
        deliver_failures_noexcept(&failures);
        if (!progress_status.ok()) {
            attempt.reset();
            return progress_status.materialize();
        }
        return AIAdmissionTicket(attempt);
    }

    void cancel_attempt(const std::shared_ptr<AIAdmissionAttempt>& attempt) {
        const AIMemoryContext memory = attempt->request.memory;
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            if (!attempt->done && !attempt->cancelled) {
                attempt->cancelled = true;
                _cancel_queue.append(attempt);
                _request_pump_or_fail_locked(&failures);
            }
        });
        deliver_failures_noexcept(&failures);
    }

    void cancel_query(const AIWorkGroupKey& workgroup_key, const UniqueId& query_id) {
        AIMemoryContext memory;
        {
            std::lock_guard lock(_mutex);
            auto wg_it = _query_attempts.find(workgroup_key);
            if (wg_it == _query_attempts.end()) {
                return;
            }
            auto query_it = wg_it->second.find(query_id);
            if (query_it == wg_it->second.end()) {
                return;
            }
            for (uint64_t registration_id : query_it->second) {
                auto attempt_it = _attempts.find(registration_id);
                if (attempt_it != _attempts.end()) {
                    memory = attempt_it->second->request.memory;
                    break;
                }
            }
        }
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            auto wg_it = _query_attempts.find(workgroup_key);
            if (wg_it == _query_attempts.end()) {
                return;
            }
            auto query_it = wg_it->second.find(query_id);
            if (query_it == wg_it->second.end()) {
                return;
            }
            for (uint64_t registration_id : query_it->second) {
                auto attempt_it = _attempts.find(registration_id);
                if (attempt_it != _attempts.end() && !attempt_it->second->done && !attempt_it->second->cancelled) {
                    attempt_it->second->cancelled = true;
                    _cancel_queue.append(attempt_it->second);
                }
            }
            _request_pump_or_fail_locked(&failures);
        });
        deliver_failures_noexcept(&failures);
    }

    void extend_bucket_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns) {
        FailureDeliveryChain failures;
        {
            std::lock_guard lock(_mutex);
            if (_shutdown) {
                return;
            }
            _rate_limiter.extend_cooldown(key, eligible_at_ns);
            _request_pump_or_fail_locked(&failures);
        }
        deliver_failures_noexcept(&failures);
    }

    void notify_limits_changed() {
        FailureDeliveryChain failures;
        {
            std::lock_guard lock(_mutex);
            if (_shutdown) {
                return;
            }
            _reactivate_all_rate_waiters = true;
            _request_pump_or_fail_locked(&failures);
        }
        deliver_failures_noexcept(&failures);
    }

    void shutdown() {
        FailureDeliveryChain failures;
        {
            std::lock_guard lock(_mutex);
            if (_shutdown) {
                return;
            }
            _shutdown = true;
            _cancel_timer_locked();
            _request_pump_or_fail_locked(&failures, true);
        }
        deliver_failures_noexcept(&failures);
    }

    int64_t inflight() const {
        std::lock_guard lock(_mutex);
        return _inflight;
    }

    int64_t completion_in_use() const {
        std::lock_guard lock(_mutex);
        return _completion_in_use;
    }

    uint64_t attempt_count() const {
        std::lock_guard lock(_mutex);
        return _attempts.size();
    }

    uint64_t bucket_state_count() const {
        std::lock_guard lock(_mutex);
        return _bucket_states.size();
    }

    int64_t bucket_registrations(const AIRateLimitKey& key) const {
        std::lock_guard lock(_mutex);
        auto it = _bucket_states.find(key);
        return it == _bucket_states.end() ? 0 : it->second->registrations;
    }

    int64_t bucket_inflight(const AIRateLimitKey& key) const {
        std::lock_guard lock(_mutex);
        auto it = _bucket_states.find(key);
        return it == _bucket_states.end() ? 0 : it->second->inflight;
    }

    int64_t bucket_owners(const AIRateLimitKey& key) const {
        std::lock_guard lock(_mutex);
        auto it = _bucket_states.find(key);
        return it == _bucket_states.end() ? 0 : it->second->owners;
    }

    int64_t unresolved_completion_count(const AIRateLimitKey& key) const {
        std::lock_guard lock(_mutex);
        auto it = _bucket_states.find(key);
        return it == _bucket_states.end() ? 0 : it->second->unresolved;
    }

    int64_t rate_pins(const AIRateLimitKey& key) const {
        std::lock_guard lock(_mutex);
        return _rate_limiter._pins_for_test(key);
    }

    uint64_t scheduling_steps() const {
        std::lock_guard lock(_mutex);
        return _scheduling_steps;
    }

    uint64_t rate_waiter_count() const {
        std::lock_guard lock(_mutex);
        uint64_t count = 0;
        for (const auto& [_, bucket_state] : _bucket_states) {
            count += bucket_state->rate_waiters.size();
        }
        return count;
    }

    void commit_grant(const std::shared_ptr<AIAdmissionBucketState>& bucket_state, AITokenReservation* token,
                      const AIMemoryContext& memory) noexcept {
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            if (_shutdown) {
                token->_commit_without_refill();
            } else {
                token->commit();
                if (!bucket_state->indefinite_rate_waiters.empty()) {
                    bucket_state->reactivate_indefinite_rate_waiters = true;
                    _request_pump_or_fail_locked(&failures);
                }
            }
        });
        deliver_failures_noexcept(&failures);
    }

    void rollback_grant(const AIWorkGroupKey& workgroup_key,
                        const std::shared_ptr<AIAdmissionBucketState>& bucket_state, AITokenReservation* token,
                        const AIMemoryContext& memory) noexcept {
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            if (_shutdown) {
                token->_rollback_without_refill();
            } else {
                token->_reset();
                if (!bucket_state->rate_waiters.empty()) {
                    bucket_state->reactivate_rate_waiters = true;
                }
            }
            DCHECK_GT(_inflight, 0);
            DCHECK_GT(_completion_in_use, 0);
            DCHECK_GT(bucket_state->inflight, 0);
            DCHECK_GT(bucket_state->owners, 0);
            --_inflight;
            --_completion_in_use;
            --bucket_state->inflight;
            --bucket_state->owners;
            _release_active_workgroup_grant_locked(workgroup_key);
            _request_pump_or_fail_locked(&failures);
            _maybe_erase_bucket_state_locked(bucket_state);
        });
        deliver_failures_noexcept(&failures);
    }

    AIBucketResolutionGuard complete_transport(const std::shared_ptr<AIAdmissionBucketState>& bucket_state,
                                               const AIWorkGroupKey& workgroup_key, AITokenReservation* token,
                                               const AIMemoryContext& memory) noexcept {
        FailureDeliveryChain failures;
        AIBucketResolutionGuard guard;
        run_in_physical_scope(memory, [&] {
            DCHECK(token != nullptr);
            TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
            guard = AIBucketResolutionGuard(shared_from_this(), bucket_state, std::move(*token), memory);
            std::lock_guard lock(_mutex);
            DCHECK_GT(_inflight, 0);
            DCHECK_GT(bucket_state->inflight, 0);
            DCHECK_GT(bucket_state->owners, 0);
            --_inflight;
            --bucket_state->inflight;
            ++bucket_state->unresolved;
            _release_active_workgroup_grant_locked(workgroup_key);
            _request_pump_or_fail_locked(&failures);
        });
        deliver_failures_noexcept(&failures);
        return guard;
    }

    void resolve_bucket(const std::shared_ptr<AIAdmissionBucketState>& bucket_state, AITokenReservation* token,
                        std::optional<int64_t> cooldown_until_ns, const AIMemoryContext& memory) noexcept {
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            if (!_shutdown && cooldown_until_ns.has_value()) {
                token->_extend_cooldown(*cooldown_until_ns);
            }
            if (bucket_state->unresolved == 0) {
                return;
            }
            if (--bucket_state->unresolved == 0) {
                if (!_shutdown) {
                    bucket_state->reactivate_completion_waiters = true;
                    _request_pump_or_fail_locked(&failures);
                }
            }
        });
        deliver_failures_noexcept(&failures);
    }

    std::optional<int64_t> validate_cooldown(int64_t eligible_at_ns) noexcept {
        if (eligible_at_ns == std::numeric_limits<int64_t>::max()) {
            return std::nullopt;
        }
        std::lock_guard lock(_mutex);
        if (_shutdown || eligible_at_ns <= _clock->monotonic_now_ns()) {
            return std::nullopt;
        }
        return eligible_at_ns;
    }

    void release_completion_capacity(const std::shared_ptr<AIAdmissionBucketState>& bucket_state,
                                     const AIMemoryContext& memory) noexcept {
        FailureDeliveryChain failures;
        run_in_physical_scope(memory, [&] {
            std::lock_guard lock(_mutex);
            DCHECK_GT(_completion_in_use, 0);
            DCHECK_GT(bucket_state->owners, 0);
            --_completion_in_use;
            --bucket_state->owners;
            _request_pump_or_fail_locked(&failures);
            _maybe_erase_bucket_state_locked(bucket_state);
        });
        deliver_failures_noexcept(&failures);
    }

private:
    friend class AIAdmissionGrant;
    friend class AIBucketResolutionGuard;

    Status _request_pump_locked(bool shutdown_drain = false) {
        if (_pump_posted || (_shutdown && !shutdown_drain)) {
            return Status::OK();
        }
        auto core = shared_from_this();
        AIControlScheduler::Task task;
        try {
            TEST_SYNC_POINT("AIAdmissionController::_request_pump_locked:before_task_materialization");
            task = [core = std::move(core)]() { core->_pump(); };
        } catch (const std::bad_alloc&) {
            return Status::MemoryLimitExceeded("failed to materialize AI admission pump task");
        }
        Status status = _scheduler->post(std::move(task));
        if (status.ok()) {
            _pump_posted = true;
        }
        return status;
    }

    void _request_pump_or_fail_locked(FailureDeliveryChain* failures, bool shutdown_drain = false) noexcept {
        try {
            Status status = _request_pump_locked(shutdown_drain);
            if (!status.ok()) {
                _handle_progress_failure_locked(status, failures);
            }
        } catch (...) {
            _pump_posted = false;
            _drain_pending_locked(
                    _shutdown ? AIAdmissionFailureReason::SHUTDOWN : AIAdmissionFailureReason::LOCAL_RESOURCE,
                    failures);
        }
    }

    void _defer_attempt_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt, int64_t eligible_at_ns) {
        ++attempt->deferred_generation;
        _deferred_heap.push(
                TimedAttempt{eligible_at_ns, attempt->registration_id, attempt->deferred_generation, false});
    }

    void _defer_rate_wait_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt, int64_t eligible_at_ns) {
        DCHECK(attempt->bucket_state != nullptr);
        ++attempt->deferred_generation;
        _deferred_heap.push(TimedAttempt{eligible_at_ns, attempt->registration_id, attempt->deferred_generation, true});
        attempt->bucket_state->rate_waiters[attempt->registration_id] = attempt->deferred_generation;
        if (eligible_at_ns == std::numeric_limits<int64_t>::max()) {
            attempt->bucket_state->indefinite_rate_waiters[attempt->registration_id] = attempt->deferred_generation;
        }
    }

    void _add_workgroup_to_ring_locked(const AIWorkGroupKey& workgroup_key, WorkGroupQueue* workgroup,
                                       bool latency_sensitive_newcomer) {
        if (workgroup->in_ring || workgroup->query_ring.empty()) {
            return;
        }
        workgroup->in_ring = true;
        TEST_SYNC_POINT("AIAdmissionController::_add_workgroup_to_ring_locked:before_workgroup_ring_emplace");
        if (latency_sensitive_newcomer && _workgroup_ring.size() == 1 && _last_selected_workgroup.has_value() &&
            _workgroup_ring.front() == *_last_selected_workgroup) {
            _workgroup_ring.emplace_front(workgroup_key);
        } else {
            _workgroup_ring.emplace_back(workgroup_key);
        }
    }

    void _add_query_to_ring_locked(const AIWorkGroupKey& workgroup_key, const UniqueId& query_id,
                                   WorkGroupQueue* workgroup, QueryQueue* query, bool latency_sensitive_newcomer) {
        if (query->in_ring || query->blocker != nullptr || query->attempts.empty()) {
            return;
        }
        query->in_ring = true;
        auto last_query_it = _last_selected_queries.find(workgroup_key);
        TEST_SYNC_POINT("AIAdmissionController::_add_query_to_ring_locked:before_query_ring_emplace");
        if (latency_sensitive_newcomer && workgroup->query_ring.size() == 1 &&
            last_query_it != _last_selected_queries.end() && workgroup->query_ring.front() == last_query_it->second) {
            workgroup->query_ring.emplace_front(query_id);
        } else {
            workgroup->query_ring.emplace_back(query_id);
        }
        _add_workgroup_to_ring_locked(workgroup_key, workgroup, latency_sensitive_newcomer);
    }

    void _enqueue_attempt_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt, bool latency_sensitive_newcomer) {
        const AIWorkGroupKey workgroup_key = attempt->request.workgroup_key;
        const UniqueId query_id = attempt->request.query_id;
        TEST_SYNC_POINT("AIAdmissionController::_enqueue_attempt_locked:before_workgroup_emplace");
        auto [wg_it, inserted_workgroup] = _workgroups.try_emplace(workgroup_key);
        (void)inserted_workgroup;
        auto& workgroup = wg_it->second;
        TEST_SYNC_POINT("AIAdmissionController::_enqueue_attempt_locked:before_query_emplace");
        auto [query_it, inserted_query] = workgroup.queries.try_emplace(query_id);
        (void)inserted_query;
        auto& query = query_it->second;
        TEST_SYNC_POINT("AIAdmissionController::_enqueue_attempt_locked:before_attempt_emplace");
        query.attempts.emplace_back(attempt);
        _add_query_to_ring_locked(workgroup_key, query_id, &workgroup, &query, latency_sensitive_newcomer);
    }

    static bool _bucket_state_unused(const std::shared_ptr<AIAdmissionBucketState>& bucket_state) noexcept {
        return bucket_state != nullptr && bucket_state->registrations == 0 && bucket_state->owners == 0 &&
               bucket_state->inflight == 0 && bucket_state->unresolved == 0 && bucket_state->rate_waiters.empty() &&
               bucket_state->indefinite_rate_waiters.empty() && bucket_state->completion_waiters.empty();
    }

    void _maybe_erase_bucket_state_locked(const std::shared_ptr<AIAdmissionBucketState>& bucket_state) noexcept {
        if (!_bucket_state_unused(bucket_state)) {
            return;
        }
        auto it = _bucket_states.find(bucket_state->key);
        if (it != _bucket_states.end() && it->second == bucket_state) {
            _bucket_states.erase(it);
        }
    }

    void _release_registration_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt) noexcept {
        if (!attempt->registration_counted) {
            _maybe_erase_bucket_state_locked(attempt->bucket_state);
            return;
        }
        DCHECK(attempt->bucket_state != nullptr);
        DCHECK_GT(attempt->bucket_state->registrations, 0);
        --attempt->bucket_state->registrations;
        attempt->registration_counted = false;
        _maybe_erase_bucket_state_locked(attempt->bucket_state);
    }

    void _rollback_registration_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt) {
        attempt->done = true;
        _attempts.erase(attempt->registration_id);
        _remove_query_registration_locked(*attempt);
        _release_registration_locked(attempt);

        auto workgroup_it = _workgroups.find(attempt->request.workgroup_key);
        if (workgroup_it == _workgroups.end()) {
            return;
        }
        auto& workgroup = workgroup_it->second;
        auto query_it = workgroup.queries.find(attempt->request.query_id);
        if (query_it == workgroup.queries.end()) {
            if (workgroup.queries.empty() && workgroup.query_ring.empty()) {
                if (workgroup.in_ring) {
                    auto ring_it =
                            std::find(_workgroup_ring.begin(), _workgroup_ring.end(), attempt->request.workgroup_key);
                    if (ring_it != _workgroup_ring.end()) {
                        _workgroup_ring.erase(ring_it);
                    }
                }
                _workgroups.erase(workgroup_it);
                _cleanup_workgroup_history_locked(attempt->request.workgroup_key);
            }
            return;
        }
        auto& query = query_it->second;
        auto queued_it = std::find_if(query.attempts.begin(), query.attempts.end(), [&](const auto& queued) {
            return queued->registration_id == attempt->registration_id;
        });
        if (queued_it != query.attempts.end()) {
            query.attempts.erase(queued_it);
        }

        if (query.attempts.empty() && query.blocker == nullptr) {
            if (query.in_ring) {
                auto ring_it =
                        std::find(workgroup.query_ring.begin(), workgroup.query_ring.end(), attempt->request.query_id);
                if (ring_it != workgroup.query_ring.end()) {
                    workgroup.query_ring.erase(ring_it);
                }
            }
            workgroup.queries.erase(query_it);
        }
        if (workgroup.query_ring.empty() && workgroup.in_ring) {
            auto ring_it = std::find(_workgroup_ring.begin(), _workgroup_ring.end(), attempt->request.workgroup_key);
            if (ring_it != _workgroup_ring.end()) {
                _workgroup_ring.erase(ring_it);
            }
            workgroup.in_ring = false;
        }
        if (workgroup.queries.empty()) {
            _workgroups.erase(workgroup_it);
            _cleanup_workgroup_history_locked(attempt->request.workgroup_key);
        }
    }

    QueryQueue* _find_query_locked(const AIAdmissionAttempt& attempt, WorkGroupQueue** workgroup = nullptr) {
        auto wg_it = _workgroups.find(attempt.request.workgroup_key);
        if (wg_it == _workgroups.end()) {
            return nullptr;
        }
        auto query_it = wg_it->second.queries.find(attempt.request.query_id);
        if (query_it == wg_it->second.queries.end()) {
            return nullptr;
        }
        if (workgroup != nullptr) {
            *workgroup = &wg_it->second;
        }
        return &query_it->second;
    }

    void _suspend_query_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt) {
        QueryQueue* query = _find_query_locked(*attempt);
        DCHECK(query != nullptr);
        DCHECK(!query->in_ring);
        DCHECK(query->blocker == nullptr);
        query->blocker = attempt;
    }

    void _restore_query_blocker_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt) {
        WorkGroupQueue* workgroup = nullptr;
        QueryQueue* query = _find_query_locked(*attempt, &workgroup);
        if (query == nullptr || query->blocker == nullptr ||
            query->blocker->registration_id != attempt->registration_id) {
            return;
        }
        query->blocker.reset();
        if (!attempt->done) {
            query->attempts.emplace_front(attempt);
        }
        _add_query_to_ring_locked(attempt->request.workgroup_key, attempt->request.query_id, workgroup, query, false);
    }

    void _finish_query_blocker_locked(const AIAdmissionAttempt& attempt) {
        auto wg_it = _workgroups.find(attempt.request.workgroup_key);
        if (wg_it == _workgroups.end()) {
            return;
        }
        auto query_it = wg_it->second.queries.find(attempt.request.query_id);
        if (query_it == wg_it->second.queries.end() || query_it->second.blocker == nullptr ||
            query_it->second.blocker->registration_id != attempt.registration_id) {
            return;
        }
        query_it->second.blocker.reset();
        if (!query_it->second.attempts.empty()) {
            _add_query_to_ring_locked(attempt.request.workgroup_key, attempt.request.query_id, &wg_it->second,
                                      &query_it->second, false);
            return;
        }
        wg_it->second.queries.erase(query_it);
        if (wg_it->second.queries.empty() && !wg_it->second.in_ring) {
            _workgroups.erase(wg_it);
            _cleanup_workgroup_history_locked(attempt.request.workgroup_key);
        }
    }

    void _remove_query_registration_locked(const AIAdmissionAttempt& attempt) {
        auto wg_it = _query_attempts.find(attempt.request.workgroup_key);
        if (wg_it == _query_attempts.end()) {
            return;
        }
        auto query_it = wg_it->second.find(attempt.request.query_id);
        if (query_it == wg_it->second.end()) {
            if (wg_it->second.empty()) {
                _query_attempts.erase(wg_it);
            }
            return;
        }
        query_it->second.erase(attempt.registration_id);
        if (query_it->second.empty()) {
            wg_it->second.erase(query_it);
            if (wg_it->second.empty()) {
                _query_attempts.erase(wg_it);
            }
        }
    }

    void _finish_failure_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt, AIAdmissionFailureReason reason,
                                FailureDeliveryChain* deliveries) {
        if (attempt->done) {
            return;
        }
        _prepare_failure_delivery_locked(attempt, reason, deliveries);
        attempt->done = true;
        _erase_rate_waiter_locked(*attempt);
        auto& completion_waiters = attempt->bucket_state->completion_waiters;
        completion_waiters.erase(
                std::remove(completion_waiters.begin(), completion_waiters.end(), attempt->registration_id),
                completion_waiters.end());
        _attempts.erase(attempt->registration_id);
        _remove_query_registration_locked(*attempt);
        _release_registration_locked(attempt);
        _finish_query_blocker_locked(*attempt);
    }

    void _finish_admission_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt) {
        DCHECK(!attempt->done);
        DCHECK(attempt->registration_counted);
        DCHECK(attempt->bucket_state != nullptr);
        DCHECK_GT(attempt->bucket_state->registrations, 0);
        attempt->done = true;
        _erase_rate_waiter_locked(*attempt);
        _attempts.erase(attempt->registration_id);
        _remove_query_registration_locked(*attempt);
        --attempt->bucket_state->registrations;
        attempt->registration_counted = false;
        ++attempt->bucket_state->inflight;
        ++attempt->bucket_state->owners;
    }

    void _drain_cancel_queue_locked(FailureDeliveryChain* deliveries) {
        while (!_cancel_queue.empty()) {
            auto attempt = _cancel_queue.pop_front();
            if (!attempt->done) {
                _finish_failure_locked(attempt, AIAdmissionFailureReason::CANCELLED, deliveries);
            }
        }
    }

    std::shared_ptr<AIAdmissionAttempt> _take_due_deadline_locked(int64_t now_ns) {
        while (!_deadline_heap.empty() && _deadline_heap.top().when_ns <= now_ns) {
            TimedAttempt timed = _deadline_heap.top();
            _deadline_heap.pop();
            auto attempt_it = _attempts.find(timed.registration_id);
            if (attempt_it != _attempts.end() && !attempt_it->second->done &&
                attempt_it->second->deadline_generation == timed.generation &&
                attempt_it->second->armed_deadline_ns == timed.when_ns) {
                auto attempt = attempt_it->second;
                ++attempt->deadline_generation;
                attempt->armed_deadline_ns = 0;
                return attempt;
            }
        }
        return nullptr;
    }

    void _recheck_due_deadlines(FailureDeliveryChain* deliveries) {
        for (;;) {
            const int64_t now_ns = _clock->monotonic_now_ns();
            std::shared_ptr<AIAdmissionAttempt> attempt;
            {
                std::lock_guard lock(_mutex);
                if (_shutdown) {
                    return;
                }
                attempt = _take_due_deadline_locked(now_ns);
            }
            if (attempt == nullptr) {
                return;
            }

            // The Query-owned probe may lock external state or execute user
            // code. It must never run while the controller mutex is held.
            AILifecycleObservation lifecycle =
                    observe_ai_lifecycle(attempt->request.lifecycle, attempt->request.request_deadline_ns, now_ns);
            const int64_t after_probe_now_ns = _clock->monotonic_now_ns();
            if (lifecycle.state == AILifecycleState::ACTIVE && after_probe_now_ns >= lifecycle.effective_deadline_ns) {
                lifecycle.state = AILifecycleState::DEADLINE_EXCEEDED;
            }

            run_in_physical_scope(attempt->request.memory, [&] {
                std::lock_guard lock(_mutex);
                if (_shutdown || attempt->done) {
                    return;
                }
                if (attempt->cancelled || lifecycle.state == AILifecycleState::CANCELLED) {
                    _finish_failure_locked(attempt, AIAdmissionFailureReason::CANCELLED, deliveries);
                    return;
                }
                if (lifecycle.state == AILifecycleState::DEADLINE_EXCEEDED) {
                    _finish_failure_locked(attempt, AIAdmissionFailureReason::DEADLINE_EXCEEDED, deliveries);
                    return;
                }
                attempt->armed_deadline_ns = lifecycle.effective_deadline_ns;
                _deadline_heap.push(TimedAttempt{attempt->armed_deadline_ns, attempt->registration_id,
                                                 attempt->deadline_generation, false});
            });
        }
    }

    void _activate_deferred_locked(int64_t now_ns) {
        while (!_deferred_heap.empty() && _deferred_heap.top().when_ns <= now_ns) {
            TimedAttempt timed = _deferred_heap.top();
            _deferred_heap.pop();
            auto attempt_it = _attempts.find(timed.registration_id);
            if (attempt_it != _attempts.end() && !attempt_it->second->done &&
                attempt_it->second->deferred_generation == timed.generation) {
                auto attempt = attempt_it->second;
                if (timed.rate_wait) {
                    _erase_rate_waiter_locked(*attempt);
                }
                ++attempt->deferred_generation;
                _restore_query_blocker_locked(attempt);
            }
        }
    }

    void _reactivate_rate_waiters_locked(const std::shared_ptr<AIAdmissionBucketState>& bucket_state) {
        std::map<uint64_t, uint64_t> waiters;
        waiters.swap(bucket_state->rate_waiters);
        bucket_state->indefinite_rate_waiters.clear();
        for (const auto& [registration_id, generation] : waiters) {
            auto attempt_it = _attempts.find(registration_id);
            if (attempt_it == _attempts.end() || attempt_it->second->done ||
                attempt_it->second->deferred_generation != generation) {
                continue;
            }
            auto attempt = attempt_it->second;
            ++attempt->deferred_generation;
            _restore_query_blocker_locked(attempt);
        }
    }

    void _reactivate_indefinite_rate_waiters_locked(const std::shared_ptr<AIAdmissionBucketState>& bucket_state) {
        std::map<uint64_t, uint64_t> waiters;
        waiters.swap(bucket_state->indefinite_rate_waiters);
        for (const auto& [registration_id, generation] : waiters) {
            bucket_state->rate_waiters.erase(registration_id);
            auto attempt_it = _attempts.find(registration_id);
            if (attempt_it == _attempts.end() || attempt_it->second->done ||
                attempt_it->second->deferred_generation != generation) {
                continue;
            }
            auto attempt = attempt_it->second;
            ++attempt->deferred_generation;
            _restore_query_blocker_locked(attempt);
        }
    }

    void _erase_rate_waiter_locked(const AIAdmissionAttempt& attempt) {
        if (attempt.bucket_state == nullptr) {
            return;
        }
        attempt.bucket_state->rate_waiters.erase(attempt.registration_id);
        attempt.bucket_state->indefinite_rate_waiters.erase(attempt.registration_id);
    }

    void _reactivate_completion_waiters_locked(const std::shared_ptr<AIAdmissionBucketState>& bucket_state) {
        std::deque<uint64_t> waiters;
        waiters.swap(bucket_state->completion_waiters);
        for (uint64_t registration_id : waiters) {
            auto attempt_it = _attempts.find(registration_id);
            if (attempt_it != _attempts.end() && !attempt_it->second->done) {
                _restore_query_blocker_locked(attempt_it->second);
            }
        }
    }

    void _activate_dirty_waiters_locked() {
        const bool reactivate_all = std::exchange(_reactivate_all_rate_waiters, false);
        for (auto it = _bucket_states.begin(); it != _bucket_states.end();) {
            auto& bucket_state = it->second;
            if (reactivate_all || bucket_state->reactivate_rate_waiters) {
                bucket_state->reactivate_rate_waiters = false;
                bucket_state->reactivate_indefinite_rate_waiters = false;
                _reactivate_rate_waiters_locked(bucket_state);
            } else if (bucket_state->reactivate_indefinite_rate_waiters) {
                bucket_state->reactivate_indefinite_rate_waiters = false;
                _reactivate_indefinite_rate_waiters_locked(bucket_state);
            }
            if (bucket_state->reactivate_completion_waiters) {
                bucket_state->reactivate_completion_waiters = false;
                _reactivate_completion_waiters_locked(bucket_state);
            }
            if (_bucket_state_unused(bucket_state)) {
                it = _bucket_states.erase(it);
            } else {
                ++it;
            }
        }
    }

    void _cleanup_workgroup_history_locked(const AIWorkGroupKey& workgroup_key) {
        if (!_workgroups.contains(workgroup_key) && !_active_workgroup_grants.contains(workgroup_key)) {
            _last_selected_queries.erase(workgroup_key);
            if (_last_selected_workgroup == workgroup_key) {
                _last_selected_workgroup.reset();
            }
        }
    }

    void _release_active_workgroup_grant_locked(const AIWorkGroupKey& workgroup_key) {
        auto active_it = _active_workgroup_grants.find(workgroup_key);
        DCHECK(active_it != _active_workgroup_grants.end());
        DCHECK_GT(active_it->second, 0);
        if (--active_it->second == 0) {
            _active_workgroup_grants.erase(active_it);
            _cleanup_workgroup_history_locked(workgroup_key);
        }
    }

    std::optional<GrantDelivery> _try_admit_one_locked(int64_t now_ns, FailureDeliveryChain* failures) {
        size_t remaining_workgroups = _workgroup_ring.size();
        while (remaining_workgroups-- > 0 && !_workgroup_ring.empty()) {
            const AIWorkGroupKey workgroup_key = _workgroup_ring.front();
            _workgroup_ring.pop_front();
            auto wg_it = _workgroups.find(workgroup_key);
            if (wg_it == _workgroups.end()) {
                continue;
            }
            auto& workgroup = wg_it->second;
            workgroup.in_ring = false;
            size_t remaining_queries = workgroup.query_ring.size();
            while (remaining_queries-- > 0 && !workgroup.query_ring.empty()) {
                const UniqueId query_id = workgroup.query_ring.front();
                workgroup.query_ring.pop_front();
                auto query_it = workgroup.queries.find(query_id);
                if (query_it == workgroup.queries.end()) {
                    continue;
                }
                auto& query = query_it->second;
                query.in_ring = false;
                auto& queue = query.attempts;
                while (!queue.empty() && (queue.front()->done || queue.front()->cancelled)) {
                    auto skipped = std::move(queue.front());
                    queue.pop_front();
                    if (!skipped->done) {
                        _finish_failure_locked(skipped, AIAdmissionFailureReason::CANCELLED, failures);
                    }
                }
                if (queue.empty()) {
                    workgroup.queries.erase(query_it);
                    continue;
                }

                auto attempt = std::move(queue.front());
                queue.pop_front();
                ++_scheduling_steps;

                std::optional<AITokenReservation> token;
                if (attempt->request.eligible_at_ns > now_ns) {
                    _suspend_query_locked(attempt);
                    _defer_attempt_locked(attempt, attempt->request.eligible_at_ns);
                } else if (attempt->bucket_state->unresolved > 0) {
                    _suspend_query_locked(attempt);
                    attempt->bucket_state->completion_waiters.emplace_back(attempt->registration_id);
                } else {
                    int64_t token_eligible_ns = std::numeric_limits<int64_t>::max();
                    token = _rate_limiter.try_reserve(attempt->bucket_state->key, &token_eligible_ns);
                    if (!token.has_value()) {
                        _suspend_query_locked(attempt);
                        _defer_rate_wait_locked(attempt, token_eligible_ns);
                    }
                }

                if (token.has_value()) {
                    if (!queue.empty()) {
                        _add_query_to_ring_locked(workgroup_key, query_id, &workgroup, &query, false);
                    } else {
                        workgroup.queries.erase(query_it);
                    }
                    if (!workgroup.query_ring.empty()) {
                        _add_workgroup_to_ring_locked(workgroup_key, &workgroup, false);
                    } else if (workgroup.queries.empty()) {
                        _workgroups.erase(wg_it);
                    }

                    // Prepare every allocation before making the attempt terminal. The local reservation rolls back
                    // automatically and the pump drains the still-registered attempt if any preparation fails.
                    auto core = shared_from_this();
                    auto bucket_state = attempt->bucket_state;
                    auto memory = attempt->request.memory;
                    AIAdmissionCallback callback;
                    auto [history_it, history_inserted] = _last_selected_queries.try_emplace(workgroup_key);
                    bool history_committed = false;
                    SCOPED_CLEANUP({
                        if (!history_committed && history_inserted) {
                            _last_selected_queries.erase(history_it);
                        }
                    });
                    TEST_SYNC_POINT(
                            "AIAdmissionController::_try_admit_one_locked:before_active_workgroup_grant_emplace");
                    auto [active_it, active_inserted] = _active_workgroup_grants.try_emplace(workgroup_key, 0);
                    (void)active_inserted;

                    // From here through construction of the delivery, every operation is non-allocating or a
                    // noexcept move. Register the counters before constructing the bound grant so its destructor can
                    // always release resources against a complete accounting record.
                    history_it->second = query_id;
                    _last_selected_workgroup = workgroup_key;
                    _finish_admission_locked(attempt);
                    ++_inflight;
                    ++_completion_in_use;
                    ++active_it->second;
                    AIAdmissionGrant grant;
                    run_in_physical_scope(memory, [&] {
                        TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
                        grant = AIAdmissionGrant(std::move(core), std::move(*token), std::move(bucket_state),
                                                 workgroup_key, query_id, attempt->request.attempt_id, memory);
                        callback = std::move(attempt->callback);
                    });
                    history_committed = true;
                    return GrantDelivery{std::move(callback), std::move(grant), std::move(memory)};
                }
            }

            if (!workgroup.query_ring.empty()) {
                _add_workgroup_to_ring_locked(workgroup_key, &workgroup, false);
            } else if (workgroup.queries.empty()) {
                _workgroups.erase(wg_it);
                _cleanup_workgroup_history_locked(workgroup_key);
            }
        }
        return std::nullopt;
    }

    int64_t _next_wakeup_locked() {
        while (!_deadline_heap.empty()) {
            auto attempt_it = _attempts.find(_deadline_heap.top().registration_id);
            if (attempt_it != _attempts.end() && !attempt_it->second->done &&
                attempt_it->second->deadline_generation == _deadline_heap.top().generation &&
                attempt_it->second->armed_deadline_ns == _deadline_heap.top().when_ns) {
                break;
            }
            _deadline_heap.pop();
        }
        while (!_deferred_heap.empty()) {
            auto attempt_it = _attempts.find(_deferred_heap.top().registration_id);
            if (attempt_it != _attempts.end() && !attempt_it->second->done &&
                attempt_it->second->deferred_generation == _deferred_heap.top().generation) {
                break;
            }
            _deferred_heap.pop();
        }
        int64_t wakeup_ns = std::numeric_limits<int64_t>::max();
        if (!_deadline_heap.empty()) {
            wakeup_ns = std::min(wakeup_ns, _deadline_heap.top().when_ns);
        }
        if (!_deferred_heap.empty()) {
            wakeup_ns = std::min(wakeup_ns, _deferred_heap.top().when_ns);
        }
        return wakeup_ns;
    }

    void _cancel_timer_locked() noexcept {
        ++_timer_generation;
        if (_timer_id.has_value()) {
            const AIControlScheduler::TaskId timer_id = *_timer_id;
            _timer_id.reset();
            try {
                _scheduler->cancel(timer_id);
            } catch (...) {
                // The generation was invalidated before cancellation. A late task is therefore harmless.
            }
        }
    }

    Status _arm_timer_locked(int64_t wakeup_ns) {
        _cancel_timer_locked();
        if (wakeup_ns == std::numeric_limits<int64_t>::max()) {
            return Status::OK();
        }
        const uint64_t generation = _timer_generation;
        std::weak_ptr<AIAdmissionControllerCore> weak = weak_from_this();
        AIControlScheduler::Task task;
        try {
            TEST_SYNC_POINT("AIAdmissionController::_arm_timer_locked:before_task_materialization");
            task = [weak = std::move(weak), generation]() {
                auto core = weak.lock();
                if (core == nullptr) {
                    return;
                }
                FailureDeliveryChain failures;
                {
                    std::lock_guard lock(core->_mutex);
                    if (core->_shutdown || core->_timer_generation != generation) {
                        return;
                    }
                    core->_timer_id.reset();
                    ++core->_timer_generation;
                    core->_request_pump_or_fail_locked(&failures);
                }
                deliver_failures_noexcept(&failures);
            };
        } catch (const std::bad_alloc&) {
            return Status::MemoryLimitExceeded("failed to materialize AI admission timer task");
        }
        auto timer_id = _scheduler->schedule_at(wakeup_ns, std::move(task));
        if (!timer_id.ok()) {
            return timer_id.status();
        }
        _timer_id = std::move(timer_id).value();
        return Status::OK();
    }

    void _drain_pending_locked(AIAdmissionFailureReason reason, FailureDeliveryChain* deliveries) {
        for (auto& [_, attempt] : _attempts) {
            if (!attempt->done) {
                _prepare_failure_delivery_locked(attempt, reason, deliveries);
                attempt->done = true;
                _release_registration_locked(attempt);
            }
        }
        _attempts.clear();
        _query_attempts.clear();
        _cancel_queue.clear();
        _workgroup_ring.clear();
        _workgroups.clear();
        for (auto it = _last_selected_queries.begin(); it != _last_selected_queries.end();) {
            if (!_active_workgroup_grants.contains(it->first)) {
                it = _last_selected_queries.erase(it);
            } else {
                ++it;
            }
        }
        if (_last_selected_workgroup.has_value() && !_active_workgroup_grants.contains(*_last_selected_workgroup)) {
            _last_selected_workgroup.reset();
        }
        for (auto it = _bucket_states.begin(); it != _bucket_states.end();) {
            auto& bucket_state = it->second;
            bucket_state->rate_waiters.clear();
            bucket_state->indefinite_rate_waiters.clear();
            bucket_state->completion_waiters.clear();
            bucket_state->reactivate_rate_waiters = false;
            bucket_state->reactivate_indefinite_rate_waiters = false;
            bucket_state->reactivate_completion_waiters = false;
            if (bucket_state->registrations == 0 && bucket_state->owners == 0 && bucket_state->inflight == 0 &&
                bucket_state->unresolved == 0) {
                it = _bucket_states.erase(it);
            } else {
                ++it;
            }
        }
        _reactivate_all_rate_waiters = false;
        _deadline_heap = {};
        _deferred_heap = {};
        _pump_posted = false;
        _cancel_timer_locked();
    }

    void _prepare_failure_delivery_locked(const std::shared_ptr<AIAdmissionAttempt>& attempt,
                                          AIAdmissionFailureReason reason, FailureDeliveryChain* deliveries) noexcept {
        attempt->failure_reason = reason;
        deliveries->append(attempt);
    }

    void _handle_progress_failure_locked(const Status& status, FailureDeliveryChain* deliveries) {
        if (status.is_shutdown()) {
            _shutdown = true;
        }
        const bool shutting_down = _shutdown;
        _drain_pending_locked(
                shutting_down ? AIAdmissionFailureReason::SHUTDOWN : AIAdmissionFailureReason::LOCAL_RESOURCE,
                deliveries);
    }

    void _fail_closed_after_pump_exception(FailureDeliveryChain* failures) {
        std::lock_guard lock(_mutex);
        _pump_posted = false;
        _drain_pending_locked(_shutdown ? AIAdmissionFailureReason::SHUTDOWN : AIAdmissionFailureReason::LOCAL_RESOURCE,
                              failures);
    }

    void _pump() {
        FailureDeliveryChain failures;
        std::optional<GrantDelivery> grant_delivery;
        bool schedule_next = false;

        try {
            int64_t now_ns = 0;
            bool recheck_lifecycle = false;
            {
                std::lock_guard lock(_mutex);
                TEST_SYNC_POINT("AIAdmissionController::_pump:before_body");
                _pump_posted = false;
                _cancel_timer_locked();
                if (_shutdown) {
                    _drain_pending_locked(AIAdmissionFailureReason::SHUTDOWN, &failures);
                } else {
                    now_ns = _clock->monotonic_now_ns();
                    _drain_cancel_queue_locked(&failures);
                    recheck_lifecycle = true;
                }
            }

            if (recheck_lifecycle) {
                _recheck_due_deadlines(&failures);
                now_ns = _clock->monotonic_now_ns();
                std::lock_guard lock(_mutex);
                if (_shutdown) {
                    _drain_pending_locked(AIAdmissionFailureReason::SHUTDOWN, &failures);
                } else {
                    _activate_dirty_waiters_locked();
                    _activate_deferred_locked(now_ns);

                    const int64_t inflight_cap = std::max<int64_t>(0, _limits->max_inflight());
                    if (_inflight < inflight_cap && _completion_in_use < _completion_capacity) {
                        grant_delivery = _try_admit_one_locked(now_ns, &failures);
                    }
                    // A blocked front has already been detached to a deferred heap or bucket waiter. Other runnable
                    // queries were examined in this decision; hidden attempts in the same query wait for that
                    // blocker's timer/guard instead of creating a control-task spin proportional to the row backlog.
                    schedule_next = grant_delivery.has_value();
                    if (!schedule_next) {
                        Status timer_status = _arm_timer_locked(_next_wakeup_locked());
                        if (!timer_status.ok()) {
                            _handle_progress_failure_locked(timer_status, &failures);
                        }
                    }
                }
            }

            deliver_failures_noexcept(&failures);
            deliver_grant_noexcept(&grant_delivery);
            if (schedule_next) {
                std::lock_guard lock(_mutex);
                _request_pump_or_fail_locked(&failures);
            }
        } catch (const std::bad_alloc&) {
            LOG(WARNING) << "AI admission pump failed due to an allocation failure";
            _fail_closed_after_pump_exception(&failures);
        } catch (...) {
            LOG(WARNING) << "AI admission pump failed due to an unexpected exception";
            _fail_closed_after_pump_exception(&failures);
        }
        deliver_failures_noexcept(&failures);
        deliver_grant_noexcept(&grant_delivery);
    }

    const AIClock* _clock;
    AIControlScheduler* _scheduler;
    const AIAdmissionLimitSource* _limits;
    const int64_t _completion_capacity;
    AIRateLimiter _rate_limiter;

    mutable std::mutex _mutex;
    bool _shutdown = false;
    bool _pump_posted = false;
    std::optional<AIControlScheduler::TaskId> _timer_id;
    uint64_t _timer_generation = 0;
    uint64_t _next_registration_id = 0;
    uint64_t _scheduling_steps = 0;
    int64_t _inflight = 0;
    int64_t _completion_in_use = 0;
    std::map<uint64_t, std::shared_ptr<AIAdmissionAttempt>> _attempts;
    std::unordered_map<AIRateLimitKey, std::shared_ptr<AIAdmissionBucketState>, AIRateLimitKeyHash> _bucket_states;
    std::unordered_map<AIWorkGroupKey, std::unordered_map<UniqueId, std::unordered_set<uint64_t>>> _query_attempts;
    CancelAttemptChain _cancel_queue;
    TimedAttemptHeap _deadline_heap;
    TimedAttemptHeap _deferred_heap;
    bool _reactivate_all_rate_waiters = false;
    std::deque<AIWorkGroupKey> _workgroup_ring;
    std::unordered_map<AIWorkGroupKey, WorkGroupQueue> _workgroups;
    std::optional<AIWorkGroupKey> _last_selected_workgroup;
    std::unordered_map<AIWorkGroupKey, UniqueId> _last_selected_queries;
    std::unordered_map<AIWorkGroupKey, int64_t> _active_workgroup_grants;
};

AIBucketResolutionGuard::AIBucketResolutionGuard(std::shared_ptr<AIAdmissionControllerCore> core,
                                                 std::shared_ptr<AIAdmissionBucketState> bucket_state,
                                                 AITokenReservation token, AIMemoryContext memory) noexcept
        : _memory(std::move(memory)),
          _core(std::move(core)),
          _bucket_state(std::move(bucket_state)),
          _token(std::move(token)) {}

AIBucketResolutionGuard::~AIBucketResolutionGuard() noexcept {
    _reset();
}

AIBucketResolutionGuard::AIBucketResolutionGuard(AIBucketResolutionGuard&& other) noexcept {
    const AIMemoryContext memory = other._memory;
    run_in_physical_scope(memory, [&] {
        _memory = std::move(other._memory);
        _core = std::move(other._core);
        _bucket_state = std::move(other._bucket_state);
        if (other._token.has_value()) {
            TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
        }
        _token = std::move(other._token);
        _bucket_resolved = other._bucket_resolved;
        other._token.reset();
        other._bucket_resolved = true;
    });
}

AIBucketResolutionGuard& AIBucketResolutionGuard::operator=(AIBucketResolutionGuard&& other) noexcept {
    if (this != &other) {
        _reset();
        const AIMemoryContext memory = other._memory;
        run_in_physical_scope(memory, [&] {
            _memory = std::move(other._memory);
            _core = std::move(other._core);
            _bucket_state = std::move(other._bucket_state);
            if (other._token.has_value()) {
                TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
            }
            _token = std::move(other._token);
            _bucket_resolved = other._bucket_resolved;
            other._token.reset();
            other._bucket_resolved = true;
        });
    }
    return *this;
}

void AIBucketResolutionGuard::resolve_without_cooldown() noexcept {
    _resolve(std::nullopt);
}

void AIBucketResolutionGuard::resolve_with_cooldown(int64_t eligible_at_ns) noexcept {
    _resolve(_core == nullptr ? std::nullopt : _core->validate_cooldown(eligible_at_ns));
}

void AIBucketResolutionGuard::_resolve(std::optional<int64_t> cooldown_until_ns) noexcept {
    if (_core != nullptr && _bucket_state != nullptr && !_bucket_resolved) {
        DCHECK(_token.has_value());
        _core->resolve_bucket(_bucket_state, &*_token, cooldown_until_ns, _memory);
        _bucket_resolved = true;
    }
}

void AIBucketResolutionGuard::_reset() noexcept {
    const AIMemoryContext memory = _memory;
    if (_core != nullptr && _bucket_state != nullptr) {
        if (!_bucket_resolved) {
            DCHECK(_token.has_value());
            _core->resolve_bucket(_bucket_state, &*_token, std::nullopt, memory);
        }
        _core->release_completion_capacity(_bucket_state, memory);
    }
    run_in_physical_scope(memory, [&] {
        _token.reset();
        _bucket_state.reset();
        _core.reset();
        _memory = {};
    });
    _bucket_resolved = true;
}

AIAdmissionGrant::AIAdmissionGrant(std::shared_ptr<AIAdmissionControllerCore> core, AITokenReservation token,
                                   std::shared_ptr<AIAdmissionBucketState> bucket_state, AIWorkGroupKey workgroup_key,
                                   UniqueId query_id, uint64_t attempt_id, AIMemoryContext memory) noexcept
        : _memory(std::move(memory)),
          _core(std::move(core)),
          _token(std::move(token)),
          _bucket_state(std::move(bucket_state)),
          _workgroup_key(workgroup_key),
          _query_id(query_id),
          _attempt_id(attempt_id) {}

AIAdmissionGrant::~AIAdmissionGrant() noexcept {
    _reset();
}

AIAdmissionGrant::AIAdmissionGrant(AIAdmissionGrant&& other) noexcept {
    const AIMemoryContext memory = other._memory;
    run_in_physical_scope(memory, [&] {
        _memory = std::move(other._memory);
        _core = std::move(other._core);
        if (other._token.has_value()) {
            TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
        }
        _token = std::move(other._token);
        _bucket_state = std::move(other._bucket_state);
        _workgroup_key = other._workgroup_key;
        _query_id = other._query_id;
        _attempt_id = other._attempt_id;
        _network_committed = other._network_committed;
        _transport_completed = other._transport_completed;
        other._token.reset();
        other._transport_completed = true;
    });
}

AIAdmissionGrant& AIAdmissionGrant::operator=(AIAdmissionGrant&& other) noexcept {
    if (this != &other) {
        _reset();
        const AIMemoryContext memory = other._memory;
        run_in_physical_scope(memory, [&] {
            _memory = std::move(other._memory);
            _core = std::move(other._core);
            if (other._token.has_value()) {
                TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
            }
            _token = std::move(other._token);
            _bucket_state = std::move(other._bucket_state);
            _workgroup_key = other._workgroup_key;
            _query_id = other._query_id;
            _attempt_id = other._attempt_id;
            _network_committed = other._network_committed;
            _transport_completed = other._transport_completed;
            other._token.reset();
            other._transport_completed = true;
        });
    }
    return *this;
}

void AIAdmissionGrant::commit_network_attempt() noexcept {
    if (_core != nullptr && _token.has_value() && !_network_committed) {
        _core->commit_grant(_bucket_state, &*_token, _memory);
        _network_committed = true;
    }
}

AIBucketResolutionGuard AIAdmissionGrant::complete_transport() noexcept {
    if (_core == nullptr || !_network_committed || _transport_completed) {
        return {};
    }
    _transport_completed = true;
    DCHECK(_token.has_value());
    AITokenReservation token;
    run_in_physical_scope(_memory, [&] {
        TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
        token = std::move(*_token);
        _token.reset();
    });
    return _core->complete_transport(_bucket_state, _workgroup_key, &token, _memory);
}

void AIAdmissionGrant::_reset() noexcept {
    const AIMemoryContext memory = _memory;
    if (_core != nullptr && !_transport_completed) {
        DCHECK(_token.has_value());
        if (_network_committed) {
            AITokenReservation token;
            run_in_physical_scope(memory, [&] {
                TEST_SYNC_POINT("AIAdmissionController:request_owned_token_move");
                token = std::move(*_token);
                _token.reset();
            });
            AIBucketResolutionGuard abandoned =
                    _core->complete_transport(_bucket_state, _workgroup_key, &token, memory);
        } else {
            _core->rollback_grant(_workgroup_key, _bucket_state, &*_token, memory);
        }
    }
    _transport_completed = true;
    run_in_physical_scope(memory, [&] {
        _token.reset();
        _bucket_state.reset();
        _core.reset();
        _memory = {};
    });
}

void AIAdmissionTicket::cancel() {
    auto attempt = _attempt.lock();
    if (attempt == nullptr) {
        return;
    }
    const AIMemoryContext memory = attempt->request.memory;
    std::shared_ptr<AIAdmissionControllerCore> core;
    run_in_physical_scope(memory, [&] { core = attempt->core.lock(); });
    if (core != nullptr) {
        core->cancel_attempt(attempt);
    }
    run_in_physical_scope(memory, [&] { core.reset(); });
}

AIAdmissionController::AIAdmissionController(const AIClock* clock, AIControlScheduler* scheduler,
                                             const AIAdmissionLimitSource* limits, int64_t completion_capacity)
        : _core(std::make_shared<AIAdmissionControllerCore>(clock, scheduler, limits, completion_capacity)) {}

AIAdmissionController::~AIAdmissionController() {
    shutdown();
}

StatusOr<AIAdmissionTicket> AIAdmissionController::enqueue(const AIAdmissionRequest& request,
                                                           AIAdmissionCallback&& callback) {
    return _core->enqueue(request, std::move(callback));
}

void AIAdmissionController::cancel_query(const AIWorkGroupKey& workgroup_key, const UniqueId& query_id) {
    _core->cancel_query(workgroup_key, query_id);
}

void AIAdmissionController::extend_bucket_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns) {
    _core->extend_bucket_cooldown(key, eligible_at_ns);
}

void AIAdmissionController::notify_limits_changed() {
    _core->notify_limits_changed();
}

void AIAdmissionController::shutdown() {
    if (_core != nullptr) {
        _core->shutdown();
    }
}

int64_t AIAdmissionController::_inflight_for_test() const {
    return _core->inflight();
}

int64_t AIAdmissionController::_completion_in_use_for_test() const {
    return _core->completion_in_use();
}

uint64_t AIAdmissionController::_attempt_count_for_test() const {
    return _core->attempt_count();
}

uint64_t AIAdmissionController::_bucket_state_count_for_test() const {
    return _core->bucket_state_count();
}

int64_t AIAdmissionController::_bucket_registrations_for_test(const AIRateLimitKey& key) const {
    return _core->bucket_registrations(key);
}

int64_t AIAdmissionController::_bucket_inflight_for_test(const AIRateLimitKey& key) const {
    return _core->bucket_inflight(key);
}

int64_t AIAdmissionController::_bucket_owners_for_test(const AIRateLimitKey& key) const {
    return _core->bucket_owners(key);
}

int64_t AIAdmissionController::_unresolved_completion_count_for_test(const AIRateLimitKey& key) const {
    return _core->unresolved_completion_count(key);
}

int64_t AIAdmissionController::_rate_pins_for_test(const AIRateLimitKey& key) const {
    return _core->rate_pins(key);
}

uint64_t AIAdmissionController::_scheduling_steps_for_test() const {
    return _core->scheduling_steps();
}

uint64_t AIAdmissionController::_rate_waiter_count_for_test() const {
    return _core->rate_waiter_count();
}

} // namespace starrocks
