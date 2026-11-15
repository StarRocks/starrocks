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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <variant>

#include "base/statusor.h"
#include "base/uid_util.h"
#include "platform/llm/ai_lifecycle.h"
#include "platform/llm/ai_memory.h"
#include "platform/llm/ai_rate_limiter.h"

namespace starrocks {

class AIAdmissionControllerCore;
class AIAdmissionControllerTestPeer;
struct AIAdmissionAttempt;
struct AIAdmissionBucketState;

class AIControlScheduler {
public:
    using Task = std::function<void()>;
    using TaskId = uint64_t;

    virtual ~AIControlScheduler() = default;

    // The owner keeps this dependency alive until shutdown control work has drained. Tasks are serialized. An OK
    // result transfers task ownership to the scheduler; a non-OK result retains no task and invokes nothing inline.
    virtual Status post(Task task) = 0;
    virtual StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) = 0;
    // Cancellation is non-blocking and best-effort. A cancelled task may still run late.
    virtual void cancel(TaskId id) = 0;
    // Rejects new work, cancels future timers, drains already accepted immediate work, and joins the control thread.
    // The operation is synchronous and idempotent. It is an owner operation and must not be invoked from a task
    // managed by this scheduler.
    virtual void shutdown_and_drain() = 0;
};

class AIAdmissionLimitSource : public AIRateLimitSource {
public:
    ~AIAdmissionLimitSource() override = default;

    virtual int64_t max_inflight() const = 0;
};

// Opaque Platform identity for a WorkGroup generation: hi is the WorkGroup version and lo is its id.
// The all-zero key is the valid default WorkGroup identity.
using AIWorkGroupKey = UniqueId;

struct AIAdmissionRequest {
    AIWorkGroupKey workgroup_key;
    UniqueId query_id;
    uint64_t attempt_id = 0;
    AIRateLimitKey rate_limit_key;
    int64_t eligible_at_ns = 0;
    int64_t request_deadline_ns = 0;
    AIQueryLifecycleProbe lifecycle;
    AIMemoryContext memory;
};

enum class AIAdmissionFailureReason : uint8_t { CANCELLED, DEADLINE_EXCEEDED, SHUTDOWN, LOCAL_RESOURCE };

struct AIAdmissionFailure {
    AIAdmissionFailureReason reason;
};

class AIBucketResolutionGuard {
public:
    AIBucketResolutionGuard() = default;
    ~AIBucketResolutionGuard() noexcept;

    AIBucketResolutionGuard(const AIBucketResolutionGuard&) = delete;
    AIBucketResolutionGuard& operator=(const AIBucketResolutionGuard&) = delete;
    AIBucketResolutionGuard(AIBucketResolutionGuard&& other) noexcept;
    AIBucketResolutionGuard& operator=(AIBucketResolutionGuard&& other) noexcept;

    void resolve_without_cooldown() noexcept;
    void resolve_with_cooldown(int64_t eligible_at_ns) noexcept;

private:
    friend class AIAdmissionControllerCore;

    AIBucketResolutionGuard(std::shared_ptr<AIAdmissionControllerCore> core,
                            std::shared_ptr<AIAdmissionBucketState> bucket_state, AITokenReservation token,
                            AIMemoryContext memory) noexcept;
    void _resolve(std::optional<int64_t> cooldown_until_ns) noexcept;
    void _reset() noexcept;

    AIMemoryContext _memory;
    std::shared_ptr<AIAdmissionControllerCore> _core;
    std::shared_ptr<AIAdmissionBucketState> _bucket_state;
    std::optional<AITokenReservation> _token;
    bool _bucket_resolved = false;
};

class AIAdmissionGrant {
public:
    AIAdmissionGrant() = default;
    ~AIAdmissionGrant() noexcept;

    AIAdmissionGrant(const AIAdmissionGrant&) = delete;
    AIAdmissionGrant& operator=(const AIAdmissionGrant&) = delete;
    AIAdmissionGrant(AIAdmissionGrant&& other) noexcept;
    AIAdmissionGrant& operator=(AIAdmissionGrant&& other) noexcept;

    const AIWorkGroupKey& workgroup_key() const { return _workgroup_key; }
    const UniqueId& query_id() const { return _query_id; }
    uint64_t attempt_id() const { return _attempt_id; }

    void commit_network_attempt() noexcept;
    AIBucketResolutionGuard complete_transport() noexcept;

private:
    friend class AIAdmissionControllerCore;

    AIAdmissionGrant(std::shared_ptr<AIAdmissionControllerCore> core, AITokenReservation token,
                     std::shared_ptr<AIAdmissionBucketState> bucket_state, AIWorkGroupKey workgroup_key,
                     UniqueId query_id, uint64_t attempt_id, AIMemoryContext memory) noexcept;
    void _reset() noexcept;

    AIMemoryContext _memory;
    std::shared_ptr<AIAdmissionControllerCore> _core;
    std::optional<AITokenReservation> _token;
    std::shared_ptr<AIAdmissionBucketState> _bucket_state;
    AIWorkGroupKey _workgroup_key;
    UniqueId _query_id;
    uint64_t _attempt_id = 0;
    bool _network_committed = false;
    bool _transport_completed = false;
};

using AIAdmissionResult = std::variant<AIAdmissionGrant, AIAdmissionFailure>;
using AIAdmissionCallback = std::function<void(AIAdmissionResult)>;

class AIAdmissionTicket {
public:
    AIAdmissionTicket() = default;
    ~AIAdmissionTicket() = default;

    AIAdmissionTicket(const AIAdmissionTicket&) = delete;
    AIAdmissionTicket& operator=(const AIAdmissionTicket&) = delete;
    AIAdmissionTicket(AIAdmissionTicket&&) noexcept = default;
    AIAdmissionTicket& operator=(AIAdmissionTicket&&) noexcept = default;

    void cancel();

private:
    friend class AIAdmissionControllerCore;

    explicit AIAdmissionTicket(std::weak_ptr<AIAdmissionAttempt> attempt) : _attempt(std::move(attempt)) {}

    std::weak_ptr<AIAdmissionAttempt> _attempt;
};

class AIAdmissionController {
public:
    // clock, scheduler, and limits must outlive the controller's shutdown control drain. Retained grants and guards
    // may outlive the facade, but after that drain their resource-only cleanup does not access these dependencies.
    AIAdmissionController(const AIClock* clock, AIControlScheduler* scheduler, const AIAdmissionLimitSource* limits,
                          int64_t completion_capacity);
    ~AIAdmissionController();

    AIAdmissionController(const AIAdmissionController&) = delete;
    AIAdmissionController& operator=(const AIAdmissionController&) = delete;

    // Transfers callback ownership and clears its source in request.memory on
    // every success, rejection, and exception path.
    StatusOr<AIAdmissionTicket> enqueue(const AIAdmissionRequest& request, AIAdmissionCallback&& callback);
    void cancel_query(const AIWorkGroupKey& workgroup_key, const UniqueId& query_id);
    void extend_bucket_cooldown(const AIRateLimitKey& key, int64_t eligible_at_ns);
    void notify_limits_changed();
    void shutdown();

private:
    friend class AIAdmissionControllerTestPeer;

    int64_t _inflight_for_test() const;
    int64_t _completion_in_use_for_test() const;
    uint64_t _attempt_count_for_test() const;
    uint64_t _bucket_state_count_for_test() const;
    int64_t _bucket_registrations_for_test(const AIRateLimitKey& key) const;
    int64_t _bucket_inflight_for_test(const AIRateLimitKey& key) const;
    int64_t _bucket_owners_for_test(const AIRateLimitKey& key) const;
    int64_t _unresolved_completion_count_for_test(const AIRateLimitKey& key) const;
    int64_t _rate_pins_for_test(const AIRateLimitKey& key) const;
    uint64_t _scheduling_steps_for_test() const;
    uint64_t _rate_waiter_count_for_test() const;

    std::shared_ptr<AIAdmissionControllerCore> _core;
};

} // namespace starrocks
