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

#include "platform/llm/ai_task_dispatcher.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/scoped_cleanup.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_http_client.h"
#include "platform/llm/ai_metrics.h"
#include "platform/llm/ai_provider.h"

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

class AIHttpResponseBodyTestPeer {
public:
    static AIHttpResponseBody create(std::string data, AIMemoryContext memory, size_t reserved_bytes) {
        return AIHttpResponseBody(std::move(data), std::move(memory), reserved_bytes);
    }
};

namespace {

constexpr int64_t kSecond = 1'000'000'000;
constexpr int64_t kHour = 3600 * kSecond;

class FakeAIMemoryContext {
public:
    FakeAIMemoryContext() : _state(new State()) {
        _context = AIMemoryContext::create(_state, &reserve, &release, &run, &retain, &release_owner);
    }

    FakeAIMemoryContext(const FakeAIMemoryContext&) = delete;
    FakeAIMemoryContext& operator=(const FakeAIMemoryContext&) = delete;

    AIMemoryContext context() const { return _context; }
    bool in_physical_scope() const noexcept { return _state->physical_depth > 0; }

    std::function<bool(size_t)>& on_reserve() { return _state->on_reserve; }
    std::function<void(size_t)>& on_release() { return _state->on_release; }

private:
    struct State {
        std::atomic<size_t> references = 0;
        std::atomic<int> physical_depth = 0;
        std::function<bool(size_t)> on_reserve = [](size_t) { return true; };
        std::function<void(size_t)> on_release;
    };

    static bool reserve(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<State*>(opaque);
        try {
            return state->on_reserve(bytes);
        } catch (...) {
            return false;
        }
    }

    static void release(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<State*>(opaque);
        try {
            if (state->on_release) state->on_release(bytes);
        } catch (...) {
        }
    }

    static void run(void* opaque, AIMemoryContext::Action action, void* action_context) {
        auto* state = static_cast<State*>(opaque);
        state->physical_depth.fetch_add(1, std::memory_order_relaxed);
        try {
            action(action_context);
        } catch (...) {
            state->physical_depth.fetch_sub(1, std::memory_order_relaxed);
            throw;
        }
        state->physical_depth.fetch_sub(1, std::memory_order_relaxed);
    }

    static void retain(void* opaque) noexcept {
        static_cast<State*>(opaque)->references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release_owner(void* opaque) noexcept {
        auto* state = static_cast<State*>(opaque);
        if (state->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete state;
        }
    }

    State* _state;
    AIMemoryContext _context;
};

template <typename Function>
void run_in_memory_scope(const AIMemoryContext& memory, Function&& function) {
    using FunctionType = std::remove_reference_t<Function>;
    memory.run_in_physical_scope([](void* context) { std::invoke(*static_cast<FunctionType*>(context)); },
                                 std::addressof(function));
}

struct PhysicalScopeDestructionState {
    explicit PhysicalScopeDestructionState(const FakeAIMemoryContext* expected_memory)
            : expected_memory(expected_memory) {}

    void reset() noexcept {
        destructions.store(0, std::memory_order_relaxed);
        destroyed_outside_scope.store(false, std::memory_order_relaxed);
    }

    const FakeAIMemoryContext* expected_memory;
    std::atomic<size_t> destructions{0};
    std::atomic<bool> destroyed_outside_scope{false};
};

class PhysicalScopeDestructionObserver {
public:
    explicit PhysicalScopeDestructionObserver(std::shared_ptr<PhysicalScopeDestructionState> state)
            : _state(std::move(state)) {}

    PhysicalScopeDestructionObserver(const PhysicalScopeDestructionObserver&) = default;
    PhysicalScopeDestructionObserver& operator=(const PhysicalScopeDestructionObserver&) = default;
    PhysicalScopeDestructionObserver(PhysicalScopeDestructionObserver&&) noexcept = default;
    PhysicalScopeDestructionObserver& operator=(PhysicalScopeDestructionObserver&&) noexcept = default;

    ~PhysicalScopeDestructionObserver() {
        if (_state == nullptr) return;
        _state->destructions.fetch_add(1, std::memory_order_relaxed);
        if (!_state->expected_memory->in_physical_scope()) {
            _state->destroyed_outside_scope.store(true, std::memory_order_relaxed);
        }
    }

private:
    std::shared_ptr<PhysicalScopeDestructionState> _state;
};

size_t provider_request_template_bytes(std::string_view endpoint, const std::vector<AIHttpHeader>& headers,
                                       std::string_view body) {
    return endpoint.size() + body.size() +
           std::accumulate(headers.begin(), headers.end(), size_t{0}, [](size_t total, const AIHttpHeader& header) {
               return total + header.name.size() + header.value.size() + 4;
           });
}

class ManualAIClock final : public AIClock {
public:
    int64_t monotonic_now_ns() const noexcept override { return _monotonic_ns; }
    int64_t unix_now_seconds() const noexcept override { return _unix_seconds; }

    void advance_ns(int64_t delta_ns) {
        _monotonic_ns += delta_ns;
        _unix_seconds += delta_ns / kSecond;
    }

private:
    int64_t _monotonic_ns = 100 * kSecond;
    int64_t _unix_seconds = 1'700'000'000;
};

class ManualAIControlScheduler final : public AIControlScheduler {
public:
    explicit ManualAIControlScheduler(const AIClock* clock) : _clock(clock) {}

    Status post(Task task) override {
        if (!_post_statuses.empty()) {
            Status status = std::move(_post_statuses.front());
            _post_statuses.pop_front();
            if (!status.ok()) return status;
        }
        std::lock_guard lock(_ready_mutex);
        _ready.emplace_back(std::move(task));
        return Status::OK();
    }
    StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) override {
        if (!_timer_statuses.empty()) {
            Status status = std::move(_timer_statuses.front());
            _timer_statuses.pop_front();
            if (!status.ok()) return status;
        }
        TaskId id = ++_next_id;
        _timers.push_back(Timer{id, monotonic_time_ns, std::move(task), false});
        return id;
    }
    void cancel(TaskId id) override {
        for (auto& timer : _timers) {
            if (timer.id == id) timer.cancelled = true;
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
                if (_ready.empty()) return;
                task = std::move(_ready.front());
                _ready.pop_front();
            }
            try {
                task();
            } catch (...) {
            }
        }
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
    std::mutex _ready_mutex;
    std::deque<Task> _ready;
    std::vector<Timer> _timers;
    std::deque<Status> _post_statuses;
    std::deque<Status> _timer_statuses;
};

class MutableAIAdmissionLimitSource final : public AIAdmissionLimitSource {
public:
    int64_t qps(AICapability) const noexcept override { return chat_qps; }
    int64_t max_inflight() const override { return inflight_cap; }

    int64_t chat_qps = 100;
    int64_t inflight_cap = 8;
};

class SequenceAIRandom final : public AIRandom {
public:
    void push(uint32_t value) { _values.emplace_back(value); }
    void throw_on_next_call() { _throw_on_next_call = true; }

protected:
    uint32_t uniform_unlocked(uint32_t exclusive_upper) override {
        EXPECT_EQ(2501, exclusive_upper);
        if (std::exchange(_throw_on_next_call, false)) throw std::bad_alloc();
        if (_values.empty()) return 0;
        uint32_t value = _values.front();
        _values.pop_front();
        return value;
    }

private:
    std::deque<uint32_t> _values;
    bool _throw_on_next_call = false;
};

class ConcurrentEntryDetectingAIRandom final : public AIRandom {
public:
    bool concurrent_entry() const {
        std::lock_guard lock(_mutex);
        return _concurrent_entry;
    }

protected:
    uint32_t uniform_unlocked(uint32_t exclusive_upper) override {
        EXPECT_EQ(2501, exclusive_upper);
        std::unique_lock lock(_mutex);
        const size_t call = ++_calls;
        if (_inside) {
            _concurrent_entry = true;
            _cv.notify_all();
        } else {
            _inside = true;
            if (call == 1) {
                _cv.wait_for(lock, std::chrono::milliseconds(100), [&] { return _concurrent_entry; });
            }
            _inside = false;
            _cv.notify_all();
        }
        return 0;
    }

private:
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    size_t _calls = 0;
    bool _inside = false;
    bool _concurrent_entry = false;
};

class ManualAICompletionExecutor final : public AICompletionExecutor {
public:
    Status try_submit(AICompletionWork work) override {
        if (_throw_bad_alloc_on_submit) throw std::bad_alloc();
        if (_reject_status.has_value()) return *_reject_status;
        if (_stopping) return Status::ServiceUnavailable("AI completion executor is stopping");
        if (_tasks.size() >= _capacity) return Status::ResourceBusy("AI completion executor queue is full");
        _tasks.emplace_back(std::move(work));
        return Status::OK();
    }
    void run_until_idle() {
        while (!_tasks.empty()) {
            AICompletionWork work = std::move(_tasks.front());
            _tasks.pop_front();
            work.run();
        }
    }
    void run_one() {
        ASSERT_FALSE(_tasks.empty());
        AICompletionWork work = std::move(_tasks.front());
        _tasks.pop_front();
        work.run();
    }
    AICompletionWork take_one() {
        EXPECT_FALSE(_tasks.empty());
        if (_tasks.empty()) return {};
        AICompletionWork work = std::move(_tasks.front());
        _tasks.pop_front();
        return work;
    }
    void stop_and_cancel_queued() {
        _stopping = true;
        while (!_tasks.empty()) {
            AICompletionWork work = std::move(_tasks.front());
            _tasks.pop_front();
            work.cancel();
        }
    }
    size_t pending() const { return _tasks.size(); }
    void set_capacity(size_t capacity) { _capacity = capacity; }
    void set_stopping(bool stopping) { _stopping = stopping; }
    void set_reject_status(std::optional<Status> status) { _reject_status = std::move(status); }
    void set_throw_bad_alloc_on_submit(bool value) { _throw_bad_alloc_on_submit = value; }

private:
    size_t _capacity = 16;
    bool _stopping = false;
    bool _throw_bad_alloc_on_submit = false;
    std::optional<Status> _reject_status;
    std::deque<AICompletionWork> _tasks;
};

class ScriptedAIProvider final : public AIProvider {
public:
    StatusOr<AIProviderHttpRequest> build_request(const AIChatRequest& request) const override {
        ++build_count;
        if (_build_hook) _build_hook(request);
        if (!_build_status.ok()) return _build_status;
        return AIProviderHttpRequest{
                .url = std::string(request.endpoint), .headers = request_headers, .body = request_body};
    }

    AIProviderParseResult parse_response(std::string_view) const override {
        ++parse_count;
        if (_parse_hook) _parse_hook();
        if (_parse_results.empty()) return AIProviderSuccess{.content = "ok"};
        AIProviderParseResult result = _parse_results.front();
        _parse_results.pop_front();
        return result;
    }

    void push_parse(AIProviderParseResult result) { _parse_results.emplace_back(std::move(result)); }
    void set_build_hook(std::function<void(const AIChatRequest&)> hook) { _build_hook = std::move(hook); }
    void set_parse_hook(std::function<void()> hook) { _parse_hook = std::move(hook); }
    void set_build_status(Status status) { _build_status = std::move(status); }

    mutable size_t build_count = 0;
    mutable size_t parse_count = 0;
    std::vector<AIHttpHeader> request_headers;
    std::string request_body = "{}";

private:
    Status _build_status = Status::OK();
    std::function<void(const AIChatRequest&)> _build_hook;
    std::function<void()> _parse_hook;
    mutable std::deque<AIProviderParseResult> _parse_results;
};

class ScriptedAIHttpClient final : public AIHttpClient {
public:
    enum class Mode {
        SYNC_FAILURE,
        SYNC_SHUTDOWN,
        SYNC_STATUS,
        THROW_BAD_ALLOC,
        INLINE_COMPLETION,
        INLINE_COMPLETION_THEN_THROW,
        PENDING_COMPLETION
    };
    struct Step {
        Mode mode;
        std::optional<int64_t> http_status;
        std::optional<AIHttpNoResponseCode> no_response;
        std::optional<std::string> retry_after;
        Status sync_status;
        std::optional<AIHttpResponseBody> response_body;
    };

    Status submit(AIHttpRequest request, AIHttpCallback callback) override {
        AIMemoryContext request_memory = request.memory;
        SCOPED_CLEANUP({
            try {
                run_in_memory_scope(request_memory, [&] {
                    request = AIHttpRequest{};
                    AIHttpCallback().swap(callback);
                });
            } catch (...) {
            }
        });
        ++submit_calls;
        if (before_submit) before_submit(request);
        submitted_urls.emplace_back(request.url);
        submitted_connect_timeouts_ms.emplace_back(request.connect_timeout_ms);
        if (_steps.empty()) return Status::InternalError("scripted AI HTTP result is missing");
        Step step = std::move(_steps.front());
        _steps.pop_front();
        if (step.mode == Mode::SYNC_FAILURE) {
            return Status::InvalidArgument("AI HTTP request is invalid");
        }
        if (step.mode == Mode::SYNC_SHUTDOWN) {
            return Status::ServiceUnavailable("AI HTTP client is stopping");
        }
        if (step.mode == Mode::SYNC_STATUS) {
            return std::move(step.sync_status);
        }
        if (step.mode == Mode::THROW_BAD_ALLOC) {
            throw std::bad_alloc();
        }
        ++accepted_attempts;
        if (step.mode == Mode::INLINE_COMPLETION || step.mode == Mode::INLINE_COMPLETION_THEN_THROW) {
            const bool throw_after_completion = step.mode == Mode::INLINE_COMPLETION_THEN_THROW;
            callback(make_result(std::move(step)));
            if (after_inline_callback_before_return) after_inline_callback_before_return();
            if (throw_after_completion) {
                throw std::bad_alloc();
            }
        } else {
            _pending.push_back(Pending{std::move(step), std::move(callback), request_memory});
            if (before_pending_return) before_pending_return();
        }
        return Status::OK();
    }

    void shutdown() override {
        while (!_pending.empty()) {
            Pending pending = std::move(_pending.front());
            _pending.pop_front();
            AIMemoryContext memory = pending.memory;
            SCOPED_CLEANUP({
                try {
                    run_in_memory_scope(memory, [&] {
                        AIHttpCallback().swap(pending.callback);
                        pending.memory = {};
                    });
                } catch (...) {
                }
            });
            pending.callback(AIHttpNoResponse{.code = AIHttpNoResponseCode::SHUTDOWN});
        }
    }

    void push_http(Mode mode, int64_t status, std::optional<std::string> retry_after = std::nullopt) {
        _steps.push_back(Step{mode, status, std::nullopt, std::move(retry_after)});
    }
    void push_http_body(Mode mode, int64_t status, AIHttpResponseBody body) {
        Step step{.mode = mode, .http_status = status};
        step.response_body.emplace(std::move(body));
        _steps.emplace_back(std::move(step));
    }
    void push_no_response(Mode mode, AIHttpNoResponseCode code) {
        _steps.push_back(Step{mode, std::nullopt, code, std::nullopt});
    }
    void push_sync_failure() { _steps.push_back(Step{Mode::SYNC_FAILURE, std::nullopt, std::nullopt, std::nullopt}); }
    void push_sync_shutdown() { _steps.push_back(Step{Mode::SYNC_SHUTDOWN, std::nullopt, std::nullopt, std::nullopt}); }
    void push_sync_status(Status status) {
        Step step{.mode = Mode::SYNC_STATUS};
        step.sync_status = std::move(status);
        _steps.emplace_back(std::move(step));
    }

    void complete_next() {
        ASSERT_FALSE(_pending.empty());
        Pending pending = std::move(_pending.front());
        _pending.pop_front();
        AIMemoryContext memory = pending.memory;
        SCOPED_CLEANUP({
            try {
                run_in_memory_scope(memory, [&] {
                    AIHttpCallback().swap(pending.callback);
                    pending.memory = {};
                });
            } catch (...) {
            }
        });
        pending.callback(make_result(std::move(pending.step)));
    }
    size_t pending() const { return _pending.size(); }

    size_t submit_calls = 0;
    size_t accepted_attempts = 0;
    std::vector<std::string> submitted_urls;
    std::vector<int64_t> submitted_connect_timeouts_ms;
    std::function<void(const AIHttpRequest&)> before_submit;
    std::function<void()> after_inline_callback_before_return;
    std::function<void()> before_pending_return;

private:
    struct Pending {
        Step step;
        AIHttpCallback callback;
        AIMemoryContext memory;
    };

    static AIHttpResult make_result(Step step) {
        if (step.no_response.has_value()) return AIHttpNoResponse{.code = *step.no_response};
        AIHttpResponse response;
        response.status_code = *step.http_status;
        response.retry_after = std::move(step.retry_after);
        if (step.response_body.has_value()) response.body = std::move(*step.response_body);
        return AIHttpResult{std::move(response)};
    }

    std::deque<Step> _steps;
    std::deque<Pending> _pending;
};

TEST(AITaskSuccessTest, IsMoveOnlyAndReleasesExactlyOnceAcrossMovesCallbackAndQueue) {
    static_assert(!std::is_copy_constructible_v<AITaskSuccess>);
    static_assert(!std::is_copy_assignable_v<AITaskSuccess>);
    static_assert(std::is_nothrow_move_constructible_v<AITaskSuccess>);
    static_assert(std::is_nothrow_move_assignable_v<AITaskSuccess>);
    static_assert(std::is_nothrow_destructible_v<AITaskSuccess>);

    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    FakeAIMemoryContext memory;
    memory.on_reserve() = [&](size_t bytes) {
        ++reserve_calls;
        reserved_bytes += bytes;
        return true;
    };
    memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };

    std::vector<AITaskResult> queue;
    {
        auto first_result = AITaskSuccess::create("first", memory.context());
        auto replaced_result = AITaskSuccess::create("replacement", memory.context());
        ASSERT_TRUE(first_result.ok()) << first_result.status();
        ASSERT_TRUE(replaced_result.ok()) << replaced_result.status();

        AITaskSuccess first = std::move(first_result).value();
        AITaskSuccess replaced = std::move(replaced_result).value();
        AITaskSuccess moved = std::move(first);
        replaced = std::move(moved);
        EXPECT_EQ(1, release_calls) << "move assignment releases the replaced reservation";

        std::function<void(AITaskResult)> callback = [&](AITaskResult result) {
            queue.emplace_back(std::move(result));
        };
        callback(AITaskResult{std::move(replaced)});
    }

    ASSERT_EQ(1, queue.size());
    ASSERT_TRUE(std::holds_alternative<AITaskSuccess>(queue.front()));
    EXPECT_EQ("first", std::get<AITaskSuccess>(queue.front()).content());
    EXPECT_EQ(2, reserve_calls);
    EXPECT_EQ(std::string_view("first").size() + std::string_view("replacement").size(), reserved_bytes);
    EXPECT_EQ(1, release_calls);

    queue.clear();
    EXPECT_EQ(2, release_calls);
    EXPECT_EQ(reserved_bytes, released_bytes);
}

TEST(AITaskSuccessTest, ReserveRejectionAndExceptionDoNotCreateAnOwner) {
    size_t release_calls = 0;
    FakeAIMemoryContext rejected;
    rejected.on_reserve() = [](size_t) { return false; };
    rejected.on_release() = [&](size_t) { ++release_calls; };
    auto rejected_result = AITaskSuccess::create("rejected", rejected.context());
    ASSERT_FALSE(rejected_result.ok());
    EXPECT_TRUE(rejected_result.status().is_mem_limit_exceeded()) << rejected_result.status();

    FakeAIMemoryContext throwing;
    throwing.on_reserve() = [](size_t) -> bool { throw 1; };
    throwing.on_release() = [&](size_t) { ++release_calls; };
    auto throwing_result = AITaskSuccess::create("throwing", throwing.context());
    ASSERT_FALSE(throwing_result.ok());
    EXPECT_TRUE(throwing_result.status().is_mem_limit_exceeded()) << throwing_result.status();
    EXPECT_EQ(0, release_calls);
}

class AITaskDispatcherTest : public ::testing::Test {
protected:
    explicit AITaskDispatcherTest(int64_t completion_capacity = 16)
            : _control(&_clock),
              _controller(&_clock, &_control, &_limits, completion_capacity),
              _dispatcher(&_controller, &_http, &_provider, &_completion, &_clock, &_random, &_metrics,
                          AITaskDispatcherOptions{.max_retries = 1, .max_throttle_retries = 2}) {}

    void TearDown() override {
        for (auto& handle : _handles) handle.cancel();
        _controller.shutdown();
        _control.run_until_idle();
        _http.shutdown();
        _completion.run_until_idle();
        _control.run_until_idle();
    }

    AIDispatchRequest request(AIWorkGroupKey workgroup_key, UniqueId query_id, uint64_t task_id,
                              std::string_view endpoint = "https://model.invalid/v1/chat") {
        AIDispatchRequest result;
        result.workgroup_key = workgroup_key;
        result.query_id = query_id;
        result.task_id = task_id;
        result.chat_request =
                AIChatRequest{.endpoint = endpoint, .model = "model", .api_key = "secret-key", .prompt = "p"};
        result.request_deadline_ns = _clock.monotonic_now_ns() + kHour;
        result.connect_timeout_ms = 1000;
        result.max_response_bytes = 1024;
        const int64_t query_deadline_ns = result.request_deadline_ns;
        result.lifecycle = [query_deadline_ns] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
        };
        result.memory = _memory.context();
        return result;
    }

    AIDispatchRequest request(int64_t workgroup_id, UniqueId query_id, uint64_t task_id,
                              std::string_view endpoint = "https://model.invalid/v1/chat") {
        return request(UniqueId{0, workgroup_id}, query_id, task_id, endpoint);
    }

    void submit(AIDispatchRequest request) {
        const uint64_t task_id = request.task_id;
        auto handle = _dispatcher.submit(std::move(request), [this, task_id](AITaskResult result) {
            if (_before_result_callback) _before_result_callback();
            _result_task_ids.emplace_back(task_id);
            _results.emplace_back(std::move(result));
        });
        ASSERT_TRUE(handle.ok()) << handle.status();
        _handles.emplace_back(std::move(handle).value());
    }

    void submit_and_run_control(AIDispatchRequest request) {
        submit(std::move(request));
        _control.run_until_idle();
    }

    void complete_next_transport_and_run_control() {
        _http.complete_next();
        _control.run_until_idle();
    }

    ManualAIClock _clock;
    ManualAIControlScheduler _control;
    MutableAIAdmissionLimitSource _limits;
    AIAdmissionController _controller;
    ScriptedAIHttpClient _http;
    ScriptedAIProvider _provider;
    ManualAICompletionExecutor _completion;
    SequenceAIRandom _random;
    AIMetrics _metrics;
    FakeAIMemoryContext _memory;
    AITaskDispatcher _dispatcher;
    std::vector<AITaskHandle> _handles;
    std::vector<uint64_t> _result_task_ids;
    std::vector<AITaskResult> _results;
    std::function<void()> _before_result_callback;
};

class AITaskDispatcherSingleCompletionTest : public AITaskDispatcherTest {
protected:
    AITaskDispatcherSingleCompletionTest() : AITaskDispatcherTest(1) {}
};

TEST_F(AITaskDispatcherTest, InitialAdmissionPostMemoryLimitIsLocalResourceWithoutHttpAttempt) {
    _control.fail_next_post(Status::MemoryLimitExceeded("admission-post-allocation"));

    submit(request(1, UniqueId{1, 40}, 1));

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(0, _http.submit_calls);
}

TEST_F(AITaskDispatcherTest, InitialAdmissionMaterializationExceptionCompletesSynchronouslyAsLocalResource) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AITaskState::_prepare_admission:before_admission_materialization",
                            [](void*) { throw std::bad_alloc(); });
    std::optional<StatusOr<AITaskHandle>> submitted;

    EXPECT_NO_THROW({
        submitted.emplace(_dispatcher.submit(request(1, UniqueId{1, 42}, 1), [this](AITaskResult result) {
            _results.emplace_back(std::move(result));
        }));
    });

    ASSERT_TRUE(submitted.has_value());
    ASSERT_TRUE(submitted->ok()) << submitted->status();
    _handles.emplace_back(std::move(*submitted).value());
    ASSERT_EQ(1, _results.size()) << "initial enqueue failure must preserve the synchronous callback contract";
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(0, _http.submit_calls);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, RetryAdmissionMaterializationExceptionCompletesAsLocalResourceWithoutCooldown) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    submit(request(1, UniqueId{1, 43}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AITaskState::_prepare_admission:before_admission_materialization",
                            [](void*) { throw std::bad_alloc(); });

    EXPECT_NO_THROW(_completion.run_until_idle());
    sync_point->ClearCallBack("AITaskState::_prepare_admission:before_admission_materialization");

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(1, _http.submit_calls);
    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));

    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(2, UniqueId{1, 45}, 2));
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "local retry enqueue failure must not install a shared throttle cooldown";
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherTest, RetryRandomExceptionCompletesAsLocalResourceWithoutLeakingAdmission) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    submit(request(1, UniqueId{1, 44}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _random.throw_on_next_call();

    EXPECT_NO_THROW(_completion.run_until_idle());

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(1, _http.submit_calls);
    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, AcceptedAdmissionTimerMemoryLimitIsLocalResourceWithoutHttpAttempt) {
    _limits.inflight_cap = 0;
    submit(request(1, UniqueId{1, 41}, 1));
    _control.fail_next_timer(Status::MemoryLimitExceeded("admission-timer-allocation"));

    _control.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(0, _http.submit_calls);
}

TEST_F(AITaskDispatcherTest, PreservesVersionedWorkGroupFairnessThroughDispatch) {
    const AIWorkGroupKey old_workgroup{1, 10};
    const AIWorkGroupKey new_workgroup{2, 10};
    _limits.inflight_cap = 3;
    for (int i = 0; i < 3; ++i) {
        _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    }

    submit(request(old_workgroup, UniqueId{0, 101}, 1, "https://old-1.invalid/v1/chat"));
    submit(request(old_workgroup, UniqueId{0, 102}, 2, "https://old-2.invalid/v1/chat"));
    submit(request(new_workgroup, UniqueId{0, 201}, 3, "https://new.invalid/v1/chat"));
    _control.run_until_idle();

    EXPECT_EQ((std::vector<std::string>{"https://old-1.invalid/v1/chat", "https://new.invalid/v1/chat",
                                        "https://old-2.invalid/v1/chat"}),
              _http.submitted_urls);
}

TEST_F(AITaskDispatcherTest, ClassifiesEveryNoResponseCodeByExplicitAllowlist) {
    const std::vector<AIHttpNoResponseCode> retryable = {
            AIHttpNoResponseCode::DNS,
            AIHttpNoResponseCode::CONNECT,
            AIHttpNoResponseCode::TIMEOUT,
            AIHttpNoResponseCode::SEND,
            AIHttpNoResponseCode::RECEIVE,
            AIHttpNoResponseCode::EMPTY_REPLY,
            AIHttpNoResponseCode::PARTIAL_TRANSFER,
            AIHttpNoResponseCode::HTTP2_STREAM_RESET,
    };
    for (AIHttpNoResponseCode code : retryable) {
        EXPECT_EQ(AIAttemptAction::RETRY, classify_ai_no_response(code)) << static_cast<int>(code);
    }

    const std::vector<AIHttpNoResponseCode> terminal = {
            AIHttpNoResponseCode::TLS_HANDSHAKE, AIHttpNoResponseCode::TLS_VERIFICATION,
            AIHttpNoResponseCode::CANCELLATION,  AIHttpNoResponseCode::DEADLINE,
            AIHttpNoResponseCode::RESPONSE_CAP,  AIHttpNoResponseCode::MEMORY_LIMIT,
            AIHttpNoResponseCode::SHUTDOWN,      AIHttpNoResponseCode::UNKNOWN,
    };
    for (AIHttpNoResponseCode code : terminal) {
        EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_no_response(code)) << static_cast<int>(code);
    }
    EXPECT_EQ(AIAttemptAction::TERMINAL,
              classify_ai_no_response(static_cast<AIHttpNoResponseCode>(std::numeric_limits<uint8_t>::max())))
            << "an unknown future enum value must fail closed";
}

TEST_F(AITaskDispatcherTest, AppliesOrderedHttpAndProviderClassification) {
    const AIProviderParseResult success = AIProviderSuccess{.content = "ok"};
    const AIProviderParseResult retryable = AIProviderStructuredError{.code = AIProviderErrorCode::SERVER_ERROR};
    const AIProviderParseResult throttled = AIProviderStructuredError{.code = AIProviderErrorCode::RATE_LIMIT_EXCEEDED};
    const AIProviderParseResult unknown = AIProviderStructuredError{.code = AIProviderErrorCode::UNKNOWN};
    const AIProviderParseResult malformed = AIProviderMalformed{};

    EXPECT_EQ(AIAttemptAction::SUCCEEDED, classify_ai_http_response(200, success));
    EXPECT_EQ(AIAttemptAction::SUCCEEDED, classify_ai_http_response(201, success));
    EXPECT_EQ(AIAttemptAction::SUCCEEDED, classify_ai_http_response(299, success));
    EXPECT_EQ(AIAttemptAction::RETRY, classify_ai_http_response(200, retryable));
    EXPECT_EQ(AIAttemptAction::THROTTLE, classify_ai_http_response(200, throttled));
    EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_http_response(200, unknown));
    EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_http_response(200, malformed));
    EXPECT_EQ(AIAttemptAction::RETRY, classify_ai_http_response(400, retryable));
    EXPECT_EQ(AIAttemptAction::THROTTLE, classify_ai_http_response(400, throttled));
    EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_http_response(400, success));
    EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_http_response(400, malformed));

    for (int64_t status : {408, 500, 502, 503, 504}) {
        EXPECT_EQ(AIAttemptAction::RETRY, classify_ai_http_response(status, malformed)) << status;
        EXPECT_EQ(AIAttemptAction::RETRY, classify_ai_http_response(status, unknown)) << status;
    }
    EXPECT_EQ(AIAttemptAction::THROTTLE, classify_ai_http_response(429, malformed));
    for (int64_t status : {300, 401, 403, 404, 409, 422, 501}) {
        EXPECT_EQ(AIAttemptAction::TERMINAL, classify_ai_http_response(status, retryable)) << status;
    }
}

TEST_F(AITaskDispatcherTest, UsesSharedRetryOrdinalWithClassSpecificCeilings) {
    EXPECT_TRUE(ai_should_retry(0, AIAttemptAction::RETRY, 3, 5));
    EXPECT_TRUE(ai_should_retry(1, AIAttemptAction::THROTTLE, 3, 5));
    EXPECT_TRUE(ai_should_retry(2, AIAttemptAction::RETRY, 3, 5));
    EXPECT_TRUE(ai_should_retry(3, AIAttemptAction::THROTTLE, 3, 5));
    EXPECT_FALSE(ai_should_retry(4, AIAttemptAction::RETRY, 3, 5));
    EXPECT_TRUE(ai_should_retry(4, AIAttemptAction::THROTTLE, 3, 5));
    EXPECT_FALSE(ai_should_retry(3, AIAttemptAction::RETRY, 3, 5));
    EXPECT_FALSE(ai_should_retry(5, AIAttemptAction::THROTTLE, 3, 5));
}

TEST_F(AITaskDispatcherTest, ComputesBoundedExponentialBackoffJitterAndRetryAfter) {
    const std::vector<int64_t> base_seconds = {1, 2, 4, 8, 16, 32, 32};
    for (size_t index = 0; index < base_seconds.size(); ++index) {
        EXPECT_EQ(base_seconds[index] * kSecond, ai_retry_backoff_ns(index + 1, 0));
        EXPECT_EQ(base_seconds[index] * kSecond * 5 / 4, ai_retry_backoff_ns(index + 1, 2500));
    }

    constexpr int64_t now_ns = 100 * kSecond;
    constexpr int64_t deadline_ns = 200 * kSecond;
    constexpr int64_t wall_now = 1'700'000'000;
    EXPECT_EQ(now_ns + 3 * kSecond,
              ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 0, std::string_view("3")));
    EXPECT_EQ(now_ns + 3 * kSecond, ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 0,
                                                            std::string_view("Tue, 14 Nov 2023 22:13:23 GMT")));
    EXPECT_EQ(now_ns + kSecond,
              ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 0, std::string_view("invalid")));
    EXPECT_EQ(now_ns + kSecond, ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 0,
                                                        std::string_view("Tue, 14 Nov 2023 22:13:19 GMT")))
            << "a past Retry-After date contributes zero delay, so exponential backoff remains the maximum";
    EXPECT_EQ(now_ns + kSecond, ai_retry_eligible_at_ns(now_ns, std::numeric_limits<int64_t>::max(), deadline_ns, 1, 0,
                                                        std::string_view("Mon, 01 Jan 0001 00:00:00 GMT")))
            << "subtracting a far-past HTTP date from a maximum wall clock must not overflow";
    EXPECT_EQ(now_ns + 5 * kSecond / 4,
              ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 2500, std::string_view("1")));
    EXPECT_EQ(now_ns + kSecond,
              ai_retry_eligible_at_ns(now_ns, wall_now, now_ns + 3 * kSecond, 1, 0, std::string_view("3")))
            << "a Retry-After at the deadline is ignored when local backoff still fits";
    EXPECT_EQ(now_ns + kSecond, ai_retry_eligible_at_ns(now_ns, wall_now, deadline_ns, 1, 0,
                                                        std::string_view("999999999999999999999999999999999999")))
            << "an overflowing Retry-After is ignored when local backoff still fits";
    EXPECT_FALSE(
            ai_retry_eligible_at_ns(now_ns, wall_now, now_ns + kSecond, 1, 0, std::string_view("invalid")).has_value())
            << "local backoff itself must remain strictly before the effective deadline";
    EXPECT_EQ(std::numeric_limits<int64_t>::max() - kSecond,
              ai_retry_eligible_at_ns(std::numeric_limits<int64_t>::max() - 2 * kSecond, wall_now,
                                      std::numeric_limits<int64_t>::max(), 1, 0, std::string_view("3")))
            << "a Retry-After addition overflow is ignored when local backoff still fits";
}

TEST_F(AITaskDispatcherTest, AcceptedInlineCompletionCommitsBeforeReleaseAndDefersProviderParsing) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "inline-ok"});
    bool observed_pending_submit_result = false;
    _http.after_inline_callback_before_return = [&] {
        observed_pending_submit_result = true;
        EXPECT_FALSE(_memory.in_physical_scope());
        EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
        EXPECT_EQ(0, _completion.pending());
        EXPECT_EQ(0, _provider.parse_count);
        EXPECT_TRUE(_results.empty());
    };

    submit(request(7, UniqueId{9, 11}, 1));
    _control.run_until_idle();

    EXPECT_TRUE(observed_pending_submit_result);
    EXPECT_EQ(1, _http.accepted_attempts);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(1, _completion.pending());
    EXPECT_TRUE(_results.empty());

    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));
    EXPECT_EQ("inline-ok", std::get<AITaskSuccess>(_results.front()).content());
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));

    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(7, UniqueId{9, 12}, 2));
    _control.run_until_idle();
    EXPECT_EQ(1, _http.submit_calls) << "the accepted inline attempt committed its QPS token";
    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, AcceptedAsyncAttemptSurvivesPersistentDirtyWaiterRestoreFailure) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 2;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "accepted"});

    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    auto blocked_reasons = std::make_shared<std::vector<AIAdmissionFailureReason>>();
    std::optional<AIAdmissionTicket> blocked_ticket;
    bool inject_restore_failure = false;
    bool prepared_waiter = false;

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AIAdmissionController::_add_query_to_ring_locked:before_query_ring_emplace",
                            [&inject_restore_failure](void*) {
                                if (inject_restore_failure) {
                                    throw std::bad_alloc();
                                }
                            });

    _http.before_pending_return = [&] {
        if (prepared_waiter) {
            return;
        }
        prepared_waiter = true;
        _limits.chat_qps = 0;
        AIAdmissionRequest blocked{.workgroup_key = UniqueId{0, 2},
                                   .query_id = UniqueId{8, 2},
                                   .attempt_id = 2,
                                   .rate_limit_key = key,
                                   .eligible_at_ns = _clock.monotonic_now_ns(),
                                   .request_deadline_ns = _clock.monotonic_now_ns() + kHour};
        const int64_t blocked_query_deadline_ns = blocked.request_deadline_ns;
        blocked.lifecycle = [blocked_query_deadline_ns] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = blocked_query_deadline_ns};
        };
        auto ticket = _controller.enqueue(std::move(blocked), [blocked_reasons](AIAdmissionResult result) {
            if (auto* failure = std::get_if<AIAdmissionFailure>(&result); failure != nullptr) {
                blocked_reasons->emplace_back(failure->reason);
            }
        });
        ASSERT_TRUE(ticket.ok()) << ticket.status();
        blocked_ticket.emplace(std::move(ticket).value());
        _control.run_until_idle();
        EXPECT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));
        _limits.chat_qps = 1;
        inject_restore_failure = true;
    };

    submit(request(1, UniqueId{8, 1}, 1));
    _control.run_until_idle();
    _http.before_pending_return = {};
    sync_point->ClearCallBack("AIAdmissionController::_add_query_to_ring_locked:before_query_ring_emplace");

    ASSERT_TRUE(prepared_waiter);
    ASSERT_EQ(1, _http.accepted_attempts);
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    _control.run_until_idle();

    EXPECT_EQ(1, _results.size()) << "an accepted HTTP attempt must retain exactly one terminal callback";
    if (!_results.empty()) {
        EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));
    }
    EXPECT_EQ((std::vector<AIAdmissionFailureReason>{AIAdmissionFailureReason::LOCAL_RESOURCE}), *blocked_reasons);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, AdmissionFailureCallbackCanCancelCompletingTaskWithoutStateMutexDeadlock) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(1, UniqueId{8, 3}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _http.pending());

    std::mutex cancellation_mutex;
    std::condition_variable cancellation_cv;
    bool cancellation_finished = false;
    bool cancellation_finished_inside_callback = false;
    std::thread cancellation_thread;
    std::optional<AITaskResult> blocked_result;
    auto blocked = _dispatcher.submit(request(2, UniqueId{8, 4}, 2), [&](AITaskResult result) {
        blocked_result.emplace(std::move(result));
        cancellation_thread = std::thread([&] {
            _handles.front().cancel();
            {
                std::lock_guard lock(cancellation_mutex);
                cancellation_finished = true;
            }
            cancellation_cv.notify_all();
        });
        std::unique_lock lock(cancellation_mutex);
        cancellation_finished_inside_callback =
                cancellation_cv.wait_for(lock, std::chrono::seconds(5), [&] { return cancellation_finished; });
    });
    ASSERT_TRUE(blocked.ok()) << blocked.status();
    _handles.emplace_back(std::move(blocked).value());
    _control.run_until_idle();
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::attempt_count(_controller));

    _control.fail_next_post(Status::MemoryLimitExceeded("completion-followup-post"));
    _http.complete_next();
    if (cancellation_thread.joinable()) cancellation_thread.join();

    EXPECT_TRUE(cancellation_finished_inside_callback)
            << "admission failure callbacks must run after the completing task releases its state mutex";
    ASSERT_TRUE(blocked_result.has_value());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(*blocked_result));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(*blocked_result).failure_class);

    _control.run_until_idle();
    _completion.run_until_idle();
    _control.run_until_idle();
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);

    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, AdmissionFailureCallbackCanCancelRetryingTaskDuringNetworkCommit) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(1, UniqueId{8, 7}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_TRUE(_results.empty());

    _limits.chat_qps = 0;
    std::mutex cancellation_mutex;
    std::condition_variable cancellation_cv;
    bool cancellation_finished = false;
    bool cancellation_finished_inside_callback = false;
    std::thread cancellation_thread;
    std::optional<AITaskResult> blocked_result;
    SCOPED_CLEANUP({
        _http.before_pending_return = {};
        if (_handles.size() > 1) _handles.back().cancel();
        _control.run_until_idle();
        if (cancellation_thread.joinable()) cancellation_thread.join();
    });
    auto blocked = _dispatcher.submit(request(2, UniqueId{8, 8}, 2), [&](AITaskResult result) {
        blocked_result.emplace(std::move(result));
        cancellation_thread = std::thread([&] {
            _handles.front().cancel();
            {
                std::lock_guard lock(cancellation_mutex);
                cancellation_finished = true;
            }
            cancellation_cv.notify_all();
        });
        std::unique_lock lock(cancellation_mutex);
        cancellation_finished_inside_callback =
                cancellation_cv.wait_for(lock, std::chrono::seconds(5), [&] { return cancellation_finished; });
    });
    ASSERT_TRUE(blocked.ok()) << blocked.status();
    _handles.emplace_back(std::move(blocked).value());
    _control.run_until_idle();
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::rate_waiter_count(_controller));

    _limits.chat_qps = 1;
    _http.before_pending_return = [&] {
        _control.fail_next_post(Status::MemoryLimitExceeded("network-commit-followup-post"));
    };
    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(0, _http.pending()) << "the first wakeup observes the live QPS increase and schedules token refill";
    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    _http.before_pending_return = {};
    if (cancellation_thread.joinable()) cancellation_thread.join();

    EXPECT_TRUE(cancellation_finished_inside_callback)
            << "network commit must release the task mutex before admission callbacks can run";
    ASSERT_TRUE(blocked_result.has_value());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(*blocked_result));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(*blocked_result).failure_class);
    ASSERT_EQ(1, _http.pending());
    EXPECT_EQ(2, _http.accepted_attempts);

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    _control.run_until_idle();
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);

    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, ThrowingCancellationProbeBeforeAdmissionFailsClosedAsCancellation) {
    AIDispatchRequest dispatch = request(1, UniqueId{8, 5}, 1);
    dispatch.lifecycle = []() -> AIQueryLifecycleSnapshot { throw 1; };
    std::optional<StatusOr<AITaskHandle>> submitted;

    EXPECT_NO_THROW({
        submitted.emplace(_dispatcher.submit(
                std::move(dispatch), [this](AITaskResult result) { _results.emplace_back(std::move(result)); }));
    });

    ASSERT_TRUE(submitted.has_value());
    ASSERT_TRUE(submitted->ok()) << submitted->status();
    _handles.emplace_back(std::move(*submitted).value());
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(0, _http.submit_calls);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, ThrowingCancellationProbeBeforeClassificationFailsClosedAsCancellation) {
    std::atomic<bool> throw_from_probe = false;
    AIDispatchRequest dispatch = request(1, UniqueId{8, 6}, 1);
    const int64_t query_deadline_ns = dispatch.request_deadline_ns;
    dispatch.lifecycle = [&, query_deadline_ns]() -> AIQueryLifecycleSnapshot {
        if (throw_from_probe.load(std::memory_order_acquire)) throw 1;
        return {.monotonic_deadline_ns = query_deadline_ns};
    };
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(std::move(dispatch));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    throw_from_probe.store(true, std::memory_order_release);

    EXPECT_NO_THROW(_completion.run_until_idle());
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(0, _provider.parse_count);

    const AIRateLimitKey key =
            AIRateLimitKey::create("https://model.invalid/v1/chat", "secret-key", AICapability::CHAT);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::completion_in_use(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::attempt_count(_controller));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_registrations(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_inflight(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_owners(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::unresolved_completion_count(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::rate_pins(_controller, key));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::bucket_state_count(_controller));
}

TEST_F(AITaskDispatcherTest, ParsedSuccessReservationLivesThroughCallbackQueue) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "accounted-content"});
    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    FakeAIMemoryContext memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 20}, 1);
    memory.on_reserve() = [&](size_t bytes) {
        ++reserve_calls;
        reserved_bytes += bytes;
        return true;
    };
    memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    dispatch.memory = memory.context();

    submit_and_run_control(std::move(dispatch));
    complete_next_transport_and_run_control();
    _completion.run_until_idle();

    const size_t template_bytes = provider_request_template_bytes("https://model.invalid/v1/chat",
                                                                  _provider.request_headers, _provider.request_body);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));
    EXPECT_EQ("accounted-content", std::get<AITaskSuccess>(_results.front()).content());
    EXPECT_EQ(2, reserve_calls);
    EXPECT_EQ(template_bytes + std::string_view("accounted-content").size(), reserved_bytes);
    EXPECT_EQ(1, release_calls) << "only the terminal request template has been released";
    EXPECT_EQ(template_bytes, released_bytes);

    _results.clear();
    EXPECT_EQ(2, release_calls);
    EXPECT_EQ(reserved_bytes, released_bytes);
}

TEST_F(AITaskDispatcherTest, ProviderRequestTemplateReserveRejectionIsSanitizedBeforeAdmission) {
    constexpr std::string_view endpoint = "https://memory.invalid/v1/chat";
    _provider.request_headers = {{"Content-Type", "application/json"},
                                 {"Authorization", "Bearer request-secret-sentinel"}};
    _provider.request_body = R"({"prompt":"request-secret-sentinel"})";
    const size_t expected_bytes =
            provider_request_template_bytes(endpoint, _provider.request_headers, _provider.request_body);
    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    FakeAIMemoryContext memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 33}, 1, endpoint);
    memory.on_reserve() = [&](size_t bytes) {
        ++reserve_calls;
        reserved_bytes += bytes;
        return false;
    };
    memory.on_release() = [&](size_t) { ++release_calls; };
    dispatch.memory = memory.context();

    submit(std::move(dispatch));

    ASSERT_EQ(1, _results.size()) << "request-template memory rejection must complete synchronously";
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(1, reserve_calls);
    EXPECT_EQ(expected_bytes, reserved_bytes);
    EXPECT_EQ(0, release_calls) << "a rejected reservation owns no bytes";
    EXPECT_EQ(0, _http.submit_calls);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AITaskDispatcherTest, ProviderRequestTemplateReservationSurvivesRetryAndReleasesOnceAtTerminalResult) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 401);
    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    FakeAIMemoryContext memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 34}, 1);
    memory.on_reserve() = [&](size_t bytes) {
        ++reserve_calls;
        reserved_bytes += bytes;
        return true;
    };
    memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    dispatch.memory = memory.context();

    submit(std::move(dispatch));
    ASSERT_EQ(1, reserve_calls);
    EXPECT_GT(reserved_bytes, 0);
    EXPECT_EQ(0, release_calls);
    _control.run_until_idle();
    complete_next_transport_and_run_control();
    _completion.run_until_idle();

    EXPECT_TRUE(_results.empty());
    EXPECT_EQ(1, reserve_calls) << "retry backoff retains the original provider request template";
    EXPECT_EQ(0, release_calls);

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);
    EXPECT_EQ(1, reserve_calls) << "each HTTP attempt copy is accounted by the HTTP client, not the template owner";
    complete_next_transport_and_run_control();
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(1, release_calls);
    EXPECT_EQ(reserved_bytes, released_bytes);
    _handles.front().cancel();
    _handles.front().cancel();
    EXPECT_EQ(1, release_calls);
}

TEST_F(AITaskDispatcherTest, ProviderRequestTemplateReservationReleasesOnceWhenQueuedTaskIsCancelled) {
    _limits.inflight_cap = 0;
    size_t reserve_calls = 0;
    size_t reserved_bytes = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    FakeAIMemoryContext memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 35}, 1);
    memory.on_reserve() = [&](size_t bytes) {
        ++reserve_calls;
        reserved_bytes += bytes;
        return true;
    };
    memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    dispatch.memory = memory.context();

    submit(std::move(dispatch));
    ASSERT_EQ(1, reserve_calls);
    EXPECT_EQ(0, release_calls);
    _handles.front().cancel();
    _control.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, release_calls);
    EXPECT_EQ(reserved_bytes, released_bytes);
    _handles.front().cancel();
    _control.run_until_idle();
    EXPECT_EQ(1, release_calls);
    EXPECT_EQ(0, _http.submit_calls);
}

TEST_F(AITaskDispatcherTest, ParsedSuccessReservationHandoffsBeforeResponseBodyReservationIsReleased) {
    constexpr std::string_view response_content = "tracked-response";
    constexpr std::string_view parsed_content = "tracked-result";
    const size_t template_bytes = provider_request_template_bytes("https://model.invalid/v1/chat",
                                                                  _provider.request_headers, _provider.request_body);
    std::vector<std::string> events;
    size_t outstanding_bytes = response_content.size();
    events.emplace_back("response reserve");
    FakeAIMemoryContext response_memory;
    response_memory.on_release() = [&](size_t bytes) {
        events.emplace_back("response release");
        EXPECT_EQ(response_content.size() + parsed_content.size(), outstanding_bytes);
        outstanding_bytes -= bytes;
        EXPECT_GT(outstanding_bytes, 0);
    };
    AIHttpResponseBody response_body = AIHttpResponseBodyTestPeer::create(
            std::string(response_content), response_memory.context(), response_content.size());
    _http.push_http_body(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200, std::move(response_body));
    _provider.push_parse(AIProviderSuccess{.content = std::string(parsed_content)});

    FakeAIMemoryContext dispatch_memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 30}, 1);
    size_t dispatch_reserve_calls = 0;
    dispatch_memory.on_reserve() = [&](size_t bytes) {
        ++dispatch_reserve_calls;
        if (dispatch_reserve_calls == 1) {
            events.emplace_back("template reserve");
            EXPECT_EQ(template_bytes, bytes);
            EXPECT_EQ(response_content.size(), outstanding_bytes);
        } else {
            events.emplace_back("result reserve");
            EXPECT_EQ(parsed_content.size(), bytes);
            EXPECT_EQ(response_content.size() + template_bytes, outstanding_bytes);
        }
        outstanding_bytes += bytes;
        return true;
    };
    size_t dispatch_release_calls = 0;
    dispatch_memory.on_release() = [&](size_t bytes) {
        ++dispatch_release_calls;
        events.emplace_back(dispatch_release_calls == 1 ? "template release" : "result release");
        outstanding_bytes -= bytes;
    };
    dispatch.memory = dispatch_memory.context();
    _provider.set_parse_hook([&] { EXPECT_TRUE(dispatch_memory.in_physical_scope()); });
    submit_and_run_control(std::move(dispatch));
    complete_next_transport_and_run_control();
    _completion.run_until_idle();

    EXPECT_EQ((std::vector<std::string>{"response reserve", "template reserve", "result reserve", "template release",
                                        "response release"}),
              events);
    EXPECT_EQ(parsed_content.size(), outstanding_bytes);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));

    _results.clear();
    EXPECT_EQ((std::vector<std::string>{"response reserve", "template reserve", "result reserve", "template release",
                                        "response release", "result release"}),
              events);
    EXPECT_EQ(0, outstanding_bytes);
}

TEST_F(AITaskDispatcherTest, ParsedSuccessReserveFailuresAreFixedLocalResourceWithoutRetry) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "rejected"});
    _provider.push_parse(AIProviderSuccess{.content = "throwing"});
    size_t rejected_reserve_calls = 0;
    size_t throwing_reserve_calls = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    const size_t template_bytes = provider_request_template_bytes("https://model.invalid/v1/chat",
                                                                  _provider.request_headers, _provider.request_body);

    FakeAIMemoryContext rejected_memory;
    AIDispatchRequest rejected = request(1, UniqueId{9, 21}, 1);
    rejected_memory.on_reserve() = [&](size_t) {
        ++rejected_reserve_calls;
        return rejected_reserve_calls == 1;
    };
    rejected_memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    rejected.memory = rejected_memory.context();
    submit(std::move(rejected));

    FakeAIMemoryContext throwing_memory;
    AIDispatchRequest throwing = request(2, UniqueId{9, 22}, 2);
    throwing_memory.on_reserve() = [&](size_t) -> bool {
        ++throwing_reserve_calls;
        if (throwing_reserve_calls == 1) return true;
        throw 1;
    };
    throwing_memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    throwing.memory = throwing_memory.context();
    submit(std::move(throwing));

    _control.run_until_idle();
    ASSERT_EQ(2, _http.pending());
    _http.complete_next();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    ASSERT_EQ(2, _results.size());
    for (const AITaskResult& result : _results) {
        ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(result));
        EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(result).failure_class);
    }
    EXPECT_EQ(2, rejected_reserve_calls);
    EXPECT_EQ(2, throwing_reserve_calls);
    EXPECT_EQ(2, release_calls) << "only the two successfully reserved request templates are released";
    EXPECT_EQ(2 * template_bytes, released_bytes);

    _clock.advance_ns(10 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "parsed-result memory failures are never retried";
}

TEST_F(AITaskDispatcherTest, CancellationDuringParsedSuccessReserveReleasesBeforePublishingLifecycleResult) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "cancelled-content"});
    size_t reserve_calls = 0;
    size_t release_calls = 0;
    size_t released_bytes = 0;
    FakeAIMemoryContext memory;
    AIDispatchRequest dispatch = request(1, UniqueId{9, 23}, 1);
    memory.on_reserve() = [&](size_t) {
        ++reserve_calls;
        if (reserve_calls == 2) _handles.front().cancel();
        return true;
    };
    memory.on_release() = [&](size_t bytes) {
        ++release_calls;
        released_bytes += bytes;
    };
    dispatch.memory = memory.context();

    submit(std::move(dispatch));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    const size_t template_bytes = provider_request_template_bytes("https://model.invalid/v1/chat",
                                                                  _provider.request_headers, _provider.request_body);
    EXPECT_EQ(2, reserve_calls);
    EXPECT_EQ(2, release_calls);
    EXPECT_EQ(template_bytes + std::string_view("cancelled-content").size(), released_bytes);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
}

TEST_F(AITaskDispatcherTest, CancellationReplacementReleasesSuccessOutsideStateMutexAndAllowsReentrantCancel) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "reentrant-cancel"});
    size_t release_calls = 0;
    size_t reserve_calls = 0;
    FakeAIMemoryContext memory;
    bool callback_moved_in_scope = false;
    bool callback_cleared_in_scope = false;
    bool callback_invoked_outside_scope = false;
    sync_point->SetCallBack("AITaskState::_publish:callback_moved:in_physical_scope", [&](void* context) {
        if (*static_cast<AIMemoryContext*>(context) == memory.context()) {
            callback_moved_in_scope = true;
            EXPECT_TRUE(memory.in_physical_scope());
        }
    });
    sync_point->SetCallBack("AITaskState::_publish:callback_cleared:in_physical_scope", [&](void* context) {
        if (*static_cast<AIMemoryContext*>(context) == memory.context()) {
            callback_cleared_in_scope = true;
            EXPECT_TRUE(memory.in_physical_scope());
        }
    });
    _before_result_callback = [&] {
        callback_invoked_outside_scope = true;
        EXPECT_FALSE(memory.in_physical_scope());
    };
    AIDispatchRequest dispatch = request(1, UniqueId{9, 24}, 1);
    memory.on_reserve() = [&](size_t) {
        if (++reserve_calls == 2) _handles.front().cancel();
        return true;
    };
    memory.on_release() = [&](size_t) {
        ++release_calls;
        _handles.front().cancel();
    };
    dispatch.memory = memory.context();

    auto callback_destruction = std::make_shared<PhysicalScopeDestructionState>(&memory);
    AITaskCallback callback;
    run_in_memory_scope(memory.context(), [&] {
        callback = [this, callback_destruction,
                    observer = PhysicalScopeDestructionObserver(callback_destruction)](AITaskResult result) {
            if (_before_result_callback) _before_result_callback();
            _result_task_ids.emplace_back(1);
            _results.emplace_back(std::move(result));
        };
    });
    callback_destruction->reset();
    auto handle = _dispatcher.submit(std::move(dispatch), std::move(callback));
    ASSERT_TRUE(handle.ok()) << handle.status();
    _handles.emplace_back(std::move(handle).value());
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    EXPECT_EQ(2, release_calls);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_TRUE(callback_moved_in_scope);
    EXPECT_TRUE(callback_cleared_in_scope);
    EXPECT_TRUE(callback_invoked_outside_scope);
    EXPECT_GT(callback_destruction->destructions.load(std::memory_order_relaxed), 0);
    EXPECT_FALSE(callback_destruction->destroyed_outside_scope.load(std::memory_order_relaxed))
            << "every transferred callback target must be destroyed in its request physical scope";
    _before_result_callback = {};
}

TEST_F(AITaskDispatcherTest, ProviderParseExceptionIsFixedLocalResourceAndResolvesAttemptExactlyOnce) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.set_parse_hook([] { throw std::bad_alloc(); });
    submit(request(1, UniqueId{9, 25}, 1));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();

    EXPECT_NO_THROW(_completion.run_until_idle());
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);

    _provider.set_parse_hook({});
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-provider-exception"});
    submit(request(2, UniqueId{9, 26}, 2));
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls) << "the failed classification resolved its bucket guard";
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
    _clock.advance_ns(10 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "provider exceptions are never retried";
}

TEST_F(AITaskDispatcherTest, ProviderParseExceptionPastTaskDeadlinePublishesDeadlineBeforeLocalResource) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.set_parse_hook([this] {
        _clock.advance_ns(kSecond);
        throw std::bad_alloc();
    });
    AIDispatchRequest dispatch = request(1, UniqueId{9, 31}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();

    EXPECT_NO_THROW(_completion.run_until_idle());
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());

    _provider.set_parse_hook({});
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-deadline"});
    submit(request(2, UniqueId{9, 32}, 2));
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls) << "the deadline result resolved its bucket guard";
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherSingleCompletionTest,
       CancellationAfterInlineCallbackButBeforeSubmitReturnsCommitsAndReleasesExactlyOnce) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    bool cancelled_inside_submit = false;
    _http.after_inline_callback_before_return = [&] {
        _handles.front().cancel();
        cancelled_inside_submit = true;
        EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
        EXPECT_EQ(0, _completion.pending());
        EXPECT_EQ(0, _provider.parse_count);
        EXPECT_TRUE(_results.empty());
    };

    submit(request(7, UniqueId{9, 13}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_TRUE(_results.empty());

    _clock.advance_ns(kSecond);
    _control.run_until_idle();

    EXPECT_TRUE(cancelled_inside_submit);
    EXPECT_EQ(2, _http.accepted_attempts);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(1, _completion.pending()) << "accepted inline completion is handed off only after submit returns OK";
    EXPECT_TRUE(_results.empty());

    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ((std::vector<uint64_t>{1}), _result_task_ids);

    _handles.front().cancel();
    _handles.front().cancel();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, _results.size());
    EXPECT_EQ((std::vector<uint64_t>{1}), _result_task_ids)
            << "repeated cancellation cannot republish the terminal result";

    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-cancel"});
    submit(request(7, UniqueId{9, 14}, 2));
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "the accepted cancelled attempt still committed its same-bucket QPS token";

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(3, _http.submit_calls)
            << "inflight, completion capacity, and bucket resolution were each released exactly once";
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherTest, BuildsProviderRequestSynchronouslyBeforeBorrowedChatViewsExpire) {
    std::string endpoint = "https://owned.invalid/v1/chat";
    std::string model = "owned-model";
    std::string api_key = "owned-secret";
    std::string prompt = "owned-prompt";
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    bool http_submit_outside_scope = false;
    _http.before_submit = [&](const AIHttpRequest&) {
        http_submit_outside_scope = true;
        EXPECT_FALSE(_memory.in_physical_scope());
    };

    AIDispatchRequest dispatch = request(1, UniqueId{1, 2}, 1);
    dispatch.chat_request = AIChatRequest{.endpoint = endpoint, .model = model, .api_key = api_key, .prompt = prompt};
    _provider.set_build_hook([&](const AIChatRequest& request) {
        EXPECT_TRUE(_memory.in_physical_scope());
        EXPECT_EQ(api_key, request.api_key);
    });
    submit(std::move(dispatch));
    EXPECT_EQ(1, _provider.build_count);
    _provider.set_build_hook({});

    endpoint.assign("destroyed");
    model.assign("destroyed");
    api_key.assign("destroyed");
    prompt.assign("destroyed");
    _control.run_until_idle();
    _http.before_submit = {};
    EXPECT_TRUE(http_submit_outside_scope);
    ASSERT_EQ(1, _http.submitted_urls.size());
    EXPECT_EQ("https://owned.invalid/v1/chat", _http.submitted_urls.front());

    complete_next_transport_and_run_control();
    _completion.run_until_idle();
}

TEST_F(AITaskDispatcherTest, ZeroConnectTimeoutUsesCeilingOfRemainingTaskDeadline) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest dispatch = request(1, UniqueId{1, 20}, 1);
    dispatch.connect_timeout_ms = 0;
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + 1'500'001;
    submit_and_run_control(std::move(dispatch));

    ASSERT_EQ(1, _http.submitted_connect_timeouts_ms.size());
    EXPECT_EQ(2, _http.submitted_connect_timeouts_ms.front());
}

TEST_F(AITaskDispatcherTest, ZeroConnectTimeoutRoundsSubMillisecondRemainingDeadlineUpToOne) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest dispatch = request(1, UniqueId{1, 21}, 1);
    dispatch.connect_timeout_ms = 0;
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + 1;
    submit_and_run_control(std::move(dispatch));

    ASSERT_EQ(1, _http.submitted_connect_timeouts_ms.size());
    EXPECT_EQ(1, _http.submitted_connect_timeouts_ms.front());
}

TEST_F(AITaskDispatcherTest, ZeroConnectTimeoutHandlesMaximumDeadlineWithoutOverflow) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest dispatch = request(1, UniqueId{1, 22}, 1);
    dispatch.connect_timeout_ms = 0;
    dispatch.request_deadline_ns = std::numeric_limits<int64_t>::max();
    const int64_t remaining_ns = dispatch.request_deadline_ns - _clock.monotonic_now_ns();
    constexpr int64_t kNanosecondsPerMillisecond = 1'000'000;
    const int64_t expected_ms =
            remaining_ns / kNanosecondsPerMillisecond + (remaining_ns % kNanosecondsPerMillisecond != 0 ? 1 : 0);
    submit_and_run_control(std::move(dispatch));

    ASSERT_EQ(1, _http.submitted_connect_timeouts_ms.size());
    EXPECT_EQ(expected_ms, _http.submitted_connect_timeouts_ms.front());
}

TEST_F(AITaskDispatcherTest, PositiveConnectTimeoutIsForwardedUnchanged) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest dispatch = request(1, UniqueId{1, 23}, 1);
    dispatch.connect_timeout_ms = 37;
    submit_and_run_control(std::move(dispatch));

    ASSERT_EQ(1, _http.submitted_connect_timeouts_ms.size());
    EXPECT_EQ(37, _http.submitted_connect_timeouts_ms.front());
}

TEST_F(AITaskDispatcherTest, SynchronousRequestPreparationFailuresAreFixedLocalRowFailuresWithoutHttpAttempt) {
    {
        SCOPED_TRACE("missing memory context");
        AIDispatchRequest missing = request(1, UniqueId{9, 27}, 1);
        missing.memory = {};
        submit(std::move(missing));

        ASSERT_EQ(1, _results.size());
        ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
        EXPECT_EQ(AISanitizedFailureClass::LOCAL_REQUEST,
                  std::get<AISanitizedRowFailure>(_results.front()).failure_class);
        EXPECT_EQ(0, _provider.build_count);
        _results.clear();
        _result_task_ids.clear();
    }

    SCOPED_TRACE("provider build failure");
    _provider.set_build_status(Status::InvalidArgument("provider-build-secret-sentinel"));
    AIDispatchRequest provider_failure = request(1, UniqueId{1, 2}, 1);
    auto lifecycle_destruction = std::make_shared<PhysicalScopeDestructionState>(&_memory);
    const int64_t query_deadline_ns = provider_failure.request_deadline_ns;
    run_in_memory_scope(_memory.context(), [&] {
        provider_failure.lifecycle = [query_deadline_ns, lifecycle_destruction,
                                      observer = PhysicalScopeDestructionObserver(lifecycle_destruction)] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
        };
    });
    lifecycle_destruction->reset();
    submit_and_run_control(std::move(provider_failure));
    _completion.run_until_idle();

    EXPECT_EQ(0, _http.submit_calls);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_REQUEST, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_GT(lifecycle_destruction->destructions.load(std::memory_order_relaxed), 0);
    EXPECT_FALSE(lifecycle_destruction->destroyed_outside_scope.load(std::memory_order_relaxed))
            << "provider preparation failure must destroy the lifecycle probe in the request physical scope";
}

TEST_F(AITaskDispatcherSingleCompletionTest, SynchronousSubmitFailureRefundsTokenAndPermit) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    _http.push_sync_failure();
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "second-ok"});

    submit(request(1, UniqueId{1, 1}, 1));
    submit(request(2, UniqueId{2, 2}, 2));
    _control.run_until_idle();
    _completion.run_until_idle();

    EXPECT_EQ(2, _http.submit_calls);
    EXPECT_EQ(1, _http.accepted_attempts);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results[0]));
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results[1]));
}

TEST_F(AITaskDispatcherTest, MetricsCountOnlyAcceptedInitialAndRetryAttempts) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "ok"});

    submit_and_run_control(request(1, UniqueId{1, 7}, 1));
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());

    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value()) << "a scheduled retry is not an accepted HTTP attempt";

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_retries_total.value());

    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    _clock.advance_ns(2 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(3, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(2, _metrics.ai_http_retries_total.value());

    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));
}

TEST_F(AITaskDispatcherTest, MetricsDoNotCountRejectedOrCancelledRetryAttempts) {
    _http.push_sync_failure();
    submit(request(1, UniqueId{1, 8}, 1));
    _control.run_until_idle();
    EXPECT_EQ(0, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());

    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    submit(request(2, UniqueId{1, 9}, 2));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());

    _handles.back().cancel();
    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, MetricsCountEachAcceptedTransportTimeoutOnce) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::TIMEOUT);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "ok"});

    submit_and_run_control(request(1, UniqueId{1, 10}, 1));
    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_retries_total.value());
    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, MetricsDoNotClassifyOtherTransportFailuresAsTimeout) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::DNS);
    submit_and_run_control(request(1, UniqueId{1, 15}, 1));
    complete_next_transport_and_run_control();
    _completion.run_until_idle();

    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
    _handles.front().cancel();
}

TEST_F(AITaskDispatcherTest, MetricsCountDeadlineOnlyAfterAnAttemptWasAccepted) {
    AIDispatchRequest expired = request(1, UniqueId{1, 11}, 1);
    expired.request_deadline_ns = _clock.monotonic_now_ns();
    submit_and_run_control(std::move(expired));
    EXPECT_EQ(0, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value()) << "admission deadline emitted no HTTP attempt";

    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::DEADLINE);
    submit_and_run_control(request(2, UniqueId{1, 12}, 2));
    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, LiveQueryExtensionAllowsBackoffWithoutCountingTimeout) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest dispatch = request(1, UniqueId{1, 16}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + 20 * kSecond;
    auto live_query_deadline_ns = std::make_shared<std::atomic<int64_t>>(_clock.monotonic_now_ns() + kSecond);
    dispatch.lifecycle = [live_query_deadline_ns] {
        return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = live_query_deadline_ns->load()};
    };
    submit(std::move(dispatch));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    live_query_deadline_ns->store(_clock.monotonic_now_ns() + 10 * kSecond);
    _completion.run_until_idle();
    EXPECT_TRUE(_results.empty());

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AITaskSuccess>(_results.front()));
    EXPECT_EQ(2, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_retries_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, MetricsDoNotClassifyCancellationOrShutdownAsTimeout) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(1, UniqueId{1, 13}, 1));
    _control.run_until_idle();
    _handles.front().cancel();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::SHUTDOWN);
    submit(request(2, UniqueId{1, 14}, 2));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    EXPECT_EQ(2, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, SynchronousSubmitShutdownRefundsAdmissionAndReturnsLifecycleShutdown) {
    _limits.inflight_cap = 1;
    _http.push_sync_shutdown();
    submit_and_run_control(request(1, UniqueId{1, 3}, 1));

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, _completion.pending());
}

TEST_F(AITaskDispatcherTest, MapsSynchronousSubmitStatusCodesToTypedResults) {
    _http.push_sync_status(Status::Shutdown("native HTTP client is stopping"));
    _http.push_sync_status(Status::MemoryLimitExceeded("request allocation failed"));
    _http.push_sync_status(Status::InvalidArgument("request is invalid"));
    submit(request(1, UniqueId{1, 4}, 1));
    submit(request(2, UniqueId{1, 5}, 2));
    submit(request(3, UniqueId{1, 6}, 3));
    _control.run_until_idle();

    ASSERT_EQ(3, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results[0]));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results[0]).reason);
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results[1]));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results[1]).failure_class);
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results[2]));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_REQUEST, std::get<AISanitizedRowFailure>(_results[2]).failure_class);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, _completion.pending());
}

TEST_F(AITaskDispatcherSingleCompletionTest, SubmitBadAllocPublishesLocalResourceAndRefundsAdmission) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::THROW_BAD_ALLOC, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-submit-exception"});

    submit(request(1, UniqueId{4, 1}, 1));
    submit(request(2, UniqueId{4, 2}, 2));
    EXPECT_NO_THROW(_control.run_until_idle());

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(2, _http.submit_calls) << "the throwing submit refunds its uncommitted token and permit";
    EXPECT_EQ(1, _http.pending());
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AITaskDispatcherSingleCompletionTest, InlineCompletionThenThrowReleasesPayloadAndPublishesOnce) {
    _limits.chat_qps = 1;
    _limits.inflight_cap = 1;
    size_t response_release_calls = 0;
    FakeAIMemoryContext response_memory;
    response_memory.on_release() = [&](size_t) { ++response_release_calls; };
    AIHttpResponseBody response_body = AIHttpResponseBodyTestPeer::create("inline-response", response_memory.context(),
                                                                          std::string_view("inline-response").size());
    _http.push_http_body(ScriptedAIHttpClient::Mode::INLINE_COMPLETION_THEN_THROW, 200, std::move(response_body));
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-inline-exception"});

    submit(request(1, UniqueId{4, 3}, 1));
    submit(request(2, UniqueId{4, 4}, 2));
    EXPECT_NO_THROW(_control.run_until_idle());

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(1, response_release_calls);
    EXPECT_EQ(2, _http.submit_calls) << "the inline callback cannot retain the throwing attempt's admission grant";
    EXPECT_EQ(1, _http.pending());

    _handles.front().cancel();
    _handles.front().cancel();
    _control.run_until_idle();
    EXPECT_EQ(1, _results.size()) << "the submit exception publishes exactly one terminal result";
    EXPECT_EQ(1, response_release_calls);

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AITaskDispatcherTest, SharesOneRetryOrdinalAcrossOrdinaryAndThrottleFailures) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _provider.push_parse(AIProviderStructuredError{.code = AIProviderErrorCode::RATE_LIMIT_EXCEEDED});

    submit(request(1, UniqueId{1, 1}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_TRUE(_results.empty());
    EXPECT_EQ(0, _provider.parse_count);

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_TRUE(_results.empty());
    EXPECT_EQ(1, _provider.parse_count);

    _clock.advance_ns(2 * kSecond);
    _control.run_until_idle();
    ASSERT_EQ(3, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(3, _http.submit_calls) << "the final 503 cannot reuse the exhausted ordinary retry allowance";
}

TEST_F(AITaskDispatcherTest, SerializesSharedRandomSourceAcrossConcurrentCompletions) {
    ConcurrentEntryDetectingAIRandom random;
    AITaskDispatcher first_dispatcher(&_controller, &_http, &_provider, &_completion, &_clock, &random, &_metrics,
                                      AITaskDispatcherOptions{.max_retries = 1, .max_throttle_retries = 1});
    AITaskDispatcher second_dispatcher(&_controller, &_http, &_provider, &_completion, &_clock, &random, &_metrics,
                                       AITaskDispatcherOptions{.max_retries = 1, .max_throttle_retries = 1});
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);

    AITaskDispatcher* dispatchers[] = {&first_dispatcher, &second_dispatcher};
    for (size_t index = 0; index < std::size(dispatchers); ++index) {
        const uint64_t task_id = index + 1;
        auto handle = dispatchers[index]->submit(request(task_id, UniqueId{2, static_cast<int64_t>(task_id)}, task_id),
                                                 [this, task_id](AITaskResult result) {
                                                     _result_task_ids.emplace_back(task_id);
                                                     _results.emplace_back(std::move(result));
                                                 });
        ASSERT_TRUE(handle.ok()) << handle.status();
        _handles.emplace_back(std::move(handle).value());
    }
    _control.run_until_idle();
    ASSERT_EQ(2, _http.pending());
    _http.complete_next();
    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(2, _completion.pending());

    AICompletionWork first_work = _completion.take_one();
    AICompletionWork second_work = _completion.take_one();
    std::barrier start(3);
    std::thread first([&start, work = std::move(first_work)]() mutable {
        start.arrive_and_wait();
        work.run();
    });
    std::thread second([&start, work = std::move(second_work)]() mutable {
        start.arrive_and_wait();
        work.run();
    });
    start.arrive_and_wait();
    first.join();
    second.join();

    EXPECT_FALSE(random.concurrent_entry()) << "the process-shared AIRandom dependency is not thread-safe by contract";
    _handles[0].cancel();
    _handles[1].cancel();
    _control.run_until_idle();
}

TEST_F(AITaskDispatcherTest, Http429DoesNotParseAndSuppressedRetryReturnsTypedDeadline) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    AIDispatchRequest dispatch = request(1, UniqueId{2, 2}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit_and_run_control(std::move(dispatch));
    ASSERT_EQ(1, _http.submit_calls);

    complete_next_transport_and_run_control();
    _completion.run_until_idle();
    EXPECT_EQ(0, _provider.parse_count) << "HTTP 429 is classified before provider body parsing";
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _http.submit_calls);
}

TEST_F(AITaskDispatcherTest, DeadlineSuppressedThrottleDoesNotCoolSharedBucket) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest throttled = request(1, UniqueId{2, 3}, 1, "https://shared.invalid/v1/chat");
    throttled.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(throttled));
    submit(request(2, UniqueId{2, 4}, 2, "https://shared.invalid/v1/chat"));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls) << "a throttle cooldown cannot outlive the triggering task deadline";
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
}

TEST_F(AITaskDispatcherTest, ExhaustedThrottleDoesNotCoolBeyondTaskDeadline) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 429, "10");
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    AIDispatchRequest throttled = request(1, UniqueId{2, 5}, 1, "https://shared.invalid/v1/chat");
    throttled.request_deadline_ns = _clock.monotonic_now_ns() + 6 * kSecond;
    submit(std::move(throttled));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    _clock.advance_ns(2 * kSecond);
    _control.run_until_idle();
    ASSERT_EQ(3, _http.submit_calls);

    submit(request(2, UniqueId{2, 6}, 2, "https://shared.invalid/v1/chat"));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    _control.run_until_idle();
    ASSERT_EQ(4, _http.submit_calls)
            << "an exhausted throttle cannot install either Retry-After or local backoff beyond its task deadline";
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
}

TEST_F(AITaskDispatcherTest, TransportCompletionReleasesPermitBeforeParseOrRetryBackoff) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "small"});
    _provider.push_parse(AIProviderSuccess{.content = "retry"});

    submit(request(1, UniqueId{1, 1}, 1, "https://large.invalid/v1/chat"));
    submit(request(2, UniqueId{2, 2}, 2, "https://small.invalid/v1/chat"));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);

    _http.complete_next();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, _provider.parse_count);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "the small query is admitted before response processing/backoff";

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, _provider.parse_count) << "503 classification must not parse its response body";
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(2, _http.submit_calls);

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(3, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
}

TEST_F(AITaskDispatcherTest, StructuredThrottleBlocksOnlyItsBucketUntilClassificationInstallsCooldown) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderStructuredError{.code = AIProviderErrorCode::RATE_LIMIT_EXCEEDED});
    _provider.push_parse(AIProviderSuccess{.content = "other-bucket"});
    _provider.push_parse(AIProviderSuccess{.content = "same-bucket-small"});

    submit(request(1, UniqueId{1, 1}, 1, "https://shared.invalid/v1/chat"));
    submit(request(2, UniqueId{2, 2}, 2, "https://shared.invalid/v1/chat"));
    submit(request(3, UniqueId{3, 3}, 3, "https://other.invalid/v1/chat"));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);

    _http.complete_next();
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "unresolved classification blocks only the completed attempt's bucket";
    EXPECT_EQ("https://other.invalid/v1/chat", _http.submitted_urls.back());
    EXPECT_EQ(0, _provider.parse_count);

    _completion.run_one();
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls);
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_one();
    EXPECT_EQ((std::vector<uint64_t>{3}), _result_task_ids);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "same-bucket work remains behind the installed cooldown";

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(3, _http.submit_calls);
    EXPECT_EQ("https://shared.invalid/v1/chat", _http.submitted_urls.back());
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_one();
    ASSERT_EQ(2, _result_task_ids.size());
    EXPECT_EQ(2, _result_task_ids.back()) << "the waiting small query precedes the throttled retry tail";
}

TEST_F(AITaskDispatcherTest, OrdinaryRetryAfterDoesNotBecomeSharedBucketCooldown) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503, "10");
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "small"});

    submit(request(1, UniqueId{1, 1}, 1, "https://shared.invalid/v1/chat"));
    submit(request(2, UniqueId{2, 2}, 2, "https://shared.invalid/v1/chat"));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.submit_calls);

    _http.complete_next();
    _control.run_until_idle();
    EXPECT_EQ(1, _http.submit_calls) << "the unresolved completion guard is still active";
    _completion.run_one();
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);
    EXPECT_EQ(0, _provider.parse_count) << "503 is classified without parsing";

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_one();
    ASSERT_EQ((std::vector<uint64_t>{2}), _result_task_ids);
    _clock.advance_ns(9 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "Retry-After delays only the original task";
}

TEST_F(AITaskDispatcherTest, CancellationDuringBackoffPreventsRetryAndReturnsLifecycleResult) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 500);
    submit(request(1, UniqueId{3, 4}, 1));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_TRUE(_results.empty());

    _handles.front().cancel();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    EXPECT_EQ(1, _http.submit_calls);
}

TEST_F(AITaskDispatcherTest, ConcurrentRetryReentryCannotReplaceTheCurrentAdmissionTicketWithAStaleTicket) {
    AITaskDispatcher dispatcher(&_controller, &_http, &_provider, &_completion, &_clock, &_random, &_metrics,
                                AITaskDispatcherOptions{.max_retries = 3, .max_throttle_retries = 3});
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 503);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 503);

    std::mutex coordination_mutex;
    std::condition_variable coordination_cv;
    std::atomic<bool> arm_retry_store_block = false;
    std::atomic<bool> retry_store_blocked = false;
    bool first_retry_registered = false;
    bool release_first_retry_store = false;

    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AITaskState::_finish_admission:before_ticket_store", [&](void*) {
        if (!arm_retry_store_block.load(std::memory_order_acquire) || retry_store_blocked.exchange(true)) return;
        std::unique_lock lock(coordination_mutex);
        first_retry_registered = true;
        coordination_cv.notify_all();
        coordination_cv.wait(lock, [&] { return release_first_retry_store; });
    });

    auto submitted = dispatcher.submit(request(1, UniqueId{4, 5}, 1), [this](AITaskResult result) {
        _result_task_ids.emplace_back(1);
        _results.emplace_back(std::move(result));
    });
    ASSERT_TRUE(submitted.ok()) << submitted.status();
    _handles.emplace_back(std::move(submitted).value());

    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());
    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());

    arm_retry_store_block.store(true, std::memory_order_release);
    std::thread first_retry_thread([&] { _completion.run_one(); });
    {
        std::unique_lock lock(coordination_mutex);
        coordination_cv.wait(lock, [&] { return first_retry_registered; });
    }

    _clock.advance_ns(kSecond);
    _control.run_until_idle();
    ASSERT_EQ(2, _http.submit_calls);
    ASSERT_EQ(1, _completion.pending());
    _completion.run_one();

    {
        std::lock_guard lock(coordination_mutex);
        release_first_retry_store = true;
        coordination_cv.notify_all();
    }
    first_retry_thread.join();
    ASSERT_TRUE(_results.empty());

    _handles.front().cancel();
    _control.run_until_idle();
    ASSERT_EQ(1, _results.size())
            << "cancellation must target the second retry ticket, not the first retry's stale returned ticket";
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(2, _http.submit_calls) << "the cancelled future retry must never reach HTTP";
}

TEST_F(AITaskDispatcherTest, LogicalCancellationKeepsPermitUntilTransportCompletion) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    submit(request(1, UniqueId{5, 6}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));

    _handles.front().cancel();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_TRUE(_results.empty());

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(0, _provider.parse_count);
}

TEST_F(AITaskDispatcherTest, ResponseCapacityAndMemoryFailuresAreFixedLocalResourceFailures) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::RESPONSE_CAP);
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::MEMORY_LIMIT);
    submit(request(1, UniqueId{5, 7}, 1));
    submit(request(2, UniqueId{5, 8}, 2));
    _control.run_until_idle();
    ASSERT_EQ(2, _http.pending());

    _http.complete_next();
    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();

    ASSERT_EQ(2, _results.size());
    for (const AITaskResult& result : _results) {
        ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(result));
        EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(result).failure_class);
    }
    EXPECT_EQ(0, _provider.parse_count);
    _clock.advance_ns(10 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls) << "local capacity failures are never retried";
}

TEST_F(AITaskDispatcherTest, ConcurrentCancellationAndTransportCompletionProduceOneTerminalResult) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-win-cancellation"});
    submit(request(1, UniqueId{6, 7}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _http.pending());

    std::barrier start(3);
    std::thread cancel_thread([&] {
        start.arrive_and_wait();
        _handles.front().cancel();
    });
    std::thread completion_thread([&] {
        start.arrive_and_wait();
        _http.complete_next();
    });
    start.arrive_and_wait();
    cancel_thread.join();
    completion_thread.join();

    _control.run_until_idle();
    _completion.run_until_idle();
    _control.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));

    _handles.front().cancel();
    _handles.front().cancel();
    _control.run_until_idle();
    _completion.run_until_idle();
    EXPECT_EQ(1, _results.size()) << "cancellation after a terminal result is idempotent";
}

TEST_F(AITaskDispatcherTest, CancellationObservedAfterAdmissionCannotTerminateAnAcceptedFiringAttempt) {
    _limits.inflight_cap = 1;
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);

    std::mutex coordination_mutex;
    std::condition_variable coordination_cv;
    bool ticket_store_entered = false;
    bool allow_ticket_store = false;
    bool http_submit_entered = false;
    bool allow_http_submit_return = false;
    std::atomic<bool> logical_cancelled = false;
    std::atomic<bool> ticket_store_blocked = false;

    AIDispatchRequest dispatch = request(1, UniqueId{6, 8}, 1);
    const int64_t query_deadline_ns = dispatch.request_deadline_ns;
    dispatch.lifecycle = [&, query_deadline_ns] {
        return AIQueryLifecycleSnapshot{.cancelled = logical_cancelled.load(),
                                        .monotonic_deadline_ns = query_deadline_ns};
    };
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    sync_point->SetCallBack("AITaskState::_finish_admission:before_ticket_store", [&](void*) {
        if (ticket_store_blocked.exchange(true)) return;
        std::unique_lock lock(coordination_mutex);
        ticket_store_entered = true;
        coordination_cv.notify_all();
        coordination_cv.wait(lock, [&] { return allow_ticket_store; });
    });
    _http.before_pending_return = [&] {
        std::unique_lock lock(coordination_mutex);
        http_submit_entered = true;
        coordination_cv.notify_all();
        coordination_cv.wait(lock, [&] { return allow_http_submit_return; });
    };

    std::optional<StatusOr<AITaskHandle>> submitted_handle;
    std::thread submit_thread([&] {
        submitted_handle.emplace(_dispatcher.submit(std::move(dispatch), [this](AITaskResult result) {
            _result_task_ids.emplace_back(1);
            _results.emplace_back(std::move(result));
        }));
    });
    {
        std::unique_lock lock(coordination_mutex);
        coordination_cv.wait(lock, [&] { return ticket_store_entered; });
    }

    std::thread control_thread([&] { _control.run_until_idle(); });
    {
        std::unique_lock lock(coordination_mutex);
        coordination_cv.wait(lock, [&] { return http_submit_entered; });
        logical_cancelled.store(true);
        allow_ticket_store = true;
        coordination_cv.notify_all();
    }
    submit_thread.join();

    ASSERT_TRUE(submitted_handle.has_value());
    ASSERT_TRUE(submitted_handle->ok()) << submitted_handle->status();
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_TRUE(_results.empty()) << "FIRING cancellation must wait for the accepted transport completion";

    {
        std::lock_guard lock(coordination_mutex);
        allow_http_submit_return = true;
        coordination_cv.notify_all();
    }
    control_thread.join();
    EXPECT_EQ(1, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_TRUE(_results.empty());

    _http.complete_next();
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(1, _results.size());
    EXPECT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    EXPECT_EQ(0, _provider.parse_count);
}

TEST_F(AITaskDispatcherTest, SaturatedCompletionHandoffReturnsFixedLocalFailureWithoutInlineParseOrRetry) {
    _completion.set_capacity(0);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-parse"});
    submit(request(1, UniqueId{7, 8}, 1));
    _control.run_until_idle();

    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);

    _clock.advance_ns(10 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(1, _http.submit_calls);
    EXPECT_EQ(1, _results.size());

    _completion.set_capacity(16);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-rejection"});
    submit(request(2, UniqueId{9, 10}, 2));
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls)
            << "rejected handoff releases both the completion reservation and the same-bucket resolution";
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherTest, CompletionHandoffBadAllocPublishesLocalResourceAndReleasesAdmission) {
    _completion.set_throw_bad_alloc_on_submit(true);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-parse"});
    submit(request(1, UniqueId{9, 10}, 1));

    EXPECT_NO_THROW(_control.run_until_idle());
    EXPECT_EQ(0, _completion.pending());
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);

    _completion.set_throw_bad_alloc_on_submit(false);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-bad-alloc"});
    submit(request(2, UniqueId{9, 11}, 2));
    _control.run_until_idle();
    EXPECT_EQ(2, _http.submit_calls)
            << "failed handoff releases both the completion reservation and the same-bucket resolution";
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherTest, CompletionClassificationExceptionPublishesLocalResourceAndReleasesAdmission) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->SetCallBack("AITaskState::CompletionEnvelope::run:before_classify",
                            [](void*) { throw std::bad_alloc(); });
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->DisableProcessing();
        sync_point->ClearAllCallBacks();
    });

    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-parse"});
    submit(request(1, UniqueId{10, 11}, 1));
    _control.run_until_idle();

    EXPECT_NO_THROW(_completion.run_until_idle());
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results.front()));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results.front()).failure_class);

    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "after-classification-failure"});
    submit(request(2, UniqueId{10, 12}, 2));
    _control.run_until_idle();
    _completion.run_until_idle();
    ASSERT_EQ(2, _results.size());
    EXPECT_TRUE(std::holds_alternative<AITaskSuccess>(_results.back()));
}

TEST_F(AITaskDispatcherTest, StoppingCompletionHandoffReturnsLifecycleShutdownExactlyOnce) {
    _completion.set_stopping(true);
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    submit(request(1, UniqueId{9, 10}, 1));
    _control.run_until_idle();

    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results.front()).reason);

    _clock.advance_ns(10 * kSecond);
    _control.run_until_idle();
    EXPECT_EQ(1, _http.submit_calls);
    EXPECT_EQ(1, _results.size());
}

TEST_F(AITaskDispatcherTest, MapsExactCompletionRejectionStatusCodesToTypedResults) {
    _completion.set_reject_status(Status::Shutdown("completion executor is shut down"));
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    submit(request(1, UniqueId{9, 11}, 1));
    _control.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results[0]));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results[0]).reason);

    _completion.set_reject_status(Status::MemoryLimitExceeded("completion queue memory limit"));
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    submit(request(2, UniqueId{9, 12}, 2));
    _control.run_until_idle();

    ASSERT_EQ(2, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results[1]));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results[1]).failure_class);

    _completion.set_reject_status(Status::ResourceBusy("completion queue is full"));
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    submit(request(3, UniqueId{9, 13}, 3));
    _control.run_until_idle();

    ASSERT_EQ(3, _results.size());
    ASSERT_TRUE(std::holds_alternative<AISanitizedRowFailure>(_results[2]));
    EXPECT_EQ(AISanitizedFailureClass::LOCAL_RESOURCE, std::get<AISanitizedRowFailure>(_results[2]).failure_class);
    EXPECT_EQ(0, _provider.parse_count);
    EXPECT_EQ(0, AIAdmissionControllerTestPeer::inflight(_controller));
}

TEST_F(AITaskDispatcherTest, ExecutorStopCancelsAlreadyQueuedCompletionWorkExactlyOnce) {
    _http.push_http(ScriptedAIHttpClient::Mode::INLINE_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-parse"});
    submit(request(1, UniqueId{11, 12}, 1));
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    EXPECT_TRUE(_results.empty());

    _completion.stop_and_cancel_queued();
    EXPECT_EQ(0, _provider.parse_count);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results.front()).reason);

    _completion.run_until_idle();
    _control.run_until_idle();
    EXPECT_EQ(1, _results.size());
}

TEST_F(AITaskDispatcherTest, CompletionRejectReleasesResponseExactlyOnceWhenReleaseReentersWorkCancellation) {
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->ClearAllCallBacks();
    sync_point->EnableProcessing();
    SCOPED_CLEANUP({
        sync_point->ClearAllCallBacks();
        sync_point->DisableProcessing();
    });
    bool envelope_constructed_in_scope = false;
    bool callables_constructed_in_scope = false;
    bool callables_moved_in_scope = false;
    bool callables_cleared_in_scope = false;
    sync_point->SetCallBack("AITaskState::_handoff:completion_envelope_constructed:in_physical_scope", [&](void*) {
        envelope_constructed_in_scope = true;
        EXPECT_TRUE(_memory.in_physical_scope());
    });
    sync_point->SetCallBack("AICompletionWork::AICompletionWork:callables_constructed:in_physical_scope",
                            [&](void* context) {
                                if (*static_cast<AIMemoryContext*>(context) == _memory.context()) {
                                    callables_constructed_in_scope = true;
                                    EXPECT_TRUE(_memory.in_physical_scope());
                                }
                            });
    sync_point->SetCallBack("AICompletionWork::_move_from:callables_moved:in_physical_scope", [&](void* context) {
        if (*static_cast<AIMemoryContext*>(context) == _memory.context()) {
            callables_moved_in_scope = true;
            EXPECT_TRUE(_memory.in_physical_scope());
        }
    });
    sync_point->SetCallBack("AICompletionWork::_clear:callables_cleared:in_physical_scope", [&](void* context) {
        if (*static_cast<AIMemoryContext*>(context) == _memory.context()) {
            callables_cleared_in_scope = true;
            EXPECT_TRUE(_memory.in_physical_scope());
        }
    });

    size_t release_calls = 0;
    AICompletionWork* work_to_reenter = nullptr;
    FakeAIMemoryContext response_memory;
    response_memory.on_release() = [&](size_t) {
        ++release_calls;
        ASSERT_NE(nullptr, work_to_reenter);
        work_to_reenter->cancel();
    };
    AIHttpResponseBody response_body = AIHttpResponseBodyTestPeer::create("tracked-response", response_memory.context(),
                                                                          std::string_view("tracked-response").size());
    _http.push_http_body(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200, std::move(response_body));
    submit(request(1, UniqueId{11, 20}, 1));
    _control.run_until_idle();
    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());

    AICompletionWork work = _completion.take_one();
    work_to_reenter = &work;
    work.cancel();

    EXPECT_TRUE(envelope_constructed_in_scope);
    EXPECT_TRUE(callables_constructed_in_scope);
    EXPECT_TRUE(callables_moved_in_scope);
    EXPECT_TRUE(callables_cleared_in_scope);
    EXPECT_EQ(1, release_calls);
    EXPECT_EQ(0, _provider.parse_count);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results.front()).reason);
}

TEST_F(AITaskDispatcherTest, QueuedTransportShutdownWinsOverElapsedTaskDeadline) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::SHUTDOWN);
    AIDispatchRequest dispatch = request(1, UniqueId{11, 14}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _clock.advance_ns(kSecond);
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::SHUTDOWN, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, QueuedTransportCancellationWinsOverElapsedTaskDeadline) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::CANCELLATION);
    AIDispatchRequest dispatch = request(1, UniqueId{11, 15}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _clock.advance_ns(kSecond);
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, MetricsCountQueuedTransportTimeoutPastTaskDeadlineOnce) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::TIMEOUT);
    AIDispatchRequest dispatch = request(1, UniqueId{11, 16}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _clock.advance_ns(kSecond);
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, MetricsCountQueuedTransportDeadlinePastTaskDeadlineOnce) {
    _http.push_no_response(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, AIHttpNoResponseCode::DEADLINE);
    AIDispatchRequest dispatch = request(1, UniqueId{11, 17}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _clock.advance_ns(kSecond);
    _completion.run_until_idle();

    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_retries_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, ProviderParseCrossingTaskDeadlinePublishesDeadlineInsteadOfContent) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-publish"});
    _provider.set_parse_hook([this] { _clock.advance_ns(kSecond); });
    AIDispatchRequest dispatch = request(1, UniqueId{11, 18}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _completion.run_until_idle();

    EXPECT_EQ(1, _provider.parse_count);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, HandleCancellationDuringProviderParseWinsOverElapsedTaskDeadline) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-publish"});
    _provider.set_parse_hook([this] {
        _handles.front().cancel();
        _clock.advance_ns(kSecond);
    });
    AIDispatchRequest dispatch = request(1, UniqueId{11, 19}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _completion.run_until_idle();

    EXPECT_EQ(1, _provider.parse_count);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::CANCELLED, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(0, _metrics.ai_http_timeouts_total.value());
}

TEST_F(AITaskDispatcherTest, CompletionQueuedPastQueryDeadlineSkipsProviderParsing) {
    _http.push_http(ScriptedAIHttpClient::Mode::PENDING_COMPLETION, 200);
    _provider.push_parse(AIProviderSuccess{.content = "must-not-parse"});
    AIDispatchRequest dispatch = request(1, UniqueId{11, 13}, 1);
    dispatch.request_deadline_ns = _clock.monotonic_now_ns() + kSecond / 2;
    submit(std::move(dispatch));
    _control.run_until_idle();

    _http.complete_next();
    _control.run_until_idle();
    ASSERT_EQ(1, _completion.pending());
    _clock.advance_ns(kSecond);
    _completion.run_until_idle();

    EXPECT_EQ(0, _provider.parse_count);
    ASSERT_EQ(1, _results.size());
    ASSERT_TRUE(std::holds_alternative<AILifecycleCancelled>(_results.front()));
    EXPECT_EQ(AILifecycleReason::DEADLINE, std::get<AILifecycleCancelled>(_results.front()).reason);
    EXPECT_EQ(1, _metrics.ai_http_requests_total.value());
    EXPECT_EQ(1, _metrics.ai_http_timeouts_total.value());
}

} // namespace
} // namespace starrocks
