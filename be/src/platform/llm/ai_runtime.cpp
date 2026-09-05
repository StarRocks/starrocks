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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <new>
#include <random>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "common/logging.h"
#include "common/thread/thread.h"
#include "common/thread/threadpool.h"

namespace starrocks {

namespace {

enum class AIRuntimeLifecycle : uint8_t { ACCEPTING, STOPPING, STOPPED };

thread_local const void* tls_ai_completion_callback_owner = nullptr;

class AICompletionCallbackScope {
public:
    explicit AICompletionCallbackScope(const void* owner)
            : _previous(std::exchange(tls_ai_completion_callback_owner, owner)) {}
    ~AICompletionCallbackScope() { tls_ai_completion_callback_owner = _previous; }

private:
    const void* _previous;
};

template <typename Function>
void run_in_physical_scope(const AIMemoryContext& memory, Function&& function) {
    using FunctionType = std::remove_reference_t<Function>;
    FunctionType* function_pointer = std::addressof(function);
    memory.run_in_physical_scope(
            [](void* context) {
                auto* action = static_cast<FunctionType*>(context);
                std::invoke(*action);
            },
            function_pointer);
}

void clear_completion_callable(const AIMemoryContext& memory, std::function<void()>* callable) noexcept {
    try {
        run_in_physical_scope(memory, [&] {
            *callable = {};
            TEST_SYNC_POINT_CALLBACK("AICompletionWork::_clear:callables_cleared:in_physical_scope",
                                     const_cast<AIMemoryContext*>(&memory));
        });
    } catch (...) {
        *callable = {};
    }
}

} // namespace

Status AIRuntimeConfig::validate() const {
    if (request_timeout_ms < 0) {
        return Status::InvalidArgument("ai_function_request_timeout_ms must be nonnegative");
    }
    if (connect_timeout_ms < 0) {
        return Status::InvalidArgument("ai_function_connect_timeout_ms must be nonnegative");
    }
    if (max_response_bytes <= 0) {
        return Status::InvalidArgument("ai_function_max_response_bytes must be positive");
    }
    if (worker_thread_num <= 0) {
        return Status::InvalidArgument("ai_function_worker_thread_num must be positive");
    }
    if (sub_chunk_size <= 0) {
        return Status::InvalidArgument("ai_function_sub_chunk_size must be positive");
    }
    if (max_retries < 0) {
        return Status::InvalidArgument("ai_function_max_retries must be nonnegative");
    }
    if (max_retries_on_throttle < 0) {
        return Status::InvalidArgument("ai_function_max_retries_on_throttle must be nonnegative");
    }
    if (on_error != "ignore" && on_error != "fail") {
        return Status::InvalidArgument("ai_function_on_error must be ignore or fail");
    }
    if (rate_limit_qps_chat <= 0) {
        return Status::InvalidArgument("ai_function_rate_limit_qps_chat must be positive");
    }
    if (max_inflight <= 0) {
        return Status::InvalidArgument("ai_function_max_inflight must be positive");
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<AIRuntimeConfigSource>> AIRuntimeConfigSource::create(AIRuntimeConfig initial) {
    RETURN_IF_ERROR(initial.validate());
    try {
        auto snapshot = std::make_shared<const AIRuntimeConfig>(std::move(initial));
        return std::unique_ptr<AIRuntimeConfigSource>(new AIRuntimeConfigSource(std::move(snapshot)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("failed to allocate AI runtime config snapshot");
    }
}

AIRuntimeConfigSource::AIRuntimeConfigSource(std::shared_ptr<const AIRuntimeConfig> initial)
        : _snapshot(std::move(initial)) {}

std::shared_ptr<const AIRuntimeConfig> AIRuntimeConfigSource::_load_snapshot() const noexcept {
    return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
}

AIRuntimeConfig AIRuntimeConfigSource::snapshot() const {
    return *_load_snapshot();
}

Status AIRuntimeConfigSource::update(AIRuntimeConfig candidate) {
    ASSIGN_OR_RETURN(auto prepared, prepare(std::move(candidate)));
    publish(std::move(prepared));
    return Status::OK();
}

StatusOr<AIRuntimeConfigSource::PreparedUpdate> AIRuntimeConfigSource::prepare(AIRuntimeConfig candidate) const {
    RETURN_IF_ERROR(candidate.validate());
    try {
        TEST_SYNC_POINT("AIRuntimeConfigSource::prepare:before_snapshot_allocation");
        auto snapshot = std::make_shared<const AIRuntimeConfig>(std::move(candidate));
        return PreparedUpdate(std::move(snapshot));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("failed to allocate AI runtime config snapshot");
    }
}

void AIRuntimeConfigSource::publish(PreparedUpdate prepared) noexcept {
    DCHECK(prepared._snapshot != nullptr);
    std::atomic_store_explicit(&_snapshot, std::move(prepared._snapshot), std::memory_order_release);
}

int32_t AIRuntimeConfigSource::worker_thread_num() const {
    return _load_snapshot()->worker_thread_num;
}

int64_t AIRuntimeConfigSource::qps(AICapability capability) const noexcept {
    switch (capability) {
    case AICapability::CHAT:
        return _load_snapshot()->rate_limit_qps_chat;
    }
    return 0;
}

int64_t AIRuntimeConfigSource::max_inflight() const {
    return _load_snapshot()->max_inflight;
}

int ai_completion_capacity(int64_t max_inflight, int64_t worker_threads) {
    constexpr int64_t kMaximumCapacity = std::numeric_limits<int>::max();
    const int64_t bounded_inflight = std::clamp<int64_t>(max_inflight, 0, kMaximumCapacity);
    const int64_t bounded_workers = std::clamp<int64_t>(worker_threads, 0, kMaximumCapacity);
    if (bounded_inflight > kMaximumCapacity - bounded_workers) {
        return static_cast<int>(kMaximumCapacity);
    }
    return static_cast<int>(bounded_inflight + bounded_workers);
}

int64_t SystemAIClock::monotonic_now_ns() const noexcept {
    return MonotonicNanos();
}

int64_t SystemAIClock::unix_now_seconds() const noexcept {
    return UnixSeconds();
}

class SystemAIRandom::Impl {
public:
    Impl() : engine(std::random_device{}()) {}

    std::mt19937 engine;
};

SystemAIRandom::SystemAIRandom() : _impl(std::make_unique<Impl>()) {}

SystemAIRandom::~SystemAIRandom() = default;

uint32_t SystemAIRandom::uniform_unlocked(uint32_t exclusive_upper) {
    if (exclusive_upper == 0) return 0;
    return std::uniform_int_distribution<uint32_t>(0, exclusive_upper - 1)(_impl->engine);
}

AICompletionWork::AICompletionWork(std::function<void()> run, std::function<void()> cancel)
        : _run(std::move(run)), _cancel(std::move(cancel)) {}

AICompletionWork::~AICompletionWork() noexcept {
    _clear();
}

AICompletionWork::AICompletionWork(AICompletionWork&& other) noexcept {
    _move_from(other);
}

AICompletionWork& AICompletionWork::operator=(AICompletionWork&& other) noexcept {
    if (this != &other) {
        _clear();
        _move_from(other);
    }
    return *this;
}

void AICompletionWork::_move_from(AICompletionWork& other) noexcept {
    AIMemoryContext memory = other._memory;
    bool moved = false;
    try {
        run_in_physical_scope(memory, [&] {
            _memory = std::move(other._memory);
            _run = std::move(other._run);
            _cancel = std::move(other._cancel);
            other._run = {};
            other._cancel = {};
            other._memory = {};
            moved = true;
            TEST_SYNC_POINT_CALLBACK("AICompletionWork::_move_from:callables_moved:in_physical_scope", &memory);
        });
    } catch (...) {
        // A conforming AIMemoryContext runner cannot fail a noexcept move action. Keep move operations noexcept if a
        // foreign runner violates that contract before invoking the action.
        if (!moved) {
            _memory = std::move(other._memory);
            _run = std::move(other._run);
            _cancel = std::move(other._cancel);
            other._run = {};
            other._cancel = {};
            other._memory = {};
        }
    }
}

void AICompletionWork::_callables_constructed_for_test() {
    TEST_SYNC_POINT_CALLBACK("AICompletionWork::AICompletionWork:callables_constructed:in_physical_scope", &_memory);
}

void AICompletionWork::_clear() noexcept {
    if (!_run && !_cancel) {
        _memory = {};
        return;
    }

    AIMemoryContext memory = _memory;
    bool cleared = false;
    try {
        run_in_physical_scope(memory, [&] {
            _run = {};
            _cancel = {};
            _memory = {};
            cleared = true;
            TEST_SYNC_POINT_CALLBACK("AICompletionWork::_clear:callables_cleared:in_physical_scope", &memory);
        });
    } catch (...) {
        if (!cleared) {
            _run = {};
            _cancel = {};
            _memory = {};
        }
    }
}

void AICompletionWork::_invoke(std::function<void()> AICompletionWork::*selected) noexcept {
    if (!_run && !_cancel) {
        _memory = {};
        return;
    }

    AIMemoryContext memory = _memory;
    std::function<void()> work;
    bool extracted = false;
    try {
        run_in_physical_scope(memory, [&] {
            work = std::move(this->*selected);
            _run = {};
            _cancel = {};
            _memory = {};
            extracted = true;
            TEST_SYNC_POINT_CALLBACK("AICompletionWork::_clear:callables_cleared:in_physical_scope", &memory);
        });
    } catch (...) {
        clear_completion_callable(memory, &work);
        if (!extracted) _clear();
        LOG(WARNING) << "AI completion work extraction failed";
        return;
    }

    try {
        if (work) work();
    } catch (...) {
        clear_completion_callable(memory, &work);
        LOG(WARNING) << "AI completion callback threw an exception";
        return;
    }
    clear_completion_callable(memory, &work);
}

void AICompletionWork::run() noexcept {
    _invoke(&AICompletionWork::_run);
}

void AICompletionWork::cancel() noexcept {
    _invoke(&AICompletionWork::_cancel);
}

class AIControlThreadScheduler::Impl {
public:
    using TimerKey = std::pair<int64_t, TaskId>;

    Status start() {
        return Thread::create(
                "ai", "ai-control", [this] { run(); }, &thread);
    }

    Status post(Task task) {
        std::lock_guard lock(mutex);
        if (lifecycle != AIRuntimeLifecycle::ACCEPTING) {
            return Status::Shutdown("AI control scheduler is stopping");
        }
        try {
            TEST_SYNC_POINT("AIControlThreadScheduler::post:before_ready_emplace");
            ready.emplace_back();
            ready.back() = std::move(task);
        } catch (const std::bad_alloc&) {
            return Status::MemoryLimitExceeded("failed to allocate AI control work");
        }
        cv.notify_one();
        return Status::OK();
    }

    StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) {
        std::lock_guard lock(mutex);
        if (lifecycle != AIRuntimeLifecycle::ACCEPTING) {
            return Status::Shutdown("AI control scheduler is stopping");
        }

        TaskId id;
        do {
            id = ++next_task_id;
        } while (id == 0 || timer_keys.contains(id));
        const TimerKey key{monotonic_time_ns, id};
        auto timer = timers.end();
        try {
            TEST_SYNC_POINT("AIControlThreadScheduler::schedule_at:before_timer_emplace");
            auto [inserted_timer, inserted] = timers.try_emplace(key);
            DCHECK(inserted);
            timer = inserted_timer;
            timer->second = std::move(task);

            TEST_SYNC_POINT("AIControlThreadScheduler::schedule_at:before_timer_key_emplace");
            auto [_, inserted_key] = timer_keys.emplace(id, key);
            DCHECK(inserted_key);
        } catch (const std::bad_alloc&) {
            if (timer != timers.end()) {
                timers.erase(timer);
            }
            return Status::MemoryLimitExceeded("failed to allocate AI control timer");
        }
        cv.notify_one();
        return id;
    }

    void cancel(TaskId id) {
        std::lock_guard lock(mutex);
        auto key = timer_keys.find(id);
        if (key == timer_keys.end()) return;
        timers.erase(key->second);
        timer_keys.erase(key);
        cv.notify_one();
    }

    void shutdown_and_drain() {
        CHECK_NE(Thread::current_thread(), thread.get())
                << "AI control scheduler cannot be shut down from its managed control thread";
        std::unique_lock shutdown_lock(shutdown_mutex);
        {
            std::lock_guard lock(mutex);
            if (lifecycle == AIRuntimeLifecycle::STOPPED) return;
            if (lifecycle == AIRuntimeLifecycle::ACCEPTING) {
                lifecycle = AIRuntimeLifecycle::STOPPING;
                timers.clear();
                timer_keys.clear();
                cv.notify_one();
            }
        }
        thread->join();
    }

private:
    static void run_task(Task task) noexcept {
        try {
            task();
        } catch (...) {
            LOG(WARNING) << "AI control task threw an exception";
        }
    }

    void run() {
        std::unique_lock lock(mutex);
        for (;;) {
            std::optional<Task> allocation_fallback;
            const int64_t now_ns = MonotonicNanos();
            while (!timers.empty() && timers.begin()->first.first <= now_ns) {
                auto timer = timers.extract(timers.begin());
                timer_keys.erase(timer.key().second);
                try {
                    TEST_SYNC_POINT("AIControlThreadScheduler::run:before_ready_emplace");
                    ready.emplace_back();
                    ready.back() = std::move(timer.mapped());
                } catch (const std::bad_alloc&) {
                    // The extracted node retains ownership until the ready allocation succeeds. Under allocation
                    // pressure, run this timer directly on the control thread outside the scheduler mutex. This
                    // preserves exactly-once progress but intentionally does not promise strict ready/timer FIFO.
                    allocation_fallback.emplace(std::move(timer.mapped()));
                    break;
                }
            }

            if (allocation_fallback.has_value()) {
                Task task = std::move(*allocation_fallback);
                lock.unlock();
                run_task(std::move(task));
                lock.lock();
                continue;
            }

            if (!ready.empty()) {
                Task task = std::move(ready.front());
                ready.pop_front();
                lock.unlock();
                run_task(std::move(task));
                lock.lock();
                continue;
            }

            if (lifecycle == AIRuntimeLifecycle::STOPPING) {
                lifecycle = AIRuntimeLifecycle::STOPPED;
                return;
            }

            if (timers.empty()) {
                cv.wait(lock);
            } else {
                const int64_t current_ns = MonotonicNanos();
                const int64_t remaining_ns =
                        timers.begin()->first.first <= current_ns ? 0 : timers.begin()->first.first - current_ns;
                cv.wait_for(lock, std::chrono::nanoseconds(remaining_ns));
            }
        }
    }

public:
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<Task> ready;
    std::map<TimerKey, Task> timers;
    std::unordered_map<TaskId, TimerKey> timer_keys;
    TaskId next_task_id = 0;
    AIRuntimeLifecycle lifecycle = AIRuntimeLifecycle::ACCEPTING;
    scoped_refptr<Thread> thread;
    std::mutex shutdown_mutex;
};

StatusOr<std::unique_ptr<AIControlThreadScheduler>> AIControlThreadScheduler::create() {
    auto impl = std::make_unique<Impl>();
    RETURN_IF_ERROR(impl->start());
    return std::unique_ptr<AIControlThreadScheduler>(new AIControlThreadScheduler(std::move(impl)));
}

AIControlThreadScheduler::AIControlThreadScheduler(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}

AIControlThreadScheduler::~AIControlThreadScheduler() {
    shutdown_and_drain();
}

Status AIControlThreadScheduler::post(Task task) {
    return _impl->post(std::move(task));
}

StatusOr<AIControlScheduler::TaskId> AIControlThreadScheduler::schedule_at(int64_t monotonic_time_ns, Task task) {
    return _impl->schedule_at(monotonic_time_ns, std::move(task));
}

void AIControlThreadScheduler::cancel(TaskId id) {
    _impl->cancel(id);
}

void AIControlThreadScheduler::shutdown_and_drain() {
    _impl->shutdown_and_drain();
}

class AIThreadPoolCompletionExecutor::Impl {
public:
    Status try_submit(AICompletionWork work) {
        std::lock_guard lock(mutex);
        if (lifecycle != AIRuntimeLifecycle::ACCEPTING) {
            return Status::Shutdown("AI completion executor is stopping");
        }

        std::shared_ptr<CancellableRunnable> runnable;
        try {
            auto shared_work = std::make_shared<AICompletionWork>(std::move(work));
            runnable = std::make_shared<CancellableRunnable>(
                    [this, shared_work]() noexcept {
                        AICompletionCallbackScope callback_scope(this);
                        bool should_run = false;
                        {
                            std::lock_guard lock(mutex);
                            should_run = lifecycle == AIRuntimeLifecycle::ACCEPTING;
                        }
                        if (should_run) {
                            shared_work->run();
                        } else {
                            shared_work->cancel();
                        }
                    },
                    [this, shared_work]() noexcept {
                        AICompletionCallbackScope callback_scope(this);
                        shared_work->cancel();
                    });
        } catch (const std::bad_alloc&) {
            return Status::MemoryLimitExceeded("failed to allocate AI completion work");
        }
        Status status = thread_pool->submit(std::move(runnable));
        if (status.is_service_unavailable()) {
            return Status::ResourceBusy("AI completion executor queue is full");
        }
        return status;
    }

    Status update_worker_threads(int worker_threads) {
        if (worker_threads <= 0) {
            return Status::InvalidArgument("AI completion executor worker count must be positive");
        }
        std::lock_guard lock(mutex);
        if (lifecycle != AIRuntimeLifecycle::ACCEPTING) {
            return Status::Shutdown("AI completion executor is stopping");
        }
        return thread_pool->update_max_threads(worker_threads);
    }

    void shutdown() {
        CHECK_NE(tls_ai_completion_callback_owner, this)
                << "AI completion executor cannot be shut down from its managed work callback";
        std::unique_lock lock(mutex);
        if (lifecycle == AIRuntimeLifecycle::STOPPED) return;
        if (lifecycle == AIRuntimeLifecycle::STOPPING) {
            stopped.wait(lock, [this] { return lifecycle == AIRuntimeLifecycle::STOPPED; });
            return;
        }
        lifecycle = AIRuntimeLifecycle::STOPPING;
        ThreadPool* pool = thread_pool.get();
        lock.unlock();

        pool->shutdown();

        lock.lock();
        lifecycle = AIRuntimeLifecycle::STOPPED;
        lock.unlock();
        stopped.notify_all();
    }

    std::mutex mutex;
    std::condition_variable stopped;
    AIRuntimeLifecycle lifecycle = AIRuntimeLifecycle::ACCEPTING;
    std::unique_ptr<ThreadPool> thread_pool;
};

StatusOr<std::unique_ptr<AIThreadPoolCompletionExecutor>> AIThreadPoolCompletionExecutor::create(int worker_threads,
                                                                                                 int queue_capacity) {
    if (worker_threads <= 0) {
        return Status::InvalidArgument("AI completion executor worker count must be positive");
    }
    if (queue_capacity <= 0) {
        return Status::InvalidArgument("AI completion executor queue capacity must be positive");
    }

    auto impl = std::make_unique<Impl>();
    RETURN_IF_ERROR(ThreadPoolBuilder("ai-completion")
                            .set_min_threads(0)
                            .set_max_threads(worker_threads)
                            .set_max_queue_size(queue_capacity)
                            .build(&impl->thread_pool));
    return std::unique_ptr<AIThreadPoolCompletionExecutor>(new AIThreadPoolCompletionExecutor(std::move(impl)));
}

AIThreadPoolCompletionExecutor::AIThreadPoolCompletionExecutor(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}

AIThreadPoolCompletionExecutor::~AIThreadPoolCompletionExecutor() {
    shutdown();
}

Status AIThreadPoolCompletionExecutor::try_submit(AICompletionWork work) {
    return _impl->try_submit(std::move(work));
}

Status AIThreadPoolCompletionExecutor::update_worker_threads(int worker_threads) {
    return _impl->update_worker_threads(worker_threads);
}

void AIThreadPoolCompletionExecutor::shutdown() {
    _impl->shutdown();
}

} // namespace starrocks
