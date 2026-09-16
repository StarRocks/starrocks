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
#include <mutex>
#include <string>
#include <utility>

#include "base/status.h"
#include "base/statusor.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_memory.h"

namespace starrocks {

struct AIRuntimeConfig {
    int64_t request_timeout_ms = 600000;
    int64_t connect_timeout_ms = 10000;
    int64_t max_response_bytes = 8388608;
    int32_t worker_thread_num = 16;
    int32_t sub_chunk_size = 64;
    int32_t max_retries = 3;
    int32_t max_retries_on_throttle = 5;
    std::string on_error = "ignore";
    int32_t rate_limit_qps_chat = 128;
    int32_t max_inflight = 512;

    Status validate() const;
};

// Publishes complete immutable snapshots so readers never combine fields from different updates.
class AIRuntimeConfigSource final : public AIAdmissionLimitSource {
public:
    class PreparedUpdate {
    public:
        PreparedUpdate(const PreparedUpdate&) = delete;
        PreparedUpdate& operator=(const PreparedUpdate&) = delete;
        PreparedUpdate(PreparedUpdate&&) noexcept = default;
        PreparedUpdate& operator=(PreparedUpdate&&) noexcept = default;

    private:
        friend class AIRuntimeConfigSource;

        explicit PreparedUpdate(std::shared_ptr<const AIRuntimeConfig> snapshot) : _snapshot(std::move(snapshot)) {}

        std::shared_ptr<const AIRuntimeConfig> _snapshot;
    };

    static StatusOr<std::unique_ptr<AIRuntimeConfigSource>> create(AIRuntimeConfig initial = AIRuntimeConfig{});

    AIRuntimeConfigSource(const AIRuntimeConfigSource&) = delete;
    AIRuntimeConfigSource& operator=(const AIRuntimeConfigSource&) = delete;

    AIRuntimeConfig snapshot() const;
    StatusOr<PreparedUpdate> prepare(AIRuntimeConfig candidate) const;
    void publish(PreparedUpdate prepared) noexcept;
    Status update(AIRuntimeConfig candidate);

    int32_t worker_thread_num() const;
    int64_t qps(AICapability capability) const noexcept override;
    int64_t max_inflight() const override;

private:
    explicit AIRuntimeConfigSource(std::shared_ptr<const AIRuntimeConfig> initial);
    std::shared_ptr<const AIRuntimeConfig> _load_snapshot() const noexcept;

    std::shared_ptr<const AIRuntimeConfig> _snapshot;
};

int ai_completion_capacity(int64_t max_inflight, int64_t worker_threads);

class SystemAIClock final : public AIClock {
public:
    int64_t monotonic_now_ns() const noexcept override;
    int64_t unix_now_seconds() const noexcept override;
};

class AIRandom {
public:
    virtual ~AIRandom() = default;

    // Serializes calls on a shared random source and returns a value in [0, exclusive_upper).
    uint32_t uniform(uint32_t exclusive_upper) {
        std::lock_guard lock(_mutex);
        return uniform_unlocked(exclusive_upper);
    }

protected:
    virtual uint32_t uniform_unlocked(uint32_t exclusive_upper) = 0;

private:
    std::mutex _mutex;
};

class SystemAIRandom final : public AIRandom {
public:
    SystemAIRandom();
    ~SystemAIRandom() override;

protected:
    uint32_t uniform_unlocked(uint32_t exclusive_upper) override;

private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};

class AICompletionWork {
public:
    AICompletionWork() = default;
    ~AICompletionWork() noexcept;

    AICompletionWork(std::function<void()> run, std::function<void()> cancel);

    template <typename Run, typename Cancel>
    AICompletionWork(AIMemoryContext memory, Run&& run, Cancel&& cancel) : _memory(std::move(memory)) {
        auto initialize = [&] {
            try {
                _run = std::function<void()>(std::forward<Run>(run));
                _cancel = std::function<void()>(std::forward<Cancel>(cancel));
                _callables_constructed_for_test();
            } catch (...) {
                _run = {};
                _cancel = {};
                throw;
            }
        };
        _memory.run_in_physical_scope([](void* opaque) { (*static_cast<decltype(initialize)*>(opaque))(); },
                                      std::addressof(initialize));
    }

    AICompletionWork(const AICompletionWork&) = delete;
    AICompletionWork& operator=(const AICompletionWork&) = delete;
    AICompletionWork(AICompletionWork&& other) noexcept;
    AICompletionWork& operator=(AICompletionWork&& other) noexcept;

    void run() noexcept;
    void cancel() noexcept;

private:
    void _callables_constructed_for_test();
    void _clear() noexcept;
    void _invoke(std::function<void()> AICompletionWork::*selected) noexcept;
    void _move_from(AICompletionWork& other) noexcept;

    AIMemoryContext _memory;
    std::function<void()> _run;
    std::function<void()> _cancel;
};

class AICompletionExecutor {
public:
    virtual ~AICompletionExecutor() = default;

    // Accepted work never runs inline and is later resolved exactly once through run() or cancel(). On a non-OK
    // result, the executor retains no work and invokes neither callback. ResourceBusy reports queue saturation;
    // Shutdown is reserved for the executor lifecycle.
    virtual Status try_submit(AICompletionWork work) = 0;
};

class AIControlThreadScheduler final : public AIControlScheduler {
public:
    static StatusOr<std::unique_ptr<AIControlThreadScheduler>> create();
    // Destruction is an external owner operation and must not happen from a managed control task.
    ~AIControlThreadScheduler() override;

    Status post(Task task) override;
    StatusOr<TaskId> schedule_at(int64_t monotonic_time_ns, Task task) override;
    void cancel(TaskId id) override;
    void shutdown_and_drain() override;

private:
    class Impl;

    explicit AIControlThreadScheduler(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> _impl;
};

class AIThreadPoolCompletionExecutor final : public AICompletionExecutor {
public:
    static StatusOr<std::unique_ptr<AIThreadPoolCompletionExecutor>> create(int worker_threads, int queue_capacity);
    // Destruction is an external owner operation and must not happen from managed run() or cancel() callbacks.
    ~AIThreadPoolCompletionExecutor() override;

    Status try_submit(AICompletionWork work) override;
    Status update_worker_threads(int worker_threads);
    // This synchronous barrier is an external owner operation. It must not be invoked from managed run() or cancel()
    // callbacks.
    void shutdown();

private:
    class Impl;

    explicit AIThreadPoolCompletionExecutor(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> _impl;
};

} // namespace starrocks
