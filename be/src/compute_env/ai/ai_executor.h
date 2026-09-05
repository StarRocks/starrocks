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

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "base/status.h"
#include "base/statusor.h"
#include "platform/llm/ai_runtime.h"

namespace starrocks {

class AIAdmissionController;
class AIControlScheduler;
class AIHttpClient;

// Owns the single process-wide AI runtime graph. ComputeEnv is the intended owner; this object is neither a singleton
// nor a WorkGroup resource. Accessors are non-owning and remain valid through the synchronous shutdown barrier.
class AIExecutor final {
public:
    static StatusOr<std::unique_ptr<AIExecutor>> create(AIRuntimeConfig config = AIRuntimeConfig{});

    AIExecutor(const AIExecutor&) = delete;
    AIExecutor& operator=(const AIExecutor&) = delete;
    // Destruction and shutdown are external owner operations. They must not run from callbacks managed by this runtime.
    ~AIExecutor();

    AIClock* clock() const { return _clock.get(); }
    const AIRuntimeConfigSource* config_source() const { return _config_source.get(); }
    AIRandom* random() const { return _random.get(); }
    AIControlScheduler* control_scheduler() const { return _control_scheduler.get(); }
    AICompletionExecutor* completion_executor() const { return _completion_executor.get(); }
    AIHttpClient* http_client() const { return _http_client.get(); }
    AIAdmissionController* admission_controller() const { return _admission_controller.get(); }

    AIRuntimeConfig config_snapshot() const { return _config_source->snapshot(); }
    int completion_capacity() const { return _completion_capacity; }

    Status update_request_timeout_ms(int64_t value);
    Status update_connect_timeout_ms(int64_t value);
    Status update_max_response_bytes(int64_t value);
    Status update_worker_thread_num(int32_t value);
    Status update_sub_chunk_size(int32_t value);
    Status update_max_retries(int32_t value);
    Status update_max_retries_on_throttle(int32_t value);
    Status update_on_error(std::string_view value);
    Status update_rate_limit_qps_chat(int32_t value);
    Status update_max_inflight(int32_t value);

    // Linearizes with config updates, rejects future work, and synchronously resolves all accepted work. Resources stay
    // allocated in a rejecting state until destruction so late query-context cleanup cannot observe dangling facades.
    void shutdown();

private:
    enum class Lifecycle : uint8_t { ACCEPTING, STOPPING, STOPPED };
    enum class ConfigField : uint8_t {
        REQUEST_TIMEOUT_MS,
        CONNECT_TIMEOUT_MS,
        MAX_RESPONSE_BYTES,
        WORKER_THREAD_NUM,
        SUB_CHUNK_SIZE,
        MAX_RETRIES,
        MAX_RETRIES_ON_THROTTLE,
        RATE_LIMIT_QPS_CHAT,
        MAX_INFLIGHT,
    };

    AIExecutor(int completion_capacity, std::unique_ptr<SystemAIClock> clock,
               std::unique_ptr<AIRuntimeConfigSource> config_source, std::unique_ptr<SystemAIRandom> random,
               std::unique_ptr<AIControlThreadScheduler> control_scheduler,
               std::unique_ptr<AIThreadPoolCompletionExecutor> completion_executor,
               std::unique_ptr<AIHttpClient> http_client,
               std::unique_ptr<AIAdmissionController> admission_controller) noexcept;

    Status _update_integer(ConfigField field, int64_t value);
    Status _publish_locked(AIRuntimeConfig candidate, bool worker_changed);

    const int _completion_capacity;
    // Declaration order intentionally produces dependency-safe reverse destruction after shutdown().
    std::unique_ptr<SystemAIClock> _clock;
    std::unique_ptr<AIRuntimeConfigSource> _config_source;
    std::unique_ptr<SystemAIRandom> _random;
    std::unique_ptr<AIControlThreadScheduler> _control_scheduler;
    std::unique_ptr<AIThreadPoolCompletionExecutor> _completion_executor;
    std::unique_ptr<AIHttpClient> _http_client;
    std::unique_ptr<AIAdmissionController> _admission_controller;

    std::mutex _lifecycle_mutex;
    std::condition_variable _stopped;
    Lifecycle _lifecycle = Lifecycle::ACCEPTING;
};

} // namespace starrocks
