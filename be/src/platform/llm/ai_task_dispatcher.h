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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "base/status.h"
#include "base/statusor.h"
#include "base/uid_util.h"
#include "platform/llm/ai_admission_controller.h"
#include "platform/llm/ai_http_client.h"
#include "platform/llm/ai_provider.h"
#include "platform/llm/ai_runtime.h"

namespace starrocks {

enum class AIAttemptAction : uint8_t { SUCCEEDED, RETRY, THROTTLE, TERMINAL };

AIAttemptAction classify_ai_no_response(AIHttpNoResponseCode code);
AIAttemptAction classify_ai_http_response(int64_t status_code, const AIProviderParseResult& provider_result);
bool ai_should_retry(size_t retry_ordinal, AIAttemptAction action, int max_retries, int max_throttle_retries);
int64_t ai_retry_backoff_ns(size_t retry_ordinal, uint32_t jitter_basis_points);
std::optional<int64_t> ai_retry_eligible_at_ns(int64_t monotonic_now_ns, int64_t wall_now_seconds,
                                               int64_t monotonic_deadline_ns, size_t retry_ordinal,
                                               uint32_t jitter_basis_points,
                                               std::optional<std::string_view> retry_after = std::nullopt);

class AITaskSuccess {
public:
    ~AITaskSuccess() noexcept;

    AITaskSuccess(const AITaskSuccess&) = delete;
    AITaskSuccess& operator=(const AITaskSuccess&) = delete;
    AITaskSuccess(AITaskSuccess&& other) noexcept;
    AITaskSuccess& operator=(AITaskSuccess&& other) noexcept;

    static StatusOr<AITaskSuccess> create(std::string content, AIMemoryContext memory);

    std::string_view content() const noexcept { return _content; }

private:
    AITaskSuccess(std::string content, AIMemoryContext memory, size_t reserved_bytes) noexcept;
    void _release() noexcept;

    std::string _content;
    AIMemoryContext _memory;
    size_t _reserved_bytes = 0;
};

enum class AISanitizedFailureClass : uint8_t { LOCAL_REQUEST, LOCAL_RESOURCE, TRANSPORT, PROVIDER_RESPONSE };

struct AISanitizedRowFailure {
    AISanitizedFailureClass failure_class = AISanitizedFailureClass::PROVIDER_RESPONSE;
};

enum class AILifecycleReason : uint8_t { CANCELLED, DEADLINE, SHUTDOWN };

struct AILifecycleCancelled {
    AILifecycleReason reason = AILifecycleReason::CANCELLED;
};

using AITaskResult = std::variant<AITaskSuccess, AISanitizedRowFailure, AILifecycleCancelled>;
// Callbacks may run on the admission scheduler, a completion worker, or the native HTTP I/O rejection path. They must
// be O(1) and non-blocking, and may only publish terminal task state or wake an observer; downstream expression work
// must be scheduled by that observer, never inline here.
using AITaskCallback = std::function<void(AITaskResult)>;

struct AIDispatchRequest {
    AIWorkGroupKey workgroup_key;
    UniqueId query_id;
    uint64_t task_id = 0;
    AIChatRequest chat_request;
    int64_t request_deadline_ns = 0;
    int64_t connect_timeout_ms = 0;
    size_t max_response_bytes = 0;
    std::shared_ptr<const ResolvedHttpEndpoint> resolved_endpoint;
    AIQueryLifecycleProbe lifecycle;
    AIMemoryContext memory;
};

struct AITaskDispatcherOptions {
    int max_retries = 3;
    int max_throttle_retries = 5;
};

class AITaskState;
class AITaskDispatcherCore;
class AIMetrics;

class AITaskHandle {
public:
    AITaskHandle() = default;

    AITaskHandle(const AITaskHandle&) = delete;
    AITaskHandle& operator=(const AITaskHandle&) = delete;
    AITaskHandle(AITaskHandle&&) noexcept = default;
    AITaskHandle& operator=(AITaskHandle&&) noexcept = default;

    void cancel();

private:
    friend class AITaskDispatcher;

    explicit AITaskHandle(std::weak_ptr<AITaskState> state) : _state(std::move(state)) {}

    std::weak_ptr<AITaskState> _state;
};

class AITaskDispatcher {
public:
    // Injected dependencies must outlive the dispatcher and all task handles/completions created through it.
    AITaskDispatcher(AIAdmissionController* admission, AIHttpClient* http, const AIProvider* provider,
                     AICompletionExecutor* completion, const AIClock* clock, AIRandom* random, AIMetrics* metrics,
                     AITaskDispatcherOptions options = {});

    AITaskDispatcher(const AITaskDispatcher&) = delete;
    AITaskDispatcher& operator=(const AITaskDispatcher&) = delete;

    // Transfers both callback and request.lifecycle. Their source objects are
    // cleared in request.memory before this method returns on every path.
    StatusOr<AITaskHandle> submit(AIDispatchRequest&& request, AITaskCallback&& callback);

private:
    std::shared_ptr<AITaskDispatcherCore> _core;
};

} // namespace starrocks
