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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/utility/scoped_cleanup.h"
#include "common/logging.h"
#include "platform/llm/ai_metrics.h"

namespace starrocks {

namespace {

constexpr int64_t kNanosecondsPerSecond = 1'000'000'000;
constexpr uint64_t kNanosecondsPerMillisecond = 1'000'000;
constexpr uint32_t kMaximumJitterBasisPoints = 2500;

enum class RetryAfterParseKind : uint8_t { ABSENT_OR_INVALID, DELAY_SECONDS, VALUE_OVERFLOW };

struct RetryAfterParseResult {
    RetryAfterParseKind kind = RetryAfterParseKind::ABSENT_OR_INVALID;
    int64_t delay_seconds = 0;
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

void clear_task_callback(const AIMemoryContext& memory, AITaskCallback* callback) noexcept {
    try {
        run_in_physical_scope(memory, [&] {
            AITaskCallback().swap(*callback);
            TEST_SYNC_POINT_CALLBACK("AITaskState::_publish:callback_cleared:in_physical_scope",
                                     const_cast<AIMemoryContext*>(&memory));
        });
    } catch (...) {
        AITaskCallback().swap(*callback);
    }
}

void clear_lifecycle_probe(const AIMemoryContext& memory, AIQueryLifecycleProbe* lifecycle) noexcept {
    try {
        run_in_physical_scope(memory, [&] { AIQueryLifecycleProbe().swap(*lifecycle); });
    } catch (...) {
        AIQueryLifecycleProbe().swap(*lifecycle);
    }
}

void invoke_task_callback_noexcept(const AIMemoryContext& memory, AITaskCallback* callback,
                                   AITaskResult result) noexcept {
    try {
        if (*callback) {
            (*callback)(std::move(result));
        }
    } catch (...) {
        LOG(WARNING) << "AI task callback threw an exception";
    }
    clear_task_callback(memory, callback);
}

bool is_digit(char value) {
    return value >= '0' && value <= '9';
}

int64_t effective_connect_timeout_ms(int64_t configured_timeout_ms, int64_t monotonic_now_ns,
                                     int64_t request_deadline_ns) {
    if (configured_timeout_ms != 0) return configured_timeout_ms;
    if (request_deadline_ns == 0) return 0;
    if (request_deadline_ns <= monotonic_now_ns) return 1;

    const uint64_t remaining_ns = static_cast<uint64_t>(request_deadline_ns) - static_cast<uint64_t>(monotonic_now_ns);
    uint64_t remaining_ms = remaining_ns / kNanosecondsPerMillisecond;
    if (remaining_ns % kNanosecondsPerMillisecond != 0) ++remaining_ms;
    return static_cast<int64_t>(
            std::min<uint64_t>(remaining_ms, static_cast<uint64_t>(std::numeric_limits<long>::max())));
}

int parse_two_digits(std::string_view value, size_t offset) {
    if (offset + 2 > value.size() || !is_digit(value[offset]) || !is_digit(value[offset + 1])) return -1;
    return (value[offset] - '0') * 10 + value[offset + 1] - '0';
}

int parse_four_digits(std::string_view value, size_t offset) {
    if (offset + 4 > value.size()) return -1;
    int result = 0;
    for (size_t index = offset; index < offset + 4; ++index) {
        if (!is_digit(value[index])) return -1;
        result = result * 10 + value[index] - '0';
    }
    return result;
}

int parse_month(std::string_view value) {
    constexpr std::string_view months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    for (size_t index = 0; index < std::size(months); ++index) {
        if (value == months[index]) return static_cast<int>(index + 1);
    }
    return -1;
}

std::optional<int64_t> parse_imf_fixdate(std::string_view value) {
    // RFC 9110 IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT".
    if (value.size() != 29 || value.substr(3, 2) != ", " || value[7] != ' ' || value[11] != ' ' || value[16] != ' ' ||
        value[19] != ':' || value[22] != ':' || value.substr(25) != " GMT") {
        return std::nullopt;
    }

    const int day = parse_two_digits(value, 5);
    const int month = parse_month(value.substr(8, 3));
    const int year = parse_four_digits(value, 12);
    const int hour = parse_two_digits(value, 17);
    const int minute = parse_two_digits(value, 20);
    const int second = parse_two_digits(value, 23);
    if (day <= 0 || month <= 0 || year <= 0 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
        second > 59) {
        return std::nullopt;
    }

    using namespace std::chrono;
    const year_month_day date{std::chrono::year{year}, std::chrono::month{static_cast<unsigned>(month)},
                              std::chrono::day{static_cast<unsigned>(day)}};
    if (!date.ok()) return std::nullopt;

    const sys_days days{date};
    constexpr std::string_view weekdays[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    if (value.substr(0, 3) != weekdays[weekday{days}.c_encoding()]) return std::nullopt;

    return duration_cast<seconds>(days.time_since_epoch()).count() + hour * 3600 + minute * 60 + second;
}

RetryAfterParseResult parse_retry_after(std::optional<std::string_view> value, int64_t wall_now_seconds) {
    if (!value.has_value() || value->empty()) return {};

    bool decimal = true;
    uint64_t seconds = 0;
    for (char character : *value) {
        if (!is_digit(character)) {
            decimal = false;
            break;
        }
        const uint64_t digit = static_cast<uint64_t>(character - '0');
        if (seconds > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
            return {.kind = RetryAfterParseKind::VALUE_OVERFLOW};
        }
        seconds = seconds * 10 + digit;
    }
    if (decimal) {
        if (seconds > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / kNanosecondsPerSecond)) {
            return {.kind = RetryAfterParseKind::VALUE_OVERFLOW};
        }
        return {.kind = RetryAfterParseKind::DELAY_SECONDS, .delay_seconds = static_cast<int64_t>(seconds)};
    }

    const std::optional<int64_t> absolute_seconds = parse_imf_fixdate(*value);
    if (!absolute_seconds.has_value()) return {};
    if (*absolute_seconds <= wall_now_seconds) {
        return {.kind = RetryAfterParseKind::DELAY_SECONDS, .delay_seconds = 0};
    }
    if (wall_now_seconds < 0 && *absolute_seconds > std::numeric_limits<int64_t>::max() + wall_now_seconds) {
        return {.kind = RetryAfterParseKind::VALUE_OVERFLOW};
    }
    return {.kind = RetryAfterParseKind::DELAY_SECONDS, .delay_seconds = *absolute_seconds - wall_now_seconds};
}

} // namespace

AITaskSuccess::AITaskSuccess(std::string content, AIMemoryContext memory, size_t reserved_bytes) noexcept
        : _content(std::move(content)), _memory(std::move(memory)), _reserved_bytes(reserved_bytes) {}

AITaskSuccess::~AITaskSuccess() noexcept {
    _release();
}

AITaskSuccess::AITaskSuccess(AITaskSuccess&& other) noexcept
        : _content(std::move(other._content)),
          _memory(std::move(other._memory)),
          _reserved_bytes(std::exchange(other._reserved_bytes, 0)) {}

AITaskSuccess& AITaskSuccess::operator=(AITaskSuccess&& other) noexcept {
    if (this != &other) {
        _release();
        _content = std::move(other._content);
        _memory = std::move(other._memory);
        _reserved_bytes = std::exchange(other._reserved_bytes, 0);
    }
    return *this;
}

StatusOr<AITaskSuccess> AITaskSuccess::create(std::string content, AIMemoryContext memory) {
    const size_t bytes = content.size();
    if (bytes == 0 || !memory) {
        return AITaskSuccess(std::move(content), {}, 0);
    }

    if (!memory.reserve(bytes)) {
        return Status::MemoryLimitExceeded("AI parsed result memory limit exceeded");
    }
    return AITaskSuccess(std::move(content), std::move(memory), bytes);
}

void AITaskSuccess::_release() noexcept {
    const size_t bytes = std::exchange(_reserved_bytes, 0);
    AIMemoryContext memory = _memory;
    // Free the physical buffer before returning its accounting so a release hook can safely admit replacement work.
    try {
        run_in_physical_scope(memory, [&] {
            std::string().swap(_content);
            _memory = {};
        });
    } catch (...) {
        return;
    }
    memory.release(bytes);
}

AIAttemptAction classify_ai_no_response(AIHttpNoResponseCode code) {
    switch (code) {
    case AIHttpNoResponseCode::DNS:
    case AIHttpNoResponseCode::CONNECT:
    case AIHttpNoResponseCode::TIMEOUT:
    case AIHttpNoResponseCode::SEND:
    case AIHttpNoResponseCode::RECEIVE:
    case AIHttpNoResponseCode::EMPTY_REPLY:
    case AIHttpNoResponseCode::PARTIAL_TRANSFER:
    case AIHttpNoResponseCode::HTTP2_STREAM_RESET:
        return AIAttemptAction::RETRY;
    case AIHttpNoResponseCode::TLS_HANDSHAKE:
    case AIHttpNoResponseCode::TLS_VERIFICATION:
    case AIHttpNoResponseCode::CANCELLATION:
    case AIHttpNoResponseCode::DEADLINE:
    case AIHttpNoResponseCode::RESPONSE_CAP:
    case AIHttpNoResponseCode::MEMORY_LIMIT:
    case AIHttpNoResponseCode::SHUTDOWN:
    case AIHttpNoResponseCode::UNKNOWN:
        return AIAttemptAction::TERMINAL;
    }
    return AIAttemptAction::TERMINAL;
}

AIAttemptAction classify_ai_http_response(int64_t status_code, const AIProviderParseResult& provider_result) {
    if (status_code == 429) return AIAttemptAction::THROTTLE;
    if (status_code == 408 || status_code == 500 || status_code == 502 || status_code == 503 || status_code == 504) {
        return AIAttemptAction::RETRY;
    }
    if ((status_code < 200 || status_code >= 300) && status_code != 400) return AIAttemptAction::TERMINAL;

    if (const auto* error = std::get_if<AIProviderStructuredError>(&provider_result)) {
        switch (ai_provider_error_action(error->code)) {
        case AIProviderErrorAction::THROTTLED:
            return AIAttemptAction::THROTTLE;
        case AIProviderErrorAction::RETRYABLE:
            return AIAttemptAction::RETRY;
        case AIProviderErrorAction::TERMINAL:
            return AIAttemptAction::TERMINAL;
        }
        return AIAttemptAction::TERMINAL;
    }
    if (status_code >= 200 && status_code < 300 && std::holds_alternative<AIProviderSuccess>(provider_result)) {
        return AIAttemptAction::SUCCEEDED;
    }
    return AIAttemptAction::TERMINAL;
}

bool ai_should_retry(size_t retry_ordinal, AIAttemptAction action, int max_retries, int max_throttle_retries) {
    if (action == AIAttemptAction::RETRY) {
        return max_retries > 0 && retry_ordinal < static_cast<size_t>(max_retries);
    }
    if (action == AIAttemptAction::THROTTLE) {
        return max_throttle_retries > 0 && retry_ordinal < static_cast<size_t>(max_throttle_retries);
    }
    return false;
}

int64_t ai_retry_backoff_ns(size_t retry_ordinal, uint32_t jitter_basis_points) {
    constexpr size_t kMaximumShift = 5;
    const size_t shift = retry_ordinal == 0 ? 0 : std::min(retry_ordinal - 1, kMaximumShift);
    const int64_t base = (int64_t{1} << shift) * kNanosecondsPerSecond;
    const uint32_t jitter = std::min(jitter_basis_points, kMaximumJitterBasisPoints);
    return base + base * jitter / 10'000;
}

std::optional<int64_t> ai_retry_eligible_at_ns(int64_t monotonic_now_ns, int64_t wall_now_seconds,
                                               int64_t monotonic_deadline_ns, size_t retry_ordinal,
                                               uint32_t jitter_basis_points,
                                               std::optional<std::string_view> retry_after) {
    const int64_t local_backoff_ns = ai_retry_backoff_ns(retry_ordinal, jitter_basis_points);
    if (monotonic_now_ns > std::numeric_limits<int64_t>::max() - local_backoff_ns) return std::nullopt;
    const int64_t local_eligible_at_ns = monotonic_now_ns + local_backoff_ns;
    if (local_eligible_at_ns >= monotonic_deadline_ns) return std::nullopt;

    const RetryAfterParseResult parsed_retry_after = parse_retry_after(retry_after, wall_now_seconds);
    if (parsed_retry_after.kind != RetryAfterParseKind::DELAY_SECONDS) return local_eligible_at_ns;

    if (parsed_retry_after.delay_seconds > std::numeric_limits<int64_t>::max() / kNanosecondsPerSecond) {
        return local_eligible_at_ns;
    }
    const int64_t retry_after_ns = parsed_retry_after.delay_seconds * kNanosecondsPerSecond;
    if (monotonic_now_ns > std::numeric_limits<int64_t>::max() - retry_after_ns) return local_eligible_at_ns;
    const int64_t retry_after_eligible_at_ns = monotonic_now_ns + retry_after_ns;
    if (retry_after_eligible_at_ns >= monotonic_deadline_ns) return local_eligible_at_ns;
    return std::max(local_eligible_at_ns, retry_after_eligible_at_ns);
}

class AITaskDispatcherCore {
public:
    AITaskDispatcherCore(AIAdmissionController* admission, AIHttpClient* http, const AIProvider* provider,
                         AICompletionExecutor* completion, const AIClock* clock, AIRandom* random, AIMetrics* metrics,
                         AITaskDispatcherOptions options)
            : admission(admission),
              http(http),
              provider(provider),
              completion(completion),
              clock(clock),
              random(random),
              metrics(metrics),
              options(options) {}

    AIAdmissionController* admission;
    AIHttpClient* http;
    const AIProvider* provider;
    AICompletionExecutor* completion;
    const AIClock* clock;
    AIRandom* random;
    AIMetrics* metrics;
    AITaskDispatcherOptions options;
};

namespace {

enum class AITaskPhase : uint8_t {
    INITIAL,
    WAITING_ADMISSION,
    FIRING,
    COMPLETED_PENDING_SUBMIT_RESULT,
    IN_FLIGHT,
    AWAITING_CLASSIFICATION,
    BACKOFF,
    TERMINAL,
};

class AIProviderRequestTemplate {
public:
    ~AIProviderRequestTemplate() noexcept { release(); }

    AIProviderRequestTemplate(const AIProviderRequestTemplate&) = delete;
    AIProviderRequestTemplate& operator=(const AIProviderRequestTemplate&) = delete;

    AIProviderRequestTemplate(AIProviderRequestTemplate&& other) noexcept
            : _request(std::move(other._request)),
              _memory(std::move(other._memory)),
              _reserved_bytes(std::exchange(other._reserved_bytes, 0)) {}

    AIProviderRequestTemplate& operator=(AIProviderRequestTemplate&& other) noexcept {
        if (this != &other) {
            release();
            _request = std::move(other._request);
            _memory = std::move(other._memory);
            _reserved_bytes = std::exchange(other._reserved_bytes, 0);
        }
        return *this;
    }

    static StatusOr<AIProviderRequestTemplate> create(AIProviderHttpRequest request, AIMemoryContext memory) {
        size_t bytes = 0;
        auto add_bytes = [&bytes](size_t value) {
            if (value > std::numeric_limits<size_t>::max() - bytes) return false;
            bytes += value;
            return true;
        };
        if (!add_bytes(request.url.size()) || !add_bytes(request.body.size())) {
            return Status::MemoryLimitExceeded("AI provider request template memory limit exceeded");
        }
        // Match AIHttpClient request accounting: each header contributes its stored strings plus ": " and CRLF.
        for (const AIHttpHeader& header : request.headers) {
            if (!add_bytes(header.name.size()) || !add_bytes(header.value.size()) || !add_bytes(4)) {
                return Status::MemoryLimitExceeded("AI provider request template memory limit exceeded");
            }
        }

        const size_t tracked_bytes = memory ? bytes : 0;
        if (tracked_bytes > 0 && !memory.reserve(tracked_bytes)) {
            return Status::MemoryLimitExceeded("AI provider request template memory limit exceeded");
        }
        return AIProviderRequestTemplate(std::move(request), std::move(memory), tracked_bytes);
    }

    const AIProviderHttpRequest& request() const noexcept { return _request; }

    void release() noexcept {
        const size_t bytes = std::exchange(_reserved_bytes, 0);
        AIMemoryContext memory = _memory;
        try {
            run_in_physical_scope(memory, [&] {
                std::string().swap(_request.url);
                std::vector<AIHttpHeader>().swap(_request.headers);
                std::string().swap(_request.body);
                _memory = {};
            });
        } catch (...) {
            return;
        }
        memory.release(bytes);
    }

private:
    AIProviderRequestTemplate(AIProviderHttpRequest request, AIMemoryContext memory, size_t reserved_bytes) noexcept
            : _request(std::move(request)), _memory(std::move(memory)), _reserved_bytes(reserved_bytes) {}

    AIProviderHttpRequest _request;
    AIMemoryContext _memory;
    size_t _reserved_bytes = 0;
};

AITaskResult admission_failure_result(AIAdmissionFailureReason reason) {
    switch (reason) {
    case AIAdmissionFailureReason::CANCELLED:
        return AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED};
    case AIAdmissionFailureReason::DEADLINE_EXCEEDED:
        return AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE};
    case AIAdmissionFailureReason::SHUTDOWN:
        return AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN};
    case AIAdmissionFailureReason::LOCAL_RESOURCE:
        return AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE};
    }
    return AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE};
}

} // namespace

class AITaskState final : public std::enable_shared_from_this<AITaskState> {
public:
    AITaskState(std::shared_ptr<AITaskDispatcherCore> core, AIDispatchRequest& request,
                AIProviderRequestTemplate request_template, AIRateLimitKey rate_limit_key, AITaskCallback&& callback)
            : _core(std::move(core)),
              _workgroup_key(request.workgroup_key),
              _query_id(request.query_id),
              _task_id(request.task_id),
              _request_template(std::move(request_template)),
              _rate_limit_key(std::move(rate_limit_key)),
              _request_deadline_ns(request.request_deadline_ns),
              _connect_timeout_ms(request.connect_timeout_ms),
              _max_response_bytes(request.max_response_bytes),
              _resolved_endpoint(std::move(request.resolved_endpoint)),
              _lifecycle(std::move(request.lifecycle)),
              _memory(request.memory),
              _callback(std::move(callback)) {}

    void start() {
        PendingAdmission registration =
                _prepare_admission(_core->clock->monotonic_now_ns(), AITaskPhase::WAITING_ADMISSION);
        if (registration.failure.has_value()) {
            _publish(std::move(*registration.failure));
        } else {
            _finish_admission(std::move(registration));
        }
    }

    void cancel() {
        _cancel_requested.store(true, std::memory_order_release);

        std::optional<AIAdmissionTicket> ticket;
        bool publish_immediately = false;
        {
            std::lock_guard lock(_mutex);
            if (_phase == AITaskPhase::TERMINAL) return;
            if (_phase == AITaskPhase::WAITING_ADMISSION || _phase == AITaskPhase::BACKOFF) {
                if (_ticket.has_value()) {
                    ticket.emplace(std::move(*_ticket));
                    _ticket.reset();
                } else {
                    publish_immediately = true;
                }
            }
        }
        if (ticket.has_value()) ticket->cancel();
        if (publish_immediately) _publish(AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
    }

private:
    struct CompletionEnvelope;

    struct PendingAdmission {
        AITaskPhase phase = AITaskPhase::INITIAL;
        uint64_t generation = 0;
        std::optional<AIAdmissionTicket> ticket;
        std::optional<AITaskResult> failure;
    };

    AILifecycleObservation _observe_lifecycle() const noexcept {
        if (_cancel_requested.load(std::memory_order_acquire)) {
            return {.state = AILifecycleState::CANCELLED};
        }
        return observe_ai_lifecycle(_lifecycle, _request_deadline_ns, _core->clock->monotonic_now_ns());
    }

    static std::optional<AITaskResult> _lifecycle_failure(const AILifecycleObservation& lifecycle) {
        switch (lifecycle.state) {
        case AILifecycleState::ACTIVE:
            return std::nullopt;
        case AILifecycleState::CANCELLED:
            return AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED};
        case AILifecycleState::DEADLINE_EXCEEDED:
            return AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE};
        }
        return AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED};
    }

    AIQueryLifecycleProbe _make_lifecycle_probe() {
        std::weak_ptr<AITaskState> weak_state = shared_from_this();
        return [weak_state]() -> AIQueryLifecycleSnapshot {
            std::shared_ptr<AITaskState> state = weak_state.lock();
            if (state == nullptr || state->_cancel_requested.load(std::memory_order_acquire)) {
                return {.cancelled = true, .monotonic_deadline_ns = 1};
            }
            return state->_lifecycle();
        };
    }

    PendingAdmission _prepare_admission(int64_t eligible_at_ns, AITaskPhase phase) {
        PendingAdmission registration;
        registration.phase = phase;
        if (std::optional<AITaskResult> failure = _lifecycle_failure(_observe_lifecycle()); failure.has_value()) {
            registration.failure.emplace(std::move(*failure));
            return registration;
        }

        {
            std::lock_guard lock(_mutex);
            if (_phase == AITaskPhase::TERMINAL) return registration;
            registration.generation = ++_admission_generation;
            _phase = phase;
            _ticket.reset();
        }

        std::optional<StatusOr<AIAdmissionTicket>> admitted;
        std::optional<AIAdmissionRequest> admission_request;
        AIAdmissionCallback admission_callback;
        try {
            TEST_SYNC_POINT("AITaskState::_prepare_admission:before_admission_materialization");
            run_in_physical_scope(_memory, [&] {
                admission_request.emplace(AIAdmissionRequest{
                        .workgroup_key = _workgroup_key,
                        .query_id = _query_id,
                        .attempt_id = _task_id,
                        .rate_limit_key = _rate_limit_key,
                        .eligible_at_ns = eligible_at_ns,
                        .request_deadline_ns = _request_deadline_ns,
                        .lifecycle = _make_lifecycle_probe(),
                        .memory = _memory,
                });
                auto state = shared_from_this();
                const uint64_t generation = registration.generation;
                admission_callback = [state, generation](AIAdmissionResult result) mutable {
                    state->_on_admission(generation, std::move(result));
                };
            });
            admitted.emplace(_core->admission->enqueue(*admission_request, std::move(admission_callback)));
        } catch (...) {
            // The caller publishes outside this boundary, so a user callback is never mistaken for enqueue failure.
        }
        try {
            run_in_physical_scope(_memory, [&] {
                admission_request.reset();
                AIAdmissionCallback().swap(admission_callback);
            });
        } catch (...) {
        }
        if (!admitted.has_value()) {
            registration.failure.emplace(
                    AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            return registration;
        }
        if (!admitted->ok()) {
            if (admitted->status().is_shutdown() || admitted->status().is_service_unavailable()) {
                registration.failure.emplace(AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN});
            } else if (admitted->status().is_cancelled()) {
                registration.failure.emplace(AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
            } else if (admitted->status().is_time_out()) {
                registration.failure.emplace(AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
            } else {
                registration.failure.emplace(
                        AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            }
            return registration;
        }
        registration.ticket.emplace(std::move(*admitted).value());
        return registration;
    }

    void _finish_admission(PendingAdmission registration) {
        if (!registration.ticket.has_value()) return;
        TEST_SYNC_POINT("AITaskState::_finish_admission:before_ticket_store");
        std::optional<AIAdmissionTicket> ticket_to_cancel;
        const AILifecycleObservation lifecycle = _observe_lifecycle();
        const bool active = lifecycle.state == AILifecycleState::ACTIVE;
        bool current_registration = false;
        {
            std::lock_guard lock(_mutex);
            current_registration = _admission_generation == registration.generation && _phase == registration.phase;
            if (current_registration && active && !_cancel_requested.load(std::memory_order_acquire)) {
                _ticket.emplace(std::move(*registration.ticket));
            } else {
                ticket_to_cancel.emplace(std::move(*registration.ticket));
            }
        }
        if (ticket_to_cancel.has_value()) ticket_to_cancel->cancel();
        if (current_registration && !active) {
            if (std::optional<AITaskResult> failure = _lifecycle_failure(lifecycle); failure.has_value()) {
                _publish(std::move(*failure));
            }
        }
    }

    void _on_admission(uint64_t generation, AIAdmissionResult result) {
        if (auto* failure = std::get_if<AIAdmissionFailure>(&result)) {
            {
                std::lock_guard lock(_mutex);
                if (_admission_generation != generation || _phase == AITaskPhase::TERMINAL) return;
                _ticket.reset();
            }
            _publish(admission_failure_result(failure->reason));
            return;
        }

        AIAdmissionGrant grant = std::move(std::get<AIAdmissionGrant>(result));
        const AILifecycleObservation lifecycle = _observe_lifecycle();
        const bool active = lifecycle.state == AILifecycleState::ACTIVE;
        bool admitted = false;
        {
            std::lock_guard lock(_mutex);
            if (_admission_generation != generation || _phase == AITaskPhase::TERMINAL) {
                return;
            }
            _ticket.reset();
            if (active && !_cancel_requested.load(std::memory_order_acquire)) {
                _grant.emplace(std::move(grant));
                _phase = AITaskPhase::FIRING;
                admitted = true;
            }
        }
        if (!admitted) {
            // Release the uncommitted grant before making the terminal callback visible.
            grant = AIAdmissionGrant{};
            if (std::optional<AITaskResult> failure = _lifecycle_failure(lifecycle); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }
        if (const AILifecycleObservation before_http = _observe_lifecycle();
            before_http.state != AILifecycleState::ACTIVE) {
            std::optional<AIAdmissionGrant> rolled_back_grant;
            {
                std::lock_guard lock(_mutex);
                if (_phase == AITaskPhase::FIRING && _grant.has_value()) {
                    rolled_back_grant.emplace(std::move(*_grant));
                    _grant.reset();
                }
            }
            rolled_back_grant.reset();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(before_http); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }
        _fire_http();
    }

    AIHttpRequest _make_http_request() {
        AIHttpRequest request;
        run_in_physical_scope(_memory, [&] {
            const AIProviderHttpRequest& request_template = _request_template.request();
            request.url = request_template.url;
            request.headers = request_template.headers;
            request.body = request_template.body;
            request.request_deadline_ns = _request_deadline_ns;
            request.connect_timeout_ms = effective_connect_timeout_ms(
                    _connect_timeout_ms, _core->clock->monotonic_now_ns(), _request_deadline_ns);
            request.max_response_bytes = _max_response_bytes;
            request.resolved_endpoint = _resolved_endpoint;
            request.lifecycle = _make_lifecycle_probe();
            request.memory = _memory;
        });
        return request;
    }

    void _fire_http() {
        const AILifecycleObservation lifecycle = _observe_lifecycle();
        if (lifecycle.state != AILifecycleState::ACTIVE) {
            std::optional<AIAdmissionGrant> rolled_back_grant;
            {
                std::lock_guard lock(_mutex);
                if (_phase == AITaskPhase::FIRING && _grant.has_value()) {
                    rolled_back_grant.emplace(std::move(*_grant));
                    _grant.reset();
                }
            }
            rolled_back_grant.reset();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(lifecycle); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }

        bool retry_attempt = false;
        {
            std::lock_guard lock(_mutex);
            retry_attempt = _retry_ordinal > 0;
        }
        enum class HttpSubmitResult : uint8_t { ACCEPTED, SHUTDOWN, LOCAL_RESOURCE, LOCAL_REQUEST };
        HttpSubmitResult submit_result = HttpSubmitResult::LOCAL_RESOURCE;
        std::optional<AIHttpRequest> request;
        AIHttpCallback callback;
        try {
            run_in_physical_scope(_memory, [&] {
                request.emplace(_make_http_request());
                auto state = shared_from_this();
                callback = [state](AIHttpResult result) mutable { state->_on_transport(std::move(result)); };
            });
            Status status = _core->http->submit(std::move(*request), std::move(callback));
            if (status.ok()) {
                submit_result = HttpSubmitResult::ACCEPTED;
            } else if (status.is_shutdown() || status.is_service_unavailable()) {
                submit_result = HttpSubmitResult::SHUTDOWN;
            } else if (status.is_mem_limit_exceeded()) {
                submit_result = HttpSubmitResult::LOCAL_RESOURCE;
            } else {
                submit_result = HttpSubmitResult::LOCAL_REQUEST;
            }
        } catch (...) {
            submit_result = HttpSubmitResult::LOCAL_RESOURCE;
        }
        try {
            run_in_physical_scope(_memory, [&] {
                request.reset();
                AIHttpCallback().swap(callback);
            });
        } catch (...) {
        }

        std::optional<AIAdmissionGrant> rolled_back_grant;
        std::optional<AIHttpResult> inline_result;
        std::optional<AIBucketResolutionGuard> guard;
        if (submit_result == HttpSubmitResult::ACCEPTED) {
            _core->metrics->record_accepted_attempt(retry_attempt);
            std::optional<AIAdmissionGrant> committed_grant;
            {
                std::lock_guard lock(_mutex);
                if (!_grant.has_value()) return;
                committed_grant.emplace(std::move(*_grant));
                _grant.reset();
            }
            committed_grant->commit_network_attempt();
            bool completed_inline = false;
            {
                std::lock_guard lock(_mutex);
                if (_phase == AITaskPhase::FIRING) {
                    _grant.emplace(std::move(*committed_grant));
                    _phase = AITaskPhase::IN_FLIGHT;
                } else if (_phase == AITaskPhase::COMPLETED_PENDING_SUBMIT_RESULT) {
                    inline_result.emplace(std::move(*_inline_result));
                    _inline_result.reset();
                    _phase = AITaskPhase::AWAITING_CLASSIFICATION;
                    completed_inline = true;
                }
            }
            if (completed_inline) {
                guard.emplace(committed_grant->complete_transport());
                committed_grant.reset();
                _handoff(std::move(*inline_result), std::move(*guard));
            }
            return;
        }

        std::optional<AIHttpResult> abandoned_inline_result;
        {
            std::lock_guard lock(_mutex);
            if (_grant.has_value()) {
                rolled_back_grant.emplace(std::move(*_grant));
                _grant.reset();
            }
            if (_inline_result.has_value()) {
                abandoned_inline_result.emplace(std::move(*_inline_result));
                _inline_result.reset();
            }
        }
        rolled_back_grant.reset();
        abandoned_inline_result.reset();
        if (std::optional<AITaskResult> failure = _lifecycle_failure(_observe_lifecycle()); failure.has_value()) {
            _publish(std::move(*failure));
        } else if (submit_result == HttpSubmitResult::SHUTDOWN) {
            _publish(AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN});
        } else if (submit_result == HttpSubmitResult::LOCAL_RESOURCE) {
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
        } else {
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_REQUEST});
        }
    }

    void _on_transport(AIHttpResult result) {
        std::optional<AIAdmissionGrant> completed_grant;
        {
            std::lock_guard lock(_mutex);
            if (_phase == AITaskPhase::FIRING) {
                _inline_result.emplace(std::move(result));
                _phase = AITaskPhase::COMPLETED_PENDING_SUBMIT_RESULT;
                return;
            }
            if (_phase != AITaskPhase::IN_FLIGHT || !_grant.has_value()) return;
            completed_grant.emplace(std::move(*_grant));
            _grant.reset();
            _phase = AITaskPhase::AWAITING_CLASSIFICATION;
        }
        std::optional<AIBucketResolutionGuard> guard;
        guard.emplace(completed_grant->complete_transport());
        completed_grant.reset();
        _handoff(std::move(result), std::move(*guard));
    }

    void _handoff(AIHttpResult result, AIBucketResolutionGuard guard);
    void _classify(AIHttpResult result, AIBucketResolutionGuard guard);

    bool _try_compute_retry_eligible_at(int64_t effective_deadline_ns, size_t retry_ordinal,
                                        std::optional<std::string_view> retry_after,
                                        std::optional<int64_t>* eligible_at_ns) noexcept {
        try {
            const uint32_t jitter_basis_points = _core->random->uniform(kMaximumJitterBasisPoints + 1);
            *eligible_at_ns =
                    ai_retry_eligible_at_ns(_core->clock->monotonic_now_ns(), _core->clock->unix_now_seconds(),
                                            effective_deadline_ns, retry_ordinal, jitter_basis_points, retry_after);
            return true;
        } catch (...) {
            eligible_at_ns->reset();
            return false;
        }
    }

    void _retry_or_finish(AIAttemptAction action, std::optional<std::string_view> retry_after,
                          AIBucketResolutionGuard guard, AISanitizedFailureClass failure_class) {
        const AILifecycleObservation lifecycle = _observe_lifecycle();
        if (lifecycle.state != AILifecycleState::ACTIVE) {
            guard.resolve_without_cooldown();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(lifecycle); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }

        size_t retry_ordinal = 0;
        bool retry_exhausted = false;
        {
            std::lock_guard lock(_mutex);
            if (_phase == AITaskPhase::TERMINAL) return;
            if (!ai_should_retry(_retry_ordinal, action, _core->options.max_retries,
                                 _core->options.max_throttle_retries)) {
                _phase = AITaskPhase::AWAITING_CLASSIFICATION;
                retry_exhausted = true;
            } else {
                retry_ordinal = ++_retry_ordinal;
                _phase = AITaskPhase::BACKOFF;
            }
        }
        if (retry_exhausted) {
            if (action == AIAttemptAction::THROTTLE) {
                std::optional<int64_t> cooldown_at_ns;
                if (!_try_compute_retry_eligible_at(lifecycle.effective_deadline_ns, _retry_ordinal + 1, retry_after,
                                                    &cooldown_at_ns)) {
                    guard.resolve_without_cooldown();
                    _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
                    return;
                }
                const AILifecycleObservation before_cooldown = _observe_lifecycle();
                const int64_t before_cooldown_now_ns = _core->clock->monotonic_now_ns();
                if (before_cooldown.state != AILifecycleState::ACTIVE ||
                    before_cooldown_now_ns >= before_cooldown.effective_deadline_ns) {
                    guard.resolve_without_cooldown();
                    if (std::optional<AITaskResult> failure = _lifecycle_failure(before_cooldown);
                        failure.has_value()) {
                        _publish(std::move(*failure));
                    } else {
                        _publish(AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
                    }
                    return;
                }
                if (cooldown_at_ns.has_value() && *cooldown_at_ns != std::numeric_limits<int64_t>::max() &&
                    *cooldown_at_ns > before_cooldown_now_ns &&
                    *cooldown_at_ns < before_cooldown.effective_deadline_ns) {
                    guard.resolve_with_cooldown(*cooldown_at_ns);
                } else {
                    guard.resolve_without_cooldown();
                }
            } else {
                guard.resolve_without_cooldown();
            }
            _publish(AISanitizedRowFailure{.failure_class = failure_class});
            return;
        }

        std::optional<int64_t> eligible_at_ns;
        if (!_try_compute_retry_eligible_at(lifecycle.effective_deadline_ns, retry_ordinal, retry_after,
                                            &eligible_at_ns)) {
            guard.resolve_without_cooldown();
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            return;
        }
        const AILifecycleObservation before_retry = _observe_lifecycle();
        if (before_retry.state != AILifecycleState::ACTIVE || !eligible_at_ns.has_value() ||
            *eligible_at_ns >= before_retry.effective_deadline_ns) {
            guard.resolve_without_cooldown();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(before_retry); failure.has_value()) {
                _publish(std::move(*failure));
            } else {
                _publish(AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
            }
            return;
        }

        PendingAdmission registration = _prepare_admission(*eligible_at_ns, AITaskPhase::BACKOFF);
        if (registration.failure.has_value()) {
            guard.resolve_without_cooldown();
            _publish(std::move(*registration.failure));
            return;
        }
        if (!registration.ticket.has_value()) {
            guard.resolve_without_cooldown();
            return;
        }

        const AILifecycleObservation before_cooldown = _observe_lifecycle();
        const int64_t before_cooldown_now_ns = _core->clock->monotonic_now_ns();
        if (before_cooldown.state != AILifecycleState::ACTIVE ||
            before_cooldown_now_ns >= before_cooldown.effective_deadline_ns ||
            *eligible_at_ns >= before_cooldown.effective_deadline_ns) {
            registration.ticket->cancel();
            guard.resolve_without_cooldown();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(before_cooldown); failure.has_value()) {
                _publish(std::move(*failure));
            } else {
                _publish(AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
            }
            return;
        }
        if (action == AIAttemptAction::THROTTLE && *eligible_at_ns != std::numeric_limits<int64_t>::max() &&
            *eligible_at_ns > before_cooldown_now_ns) {
            guard.resolve_with_cooldown(*eligible_at_ns);
        } else {
            guard.resolve_without_cooldown();
        }
        _finish_admission(std::move(registration));
    }

    void _publish(AITaskResult result) noexcept {
        const bool cancelled = _observe_lifecycle().state == AILifecycleState::CANCELLED;
        AIMemoryContext memory = _memory;
        AITaskCallback callback;
        bool replace_with_cancellation = false;
        bool should_publish = false;
        auto transition = [&] {
            std::lock_guard lock(_mutex);
            if (_phase == AITaskPhase::TERMINAL) return;
            _phase = AITaskPhase::TERMINAL;
            _ticket.reset();
            replace_with_cancellation = cancelled;
            callback = std::move(_callback);
            AITaskCallback().swap(_callback);
            should_publish = true;
            TEST_SYNC_POINT_CALLBACK("AITaskState::_publish:callback_moved:in_physical_scope", &memory);
        };
        try {
            run_in_physical_scope(memory, transition);
        } catch (...) {
            try {
                if (!should_publish) transition();
            } catch (...) {
                LOG(WARNING) << "AI task terminal state publication failed";
                return;
            }
        }
        if (!should_publish) return;
        if (replace_with_cancellation) {
            result = AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED};
        }
        _request_template.release();
        invoke_task_callback_noexcept(memory, &callback, std::move(result));
    }

    struct CompletionEnvelope {
        CompletionEnvelope(AIMemoryContext memory, std::shared_ptr<AITaskState> state, AIHttpResult result,
                           AIBucketResolutionGuard guard)
                : memory(std::move(memory)),
                  state(std::move(state)),
                  result(std::move(result)),
                  guard(std::move(guard)) {}

        void run() noexcept {
            std::optional<AIHttpResult> local_result;
            std::optional<AIBucketResolutionGuard> local_guard;
            bool should_run = false;
            try {
                run_in_physical_scope(memory, [&] {
                    std::lock_guard lock(mutex);
                    if (consumed) return;
                    consumed = true;
                    local_result.emplace(std::move(*result));
                    result.reset();
                    local_guard.emplace(std::move(*guard));
                    guard.reset();
                    should_run = true;
                });
            } catch (...) {
                reject(false);
                return;
            }
            if (!should_run) return;
            try {
                TEST_SYNC_POINT("AITaskState::CompletionEnvelope::run:before_classify");
                state->_classify(std::move(*local_result), std::move(*local_guard));
            } catch (...) {
                local_guard.reset();
                try {
                    run_in_physical_scope(memory, [&] { local_result.reset(); });
                } catch (...) {
                    local_result.reset();
                }
                LOG(WARNING) << "AI task completion classification threw an exception";
                state->_publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            }
        }

        void reject(bool shutting_down) noexcept {
            std::optional<AIHttpResult> local_result;
            std::optional<AIBucketResolutionGuard> local_guard;
            bool should_reject = false;
            try {
                run_in_physical_scope(memory, [&] {
                    std::lock_guard lock(mutex);
                    if (consumed) return;
                    consumed = true;
                    local_result.emplace(std::move(*result));
                    result.reset();
                    local_guard.emplace(std::move(*guard));
                    guard.reset();
                    should_reject = true;
                });
            } catch (...) {
                LOG(WARNING) << "AI task completion rejection extraction failed";
                return;
            }
            if (!should_reject) return;
            try {
                run_in_physical_scope(memory, [&] { local_result.reset(); });
            } catch (...) {
                local_result.reset();
            }
            local_guard->resolve_without_cooldown();
            if (shutting_down) {
                state->_publish(AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN});
            } else {
                state->_publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            }
        }

        AIMemoryContext memory;
        std::mutex mutex;
        bool consumed = false;
        std::shared_ptr<AITaskState> state;
        std::optional<AIHttpResult> result;
        std::optional<AIBucketResolutionGuard> guard;
    };

    std::shared_ptr<AITaskDispatcherCore> _core;
    AIWorkGroupKey _workgroup_key;
    UniqueId _query_id;
    uint64_t _task_id;
    AIProviderRequestTemplate _request_template;
    AIRateLimitKey _rate_limit_key;
    int64_t _request_deadline_ns;
    int64_t _connect_timeout_ms;
    size_t _max_response_bytes;
    std::shared_ptr<const ResolvedHttpEndpoint> _resolved_endpoint;
    AIQueryLifecycleProbe _lifecycle;
    AIMemoryContext _memory;

    mutable std::mutex _mutex;
    std::atomic<bool> _cancel_requested = false;
    AITaskPhase _phase = AITaskPhase::INITIAL;
    uint64_t _admission_generation = 0;
    size_t _retry_ordinal = 0;
    std::optional<AIAdmissionTicket> _ticket;
    std::optional<AIAdmissionGrant> _grant;
    std::optional<AIHttpResult> _inline_result;
    AITaskCallback _callback;
};

void AITaskState::_handoff(AIHttpResult result, AIBucketResolutionGuard guard) {
    std::optional<AIHttpResult> pending_result(std::move(result));
    std::optional<AIBucketResolutionGuard> pending_guard(std::move(guard));
    std::shared_ptr<CompletionEnvelope> envelope;
    std::optional<AICompletionWork> work;
    Status status;
    try {
        run_in_physical_scope(_memory, [&] {
            envelope = ai_allocate_shared<CompletionEnvelope>(_memory, _memory, shared_from_this(),
                                                              std::move(*pending_result), std::move(*pending_guard));
            pending_result.reset();
            pending_guard.reset();
            TEST_SYNC_POINT("AITaskState::_handoff:completion_envelope_constructed:in_physical_scope");
            work.emplace(
                    _memory, [envelope] { envelope->run(); }, [envelope] { envelope->reject(true); });
        });
        status = _core->completion->try_submit(std::move(*work));
    } catch (...) {
        try {
            run_in_physical_scope(_memory, [&] {
                pending_result.reset();
                work.reset();
            });
        } catch (...) {
        }
        if (pending_guard.has_value()) {
            pending_guard->resolve_without_cooldown();
            pending_guard.reset();
        }
        if (envelope != nullptr) {
            envelope->reject(false);
        } else {
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
        }
        return;
    }
    if (!status.ok()) envelope->reject(status.is_shutdown() || status.is_service_unavailable());
}

void AITaskState::_classify(AIHttpResult result, AIBucketResolutionGuard guard) {
    const auto* no_response = std::get_if<AIHttpNoResponse>(&result);
    if (no_response != nullptr && no_response->code == AIHttpNoResponseCode::CANCELLATION) {
        guard.resolve_without_cooldown();
        _publish(AILifecycleCancelled{.reason = AILifecycleReason::CANCELLED});
        return;
    }
    if (no_response != nullptr && no_response->code == AIHttpNoResponseCode::SHUTDOWN) {
        guard.resolve_without_cooldown();
        _publish(AILifecycleCancelled{.reason = AILifecycleReason::SHUTDOWN});
        return;
    }

    const AILifecycleObservation lifecycle = _observe_lifecycle();
    if (lifecycle.state != AILifecycleState::ACTIVE) {
        if (lifecycle.state == AILifecycleState::DEADLINE_EXCEEDED) {
            _core->metrics->record_timeout();
        }
        guard.resolve_without_cooldown();
        if (std::optional<AITaskResult> failure = _lifecycle_failure(lifecycle); failure.has_value()) {
            _publish(std::move(*failure));
        }
        return;
    }

    if (no_response != nullptr) {
        switch (no_response->code) {
        case AIHttpNoResponseCode::CANCELLATION:
            DCHECK(false) << "transport cancellation must be classified before the task deadline";
            return;
        case AIHttpNoResponseCode::DEADLINE:
            _core->metrics->record_timeout();
            guard.resolve_without_cooldown();
            _publish(AILifecycleCancelled{.reason = AILifecycleReason::DEADLINE});
            return;
        case AIHttpNoResponseCode::SHUTDOWN:
            DCHECK(false) << "transport shutdown must be classified before the task deadline";
            return;
        case AIHttpNoResponseCode::TIMEOUT:
            _core->metrics->record_timeout();
            _retry_or_finish(classify_ai_no_response(no_response->code), std::nullopt, std::move(guard),
                             AISanitizedFailureClass::TRANSPORT);
            return;
        case AIHttpNoResponseCode::DNS:
        case AIHttpNoResponseCode::CONNECT:
        case AIHttpNoResponseCode::SEND:
        case AIHttpNoResponseCode::RECEIVE:
        case AIHttpNoResponseCode::EMPTY_REPLY:
        case AIHttpNoResponseCode::PARTIAL_TRANSFER:
        case AIHttpNoResponseCode::HTTP2_STREAM_RESET:
        case AIHttpNoResponseCode::TLS_HANDSHAKE:
        case AIHttpNoResponseCode::TLS_VERIFICATION:
        case AIHttpNoResponseCode::UNKNOWN:
            _retry_or_finish(classify_ai_no_response(no_response->code), std::nullopt, std::move(guard),
                             AISanitizedFailureClass::TRANSPORT);
            return;
        case AIHttpNoResponseCode::RESPONSE_CAP:
        case AIHttpNoResponseCode::MEMORY_LIMIT:
            _retry_or_finish(AIAttemptAction::TERMINAL, std::nullopt, std::move(guard),
                             AISanitizedFailureClass::LOCAL_RESOURCE);
            return;
        }
        guard.resolve_without_cooldown();
        _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::TRANSPORT});
        return;
    }

    AIHttpResponse response = std::move(std::get<AIHttpResponse>(result));
    AIProviderParseResult provider_result = AIProviderMalformed{};
    auto clear_provider_result = [&]() noexcept {
        try {
            run_in_physical_scope(_memory, [&] { provider_result = AIProviderMalformed{}; });
        } catch (...) {
        }
    };
    if ((response.status_code >= 200 && response.status_code < 300) || response.status_code == 400) {
        try {
            run_in_physical_scope(_memory,
                                  [&] { provider_result = _core->provider->parse_response(response.body.data()); });
        } catch (...) {
            clear_provider_result();
            const AILifecycleObservation after_parse = _observe_lifecycle();
            if (after_parse.state != AILifecycleState::ACTIVE) {
                if (after_parse.state == AILifecycleState::DEADLINE_EXCEEDED) {
                    _core->metrics->record_timeout();
                }
                guard.resolve_without_cooldown();
                if (std::optional<AITaskResult> failure = _lifecycle_failure(after_parse); failure.has_value()) {
                    _publish(std::move(*failure));
                }
                return;
            }
            guard.resolve_without_cooldown();
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            return;
        }
        const AILifecycleObservation after_parse = _observe_lifecycle();
        if (after_parse.state != AILifecycleState::ACTIVE) {
            clear_provider_result();
            if (after_parse.state == AILifecycleState::DEADLINE_EXCEEDED) {
                _core->metrics->record_timeout();
            }
            guard.resolve_without_cooldown();
            if (std::optional<AITaskResult> failure = _lifecycle_failure(after_parse); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }
    }

    const AIAttemptAction action = classify_ai_http_response(response.status_code, provider_result);
    if (action == AIAttemptAction::SUCCEEDED) {
        auto* success = std::get_if<AIProviderSuccess>(&provider_result);
        std::optional<AITaskSuccess> task_success;
        bool local_resource_failure = false;
        try {
            run_in_physical_scope(_memory, [&] {
                StatusOr<AITaskSuccess> created = AITaskSuccess::create(std::move(success->content), _memory);
                if (created.ok()) {
                    task_success.emplace(std::move(created).value());
                } else {
                    local_resource_failure = true;
                }
                provider_result = AIProviderMalformed{};
            });
        } catch (...) {
            local_resource_failure = true;
            clear_provider_result();
        }
        guard.resolve_without_cooldown();
        if (local_resource_failure) {
            _publish(AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_RESOURCE});
            return;
        }
        const AILifecycleObservation before_success = _observe_lifecycle();
        if (before_success.state != AILifecycleState::ACTIVE) {
            if (before_success.state == AILifecycleState::DEADLINE_EXCEEDED) {
                _core->metrics->record_timeout();
            }
            if (std::optional<AITaskResult> failure = _lifecycle_failure(before_success); failure.has_value()) {
                _publish(std::move(*failure));
            }
            return;
        }
        _publish(AITaskResult{std::move(*task_success)});
        return;
    }

    std::optional<std::string_view> retry_after;
    if (response.retry_after.has_value()) retry_after = *response.retry_after;
    clear_provider_result();
    _retry_or_finish(action, retry_after, std::move(guard), AISanitizedFailureClass::PROVIDER_RESPONSE);
}

void AITaskHandle::cancel() {
    if (std::shared_ptr<AITaskState> state = _state.lock()) state->cancel();
}

AITaskDispatcher::AITaskDispatcher(AIAdmissionController* admission, AIHttpClient* http, const AIProvider* provider,
                                   AICompletionExecutor* completion, const AIClock* clock, AIRandom* random,
                                   AIMetrics* metrics, AITaskDispatcherOptions options)
        : _core(std::make_shared<AITaskDispatcherCore>(admission, http, provider, completion, clock, random, metrics,
                                                       options)) {}

StatusOr<AITaskHandle> AITaskDispatcher::submit(AIDispatchRequest&& request, AITaskCallback&& callback) {
    const AIMemoryContext memory = request.memory;
    SCOPED_CLEANUP({
        clear_task_callback(memory, &callback);
        clear_lifecycle_probe(memory, &request.lifecycle);
    });
    if (!callback) return Status::InvalidArgument("AI task callback is required");
    if (_core->admission == nullptr || _core->http == nullptr || _core->provider == nullptr ||
        _core->completion == nullptr || _core->clock == nullptr || _core->random == nullptr ||
        _core->metrics == nullptr) {
        return Status::InvalidArgument("AI task dispatcher dependencies are required");
    }
    if (!request.memory) {
        invoke_task_callback_noexcept(memory, &callback,
                                      AISanitizedRowFailure{.failure_class = AISanitizedFailureClass::LOCAL_REQUEST});
        return AITaskHandle{};
    }

    enum class SubmitResult : uint8_t { CREATED, LOCAL_REQUEST, LOCAL_RESOURCE };
    SubmitResult submit_result = SubmitResult::LOCAL_RESOURCE;
    std::shared_ptr<AITaskState> state;
    try {
        run_in_physical_scope(request.memory, [&] {
            StatusOr<AIProviderHttpRequest> provider_request = _core->provider->build_request(request.chat_request);
            if (!provider_request.ok()) {
                submit_result = SubmitResult::LOCAL_REQUEST;
                return;
            }

            StatusOr<AIProviderRequestTemplate> request_template =
                    AIProviderRequestTemplate::create(std::move(provider_request).value(), request.memory);
            if (!request_template.ok()) {
                submit_result = SubmitResult::LOCAL_RESOURCE;
                return;
            }

            AIRateLimitKey rate_limit_key = AIRateLimitKey::create(std::string(request.chat_request.endpoint),
                                                                   request.chat_request.api_key, AICapability::CHAT);
            state = ai_allocate_shared<AITaskState>(request.memory, _core, request, std::move(request_template).value(),
                                                    std::move(rate_limit_key), std::move(callback));
            submit_result = SubmitResult::CREATED;
        });
    } catch (...) {
        submit_result = SubmitResult::LOCAL_RESOURCE;
    }

    if (submit_result != SubmitResult::CREATED) {
        const AISanitizedFailureClass failure_class = submit_result == SubmitResult::LOCAL_REQUEST
                                                              ? AISanitizedFailureClass::LOCAL_REQUEST
                                                              : AISanitizedFailureClass::LOCAL_RESOURCE;
        invoke_task_callback_noexcept(memory, &callback, AISanitizedRowFailure{.failure_class = failure_class});
        return AITaskHandle{};
    }

    AITaskHandle handle{state};
    state->start();
    return handle;
}

} // namespace starrocks
