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

#include "platform/llm/ai_http_client.h"

#include <curl/curl.h>
#include <curl/multi.h>
#include <curl/urlapi.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "common/thread/thread.h"

namespace starrocks {
namespace {

constexpr size_t kMaxUrlBytes = 16 * 1024;
constexpr size_t kMaxHeaderCount = 64;
constexpr size_t kMaxHeaderBytes = 64 * 1024;
constexpr size_t kMaxRetryAfterBytes = 256;
constexpr int kPollTimeoutMs = 20;
constexpr std::string_view kInvalidUrlStatus = "invalid AI HTTP URL";
constexpr std::string_view kInvalidHeaderStatus = "invalid AI HTTP header";
constexpr std::string_view kInvalidRequestStatus = "invalid AI HTTP request";
constexpr std::string_view kRequestMemoryStatus = "AI HTTP request memory limit exceeded";
constexpr std::string_view kClientShutdownStatus = "AI HTTP client is shut down";
constexpr std::string_view kClientCreateStatus = "failed to create AI HTTP client";

bool has_control_character(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return c < 0x20 || c == 0x7f; });
}

bool ascii_is_digit(unsigned char c) {
    return c >= '0' && c <= '9';
}

bool ascii_is_alpha(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
}

char ascii_lower(char c) {
    return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c;
}

std::string ascii_lower_copy(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    std::transform(value.begin(), value.end(), std::back_inserter(result), ascii_lower);
    return result;
}

bool ascii_case_equal(std::string_view left, std::string_view right) {
    return left.size() == right.size() && std::equal(left.begin(), left.end(), right.begin(), [](char lhs, char rhs) {
               return ascii_lower(lhs) == ascii_lower(rhs);
           });
}

bool is_header_token_character(unsigned char c) {
    if (ascii_is_alpha(c) || ascii_is_digit(c)) {
        return true;
    }
    constexpr std::string_view special = "!#$%&'*+-.^_`|~";
    return special.find(static_cast<char>(c)) != std::string_view::npos;
}

bool checked_add(size_t value, size_t* total) {
    if (value > std::numeric_limits<size_t>::max() - *total) {
        return false;
    }
    *total += value;
    return true;
}

void release_memory_noexcept(const AIMemoryContext& memory, size_t bytes) noexcept {
    memory.release(bytes);
}

template <typename Function>
auto run_in_physical_scope(const AIMemoryContext& memory, Function&& function) {
    using FunctionType = std::remove_reference_t<Function>;
    using Result = std::invoke_result_t<FunctionType&>;
    if constexpr (std::is_void_v<Result>) {
        FunctionType* function_pointer = std::addressof(function);
        memory.run_in_physical_scope(
                [](void* context) {
                    auto* action = static_cast<FunctionType*>(context);
                    std::invoke(*action);
                },
                function_pointer);
    } else {
        struct Context {
            FunctionType* function;
            std::optional<Result>* result;
        };
        std::optional<Result> result;
        Context context{std::addressof(function), &result};
        memory.run_in_physical_scope(
                [](void* opaque) {
                    auto* action = static_cast<Context*>(opaque);
                    action->result->emplace(std::invoke(*action->function));
                },
                &context);
        return std::move(*result);
    }
}

void clear_submit_payload(AIHttpRequest* request, AIHttpCallback* callback) noexcept {
    std::string().swap(request->url);
    std::vector<AIHttpHeader>().swap(request->headers);
    std::string().swap(request->body);
    request->resolved_endpoint.reset();
    AIQueryLifecycleProbe().swap(request->lifecycle);
    AIHttpCallback().swap(*callback);
    request->memory = {};
}

void destroy_submit_payload(AIHttpRequest* request, AIHttpCallback* callback) {
    AIMemoryContext memory = request->memory;
    run_in_physical_scope(memory, [&] { clear_submit_payload(request, callback); });
}

enum class AIHttpValidationResult : uint8_t {
    OK,
    INVALID_URL,
    INVALID_HEADER,
    INVALID_REQUEST,
    URL_MEMORY_LIMIT,
};

enum class AIHttpSchemePolicy : uint8_t {
    HTTP_OR_HTTPS,
    HTTPS_ONLY,
};

Status validation_status(AIHttpValidationResult result) {
    switch (result) {
    case AIHttpValidationResult::OK:
        return Status::OK();
    case AIHttpValidationResult::INVALID_URL:
        return Status::InvalidArgument(kInvalidUrlStatus);
    case AIHttpValidationResult::INVALID_HEADER:
        return Status::InvalidArgument(kInvalidHeaderStatus);
    case AIHttpValidationResult::INVALID_REQUEST:
        return Status::InvalidArgument(kInvalidRequestStatus);
    case AIHttpValidationResult::URL_MEMORY_LIMIT:
        return Status::MemoryLimitExceeded(kInvalidUrlStatus);
    }
    return Status::InvalidArgument(kInvalidRequestStatus);
}

AIHttpValidationResult validate_url_impl(const std::string& value,
                                         AIHttpSchemePolicy scheme_policy = AIHttpSchemePolicy::HTTP_OR_HTTPS) {
    if (value.empty() || value.size() > kMaxUrlBytes || has_control_character(value)) {
        return AIHttpValidationResult::INVALID_URL;
    }
    size_t authority = value.find("://");
    if (authority == std::string::npos || authority + 3 >= value.size() || value[authority + 3] == '/' ||
        value[authority + 3] == '?' || value[authority + 3] == '#') {
        return AIHttpValidationResult::INVALID_URL;
    }

    std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> parsed(curl_url(), curl_url_cleanup);
    if (parsed == nullptr) {
        return AIHttpValidationResult::URL_MEMORY_LIMIT;
    }
    if (curl_url_set(parsed.get(), CURLUPART_URL, value.c_str(), 0) != CURLUE_OK) {
        return AIHttpValidationResult::INVALID_URL;
    }

    auto get_part = [&](CURLUPart part, std::string* output) {
        char* raw = nullptr;
        CURLUcode code = curl_url_get(parsed.get(), part, &raw, 0);
        if (code != CURLUE_OK) {
            return code;
        }
        std::unique_ptr<char, decltype(&curl_free)> owned(raw, curl_free);
        output->assign(owned == nullptr ? "" : owned.get());
        return CURLUE_OK;
    };

    std::string scheme;
    std::string host;
    if (get_part(CURLUPART_SCHEME, &scheme) != CURLUE_OK || get_part(CURLUPART_HOST, &host) != CURLUE_OK) {
        return AIHttpValidationResult::INVALID_URL;
    }
    const bool scheme_is_allowed =
            ascii_case_equal(scheme, "https") ||
            (scheme_policy == AIHttpSchemePolicy::HTTP_OR_HTTPS && ascii_case_equal(scheme, "http"));
    if (host.empty() || !scheme_is_allowed) {
        return AIHttpValidationResult::INVALID_URL;
    }

    for (CURLUPart part : {CURLUPART_USER, CURLUPART_PASSWORD, CURLUPART_OPTIONS, CURLUPART_FRAGMENT}) {
        std::string unused;
        if (get_part(part, &unused) == CURLUE_OK) {
            return AIHttpValidationResult::INVALID_URL;
        }
    }
    return AIHttpValidationResult::OK;
}

AIHttpValidationResult validate_headers(const std::vector<AIHttpHeader>& headers, size_t* header_bytes) {
    if (headers.size() > kMaxHeaderCount) {
        return AIHttpValidationResult::INVALID_HEADER;
    }

    static const std::unordered_set<std::string> transport_owned = {
            "connection", "content-length",      "expect",           "host",
            "keep-alive", "proxy-authorization", "proxy-connection", "te",
            "trailer",    "transfer-encoding",   "upgrade",
    };
    std::unordered_set<std::string> names;
    size_t total = 0;
    for (const auto& header : headers) {
        if (header.name.empty() ||
            !std::all_of(header.name.begin(), header.name.end(),
                         [](unsigned char c) { return is_header_token_character(c); }) ||
            has_control_character(header.value)) {
            return AIHttpValidationResult::INVALID_HEADER;
        }
        std::string lower_name = ascii_lower_copy(header.name);
        if (transport_owned.contains(lower_name) || !names.emplace(lower_name).second) {
            return AIHttpValidationResult::INVALID_HEADER;
        }
        if (lower_name == "authorization" && std::all_of(header.value.begin(), header.value.end(),
                                                         [](unsigned char c) { return c == ' ' || c == '\t'; })) {
            return AIHttpValidationResult::INVALID_HEADER;
        }
        size_t line_bytes = header.name.size();
        if (!checked_add(header.value.size(), &line_bytes) || !checked_add(4, &line_bytes) ||
            !checked_add(line_bytes, &total) || total > kMaxHeaderBytes) {
            return AIHttpValidationResult::INVALID_HEADER;
        }
    }
    *header_bytes = total;
    return AIHttpValidationResult::OK;
}

AIHttpValidationResult validate_request(const AIHttpRequest& request, const AIHttpCallback& callback,
                                        size_t* request_bytes) {
    if (!callback) {
        return AIHttpValidationResult::INVALID_REQUEST;
    }
    AIHttpValidationResult result = validate_url_impl(request.url);
    if (result != AIHttpValidationResult::OK) {
        return result;
    }
    size_t header_bytes = 0;
    result = validate_headers(request.headers, &header_bytes);
    if (result != AIHttpValidationResult::OK) {
        return result;
    }
    if (request.request_deadline_ns < 0 || request.connect_timeout_ms < 0 ||
        request.connect_timeout_ms > std::numeric_limits<long>::max() || request.max_response_bytes == 0) {
        return AIHttpValidationResult::INVALID_REQUEST;
    }
    if (request.resolved_endpoint != nullptr &&
        !validate_resolved_http_endpoint(request.url, *request.resolved_endpoint).ok()) {
        return AIHttpValidationResult::INVALID_URL;
    }
    if (!request.lifecycle) {
        return AIHttpValidationResult::INVALID_REQUEST;
    }
    size_t total = request.url.size();
    if (!checked_add(header_bytes, &total) || !checked_add(request.body.size(), &total)) {
        return AIHttpValidationResult::INVALID_REQUEST;
    }
    *request_bytes = total;
    return AIHttpValidationResult::OK;
}

std::optional<AIHttpNoResponseCode> observe_request_lifecycle(const AIHttpRequest& request,
                                                              int64_t monotonic_now_ns) noexcept {
    switch (observe_ai_lifecycle(request.lifecycle, request.request_deadline_ns, monotonic_now_ns).state) {
    case AILifecycleState::ACTIVE:
        return std::nullopt;
    case AILifecycleState::CANCELLED:
        return AIHttpNoResponseCode::CANCELLATION;
    case AILifecycleState::DEADLINE_EXCEEDED:
        return AIHttpNoResponseCode::DEADLINE;
    }
    return AIHttpNoResponseCode::CANCELLATION;
}

AIHttpNoResponseCode classify_curl_result(CURLcode code, const AIHttpRequest& request) {
    switch (code) {
    case CURLE_COULDNT_RESOLVE_PROXY:
    case CURLE_COULDNT_RESOLVE_HOST:
        return AIHttpNoResponseCode::DNS;
    case CURLE_COULDNT_CONNECT:
        return AIHttpNoResponseCode::CONNECT;
    case CURLE_OPERATION_TIMEDOUT: {
        const std::optional<AIHttpNoResponseCode> lifecycle = observe_request_lifecycle(request, MonotonicNanos());
        return lifecycle.value_or(AIHttpNoResponseCode::TIMEOUT);
    }
    case CURLE_SEND_ERROR:
    case CURLE_UPLOAD_FAILED:
        return AIHttpNoResponseCode::SEND;
    case CURLE_RECV_ERROR:
        return AIHttpNoResponseCode::RECEIVE;
    case CURLE_GOT_NOTHING:
        return AIHttpNoResponseCode::EMPTY_REPLY;
    case CURLE_PARTIAL_FILE:
        return AIHttpNoResponseCode::PARTIAL_TRANSFER;
    case CURLE_HTTP2_STREAM:
        return AIHttpNoResponseCode::HTTP2_STREAM_RESET;
    case CURLE_SSL_CONNECT_ERROR:
    case CURLE_USE_SSL_FAILED:
        return AIHttpNoResponseCode::TLS_HANDSHAKE;
    case CURLE_PEER_FAILED_VERIFICATION:
    case CURLE_SSL_CACERT_BADFILE:
    case CURLE_SSL_CERTPROBLEM:
    case CURLE_SSL_CRL_BADFILE:
    case CURLE_SSL_ISSUER_ERROR:
    case CURLE_SSL_INVALIDCERTSTATUS:
        return AIHttpNoResponseCode::TLS_VERIFICATION;
    default:
        return AIHttpNoResponseCode::UNKNOWN;
    }
}

long deadline_timeout_ms(int64_t deadline_ns) {
    if (deadline_ns == 0) {
        return 0;
    }
    int64_t remaining_ns = deadline_ns - MonotonicNanos();
    if (remaining_ns <= 0) {
        return 0;
    }
    constexpr int64_t nanos_per_milli = 1'000'000;
    int64_t timeout_ms = remaining_ns / nanos_per_milli + (remaining_ns % nanos_per_milli != 0);
    return static_cast<long>(std::min<int64_t>(timeout_ms, std::numeric_limits<long>::max()));
}

int parse_fixed_decimal(std::string_view value, size_t offset, size_t length) {
    int parsed = 0;
    for (size_t index = offset; index < offset + length; ++index) {
        unsigned char c = static_cast<unsigned char>(value[index]);
        if (!ascii_is_digit(c)) {
            return -1;
        }
        parsed = parsed * 10 + (c - '0');
    }
    return parsed;
}

int fixed_token_index(std::string_view value, const std::string_view* tokens, size_t count) {
    for (size_t index = 0; index < count; ++index) {
        if (value == tokens[index]) {
            return static_cast<int>(index);
        }
    }
    return -1;
}

// Deliberately accept only RFC 9110's preferred IMF-fixdate form, not the obsolete date alternatives. Keeping this
// parser fixed-width and allocation-free also makes it safe to call from libcurl's noexcept header callback.
bool is_strict_imf_fixdate(std::string_view value) {
    if (value.size() != 29 || value[3] != ',' || value[4] != ' ' || value[7] != ' ' || value[11] != ' ' ||
        value[16] != ' ' || value[19] != ':' || value[22] != ':' || value[25] != ' ' || value.substr(26) != "GMT") {
        return false;
    }
    constexpr std::string_view weekdays[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    constexpr std::string_view months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    int expected_weekday = fixed_token_index(value.substr(0, 3), weekdays, std::size(weekdays));
    int month = fixed_token_index(value.substr(8, 3), months, std::size(months));
    int day = parse_fixed_decimal(value, 5, 2);
    int year = parse_fixed_decimal(value, 12, 4);
    int hour = parse_fixed_decimal(value, 17, 2);
    int minute = parse_fixed_decimal(value, 20, 2);
    int second = parse_fixed_decimal(value, 23, 2);
    if (expected_weekday < 0 || month < 0 || year <= 0 || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59) {
        return false;
    }
    std::chrono::year_month_day date{std::chrono::year{year}, std::chrono::month{static_cast<unsigned>(month + 1)},
                                     std::chrono::day{static_cast<unsigned>(day)}};
    return date.ok() && std::chrono::weekday{std::chrono::sys_days{date}}.c_encoding() == expected_weekday;
}

bool is_valid_retry_after(std::string_view value) {
    if (!value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char c) { return ascii_is_digit(c); })) {
        return true;
    }
    return is_strict_imf_fixdate(value);
}

} // namespace

Status validate_ai_http_url(const std::string& url) {
    return validation_status(validate_url_impl(url));
}

Status validate_ai_https_url(const std::string& url) {
    return validation_status(validate_url_impl(url, AIHttpSchemePolicy::HTTPS_ONLY));
}

AIHttpResponseBody::AIHttpResponseBody(std::string data, AIMemoryContext memory, size_t reserved_bytes) noexcept
        : _memory(std::move(memory)), _reserved_bytes(reserved_bytes) {
    _data.swap(data);
}

AIHttpResponseBody::~AIHttpResponseBody() noexcept {
    _release();
}

AIHttpResponseBody::AIHttpResponseBody(AIHttpResponseBody&& other) noexcept
        : _memory(std::move(other._memory)), _reserved_bytes(std::exchange(other._reserved_bytes, 0)) {
    _data.swap(other._data);
}

AIHttpResponseBody& AIHttpResponseBody::operator=(AIHttpResponseBody&& other) noexcept {
    if (this != &other) {
        _release();
        _data.swap(other._data);
        _memory = std::move(other._memory);
        _reserved_bytes = std::exchange(other._reserved_bytes, 0);
    }
    return *this;
}

void AIHttpResponseBody::_release() noexcept {
    size_t bytes = std::exchange(_reserved_bytes, 0);
    AIMemoryContext memory = _memory;
    // Return the physical buffer before returning its accounting. This ordering is also required when _release() is
    // called by move assignment, because logical release may immediately admit replacement work.
    bool physical_buffer_destroyed = false;
    try {
        run_in_physical_scope(memory, [&] {
            std::string().swap(_data);
            _memory = {};
            physical_buffer_destroyed = true;
            TEST_SYNC_POINT("AIHttpResponseBody::_release:in_physical_scope");
        });
    } catch (...) {
        // The Exec runner cannot throw. If a foreign runner throws before invoking the action, retain ownership so a
        // later destruction attempt cannot silently lose the logical reservation.
        if (!physical_buffer_destroyed) {
            _reserved_bytes = bytes;
            return;
        }
    }
    release_memory_noexcept(memory, bytes);
}

AIHttpResponse::AIHttpResponse(int64_t status_code, AIHttpResponseBody body,
                               std::optional<std::string> retry_after_value, AIMemoryContext memory) noexcept
        : status_code(status_code), body(std::move(body)), _memory(std::move(memory)) {
    retry_after.swap(retry_after_value);
}

AIHttpResponse::~AIHttpResponse() noexcept {
    _release_retry_after();
}

AIHttpResponse::AIHttpResponse(AIHttpResponse&& other) noexcept
        : status_code(other.status_code), body(std::move(other.body)), _memory(std::move(other._memory)) {
    retry_after.swap(other.retry_after);
}

AIHttpResponse& AIHttpResponse::operator=(AIHttpResponse&& other) noexcept {
    if (this != &other) {
        _release_retry_after();
        status_code = other.status_code;
        body = std::move(other.body);
        retry_after.swap(other.retry_after);
        _memory = std::move(other._memory);
    }
    return *this;
}

void AIHttpResponse::_release_retry_after() noexcept {
    AIMemoryContext memory = _memory;
    try {
        run_in_physical_scope(memory, [&] {
            retry_after.reset();
            _memory = {};
        });
    } catch (...) {
    }
}

class AIHttpClientImpl final : public AIHttpClient {
public:
    explicit AIHttpClientImpl(AIHttpClientOptions options) : _options(std::move(options)) {}
    ~AIHttpClientImpl() override { shutdown(); }

    Status start() {
        try {
            _thread = std::thread([this] { run(); });
        } catch (...) {
            return Status::InternalError(kClientCreateStatus);
        }

        std::unique_lock lock(_mutex);
        _initialized_cv.wait(lock, [this] { return _initialized; });
        Status status = _initialization_status;
        if (status.ok()) {
            _accepting = true;
            return Status::OK();
        }
        lock.unlock();
        if (_thread.joinable()) {
            _thread.join();
        }
        return status;
    }

    Status submit(AIHttpRequest request, AIHttpCallback callback) override {
        AIMemoryContext memory = request.memory;
        size_t request_bytes = 0;
        AIHttpValidationResult validation = AIHttpValidationResult::OK;
        try {
            validation = run_in_physical_scope(memory, [&] {
                AIHttpValidationResult result = validate_request(request, callback, &request_bytes);
                TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::submit:validation_result:in_physical_scope", &result);
                return result;
            });
        } catch (...) {
            try {
                destroy_submit_payload(&request, &callback);
            } catch (...) {
            }
            return Status::MemoryLimitExceeded(kRequestMemoryStatus);
        }
        if (validation != AIHttpValidationResult::OK) {
            try {
                destroy_submit_payload(&request, &callback);
            } catch (...) {
                return Status::MemoryLimitExceeded(kRequestMemoryStatus);
            }
            Status status = validation_status(validation);
            TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::submit:validation_status:outside_physical_scope", &status);
            return status;
        }

        bool reserved = false;
        if (memory && request_bytes > 0) {
            reserved = memory.reserve(request_bytes);
            if (!reserved) {
                try {
                    destroy_submit_payload(&request, &callback);
                } catch (...) {
                }
                return Status::MemoryLimitExceeded(kRequestMemoryStatus);
            }
        }

        std::unique_ptr<Attempt> attempt;
        try {
            run_in_physical_scope(memory, [&] {
                TEST_SYNC_POINT("AIHttpClientImpl::submit:before_attempt_allocation");
                attempt = std::make_unique<Attempt>(std::move(request), std::move(callback), request_bytes, reserved);
                clear_submit_payload(&request, &callback);
                TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::submit:accepted_request_source_cleared:in_physical_scope",
                                         &request);
                TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::submit:accepted_callback_source_cleared:in_physical_scope",
                                         &callback);
            });
        } catch (...) {
            try {
                destroy_submit_payload(&request, &callback);
            } catch (...) {
            }
            release_memory_noexcept(memory, reserved ? request_bytes : 0);
            return Status::MemoryLimitExceeded(kRequestMemoryStatus);
        }

        enum class EnqueueResult { ACCEPTED, SHUTDOWN, MEMORY_LIMIT };
        EnqueueResult enqueue_result = EnqueueResult::ACCEPTED;
        {
            std::lock_guard lock(_mutex);
            if (!_accepting || _shutdown_requested) {
                enqueue_result = EnqueueResult::SHUTDOWN;
            } else {
                try {
                    // Allocate the list node before moving |attempt| so an allocation failure leaves local ownership
                    // intact for lock-free payload destruction and memory release below.
                    run_in_physical_scope(memory, [&] {
                        _queued.emplace_back(std::move(attempt));
                        TEST_SYNC_POINT("AIHttpClientImpl::queued_node_allocated:in_physical_scope");
                    });
                    if (_multi != nullptr) {
                        (void)curl_multi_wakeup(_multi);
                    }
                } catch (...) {
                    enqueue_result = EnqueueResult::MEMORY_LIMIT;
                }
            }
        }
        if (enqueue_result != EnqueueResult::ACCEPTED) {
            release_unaccepted(std::move(attempt));
            return enqueue_result == EnqueueResult::SHUTDOWN ? Status::Shutdown(kClientShutdownStatus)
                                                             : Status::MemoryLimitExceeded(kRequestMemoryStatus);
        }
        _work_cv.notify_one();
        return Status::OK();
    }

    void shutdown() override {
        bool called_from_io_thread = false;
        {
            std::lock_guard lock(_mutex);
            _accepting = false;
            _shutdown_requested = true;
            called_from_io_thread = _io_thread_running && _io_thread_id == std::this_thread::get_id();
            if (_multi != nullptr) {
                (void)curl_multi_wakeup(_multi);
            }
        }
        _work_cv.notify_one();
        // A callback or memory hook runs on the I/O thread. It may request shutdown, but it must never wait for an
        // external joiner that is itself waiting for this thread to finish.
        if (called_from_io_thread) {
            return;
        }

        std::unique_lock join_lock(_shutdown_mutex);
        if (_join_complete) {
            return;
        }
        if (_join_in_progress) {
            _shutdown_cv.wait(join_lock, [this] { return _join_complete; });
            return;
        }
        if (!_thread.joinable()) {
            _join_complete = true;
            join_lock.unlock();
            _shutdown_cv.notify_all();
            return;
        }
        _join_in_progress = true;
        join_lock.unlock();

        _thread.join();

        join_lock.lock();
        _join_in_progress = false;
        _join_complete = true;
        join_lock.unlock();
        _shutdown_cv.notify_all();
    }

private:
    struct Attempt {
        Attempt(AIHttpRequest request, AIHttpCallback callback, size_t request_bytes, bool request_reserved) noexcept
                : request(std::move(request)),
                  callback(std::move(callback)),
                  request_bytes(request_bytes),
                  request_reserved(request_reserved) {}

        AIHttpRequest request;
        AIHttpCallback callback;
        size_t request_bytes = 0;
        bool request_reserved = false;
        CURL* easy = nullptr;
        curl_slist* header_list = nullptr;
        curl_slist* resolve_list = nullptr;
        std::string response;
        size_t response_reserved = 0;
        std::optional<AIHttpNoResponseCode> local_failure;
        std::optional<std::string> retry_after;
        bool retry_after_seen = false;
        bool retry_after_invalid = false;
    };

    static void release_unaccepted(std::unique_ptr<Attempt> attempt) noexcept {
        if (attempt == nullptr) {
            return;
        }
        AIMemoryContext memory = attempt->request.memory;
        size_t request_bytes = std::exchange(attempt->request_bytes, 0);
        bool request_reserved = std::exchange(attempt->request_reserved, false);
        // Destroy URL/header/body capacity before releasing its accounting. The external hook is intentionally called
        // only after the attempt is gone and never while _mutex is held.
        try {
            run_in_physical_scope(memory, [&] {
                TEST_SYNC_POINT("AIHttpClientImpl::destroy_attempt:in_physical_scope");
                attempt.reset();
            });
        } catch (...) {
        }
        release_memory_noexcept(memory, request_reserved ? request_bytes : 0);
    }

    static size_t write_callback(char* data, size_t size, size_t count, void* context) noexcept {
        auto* attempt = static_cast<Attempt*>(context);
        if (size != 0 && count > std::numeric_limits<size_t>::max() / size) {
            attempt->local_failure = AIHttpNoResponseCode::RESPONSE_CAP;
            return 0;
        }
        size_t bytes = size * count;
        if (bytes > attempt->request.max_response_bytes - attempt->response.size()) {
            attempt->local_failure = AIHttpNoResponseCode::RESPONSE_CAP;
            return 0;
        }
        try {
            run_in_physical_scope(attempt->request.memory, [&] {
                TEST_SYNC_POINT("AIHttpClientImpl::write_callback:before_response_append");
                attempt->response.append(data, bytes);
                TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::write_callback:after_response_append", &attempt->response);
            });
        } catch (...) {
            attempt->local_failure = AIHttpNoResponseCode::MEMORY_LIMIT;
            return 0;
        }
        if (bytes > 0 && attempt->request.memory) {
            if (!attempt->request.memory.reserve(bytes)) {
                // The just-appended bytes have physical process accounting but no Query/WorkGroup label. Returning
                // zero terminates libcurl; finish_no_response destroys the whole response in the same process scope
                // and releases only earlier successful logical reservations.
                attempt->local_failure = AIHttpNoResponseCode::MEMORY_LIMIT;
                return 0;
            }
            attempt->response_reserved += bytes;
        }
        return bytes;
    }

    static size_t header_callback(char* data, size_t size, size_t count, void* context) noexcept {
        auto* attempt = static_cast<Attempt*>(context);
        if (size != 0 && count > std::numeric_limits<size_t>::max() / size) {
            return 0;
        }
        size_t bytes = size * count;
        std::string_view line(data, bytes);
        if (line.starts_with("HTTP/")) {
            bool reset_failed = false;
            try {
                run_in_physical_scope(attempt->request.memory, [&] { attempt->retry_after.reset(); });
            } catch (...) {
                reset_failed = true;
            }
            attempt->retry_after_seen = false;
            attempt->retry_after_invalid = reset_failed;
            return bytes;
        }
        size_t colon = line.find(':');
        if (colon == std::string_view::npos || !ascii_case_equal(line.substr(0, colon), "Retry-After")) {
            return bytes;
        }
        if (attempt->retry_after_seen) {
            attempt->retry_after_invalid = true;
            try {
                run_in_physical_scope(attempt->request.memory, [&] { attempt->retry_after.reset(); });
            } catch (...) {
            }
            return bytes;
        }
        attempt->retry_after_seen = true;
        std::string_view value = line.substr(colon + 1);
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
            value.remove_prefix(1);
        }
        while (!value.empty() &&
               (value.back() == '\r' || value.back() == '\n' || value.back() == ' ' || value.back() == '\t')) {
            value.remove_suffix(1);
        }
        if (value.empty() || value.size() > kMaxRetryAfterBytes ||
            !std::all_of(value.begin(), value.end(), [](unsigned char c) { return c >= 0x20 && c <= 0x7e; }) ||
            !is_valid_retry_after(value)) {
            attempt->retry_after_invalid = true;
            return bytes;
        }
        try {
            run_in_physical_scope(attempt->request.memory, [&] { attempt->retry_after.emplace(value); });
        } catch (...) {
            attempt->retry_after_invalid = true;
        }
        return bytes;
    }

    static int progress_callback(void* context, curl_off_t, curl_off_t, curl_off_t, curl_off_t) noexcept {
        auto* attempt = static_cast<Attempt*>(context);
        if (std::optional<AIHttpNoResponseCode> code = observe_request_lifecycle(attempt->request, MonotonicNanos());
            code.has_value()) {
            attempt->local_failure = *code;
            return 1;
        }
        return 0;
    }

    template <typename T>
    static bool set_option(CURL* easy, CURLoption option, T value) {
        return curl_easy_setopt(easy, option, value) == CURLE_OK;
    }

    bool configure(Attempt* attempt) {
        attempt->easy = curl_easy_init();
        if (attempt->easy == nullptr) {
            return false;
        }
        for (const auto& header : attempt->request.headers) {
            std::string line;
            try {
                line.reserve(header.name.size() + header.value.size() + 2);
                line.append(header.name).append(": ").append(header.value);
            } catch (...) {
                return false;
            }
            curl_slist* appended = curl_slist_append(attempt->header_list, line.c_str());
            if (appended == nullptr) {
                return false;
            }
            attempt->header_list = appended;
        }
        curl_slist* appended = curl_slist_append(attempt->header_list, "Expect:");
        if (appended == nullptr) {
            return false;
        }
        attempt->header_list = appended;

        if (attempt->request.resolved_endpoint != nullptr &&
            http_endpoint_needs_dns_pinning(*attempt->request.resolved_endpoint)) {
            auto resolve_entry = make_curl_resolve_entry(*attempt->request.resolved_endpoint);
            if (!resolve_entry.ok()) {
                return false;
            }
            appended = curl_slist_append(attempt->resolve_list, resolve_entry->c_str());
            if (appended == nullptr) {
                return false;
            }
            attempt->resolve_list = appended;
        }

        bool ok = set_option(attempt->easy, CURLOPT_URL, attempt->request.url.c_str()) &&
                  set_option(attempt->easy, CURLOPT_POST, 1L) &&
                  set_option(attempt->easy, CURLOPT_POSTFIELDS, attempt->request.body.data()) &&
                  set_option(attempt->easy, CURLOPT_POSTFIELDSIZE_LARGE,
                             static_cast<curl_off_t>(attempt->request.body.size())) &&
                  set_option(attempt->easy, CURLOPT_HTTPHEADER, attempt->header_list) &&
                  set_option(attempt->easy, CURLOPT_FAILONERROR, 0L) &&
                  set_option(attempt->easy, CURLOPT_FOLLOWLOCATION, 0L) &&
                  set_option(attempt->easy, CURLOPT_PROXY, "") && set_option(attempt->easy, CURLOPT_NOPROXY, "*") &&
                  set_option(attempt->easy, CURLOPT_PROTOCOLS_STR, "http,https") &&
                  set_option(attempt->easy, CURLOPT_REDIR_PROTOCOLS_STR, "http,https") &&
                  set_option(attempt->easy, CURLOPT_SSL_VERIFYPEER, 1L) &&
                  set_option(attempt->easy, CURLOPT_SSL_VERIFYHOST, 2L) &&
                  set_option(attempt->easy, CURLOPT_NOSIGNAL, 1L) &&
                  set_option(attempt->easy, CURLOPT_WRITEFUNCTION, &AIHttpClientImpl::write_callback) &&
                  set_option(attempt->easy, CURLOPT_WRITEDATA, attempt) &&
                  set_option(attempt->easy, CURLOPT_HEADERFUNCTION, &AIHttpClientImpl::header_callback) &&
                  set_option(attempt->easy, CURLOPT_HEADERDATA, attempt) &&
                  set_option(attempt->easy, CURLOPT_NOPROGRESS, 0L) &&
                  set_option(attempt->easy, CURLOPT_XFERINFOFUNCTION, &AIHttpClientImpl::progress_callback) &&
                  set_option(attempt->easy, CURLOPT_XFERINFODATA, attempt);
        if (ok && attempt->resolve_list != nullptr) {
            ok = set_option(attempt->easy, CURLOPT_RESOLVE, attempt->resolve_list);
        }
        if (ok && attempt->request.connect_timeout_ms > 0) {
            ok = set_option(attempt->easy, CURLOPT_CONNECTTIMEOUT_MS,
                            static_cast<long>(attempt->request.connect_timeout_ms));
        }
        if (ok && attempt->request.request_deadline_ns > 0) {
            const long timeout_ms = deadline_timeout_ms(attempt->request.request_deadline_ns);
            if (timeout_ms <= 0) {
                attempt->local_failure = AIHttpNoResponseCode::DEADLINE;
                return false;
            }
            ok = set_option(attempt->easy, CURLOPT_TIMEOUT_MS, timeout_ms);
        }
        if (ok && !_options.ca_bundle_path.empty()) {
            ok = set_option(attempt->easy, CURLOPT_CAINFO, _options.ca_bundle_path.c_str());
        }
        return ok;
    }

    void cleanup_curl(Attempt* attempt) {
        if (attempt->easy != nullptr) {
            curl_easy_cleanup(attempt->easy);
            attempt->easy = nullptr;
        }
        if (attempt->header_list != nullptr) {
            curl_slist_free_all(attempt->header_list);
            attempt->header_list = nullptr;
        }
        if (attempt->resolve_list != nullptr) {
            curl_slist_free_all(attempt->resolve_list);
            attempt->resolve_list = nullptr;
        }
    }

    void invoke_callback(AIHttpCallback* callback, AIHttpResult result, const AIMemoryContext& memory) noexcept {
        try {
            (*callback)(std::move(result));
        } catch (...) {
        }
        try {
            run_in_physical_scope(memory, [&] {
                AIHttpCallback().swap(*callback);
                TEST_SYNC_POINT("AIHttpClientImpl::invoke_callback:callback_cleared:in_physical_scope");
            });
        } catch (...) {
        }
    }

    void finish_no_response(std::unique_ptr<Attempt> attempt, AIHttpNoResponseCode code) {
        AIMemoryContext memory = attempt->request.memory;
        AIHttpCallback callback;
        size_t response_bytes = std::exchange(attempt->response_reserved, 0);
        size_t request_bytes = std::exchange(attempt->request_bytes, 0);
        bool request_reserved = std::exchange(attempt->request_reserved, false);
        // The callback/result state is already local. Destroy all failed response and sensitive request capacity before
        // returning the corresponding accounting, then invoke user code with no Attempt left alive.
        run_in_physical_scope(memory, [&] {
            cleanup_curl(attempt.get());
            callback = std::move(attempt->callback);
            TEST_SYNC_POINT("AIHttpClientImpl::destroy_attempt:in_physical_scope");
            attempt.reset();
        });
        release_memory_noexcept(memory, response_bytes);
        release_memory_noexcept(memory, request_reserved ? request_bytes : 0);
        invoke_callback(&callback, AIHttpNoResponse{code}, memory);
    }

    void finish_response(std::unique_ptr<Attempt> attempt, int64_t status_code) {
        AIMemoryContext memory = attempt->request.memory;
        AIHttpCallback callback;
        std::string response_data;
        std::optional<std::string> retry_after;
        size_t response_bytes = std::exchange(attempt->response_reserved, 0);
        size_t request_bytes = std::exchange(attempt->request_bytes, 0);
        bool request_reserved = std::exchange(attempt->request_reserved, false);

        // Free the sensitive request buffers before returning their accounting. The successful response buffer remains
        // reserved and moves into AIHttpResponseBody below.
        run_in_physical_scope(memory, [&] {
            cleanup_curl(attempt.get());
            callback = std::move(attempt->callback);
            response_data = std::move(attempt->response);
            if (!attempt->retry_after_invalid) {
                retry_after = std::move(attempt->retry_after);
            }
            TEST_SYNC_POINT("AIHttpClientImpl::destroy_attempt:in_physical_scope");
            attempt.reset();
        });
        release_memory_noexcept(memory, request_reserved ? request_bytes : 0);

        std::optional<AIHttpResponse> response;
        try {
            run_in_physical_scope(memory, [&] {
                AIHttpResponseBody body(std::move(response_data), memory, response_bytes);
                AIHttpResponse completed(status_code, std::move(body), std::move(retry_after), memory);
                response.emplace(std::move(completed));
                std::string().swap(response_data);
                retry_after.reset();
                TEST_SYNC_POINT_CALLBACK("AIHttpClientImpl::finish_response:response_source_cleared:in_physical_scope",
                                         &response_data);
                TEST_SYNC_POINT_CALLBACK(
                        "AIHttpClientImpl::finish_response:retry_after_source_cleared:in_physical_scope", &retry_after);
            });
        } catch (...) {
            if (response.has_value()) {
                try {
                    run_in_physical_scope(memory, [&] { response.reset(); });
                } catch (...) {
                }
            } else {
                try {
                    run_in_physical_scope(memory, [&] {
                        std::string().swap(response_data);
                        retry_after.reset();
                    });
                } catch (...) {
                }
                release_memory_noexcept(memory, response_bytes);
            }
            invoke_callback(&callback, AIHttpNoResponse{AIHttpNoResponseCode::MEMORY_LIMIT}, memory);
            return;
        }
        invoke_callback(&callback, std::move(*response), memory);
    }

    void start_attempt(std::unique_ptr<Attempt> attempt, CURLM* multi) {
        if (std::optional<AIHttpNoResponseCode> code = observe_request_lifecycle(attempt->request, MonotonicNanos());
            code.has_value()) {
            finish_no_response(std::move(attempt), *code);
            return;
        }
        bool configured = false;
        try {
            configured = run_in_physical_scope(attempt->request.memory, [&] { return configure(attempt.get()); });
        } catch (...) {
        }
        if (!configured) {
            AIHttpNoResponseCode code = attempt->local_failure.value_or(AIHttpNoResponseCode::UNKNOWN);
            finish_no_response(std::move(attempt), code);
            return;
        }
        CURL* easy = attempt->easy;
        try {
            auto [it, inserted] = run_in_physical_scope(attempt->request.memory, [&] {
                auto result = _active.try_emplace(easy, std::move(attempt));
                if (result.second) {
                    TEST_SYNC_POINT("AIHttpClientImpl::active_node_allocated:in_physical_scope");
                }
                return result;
            });
            if (!inserted) {
                finish_no_response(std::move(attempt), AIHttpNoResponseCode::UNKNOWN);
                return;
            }
        } catch (...) {
            finish_no_response(std::move(attempt), AIHttpNoResponseCode::UNKNOWN);
            return;
        }
        CURLMcode add_status = run_in_physical_scope(_active.at(easy)->request.memory,
                                                     [&] { return curl_multi_add_handle(multi, easy); });
        if (add_status != CURLM_OK) {
            auto it = _active.find(easy);
            std::unique_ptr<Attempt> failed = std::move(it->second);
            run_in_physical_scope(failed->request.memory, [&] { _active.erase(it); });
            finish_no_response(std::move(failed), AIHttpNoResponseCode::UNKNOWN);
        }
    }

    void cancel_expired_active(CURLM* multi) {
        const int64_t now = MonotonicNanos();
        for (auto it = _active.begin(); it != _active.end();) {
            const std::optional<AIHttpNoResponseCode> code = observe_request_lifecycle(it->second->request, now);
            if (!code.has_value()) {
                ++it;
                continue;
            }
            std::unique_ptr<Attempt> attempt = std::move(it->second);
            AIMemoryContext memory = attempt->request.memory;
            run_in_physical_scope(memory, [&] {
                it = _active.erase(it);
                (void)curl_multi_remove_handle(multi, attempt->easy);
            });
            finish_no_response(std::move(attempt), *code);
        }
    }

    void process_completions(CURLM* multi) {
        int messages = 0;
        while (CURLMsg* message = curl_multi_info_read(multi, &messages)) {
            if (message->msg != CURLMSG_DONE) {
                continue;
            }
            auto it = _active.find(message->easy_handle);
            if (it == _active.end()) {
                continue;
            }
            std::unique_ptr<Attempt> attempt = std::move(it->second);
            run_in_physical_scope(attempt->request.memory, [&] {
                _active.erase(it);
                TEST_SYNC_POINT("AIHttpClientImpl::active_node_deallocated:in_physical_scope");
                (void)curl_multi_remove_handle(multi, message->easy_handle);
            });

            if (message->data.result != CURLE_OK) {
                AIHttpNoResponseCode code =
                        attempt->local_failure.value_or(classify_curl_result(message->data.result, attempt->request));
                finish_no_response(std::move(attempt), code);
                continue;
            }
            long status_code = 0;
            if (attempt->local_failure.has_value() ||
                curl_easy_getinfo(message->easy_handle, CURLINFO_RESPONSE_CODE, &status_code) != CURLE_OK ||
                status_code <= 0) {
                finish_no_response(std::move(attempt), attempt->local_failure.value_or(AIHttpNoResponseCode::UNKNOWN));
                continue;
            }
            finish_response(std::move(attempt), status_code);
        }
    }

    void complete_all_active(CURLM* multi, AIHttpNoResponseCode code) {
        while (!_active.empty()) {
            auto it = _active.begin();
            std::unique_ptr<Attempt> attempt = std::move(it->second);
            run_in_physical_scope(attempt->request.memory, [&] {
                _active.erase(it);
                (void)curl_multi_remove_handle(multi, attempt->easy);
            });
            finish_no_response(std::move(attempt), code);
        }
    }

    void run() {
        Thread::set_thread_name(pthread_self(), "ai_http_io");
        CURLM* multi = curl_multi_init();
        {
            std::lock_guard lock(_mutex);
            _io_thread_id = std::this_thread::get_id();
            _io_thread_running = multi != nullptr;
            _initialized = true;
            _initialization_status = multi == nullptr ? Status::InternalError(kClientCreateStatus) : Status::OK();
            _multi = multi;
        }
        _initialized_cv.notify_all();
        if (multi == nullptr) {
            return;
        }

        for (;;) {
            std::list<std::unique_ptr<Attempt>> queued;
            bool shutdown_requested = false;
            {
                std::unique_lock lock(_mutex);
                if (_active.empty() && _queued.empty() && !_shutdown_requested) {
                    TEST_SYNC_POINT("AIHttpClientImpl::run:before_idle_wait");
                    _work_cv.wait(lock, [this] { return _shutdown_requested || !_queued.empty(); });
                }
                queued.splice(queued.end(), _queued);
                shutdown_requested = _shutdown_requested;
            }
            while (!queued.empty()) {
                AIMemoryContext memory = queued.front()->request.memory;
                std::unique_ptr<Attempt> attempt;
                run_in_physical_scope(memory, [&] {
                    attempt = std::move(queued.front());
                    queued.pop_front();
                    TEST_SYNC_POINT("AIHttpClientImpl::queued_node_deallocated:in_physical_scope");
                });
                if (shutdown_requested) {
                    finish_no_response(std::move(attempt), AIHttpNoResponseCode::SHUTDOWN);
                } else {
                    start_attempt(std::move(attempt), multi);
                }
            }
            if (shutdown_requested) {
                complete_all_active(multi, AIHttpNoResponseCode::SHUTDOWN);
                break;
            }
            cancel_expired_active(multi);

            int running = 0;
            CURLMcode perform_status = curl_multi_perform(multi, &running);
            if (perform_status != CURLM_OK) {
                complete_all_active(multi, AIHttpNoResponseCode::UNKNOWN);
            } else {
                process_completions(multi);
            }

            int ready = 0;
            if (curl_multi_poll(multi, nullptr, 0, kPollTimeoutMs, &ready) != CURLM_OK) {
                complete_all_active(multi, AIHttpNoResponseCode::UNKNOWN);
            }
        }

        {
            std::lock_guard lock(_mutex);
            _io_thread_running = false;
            _multi = nullptr;
        }
        curl_multi_cleanup(multi);
    }

    AIHttpClientOptions _options;
    std::mutex _mutex;
    std::condition_variable _initialized_cv;
    std::condition_variable _work_cv;
    bool _initialized = false;
    Status _initialization_status = Status::Uninitialized(kClientCreateStatus);
    std::thread::id _io_thread_id;
    // Thread IDs may be reused after exit, so shutdown's reentrant fast path must also require this live-state bit.
    bool _io_thread_running = false;
    bool _accepting = false;
    bool _shutdown_requested = false;
    // Node-only containers have no persistent bucket/block capacity. Each node is allocated and erased under the
    // corresponding request context, so physical allocation and deallocation always use the same process scope.
    std::list<std::unique_ptr<Attempt>> _queued;
    std::map<CURL*, std::unique_ptr<Attempt>> _active;
    CURLM* _multi = nullptr;
    std::thread _thread;
    std::mutex _shutdown_mutex;
    std::condition_variable _shutdown_cv;
    bool _join_in_progress = false;
    bool _join_complete = false;
};

StatusOr<std::unique_ptr<AIHttpClient>> AIHttpClient::create(AIHttpClientOptions options) {
    if (has_control_character(options.ca_bundle_path)) {
        return Status::InvalidArgument(kClientCreateStatus);
    }
    std::unique_ptr<AIHttpClientImpl> client;
    try {
        client = std::make_unique<AIHttpClientImpl>(std::move(options));
    } catch (...) {
        return Status::MemoryLimitExceeded(kClientCreateStatus);
    }
    RETURN_IF_ERROR(client->start());
    return std::unique_ptr<AIHttpClient>(std::move(client));
}

} // namespace starrocks
