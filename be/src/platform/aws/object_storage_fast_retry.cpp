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

#include "platform/aws/object_storage_fast_retry.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/monitoring/HttpClientMetrics.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <ios>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "common/config_object_storage_fwd.h"
#include "common/logging.h"
#include "fs/plain_aws_http_client_scope.h"

namespace starrocks {

namespace {

constexpr char kAllocationTag[] = "ObjectStorageFastRetry";

// Value of the amz-sdk-request header on the SDK's first attempt of a request. Later attempts carry
// "attempt=<n>; max=<m>".
constexpr std::string_view kSdkFirstAttempt = "attempt=1";
constexpr char kIfMatchHeader[] = "if-match";
constexpr char kIfNoneMatchHeader[] = "if-none-match";

constexpr std::array<const char*, kNumFastRetryResults> kResultNames = {"expired", "resend_fast", "resend_slow",
                                                                        "resend_error", "no_budget"};

// Short timeout of the send in progress on this thread. CurlHttpClient::MakeRequest() sends synchronously on the
// calling thread and calls OverrideOptionsOnConnectionHandle() right before curl_easy_perform(), so the override
// reads the value that was set just before the call. The override clears it, and ScopedShortTimeout clears it
// again when the call returns, in case the override was not reached. The value can never stay on the thread
// for a later request.
thread_local int64_t tls_short_timeout_ms = 0;

// Aborts a GET send that goes idle_ms without receiving a byte, with a token of the budget that it takes only then:
// a GET holds no token while it runs, so any number of GETs in flight can be watched. Without a token the send goes
// on, and it is aborted later once a token is there. libcurl calls progress() while the send runs, about once per
// second when nothing arrives. The pool resets a handle when the send succeeds and destroys it when the send fails,
// which removes the callback with it.
struct IdleWatch {
    int64_t idle_ms = 0;
    FastRetryBudget* budget = nullptr;
    std::chrono::steady_clock::time_point last_byte;
    curl_off_t received = 0;
    bool expired = false;
    bool no_budget = false;

    static int progress(void* data, curl_off_t /*dltotal*/, curl_off_t dlnow, curl_off_t /*ultotal*/,
                        curl_off_t /*ulnow*/) {
        auto* watch = static_cast<IdleWatch*>(data);
        const auto now = std::chrono::steady_clock::now();
        if (dlnow != watch->received) {
            watch->received = dlnow;
            watch->last_byte = now;
            return 0;
        }
        if (now - watch->last_byte < std::chrono::milliseconds(watch->idle_ms)) {
            return 0;
        }
        if (!watch->budget->try_acquire()) {
            watch->no_budget = true;
            return 0;
        }
        watch->expired = true;
        return 1;
    }
};

// The idle watch of the GET send in progress on this thread, handed to OverrideOptionsOnConnectionHandle() like
// tls_short_timeout_ms.
thread_local IdleWatch* tls_idle_watch = nullptr;

class ScopedIdleWatch {
public:
    explicit ScopedIdleWatch(IdleWatch* watch) { tls_idle_watch = watch; }
    ~ScopedIdleWatch() { tls_idle_watch = nullptr; }

    ScopedIdleWatch(const ScopedIdleWatch&) = delete;
    ScopedIdleWatch& operator=(const ScopedIdleWatch&) = delete;
};

class ScopedShortTimeout {
public:
    explicit ScopedShortTimeout(int64_t timeout_ms) { tls_short_timeout_ms = timeout_ms; }
    ~ScopedShortTimeout() { tls_short_timeout_ms = 0; }

    ScopedShortTimeout(const ScopedShortTimeout&) = delete;
    ScopedShortTimeout& operator=(const ScopedShortTimeout&) = delete;
};

// The token that a send took. It goes back to the budget when the send ends, also when the send throws, unless the
// send timed out.
class ScopedToken {
public:
    explicit ScopedToken(FastRetryBudget* budget) : _budget(budget) {}
    ~ScopedToken() {
        if (_budget != nullptr) {
            _budget->release();
        }
    }

    ScopedToken(const ScopedToken&) = delete;
    ScopedToken& operator=(const ScopedToken&) = delete;

    // The send timed out: it keeps the token.
    void consume() { _budget = nullptr; }

private:
    FastRetryBudget* _budget;
};

std::string_view as_string_view(const Aws::String& value) {
    return std::string_view(value.data(), value.size());
}

// The value of a header, or nullopt when the request does not have it. HttpRequest::GetHeaderValue() asserts that
// the header exists.
std::optional<std::string_view> header_value(const Aws::Http::HttpRequest& request, const char* name) {
    if (!request.HasHeader(name)) {
        return std::nullopt;
    }
    return as_string_view(request.GetHeaderValue(name));
}

std::optional<int64_t> content_length(const Aws::Http::HttpRequest& request) {
    std::optional<std::string_view> value = header_value(request, Aws::Http::CONTENT_LENGTH_HEADER);
    if (!value.has_value()) {
        return std::nullopt;
    }
    int64_t length = 0;
    const char* end = value->data() + value->size();
    auto [ptr, ec] = std::from_chars(value->data(), end, length);
    if (ec != std::errc() || ptr != end || length < 0) {
        return std::nullopt;
    }
    return length;
}

// The length of the closed byte range of a GET (Range: bytes=first-last), or nullopt for any other range.
std::optional<int64_t> range_bytes(const Aws::Http::HttpRequest& request) {
    constexpr std::string_view kPrefix = "bytes=";
    std::optional<std::string_view> value = header_value(request, "range");
    if (!value.has_value() || !value->starts_with(kPrefix)) {
        return std::nullopt;
    }
    value->remove_prefix(kPrefix.size());
    const char* end = value->data() + value->size();
    int64_t first = 0;
    int64_t last = 0;
    auto [dash, first_ec] = std::from_chars(value->data(), end, first);
    if (first_ec != std::errc() || dash == end || *dash != '-') {
        return std::nullopt;
    }
    auto [ptr, last_ec] = std::from_chars(dash + 1, end, last);
    // A length that int64_t cannot hold is no small range either.
    if (last_ec != std::errc() || ptr != end || first < 0 || last < first ||
        last - first == std::numeric_limits<int64_t>::max()) {
        return std::nullopt;
    }
    return last - first + 1;
}

// CurlHttpClient reports a curl error as "curlCode: <code>, <message>". On a send that carries the short timeout,
// CURLE_OPERATION_TIMEDOUT means that the send timed out: the short timeout expired, or the connect timeout or the
// low speed time ran out first. With the default configs the short timeout is the shortest of the three. Each of
// them ends the send without a response.
bool short_timeout_expired(const Aws::Http::HttpResponse& response) {
    static const std::string kTimeoutPrefix =
            "curlCode: " + std::to_string(static_cast<int>(CURLE_OPERATION_TIMEDOUT)) + ",";
    return response.HasClientError() && as_string_view(response.GetClientErrorMessage()).starts_with(kTimeoutPrefix);
}

// Moves the request body back to where the aborted send started reading it.
bool rewind_body(const Aws::Http::HttpRequest& request, std::streampos start) {
    const std::shared_ptr<Aws::IOStream>& body = request.GetContentBody();
    if (body == nullptr) {
        return true;
    }
    body->clear();
    body->seekg(start);
    return !body->fail();
}

int64_t elapsed_ms(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
}

} // namespace

const char* fast_retry_request_class_name(FastRetryRequestClass request_class) {
    switch (request_class) {
    case FastRetryRequestClass::PUT_OBJECT:
        return "put_object";
    case FastRetryRequestClass::HEAD_OBJECT:
        return "head_object";
    case FastRetryRequestClass::GET_OBJECT:
        return "get_object";
    }
    return "unknown";
}

std::optional<FastRetryRequestClass> classify_fast_retry_request(const Aws::Http::HttpRequest& request) {
    const Aws::Http::HttpMethod method = request.GetMethod();
    if (method != Aws::Http::HttpMethod::HTTP_PUT && method != Aws::Http::HttpMethod::HTTP_HEAD &&
        method != Aws::Http::HttpMethod::HTTP_GET) {
        return std::nullopt;
    }
    if (header_value(request, Aws::Http::SDK_REQUEST_HEADER) != kSdkFirstAttempt) {
        return std::nullopt;
    }
    // A query string marks multipart uploads, sub-resources such as ?tagging and access to object versions. A path
    // of "/" has no object key.
    if (!request.GetQueryString().empty() || request.GetUri().GetPath() == "/") {
        return std::nullopt;
    }
    if (method == Aws::Http::HttpMethod::HTTP_HEAD) {
        return FastRetryRequestClass::HEAD_OBJECT;
    }
    if (method == Aws::Http::HttpMethod::HTTP_GET) {
        return range_bytes(request).has_value() ? std::optional(FastRetryRequestClass::GET_OBJECT) : std::nullopt;
    }

    if (request.GetContentBody() == nullptr || content_length(request).value_or(0) <= 0) {
        return std::nullopt;
    }
    // A chunked or aws-chunked body is encoded while it is read, and its trailer depends on the bytes read so far:
    // sending the same request again would not reproduce it.
    if (request.HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER) ||
        header_value(request, Aws::Http::CONTENT_ENCODING_HEADER)
                        .value_or(std::string_view())
                        .find(Aws::Http::AWS_CHUNKED_VALUE) != std::string_view::npos) {
        return std::nullopt;
    }
    // A conditional write that the aborted send already carried out would fail when it is sent again.
    if (request.HasHeader(kIfMatchHeader) || request.HasHeader(kIfNoneMatchHeader)) {
        return std::nullopt;
    }
    return FastRetryRequestClass::PUT_OBJECT;
}

FastRetryPolicy fast_retry_policy(FastRetryRequestClass request_class) {
    FastRetryPolicy policy;
    switch (request_class) {
    case FastRetryRequestClass::PUT_OBJECT:
        policy.timeout_ms = config::object_storage_fast_retry_put_timeout_ms;
        policy.short_attempts = config::object_storage_fast_retry_put_short_attempts;
        policy.max_body_bytes = config::object_storage_fast_retry_put_max_bytes;
        break;
    case FastRetryRequestClass::HEAD_OBJECT:
        policy.timeout_ms = config::object_storage_fast_retry_head_timeout_ms;
        policy.short_attempts = config::object_storage_fast_retry_head_short_attempts;
        policy.max_body_bytes = 0;
        break;
    case FastRetryRequestClass::GET_OBJECT:
        policy.timeout_ms = config::object_storage_fast_retry_get_idle_ms;
        policy.short_attempts = config::object_storage_fast_retry_get_short_attempts;
        policy.max_body_bytes = config::object_storage_fast_retry_get_max_bytes;
        break;
    }
    policy.timeout_ms = std::max<int64_t>(policy.timeout_ms, 0);
    policy.short_attempts = std::clamp<int32_t>(policy.short_attempts, 1, kMaxFastRetryShortAttempts);
    policy.budget_percent = std::clamp<int32_t>(config::object_storage_fast_retry_budget_percent, 0, 100);
    return policy;
}

bool FastRetryBudget::try_acquire() {
    int64_t current = _milli_tokens.load(std::memory_order_relaxed);
    while (current >= kMilliTokensPerToken) {
        if (_milli_tokens.compare_exchange_weak(current, current - kMilliTokensPerToken, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void FastRetryBudget::add(int64_t milli_tokens) {
    int64_t current = _milli_tokens.load(std::memory_order_relaxed);
    while (milli_tokens > 0 && current < kMaxMilliTokens &&
           !_milli_tokens.compare_exchange_weak(current, std::min(current + milli_tokens, kMaxMilliTokens),
                                                std::memory_order_relaxed)) {
    }
}

ObjectStorageFastRetryMetrics::ObjectStorageFastRetryMetrics() {
    for (auto& counter : _counters) {
        counter = std::make_unique<IntCounter>(MetricUnit::REQUESTS);
    }
}

ObjectStorageFastRetryMetrics* ObjectStorageFastRetryMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers to MetricRegistry, so avoid
    // exit-time destruction after registry teardown.
    static auto* instance = new ObjectStorageFastRetryMetrics();
    return instance;
}

void ObjectStorageFastRetryMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;
    for (size_t c = 0; c < kNumFastRetryRequestClasses; ++c) {
        const auto request_class = static_cast<FastRetryRequestClass>(c);
        for (size_t r = 0; r < kNumFastRetryResults; ++r) {
            registry->register_metric("object_storage_fast_retry_total",
                                      MetricLabels()
                                              .add("request", fast_retry_request_class_name(request_class))
                                              .add("result", kResultNames[r]),
                                      counter(request_class, static_cast<FastRetryResult>(r)));
        }
    }
}

IntCounter* ObjectStorageFastRetryMetrics::counter(FastRetryRequestClass request_class, FastRetryResult result) const {
    return _counters[static_cast<size_t>(request_class) * kNumFastRetryResults + static_cast<size_t>(result)].get();
}

void ObjectStorageFastRetryMetrics::increment(FastRetryRequestClass request_class, FastRetryResult result) {
    counter(request_class, result)->increment(1);
}

int64_t ObjectStorageFastRetryMetrics::value(FastRetryRequestClass request_class, FastRetryResult result) const {
    return counter(request_class, result)->value();
}

void FastRetryCurlHttpClient::OverrideOptionsOnConnectionHandle(CURL* handle) const {
    if (IdleWatch* watch = std::exchange(tls_idle_watch, nullptr); watch != nullptr) {
        // CurlHttpClient installs a progress callback only for an event-stream request with a body, never for a
        // GET; it applies cancellation (ContinueRequest) and the read limiter in its write callback, which stays.
        watch->last_byte = std::chrono::steady_clock::now();
        curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(handle, CURLOPT_XFERINFOFUNCTION, &IdleWatch::progress);
        curl_easy_setopt(handle, CURLOPT_XFERINFODATA, watch);
    }
    const int64_t timeout_ms = std::exchange(tls_short_timeout_ms, 0);
    if (timeout_ms > 0) {
        // The pool resets a handle to the client's defaults when the send succeeds and destroys it when the send
        // fails, so the short timeout never reaches another request.
        curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout_ms));
    }
}

std::shared_ptr<Aws::Http::HttpResponse> FastRetryCurlHttpClient::MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
    // Start every request from a cleared value, so that a request without fast retry never reads one.
    tls_short_timeout_ms = 0;
    tls_idle_watch = nullptr;
    // Read for every request, so that turning the switch on or off at runtime also applies to the clients that
    // starlet has created already and keeps reusing.
    if (!config::enable_object_storage_fast_retry) {
        return CurlHttpClient::MakeRequest(request, readLimiter, writeLimiter);
    }
    const std::optional<FastRetryRequestClass> request_class = classify_fast_retry_request(*request);
    if (request_class.has_value()) {
        const FastRetryPolicy policy = fast_retry_policy(*request_class);
        const int64_t bytes = *request_class == FastRetryRequestClass::GET_OBJECT
                                      ? range_bytes(*request).value_or(std::numeric_limits<int64_t>::max())
                                      : content_length(*request).value_or(0);
        if (policy.enabled() && bytes <= policy.max_body_bytes) {
            return make_request_with_fast_retry(request, *request_class, policy, readLimiter, writeLimiter);
        }
    }
    return CurlHttpClient::MakeRequest(request, readLimiter, writeLimiter);
}

std::shared_ptr<Aws::Http::HttpResponse> FastRetryCurlHttpClient::make_request_with_fast_retry(
        const std::shared_ptr<Aws::Http::HttpRequest>& request, FastRetryRequestClass request_class,
        const FastRetryPolicy& policy, Aws::Utils::RateLimits::RateLimiterInterface* read_limiter,
        Aws::Utils::RateLimits::RateLimiterInterface* write_limiter) const {
    auto* metrics = ObjectStorageFastRetryMetrics::instance();
    FastRetryBudget& budget = _budgets[static_cast<size_t>(request_class)];

    // Where the body starts, so that it can be sent again after an aborted send.
    std::streampos body_start = 0;
    if (const std::shared_ptr<Aws::IOStream>& body = request->GetContentBody(); body != nullptr) {
        body_start = body->tellg();
        if (body_start == std::streampos(-1)) {
            return CurlHttpClient::MakeRequest(request, read_limiter, write_limiter);
        }
    }

    for (int32_t send_index = 0;; ++send_index) {
        bool short_timeout = send_index < policy.short_attempts;
        // A GET carries the limit as an idle watch, which takes its token only when it aborts the send. The other
        // classes carry a whole-request timeout, whose token is taken before the send, so that concurrent sends
        // cannot overdraw the budget.
        const bool watch_idle = short_timeout && request_class == FastRetryRequestClass::GET_OBJECT;
        if (short_timeout && !watch_idle && !budget.try_acquire()) {
            metrics->increment(request_class, FastRetryResult::NO_BUDGET);
            short_timeout = false;
        }
        ScopedToken token(short_timeout && !watch_idle ? &budget : nullptr);
        IdleWatch idle_watch{.idle_ms = policy.timeout_ms, .budget = &budget};
        const auto start = std::chrono::steady_clock::now();
        std::shared_ptr<Aws::Http::HttpResponse> response;
        {
            ScopedIdleWatch scoped_idle_watch(watch_idle ? &idle_watch : nullptr);
            ScopedShortTimeout scoped_short_timeout(short_timeout && !watch_idle ? policy.timeout_ms : 0);
            response = CurlHttpClient::MakeRequest(request, read_limiter, write_limiter);
        }
        const int64_t send_ms = elapsed_ms(start);
        if (idle_watch.no_budget && !idle_watch.expired) {
            metrics->increment(request_class, FastRetryResult::NO_BUDGET);
        }

        if (watch_idle ? idle_watch.expired : short_timeout && short_timeout_expired(*response)) {
            // The send timed out: it keeps its token.
            token.consume();
            metrics->increment(request_class, FastRetryResult::EXPIRED);
            VLOG(2) << "object storage fast retry: " << fast_retry_request_class_name(request_class) << " "
                    << request->GetUri().GetURIString(false) << " timed out after " << send_ms
                    << " ms with a short timeout of " << policy.timeout_ms
                    << " ms: " << response->GetClientErrorMessage();
            // Leave the request to the SDK's retry when the client is shutting down or the body cannot be rewound.
            if (!IsRequestProcessingEnabled() || !rewind_body(*request, body_start)) {
                return response;
            }
            // The SDK keeps the first value of each request metric: drop the aborted send's.
            request->SetRequestMetrics(Aws::Monitoring::HttpClientMetricsCollection());
            continue;
        }

        if (send_index == 0) {
            budget.on_completed(policy.budget_percent);
        } else if (response->HasClientError()) {
            metrics->increment(request_class, FastRetryResult::RESEND_ERROR);
        } else {
            metrics->increment(request_class, send_ms > policy.timeout_ms ? FastRetryResult::RESEND_SLOW
                                                                          : FastRetryResult::RESEND_FAST);
        }
        return response;
    }
}

std::shared_ptr<Aws::Http::HttpClient> FastRetryHttpClientFactory::CreateHttpClient(
        const Aws::Client::ClientConfiguration& client_config) const {
    if (fs::ScopedPlainAwsHttpClient::active()) {
        return std::make_shared<Aws::Http::CurlHttpClient>(client_config);
    }
    return std::make_shared<FastRetryCurlHttpClient>(client_config);
}

std::shared_ptr<Aws::Http::HttpRequest> FastRetryHttpClientFactory::CreateHttpRequest(
        const Aws::String& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory) const {
    return CreateHttpRequest(Aws::Http::URI(uri), method, stream_factory);
}

std::shared_ptr<Aws::Http::HttpRequest> FastRetryHttpClientFactory::CreateHttpRequest(
        const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& stream_factory) const {
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(kAllocationTag, uri, method);
    request->SetResponseStreamFactory(stream_factory);
    return request;
}

} // namespace starrocks
