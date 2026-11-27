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

#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "base/metrics.h"

namespace starrocks {

// Fast retry for small object storage requests.
//
// Object storage now and then answers a small request one or two seconds late, while the same request sent
// again is answered within tens of milliseconds. Fast retry gives the SDK's first attempt of such a request a
// short whole-request timeout. When a send with the short timeout times out, it is aborted and the same signed
// request is sent again at once, without the SDK's retry back-off. At most FastRetryPolicy::short_attempts sends
// carry the short timeout, each with a token from the budget. Once they are used up or no token is left, the next
// send is an ordinary one.
//
// It has three parts, so that a new request class, another way to set the threshold or another HTTP stack only
// changes one of them:
// - classification: classify_fast_retry_request() tells the class of a request from its shape;
// - the policy table: fast_retry_policy() gives the short timeout, the number of sends that carry it and the
//   body size limit of a class, from BE configs;
// - the executor: FastRetryCurlHttpClient sends a request as the policy of its class says. Only this part
//   depends on the HTTP stack.
// The budget (FastRetryBudget) and the metrics (ObjectStorageFastRetryMetrics) are kept per class.

enum class FastRetryRequestClass : uint8_t {
    PUT_OBJECT = 0,
    HEAD_OBJECT = 1,
};
inline constexpr size_t kNumFastRetryRequestClasses = 2;

const char* fast_retry_request_class_name(FastRetryRequestClass request_class);

// The class of a request, or nullopt when fast retry does not apply to it. Only the SDK's first attempt of a
// request qualifies, so the SDK's own retries keep their timing.
// - PUT_OBJECT: a PUT to an object key with a non-empty body of known length and no query string, sent as is
//   (no transfer encoding, no aws-chunked content encoding) and not conditional (no If-Match or If-None-Match).
//   This leaves out multipart uploads, copies and writes of sub-resources such as tags.
// - HEAD_OBJECT: a HEAD on an object key with no query string.
std::optional<FastRetryRequestClass> classify_fast_retry_request(const Aws::Http::HttpRequest& request);

inline constexpr int32_t kMaxFastRetryShortAttempts = 3;

struct FastRetryPolicy {
    // Whole-request timeout of a send that carries the short timeout. 0 turns fast retry off.
    int64_t timeout_ms = 0;
    // Largest number of sends that carry the short timeout, 1 to kMaxFastRetryShortAttempts. A send carries it only
    // with a token from the budget. Once these sends are used up or no token is left, the next send is an ordinary
    // one.
    int32_t short_attempts = 1;
    // Largest request body the class covers.
    int64_t max_body_bytes = 0;
    // Upper bound of the extra requests, in percent of the requests of the class. 0 turns fast retry off.
    int32_t budget_percent = 0;

    bool enabled() const { return timeout_ms > 0 && budget_percent > 0; }
};

// The current policy of a class, read from the mutable BE configs.
FastRetryPolicy fast_retry_policy(FastRetryRequestClass request_class);

// Bounds the extra requests that fast retry sends for one class on one client. A send takes a whole token before
// it carries the short timeout, keeps it when the send times out and gives it back otherwise; a send that finds no
// whole token goes without the short timeout. Every request of the class that completes without such a timeout
// adds budget_percent / 100 of a token. A client starts with kMaxTokens, which is also the cap. Since a token is
// taken before the send, concurrent sends cannot overdraw it: the extra requests stay within budget_percent of the
// requests plus kMaxTokens, and fast retry stops by itself when the object storage is slow as a whole. How many
// sends carry the short timeout at the same time depends on the tokens left when they start.
class FastRetryBudget {
public:
    static constexpr int64_t kMaxTokens = 100;

    // Takes a whole token for a send that carries the short timeout. Returns false, and takes nothing, when less than
    // a whole token is left.
    bool try_acquire();
    // Gives back the token of a send that did not time out.
    void release() { add(kMilliTokensPerToken); }
    void on_completed(int32_t budget_percent) { add(static_cast<int64_t>(budget_percent) * kMilliTokensPerPercent); }

    int64_t tokens() const { return _milli_tokens.load(std::memory_order_relaxed) / kMilliTokensPerToken; }

private:
    static constexpr int64_t kMilliTokensPerToken = 1000;
    static constexpr int64_t kMilliTokensPerPercent = kMilliTokensPerToken / 100;
    static constexpr int64_t kMaxMilliTokens = kMaxTokens * kMilliTokensPerToken;

    // Adds up to the cap.
    void add(int64_t milli_tokens);

    std::atomic<int64_t> _milli_tokens{kMaxMilliTokens};
};

enum class FastRetryResult : uint8_t {
    // A send that carried the short timeout timed out and was aborted.
    EXPIRED = 0,
    // A resend got a response within the short timeout.
    RESEND_FAST = 1,
    // A resend got a response, but took longer than the short timeout.
    RESEND_SLOW = 2,
    // A resend ended with a network error. The SDK retries the request as it does without fast retry.
    RESEND_ERROR = 3,
    // A send went without the short timeout because the budget had run out.
    NO_BUDGET = 4,
};
inline constexpr size_t kNumFastRetryResults = 5;

// object_storage_fast_retry_total{request="put_object|head_object",
//                                 result="expired|resend_fast|resend_slow|resend_error|no_budget"}
class ObjectStorageFastRetryMetrics {
public:
    ObjectStorageFastRetryMetrics();

    static ObjectStorageFastRetryMetrics* instance();

    void install(MetricRegistry* registry);
    void increment(FastRetryRequestClass request_class, FastRetryResult result);
    int64_t value(FastRetryRequestClass request_class, FastRetryResult result) const;

private:
    IntCounter* counter(FastRetryRequestClass request_class, FastRetryResult result) const;

    std::array<std::unique_ptr<IntCounter>, kNumFastRetryRequestClasses * kNumFastRetryResults> _counters;
    MetricRegistry* _registry = nullptr;
};

// The SDK's curl HTTP client with fast retry. Requests that fast retry does not apply to, and every request while
// enable_object_storage_fast_retry is false, are sent unchanged.
// A resend takes a new connection: after an error the SDK replaces the handle of the aborted send with a new one
// on top of its pool, and the resend gets that handle.
class FastRetryCurlHttpClient : public Aws::Http::CurlHttpClient {
public:
    explicit FastRetryCurlHttpClient(const Aws::Client::ClientConfiguration& client_config)
            : Aws::Http::CurlHttpClient(client_config) {}

    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
            const std::shared_ptr<Aws::Http::HttpRequest>& request,
            Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
            Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const override;

protected:
    void OverrideOptionsOnConnectionHandle(CURL* handle) const override;

private:
    std::shared_ptr<Aws::Http::HttpResponse> make_request_with_fast_retry(
            const std::shared_ptr<Aws::Http::HttpRequest>& request, FastRetryRequestClass request_class,
            const FastRetryPolicy& policy, Aws::Utils::RateLimits::RateLimiterInterface* read_limiter,
            Aws::Utils::RateLimits::RateLimiterInterface* write_limiter) const;

    mutable std::array<FastRetryBudget, kNumFastRetryRequestClasses> _budgets;
};

// Gives AWS SDK clients a FastRetryCurlHttpClient, except the clients created in a fs::ScopedPlainAwsHttpClient,
// which get the SDK's own curl client. It leaves the global state of libcurl alone, like the SDK's default
// factory with initAndCleanupCurl = false: the BE process sets libcurl up itself.
class FastRetryHttpClientFactory : public Aws::Http::HttpClientFactory {
public:
    std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(
            const Aws::Client::ClientConfiguration& client_config) const override;
    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
            const Aws::String& uri, Aws::Http::HttpMethod method,
            const Aws::IOStreamFactory& stream_factory) const override;
    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
            const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
            const Aws::IOStreamFactory& stream_factory) const override;
};

} // namespace starrocks
