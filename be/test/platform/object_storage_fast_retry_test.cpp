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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <curl/curl.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <streambuf>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/delaying_http_server.h"
#include "common/config_object_storage_fwd.h"
#include "fs/plain_aws_http_client_scope.h"
#include "platform/aws/aws_sdk_guard.h"

namespace starrocks {

namespace {

constexpr char kTag[] = "ObjectStorageFastRetryTest";

using testutil::DelayingHttpServer;
using testutil::ReceivedRequest;

int64_t ms_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
}

std::string class_of(const Aws::Http::HttpRequest& request) {
    std::optional<FastRetryRequestClass> request_class = classify_fast_retry_request(request);
    return request_class.has_value() ? fast_retry_request_class_name(*request_class) : "none";
}

// Counts the results of one request class from the moment it is created.
class ResultCounter {
public:
    explicit ResultCounter(FastRetryRequestClass request_class) : _request_class(request_class) {
        for (size_t r = 0; r < kNumFastRetryResults; ++r) {
            _start[r] =
                    ObjectStorageFastRetryMetrics::instance()->value(request_class, static_cast<FastRetryResult>(r));
        }
    }

    int64_t operator()(FastRetryResult result) const {
        return ObjectStorageFastRetryMetrics::instance()->value(_request_class, result) -
               _start[static_cast<size_t>(result)];
    }

private:
    const FastRetryRequestClass _request_class;
    std::array<int64_t, kNumFastRetryResults> _start{};
};

// A request body that can be read once but not repositioned: tellg() returns -1.
class NonSeekableBuffer : public std::streambuf {
public:
    explicit NonSeekableBuffer(std::string data) : _data(std::move(data)) {
        setg(_data.data(), _data.data(), _data.data() + _data.size());
    }

private:
    std::string _data;
};

class NonSeekableStream : public Aws::IOStream {
public:
    explicit NonSeekableStream(std::string data) : Aws::IOStream(nullptr), _buffer(std::move(data)) { rdbuf(&_buffer); }

private:
    NonSeekableBuffer _buffer;
};

std::string large_payload(size_t size) {
    std::string payload(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>('a' + (i * 7 + i / 4096) % 26);
    }
    return payload;
}

} // namespace

class ObjectStorageFastRetryTest : public testing::Test {
public:
    // Like the BE process, libcurl is set up outside the SDK, so that replacing the SDK's HTTP client factory
    // leaves it alone.
    static void SetUpTestCase() {
        ASSERT_EQ(CURLE_OK, curl_global_init(CURL_GLOBAL_ALL));
        Aws::InitAPI(sdk_options());
    }

    static void TearDownTestCase() {
        Aws::ShutdownAPI(sdk_options());
        curl_global_cleanup();
    }

protected:
    void SetUp() override {
        _saved_enable = config::enable_object_storage_fast_retry;
        _saved_put_timeout_ms = config::object_storage_fast_retry_put_timeout_ms;
        _saved_put_short_attempts = config::object_storage_fast_retry_put_short_attempts;
        _saved_put_max_bytes = config::object_storage_fast_retry_put_max_bytes;
        _saved_head_timeout_ms = config::object_storage_fast_retry_head_timeout_ms;
        _saved_head_short_attempts = config::object_storage_fast_retry_head_short_attempts;
        _saved_budget_percent = config::object_storage_fast_retry_budget_percent;

        config::enable_object_storage_fast_retry = true;
        config::object_storage_fast_retry_put_timeout_ms = 300;
        config::object_storage_fast_retry_put_short_attempts = 1;
        config::object_storage_fast_retry_put_max_bytes = 1048576;
        config::object_storage_fast_retry_head_timeout_ms = 300;
        config::object_storage_fast_retry_head_short_attempts = 1;
        config::object_storage_fast_retry_budget_percent = 2;
    }

    void TearDown() override {
        config::enable_object_storage_fast_retry = _saved_enable;
        config::object_storage_fast_retry_put_timeout_ms = _saved_put_timeout_ms;
        config::object_storage_fast_retry_put_short_attempts = _saved_put_short_attempts;
        config::object_storage_fast_retry_put_max_bytes = _saved_put_max_bytes;
        config::object_storage_fast_retry_head_timeout_ms = _saved_head_timeout_ms;
        config::object_storage_fast_retry_head_short_attempts = _saved_head_short_attempts;
        config::object_storage_fast_retry_budget_percent = _saved_budget_percent;
    }

    static Aws::SDKOptions sdk_options() {
        Aws::SDKOptions options;
        options.httpOptions.initAndCleanupCurl = false;
        return options;
    }

    static Aws::Client::ClientConfiguration client_config() {
        Aws::Client::ClientConfiguration config("default", /*shouldDisableIMDS=*/true);
        config.scheme = Aws::Http::Scheme::HTTP;
        config.connectTimeoutMs = 1000;
        config.requestTimeoutMs = 10000;
        config.maxConnections = 8;
        return config;
    }

    // A request shaped like the SDK's first attempt, with a fixed signature.
    static std::shared_ptr<Aws::Http::HttpRequest> make_request(const std::string& url, Aws::Http::HttpMethod method) {
        auto request =
                Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(kTag, Aws::Http::URI(url.c_str()), method);
        request->SetResponseStreamFactory([] { return Aws::New<Aws::StringStream>(kTag); });
        request->SetHeaderValue(Aws::Http::SDK_REQUEST_HEADER, "attempt=1");
        request->SetHeaderValue(Aws::Http::AUTHORIZATION_HEADER,
                                "AWS4-HMAC-SHA256 Credential=test/20261117/us-east-1/s3/aws4_request, Signature=abc");
        request->SetHeaderValue(Aws::Http::AWS_DATE_HEADER, "20261117T000000Z");
        return request;
    }

    static std::shared_ptr<Aws::Http::HttpRequest> put_request(const std::string& url, const std::string& payload) {
        auto request = make_request(url, Aws::Http::HttpMethod::HTTP_PUT);
        auto body = Aws::MakeShared<Aws::StringStream>(kTag);
        *body << payload;
        request->AddContentBody(body);
        request->SetContentLength(std::to_string(payload.size()).c_str());
        request->SetContentType("application/octet-stream");
        return request;
    }

    static std::shared_ptr<Aws::Http::HttpRequest> head_request(const std::string& url) {
        return make_request(url, Aws::Http::HttpMethod::HTTP_HEAD);
    }

    static FastRetryBudget& budget_of(FastRetryCurlHttpClient& client, FastRetryRequestClass request_class) {
        return client._budgets[static_cast<size_t>(request_class)];
    }

private:
    bool _saved_enable = false;
    int64_t _saved_put_timeout_ms = 0;
    int32_t _saved_put_short_attempts = 0;
    int64_t _saved_put_max_bytes = 0;
    int64_t _saved_head_timeout_ms = 0;
    int32_t _saved_head_short_attempts = 0;
    int32_t _saved_budget_percent = 0;
};

TEST_F(ObjectStorageFastRetryTest, classify_requests_by_shape) {
    const std::string object = "http://127.0.0.1:1/bucket/dir/key";
    EXPECT_EQ("put_object", class_of(*put_request(object, "payload")));
    EXPECT_EQ("head_object", class_of(*head_request(object)));

    // The SDK's own retries keep their timing, and requests that the SDK did not send are left alone.
    auto retry = put_request(object, "payload");
    retry->SetHeaderValue(Aws::Http::SDK_REQUEST_HEADER, "attempt=2; max=3");
    EXPECT_EQ("none", class_of(*retry));
    auto unsigned_request = put_request(object, "payload");
    unsigned_request->DeleteHeader(Aws::Http::SDK_REQUEST_HEADER);
    EXPECT_EQ("none", class_of(*unsigned_request));

    // Multipart upload, sub-resources, object versions, and a path without an object key.
    EXPECT_EQ("none", class_of(*put_request(object + "?partNumber=1&uploadId=abc", "payload")));
    EXPECT_EQ("none", class_of(*put_request(object + "?tagging", "payload")));
    EXPECT_EQ("none", class_of(*head_request(object + "?versionId=v1")));
    EXPECT_EQ("none", class_of(*head_request("http://127.0.0.1:1/")));

    // Bodies that are encoded while they are sent, and conditional writes.
    auto chunked = put_request(object, "payload");
    chunked->SetHeaderValue(Aws::Http::TRANSFER_ENCODING_HEADER, "chunked");
    EXPECT_EQ("none", class_of(*chunked));
    auto aws_chunked = put_request(object, "payload");
    aws_chunked->SetHeaderValue(Aws::Http::CONTENT_ENCODING_HEADER, "aws-chunked");
    EXPECT_EQ("none", class_of(*aws_chunked));
    auto if_none_match = put_request(object, "payload");
    if_none_match->SetHeaderValue("If-None-Match", "*");
    EXPECT_EQ("none", class_of(*if_none_match));
    auto if_match = put_request(object, "payload");
    if_match->SetHeaderValue("If-Match", "\"etag\"");
    EXPECT_EQ("none", class_of(*if_match));

    // A PUT without a body, such as a copy.
    auto no_body = make_request(object, Aws::Http::HttpMethod::HTTP_PUT);
    no_body->SetContentLength("0");
    EXPECT_EQ("none", class_of(*no_body));
    EXPECT_EQ("none", class_of(*put_request(object, "")));

    EXPECT_EQ("none", class_of(*make_request(object, Aws::Http::HttpMethod::HTTP_GET)));
    EXPECT_EQ("none", class_of(*make_request(object, Aws::Http::HttpMethod::HTTP_DELETE)));
    EXPECT_EQ("none", class_of(*make_request(object, Aws::Http::HttpMethod::HTTP_POST)));
}

TEST_F(ObjectStorageFastRetryTest, policy_follows_configs) {
    config::object_storage_fast_retry_put_timeout_ms = 250;
    config::object_storage_fast_retry_put_short_attempts = 2;
    config::object_storage_fast_retry_put_max_bytes = 4096;
    config::object_storage_fast_retry_head_timeout_ms = 150;
    config::object_storage_fast_retry_head_short_attempts = 3;
    config::object_storage_fast_retry_budget_percent = 5;

    FastRetryPolicy put = fast_retry_policy(FastRetryRequestClass::PUT_OBJECT);
    EXPECT_EQ(250, put.timeout_ms);
    EXPECT_EQ(2, put.short_attempts);
    EXPECT_EQ(4096, put.max_body_bytes);
    EXPECT_EQ(5, put.budget_percent);
    EXPECT_TRUE(put.enabled());

    FastRetryPolicy head = fast_retry_policy(FastRetryRequestClass::HEAD_OBJECT);
    EXPECT_EQ(150, head.timeout_ms);
    EXPECT_EQ(3, head.short_attempts);
    EXPECT_EQ(0, head.max_body_bytes);
    EXPECT_TRUE(head.enabled());

    // Out-of-range values are clamped.
    config::object_storage_fast_retry_put_short_attempts = 0;
    config::object_storage_fast_retry_head_short_attempts = 10;
    config::object_storage_fast_retry_budget_percent = 1000;
    EXPECT_EQ(1, fast_retry_policy(FastRetryRequestClass::PUT_OBJECT).short_attempts);
    EXPECT_EQ(kMaxFastRetryShortAttempts, fast_retry_policy(FastRetryRequestClass::HEAD_OBJECT).short_attempts);
    EXPECT_EQ(100, fast_retry_policy(FastRetryRequestClass::PUT_OBJECT).budget_percent);

    // A zero or negative timeout or budget turns fast retry off.
    config::object_storage_fast_retry_put_timeout_ms = 0;
    config::object_storage_fast_retry_head_timeout_ms = -1;
    EXPECT_FALSE(fast_retry_policy(FastRetryRequestClass::PUT_OBJECT).enabled());
    EXPECT_FALSE(fast_retry_policy(FastRetryRequestClass::HEAD_OBJECT).enabled());
    config::object_storage_fast_retry_put_timeout_ms = 300;
    config::object_storage_fast_retry_budget_percent = 0;
    EXPECT_FALSE(fast_retry_policy(FastRetryRequestClass::PUT_OBJECT).enabled());
}

TEST_F(ObjectStorageFastRetryTest, budget_bounds_extra_requests) {
    FastRetryBudget budget;
    EXPECT_EQ(FastRetryBudget::kMaxTokens, budget.tokens());
    // Sends whose short timeout expires keep the token they took.
    for (int64_t i = 0; i < FastRetryBudget::kMaxTokens; ++i) {
        ASSERT_TRUE(budget.try_acquire());
    }
    EXPECT_FALSE(budget.try_acquire());
    EXPECT_EQ(0, budget.tokens());

    // A send whose short timeout did not expire gives its token back.
    budget.release();
    EXPECT_TRUE(budget.try_acquire());
    EXPECT_FALSE(budget.try_acquire());

    // With a budget of 2%, 50 requests that complete normally earn one token.
    for (int i = 0; i < 49; ++i) {
        budget.on_completed(2);
    }
    EXPECT_FALSE(budget.try_acquire());
    budget.on_completed(2);
    EXPECT_TRUE(budget.try_acquire());

    // The tokens are capped, also when sends give theirs back.
    for (int i = 0; i < 1000; ++i) {
        budget.on_completed(100);
        budget.release();
    }
    EXPECT_EQ(FastRetryBudget::kMaxTokens, budget.tokens());
}

TEST_F(ObjectStorageFastRetryTest, concurrent_sends_cannot_overdraw_the_budget) {
    // The first send of every key is slower than the short timeout, and its resend is answered at once.
    std::mutex seen_mutex;
    std::set<std::string> seen;
    DelayingHttpServer server([&](size_t, const ReceivedRequest& request) -> int64_t {
        std::lock_guard lock(seen_mutex);
        return seen.insert(request.target).second ? 800 : 0;
    });
    Aws::Client::ClientConfiguration config = client_config();
    config.maxConnections = 32;
    FastRetryCurlHttpClient client(config);
    // One token left for the requests below.
    FastRetryBudget& budget = budget_of(client, FastRetryRequestClass::PUT_OBJECT);
    while (budget.tokens() > 1) {
        ASSERT_TRUE(budget.try_acquire());
    }
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    constexpr int kThreads = 16;
    std::atomic<int> ready{0};
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1);
            while (ready.load() < kThreads) {
                std::this_thread::yield();
            }
            const std::string key = "/bucket/overdraw-" + std::to_string(i);
            auto response = client.MakeRequest(put_request(server.url(key), key));
            if (response->HasClientError() || response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Only the request that took the last token carried the short timeout and was resent; the others were sent
    // once and waited for the slow answer.
    EXPECT_EQ(0, failures.load());
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(kThreads - 1, results(FastRetryResult::NO_BUDGET));
    EXPECT_EQ(static_cast<size_t>(kThreads + 1), server.requests().size());
}

TEST_F(ObjectStorageFastRetryTest, metrics_are_registered_per_class_and_result) {
    MetricRegistry registry("object_storage_fast_retry_test");
    ObjectStorageFastRetryMetrics metrics;
    metrics.install(&registry);
    metrics.increment(FastRetryRequestClass::HEAD_OBJECT, FastRetryResult::RESEND_SLOW);

    auto* resend_slow = registry.get_metric("object_storage_fast_retry_total",
                                            MetricLabels().add("request", "head_object").add("result", "resend_slow"));
    ASSERT_NE(nullptr, resend_slow);
    EXPECT_EQ("1", resend_slow->to_string());
    auto* expired = registry.get_metric("object_storage_fast_retry_total",
                                        MetricLabels().add("request", "put_object").add("result", "expired"));
    ASSERT_NE(nullptr, expired);
    EXPECT_EQ("0", expired->to_string());
    EXPECT_NE(nullptr, registry.get_metric("object_storage_fast_retry_total",
                                           MetricLabels().add("request", "put_object").add("result", "no_budget")));
}

TEST_F(ObjectStorageFastRetryTest, resends_slow_put_at_once) {
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 5000 : 0; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(put_request(server.url("/bucket/put-key"), "payload"));
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_EQ(Aws::Http::HttpResponseCode::OK, response->GetResponseCode());
    // It waited for the short timeout, not for the slow answer.
    EXPECT_GE(elapsed_ms, 250);
    EXPECT_LT(elapsed_ms, 3000);

    // The resend is the same signed request with the same body.
    std::vector<ReceivedRequest> requests = server.requests();
    ASSERT_EQ(2u, requests.size());
    for (const ReceivedRequest& request : requests) {
        EXPECT_EQ("PUT", request.method);
        EXPECT_EQ("/bucket/put-key", request.target);
        EXPECT_EQ("payload", request.body);
    }
    for (const char* header : {"authorization", "x-amz-date", "amz-sdk-request", "content-length", "content-type"}) {
        EXPECT_FALSE(requests[0].header(header).empty()) << header;
        EXPECT_EQ(requests[0].header(header), requests[1].header(header)) << header;
    }

    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    // The server answers the resend at once, but on a loaded machine the resend can still take longer than the
    // short timeout, so only its outcome is checked.
    EXPECT_EQ(1, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
    EXPECT_EQ(0, results(FastRetryResult::RESEND_ERROR));
    EXPECT_EQ(0, results(FastRetryResult::NO_BUDGET));
}

TEST_F(ObjectStorageFastRetryTest, resend_sends_the_whole_body_again) {
    const std::string payload = large_payload(256 * 1024);
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 5000 : 0; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto response = client.MakeRequest(put_request(server.url("/bucket/large-key"), payload));

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    std::vector<ReceivedRequest> requests = server.requests();
    ASSERT_EQ(2u, requests.size());
    EXPECT_EQ(payload.size(), requests[1].body.size());
    EXPECT_TRUE(requests[0].body == payload);
    EXPECT_TRUE(requests[1].body == payload);
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
}

TEST_F(ObjectStorageFastRetryTest, last_send_has_no_short_timeout) {
    // Every send is slower than the short timeout: the resend waits for the answer.
    DelayingHttpServer server([](size_t, const ReceivedRequest&) { return 800; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(put_request(server.url("/bucket/slow-key"), "payload"));
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_EQ(Aws::Http::HttpResponseCode::OK, response->GetResponseCode());
    EXPECT_GE(elapsed_ms, 300 + 800 - 50);
    EXPECT_EQ(2u, server.requests().size());
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_SLOW));
    EXPECT_EQ(0, results(FastRetryResult::RESEND_FAST));
}

TEST_F(ObjectStorageFastRetryTest, short_attempts_are_configurable) {
    config::object_storage_fast_retry_put_short_attempts = 2;
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index < 2 ? 5000 : 0; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(put_request(server.url("/bucket/twice-key"), "payload"));
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_GE(elapsed_ms, 2 * 300 - 50);
    EXPECT_LT(elapsed_ms, 3000);
    EXPECT_EQ(3u, server.requests().size());
    EXPECT_EQ(2, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
}

TEST_F(ObjectStorageFastRetryTest, resends_slow_head_at_once) {
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 5000 : 0; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::HEAD_OBJECT);

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(head_request(server.url("/bucket/head-key")));
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_EQ(Aws::Http::HttpResponseCode::OK, response->GetResponseCode());
    EXPECT_LT(elapsed_ms, 3000);
    std::vector<ReceivedRequest> requests = server.requests();
    ASSERT_EQ(2u, requests.size());
    EXPECT_EQ("HEAD", requests[1].method);
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
}

TEST_F(ObjectStorageFastRetryTest, resend_error_goes_back_to_the_sdk) {
    // The connection of the resend is closed without an answer. The error goes back to the SDK, which retries the
    // request as it does without fast retry.
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) -> int64_t { return index == 0 ? 5000 : -1; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto response = client.MakeRequest(put_request(server.url("/bucket/error-key"), "payload"));

    EXPECT_TRUE(response->HasClientError());
    EXPECT_EQ(2u, server.requests().size());
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_ERROR));
}

TEST_F(ObjectStorageFastRetryTest, other_requests_are_sent_once) {
    config::object_storage_fast_retry_put_timeout_ms = 200;
    config::object_storage_fast_retry_put_max_bytes = 4;
    constexpr int64_t kDelayMs = 500;
    DelayingHttpServer server([](size_t, const ReceivedRequest&) { return kDelayMs; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto retry = put_request(server.url("/bucket/retry"), "abc");
    retry->SetHeaderValue(Aws::Http::SDK_REQUEST_HEADER, "attempt=2; max=3");
    std::vector<std::shared_ptr<Aws::Http::HttpRequest>> requests = {
            make_request(server.url("/bucket/get"), Aws::Http::HttpMethod::HTTP_GET),
            put_request(server.url("/bucket/part?partNumber=1&uploadId=abc"), "abc"),
            put_request(server.url("/bucket/large"), "longer than the limit"),
            retry,
    };
    for (size_t i = 0; i < requests.size(); ++i) {
        auto start = std::chrono::steady_clock::now();
        auto response = client.MakeRequest(requests[i]);
        int64_t elapsed_ms = ms_since(start);
        ASSERT_FALSE(response->HasClientError()) << i << ": " << response->GetClientErrorMessage();
        EXPECT_EQ(Aws::Http::HttpResponseCode::OK, response->GetResponseCode()) << i;
        EXPECT_GE(elapsed_ms, kDelayMs - 50) << i;
        EXPECT_EQ(i + 1, server.requests().size()) << i;
    }

    // A timeout of 0 turns fast retry off.
    config::object_storage_fast_retry_put_timeout_ms = 0;
    auto response = client.MakeRequest(put_request(server.url("/bucket/off"), "abc"));
    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_EQ(requests.size() + 1, server.requests().size());

    for (size_t r = 0; r < kNumFastRetryResults; ++r) {
        EXPECT_EQ(0, results(static_cast<FastRetryResult>(r))) << r;
    }
}

TEST_F(ObjectStorageFastRetryTest, switch_is_read_for_every_request) {
    // The first send of every key is slower than the short timeout, and its resend is answered at once.
    std::mutex seen_mutex;
    std::set<std::string> seen;
    DelayingHttpServer server([&](size_t, const ReceivedRequest& request) -> int64_t {
        std::lock_guard lock(seen_mutex);
        return seen.insert(request.target).second ? 800 : 0;
    });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    // Off: the request is sent once and waits for the slow answer.
    config::enable_object_storage_fast_retry = false;
    auto start = std::chrono::steady_clock::now();
    auto off = client.MakeRequest(put_request(server.url("/bucket/switch-off"), "payload"));
    int64_t off_ms = ms_since(start);
    ASSERT_FALSE(off->HasClientError()) << off->GetClientErrorMessage();
    EXPECT_GE(off_ms, 800 - 50);
    EXPECT_EQ(1u, server.requests().size());
    for (size_t r = 0; r < kNumFastRetryResults; ++r) {
        EXPECT_EQ(0, results(static_cast<FastRetryResult>(r))) << r;
    }

    // Turned on at runtime, the same client resends the next slow request.
    config::enable_object_storage_fast_retry = true;
    auto on = client.MakeRequest(put_request(server.url("/bucket/switch-on"), "payload"));
    ASSERT_FALSE(on->HasClientError()) << on->GetClientErrorMessage();
    EXPECT_EQ(3u, server.requests().size());
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
}

TEST_F(ObjectStorageFastRetryTest, body_that_cannot_be_rewound_is_sent_once) {
    DelayingHttpServer server([](size_t, const ReceivedRequest&) { return 800; });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto request = make_request(server.url("/bucket/stream-key"), Aws::Http::HttpMethod::HTTP_PUT);
    request->AddContentBody(Aws::MakeShared<NonSeekableStream>(kTag, "payload"));
    request->SetContentLength("7");
    ASSERT_EQ("put_object", class_of(*request));

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(request);
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_GE(elapsed_ms, 800 - 50);
    std::vector<ReceivedRequest> requests = server.requests();
    ASSERT_EQ(1u, requests.size());
    EXPECT_EQ("payload", requests[0].body);
    EXPECT_EQ(0, results(FastRetryResult::EXPIRED));
}

TEST_F(ObjectStorageFastRetryTest, no_short_timeout_without_budget) {
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 800 : 0; });
    FastRetryCurlHttpClient client(client_config());
    FastRetryBudget& budget = budget_of(client, FastRetryRequestClass::PUT_OBJECT);
    while (budget.try_acquire()) {
    }
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    auto start = std::chrono::steady_clock::now();
    auto response = client.MakeRequest(put_request(server.url("/bucket/no-budget"), "payload"));
    int64_t elapsed_ms = ms_since(start);

    ASSERT_FALSE(response->HasClientError()) << response->GetClientErrorMessage();
    EXPECT_GE(elapsed_ms, 800 - 50);
    EXPECT_EQ(1u, server.requests().size());
    EXPECT_EQ(1, results(FastRetryResult::NO_BUDGET));
    EXPECT_EQ(0, results(FastRetryResult::EXPIRED));
}

TEST_F(ObjectStorageFastRetryTest, short_timeout_does_not_reach_later_requests) {
    // A PUT that is answered at once, then a GET on the same client and thread that is slower than the short
    // timeout. With a single handle the GET reuses the PUT's handle, and must not inherit its short timeout.
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 0 : 800; });
    Aws::Client::ClientConfiguration config = client_config();
    config.maxConnections = 1;
    FastRetryCurlHttpClient client(config);

    auto put = client.MakeRequest(put_request(server.url("/bucket/fast-put"), "payload"));
    ASSERT_FALSE(put->HasClientError()) << put->GetClientErrorMessage();

    auto start = std::chrono::steady_clock::now();
    auto get = client.MakeRequest(make_request(server.url("/bucket/slow-get"), Aws::Http::HttpMethod::HTTP_GET));
    int64_t elapsed_ms = ms_since(start);
    ASSERT_FALSE(get->HasClientError()) << get->GetClientErrorMessage();
    EXPECT_EQ(Aws::Http::HttpResponseCode::OK, get->GetResponseCode());
    EXPECT_GE(elapsed_ms, 800 - 50);
    EXPECT_EQ(2u, server.requests().size());
}

TEST_F(ObjectStorageFastRetryTest, concurrent_requests_share_a_client) {
    // The first send of every key is slow and its resend is answered at once.
    std::mutex seen_mutex;
    std::set<std::string> seen;
    DelayingHttpServer server([&](size_t, const ReceivedRequest& request) -> int64_t {
        std::lock_guard lock(seen_mutex);
        return seen.insert(request.target).second ? 5000 : 0;
    });
    FastRetryCurlHttpClient client(client_config());
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    constexpr int kThreads = 8;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            const std::string key = "/bucket/concurrent-" + std::to_string(i);
            auto response = client.MakeRequest(put_request(server.url(key), key));
            if (response->HasClientError() || response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    int64_t elapsed_ms = ms_since(start);

    EXPECT_EQ(0, failures.load());
    EXPECT_LT(elapsed_ms, 4000);
    std::map<std::string, int> sends;
    for (const ReceivedRequest& request : server.requests()) {
        ++sends[request.target];
        EXPECT_EQ(request.target, request.body);
    }
    EXPECT_EQ(static_cast<size_t>(kThreads), sends.size());
    for (const auto& [target, count] : sends) {
        EXPECT_EQ(2, count) << target;
    }
    EXPECT_EQ(kThreads, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(kThreads, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
}

TEST_F(ObjectStorageFastRetryTest, factory_keeps_plain_client_in_scope) {
    FastRetryHttpClientFactory factory;
    const Aws::Client::ClientConfiguration config = client_config();
    EXPECT_NE(nullptr, dynamic_cast<FastRetryCurlHttpClient*>(factory.CreateHttpClient(config).get()));
    {
        fs::ScopedPlainAwsHttpClient plain;
        std::shared_ptr<Aws::Http::HttpClient> client = factory.CreateHttpClient(config);
        EXPECT_EQ(nullptr, dynamic_cast<FastRetryCurlHttpClient*>(client.get()));
        EXPECT_NE(nullptr, dynamic_cast<Aws::Http::CurlHttpClient*>(client.get()));
        {
            fs::ScopedPlainAwsHttpClient nested;
            EXPECT_TRUE(fs::ScopedPlainAwsHttpClient::active());
        }
        EXPECT_TRUE(fs::ScopedPlainAwsHttpClient::active());
    }
    EXPECT_FALSE(fs::ScopedPlainAwsHttpClient::active());

    auto request =
            factory.CreateHttpRequest(Aws::String("http://127.0.0.1:1/bucket/key"), Aws::Http::HttpMethod::HTTP_PUT,
                                      [] { return Aws::New<Aws::StringStream>(kTag); });
    EXPECT_EQ(Aws::Http::HttpMethod::HTTP_PUT, request->GetMethod());
    EXPECT_EQ("/bucket/key", request->GetUri().GetPath());
}

// The next two cases install the factory into the SDK, which the suite's ShutdownAPI resets. The other cases do
// not go through the SDK's factory.
TEST_F(ObjectStorageFastRetryTest, sdk_guard_installs_the_factory_with_the_curl_client) {
    const bool saved_poco = config::enable_poco_client_for_aws_sdk;
    // The factory is installed also while the switch is off, so that the switch can be turned on at runtime.
    config::enable_object_storage_fast_retry = false;
    config::enable_poco_client_for_aws_sdk = false;
    {
        // The SDK is initialized already, so the guard only installs the HTTP client factory, as the BE process
        // does after it sets up libcurl.
        AwsSdkGuard guard;
        EXPECT_NE(nullptr, dynamic_cast<FastRetryCurlHttpClient*>(Aws::Http::CreateHttpClient(client_config()).get()));
        fs::ScopedPlainAwsHttpClient plain;
        EXPECT_EQ(nullptr, dynamic_cast<FastRetryCurlHttpClient*>(Aws::Http::CreateHttpClient(client_config()).get()));
    }
    config::enable_poco_client_for_aws_sdk = saved_poco;
}

TEST_F(ObjectStorageFastRetryTest, s3_client_put_object_is_resent_on_the_first_attempt) {
    DelayingHttpServer server([](size_t index, const ReceivedRequest&) { return index == 0 ? 5000 : 0; });
    Aws::Http::SetHttpClientFactory(std::make_shared<FastRetryHttpClientFactory>());

    Aws::Client::ClientConfiguration config = client_config();
    config.endpointOverride = ("127.0.0.1:" + std::to_string(server.port())).c_str();
    config.region = "us-east-1";
    config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(/*maxRetries=*/3);
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("ak", "sk");
    auto s3 = std::make_shared<Aws::S3::S3Client>(credentials, config,
                                                  Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                                  /*useVirtualAddressing=*/false);
    ResultCounter results(FastRetryRequestClass::PUT_OBJECT);

    Aws::S3::Model::PutObjectRequest put;
    put.SetBucket("bucket");
    put.SetKey("s3-key");
    auto body = Aws::MakeShared<Aws::StringStream>(kTag);
    *body << "payload";
    put.SetBody(body);

    auto start = std::chrono::steady_clock::now();
    auto outcome = s3->PutObject(put);
    int64_t elapsed_ms = ms_since(start);

    ASSERT_TRUE(outcome.IsSuccess()) << outcome.GetError().GetMessage();
    // The resend happened inside the HTTP client: the SDK saw a single attempt and did not retry.
    EXPECT_EQ(0, outcome.GetRetryCount());
    EXPECT_LT(elapsed_ms, 3000);
    std::vector<ReceivedRequest> requests = server.requests();
    ASSERT_EQ(2u, requests.size());
    EXPECT_EQ("/bucket/s3-key", requests[0].target);
    EXPECT_EQ("attempt=1", requests[0].header("amz-sdk-request"));
    EXPECT_EQ(requests[0].header("authorization"), requests[1].header("authorization"));
    EXPECT_EQ("payload", requests[1].body);
    EXPECT_EQ(1, results(FastRetryResult::EXPIRED));
    EXPECT_EQ(1, results(FastRetryResult::RESEND_FAST) + results(FastRetryResult::RESEND_SLOW));
}

} // namespace starrocks
