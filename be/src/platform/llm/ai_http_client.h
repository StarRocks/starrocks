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
#include <variant>
#include <vector>

#include "base/status.h"
#include "base/statusor.h"
#include "platform/http/resolved_http_endpoint.h"
#include "platform/llm/ai_lifecycle.h"
#include "platform/llm/ai_memory.h"

namespace starrocks {

struct AIHttpHeader {
    std::string name;
    std::string value;
};

// One immutable HTTP attempt after ownership is transferred to AIHttpClient::submit().
struct AIHttpRequest {
    AIHttpRequest() = default;
    AIHttpRequest(const AIHttpRequest&) = delete;
    AIHttpRequest& operator=(const AIHttpRequest&) = delete;
    AIHttpRequest(AIHttpRequest&&) noexcept = default;
    AIHttpRequest& operator=(AIHttpRequest&&) noexcept = default;

    std::string url;
    std::vector<AIHttpHeader> headers;
    std::string body;
    // Absolute CLOCK_MONOTONIC deadline for the immutable logical-request
    // budget. Zero disables this budget; the live Query deadline remains
    // mandatory through |lifecycle|.
    int64_t request_deadline_ns = 0;
    int64_t connect_timeout_ms = 0;
    size_t max_response_bytes = 0;
    // Optional immutable DNS snapshot. Credential-bearing production callers
    // set this after validating their BE-local endpoint binding.
    std::shared_ptr<const ResolvedHttpEndpoint> resolved_endpoint;
    AIQueryLifecycleProbe lifecycle;
    AIMemoryContext memory;
};

class AIHttpClientImpl;

class AIHttpResponseBody {
public:
    AIHttpResponseBody() noexcept = default;
    ~AIHttpResponseBody() noexcept;

    AIHttpResponseBody(const AIHttpResponseBody&) = delete;
    AIHttpResponseBody& operator=(const AIHttpResponseBody&) = delete;
    AIHttpResponseBody(AIHttpResponseBody&& other) noexcept;
    AIHttpResponseBody& operator=(AIHttpResponseBody&& other) noexcept;

    // Response memory stays reserved across moves. The final owner frees the body buffer before releasing its memory
    // accounting, including when move assignment replaces an existing body.
    const std::string& data() const { return _data; }
    size_t size() const { return _data.size(); }

private:
    friend class AIHttpClientImpl;
    friend class AIHttpResponseBodyTestPeer;

    AIHttpResponseBody(std::string data, AIMemoryContext memory, size_t reserved_bytes) noexcept;
    void _release() noexcept;

    std::string _data;
    AIMemoryContext _memory;
    size_t _reserved_bytes = 0;
};

struct AIHttpResponse {
    AIHttpResponse() noexcept = default;
    ~AIHttpResponse() noexcept;

    AIHttpResponse(const AIHttpResponse&) = delete;
    AIHttpResponse& operator=(const AIHttpResponse&) = delete;
    AIHttpResponse(AIHttpResponse&& other) noexcept;
    AIHttpResponse& operator=(AIHttpResponse&& other) noexcept;

    int64_t status_code = 0;
    AIHttpResponseBody body;
    // Exposes only ASCII delay-seconds or strict IMF-fixdate. Obsolete HTTP-date forms are intentionally discarded.
    std::optional<std::string> retry_after;

private:
    friend class AIHttpClientImpl;

    AIHttpResponse(int64_t status_code, AIHttpResponseBody body, std::optional<std::string> retry_after,
                   AIMemoryContext memory) noexcept;
    void _release_retry_after() noexcept;

    AIMemoryContext _memory;
};

enum class AIHttpNoResponseCode : uint8_t {
    DNS,
    CONNECT,
    TIMEOUT,
    SEND,
    RECEIVE,
    EMPTY_REPLY,
    PARTIAL_TRANSFER,
    HTTP2_STREAM_RESET,
    TLS_HANDSHAKE,
    TLS_VERIFICATION,
    CANCELLATION,
    DEADLINE,
    RESPONSE_CAP,
    MEMORY_LIMIT,
    SHUTDOWN,
    UNKNOWN,
};

struct AIHttpNoResponse {
    AIHttpNoResponseCode code = AIHttpNoResponseCode::UNKNOWN;
};

using AIHttpResult = std::variant<AIHttpResponse, AIHttpNoResponse>;
using AIHttpCallback = std::function<void(AIHttpResult)>;

struct AIHttpClientOptions {
    // Test-only injection point. Production leaves this empty and uses libcurl's trust store.
    std::string ca_bundle_path;
};

// Applies the transport's complete URL policy and returns only fixed,
// non-sensitive errors. Callers may validate immutable endpoint configuration
// before constructing request state without copying a weaker parser.
Status validate_ai_http_url(const std::string& url);

// Applies the same complete URL policy while requiring HTTPS. Credential-bearing
// callers use this boundary so secrets can never be attached to cleartext HTTP.
Status validate_ai_https_url(const std::string& url);

class AIHttpClient {
public:
    virtual ~AIHttpClient() = default;

    static StatusOr<std::unique_ptr<AIHttpClient>> create(AIHttpClientOptions options = {});

    // A non-OK return transfers no completion ownership and callback will not run. After an OK return, callback runs
    // exactly once with either a complete HTTP response or an explicit no-response outcome. Callback runs on the
    // native I/O thread and must only hand work off; it must not block or destroy this client.
    virtual Status submit(AIHttpRequest request, AIHttpCallback callback) = 0;

    // Idempotently reject new work and complete accepted work as SHUTDOWN. External callers join the native I/O
    // thread; a reentrant call on that thread only requests shutdown and returns, leaving a later external call to join.
    virtual void shutdown() = 0;
};

} // namespace starrocks
