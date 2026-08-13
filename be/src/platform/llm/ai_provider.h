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
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "base/statusor.h"
#include "platform/llm/ai_http_client.h"
#include "platform/llm/ai_provider_options.h"

namespace starrocks {

struct AIChatRequest {
    std::string_view endpoint;
    std::string_view model;
    std::string_view api_key;
    std::string_view prompt;
    const AIProviderOptions* options = nullptr;
};

struct AIProviderHttpRequest {
    std::string url;
    std::vector<AIHttpHeader> headers;
    std::string body;
};

enum class AIProviderErrorCode : uint8_t {
    RATE_LIMIT_EXCEEDED,
    TOO_MANY_REQUESTS,
    THROTTLING,
    RATE_LIMIT,
    SERVER_ERROR,
    INTERNAL_ERROR,
    SERVICE_UNAVAILABLE,
    TIMEOUT,
    API_CONNECTION_ERROR,
    UNKNOWN,
};

enum class AIProviderErrorAction : uint8_t {
    THROTTLED,
    RETRYABLE,
    TERMINAL,
};

AIProviderErrorAction ai_provider_error_action(AIProviderErrorCode code);

struct AIProviderSuccess {
    std::string content;
};

struct AIProviderStructuredError {
    AIProviderErrorCode code = AIProviderErrorCode::UNKNOWN;
};

struct AIProviderMalformed {};

using AIProviderParseResult = std::variant<AIProviderSuccess, AIProviderStructuredError, AIProviderMalformed>;

class AIProvider {
public:
    virtual ~AIProvider() = default;

    virtual StatusOr<AIProviderHttpRequest> build_request(const AIChatRequest& request) const = 0;
    virtual AIProviderParseResult parse_response(std::string_view body) const = 0;
};

} // namespace starrocks
