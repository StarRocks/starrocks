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

#include "platform/llm/ai_provider.h"

#include <algorithm>
#include <cctype>
#include <charconv>

#include "gen_cpp/PlanNodes_types.h"
#include "platform/llm/dashscope_multimodal_provider.h"
#include "platform/llm/openai_compatible_provider.h"

namespace starrocks {
namespace {

Status invalid_provider() {
    return Status::InvalidArgument("Invalid AI project provider configuration");
}

bool contains_control_character(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return c <= 0x1f || c == 0x7f; });
}

bool is_non_blank(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c) == 0; });
}

StatusOr<std::optional<int32_t>> parse_positive_int(std::string_view value) {
    if (value.empty()) return std::optional<int32_t>{};
    // Match FE Integer.parseInt, including its optional leading plus sign.
    if (value.front() == '+') value.remove_prefix(1);
    if (value.empty()) return invalid_provider();
    int32_t parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed <= 0) {
        return invalid_provider();
    }
    return std::optional<int32_t>{parsed};
}

} // namespace

StatusOr<AIProviderConfig> AIProviderConfig::parse(std::string_view id, const TAIProvider& provider) {
    if (!provider.__isset.id || provider.id != id || !provider.__isset.type || !provider.__isset.protocol ||
        !provider.__isset.params) {
        return invalid_provider();
    }
    AIProviderConfig config;
    switch (provider.type) {
    case TAIProviderType::CHAT:
        config.capability = AICapability::CHAT;
        break;
    case TAIProviderType::EMBEDDING:
        config.capability = AICapability::EMBEDDING;
        break;
    default:
        return invalid_provider();
    }
    const auto get = [&](const char* key) -> std::string {
        auto it = provider.params.find(key);
        return it == provider.params.end() ? std::string{} : it->second;
    };
    config.endpoint = get("endpoint");
    config.model = get("model");
    config.api_key = get("api_key");
    if (provider.protocol != TAIProviderProtocol::OPENAI) {
        return Status::NotSupported("AI provider protocol is not supported");
    }
    config.protocol = AIProviderProtocol::OPENAI;
    RETURN_IF_ERROR(validate_ai_http_url(config.endpoint));
    if (!is_non_blank(config.model) || contains_control_character(config.model) ||
        contains_control_character(config.api_key)) {
        return invalid_provider();
    }
    ASSIGN_OR_RETURN(auto timeout, parse_positive_int(get("timeout_ms")));
    if (timeout.has_value()) config.timeout_ms = *timeout;
    if (config.capability == AICapability::EMBEDDING) {
        ASSIGN_OR_RETURN(config.dimensions, parse_positive_int(get("dimensions")));
    }
    return config;
}

Status validate_ai_protocol(AIProviderProtocol protocol, AICapability capability) {
    if (protocol == AIProviderProtocol::OPENAI &&
        (capability == AICapability::CHAT || capability == AICapability::EMBEDDING)) {
        return Status::OK();
    }
    if (protocol == AIProviderProtocol::DASHSCOPE_MULTIMODAL && capability == AICapability::EMBEDDING) {
        return Status::OK();
    }
    return Status::NotSupported("AI protocol does not support the requested capability");
}

StatusOr<std::unique_ptr<AIProvider>> create_ai_provider(const AIProviderConfig& config) {
    RETURN_IF_ERROR(validate_ai_protocol(config.protocol, config.capability));
    if (config.protocol == AIProviderProtocol::DASHSCOPE_MULTIMODAL) {
        return std::unique_ptr<AIProvider>(std::make_unique<DashScopeMultimodalProvider>());
    }
    return std::unique_ptr<AIProvider>(std::make_unique<OpenAICompatibleProvider>());
}

std::optional<AIProviderErrorCode> parse_ai_provider_error_code(std::string_view identifier) {
    if (identifier.empty() || identifier.size() > 128) return std::nullopt;
    std::string value;
    value.reserve(identifier.size());
    for (unsigned char byte : identifier) {
        const bool safe = (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z') ||
                          (byte >= '0' && byte <= '9') || byte == '.' || byte == '_' || byte == '-';
        if (!safe) return std::nullopt;
        value.push_back(byte >= 'A' && byte <= 'Z' ? static_cast<char>(byte - 'A' + 'a') : static_cast<char>(byte));
    }
    if (value == "rate_limit_exceeded") return AIProviderErrorCode::RATE_LIMIT_EXCEEDED;
    if (value == "too_many_requests") return AIProviderErrorCode::TOO_MANY_REQUESTS;
    if (value == "server_error") return AIProviderErrorCode::SERVER_ERROR;
    if (value == "internal_error") return AIProviderErrorCode::INTERNAL_ERROR;
    if (value == "service_unavailable") return AIProviderErrorCode::SERVICE_UNAVAILABLE;
    if (value == "timeout") return AIProviderErrorCode::TIMEOUT;
    if (value == "api_connection_error") return AIProviderErrorCode::API_CONNECTION_ERROR;
    if (value.starts_with("throttling.")) return AIProviderErrorCode::THROTTLING;
    if (value.starts_with("ratelimit.")) return AIProviderErrorCode::RATE_LIMIT;
    if (value.starts_with("internalerror.")) return AIProviderErrorCode::INTERNAL_ERROR;
    if (value.starts_with("serviceunavailable.")) return AIProviderErrorCode::SERVICE_UNAVAILABLE;
    return AIProviderErrorCode::UNKNOWN;
}

AIProviderErrorAction ai_provider_error_action(AIProviderErrorCode code) {
    switch (code) {
    case AIProviderErrorCode::RATE_LIMIT_EXCEEDED:
    case AIProviderErrorCode::TOO_MANY_REQUESTS:
    case AIProviderErrorCode::THROTTLING:
    case AIProviderErrorCode::RATE_LIMIT:
        return AIProviderErrorAction::THROTTLED;
    case AIProviderErrorCode::SERVER_ERROR:
    case AIProviderErrorCode::INTERNAL_ERROR:
    case AIProviderErrorCode::SERVICE_UNAVAILABLE:
    case AIProviderErrorCode::TIMEOUT:
    case AIProviderErrorCode::API_CONNECTION_ERROR:
        return AIProviderErrorAction::RETRYABLE;
    case AIProviderErrorCode::UNKNOWN:
        return AIProviderErrorAction::TERMINAL;
    }
    return AIProviderErrorAction::TERMINAL;
}

} // namespace starrocks
