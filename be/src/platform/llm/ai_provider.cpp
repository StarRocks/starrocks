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

#include "platform/llm/dashscope_multimodal_provider.h"
#include "platform/llm/openai_compatible_provider.h"

namespace starrocks {

Status validate_ai_protocol(std::string_view protocol, AICapability capability) {
    if (protocol == "openai_compatible" &&
        (capability == AICapability::CHAT || capability == AICapability::TEXT_EMBEDDING)) {
        return Status::OK();
    }
    if (protocol == "qwen_compatible" && capability == AICapability::CHAT) {
        return Status::OK();
    }
    if (protocol == "dashscope_multimodal" && capability == AICapability::TEXT_EMBEDDING) {
        return Status::OK();
    }
    return Status::NotSupported("AI protocol does not support the requested capability");
}

StatusOr<std::unique_ptr<AIProvider>> create_ai_provider(std::string_view protocol, AICapability capability) {
    auto status = validate_ai_protocol(protocol, capability);
    if (!status.ok()) return status;
    if (protocol == "dashscope_multimodal") {
        return std::unique_ptr<AIProvider>(std::make_unique<DashScopeMultimodalProvider>());
    }
    return std::unique_ptr<AIProvider>(std::make_unique<OpenAICompatibleProvider>(protocol == "qwen_compatible"));
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
