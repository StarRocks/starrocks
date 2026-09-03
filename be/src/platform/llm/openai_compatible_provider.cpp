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

#include "platform/llm/openai_compatible_provider.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <optional>
#include <string>

namespace starrocks {
namespace {

constexpr std::string_view kSystemPrompt = "You are a helpful assistant.";
using RequestWriter = rapidjson::Writer<rapidjson::StringBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>,
                                        rapidjson::CrtAllocator, rapidjson::kWriteValidateEncodingFlag>;

Status invalid_request() {
    return Status::InvalidArgument("AI provider request is invalid");
}

rapidjson::Type json_type(AIProviderOptionKind kind) {
    switch (kind) {
    case AIProviderOptionKind::NULL_VALUE:
        return rapidjson::kNullType;
    case AIProviderOptionKind::FALSE_VALUE:
        return rapidjson::kFalseType;
    case AIProviderOptionKind::TRUE_VALUE:
        return rapidjson::kTrueType;
    case AIProviderOptionKind::OBJECT:
        return rapidjson::kObjectType;
    case AIProviderOptionKind::ARRAY:
        return rapidjson::kArrayType;
    case AIProviderOptionKind::STRING:
        return rapidjson::kStringType;
    case AIProviderOptionKind::NUMBER:
        return rapidjson::kNumberType;
    }
    return rapidjson::kNullType;
}

bool has_invalid_api_key_byte(std::string_view api_key) {
    if (api_key.empty()) {
        return true;
    }
    for (unsigned char byte : api_key) {
        if (byte <= 0x1f || byte == 0x7f) {
            return true;
        }
    }
    return false;
}

bool is_safe_identifier(std::string_view identifier) {
    if (identifier.empty() || identifier.size() > 128) {
        return false;
    }
    for (unsigned char byte : identifier) {
        const bool safe = (byte >= 'A' && byte <= 'Z') || (byte >= 'a' && byte <= 'z') ||
                          (byte >= '0' && byte <= '9') || byte == '.' || byte == '_' || byte == '-';
        if (!safe) {
            return false;
        }
    }
    return true;
}

std::string ascii_lower(std::string_view value) {
    std::string result;
    result.reserve(value.size());
    for (unsigned char byte : value) {
        result.push_back(byte >= 'A' && byte <= 'Z' ? static_cast<char>(byte - 'A' + 'a') : static_cast<char>(byte));
    }
    return result;
}

AIProviderErrorCode classify_identifier(std::string_view identifier) {
    const std::string value = ascii_lower(identifier);
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

std::optional<AIProviderErrorCode> classify_member(const rapidjson::Value& envelope, const char* name) {
    const auto member = envelope.FindMember(name);
    if (member == envelope.MemberEnd() || !member->value.IsString()) {
        return std::nullopt;
    }
    const std::string_view value(member->value.GetString(), member->value.GetStringLength());
    if (!is_safe_identifier(value)) {
        return std::nullopt;
    }
    return classify_identifier(value);
}

AIProviderErrorCode classify_error(const rapidjson::Value& envelope) {
    if (auto code = classify_member(envelope, "code"); code.has_value()) {
        return *code;
    }
    if (auto type = classify_member(envelope, "type"); type.has_value()) {
        return *type;
    }
    return AIProviderErrorCode::UNKNOWN;
}

} // namespace

StatusOr<AIProviderHttpRequest> OpenAICompatibleProvider::build_request(const AIChatRequest& request) const {
    if (request.model.empty()) {
        return Status::InvalidArgument("AI provider model is empty");
    }
    if (has_invalid_api_key_byte(request.api_key)) {
        return Status::InvalidArgument("AI provider API key is invalid");
    }

    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!writer.StartObject() || !writer.Key("model") || !writer.String(request.model.data(), request.model.size()) ||
        !writer.Key("messages") || !writer.StartArray() || !writer.StartObject() || !writer.Key("role") ||
        !writer.String("system") || !writer.Key("content") ||
        !writer.String(kSystemPrompt.data(), kSystemPrompt.size()) || !writer.EndObject() || !writer.StartObject() ||
        !writer.Key("role") || !writer.String("user") || !writer.Key("content") ||
        !writer.String(request.prompt.data(), request.prompt.size()) || !writer.EndObject() || !writer.EndArray() ||
        !writer.Key("stream") || !writer.Bool(false)) {
        return invalid_request();
    }

    if (request.options != nullptr) {
        for (const auto& option : request.options->members()) {
            if (option.key == "model" || option.key == "messages" || option.key == "stream" ||
                !writer.Key(option.key.data(), option.key.size()) ||
                !writer.RawValue(option.serialized_json.data(), option.serialized_json.size(),
                                 json_type(option.kind))) {
                return invalid_request();
            }
        }
    }
    if (!writer.EndObject() || !writer.IsComplete()) {
        return invalid_request();
    }

    AIProviderHttpRequest result;
    result.url.assign(request.endpoint.data(), request.endpoint.size());
    result.headers = {
            AIHttpHeader{.name = "Content-Type", .value = "application/json"},
            AIHttpHeader{.name = "Accept", .value = "application/json"},
            AIHttpHeader{.name = "Authorization", .value = "Bearer " + std::string(request.api_key)},
    };
    result.body.assign(buffer.GetString(), buffer.GetSize());
    return result;
}

AIProviderParseResult OpenAICompatibleProvider::parse_response(std::string_view body) const {
    if (body.find('\0') != std::string_view::npos) {
        return AIProviderMalformed{};
    }

    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(body.data(),
                                                                                               body.size());
    if (document.HasParseError() || !document.IsObject()) {
        return AIProviderMalformed{};
    }

    const auto nested_error = document.FindMember("error");
    if (nested_error != document.MemberEnd() && nested_error->value.IsObject()) {
        return AIProviderStructuredError{.code = classify_error(nested_error->value)};
    }
    if (document.HasMember("code") || document.HasMember("type")) {
        return AIProviderStructuredError{.code = classify_error(document)};
    }

    const auto choices = document.FindMember("choices");
    if (choices == document.MemberEnd() || !choices->value.IsArray() || choices->value.Empty() ||
        !choices->value[0].IsObject()) {
        return AIProviderMalformed{};
    }
    const auto message = choices->value[0].FindMember("message");
    if (message == choices->value[0].MemberEnd() || !message->value.IsObject()) {
        return AIProviderMalformed{};
    }
    const auto content = message->value.FindMember("content");
    if (content == message->value.MemberEnd() || !content->value.IsString()) {
        return AIProviderMalformed{};
    }
    return AIProviderSuccess{.content = std::string(content->value.GetString(), content->value.GetStringLength())};
}

} // namespace starrocks
