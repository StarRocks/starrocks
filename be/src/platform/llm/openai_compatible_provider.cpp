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

#include <cmath>
#include <limits>
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

AIProviderUsage parse_usage(const rapidjson::Value& document) {
    const auto usage = document.FindMember("usage");
    if (usage == document.MemberEnd() || !usage->value.IsObject()) return {};
    const auto token_count = [&](const char* name) -> std::optional<int64_t> {
        const auto member = usage->value.FindMember(name);
        if (member == usage->value.MemberEnd() || !member->value.IsInt64() || member->value.GetInt64() < 0) {
            return std::nullopt;
        }
        return member->value.GetInt64();
    };
    return {.prompt_tokens = token_count("prompt_tokens"),
            .completion_tokens = token_count("completion_tokens"),
            .total_tokens = token_count("total_tokens")};
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
    for (unsigned char byte : api_key) {
        if (byte <= 0x1f || byte == 0x7f) {
            return true;
        }
    }
    return false;
}

std::optional<AIProviderErrorCode> classify_member(const rapidjson::Value& envelope, const char* name) {
    const auto member = envelope.FindMember(name);
    if (member == envelope.MemberEnd() || !member->value.IsString()) {
        return std::nullopt;
    }
    const std::string_view value(member->value.GetString(), member->value.GetStringLength());
    return parse_ai_provider_error_code(value);
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

    std::string media_uri;
    const char* media_part_type = "image_url";
    if (request.media != nullptr) {
        if (request.capability != AICapability::CHAT) {
            return Status::NotSupported("OpenAI-compatible embedding does not support FILE input");
        }
        if (request.media->mime_type == "video/mp4") {
            if (!_supports_video) return Status::NotSupported("AI protocol does not support video input");
            media_part_type = "video_url";
        }
        auto encoded = encode_ai_media_data_uri(*request.media);
        if (!encoded.ok()) return encoded.status();
        media_uri = std::move(*encoded);
    }

    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!writer.StartObject() || !writer.Key("model") || !writer.String(request.model.data(), request.model.size())) {
        return invalid_request();
    }
    switch (request.capability) {
    case AICapability::CHAT:
        if (!writer.Key("messages") || !writer.StartArray() || !writer.StartObject() || !writer.Key("role") ||
            !writer.String("system") || !writer.Key("content") ||
            !writer.String(kSystemPrompt.data(), kSystemPrompt.size()) || !writer.EndObject() ||
            !writer.StartObject() || !writer.Key("role") || !writer.String("user") || !writer.Key("content")) {
            return invalid_request();
        }
        if (request.media == nullptr) {
            if (!writer.String(request.prompt.data(), request.prompt.size())) return invalid_request();
        } else if (!writer.StartArray() || !writer.StartObject() || !writer.Key("type") || !writer.String("text") ||
                   !writer.Key("text") || !writer.String(request.prompt.data(), request.prompt.size()) ||
                   !writer.EndObject() || !writer.StartObject() || !writer.Key("type") ||
                   !writer.String(media_part_type) || !writer.Key(media_part_type) || !writer.StartObject() ||
                   !writer.Key("url") || !writer.String(media_uri.data(), media_uri.size()) || !writer.EndObject() ||
                   !writer.EndObject() || !writer.EndArray()) {
            return invalid_request();
        }
        if (!writer.EndObject() || !writer.EndArray() || !writer.Key("stream") || !writer.Bool(false)) {
            return invalid_request();
        }
        break;
    case AICapability::TEXT_EMBEDDING:
        if (!writer.Key("input") || !writer.String(request.prompt.data(), request.prompt.size()) ||
            !writer.Key("encoding_format") || !writer.String("float")) {
            return invalid_request();
        }
        break;
    default:
        return invalid_request();
    }

    if (request.options != nullptr) {
        for (const auto& option : request.options->members()) {
            const bool reserved = option.key == "model" ||
                                  (request.capability == AICapability::CHAT &&
                                   (option.key == "messages" || option.key == "stream")) ||
                                  (request.capability == AICapability::TEXT_EMBEDDING &&
                                   (option.key == "input" || option.key == "encoding_format"));
            if (reserved || !writer.Key(option.key.data(), option.key.size()) ||
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
    };
    if (!request.api_key.empty()) {
        result.headers.emplace_back(
                AIHttpHeader{.name = "Authorization", .value = "Bearer " + std::string(request.api_key)});
    }
    result.body.assign(buffer.GetString(), buffer.GetSize());
    return result;
}

AIProviderParseResult OpenAICompatibleProvider::parse_response(std::string_view body, AICapability capability) const {
    if (body.find('\0') != std::string_view::npos) {
        return AIProviderMalformed{};
    }

    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(body.data(),
                                                                                               body.size());
    if (document.HasParseError() || !document.IsObject()) {
        return AIProviderMalformed{};
    }

    const AIProviderUsage usage = parse_usage(document);
    const auto nested_error = document.FindMember("error");
    if (nested_error != document.MemberEnd() && nested_error->value.IsObject()) {
        return AIProviderStructuredError{.code = classify_error(nested_error->value), .usage = usage};
    }
    if (document.HasMember("code") || document.HasMember("type")) {
        return AIProviderStructuredError{.code = classify_error(document), .usage = usage};
    }

    if (capability == AICapability::TEXT_EMBEDDING) {
        const auto data = document.FindMember("data");
        if (data == document.MemberEnd() || !data->value.IsArray() || data->value.Size() != 1 ||
            !data->value[0].IsObject()) {
            return AIProviderMalformed{.usage = usage};
        }
        const auto& item = data->value[0];
        const auto index = item.FindMember("index");
        const auto embedding = item.FindMember("embedding");
        if (index == item.MemberEnd() || !index->value.IsUint() || index->value.GetUint() != 0 ||
            embedding == item.MemberEnd() || !embedding->value.IsArray() || embedding->value.Empty()) {
            return AIProviderMalformed{.usage = usage};
        }
        std::vector<float> values;
        values.reserve(embedding->value.Size());
        for (const auto& element : embedding->value.GetArray()) {
            if (!element.IsNumber()) return AIProviderMalformed{.usage = usage};
            const double value = element.GetDouble();
            if (!std::isfinite(value) || value < std::numeric_limits<float>::lowest() ||
                value > std::numeric_limits<float>::max()) {
                return AIProviderMalformed{.usage = usage};
            }
            values.emplace_back(static_cast<float>(value));
        }
        return AIProviderSuccess{.value = std::move(values), .usage = usage};
    }
    if (capability != AICapability::CHAT) return AIProviderMalformed{.usage = usage};

    const auto choices = document.FindMember("choices");
    if (choices == document.MemberEnd() || !choices->value.IsArray() || choices->value.Empty() ||
        !choices->value[0].IsObject()) {
        return AIProviderMalformed{.usage = usage};
    }
    const auto message = choices->value[0].FindMember("message");
    if (message == choices->value[0].MemberEnd() || !message->value.IsObject()) {
        return AIProviderMalformed{.usage = usage};
    }
    const auto content = message->value.FindMember("content");
    if (content == message->value.MemberEnd() || !content->value.IsString()) {
        return AIProviderMalformed{.usage = usage};
    }
    return AIProviderSuccess{.value = std::string(content->value.GetString(), content->value.GetStringLength()),
                             .usage = usage};
}

} // namespace starrocks
