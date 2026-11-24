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
#include <string_view>

namespace starrocks {
namespace {

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

Status write_chat_messages(const AIChatRequest& request, RequestWriter& writer) {
    std::string encoded_media;
    rapidjson::Document messages(rapidjson::kArrayType);
    auto& allocator = messages.GetAllocator();
    rapidjson::Value system(rapidjson::kObjectType);
    system.AddMember("role", "system", allocator);
    system.AddMember("content", "You are a helpful assistant.", allocator);
    messages.PushBack(system, allocator);

    const auto prompt =
            rapidjson::StringRef(request.prompt.empty() ? "" : request.prompt.data(), request.prompt.size());
    rapidjson::Value user(rapidjson::kObjectType);
    user.AddMember("role", "user", allocator);
    if (!request.media) {
        user.AddMember("content", prompt, allocator);
    } else {
        const auto& media = *request.media;
        if (!media.is_image()) {
            return Status::NotSupported("OpenAI-compatible chat only supports images as FILE input");
        }
        std::string_view uri;
        if (media.referenced_url) {
            if (media.inline_bytes || media.referenced_url->empty()) return invalid_request();
            uri = *media.referenced_url;
        } else {
            ASSIGN_OR_RETURN(encoded_media, encode_ai_media_data_uri(media));
            uri = encoded_media;
        }

        rapidjson::Value content(rapidjson::kArrayType);
        rapidjson::Value text(rapidjson::kObjectType);
        text.AddMember("type", "text", allocator);
        text.AddMember("text", prompt, allocator);
        content.PushBack(text, allocator);

        rapidjson::Value url(rapidjson::kObjectType);
        url.AddMember("url", rapidjson::StringRef(uri.data(), uri.size()), allocator);
        rapidjson::Value part(rapidjson::kObjectType);
        part.AddMember("type", "image_url", allocator);
        part.AddMember("image_url", url, allocator);
        content.PushBack(part, allocator);
        user.AddMember("content", content, allocator);
    }
    messages.PushBack(user, allocator);
    if (!writer.Key("messages") || !messages.Accept(writer)) return invalid_request();
    return Status::OK();
}

Status write_options(const AIChatRequest& request, std::optional<int32_t> default_embedding_dimensions,
                     RequestWriter& writer) {
    if (request.options) {
        for (const auto& option : request.options->members()) {
            const bool reserved = option.key == "model" ||
                                  (request.capability == AICapability::CHAT &&
                                   (option.key == "messages" || option.key == "stream")) ||
                                  (request.capability == AICapability::EMBEDDING &&
                                   (option.key == "input" || option.key == "encoding_format"));
            if (reserved || !writer.Key(option.key.data(), option.key.size()) ||
                !writer.RawValue(option.serialized_json.data(), option.serialized_json.size(),
                                 json_type(option.kind))) {
                return invalid_request();
            }
            if (option.key == "dimensions") default_embedding_dimensions.reset();
        }
    }
    if (request.capability == AICapability::EMBEDDING && default_embedding_dimensions &&
        (!writer.Key("dimensions") || !writer.Int(*default_embedding_dimensions))) {
        return invalid_request();
    }
    return Status::OK();
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

StatusOr<std::unique_ptr<AIProvider>> OpenAICompatibleProvider::create(const AIProviderConfig& config) {
    return std::unique_ptr<AIProvider>(std::make_unique<OpenAICompatibleProvider>(config.dimensions));
}

StatusOr<AIProviderHttpRequest> OpenAICompatibleProvider::build_request(const AIChatRequest& request) const {
    if (request.model.empty()) {
        return Status::InvalidArgument("AI provider model is empty");
    }

    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!writer.StartObject() || !writer.Key("model") || !writer.String(request.model.data(), request.model.size())) {
        return invalid_request();
    }
    switch (request.capability) {
    case AICapability::CHAT:
        RETURN_IF_ERROR(write_chat_messages(request, writer));
        if (!writer.Key("stream") || !writer.Bool(false)) return invalid_request();
        break;
    case AICapability::EMBEDDING:
        if (request.media) return Status::NotSupported("OpenAI-compatible embedding does not support FILE input");
        if (!writer.Key("input") || !writer.String(request.prompt.data(), request.prompt.size()) ||
            !writer.Key("encoding_format") || !writer.String("float")) {
            return invalid_request();
        }
        break;
    default:
        return invalid_request();
    }

    RETURN_IF_ERROR(write_options(request, _default_embedding_dimensions, writer));
    if (!writer.EndObject() || !writer.IsComplete()) {
        return invalid_request();
    }

    AIProviderHttpRequest result;
    result.url = request.endpoint;
    result.headers = {{.name = "Content-Type", .value = "application/json"},
                      {.name = "Accept", .value = "application/json"}};
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

    if (capability == AICapability::EMBEDDING) {
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
