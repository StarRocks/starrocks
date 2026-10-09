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

#include "platform/llm/dashscope_multimodal_provider.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cmath>
#include <limits>

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
    auto prompt_tokens = token_count("input_tokens");
    // Top-level image tokens are separate from text input tokens. Nested input_tokens_details is a
    // breakdown of the inclusive input count and must not be added again.
    if (usage->value.HasMember("image_tokens")) {
        const auto image_tokens = token_count("image_tokens");
        if (prompt_tokens && image_tokens && *prompt_tokens <= std::numeric_limits<int64_t>::max() - *image_tokens) {
            *prompt_tokens += *image_tokens;
        } else {
            prompt_tokens.reset();
        }
    }
    return {.prompt_tokens = prompt_tokens,
            .completion_tokens = token_count("output_tokens"),
            .total_tokens = token_count("total_tokens")};
}

AIProviderErrorCode classify_error(const rapidjson::Value& envelope) {
    for (const auto* name : {"code", "type"}) {
        const auto member = envelope.FindMember(name);
        if (member != envelope.MemberEnd() && member->value.IsString()) {
            auto code = parse_ai_provider_error_code({member->value.GetString(), member->value.GetStringLength()});
            if (code.has_value()) return *code;
        }
    }
    return AIProviderErrorCode::UNKNOWN;
}

} // namespace

StatusOr<AIProviderHttpRequest> DashScopeMultimodalProvider::build_request(const AIChatRequest& request) const {
    if (request.capability != AICapability::TEXT_EMBEDDING) {
        return Status::NotSupported("DashScope multimodal protocol supports embedding only");
    }
    if (request.model.empty()) return Status::InvalidArgument("AI provider model is empty");
    for (unsigned char byte : request.api_key) {
        if (byte <= 0x1f || byte == 0x7f) return Status::InvalidArgument("AI provider API key is invalid");
    }

    std::string media_uri;
    std::string_view content = request.prompt;
    const char* content_key = "text";
    if (request.media != nullptr) {
        if (request.media->mime_type == "video/mp4") {
            return Status::NotSupported("DashScope multimodal protocol does not support inline video input");
        }
        // One SQL row represents one modality, never an implicit text/image fusion.
        if (!request.prompt.empty()) return invalid_request();
        auto encoded = encode_ai_media_data_uri(*request.media);
        if (!encoded.ok()) return encoded.status();
        media_uri = std::move(*encoded);
        content = media_uri;
        content_key = "image";
    }

    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!writer.StartObject() || !writer.Key("model") || !writer.String(request.model.data(), request.model.size()) ||
        !writer.Key("input") || !writer.StartObject() || !writer.Key("contents") || !writer.StartArray() ||
        !writer.StartObject() || !writer.Key(content_key) || !writer.String(content.data(), content.size()) ||
        !writer.EndObject() || !writer.EndArray() || !writer.EndObject()) {
        return invalid_request();
    }
    if (request.options != nullptr && !request.options->members().empty()) {
        if (!writer.Key("parameters") || !writer.StartObject()) return invalid_request();
        for (const auto& option : request.options->members()) {
            if (option.key == "model" || option.key == "input" || option.key == "contents" ||
                option.key == "parameters" || option.key == "encoding_format" || option.key == "messages" ||
                option.key == "stream") {
                return invalid_request();
            }
            // The immutable option was validated at construction. Visit its typed JSON value without string coercion.
            rapidjson::Document value;
            value.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(
                    option.serialized_json.data(), option.serialized_json.size());
            if (value.HasParseError() || !writer.Key(option.key.data(), option.key.size()) || !value.Accept(writer)) {
                return invalid_request();
            }
        }
        if (!writer.EndObject()) return invalid_request();
    }
    if (!writer.EndObject() || !writer.IsComplete()) return invalid_request();

    AIProviderHttpRequest result;
    result.url.assign(request.endpoint.data(), request.endpoint.size());
    result.headers = {{.name = "Content-Type", .value = "application/json"},
                      {.name = "Accept", .value = "application/json"}};
    if (!request.api_key.empty()) {
        result.headers.emplace_back(
                AIHttpHeader{.name = "Authorization", .value = "Bearer " + std::string(request.api_key)});
    }
    result.body.assign(buffer.GetString(), buffer.GetSize());
    return result;
}

AIProviderParseResult DashScopeMultimodalProvider::parse_response(std::string_view body,
                                                                  AICapability capability) const {
    if (body.find('\0') != std::string_view::npos) return AIProviderMalformed{};
    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(body.data(),
                                                                                               body.size());
    if (document.HasParseError() || !document.IsObject()) return AIProviderMalformed{};
    const auto usage = parse_usage(document);
    const auto nested_error = document.FindMember("error");
    if (nested_error != document.MemberEnd() && nested_error->value.IsObject()) {
        return AIProviderStructuredError{.code = classify_error(nested_error->value), .usage = usage};
    }
    if (document.HasMember("code") || document.HasMember("type")) {
        return AIProviderStructuredError{.code = classify_error(document), .usage = usage};
    }
    if (capability != AICapability::TEXT_EMBEDDING) return AIProviderMalformed{.usage = usage};
    const auto output = document.FindMember("output");
    if (output == document.MemberEnd() || !output->value.IsObject()) return AIProviderMalformed{.usage = usage};
    const auto embeddings = output->value.FindMember("embeddings");
    if (embeddings == output->value.MemberEnd() || !embeddings->value.IsArray() || embeddings->value.Size() != 1 ||
        !embeddings->value[0].IsObject()) {
        return AIProviderMalformed{.usage = usage};
    }
    const auto& item = embeddings->value[0];
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

} // namespace starrocks
