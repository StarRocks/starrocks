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

#include "platform/llm/dashscope_provider.h"

#include <event2/http.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cmath>
#include <limits>

namespace starrocks {
namespace {

using RequestWriter = rapidjson::Writer<rapidjson::StringBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>,
                                        rapidjson::CrtAllocator, rapidjson::kWriteValidateEncodingFlag>;

using EndpointType = DashScopeProvider::EndpointType;

AICapability capability_for(const EndpointType endpoint_type) {
    return endpoint_type == EndpointType::TEXT_CHAT || endpoint_type == EndpointType::MULTIMODAL_CHAT
                   ? AICapability::CHAT
                   : AICapability::EMBEDDING;
}

Status invalid_request() {
    return Status::InvalidArgument("AI provider request is invalid");
}

AIProviderUsage parse_usage(const rapidjson::Value& document, EndpointType endpoint_type) {
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
    if (endpoint_type == EndpointType::TEXT_EMBEDDING && !usage->value.HasMember("input_tokens")) {
        prompt_tokens = token_count("prompt_tokens");
    }
    // Chat reports inclusive input_tokens. Some multimodal embedding models instead
    // report text and media separately; nested details already belong to the total.
    if (endpoint_type == EndpointType::MULTIMODAL_EMBEDDING && !usage->value.HasMember("input_tokens_details")) {
        for (const char* name : {"image_tokens", "video_tokens"}) {
            if (!usage->value.HasMember(name)) continue;
            const auto media_tokens = token_count(name);
            if (!prompt_tokens || !media_tokens ||
                *prompt_tokens > std::numeric_limits<int64_t>::max() - *media_tokens) {
                prompt_tokens.reset();
                break;
            }
            *prompt_tokens += *media_tokens;
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

// The request, its options, and any encoded media outlive serialization.
rapidjson::Value json_string(std::string_view text) {
    return rapidjson::Value(rapidjson::StringRef(text.empty() ? "" : text.data(), text.size()));
}

rapidjson::Value content_part(const char* key, std::string_view value, rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value part(rapidjson::kObjectType);
    part.AddMember(rapidjson::StringRef(key), json_string(value), allocator);
    return part;
}

StatusOr<rapidjson::Value> make_input(EndpointType endpoint_type, const AIChatRequest& request,
                                      rapidjson::Value& parameters, std::string* encoded_media,
                                      rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value input(rapidjson::kObjectType);
    rapidjson::Value parts(rapidjson::kArrayType);
    if (request.media != nullptr) {
        if (endpoint_type == EndpointType::TEXT_CHAT || endpoint_type == EndpointType::TEXT_EMBEDDING) {
            return Status::NotSupported("DashScope text endpoint does not support FILE input");
        }
        const auto& media = *request.media;
        const bool is_video = media.is_video();
        if (!is_video && !media.is_image()) {
            return Status::NotSupported("DashScope visual input requires an image or video");
        }
        if (endpoint_type == EndpointType::MULTIMODAL_EMBEDDING) {
            if (!request.prompt.empty()) return invalid_request();
            if (is_video && media.inline_bytes) {
                return Status::NotSupported("DashScope multimodal embedding requires a URL for video input");
            }
        }
        std::string_view uri;
        if (media.referenced_url) {
            if (media.inline_bytes || media.referenced_url->empty()) return invalid_request();
            uri = *media.referenced_url;
        } else {
            ASSIGN_OR_RETURN(*encoded_media, encode_ai_media_data_uri(media));
            uri = *encoded_media;
        }
        auto part = content_part(is_video ? "video" : "image", uri, allocator);
        // DashScope Chat takes fps on the video part. Embedding takes it in parameters.
        if (endpoint_type == EndpointType::MULTIMODAL_CHAT && is_video) {
            auto fps = parameters.FindMember("fps");
            if (fps != parameters.MemberEnd()) {
                part.AddMember("fps", fps->value, allocator);
                parameters.EraseMember(fps);
            }
        }
        parts.PushBack(part, allocator);
    }
    switch (endpoint_type) {
    case EndpointType::TEXT_EMBEDDING:
        parts.PushBack(json_string(request.prompt), allocator);
        input.AddMember("texts", parts, allocator);
        break;
    case EndpointType::MULTIMODAL_EMBEDDING:
        if (!request.media) parts.PushBack(content_part("text", request.prompt, allocator), allocator);
        input.AddMember("contents", parts, allocator);
        break;
    case EndpointType::TEXT_CHAT:
    case EndpointType::MULTIMODAL_CHAT: {
        rapidjson::Value message(rapidjson::kObjectType);
        message.AddMember("role", "user", allocator);
        if (endpoint_type == EndpointType::TEXT_CHAT) {
            message.AddMember("content", json_string(request.prompt), allocator);
        } else {
            if (!request.media || !request.prompt.empty()) {
                parts.PushBack(content_part("text", request.prompt, allocator), allocator);
            }
            message.AddMember("content", parts, allocator);
        }
        rapidjson::Value messages(rapidjson::kArrayType);
        messages.PushBack(message, allocator);
        input.AddMember("messages", messages, allocator);
        break;
    }
    }
    return input;
}

StatusOr<rapidjson::Value> make_parameters(const AIChatRequest& request,
                                           std::optional<int32_t> default_embedding_dimensions,
                                           rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value parameters(rapidjson::kObjectType);
    if (request.capability == AICapability::CHAT) parameters.AddMember("result_format", "message", allocator);
    if (request.options) {
        for (const auto& option : request.options->members()) {
            const std::string_view key = option.key;
            if (key == "model" || key == "input" || key == "contents" || key == "parameters" ||
                key == "encoding_format" || key == "messages" || key == "stream") {
                return invalid_request();
            }
            rapidjson::Document value(&allocator);
            value.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(
                    option.serialized_json.data(), option.serialized_json.size());
            if (value.HasParseError()) return invalid_request();
            if (request.capability == AICapability::CHAT && key == "result_format") {
                if (value != "message") return invalid_request();
                continue;
            }
            parameters.AddMember(json_string(key), value.Move(), allocator);
        }
    }
    if (request.capability == AICapability::EMBEDDING && default_embedding_dimensions &&
        !parameters.HasMember("dimension")) {
        parameters.AddMember("dimension", *default_embedding_dimensions, allocator);
    }
    return parameters;
}

AIProviderParseResult parse_chat(const rapidjson::Value& output, EndpointType endpoint_type, AIProviderUsage usage) {
    auto choices = output.FindMember("choices");
    if (choices == output.MemberEnd() || !choices->value.IsArray() || choices->value.Empty() ||
        !choices->value[0].IsObject())
        return AIProviderMalformed{.usage = usage};
    const auto& choice = choices->value[0];
    auto message = choice.FindMember("message");
    if (message == choice.MemberEnd() || !message->value.IsObject()) return AIProviderMalformed{.usage = usage};
    auto content = message->value.FindMember("content");
    if (content == message->value.MemberEnd()) return AIProviderMalformed{.usage = usage};
    if (content->value.IsString()) {
        return AIProviderSuccess{.value = std::string(content->value.GetString(), content->value.GetStringLength()),
                                 .usage = usage};
    }
    if (endpoint_type != EndpointType::MULTIMODAL_CHAT || !content->value.IsArray() || content->value.Empty()) {
        return AIProviderMalformed{.usage = usage};
    }
    std::string answer;
    for (const auto& part : content->value.GetArray()) {
        if (!part.IsObject()) return AIProviderMalformed{.usage = usage};
        auto text = part.FindMember("text");
        if (text == part.MemberEnd() || !text->value.IsString()) return AIProviderMalformed{.usage = usage};
        answer.append(text->value.GetString(), text->value.GetStringLength());
    }
    return AIProviderSuccess{.value = std::move(answer), .usage = usage};
}

} // namespace

StatusOr<std::unique_ptr<AIProvider>> DashScopeProvider::create(const AIProviderConfig& config) {
    std::unique_ptr<evhttp_uri, decltype(&evhttp_uri_free)> uri(evhttp_uri_parse(config.endpoint.c_str()),
                                                                evhttp_uri_free);
    const char* path = uri ? evhttp_uri_get_path(uri.get()) : nullptr;
    if (path != nullptr) {
        static constexpr struct {
            std::string_view suffix;
            EndpointType endpoint_type;
        } routes[] = {{"/text-generation/generation", EndpointType::TEXT_CHAT},
                      {"/multimodal-generation/generation", EndpointType::MULTIMODAL_CHAT},
                      {"/text-embedding/text-embedding", EndpointType::TEXT_EMBEDDING},
                      {"/multimodal-embedding/multimodal-embedding", EndpointType::MULTIMODAL_EMBEDDING}};
        for (const auto& route : routes) {
            if (std::string_view(path).ends_with(route.suffix)) {
                if (config.capability != capability_for(route.endpoint_type)) {
                    return Status::InvalidArgument("DashScope endpoint does not match the AI provider type");
                }
                return std::unique_ptr<AIProvider>(
                        std::make_unique<DashScopeProvider>(route.endpoint_type, config.dimensions));
            }
        }
    }
    return Status::InvalidArgument("Unrecognized DashScope endpoint path");
}

StatusOr<AIProviderHttpRequest> DashScopeProvider::build_request(const AIChatRequest& request) const {
    if (request.capability != capability_for(_endpoint_type)) {
        return Status::NotSupported("DashScope endpoint does not support the requested capability");
    }
    if (request.model.empty()) return Status::InvalidArgument("AI provider model is empty");

    std::string encoded_media;
    rapidjson::Document document(rapidjson::kObjectType);
    auto& allocator = document.GetAllocator();
    document.AddMember("model", json_string(request.model), allocator);
    ASSIGN_OR_RETURN(auto parameters, make_parameters(request, _default_embedding_dimensions, allocator));
    ASSIGN_OR_RETURN(auto input, make_input(_endpoint_type, request, parameters, &encoded_media, allocator));
    document.AddMember("input", input, allocator);
    if (!parameters.ObjectEmpty()) document.AddMember("parameters", parameters, allocator);

    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!document.Accept(writer) || !writer.IsComplete()) return invalid_request();

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

AIProviderParseResult DashScopeProvider::parse_response(std::string_view body, AICapability capability) const {
    if (body.find('\0') != std::string_view::npos) return AIProviderMalformed{};
    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(body.data(),
                                                                                               body.size());
    if (document.HasParseError() || !document.IsObject()) return AIProviderMalformed{};
    const auto usage = parse_usage(document, _endpoint_type);
    const auto nested_error = document.FindMember("error");
    if (nested_error != document.MemberEnd() && nested_error->value.IsObject()) {
        return AIProviderStructuredError{.code = classify_error(nested_error->value), .usage = usage};
    }
    if (document.HasMember("code") || document.HasMember("type")) {
        return AIProviderStructuredError{.code = classify_error(document), .usage = usage};
    }
    if (capability != capability_for(_endpoint_type)) return AIProviderMalformed{.usage = usage};
    const auto output = document.FindMember("output");
    if (output == document.MemberEnd() || !output->value.IsObject()) return AIProviderMalformed{.usage = usage};
    if (capability == AICapability::CHAT) return parse_chat(output->value, _endpoint_type, usage);
    const auto embeddings = output->value.FindMember("embeddings");
    if (embeddings == output->value.MemberEnd() || !embeddings->value.IsArray() || embeddings->value.Size() != 1 ||
        !embeddings->value[0].IsObject()) {
        return AIProviderMalformed{.usage = usage};
    }
    const auto& item = embeddings->value[0];
    auto index = item.FindMember("index");
    if (_endpoint_type == EndpointType::TEXT_EMBEDDING) {
        auto text_index = item.FindMember("text_index");
        if (text_index != item.MemberEnd()) {
            if (index != item.MemberEnd() && index->value != text_index->value)
                return AIProviderMalformed{.usage = usage};
            index = text_index;
        }
    }
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
