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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <limits>
#include <string_view>
#include <unordered_set>

#include "platform/llm/ai_provider.h"

namespace starrocks::ai_rerank_detail {

using RequestWriter = rapidjson::Writer<rapidjson::StringBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>,
                                        rapidjson::CrtAllocator, rapidjson::kWriteValidateEncodingFlag>;

inline Status invalid_request() {
    return Status::InvalidArgument("AI rerank provider request is invalid");
}

inline StatusOr<AIRerankResponseShape> request_shape(const AIChatRequest& request) {
    if (request.capability != AICapability::RERANK || request.rerank == nullptr || request.media != nullptr ||
        !request.prompt.empty() || request.model.empty() || request.rerank->documents.empty()) {
        return invalid_request();
    }
    if (request.rerank->documents.size() > std::numeric_limits<int32_t>::max()) {
        return Status::CapacityLimitExceed("AI rerank candidate count exceeds INT capacity");
    }
    for (unsigned char byte : request.api_key) {
        if (byte <= 0x1f || byte == 0x7f) return invalid_request();
    }
    const auto count = static_cast<int32_t>(request.rerank->documents.size());
    int32_t expected = count;
    if (request.options != nullptr) {
        RETURN_IF_ERROR(validate_ai_rerank_options(*request.options));
        auto top_n = parse_ai_rerank_top_n(*request.options);
        if (!top_n.ok()) return top_n.status();
        if (top_n->has_value()) expected = std::min(count, **top_n);
    }
    return AIRerankResponseShape{count, expected};
}

inline bool write_input(RequestWriter& writer, const AIRerankInput& input) {
    if (!writer.Key("query") || !writer.String(input.query.data(), input.query.size()) || !writer.Key("documents") ||
        !writer.StartArray())
        return false;
    for (const auto& document : input.documents) {
        if (!writer.String(document.data(), document.size())) return false;
    }
    return writer.EndArray();
}

inline bool write_options(RequestWriter& writer, const AIProviderOptions* options, int32_t top_n) {
    if (!writer.Key("top_n") || !writer.Int(top_n)) return false;
    if (options == nullptr) return true;
    for (const auto& option : options->members()) {
        if (option.key == "top_n") continue;
        rapidjson::Document value;
        value.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(
                option.serialized_json.data(), option.serialized_json.size());
        if (value.HasParseError() || !writer.Key(option.key.data(), option.key.size()) || !value.Accept(writer)) {
            return false;
        }
    }
    return true;
}

inline AIProviderHttpRequest http_request(const AIChatRequest& request, const rapidjson::StringBuffer& body,
                                          AIRerankResponseShape shape) {
    AIProviderHttpRequest result;
    result.url = request.endpoint;
    result.headers = {{.name = "Content-Type", .value = "application/json"},
                      {.name = "Accept", .value = "application/json"}};
    if (!request.api_key.empty()) {
        result.headers.push_back({.name = "Authorization", .value = "Bearer " + std::string(request.api_key)});
    }
    result.body.assign(body.GetString(), body.GetSize());
    result.response_context = AIProviderResponseContext(shape);
    return result;
}

// Duplicate fields are ambiguous, including in unconsumed envelopes and metadata.
inline bool has_duplicate_members(const rapidjson::Value& root) {
    std::vector<const rapidjson::Value*> pending{&root};
    while (!pending.empty()) {
        const auto* value = pending.back();
        pending.pop_back();
        if (value->IsObject()) {
            std::unordered_set<std::string_view> names;
            for (auto member = value->MemberBegin(); member != value->MemberEnd(); ++member) {
                if (!names.emplace(member->name.GetString(), member->name.GetStringLength()).second) return true;
                pending.push_back(&member->value);
            }
        } else if (value->IsArray()) {
            for (const auto& item : value->GetArray()) pending.push_back(&item);
        }
    }
    return false;
}

inline const rapidjson::Value* unique_member(const rapidjson::Value& value, std::string_view name) {
    if (!value.IsObject()) return nullptr;
    const rapidjson::Value* result = nullptr;
    for (auto member = value.MemberBegin(); member != value.MemberEnd(); ++member) {
        if (std::string_view(member->name.GetString(), member->name.GetStringLength()) != name) continue;
        if (result != nullptr) return nullptr;
        result = &member->value;
    }
    return result;
}

inline std::optional<int64_t> token_count(const rapidjson::Value* usage, const char* key) {
    if (usage == nullptr) return std::nullopt;
    const auto* value = unique_member(*usage, key);
    if (value == nullptr || !value->IsInt64() || value->GetInt64() < 0) return std::nullopt;
    return value->GetInt64();
}

inline AIProviderErrorCode error_code(const rapidjson::Value& envelope) {
    for (const auto* name : {"code", "type"}) {
        const auto* value = unique_member(envelope, name);
        if (value != nullptr && value->IsString()) {
            auto parsed = parse_ai_provider_error_code({value->GetString(), value->GetStringLength()});
            if (parsed) return *parsed;
        }
    }
    return AIProviderErrorCode::UNKNOWN;
}

inline AIProviderParseResult parse_hits(const rapidjson::Value* results, const AIProviderResponseContext& context,
                                        AIProviderUsage usage) {
    const auto* shape = context.rerank_shape();
    if (shape == nullptr || results == nullptr || !results->IsArray() || shape->expected_hits < 0 ||
        results->Size() != static_cast<size_t>(shape->expected_hits))
        return AIProviderMalformed{usage};
    AIRerankResult hits;
    hits.reserve(results->Size());
    for (const auto& item : results->GetArray()) {
        const auto* index = unique_member(item, "index");
        const auto* score = unique_member(item, "relevance_score");
        if (index == nullptr || !index->IsInt() || score == nullptr || !score->IsNumber()) {
            return AIProviderMalformed{usage};
        }
        hits.push_back({index->GetInt(), score->GetDouble()});
    }
    if (!validate_ai_rerank_hits(hits, *shape).ok()) return AIProviderMalformed{usage};
    return AIProviderSuccess{.value = std::move(hits), .usage = usage};
}

} // namespace starrocks::ai_rerank_detail
