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

#include "platform/llm/cohere_rerank_provider.h"

#include "platform/llm/ai_rerank_provider_utils.h"

namespace starrocks {
namespace {

AIProviderUsage parse_usage(const rapidjson::Value& document) {
    using namespace ai_rerank_detail;
    const auto* meta = unique_member(document, "meta");
    if (meta == nullptr) return {};
    // Prefer reported billed tokens per field, falling back to actual tokens when absent. Never add sources.
    const auto* billed = unique_member(*meta, "billed_units");
    const auto* tokens = unique_member(*meta, "tokens");
    auto prompt = token_count(billed, "input_tokens");
    auto completion = token_count(billed, "output_tokens");
    if (!prompt) prompt = token_count(tokens, "input_tokens");
    if (!completion) completion = token_count(tokens, "output_tokens");
    return {.prompt_tokens = prompt, .completion_tokens = completion};
}

} // namespace

StatusOr<AIProviderHttpRequest> CohereRerankProvider::build_request(const AIChatRequest& request) const {
    using namespace ai_rerank_detail;
    auto shape = request_shape(request);
    if (!shape.ok()) return shape.status();
    rapidjson::StringBuffer buffer;
    RequestWriter writer(buffer);
    if (!writer.StartObject() || !writer.Key("model") || !writer.String(request.model.data(), request.model.size()) ||
        !write_input(writer, *request.rerank) || !write_options(writer, request.options, shape->expected_hits) ||
        !writer.EndObject() || !writer.IsComplete())
        return invalid_request();
    return http_request(request, buffer, *shape);
}

AIProviderParseResult CohereRerankProvider::parse_response(std::string_view body, AICapability capability,
                                                           const AIProviderResponseContext& context) const {
    using namespace ai_rerank_detail;
    if (body.find('\0') != std::string_view::npos) return AIProviderMalformed{};
    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag |
                   rapidjson::kParseIterativeFlag>(body.data(), body.size());
    if (document.HasParseError() || !document.IsObject()) return AIProviderMalformed{};
    auto usage = parse_usage(document);
    if (has_duplicate_members(document)) return AIProviderMalformed{usage};
    const auto* error = unique_member(document, "error");
    if (error != nullptr && error->IsObject()) return AIProviderStructuredError{error_code(*error), usage};
    if (document.HasMember("code") || document.HasMember("type") || document.HasMember("message")) {
        return AIProviderStructuredError{error_code(document), usage};
    }
    if (capability != AICapability::RERANK) return AIProviderMalformed{usage};
    return parse_hits(unique_member(document, "results"), context, usage);
}

} // namespace starrocks
