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

#include "platform/llm/bailian_decision_provider.h"

#include <algorithm>
#include <cmath>

#include "platform/llm/ai_decision_json.h"

namespace starrocks {
namespace {
using namespace decision_json;

bool in_range(const Value* value, double minimum, double maximum) {
    return value != nullptr && value->IsNumber() && std::isfinite(value->GetDouble()) &&
           value->GetDouble() >= minimum && value->GetDouble() <= maximum;
}

Status invalid_response(std::string_view detail) {
    // Do not echo the response or question text into errors: both may contain
    // customer data. The request ID remains available on successful SQL output.
    return Status::DataQualityError(std::string("ai_decide response: ") + std::string(detail));
}

Status validate_distribution(const Value* probabilities, const AIDecisionQuestion& question) {
    if (probabilities == nullptr || !unique_fields(*probabilities) ||
        probabilities->MemberCount() != question.labels.size()) {
        return invalid_response("probabilities must cover exactly the requested options or score levels");
    }
    double sum = 0;
    for (const auto& label : question.labels) {
        const auto* probability = field(*probabilities, label);
        if (!in_range(probability, 0, 1)) return invalid_response("invalid probability");
        sum += probability->GetDouble();
    }
    // Permit normal serialization/rounding error without accepting arbitrary
    // scores as probability distributions.
    if (std::abs(sum - 1.0) > 1e-4) return invalid_response("probabilities must sum to one");
    return Status::OK();
}

Status validate_answer(const Value& answer, const AIDecisionQuestion& question) {
    if (!unique_fields(answer)) return invalid_response("answer must be an object with unique fields");
    const auto* type = field(answer, "type");
    if (type == nullptr || !type->IsString() || string_view(*type) != question.type) {
        return invalid_response("answer type does not match its question");
    }
    if (question.type == "noul") {
        return in_range(field(answer, "noul"), 0, 1) ? Status::OK()
                                                     : invalid_response("noul must be a probability in [0, 1]");
    }
    if (!in_range(field(answer, "confidence"), 0, 1)) return invalid_response("invalid confidence");
    RETURN_IF_ERROR(validate_distribution(field(answer, "probabilities"), question));
    if (question.type == "choice") {
        const auto* choice = field(answer, "choice");
        if (choice == nullptr || !choice->IsString() ||
            std::find(question.labels.begin(), question.labels.end(), string_view(*choice)) == question.labels.end()) {
            return invalid_response("choice must name a requested option");
        }
        return Status::OK();
    }
    if (!in_range(field(answer, "score"), 0, question.labels.size() - 1)) {
        return invalid_response("score is outside the requested scale");
    }
    const auto* legend = field(answer, "legend");
    if (legend == nullptr || !unique_fields(*legend) || legend->MemberCount() != question.labels.size()) {
        return invalid_response("legend must cover the requested score levels");
    }
    for (const auto& label : question.labels) {
        const auto* description = field(*legend, label);
        if (description == nullptr || !description->IsString()) return invalid_response("invalid score legend");
    }
    return Status::OK();
}

Status validate_response(const Value& doc, const AIDecisionResponseSchema& questions) {
    const auto* answers = field(doc, "answers");
    if (answers == nullptr || !unique_fields(*answers) || answers->MemberCount() != questions.questions.size()) {
        return invalid_response("answers must cover exactly the requested questions");
    }
    for (const auto& question : questions.questions) {
        const auto* answer = field(*answers, question.id);
        if (answer == nullptr) return invalid_response("missing requested answer");
        RETURN_IF_ERROR(validate_answer(*answer, question));
    }
    const auto* model = field(doc, "model");
    if (model != nullptr && !model->IsString()) return invalid_response("model must be a string");
    const auto* request_id = field(doc, "request_id");
    if (request_id != nullptr && !request_id->IsString()) return invalid_response("request_id must be a string");
    const auto* latency = field(doc, "latency_ms");
    if (latency != nullptr &&
        (!latency->IsNumber() || !std::isfinite(latency->GetDouble()) || latency->GetDouble() < 0)) {
        return invalid_response("invalid latency_ms");
    }
    return Status::OK();
}

} // namespace

StatusOr<AIProviderHttpRequest> BailianDecisionProvider::build_request(const AIChatRequest& request) const {
    if (request.capability != AICapability::DECISION || request.decision == nullptr ||
        request.decision->questions == nullptr || request.options != nullptr || request.media != nullptr) {
        return Status::InvalidArgument("AI decision request is invalid");
    }
    if (request.model.empty()) return Status::InvalidArgument("ai_decide requires a nonempty model");
    for (const unsigned char byte : request.api_key) {
        if (byte < 0x20 || byte == 0x7f) return Status::InvalidArgument("AI provider API key is invalid");
    }
    const auto model = request.model;
    const auto& state = request.decision->state;
    const bool state_is_json = request.decision->state_is_json;
    const auto& questions = *request.decision->questions;
    AIJsonDocument parsed_state;
    if (state_is_json) {
        if (!parse_complete_json(state, &parsed_state) || !unique_fields_recursively(parsed_state) ||
            !(parsed_state.IsString() || parsed_state.IsObject() || parsed_state.IsArray())) {
            return Status::InvalidArgument("ai_decide JSON state must be a string, object or array");
        }
    }
    AIJsonStringBuffer buffer;
    rapidjson::Writer<AIJsonStringBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>, AIJsonAllocator<>,
                      rapidjson::kWriteValidateEncodingFlag>
            writer(buffer);
    writer.StartObject();
    writer.Key("model");
    if (!writer.String(model.data(), model.size())) {
        return Status::InvalidArgument("ai_decide model must contain valid UTF-8");
    }
    writer.Key("state");
    if (state_is_json) {
        writer.RawValue(state.data(), state.size(), parsed_state.GetType());
    } else if (!writer.String(state.data(), state.size())) {
        return Status::InvalidArgument("ai_decide state must contain valid UTF-8");
    }
    writer.Key("questions");
    writer.RawValue(questions.json().data(), questions.json().size(), rapidjson::kObjectType);
    writer.EndObject();
    AIProviderHttpRequest result;
    result.url = request.endpoint;
    result.headers.push_back({.name = "Content-Type", .value = "application/json"});
    if (!request.api_key.empty()) {
        result.headers.push_back({.name = "Authorization", .value = "Bearer " + std::string(request.api_key)});
    }
    result.body.assign(buffer.GetString(), buffer.GetSize());
    result.response_context = AIProviderResponseContext(questions.schema());
    return result;
}

AIProviderParseResult BailianDecisionProvider::parse_response(std::string_view body, AICapability capability,
                                                              const AIProviderResponseContext& context) const {
    AIJsonDocument doc;
    if (!parse_complete_json(body, &doc) || !unique_fields(doc)) return AIProviderMalformed{};
    AIProviderUsage usage;
    if (const auto* value = field(doc, "usage"); value != nullptr) {
        if (!unique_fields(*value)) return AIProviderMalformed{};
        if (const auto* tokens = field(*value, "input_tokens"); tokens != nullptr) {
            if (!tokens->IsInt64() || tokens->GetInt64() < 0) return AIProviderMalformed{};
            usage.prompt_tokens = tokens->GetInt64();
            usage.completion_tokens = 0;
            usage.total_tokens = tokens->GetInt64();
        }
    }
    const auto* schema = context.decision_schema();
    if (capability != AICapability::DECISION || schema == nullptr || schema->questions.empty() ||
        !unique_fields_recursively(doc) || !validate_response(doc, *schema).ok()) {
        return AIProviderMalformed{.usage = usage};
    }
    return AIProviderSuccess{.value = std::string(body), .usage = usage};
}

} // namespace starrocks
