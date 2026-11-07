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

#include "platform/llm/ai_decision_input.h"

#include "platform/llm/ai_decision_json.h"

namespace starrocks {
namespace {
using namespace decision_json;
Status invalid_questions(std::string_view detail) {
    return Status::InvalidArgument(std::string("ai_decide questions: ") + std::string(detail));
}
} // namespace

size_t AIDecisionResponseSchema::retained_bytes() const {
    size_t bytes = questions.capacity() * sizeof(AIDecisionQuestion);
    for (const auto& question : questions) {
        bytes += question.id.capacity() + question.type.capacity() + 2;
        bytes += question.labels.capacity() * sizeof(std::string);
        for (const auto& label : question.labels) bytes += label.capacity() + 1;
    }
    return bytes;
}

StatusOr<AIDecisionQuestions> parse_ai_decision_questions(std::string_view json) {
    AIJsonDocument doc;
    if (!parse_complete_json(json, &doc) || !unique_fields(doc) || !unique_fields_recursively(doc) ||
        doc.MemberCount() == 0 || doc.MemberCount() > 16) {
        return invalid_questions("expected an object with 1 to 16 unique question IDs");
    }
    AIDecisionResponseSchema schema;
    for (const auto& member : doc.GetObject()) {
        if (member.name.GetStringLength() == 0 || !unique_fields(member.value)) {
            return invalid_questions("IDs must be nonempty and definitions must have unique fields");
        }
        const auto& definition = member.value;
        for (const auto& option : definition.GetObject()) {
            const auto key = string_view(option.name);
            if (key != "type" && key != "instructions" && key != "criteria") {
                return invalid_questions("only type, instructions and criteria are supported");
            }
        }
        const auto* type = field(definition, "type");
        if (type == nullptr || !type->IsString() ||
            (string_view(*type) != "choice" && string_view(*type) != "noul" && string_view(*type) != "score")) {
            return invalid_questions("type must be choice, noul or score");
        }
        const auto* instructions = field(definition, "instructions");
        if (instructions != nullptr && !instructions->IsString()) {
            return invalid_questions("instructions must be a string");
        }
        AIDecisionQuestion question{std::string(string_view(member.name)), std::string(string_view(*type)), {}};
        const auto* criteria = field(definition, "criteria");
        if (question.type == "score") {
            if (criteria == nullptr || !criteria->IsArray() || criteria->Size() < 2 || criteria->Size() > 255) {
                return invalid_questions("score criteria must contain 2 to 255 ordered descriptions");
            }
            for (rapidjson::SizeType index = 0; index < criteria->Size(); ++index) {
                if (!(*criteria)[index].IsString()) return invalid_questions("score descriptions must be strings");
                question.labels.push_back(std::to_string(index));
            }
        } else if (question.type == "choice" || criteria != nullptr) {
            if (criteria == nullptr || !unique_fields(*criteria) ||
                (question.type == "choice" && (criteria->MemberCount() == 0 || criteria->MemberCount() > 255))) {
                return invalid_questions(
                        "choice criteria must contain 1 to 255 unique options; noul criteria must be an object");
            }
            for (const auto& option : criteria->GetObject()) {
                const auto label = string_view(option.name);
                if (label.empty() || !option.value.IsString()) {
                    return invalid_questions("criteria must map nonempty option names to string descriptions");
                }
                if (question.type == "noul" && label != "true" && label != "false") {
                    return invalid_questions("noul criteria only accepts true and false descriptions");
                }
                question.labels.emplace_back(label);
            }
        }
        schema.questions.push_back(std::move(question));
    }
    AIJsonStringBuffer buffer;
    AIJsonWriter writer(buffer);
    doc.Accept(writer);
    return AIDecisionQuestions(std::string(buffer.GetString(), buffer.GetSize()), std::move(schema));
}

} // namespace starrocks
