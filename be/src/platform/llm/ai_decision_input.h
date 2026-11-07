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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/statusor.h"

namespace starrocks {

struct AIDecisionQuestion {
    std::string id;
    std::string type;
    std::vector<std::string> labels;
};

// Response validation needs identifiers and legal values only. Never retain
// instructions, criteria descriptions, input state or credentials here.
struct AIDecisionResponseSchema {
    std::vector<AIDecisionQuestion> questions;
    size_t retained_bytes() const;
};

class AIDecisionQuestions {
public:
    const std::string& json() const { return _json; }
    const AIDecisionResponseSchema& schema() const { return _schema; }
    size_t retained_bytes() const { return _json.capacity() + 1 + _schema.retained_bytes(); }

private:
    friend StatusOr<AIDecisionQuestions> parse_ai_decision_questions(std::string_view json);
    AIDecisionQuestions(std::string json, AIDecisionResponseSchema schema)
            : _json(std::move(json)), _schema(std::move(schema)) {}
    std::string _json;
    AIDecisionResponseSchema _schema;
};

struct AIDecisionInput {
    std::string state;
    bool state_is_json = false;
    std::shared_ptr<const AIDecisionQuestions> questions;
};

StatusOr<AIDecisionQuestions> parse_ai_decision_questions(std::string_view json);

} // namespace starrocks
