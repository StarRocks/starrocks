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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "exprs/expr.h"
#include "platform/llm/ai_provider_options.h"

namespace starrocks {

enum class AIFunctionSignature : uint8_t {
    PROMPT,
    PROMPT_OPTIONS,
    MODEL_PROMPT,
    MODEL_PROMPT_OPTIONS,
};

enum class AIFunctionRowAction : uint8_t {
    DISPATCH,
    SQL_NULL,
    TERMINAL_ROW_FAILURE,
};

struct AIFunctionRowInput {
    AIFunctionRowAction action = AIFunctionRowAction::SQL_NULL;
    std::string model;
    std::string prompt;
};

struct AIFunctionInputBatch {
    std::vector<AIFunctionRowInput> rows;
    std::shared_ptr<const AIProviderOptions> options;
};

class AIFunctionCallExpr final : public Expr {
public:
    static StatusOr<AIFunctionCallExpr*> create(ObjectPool* pool, const TExprNode& node);
    static bool is_ai_function_name(std::string_view name);
    static bool is_ai_function_id(int64_t fid);

    Expr* clone(ObjectPool* pool) const override;

    AIFunctionSignature signature() const { return _signature; }
    std::string_view model_config_id() const { return _model_config_id; }

    bool is_constant() const override { return false; }

    StatusOr<AIFunctionInputBatch> build_input_batch(ExprContext* context, Chunk* chunk,
                                                     std::string_view default_model) const;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override;

protected:
    Status prepare(RuntimeState* state, ExprContext* context) override;

private:
    AIFunctionCallExpr(const TExprNode& node, AIFunctionSignature signature,
                       std::vector<TypeDescriptor> argument_types);
    AIFunctionCallExpr(const AIFunctionCallExpr& other);

    Status _validate_children() const;

    const AIFunctionSignature _signature;
    const std::string _model_config_id;
    const std::vector<TypeDescriptor> _argument_types;
};

} // namespace starrocks
