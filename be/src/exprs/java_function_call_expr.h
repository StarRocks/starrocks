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
#include <memory>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {
struct JavaUDFContext;
struct UDFFunctionCallHelper;

class JavaFunctionCallExpr final : public Expr {
public:
    JavaFunctionCallExpr(const TExprNode& node);
    ~JavaFunctionCallExpr() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new JavaFunctionCallExpr(*this)); }
    [[nodiscard]] StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
    [[nodiscard]] Status prepare(RuntimeState* state, ExprContext* context) override;
    [[nodiscard]] Status open(RuntimeState* state, ExprContext* context,
                              FunctionContext::FunctionStateScope scope) override;
    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    bool is_constant() const override;

private:
    StatusOr<std::shared_ptr<JavaUDFContext>> _build_udf_func_desc(ExprContext* context,
                                                                   FunctionContext::FunctionStateScope scope,
                                                                   const std::string& libpath);
    void _call_udf_close();
    RuntimeState* _runtime_state = nullptr;
    std::shared_ptr<JavaUDFContext> _func_desc;
    std::shared_ptr<UDFFunctionCallHelper> _call_helper;
    bool _is_returning_random_value;
};
} // namespace starrocks
