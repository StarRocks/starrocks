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
#include "udf/udf_call_stub.h"

namespace starrocks {
struct ArrowCallStubCtx;

class ArrowFunctionCallExpr final : public Expr {
public:
    ArrowFunctionCallExpr(const TExprNode& node);
    ~ArrowFunctionCallExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrowFunctionCallExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;
    Status prepare(RuntimeState* state, ExprContext* context) override;
    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    void close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override;
    bool is_constant() const override;

private:
    std::unique_ptr<UDFCallStub> _build_stub(int32_t driver_id, FunctionContext* context);
    RuntimeState* _runtime_state = nullptr;

    std::string _lib_path;
    std::shared_ptr<ArrowCallStubCtx> _call_stub_ctx;
};
} // namespace starrocks