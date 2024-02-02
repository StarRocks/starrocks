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
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

class VectorizedLiteral final : public Expr {
public:
    VectorizedLiteral(const TExprNode& node);
    VectorizedLiteral(ColumnPtr&& value, const TypeDescriptor& type);

    ~VectorizedLiteral() override;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedLiteral(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override;

    bool is_compilable() const override;

    std::string jit_func_name() const override;

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) override;

    std::string debug_string() const override;

private:
    // @IMPORTANT: BinaryColumnPtr's build_slice will cause multi-thread(OLAP_SCANNER) crash
    ColumnPtr _value;
};
} // namespace starrocks
