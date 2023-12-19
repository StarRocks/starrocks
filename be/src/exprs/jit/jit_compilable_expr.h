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

#include <utility>

#include "exprs/expr.h"
#include "exprs/jit/ir_helper.h"

namespace starrocks {

class JITCompilableExpr : public Expr {
public:
    JITCompilableExpr(const Expr& expr) : Expr(expr) {}

    explicit JITCompilableExpr(TypeDescriptor type) : Expr(std::move(type)) {}

    explicit JITCompilableExpr(const TExprNode& node) : Expr(node) {}

    JITCompilableExpr(TypeDescriptor type, bool is_slotref) : Expr(std::move(type), is_slotref) {}

    JITCompilableExpr(const TExprNode& node, bool is_slotref);

    /**
     * @brief For JIT compile, generate IR code for this expr.
     * This function primarily generates generic union null code and calls the 'generate_ir_impl' method.
     */
    [[nodiscard]] virtual StatusOr<LLVMDatum> generate_ir(ExprContext* context, const llvm::Module& module,
                                                          llvm::IRBuilder<>& b,
                                                          const std::vector<LLVMDatum>& datums) const final;

    /**
     * @brief For JIT compile, generate specific evaluation IR code for this expr.
     * Its internal logic is similar to the 'evaluate_checked' function.
     */
    [[nodiscard]] virtual StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, const llvm::Module& module,
                                                               llvm::IRBuilder<>& b,
                                                               const std::vector<LLVMDatum>& datums) const;
};

} // namespace starrocks