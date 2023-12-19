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

#include "exprs/jit/jit_compilable_expr.h"

namespace starrocks {

StatusOr<LLVMDatum> JITCompilableExpr::generate_ir(ExprContext* context, const llvm::Module& module,
                                                   llvm::IRBuilder<>& b, const std::vector<LLVMDatum>& datums) const {
    if (!is_compilable()) {
        return Status::NotSupported("JIT expr not supported");
    }

    ASSIGN_OR_RETURN(auto datum, generate_ir_impl(context, module, b, datums))
    // Unoin null.
    if (this->is_nullable()) {
        // TODO(Yueyang): Check this.
        for (auto& input : datums) {
            datum.null_flag = b.CreateOr(datum.null_flag, input.null_flag);
        }
    }
    return datum;
}

StatusOr<LLVMDatum> JITCompilableExpr::generate_ir_impl(ExprContext* context, const llvm::Module& module,
                                                        llvm::IRBuilder<>& b,
                                                        const std::vector<LLVMDatum>& datums) const {
    return Status::NotSupported("JIT expr not supported");
}

} // namespace starrocks