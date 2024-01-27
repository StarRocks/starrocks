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
#include <cstdint>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/binary_function.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"
#include "exprs/function_helper.h"
#include "exprs/jit/ir_helper.h"
#include "gen_cpp/Exprs_types.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Type.h"

namespace starrocks {
/**
 * JITColumn is a struct used to store the data and null data of a column.
 */
struct JITColumn {
    const int8_t* datums = nullptr;
    const int8_t* null_flags = nullptr;
};

/**
 * JITScalarFunction is a function pointer to a JIT compiled scalar function.
 * @param int64_t: the number of rows.
 * @param JITColumn*: the pointer to the columns.
 */
using JITScalarFunction = void (*)(int64_t, JITColumn*);

struct JITFunctionState {
    JITScalarFunction _function;
};

/**
 * @brief JITFunction is a tool class designed to generate LLVM IR for a function.
 * It also includes a function to evaluate the compiled function.
 */
class JITFunction {
public:
    /**
     * @brief Evaluate the compiled function.
     */
    static Status llvm_function(JITScalarFunction jit_function, const Columns& columns);

    /**
     * @brief Generate the LLVM IR for a scalar function.
     */
    static Status generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr);

private:
    /**
     * @brief Generate the LLVM IR to compute single-row data for the expr.
     */
    static StatusOr<LLVMDatum> generate_exprs_ir(ExprContext* context, const llvm::Module& module, llvm::IRBuilder<>& b,
                                                 Expr* expr, const std::vector<LLVMDatum>& datums);
};

} // namespace starrocks
