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

#include "exprs/jit/jit_functions.h"

#include <glog/logging.h>
#include <llvm/IR/Value.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"
#include "exprs/function_helper.h"
#include "exprs/jit/ir_helper.h"
#include "exprs/jit/jit_engine.h"
#include "llvm/IR/Type.h"
#include "types/logical_type.h"

namespace starrocks {

Status JITFunction::generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr) {
    llvm::IRBuilder<> b(module.getContext());

    std::vector<Expr*> input_exprs;
    expr->get_uncompilable_exprs(input_exprs);
    size_t args_size = input_exprs.size();

    /// Create function type.
    auto* size_type = b.getInt64Ty();
    // Same with JITColumn.
    auto* data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    // Same with JITScalarFunction.
    auto* func_type = llvm::FunctionType::get(b.getVoidTy(), {size_type, data_type->getPointerTo()}, false);

    /// Create function in module.
    // Pseudo code: void "expr->jit_func_name()"(int64_t rows_count, JITColumn* columns);
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expr->jit_func_name(), module);
    auto* func_args = func->args().begin();
    llvm::Value* rows_count_arg = func_args++;
    llvm::Value* columns_arg = func_args++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    // Extract data and null data from function input parameters.
    std::vector<LLVMColumn> columns(args_size + 1);

    for (size_t i = 0; i < args_size + 1; ++i) {
        // i == args_size is the result column.
        auto* jit_column = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i));

        const auto& type = i == args_size ? expr->type() : input_exprs[i]->type();
        columns[i].values = b.CreateExtractValue(jit_column, {0});
        columns[i].null_flags = b.CreateExtractValue(jit_column, {1});
        ASSIGN_OR_RETURN(columns[i].value_type, IRHelper::logical_to_ir_type(b, type.type));
    }

    /// Initialize loop.
    auto* end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto* loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    // If rows_count == 0, jump to end.
    // Pseudo code: if (rows_count == 0) goto end;
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0, true)), end, loop);

    b.SetInsertPoint(loop);

    /// Loop.
    // Pseudo code: for (int64_t counter = 0; counter < rows_count; counter++)
    auto* counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    /// Initialize column row values.

    std::vector<LLVMDatum> datums;
    datums.reserve(args_size);

    for (size_t i = 0; i < args_size; ++i) {
        auto& column = columns[i];
        // Pseudo code: auto* datum_n = is_constant_n ? values_n[0] : values_n[counter];
        LLVMDatum datum(b);
        datum.value =
                b.CreateLoad(column.value_type, b.CreateInBoundsGEP(column.value_type, column.values, counter_phi));
        if (input_exprs[i]->is_nullable()) {
            datum.null_flag =
                    b.CreateLoad(b.getInt8Ty(), b.CreateInBoundsGEP(b.getInt8Ty(), column.null_flags, counter_phi));
        }
        datums.emplace_back(datum);
    }

    // Generate evaluate expr.
    // Take a + b + c as an example:
    // Pseudo code:
    // result_value = datum_a + datum_b + datum_c;
    // result_null_flag = is_null_a | is_null_b | is_null_c;
    ASSIGN_OR_RETURN(auto result, generate_exprs_ir(context, module, b, expr, datums));

    // Pseudo code:
    // values_last[counter] = result_value;
    // null_flags_last[counter] = result_null_flag;
    b.CreateStore(result.value, b.CreateInBoundsGEP(columns.back().value_type, columns.back().values, counter_phi));
    if (expr->is_nullable()) {
        b.CreateStore(result.null_flag, b.CreateInBoundsGEP(b.getInt8Ty(), columns.back().null_flags, counter_phi));
    }

    /// End of loop.
    auto* current_block = b.GetInsertBlock();
    // Pseudo code: counter++;
    auto* incremeted_counter = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(incremeted_counter, current_block);

    // Pseudo code: if (counter == rows_count) goto end;
    b.CreateCondBr(b.CreateICmpEQ(incremeted_counter, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    // Pseudo code: return;
    b.CreateRetVoid();

    return Status::OK();
}

StatusOr<LLVMDatum> JITFunction::generate_exprs_ir(ExprContext* context, const llvm::Module& module,
                                                   llvm::IRBuilder<>& b, Expr* expr,
                                                   const std::vector<LLVMDatum>& datums) {
    // Convert the expr from tree to sequence using post-order traversal.
    std::vector<Expr*> post_order_exprs;
    expr->get_jit_exprs(post_order_exprs);

    // Generate IR for intermediate results and final results.
    std::vector<LLVMDatum> intermediate(post_order_exprs.size(), LLVMDatum(b));
    size_t input_index = 0;
    for (size_t i = 0; i < post_order_exprs.size(); ++i) {
        auto* expr = post_order_exprs[i];
        if (!expr->is_compilable()) {
            // Input column.
            intermediate[i] = datums[input_index++];
        } else {
            // Regular expr and literal.
            std::vector<LLVMDatum> args;
            args.reserve(expr->get_num_children());

            int offset = i;
            for (const auto& child : expr->children()) {
                offset -= child->get_num_jit_children();
            }

            for (const auto& child : expr->children()) {
                offset += child->get_num_jit_children();
                args.emplace_back(intermediate[offset - 1]);
            }

            ASSIGN_OR_RETURN(intermediate[i], expr->generate_ir(context, module, b, args));
        }
    }

    return intermediate.back();
}

// This is the evaluate procss.
Status JITFunction::llvm_function(JITScalarFunction jit_function, const Columns& columns) {
    // Prepare input columns of jit function.
    std::vector<JITColumn> jit_columns;
    jit_columns.reserve(columns.size());
    // Extract data and null_data pointers from columns to generate JIT columns.
    for (const auto& column : columns) {
        DCHECK(!column->is_constant());
        auto [un_col, un_col_null] = ColumnHelper::unpack_nullable_column(column);
        auto data_col_ptr = reinterpret_cast<const int8_t*>(un_col->raw_data());
        const int8_t* null_flags_ptr = nullptr;
        if (un_col_null != nullptr) {
            null_flags_ptr = reinterpret_cast<const int8_t*>(un_col_null->raw_data());
        }
        jit_columns.emplace_back(JITColumn{data_col_ptr, null_flags_ptr});
    }

    // Evaluate.
    jit_function(columns.back()->size(), jit_columns.data());
    return Status::OK();
}

} // namespace starrocks
