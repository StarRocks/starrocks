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
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr.h"
#include "exprs/function_helper.h"
#include "exprs/jit/ir_helper.h"
#include "exprs/jit/jit_wrapper.h"
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
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expr->debug_string(), module);
    auto* func_args = func->args().begin();
    llvm::Value* rows_count_arg = func_args++;
    llvm::Value* columns_arg = func_args++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    // Extract data and null data from function input parameters.
    std::vector<LLVMColumn> columns(args_size + 1);
    // i == args_size is the result column.
    for (size_t i = 0; i < args_size + 1; ++i) {
        auto* jit_column = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i));

        const auto& type = i == args_size ? expr->type() : input_exprs[i]->type();
        auto status = IRHelper::logical_to_ir_type(b, type.type);
        if (!status.ok()) {
            return status.status();
        }
        columns[i].value_type = status.value();

        columns[i].values = b.CreateExtractValue(jit_column, {0});
        columns[i].null_flags = b.CreateExtractValue(jit_column, {1});
        columns[i].nullable = b.CreateICmpNE(columns[i].null_flags, llvm::ConstantPointerNull::get(b.getInt8PtrTy()));
    }

    /// Initialize loop.
    auto* end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto* loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    // If rows_count == 0, jump to end.
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    /// Loop.

    auto* counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    /// Initialize column row values.

    std::vector<LLVMDatum> datums;
    datums.reserve(args_size);

    for (size_t i = 0; i < args_size; ++i) {
        auto& column = columns[i];

        LLVMDatum datum(b);
        datum.value =
                b.CreateLoad(column.value_type, b.CreateInBoundsGEP(column.value_type, column.values, counter_phi));

        if (!expr->is_nullable()) {
            continue;
        }
        if (input_exprs[i]->is_nullable()) {
            // TODO(Yueyang): check if need to trans null to Int1Ty.
            // TODO(Yueyang): nullable expr will evaluate non-nullable result column.
            datum.null_flag =
                    b.CreateLoad(b.getInt8Ty(), b.CreateInBoundsGEP(b.getInt8Ty(), column.null_flags, counter_phi));
        }

        datums.emplace_back(datum);
    }

    // Evaluate expr.
    ASSIGN_OR_RETURN(auto result, generate_exprs_ir(context, module, b, expr, datums));

    // assert(result.datum->getType() == columns.back().datum_type);
    b.CreateStore(result.value, b.CreateInBoundsGEP(columns.back().value_type, columns.back().values, counter_phi));
    b.CreateStore(result.null_flag, b.CreateInBoundsGEP(b.getInt8Ty(), columns.back().null_flags, counter_phi));

    /// End of loop.
    auto* current_block = b.GetInsertBlock();
    auto* incremeted_counter = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(incremeted_counter, current_block);

    b.CreateCondBr(b.CreateICmpEQ(incremeted_counter, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
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

            ASSIGN_OR_RETURN(auto ir, expr->generate_ir(context, module, b, datums));

            intermediate[i] = ir;
        }
    }

    return Status::OK();
}

// This is the evaluate procss.
Status JITFunction::llvm_function(FunctionContext* context, const Columns& columns) {
    // Prepare input columns of jit function.
    std::vector<JITColumn> jit_columns;
    jit_columns.reserve(columns.size());
    // Extract data and null_data pointers from columns to generate JIT columns.
    for (const auto& column : columns) {
        if (column->is_nullable()) {
            const auto& nullable_column = down_cast<NullableColumn*>(column.get());
            const auto& data_column = nullable_column->data_column();
            const auto& null_column = nullable_column->null_column();
            jit_columns.emplace_back(JITColumn{reinterpret_cast<const int8_t*>(data_column->raw_data()),
                                               reinterpret_cast<const int8_t*>(null_column->raw_data())});
        } else {
            jit_columns.emplace_back(JITColumn{reinterpret_cast<const int8_t*>(column->raw_data()), nullptr});
        }
    }

    // Get compiled function.
    auto state = reinterpret_cast<JITFunctionState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    // Evaluate.
    LOG(INFO) << "JIT Eval" << columns.back()->size();
    state->_function(columns.back()->size(), jit_columns.data());
    return Status::OK();
}

} // namespace starrocks
