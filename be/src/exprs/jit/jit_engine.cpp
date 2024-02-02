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

#include "exprs/jit/jit_engine.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>
#include <mutex>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/MC/SubtargetFeature.h"
#include "llvm/MC/TargetRegistry.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/TargetSelect.h"

namespace starrocks {

Status JITEngine::init() {
    if (_initialized) {
        return Status::OK();
    }

    // Initialize LLVM targets and data layout.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    _target_machine = std::unique_ptr<llvm::TargetMachine>(llvm::EngineBuilder().selectTarget());
    if (_target_machine == nullptr) {
        LOG(ERROR) << "JIT: Failed to select target machine";
        return Status::JitCompileError("Failed to select target machine");
    }
    _data_layout = std::make_unique<const llvm::DataLayout>(_target_machine->createDataLayout());

    // Create a JIT engine instance.
    auto jit = llvm::orc::LLJITBuilder().create();
    if (!jit) {
        std::string error_message;
        llvm::handleAllErrors(jit.takeError(), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        error_message = "JIT: Failed to create LLJIT instance" + error_message;
        LOG(ERROR) << error_message;
        return Status::JitCompileError(error_message);
    }
    _jit = std::move(jit.get());

    // Initialize pass manager for IR optimization.
    // TODO(Yueyang): check optimization level.
    // TODO(Yueyang): check if we need to add more optimization passes.
    _pass_manager_builder.OptLevel = 3;
    _pass_manager_builder.SLPVectorize = true;
    _pass_manager_builder.LoopVectorize = true;
    _pass_manager_builder.VerifyInput = true;
    _pass_manager_builder.VerifyOutput = true;
    _pass_manager_builder.populateModulePassManager(_pass_manager);

    _initialized = true;
    _support_jit = true;
    return Status::OK();
}

StatusOr<JITScalarFunction> JITEngine::compile_scalar_function(ExprContext* context, Expr* expr,
                                                               const std::vector<Expr*>& uncompilable_exprs) {
    auto* instance = JITEngine::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    // TODO(Yueyang): optimize module name.
    auto expr_name = expr->debug_string();

    auto compiled_function = (JITScalarFunction)instance->lookup_function_with_lock(expr_name, false);
    if (compiled_function != nullptr) {
        return compiled_function;
    }

    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module = std::make_unique<llvm::Module>(expr_name, *llvm_context);
    instance->setup_module(module.get());

    // Generate scalar function IR.
    RETURN_IF_ERROR(generate_scalar_function_ir(context, *module, expr, uncompilable_exprs));
    std::string error;
    llvm::raw_string_ostream errs(error);
    if (llvm::verifyModule(*module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to generate scalar function IR, errors: {}", errs.str()));
    }

    // Optimize module.
    instance->optimize_module(module.get());
    if (llvm::verifyModule(*module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to optimize scalar function IR, errors: {}", errs.str()));
    }

    // Compile module, return function pointer (maybe nullptr).
    compiled_function = reinterpret_cast<JITScalarFunction>(
            instance->compile_module(std::move(module), std::move(llvm_context), expr_name));

    if (compiled_function == nullptr) {
        return Status::JitCompileError("Failed to compile scalar function");
    }

    // TODO(Yueyang): add to cache.

    // Return function pointer.
    return compiled_function;
}

Status JITEngine::generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr,
                                              const std::vector<Expr*>& uncompilable_exprs) {
    llvm::IRBuilder<> b(module.getContext());
    size_t args_size = uncompilable_exprs.size();

    /// Create function type.
    auto* size_type = b.getInt64Ty();
    // Same with JITColumn.
    auto* data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    // Same with JITScalarFunction.
    auto* func_type = llvm::FunctionType::get(b.getVoidTy(), {size_type, data_type->getPointerTo()}, false);

    /// Create function in module.
    // Pseudo code: void "expr->debug_string()"(int64_t rows_count, JITColumn* columns);
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expr->debug_string(), module);
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

        const auto& type = i == args_size ? expr->type() : uncompilable_exprs[i]->type();
        columns[i].values = b.CreateExtractValue(jit_column, {0});
        columns[i].null_flags = b.CreateExtractValue(jit_column, {1});
        ASSIGN_OR_RETURN(columns[i].value_type, IRHelper::logical_to_ir_type(b, type.type));
    }

    /// Initialize loop.
    auto* end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto* loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);

    b.CreateBr(loop);
    b.SetInsertPoint(loop);
    /// Loop.
    // Pseudo code: for (int64_t counter = 0; counter < rows_count; counter++)
    auto* counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    JITContext jc = {counter_phi, columns, module, b, 0};
    ASSIGN_OR_RETURN(auto result, expr->generate_ir_impl(context, &jc))

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

Status JITEngine::remove_function(const std::string& expr_name) {
    auto* instance = JITEngine::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    return instance->remove_module(expr_name);
}

void JITEngine::setup_module(llvm::Module* module) const {
    // Set the data layout of the LLVM module, telling the compiler how to arrange data.
    module->setDataLayout(*_data_layout);
    // Set the target triple of the LLVM module to specify the architecture for which the code should be generated.
    module->setTargetTriple(_target_machine->getTargetTriple().getTriple());
}

void JITEngine::optimize_module(llvm::Module* module) {
    // Create a function pass manager.
    llvm::legacy::FunctionPassManager fpm(module);

    // TODO(Yueyang): check if we need to add more passes.
    // fpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
    // mpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));

    _pass_manager_builder.populateFunctionPassManager(fpm);

    fpm.doInitialization();
    for (auto& function : *module) {
        fpm.run(function);
    }
    fpm.doFinalization();
}

JITScalarFunction JITEngine::compile_module(std::unique_ptr<llvm::Module> module,
                                            std::unique_ptr<llvm::LLVMContext> context, const std::string& expr_name) {
    // print_module(*module);
    std::lock_guard<std::mutex> lock(_mutex);
    auto func = lookup_function(expr_name, false);
    // The function has already been compiled.
    if (func != nullptr) {
        return func;
    }

    // Create a resource tracker for the module, which will be used to remove the module from the JIT engine.
    auto resource_tracker = _jit->getMainJITDylib().createResourceTracker();
    auto error =
            _jit->addIRModule(resource_tracker, llvm::orc::ThreadSafeModule(std::move(module), std::move(context)));
    if (UNLIKELY(error)) {
        std::string error_message;
        llvm::handleAllErrors(std::move(error), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        LOG(ERROR) << "JIT: Failed to add IR module to JIT: " << error_message;
        return nullptr;
    }

    _resource_tracker_map[expr_name] = std::move(resource_tracker);
    _resource_ref_count_map[expr_name] = 0;
    // Lookup the function in the JIT engine, this will trigger the compilation.
    return lookup_function(expr_name, true);
}

Status JITEngine::remove_module(const std::string& expr_name) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_resource_ref_count_map.contains(expr_name)) {
        DCHECK(false) << "Remove a non-existing jit module";
        return Status::RuntimeError("Remove a non-existing jit module");
    }
    if (_resource_ref_count_map[expr_name].fetch_sub(1) > 1) {
        return Status::OK();
    }
    auto it = _resource_tracker_map.find(expr_name);
    if (it == _resource_tracker_map.end()) {
        return Status::OK();
    }

    auto error = it->second->remove();
    _resource_tracker_map.erase(it);
    _resource_ref_count_map.erase(expr_name);
    if (UNLIKELY(error)) {
        std::string error_message;
        llvm::handleAllErrors(std::move(error), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        LOG(ERROR) << "JIT: Failed to remove IR module from JIT: " << error_message;
        return Status::JitCompileError("Failed to remove IR module from JIT");
    }
    return Status::OK();
}

void JITEngine::print_module(const llvm::Module& module) {
    std::string str;
    llvm::raw_string_ostream os(str);

    module.print(os, nullptr);
    os.flush();
    LOG(INFO) << "JIT: Generated IR:\n" << str;
}

JITScalarFunction JITEngine::lookup_function(const std::string& expr_name, bool must_exist) {
    auto addr = _jit->lookup(expr_name);
    if (UNLIKELY(!addr || UNLIKELY(addr->isNull()))) {
        if (!must_exist) {
            return nullptr;
        }

        std::string error_message = "address is null";
        if (!addr) {
            handleAllErrors(addr.takeError(), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        }
        VLOG_ROW << "Failed to find jit function: " << error_message;
        return nullptr;
    }
    _resource_ref_count_map[expr_name].fetch_add(1);
    return addr->toPtr<JITScalarFunction>();
}

JITScalarFunction JITEngine::lookup_function_with_lock(const std::string& expr_name, bool must_exist) {
    std::lock_guard<std::mutex> lock(_mutex);
    return lookup_function(expr_name, must_exist);
}

} // namespace starrocks