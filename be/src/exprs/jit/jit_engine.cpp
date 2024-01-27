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

#include <glog/logging.h>

#include <cassert>
#include <iterator>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/jit/jit_functions.h"
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/SimpleRemoteEPC.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Verifier.h"
#include "llvm/MC/SubtargetFeature.h"
#include "llvm/MC/TargetRegistry.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include <llvm/IR/PassManager.h>
#include <llvm/Passes/PassPlugin.h>
#include <llvm/Transforms/IPO/GlobalOpt.h>
#include <llvm/Transforms/Scalar/NewGVN.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Utils/Mem2Reg.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>
#include <llvm/Transforms/Vectorize/SLPVectorizer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Vectorize.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Transforms/IPO/GlobalDCE.h>
#include <llvm/Transforms/IPO/Internalize.h>
#include <llvm/IR/PassManager.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Passes/PassPlugin.h>
#include <llvm/Transforms/IPO/GlobalOpt.h>
#include <llvm/Transforms/Scalar/NewGVN.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Utils/Mem2Reg.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>
#include <llvm/Transforms/Vectorize/SLPVectorizer.h>
#include "util/defer_op.h"


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

#define JIT_V2 1

StatusOr<JITScalarFunction> JITEngine::compile_scalar_function(ExprContext* context, Expr* expr, bool* cached) {
    auto* instance = JITEngine::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    // TODO(fzh): shouldn't include uncompilable expr's name
    auto expr_name = expr->debug_string();
    JITScalarFunction compiled_function = nullptr;
    compiled_function = (JITScalarFunction)instance->lookup_function_with_lock(expr_name);

    if (compiled_function != nullptr) {
        *cached = true;
        return compiled_function;
    }

    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module = std::make_unique<llvm::Module>(expr_name, *llvm_context);
    instance->setup_module(module.get());
    // Generate scalar function IR.
#if JIT_V2
    RETURN_IF_ERROR(generate_scalar_function_ir_v2(context, *module, expr));
#else
    RETURN_IF_ERROR(JITFunction::generate_scalar_function_ir(context, *module, expr));
#endif
    if (llvm::verifyModule(*module)) {
        return Status::JitCompileError("Failed to generate scalar function IR");
    }

    // Optimize module.
    instance->optimize_module(module.get());
    if (llvm::verifyModule(*module)) {
        return Status::JitCompileError("Failed to optimize scalar function IR");
    }

    // Compile module, return function pointer (maybe nullptr).
    compiled_function = reinterpret_cast<JITScalarFunction>(
            instance->compile_module(std::move(module), std::move(llvm_context), expr_name, cached));

    if (compiled_function == nullptr) {
        return Status::JitCompileError("Failed to compile scalar function");
    }

    // TODO(Yueyang): add to cache.

    // Return function pointer.
    return compiled_function;
}

Status JITEngine::generate_scalar_function_ir_v2(ExprContext* context, llvm::Module& module, Expr* expr) {
    llvm::IRBuilder<> b(module.getContext());

    std::vector<Expr*> input_exprs;
    expr->get_uncompilable_exprs(input_exprs); // duplicated
    size_t args_size = input_exprs.size();

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

        const auto& type = i == args_size ? expr->type() : input_exprs[i]->type();
#if JIT_DEBUG
        auto tmp = i == args_size ? expr : input_exprs[i];
        LOG(INFO) << "[JIT] " << i << " col type = " << logical_type_to_string(type.type)
                  << "  nullable = " << tmp->is_nullable() << " is const " << tmp->is_constant();
#endif
        columns[i].values = b.CreateExtractValue(jit_column, {0});
        columns[i].null_flags = b.CreateExtractValue(jit_column, {1});
        ASSIGN_OR_RETURN(columns[i].value_type, IRHelper::logical_to_ir_type(b, type.type));
    }

    /// Initialize loop.
    auto* end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto* loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    // If rows_count == 0, jump to end.
    // Pseudo code: if (rows_count == 0) goto end;
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    /// Loop.
    // Pseudo code: for (int64_t counter = 0; counter < rows_count; counter++)
    auto* counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    JITContext jc(counter_phi, columns, module, b, 0);
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
#if 0
    // Create a function pass manager.
    llvm::legacy::FunctionPassManager fpm(module);

    // TODO(Yueyang): check if we need to add more passes.
    // fpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
    //mpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));

    _pass_manager_builder.populateFunctionPassManager(fpm);

    fpm.doInitialization();
    for (auto& function : *module) {
        fpm.run(function);
    }
    fpm.doFinalization();
#else
    auto target_analysis = _target_machine->getTargetIRAnalysis();
    // Setup an optimiser pipeline
    llvm::PassBuilder pass_builder;
    llvm::LoopAnalysisManager loop_am;
    llvm::FunctionAnalysisManager function_am;
    llvm::CGSCCAnalysisManager cgscc_am;
    llvm::ModuleAnalysisManager module_am;

    function_am.registerPass([&] { return target_analysis; });

    // Register required analysis managers
    pass_builder.registerModuleAnalyses(module_am);
    pass_builder.registerCGSCCAnalyses(cgscc_am);
    pass_builder.registerFunctionAnalyses(function_am);
    pass_builder.registerLoopAnalyses(loop_am);
    pass_builder.crossRegisterProxies(loop_am, function_am, cgscc_am, module_am);

    pass_builder.registerPipelineStartEPCallback([&](llvm::ModulePassManager& module_pm,
                                                     llvm::OptimizationLevel Level) {
        module_pm.addPass(llvm::ModuleInlinerPass());

        llvm::FunctionPassManager function_pm;
        function_pm.addPass(llvm::InstCombinePass());
        function_pm.addPass(llvm::PromotePass());
        function_pm.addPass(llvm::GVNPass());
        function_pm.addPass(llvm::NewGVNPass());
        function_pm.addPass(llvm::SimplifyCFGPass());
        function_pm.addPass(llvm::LoopVectorizePass());
        function_pm.addPass(llvm::SLPVectorizerPass());
        module_pm.addPass(llvm::createModuleToFunctionPassAdaptor(std::move(function_pm)));

        module_pm.addPass(llvm::GlobalOptPass());
    });

    pass_builder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3)
            .run(*module, module_am);
#endif
}

void* JITEngine::compile_module(std::unique_ptr<llvm::Module> module, std::unique_ptr<llvm::LLVMContext> context,
                                const std::string& expr_name, bool* cached) {
    // print_module(*module);
    std::lock_guard<std::mutex> lock(_mutex);
    auto* func = lookup_function(expr_name);
    // The function has already been compiled.
    if (func != nullptr) {
        *cached = true;
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
    return lookup_function(expr_name);
}

Status JITEngine::remove_module(const std::string& expr_name) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_resource_ref_count_map.contains(expr_name)) {
        DCHECK(false);
        return Status::RuntimeError("Remove a non-existing jit module");
    }
    if (_resource_ref_count_map[expr_name].fetch_sub(1) > 0) { // TODO => 1
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

void* JITEngine::lookup_function(const std::string& expr_name) {
    auto addr = _jit->lookup(expr_name);
    if (UNLIKELY(!addr || UNLIKELY(addr->isNull()))) {
        std::string error_message = "address is null";
        if (!addr) {
            handleAllErrors(addr.takeError(), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        }
        LOG(INFO) << "Failed to find jit function: " << error_message;
        return nullptr;
    }
    _resource_ref_count_map[expr_name].fetch_add(1);
    return reinterpret_cast<void*>(addr->toPtr<JITScalarFunction>());
}

void* JITEngine::lookup_function_with_lock(const std::string& expr_name) {
    std::lock_guard<std::mutex> lock(_mutex);
    return lookup_function(expr_name);
}

} // namespace starrocks