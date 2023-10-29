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

#include "exprs/jit/jit_wrapper.h"

#include <glog/logging.h>

#include <cassert>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/jit/jit_functions.h"
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
#include "util/defer_op.h"

namespace starrocks {

Status JITWapper::init() {
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
        LOG(ERROR) << "JIT: Failed to create LLJIT instance" << error_message;
        return Status::JitCompileError("Failed to create LLJIT instance");
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
    return Status::OK();
}

StatusOr<JITScalarFunction> JITWapper::compile_scalar_function(ExprContext* context, Expr* expr) {
    auto* instance = JITWapper::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    // TODO(Yueyang): optimize module name.
    auto expr_name = expr->debug_string();

    // TODO(Yueyang): loopup in cache.

    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module = std::make_unique<llvm::Module>(expr_name, *llvm_context);
    instance->setup_module(module.get());
    // Generate scalar function IR.
    RETURN_IF_ERROR(JITFunction::generate_scalar_function_ir(context, *module, expr));
    if (llvm::verifyModule(*module)) {
        return Status::JitCompileError("Failed to generate scalar function IR");
    }

    // Optimize module.
    instance->optimize_module(module.get());
    if (llvm::verifyModule(*module)) {
        return Status::JitCompileError("Failed to optimize scalar function IR");
    }

    // Compile module, return function pointer (maybe nullptr).
    JITScalarFunction compiled_function = reinterpret_cast<JITScalarFunction>(
            instance->compile_module(std::move(module), std::move(llvm_context), expr_name));

    if (compiled_function == nullptr) {
        return Status::JitCompileError("Failed to compile scalar function");
    }

    // TODO(Yueyang): add to cache.

    // Return function pointer.
    return compiled_function;
}

Status JITWapper::remove_function(const std::string& expr_name) {
    auto* instance = JITWapper::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    return instance->remove_module(expr_name);
}

void JITWapper::setup_module(llvm::Module* module) const {
    // Set the data layout of the LLVM module, telling the compiler how to arrange data.
    module->setDataLayout(*_data_layout);
    // Set the target triple of the LLVM module to specify the architecture for which the code should be generated.
    module->setTargetTriple(_target_machine->getTargetTriple().getTriple());
}

void JITWapper::optimize_module(llvm::Module* module) {
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

void* JITWapper::compile_module(std::unique_ptr<llvm::Module> module, std::unique_ptr<llvm::LLVMContext> context,
                                const std::string& expr_name) {
    // print_module(*module);

    // Create a resource tracker for the module, which will be used to remove the module from the JIT engine.
    auto resource_tracker = _jit->getMainJITDylib().createResourceTracker();
    auto error =
            _jit->addIRModule(resource_tracker, llvm::orc::ThreadSafeModule(std::move(module), std::move(context)));
    if (UNLIKELY(error)) {
        auto* func = lookup_function(expr_name);
        // The function has already been compiled.
        if (func != nullptr) {
            LOG(WARNING) << "JIT: Failed to add IR module to JIT, module already exist";
            return func;
        }

        std::string error_message;
        llvm::handleAllErrors(std::move(error), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        LOG(ERROR) << "JIT: Failed to add IR module to JIT: " << error_message;
        return nullptr;
    }

    _resource_tracker_map[expr_name] = std::move(resource_tracker);
    // Lookup the function in the JIT engine, this will trigger the compilation.
    return lookup_function(expr_name);
}

Status JITWapper::remove_module(const std::string& expr_name) {
    auto it = _resource_tracker_map.find(expr_name);
    if (it == _resource_tracker_map.end()) {
        return Status::OK();
    }

    auto error = it->second->remove();
    if (UNLIKELY(error)) {
        std::string error_message;
        llvm::handleAllErrors(std::move(error), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        LOG(ERROR) << "JIT: Failed to remove IR module from JIT: " << error_message;
        return Status::JitCompileError("Failed to remove IR module from JIT");
    } else {
        _resource_tracker_map.erase(it);
    }

    return Status::OK();
}

void JITWapper::print_module(const llvm::Module& module) {
    std::string str;
    llvm::raw_string_ostream os(str);

    module.print(os, nullptr);
    os.flush();
    LOG(INFO) << "JIT: Generated IR:\n" << str;
}

void* JITWapper::lookup_function(const std::string& expr_name) {
    auto addr = _jit->lookup(expr_name);
    if (UNLIKELY(!addr || UNLIKELY(addr->isNull()))) {
        std::string error_message = "address is null";
        if (!addr) {
            handleAllErrors(addr.takeError(), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        }
        LOG(ERROR) << "Failed to find jit function: " << error_message;
        return nullptr;
    }
    return reinterpret_cast<void*>(addr->toPtr<JITScalarFunction>());
}

} // namespace starrocks