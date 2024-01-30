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

#include <memory>
#include <mutex>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/jit/jit_functions.h"
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
#include "util/defer_op.h"

namespace starrocks {

struct JitCacheEntry {
    JitCacheEntry(llvm::orc::ResourceTrackerSP t, JITScalarFunction f) : tracker(std::move(t)), func(std::move(f)) {}
    llvm::orc::ResourceTrackerSP tracker;
    JITScalarFunction func;
};

JITEngine::~JITEngine() {
    delete _func_cache;
}

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
    //TODO(fzh): trace per function by memory usage
#if BE_TEST
    _func_cache = new_lru_cache(32); // 1 capacity per cache of 32 shards in LRU cache
#else
    auto jit_lru_cache_size = config::jit_lru_cache_size;
    if (jit_lru_cache_size < 0) {
        jit_lru_cache_size = 3200; // 100 capacity per cache of 32 shards in LRU cache
    }
    _func_cache = new_lru_cache(jit_lru_cache_size);
#endif
    return Status::OK();
}

StatusOr<std::pair<JITScalarFunction, std::function<void()>>> JITEngine::compile_scalar_function(ExprContext* context,
                                                                                                 Expr* expr) {
    auto* instance = JITEngine::get_instance();
    if (!instance->initialized()) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    // TODO(Yueyang): optimize module name.
    auto expr_name = expr->jit_func_name();

    auto compiled_function = instance->lookup_function(expr_name);
    if (compiled_function.first != nullptr) {
        return compiled_function;
    }

    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module = std::make_unique<llvm::Module>(expr_name, *llvm_context);
    instance->setup_module(module.get());

    // Generate scalar function IR.
    RETURN_IF_ERROR(JITFunction::generate_scalar_function_ir(context, *module, expr));
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
    compiled_function = instance->compile_module(std::move(module), std::move(llvm_context), expr_name);

    if (compiled_function.first == nullptr) {
        return Status::JitCompileError("Failed to compile scalar function");
    }

    // Return function pointer.
    return compiled_function;
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

std::pair<JITScalarFunction, std::function<void()>> JITEngine::compile_module(
        std::unique_ptr<llvm::Module> module, std::unique_ptr<llvm::LLVMContext> context,
        const std::string& expr_name) {
    // print_module(*module);
    auto func = lookup_function(expr_name);
    // The function has already been compiled.
    if (func.first != nullptr) {
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
        return std::make_pair(nullptr, nullptr);
    }

    // insert LRU cache
    auto addr = _jit->lookup(expr_name);
    if (UNLIKELY(!addr || UNLIKELY(addr->isNull()))) {
        std::string error_message = "address is null";
        if (!addr) {
            handleAllErrors(addr.takeError(), [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
        }
        VLOG_ROW << "Failed to find jit function: " << error_message;
        return std::make_pair(nullptr, nullptr);
    }

    auto* cache = new JitCacheEntry(resource_tracker, addr->toPtr<JITScalarFunction>());

    auto* handle = _func_cache->insert(expr_name, (void*)cache, 1, [](const CacheKey& key, void* value) {
        auto* entry = ((JitCacheEntry*)value);
        auto error = entry->tracker->remove();
        if (UNLIKELY(error)) {
            std::string error_message;
            llvm::handleAllErrors(std::move(error),
                                  [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
            LOG(ERROR) << "JIT: Failed to remove IR module from JIT: " << error_message;
        }
        delete entry;
    });
    if (handle == nullptr) {
        delete cache;
        LOG(ERROR) << "JIT: Failed to insert jit func to LRU cache";
        return std::make_pair(nullptr, nullptr);
    } else {
        return std::make_pair(cache->func, [this, handle]() { _func_cache->release(handle); });
    }
}

void JITEngine::print_module(const llvm::Module& module) {
    std::string str;
    llvm::raw_string_ostream os(str);

    module.print(os, nullptr);
    os.flush();
    LOG(INFO) << "JIT: Generated IR:\n" << str;
}

std::pair<JITScalarFunction, std::function<void()>> JITEngine::lookup_function(const std::string& expr_name) {
    auto* handle = _func_cache->lookup(expr_name);
    if (handle == nullptr) {
        return std::make_pair(nullptr, nullptr);
    }

    return std::make_pair(((JitCacheEntry*)_func_cache->value(handle))->func,
                          [this, handle]() { this->_func_cache->release(handle); });
}

} // namespace starrocks