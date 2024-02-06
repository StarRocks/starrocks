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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr_context.h"
#include "exprs/jit/ir_helper.h"
#include "util/lru_cache.h"

namespace starrocks {

/**
 * JITEngine is a wrapper of LLVM JIT engine, based on ORCv2.
 */
class JITEngine {
public:
    JITEngine() = default;

    ~JITEngine();

    JITEngine(const JITEngine&) = delete;

    JITEngine& operator=(const JITEngine&) = delete;

    static JITEngine* get_instance() {
        static JITEngine* instance = new JITEngine();
        return instance;
    }

    Status init();

    /**
     * @brief Returns whether the JIT engine is initialized.
     */
    inline bool initialized() const { return _initialized; }

    /**
     * @brief Returns whether the JIT engine is supported.
     */
    inline bool support_jit() { return _support_jit; }

    /**
     * @brief Compile the expr into LLVM IR and return the function pointer.
     */
    static StatusOr<std::pair<JITScalarFunction, std::function<void()>>> compile_scalar_function(ExprContext* context,
                                                                                                 Expr* expr);

    std::pair<JITScalarFunction, std::function<void()>> lookup_function(const std::string& expr_name);
    // used in UT
    Cache* get_func_cache() const { return _func_cache; }

private:
    static Status generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr);
    /**
     * @brief Sets up an LLVM module by specifying its data layout and target triple.
     * The data layout guides the compiler on how to arrange data.
     * The target triple informs which code architecture the module should generate for.
     */
    void setup_module(llvm::Module* module) const;

    /**
     * @brief Optimize the module, including:
     * 1. remove unused functions
     * 2. remove unused global variables
     * 3. remove unused instructions
     */
    void optimize_module(llvm::Module* module);

    /**
     * @brief Compile the module and return the function pointer.
     */
    std::pair<JITScalarFunction, std::function<void()>> compile_module(std::unique_ptr<llvm::Module> module,
                                                                       std::unique_ptr<llvm::LLVMContext> context,
                                                                       const std::string& expr_name);

    /**
     * @brief Print the LLVM IR of the module in readable format.
     */
    static void print_module(const llvm::Module& module);

    bool _initialized = false;
    bool _support_jit = false;

    std::unique_ptr<llvm::TargetMachine> _target_machine;
    std::unique_ptr<const llvm::DataLayout> _data_layout;

    llvm::PassManagerBuilder _pass_manager_builder;
    llvm::legacy::PassManager _pass_manager;

    std::unique_ptr<llvm::orc::LLJIT> _jit;
    std::mutex _mutex;
    Cache* _func_cache;
};

} // namespace starrocks
