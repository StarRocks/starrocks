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

#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/ObjectCache.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr_context.h"
#include "exprs/jit/ir_helper.h"
#include "util/lru_cache.h"

namespace starrocks {

class JitObjectCache : public llvm::ObjectCache {
public:
    explicit JitObjectCache(const std::string& expr_name, Cache* cache);

    ~JitObjectCache() override = default;

    void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj) override;

    std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M) override;

    Status register_func(JITScalarFunction func);

    const std::string& get_func_name() const { return _cache_key; };

    void set_cache(std::shared_ptr<llvm::MemoryBuffer> obj_code, JITScalarFunction func) {
        _obj_code = std::move(obj_code);
        _func = func;
    }
    JITScalarFunction get_func() const {
        return _func;
    }

private:
    const std::string _cache_key;
    JITScalarFunction _func = nullptr;
    Cache* _lru_cache = nullptr;
    std::shared_ptr<llvm::MemoryBuffer> _obj_code = nullptr;
};

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
    static Status compile_scalar_function(ExprContext* context, JitObjectCache* obj, Expr* expr);

    bool lookup_function(JitObjectCache* obj);
    // used in UT
    Cache* get_func_cache() const { return _func_cache; }

    static Status generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr);

    Cache* get_lru_cache() { return _func_cache; }

private:
    bool _initialized = false;
    bool _support_jit = false;
    Cache* _func_cache;
};

class Engine {
public:
    ~Engine();
    llvm::LLVMContext* context() { return context_.get(); }
    llvm::IRBuilder<>* ir_builder() { return ir_builder_.get(); }
    llvm::Module* module();

    static StatusOr<std::unique_ptr<Engine>> create(bool cached, std::reference_wrapper<JitObjectCache> object_cache);

    Status optimize_and_finalize_module();

    StatusOr<JITScalarFunction> get_compiled_func(const std::string& function);

    const std::string& ir();

private:
    Engine(std::unique_ptr<llvm::orc::LLJIT> lljit, std::unique_ptr<llvm::TargetMachine> target_machine, bool cached);

    std::unique_ptr<llvm::LLVMContext> context_;
    std::unique_ptr<llvm::orc::LLJIT> lljit_;
    std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
    std::unique_ptr<llvm::Module> module_;

    bool optimize_ = true;
    bool module_finalized_ = false;
    bool cached_;
    bool functions_loaded_ = false;
    std::string module_ir_;
    std::unique_ptr<llvm::TargetMachine> target_machine_;
};

} // namespace starrocks
