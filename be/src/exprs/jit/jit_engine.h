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

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exprs/expr_context.h"
#include "exprs/jit/ir_helper.h"
#include "util/lru_cache.h"

namespace starrocks {

// cache the compiled code, and register to the LRU cache
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
    JITScalarFunction get_func() const { return _func; }

    size_t get_code_size() const { return _obj_code == nullptr ? 0 : _obj_code->getBufferSize(); }

private:
    const std::string _cache_key;
    JITScalarFunction _func = nullptr;
    Cache* _lru_cache = nullptr;
    std::shared_ptr<llvm::MemoryBuffer> _obj_code = nullptr;
};

// JITEngine is a wrapper of LLVM JIT engine, based on ORCv2.
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

    inline bool initialized() const { return _initialized; }

    inline bool support_jit() { return _support_jit; }

    // Compile the expr into LLVM IR and register the compiled function into LRU cache.
    static Status compile_scalar_function(ExprContext* context, JitObjectCache* obj, Expr* expr,
                                          const std::vector<Expr*>& uncompilable_exprs);

    bool lookup_function(JitObjectCache* const obj);

    Cache* get_func_cache() const { return _func_cache; }

    static Status generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr,
                                              const std::vector<Expr*>& uncompilable_exprs, JitObjectCache* obj);

    size_t get_cache_mem_usage() const {
        DCHECK(_func_cache != nullptr);
        return _func_cache->get_memory_usage();
    }

    static std::string dump_module_ir(const llvm::Module& module);

private:
    // make an engine instance for each time of JIT
    class Engine {
    public:
        ~Engine() = default;

        Engine(const Engine&) = delete;

        Engine& operator=(const Engine&) = delete;

        llvm::Module* module() const;

        static StatusOr<std::unique_ptr<Engine>> create(std::reference_wrapper<JitObjectCache> object_cache);

        Status optimize_and_finalize_module();

        StatusOr<JITScalarFunction> get_compiled_func(const std::string& function);

    private:
        Engine(std::unique_ptr<llvm::orc::LLJIT> lljit, std::unique_ptr<llvm::TargetMachine> target_machine);

        std::unique_ptr<llvm::LLVMContext> _context;
        std::unique_ptr<llvm::orc::LLJIT> _lljit;
        std::unique_ptr<llvm::IRBuilder<>> _ir_builder;
        std::unique_ptr<llvm::Module> _module;

        bool _module_finalized = false;
        std::unique_ptr<llvm::TargetMachine> _target_machine;
    };

    bool _initialized = false;
    bool _support_jit = false;
    Cache* _func_cache;
};

} // namespace starrocks
