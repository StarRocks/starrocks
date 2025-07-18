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

class CustomizedInProcessMemoryManager;
using MemMgrPtr = std::shared_ptr<CustomizedInProcessMemoryManager>;
class JITObjectCache;
using JITObjectCachePtr = std::unique_ptr<JITObjectCache>;
class JITCallable;
using JITCallablePtr = std::shared_ptr<JITCallable>;
class JITCallableCache;
using JITCallableCachePtr = std::unique_ptr<JITCallableCache>;

class JITCallable {
public:
    JITCallable(MemMgrPtr&& mem_mgr, JITScalarFunction&& func) : _mem_mgr(std::move(mem_mgr)), _func(std::move(func)) {
        DCHECK(_mem_mgr != nullptr);
        DCHECK(_func != nullptr);
    }

    ~JITCallable() = default;

    JITCallable(const JITCallable&) = delete;

    JITCallable(JITCallable&& that) : _mem_mgr(std::move(that._mem_mgr)), _func(std::move(that._func)) {
        that._func = nullptr; // Prevent double free
    }

    JITCallable& operator=(JITCallable&& that) {
        if (this != &that) {
            _mem_mgr = std::move(that._mem_mgr);
            _func = std::move(that._func);
            that._func = nullptr; // Prevent double free
        }
        return *this;
    }

    JITCallable& operator=(const JITCallable&) = delete;

    inline JITScalarFunction get_func() const { return _func; }

    size_t getSize();

    void operator()(int64_t num_rows, JITColumn* columns) {
        DCHECK(_func != nullptr);
        _func(num_rows, columns);
    }

private:
    MemMgrPtr _mem_mgr;
    JITScalarFunction _func;
};

// JITEngine is a wrapper of LLVM JIT engine, based on ORCv2.
class JITEngine {
public:
    JITEngine() = default;

    ~JITEngine();

    JITEngine(const JITEngine&) = delete;

    JITEngine& operator=(const JITEngine&) = delete;

    static JITEngine* get_instance();

    Status init();

    inline bool initialized() const { return _initialized; }

    inline bool support_jit() { return _support_jit; }

    StatusOr<JITCallablePtr> get_jit_callable(const std::string& expr_name, ExprContext* context, Expr* expr,
                                              const std::vector<Expr*>& uncompilable_exprs);
    JITCallablePtr lookup(const std::string& expr_name);

    Cache* get_object_cache();
    Cache* get_callable_cache();

private:
    StatusOr<JITCallablePtr> _get_jit_callable_or_create(const std::string& expr_name,
                                                         std::function<StatusOr<JITCallablePtr>()>&& callable_creator);
    StatusOr<JITCallablePtr> _compile(ExprContext* context, Expr* expr, const std::vector<Expr*>& uncompilable_exprs,
                                      const std::string& expr_name);

    bool _initialized = false;
    bool _support_jit = false;
    JITObjectCachePtr _object_cache;
    JITCallableCachePtr _callable_cache;
};

} // namespace starrocks
