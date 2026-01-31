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
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/ObjectCache.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Passes/PassPlugin.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/GlobalOpt.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Scalar/NewGVN.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/Transforms/Utils/Mem2Reg.h>
#include <llvm/Transforms/Vectorize.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>
#include <llvm/Transforms/Vectorize/SLPVectorizer.h>

#include <memory>
#include <mutex>
#include <utility>

#include "base/utility/defer_op.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/mem_info.h"

namespace starrocks {

static inline Status generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr,
                                                 const std::vector<Expr*>& uncompilable_exprs,
                                                 const std::string& expr_name) {
    llvm::IRBuilder<> b(module.getContext());
    size_t args_size = uncompilable_exprs.size();

    /// Create function type.
    auto* size_type = b.getInt64Ty();
    // Same with JITColumn.
    auto* data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    // Same with JITScalarFunction.
    auto* func_type = llvm::FunctionType::get(b.getVoidTy(), {size_type, data_type->getPointerTo()}, false);

    /// Create function in module.
    // Pseudo code: void "expr->jit_expr_name"(int64_t rows_count, JITColumn* columns);
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expr_name, module);
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
    ASSIGN_OR_RETURN(auto result, expr->generate_ir(context, &jc))

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

template <typename T>
StatusOr<T> as_JIT_result(llvm::Expected<T>& expected, const std::string& error_context) {
    if (!expected) {
        return Status::JitCompileError(error_context + llvm::toString(expected.takeError()));
    }
    return std::move(expected.get());
}

StatusOr<llvm::orc::JITTargetMachineBuilder> make_target_machine_builder() {
    llvm::orc::JITTargetMachineBuilder jtmb((llvm::Triple(llvm::sys::getDefaultTargetTriple())));
    auto const opt_level = llvm::CodeGenOpt::Aggressive; // or llvm::CodeGenOpt::None;
    jtmb.setCodeGenOptLevel(opt_level);
    return jtmb;
}

void add_absolute_symbol(llvm::orc::LLJIT& lljit, const std::string& name, void* function_ptr) {
    llvm::orc::MangleAndInterner mangle(lljit.getExecutionSession(), lljit.getDataLayout());
    llvm::JITEvaluatedSymbol symbol(reinterpret_cast<llvm::JITTargetAddress>(function_ptr),
                                    llvm::JITSymbolFlags::Exported);
    auto error = lljit.getMainJITDylib().define(llvm::orc::absoluteSymbols({{mangle(name), symbol}}));
    llvm::cantFail(std::move(error));
}

// add current process symbol to dylib
void add_process_symbol(llvm::orc::LLJIT& lljit) {
    lljit.getMainJITDylib().addGenerator(llvm::cantFail(
            llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(lljit.getDataLayout().getGlobalPrefix())));
    // the `atexit` symbol cannot be found for ASAN
#ifdef ADDRESS_SANITIZER
    if (!lljit.lookup("atexit")) {
        add_absolute_symbol(lljit, "atexit", reinterpret_cast<void*>(atexit));
    }
#endif
}

class CustomizedInProcessMemoryManager : public llvm::jitlink::InProcessMemoryManager {
public:
    static std::unique_ptr<CustomizedInProcessMemoryManager> create() {
        if (auto PageSize = llvm::sys::Process::getPageSize()) {
            return std::make_unique<CustomizedInProcessMemoryManager>(*PageSize);
        } else {
            LOG(FATAL) << "Failed to get page size: " << toString(PageSize.takeError());
            return nullptr;
        }
    }

    using InProcessMemoryManager::allocate;
    using InProcessMemoryManager::deallocate;
    using InProcessMemoryManager::InProcessMemoryManager;

    void deallocate(std::vector<FinalizedAlloc> Allocs, OnDeallocatedFunction OnDeallocated) override {
        for (auto& alloc : Allocs) {
            auto* mb = alloc.getAddress().toPtr<llvm::sys::MemoryBlock*>();
            _standardSegmentsList.push_back(*mb);
        }
        InProcessMemoryManager::deallocate(std::move(Allocs), std::move(OnDeallocated));
    }

    ~CustomizedInProcessMemoryManager() override { _releaseAllExecutableMemory(); }

    size_t getSize() {
        size_t size = 0;
        for (const auto& segment : _standardSegmentsList) {
            size += segment.allocatedSize();
        }
        return size;
    }

private:
    void _releaseAllExecutableMemory() {
        for (auto& StandardSegments : _standardSegmentsList) {
            if (auto EC = llvm::sys::Memory::releaseMappedMemory(StandardSegments)) {
                LOG(FATAL) << "Failed to release memory: " << StandardSegments.base()
                           << ", size: " << StandardSegments.allocatedSize()
                           << ", error: " << toString(llvm::errorCodeToError(EC));
            }
        }
        _standardSegmentsList.clear();
    }
    std::vector<llvm::sys::MemoryBlock> _standardSegmentsList;
};

Status use_JIT_link(llvm::orc::LLJITBuilder& jit_builder, llvm::jitlink::JITLinkMemoryManager& memory_manager) {
    jit_builder.setObjectLinkingLayerCreator([&](llvm::orc::ExecutionSession& ES, const llvm::Triple& TT) {
        return std::make_unique<llvm::orc::ObjectLinkingLayer>(ES, memory_manager);
    });
    return Status::OK();
}

StatusOr<std::unique_ptr<llvm::orc::LLJIT>> build_JIT(llvm::orc::JITTargetMachineBuilder jtmb,
                                                      JITObjectCache& object_cache,
                                                      llvm::jitlink::JITLinkMemoryManager& memory_manager) {
    llvm::orc::LLJITBuilder jit_builder;
    RETURN_IF_ERROR(use_JIT_link(jit_builder, memory_manager));
    jit_builder.setJITTargetMachineBuilder(std::move(jtmb));
    jit_builder.setCompileFunctionCreator(
            [&object_cache](llvm::orc::JITTargetMachineBuilder JTMB)
                    -> llvm::Expected<std::unique_ptr<llvm::orc::IRCompileLayer::IRCompiler>> {
                auto target_machine = JTMB.createTargetMachine();
                if (!target_machine) {
                    return target_machine.takeError();
                }
                // after compilation, the object code will be stored into the given object cache
                return std::make_unique<llvm::orc::TMOwningSimpleCompiler>(std::move(*target_machine), &object_cache);
            });

    auto maybe_jit = jit_builder.create();
    ASSIGN_OR_RETURN(auto jit, as_JIT_result(maybe_jit, "Could not create LLJIT instance: "));
    add_process_symbol(*jit);
    return std::move(jit);
}

static inline void optimize_module(llvm::Module& module, llvm::TargetIRAnalysis target_analysis) {
    llvm::PassBuilder pass_builder;
    llvm::LoopAnalysisManager loop_am;
    llvm::FunctionAnalysisManager function_am;
    llvm::CGSCCAnalysisManager cgscc_am;
    llvm::ModuleAnalysisManager module_am;

    function_am.registerPass([&] { return target_analysis; });

    pass_builder.registerModuleAnalyses(module_am);
    pass_builder.registerCGSCCAnalyses(cgscc_am);
    pass_builder.registerFunctionAnalyses(function_am);
    pass_builder.registerLoopAnalyses(loop_am);
    pass_builder.crossRegisterProxies(loop_am, function_am, cgscc_am, module_am);

    pass_builder.registerPipelineStartEPCallback(
            [&](llvm::ModulePassManager& module_pm, llvm::OptimizationLevel Level) {
                llvm::ModuleInlinerPass inliner_pass;
                module_pm.addPass(std::move(inliner_pass));

                llvm::FunctionPassManager function_pm;

                llvm::InstCombinePass inst_combine_pass;
                llvm::PromotePass promote_pass;
                llvm::GVNPass gvn_pass;
                llvm::NewGVNPass new_gvn_pass;
                llvm::SimplifyCFGPass simplify_cfg_pass;
                llvm::LoopVectorizePass loop_vectorize_pass;
                llvm::SLPVectorizerPass slp_vectorize_pass;

                function_pm.addPass(std::move(inst_combine_pass));
                function_pm.addPass(std::move(promote_pass));
                function_pm.addPass(std::move(gvn_pass));
                function_pm.addPass(std::move(new_gvn_pass));
                function_pm.addPass(std::move(simplify_cfg_pass));
                function_pm.addPass(std::move(loop_vectorize_pass));
                function_pm.addPass(std::move(slp_vectorize_pass));

                module_pm.addPass(llvm::createModuleToFunctionPassAdaptor(std::move(function_pm)));

                llvm::GlobalOptPass global_opt;
                module_pm.addPass(std::move(global_opt));
            });

    auto module_pm = pass_builder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3);
    module_pm.run(module, module_am);
}

// JITObjectCache is used to cache compiled Object for reuse in the next linkage-stage.
class JITObjectCache : public llvm::ObjectCache {
public:
    explicit JITObjectCache(size_t capacity) : _cache(capacity) {}

    ~JITObjectCache() override = default;

    void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj) override {
        const auto& key = M->getModuleIdentifier();
        // MemoryBuffer would be modified in linkage stage, so we need to copy it.
        auto* value = llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier()).release();
        // getBufferSize's returning value is a little less than the real size of the buffer, since
        // the buffer contains alignment padding and keep the module identifier at its tail.
        size_t value_size = value->getBufferSize();
        GlobalEnv::GetInstance()->jit_cache_mem_tracker()->consume(value_size);
        auto* handle = _cache.insert(
                key, value, value_size,
                [](const auto& key, auto* value) {
                    auto* p = static_cast<llvm::MemoryBuffer*>(value);
                    GlobalEnv::GetInstance()->jit_cache_mem_tracker()->release(p->getBufferSize());
                    delete p; // Release the memory buffer
                },
                CachePriority::NORMAL);

        if (handle != nullptr) {
            _cache.release(handle);
        }
    }

    std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M) override {
        const auto& key = M->getModuleIdentifier();
        auto* handle = _cache.lookup(key);
        if (handle == nullptr) {
            return nullptr; // Not found in cache
        }
        DeferOp defer_op([&]() {
            _cache.release(handle); // Ensure handle is released after use
        });

        auto* value = static_cast<llvm::MemoryBuffer*>(_cache.value(handle));
        // MemoryBuffer would be modified in linkage stage, so we need to copy it.
        return llvm::MemoryBuffer::getMemBufferCopy(value->getBuffer(), value->getBufferIdentifier());
    }

    Cache* get_cache() { return &_cache; }

private:
    ShardedLRUCache _cache;
};

size_t JITCallable::getSize() {
    return _mem_mgr->getSize();
}

class JITCallableCache {
public:
    struct CacheValue {
        JITCallablePtr callable;
    };

    explicit JITCallableCache(size_t capacity) : _cache(capacity) {}
    ~JITCallableCache() = default;
    JITCallableCache(const JITCallableCache&) = delete;
    JITCallableCache& operator=(const JITCallableCache&) = delete;
    JITCallableCache(JITCallableCache&& that) = delete;
    JITCallableCache& operator=(JITCallableCache&& that) = delete;
    void populate(const std::string& func_name, JITCallablePtr callable) {
        DCHECK(callable);
        auto* value = new CacheValue{std::move(callable)};
        auto value_size = value->callable->getSize();
        GlobalEnv::GetInstance()->jit_cache_mem_tracker()->consume(value_size);
        auto* handle = _cache.insert(
                func_name, value, value_size,
                [](const auto& key, auto* value) {
                    auto* p = static_cast<CacheValue*>(value);
                    GlobalEnv::GetInstance()->jit_cache_mem_tracker()->release(p->callable->getSize());
                    delete p;
                },
                CachePriority::NORMAL);

        if (handle != nullptr) {
            VLOG(10) << "JIT callable cached for " << func_name << ", size: " << value_size << " bytes";
            _cache.release(handle);
        } else {
            VLOG(10) << "JIT callable cache for " << func_name << " is full, not cached";
            GlobalEnv::GetInstance()->jit_cache_mem_tracker()->release(value_size);
            delete value; // Release the memory if not cached
        }
    }

    JITCallablePtr probe(const std::string& func_name) {
        auto* handle = _cache.lookup(func_name);
        if (handle == nullptr) {
            return nullptr; // Not found in cache
        }
        DeferOp defer_op([&]() {
            _cache.release(handle); // Ensure handle is released after use
        });

        auto* value = static_cast<CacheValue*>(_cache.value(handle));
        return value->callable; // Return the cached callable
    }

    Cache* get_cache() { return &_cache; }

private:
    ShardedLRUCache _cache;
};

// Optimise and compile the module.
static inline StatusOr<JITCallablePtr> optimize_and_finalize_module(const std::string& expr_name,
                                                                    std::unique_ptr<llvm::LLVMContext> context,
                                                                    std::unique_ptr<llvm::Module>&& module,
                                                                    JITObjectCache& object_cache) {
    ASSIGN_OR_RETURN(auto jtmb, make_target_machine_builder());
    auto mem_mgr = CustomizedInProcessMemoryManager::create();
    ASSIGN_OR_RETURN(auto lljit, build_JIT(jtmb, object_cache, *mem_mgr));
    auto maybe_tm = jtmb.createTargetMachine();
    ASSIGN_OR_RETURN(auto target_machine, as_JIT_result(maybe_tm, "Could not create target machine: "));

    std::string error;
    llvm::raw_string_ostream errs(error);

    if (llvm::verifyModule(*module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to generate scalar function IR, errors: {}", errs.str()));
    }
    auto target_analysis = target_machine->getTargetIRAnalysis();
    optimize_module(*module, std::move(target_analysis));

    if (llvm::verifyModule(*module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to optimize scalar function IR, errors: {}", errs.str()));
    }

    llvm::orc::ThreadSafeModule tsm(std::move(module), std::move(context));
    auto err = lljit->addIRModule(std::move(tsm));
    if (err) {
        return Status::JitCompileError("Failed to add IR module to LLJIT: " + llvm::toString(std::move(err)));
    }

    auto sym = lljit->lookup(expr_name);
    if (!sym) {
        return Status::JitCompileError("Failed to look up function: " + expr_name +
                                       " error: " + llvm::toString(sym.takeError()));
    }
    JITScalarFunction fn_ptr = sym->toPtr<JITScalarFunction>();
    return std::make_shared<JITCallable>(std::move(mem_mgr), std::move(fn_ptr));
}

#ifndef BE_TEST
constexpr int64_t JIT_CACHE_LOWEST_LIMIT = (1UL << 34); // 16GB
#endif

Status JITEngine::init() {
    if (_initialized) {
        return Status::OK();
    }

    int64_t jit_lru_object_cache_size = config::jit_lru_object_cache_size;
    int64_t jit_lru_cache_size = config::jit_lru_cache_size;
#if BE_TEST
    jit_lru_object_cache_size = 16 * 1024 * 1024;
    jit_lru_cache_size = 16 * 1024 * 1024;
#else
    int64_t mem_limit = GlobalEnv::GetInstance()->process_mem_limit();
    if (jit_lru_cache_size <= 0 && jit_lru_object_cache_size <= 0) {
        if (mem_limit < JIT_CACHE_LOWEST_LIMIT) {
            _initialized = true;
            _support_jit = false;
            LOG(WARNING) << "System or Process memory limit is less than 16GB, disable JIT. You can set "
                            "jit_lru_cache_size or jit_lru_object_cache_size a properly positive value in BE's config "
                            "to force enabling JIT";
            return Status::OK();
        }
    }

    if (jit_lru_object_cache_size <= 0) {
        jit_lru_object_cache_size = std::min<int64_t>((1UL << 22), (int64_t)(mem_limit * 0.01));
    }

    if (jit_lru_cache_size <= 0) {
        jit_lru_cache_size = std::min<int64_t>((1UL << 22), (int64_t)(mem_limit * 0.01));
    }
#endif
    LOG(INFO) << "JIT LRU object cache size = " << jit_lru_object_cache_size;
    LOG(INFO) << "JIT LRU cache size = " << jit_lru_cache_size;
    _object_cache = std::make_unique<JITObjectCache>(jit_lru_object_cache_size);
    _callable_cache = std::make_unique<JITCallableCache>(jit_lru_cache_size);
    DCHECK(_object_cache != nullptr);
    DCHECK(_callable_cache != nullptr);
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
    _initialized = true;
    _support_jit = true;
    return Status::OK();
}

StatusOr<JITCallablePtr> JITEngine::_get_jit_callable_or_create(
        const std::string& expr_name, std::function<StatusOr<JITCallablePtr>()>&& callable_creator) {
    auto cached = _callable_cache->probe(expr_name);
    if (cached) {
        return cached;
    }

    // Create a new callable if not found in cache
    auto result = callable_creator();
    if (!result.ok()) {
        return result.status();
    }
    auto callable = std::move(result.value());

    _callable_cache->populate(expr_name, callable);
    VLOG(10) << "Created and cached JIT callable for " << expr_name << ", size: " << callable->getSize() << " bytes";
    return callable;
}

StatusOr<JITCallablePtr> JITEngine::_compile(ExprContext* context, Expr* expr,
                                             const std::vector<Expr*>& uncompilable_exprs,
                                             const std::string& expr_name) {
    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module_id = "sr_module_" + expr_name;
    auto module = std::make_unique<llvm::Module>(module_id, *llvm_context);
    RETURN_IF_ERROR(generate_scalar_function_ir(context, *module, expr, uncompilable_exprs, expr_name));
    // optimize module and add module
    return optimize_and_finalize_module(expr_name, std::move(llvm_context), std::move(module), *_object_cache);
}

StatusOr<JITCallablePtr> JITEngine::get_jit_callable(const std::string& expr_name, ExprContext* context, Expr* expr,
                                                     const std::vector<Expr*>& uncompilable_exprs) {
    return _get_jit_callable_or_create(expr_name,
                                       [&]() { return _compile(context, expr, uncompilable_exprs, expr_name); });
}

JITCallablePtr JITEngine::lookup(const std::string& expr_name) {
    return _callable_cache->probe(expr_name);
}

JITEngine* JITEngine::get_instance() {
    static JITEngine* instance = new JITEngine();
    return instance;
}

JITEngine::~JITEngine() {
    if (_initialized) {
        _object_cache.reset();
        _callable_cache.reset();
        _initialized = false;
    }
}
Cache* JITEngine::get_object_cache() {
    if (_object_cache == nullptr) {
        return nullptr;
    }
    return _object_cache->get_cache();
}

Cache* JITEngine::get_callable_cache() {
    if (_callable_cache == nullptr) {
        return nullptr;
    }
    return _callable_cache->get_cache();
}

} // namespace starrocks