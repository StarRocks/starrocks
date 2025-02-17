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

#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/mem_info.h"

namespace starrocks {

struct JitCacheEntry {
    JitCacheEntry(std::shared_ptr<llvm::MemoryBuffer> buff, JITScalarFunction f)
            : obj_buff(std::move(buff)), func(std::move(f)) {}
    std::shared_ptr<llvm::MemoryBuffer> obj_buff;
    JITScalarFunction func;
};

JitObjectCache::JitObjectCache(const std::string& expr_name, Cache* cache)
        : _cache_key(std::move(expr_name)), _lru_cache(std::move(cache)) {}

void JitObjectCache::notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj) {
    std::unique_ptr<llvm::MemoryBuffer> obj_buffer =
            llvm::MemoryBuffer::getMemBufferCopy(Obj.getBuffer(), Obj.getBufferIdentifier());
    _obj_code = std::move(obj_buffer);
}

Status JitObjectCache::register_func(JITScalarFunction func) {
    bool cached = JITEngine::get_instance()->lookup_function(this);
    if (cached) {
        return Status::OK();
    }
    _func = func;
    if (_obj_code == nullptr) {
        return Status::JitCompileError("JIT register must wait notifyObjectCompiled()");
    }
    auto cache_func_size = _obj_code->getBufferSize();
    // put into LRU cache
    auto* cache = new JitCacheEntry(_obj_code, _func);
    GlobalEnv::GetInstance()->jit_cache_mem_tracker()->consume(cache_func_size);
    auto* handle = _lru_cache->insert(
            _cache_key, (void*)cache, cache_func_size, cache_func_size, [](const CacheKey& key, void* value) {
                auto* entry = ((JitCacheEntry*)value);
                // maybe release earlier as the std::shared_ptr<llvm::MemoryBuffer> is hold by caller
                GlobalEnv::GetInstance()->jit_cache_mem_tracker()->release(entry->obj_buff->getBufferSize());
                delete entry;
            });
    if (handle == nullptr) {
        delete cache;
        LOG(WARNING) << "JIT register func failed, func = " << _cache_key << ", ir size = " << cache_func_size;
        return Status::JitCompileError("JIT register func failed");
    } else {
        // as caller holds the shared ptr of obj_buff, the function ptr is valid, so the handle can be released here.
        _lru_cache->release(handle);
    }
    return Status::OK();
}

std::unique_ptr<llvm::MemoryBuffer> JitObjectCache::getObject(const llvm::Module* M) {
    return nullptr;
}

JITEngine::~JITEngine() {
    delete _func_cache;
}

constexpr int64_t JIT_CACHE_LOWEST_LIMIT = (1UL << 34); // 16GB

Status JITEngine::init() {
    if (_initialized) {
        return Status::OK();
    }
#if BE_TEST
    _func_cache = new_lru_cache(32000); // 1k capacity per cache of 32 shards in LRU cache
#else
    int64_t mem_limit = MemInfo::physical_mem();
    if (GlobalEnv::GetInstance()->process_mem_tracker()->has_limit()) {
        mem_limit = GlobalEnv::GetInstance()->process_mem_tracker()->limit();
    }
    int64_t jit_lru_cache_size = config::jit_lru_cache_size;
    if (jit_lru_cache_size <= 0) {
        if (mem_limit < JIT_CACHE_LOWEST_LIMIT) {
            _initialized = true;
            _support_jit = false;
            LOG(WARNING) << "System or Process memory limit is less than 16GB, disable JIT. You can set "
                            "jit_lru_cache_size a properly positive value in BE's config to force enabling JIT";
            return Status::OK();
        } else {
            jit_lru_cache_size = std::min<int64_t>((1UL << 30), (int64_t)(mem_limit * 0.01));
        }
    }
    LOG(INFO) << "JIT LRU cache size = " << jit_lru_cache_size;
    _func_cache = new_lru_cache(jit_lru_cache_size);
#endif
    DCHECK(_func_cache != nullptr);
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
    _initialized = true;
    _support_jit = true;
    return Status::OK();
}

Status JITEngine::compile_scalar_function(ExprContext* context, JitObjectCache* func_cache, Expr* expr,
                                          const std::vector<Expr*>& uncompilable_exprs) {
    auto* instance = JITEngine::get_instance();
    if (UNLIKELY(!instance->initialized())) {
        return Status::JitCompileError("JIT engine is not initialized");
    }

    auto cached = instance->lookup_function(func_cache);
    if (cached) {
        return Status::OK();
    }

    ASSIGN_OR_RETURN(auto engine, Engine::create(*func_cache))
    // TODO: check need set module?
    // generate ir to module
    RETURN_IF_ERROR(generate_scalar_function_ir(context, *engine->module(), expr, uncompilable_exprs, func_cache));
    // optimize module and add module
    RETURN_IF_ERROR(engine->optimize_and_finalize_module());
    cached = instance->lookup_function(func_cache);
    if (cached) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto function, engine->get_compiled_func(func_cache->get_func_name()));
    RETURN_IF_ERROR(func_cache->register_func(function));
    return Status::OK();
}

std::string JITEngine::dump_module_ir(const llvm::Module& module) {
    std::string ir;
    llvm::raw_string_ostream stream(ir);
    module.print(stream, nullptr);
    return ir;
}

Status JITEngine::generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr,
                                              const std::vector<Expr*>& uncompilable_exprs, JitObjectCache* obj) {
    llvm::IRBuilder<> b(module.getContext());
    size_t args_size = uncompilable_exprs.size();

    /// Create function type.
    auto* size_type = b.getInt64Ty();
    // Same with JITColumn.
    auto* data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    // Same with JITScalarFunction.
    auto* func_type = llvm::FunctionType::get(b.getVoidTy(), {size_type, data_type->getPointerTo()}, false);

    /// Create function in module.
    // Pseudo code: void "expr->jit_func_name"(int64_t rows_count, JITColumn* columns);
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, obj->get_func_name(), module);
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

bool JITEngine::lookup_function(JitObjectCache* const obj) {
    auto* handle = _func_cache->lookup(obj->get_func_name());
    if (handle == nullptr) {
        return false;
    }
    auto* entry = (JitCacheEntry*)_func_cache->value(handle);
    obj->set_cache(entry->obj_buff, entry->func);
    _func_cache->release(handle);
    return true;
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

Status use_JIT_link(llvm::orc::LLJITBuilder& jit_builder) {
    auto maybe_mem_manager = llvm::jitlink::InProcessMemoryManager::Create();
    auto st = as_JIT_result(maybe_mem_manager, "Could not create memory manager: ");
    if (!st.ok()) {
        return st.status();
    }
    static auto memory_manager = std::move(st.value());
    jit_builder.setObjectLinkingLayerCreator([&](llvm::orc::ExecutionSession& ES, const llvm::Triple& TT) {
        return std::make_unique<llvm::orc::ObjectLinkingLayer>(ES, *memory_manager);
    });

    return Status::OK();
}

StatusOr<std::unique_ptr<llvm::orc::LLJIT>> build_JIT(llvm::orc::JITTargetMachineBuilder jtmb,
                                                      std::reference_wrapper<JitObjectCache>& object_cache) {
    llvm::orc::LLJITBuilder jit_builder;
    RETURN_IF_ERROR(use_JIT_link(jit_builder));
    jit_builder.setJITTargetMachineBuilder(std::move(jtmb));
    jit_builder.setCompileFunctionCreator(
            [&object_cache](llvm::orc::JITTargetMachineBuilder JTMB)
                    -> llvm::Expected<std::unique_ptr<llvm::orc::IRCompileLayer::IRCompiler>> {
                auto target_machine = JTMB.createTargetMachine();
                if (!target_machine) {
                    return target_machine.takeError();
                }
                // after compilation, the object code will be stored into the given object cache
                return std::make_unique<llvm::orc::TMOwningSimpleCompiler>(std::move(*target_machine),
                                                                           &object_cache.get());
            });

    auto maybe_jit = jit_builder.create();
    ASSIGN_OR_RETURN(auto jit, as_JIT_result(maybe_jit, "Could not create LLJIT instance: "));
    add_process_symbol(*jit);
    return std::move(jit);
}

JITEngine::Engine::Engine(std::unique_ptr<llvm::orc::LLJIT> lljit, std::unique_ptr<llvm::TargetMachine> target_machine)
        : _context(std::make_unique<llvm::LLVMContext>()),
          _lljit(std::move(lljit)),
          _ir_builder(std::make_unique<llvm::IRBuilder<>>(*_context)),
          _target_machine(std::move(target_machine)) {
    auto module_id = "sr_module_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    _module = std::make_unique<llvm::Module>(module_id, *_context);
}

// factory method to construct the engine.
StatusOr<std::unique_ptr<JITEngine::Engine>> JITEngine::Engine::create(
        std::reference_wrapper<JitObjectCache> object_cache) {
    ASSIGN_OR_RETURN(auto jtmb, make_target_machine_builder());
    ASSIGN_OR_RETURN(auto jit, build_JIT(jtmb, object_cache));
    auto maybe_tm = jtmb.createTargetMachine();
    ASSIGN_OR_RETURN(auto target_machine, as_JIT_result(maybe_tm, "Could not create target machine: "));
    std::unique_ptr<Engine> engine{new Engine(std::move(jit), std::move(target_machine))};
    return engine;
}

llvm::Module* JITEngine::Engine::module() const {
    DCHECK(!_module_finalized) << "module cannot be accessed after finalized";
    return _module.get();
}

static void optimize_module(llvm::Module& module, llvm::TargetIRAnalysis target_analysis) {
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

    pass_builder.registerPipelineStartEPCallback(
            [&](llvm::ModulePassManager& module_pm, llvm::OptimizationLevel Level) {
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

    pass_builder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3).run(module, module_am);
}

// Optimise and compile the module.
Status JITEngine::Engine::optimize_and_finalize_module() {
    std::string error;
    llvm::raw_string_ostream errs(error);
    if (llvm::verifyModule(*_module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to generate scalar function IR, errors: {}", errs.str()));
    }
    auto target_analysis = _target_machine->getTargetIRAnalysis();
    optimize_module(*_module, std::move(target_analysis));

    if (llvm::verifyModule(*_module, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to optimize scalar function IR, errors: {}", errs.str()));
    }
    // LOG(INFO) <<"opt module = " << dump_module_ir(*_module);

    llvm::orc::ThreadSafeModule tsm(std::move(_module), std::move(_context));
    auto err = _lljit->addIRModule(std::move(tsm));
    if (err) {
        return Status::JitCompileError("Failed to add IR module to LLJIT: " + llvm::toString(std::move(err)));
    }

    _module_finalized = true;
    return Status::OK();
}

StatusOr<JITScalarFunction> JITEngine::Engine::get_compiled_func(const std::string& function) {
    if (!_module_finalized) {
        return Status::JitCompileError("module must be finalized before getting compiled function");
    }
    auto sym = _lljit->lookup(function);
    if (!sym) {
        return Status::JitCompileError("Failed to look up function: " + function +
                                       " error: " + llvm::toString(sym.takeError()));
    }
    JITScalarFunction fn_ptr = sym->toPtr<JITScalarFunction>();
    if (fn_ptr == nullptr) {
        return Status::JitCompileError("Failed to get address for function: " + function);
    }
    return fn_ptr;
}

} // namespace starrocks