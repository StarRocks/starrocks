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
#include "util/defer_op.h"

namespace starrocks {

struct JitCacheEntry {
    JitCacheEntry(llvm::orc::ResourceTrackerSP t, JITScalarFunction f) : tracker(std::move(t)), func(std::move(f)) {}
    llvm::orc::ResourceTrackerSP tracker;
    JITScalarFunction func;
    ~JitCacheEntry() {
        auto error = tracker->remove();
        if (UNLIKELY(error)) {
            std::string error_message;
            llvm::handleAllErrors(std::move(error),
                                  [&](const llvm::ErrorInfoBase& EIB) { error_message = EIB.message(); });
            LOG(ERROR) << "JIT: Failed to remove IR module from JIT: " << error_message;
        }
    }
};

/*
class JitObjectCache : public llvm::ObjectCache {
public:
    explicit JitObjectCache(std::shared_ptr<LRUCache>& cache, const std::string& expr_name);

    ~JitObjectCache() {}

    void notifyObjectCompiled(const llvm::Module* M, llvm::MemoryBufferRef Obj);

    std::unique_ptr<llvm::MemoryBuffer> getObject(const llvm::Module* M);

private:
    const std::string _cache_key;
    std::shared_ptr<LRUCache>& _cache;
};
*/

JITEngine::~JITEngine() {
    delete _func_cache;
}

Status JITEngine::init() {
    if (_initialized) {
        return Status::OK();
    }
#if 0
    // Initialize LLVM targets and data layout.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
#else
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
#endif
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

    auto expr_name = expr->jit_func_name();

    auto compiled_function = instance->lookup_function(expr_name);
    if (compiled_function.first != nullptr) {
        return compiled_function;
    }

    auto llvm_context = std::make_unique<llvm::LLVMContext>();
    auto module = std::make_unique<llvm::Module>(expr_name, *llvm_context);
    instance->setup_module(module.get());

    // Generate scalar function IR.
    RETURN_IF_ERROR(generate_scalar_function_ir(context, *module, expr));
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

Status JITEngine::generate_scalar_function_ir(ExprContext* context, llvm::Module& module, Expr* expr) {
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
    // Pseudo code: void "expr->jit_func_name()"(int64_t rows_count, JITColumn* columns);
    auto* func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expr->jit_func_name(), module);
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

    std::unique_lock<std::mutex> lock(_mutex);
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
    lock.unlock();

    auto* cache = new JitCacheEntry(resource_tracker, addr->toPtr<JITScalarFunction>());

    auto* handle = _func_cache->insert(expr_name, (void*)cache, 1, [](const CacheKey& key, void* value) {
        auto* entry = ((JitCacheEntry*)value);
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

template <typename T>
StatusOr<T> AsJitResult(llvm::Expected<T>& expected, const std::string& error_context) {
    if (!expected) {
        return Status::JitCompileError(error_context + llvm::toString(expected.takeError()));
    }
    return std::move(expected.get());
}

StatusOr<llvm::orc::JITTargetMachineBuilder> MakeTargetMachineBuilder() {
    llvm::orc::JITTargetMachineBuilder jtmb((llvm::Triple(llvm::sys::getDefaultTargetTriple())));
    auto const opt_level = true ? llvm::CodeGenOpt::Aggressive : llvm::CodeGenOpt::None;
    jtmb.setCodeGenOptLevel(opt_level);
    return jtmb;
}

std::string DumpModuleIR(const llvm::Module& module) {
    std::string ir;
    llvm::raw_string_ostream stream(ir);
    module.print(stream, nullptr);
    return ir;
}

void AddAbsoluteSymbol(llvm::orc::LLJIT& lljit, const std::string& name, void* function_ptr) {
    llvm::orc::MangleAndInterner mangle(lljit.getExecutionSession(), lljit.getDataLayout());

    llvm::JITEvaluatedSymbol symbol(reinterpret_cast<llvm::JITTargetAddress>(function_ptr),
                                    llvm::JITSymbolFlags::Exported);

    auto error = lljit.getMainJITDylib().define(llvm::orc::absoluteSymbols({{mangle(name), symbol}}));
    llvm::cantFail(std::move(error));
}

// add current process symbol to dylib
void AddProcessSymbol(llvm::orc::LLJIT& lljit) {
    lljit.getMainJITDylib().addGenerator(llvm::cantFail(
            llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(lljit.getDataLayout().getGlobalPrefix())));
    // the `atexit` symbol cannot be found for ASAN
#ifdef ADDRESS_SANITIZER
    if (!lljit.lookup("atexit")) {
        AddAbsoluteSymbol(lljit, "atexit", reinterpret_cast<void*>(atexit));
    }
#endif
}

Status UseJITLink(llvm::orc::LLJITBuilder& jit_builder) {
    auto maybe_mem_manager = llvm::jitlink::InProcessMemoryManager::Create();
    auto st = AsJitResult(maybe_mem_manager, "Could not create memory manager: ");
    if (!st.ok()) {
        return st.status();
    }
    static auto memory_manager = std::move(st.value());
    jit_builder.setObjectLinkingLayerCreator([&](llvm::orc::ExecutionSession& ES, const llvm::Triple& TT) {
        return std::make_unique<llvm::orc::ObjectLinkingLayer>(ES, *memory_manager);
    });

    return Status::OK();
}

StatusOr<std::unique_ptr<llvm::orc::LLJIT>> BuildJIT(llvm::orc::JITTargetMachineBuilder jtmb,
                                                     std::reference_wrapper<MyObjectCache>& object_cache) {
    llvm::orc::LLJITBuilder jit_builder;

    RETURN_IF_ERROR(UseJITLink(jit_builder));

    jit_builder.setJITTargetMachineBuilder(std::move(jtmb));

    jit_builder.setCompileFunctionCreator(
            [&object_cache](llvm::orc::JITTargetMachineBuilder JTMB)
                    -> llvm::Expected<std::unique_ptr<llvm::orc::IRCompileLayer::IRCompiler>> {
                auto target_machine = JTMB.createTargetMachine();
                if (!target_machine) {
                    return target_machine.takeError();
                }
                // after compilation, the object code will be stored into the given object
                // cache
                return std::make_unique<llvm::orc::TMOwningSimpleCompiler>(std::move(*target_machine),
                                                                           &object_cache.get());
            });

    auto maybe_jit = jit_builder.create();
    ASSIGN_OR_RETURN(auto jit, AsJitResult(maybe_jit, "Could not create LLJIT instance: "));

    AddProcessSymbol(*jit);
    return std::move(jit);
}

Engine::Engine(std::unique_ptr<llvm::orc::LLJIT> lljit, std::unique_ptr<llvm::TargetMachine> target_machine,
               bool cached)
        : context_(std::make_unique<llvm::LLVMContext>()),
          lljit_(std::move(lljit)),
          ir_builder_(std::make_unique<llvm::IRBuilder<>>(*context_)),
          optimize_(1),
          cached_(cached),
          target_machine_(std::move(target_machine)) {
    // LLVM 10 doesn't like the expr function name to be the same as the module name
    auto module_id = "gdv_module_" + std::to_string(reinterpret_cast<uintptr_t>(this));
    module_ = std::make_unique<llvm::Module>(module_id, *context_);
}

Engine::~Engine() {}

/// factory method to construct the engine.
StatusOr<std::unique_ptr<Engine>> Engine::Make(bool cached, std::reference_wrapper<MyObjectCache> object_cache) {
    ASSIGN_OR_RETURN(auto jtmb, MakeTargetMachineBuilder());
    ASSIGN_OR_RETURN(auto jit, BuildJIT(jtmb, object_cache));
    auto maybe_tm = jtmb.createTargetMachine();
    ASSIGN_OR_RETURN(auto target_machine, AsJitResult(maybe_tm, "Could not create target machine: "));
    std::unique_ptr<Engine> engine{new Engine(std::move(jit), std::move(target_machine), cached)};
    return engine;
}

llvm::Module* Engine::module() {
    DCHECK(!module_finalized_) << "module cannot be accessed after finalized";
    return module_.get();
}

static void OptimizeModuleWithNewPassManager(llvm::Module& module, llvm::TargetIRAnalysis target_analysis) {
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
Status Engine::FinalizeModule() {
    std::string error;
    llvm::raw_string_ostream errs(error);
    if (llvm::verifyModule(*module_, &errs)) {
        return Status::JitCompileError(fmt::format("Failed to generate scalar function IR, errors: {}", errs.str()));
    }
    cached_ = false;
    optimize_ = true;
    if (!cached_) {
        if (optimize_) {
            auto target_analysis = target_machine_->getTargetIRAnalysis();
            OptimizeModuleWithNewPassManager(*module_, std::move(target_analysis));
        }

        if (llvm::verifyModule(*module_, &errs)) {
            return Status::JitCompileError(
                    fmt::format("Failed to optimize scalar function IR, errors: {}", errs.str()));
        }

        llvm::orc::ThreadSafeModule tsm(std::move(module_), std::move(context_));
        auto error = lljit_->addIRModule(std::move(tsm));
        if (error) {
            return Status::JitCompileError("Failed to add IR module to LLJIT: " + llvm::toString(std::move(error)));
        }
    }
    module_finalized_ = true;
    return Status::OK();
}

StatusOr<JITScalarFunction> Engine::CompiledFunction(const std::string& function) {
    DCHECK(module_finalized_) << "module must be finalized before getting compiled function";
    auto sym = lljit_->lookup(function);
    if (!sym) {
        return Status::JitCompileError("Failed to look up function: " + function +
                                       " error: " + llvm::toString(sym.takeError()));
    }

    auto fn_addr = sym->getValue();

    auto fn_ptr = reinterpret_cast<JITScalarFunction>(fn_addr);
    if (fn_ptr == nullptr) {
        return Status::JitCompileError("Failed to get address for function: " + function);
    }
    return fn_ptr;
}

const std::string& Engine::ir() {
    DCHECK(!module_ir_.empty()) << "dump_ir in Configuration must be set for dumping IR";
    return module_ir_;
}

} // namespace starrocks