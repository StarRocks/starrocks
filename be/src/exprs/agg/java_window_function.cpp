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

#include "exprs/agg/java_window_function.h"

#include <any>
#include <vector>

#include "runtime/user_function_cache.h"

namespace starrocks {

const AggregateFunction* getJavaWindowFunction() {
    static JavaWindowFunction java_window_func;
    return &java_window_func;
}

// Build a JavaUDAFSharedContext for a window function (class-level, shareable/cacheable).
static StatusOr<std::shared_ptr<JavaUDAFSharedContext>> build_window_shared_context(const std::string& libpath,
                                                                                    const std::string& symbol) {
    std::string state = symbol + "$State";

    auto shared = std::make_shared<JavaUDAFSharedContext>();
    shared->udf_classloader = std::make_unique<ClassLoader>(libpath);
    auto analyzer = std::make_unique<ClassAnalyzer>();
    RETURN_IF_ERROR(shared->udf_classloader->init());

    ASSIGN_OR_RETURN(shared->udaf_class, shared->udf_classloader->getClass(symbol));
    ASSIGN_OR_RETURN(shared->udaf_state_class, shared->udf_classloader->getClass(state));

    auto add_method = [&](const std::string& name, jclass clazz, std::unique_ptr<JavaMethodDescriptor>* res) {
        std::string method_name = name;
        std::string signature;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(analyzer->get_signature(clazz, method_name, &signature));
        RETURN_IF_ERROR(analyzer->get_udaf_method_desc(signature, &mtdesc));
        *res = std::make_unique<JavaMethodDescriptor>();
        (*res)->name = std::move(method_name);
        (*res)->signature = std::move(signature);
        (*res)->method_desc = std::move(mtdesc);
        ASSIGN_OR_RETURN((*res)->method, analyzer->get_method_object(clazz, name));
        return Status::OK();
    };

    RETURN_IF_ERROR(add_method("reset", shared->udaf_class.clazz(), &shared->reset));
    RETURN_IF_ERROR(add_method("create", shared->udaf_class.clazz(), &shared->create));
    RETURN_IF_ERROR(add_method("destroy", shared->udaf_class.clazz(), &shared->destory));
    RETURN_IF_ERROR(add_method("finalize", shared->udaf_class.clazz(), &shared->finalize));
    RETURN_IF_ERROR(add_method("windowUpdate", shared->udaf_class.clazz(), &shared->window_update));

    // Look up FunctionStates method objects once — instance creation happens per aggregator
    auto& state_clazz = JVMFunctionHelper::getInstance().function_state_clazz();
    ASSIGN_OR_RETURN(shared->states_get_method, analyzer->get_method_object(state_clazz.clazz(), "get"));
    ASSIGN_OR_RETURN(shared->states_batch_get_method, analyzer->get_method_object(state_clazz.clazz(), "batch_get"));
    ASSIGN_OR_RETURN(shared->states_add_method, analyzer->get_method_object(state_clazz.clazz(), "add"));
    ASSIGN_OR_RETURN(shared->states_remove_method, analyzer->get_method_object(state_clazz.clazz(), "remove"));
    ASSIGN_OR_RETURN(shared->states_clear_method, analyzer->get_method_object(state_clazz.clazz(), "clear"));

    return shared;
}

// Build a per-aggregator JavaUDAFUniqueContext for a window function on top of a shared context.
static Status build_window_unique_context(std::shared_ptr<JavaUDAFSharedContext> shared, FunctionContext* context) {
    auto udaf_ctx = std::make_unique<JavaUDAFUniqueContext>();
    udaf_ctx->ctx = std::move(shared);

    ASSIGN_OR_RETURN(udaf_ctx->handle, udaf_ctx->ctx->udaf_class.newInstance());

    // Create a new FunctionStates instance; clone method refs from the shared context.
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    auto& state_clazz = JVMFunctionHelper::getInstance().function_state_clazz();
    ASSIGN_OR_RETURN(auto instance, state_clazz.newInstance());
    udaf_ctx->states = std::make_unique<UDAFStateList>(
            std::move(instance), JavaGlobalRef(env->NewGlobalRef(udaf_ctx->ctx->states_get_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(udaf_ctx->ctx->states_batch_get_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(udaf_ctx->ctx->states_add_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(udaf_ctx->ctx->states_remove_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(udaf_ctx->ctx->states_clear_method.handle())));
    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle.handle(), context, udaf_ctx.get());
    attach_java_udaf_context(context, std::move(udaf_ctx));
    return Status::OK();
}

Status window_init_jvm_context(int64_t fid, const std::string& url, const std::string& checksum,
                               const std::string& symbol, FunctionContext* context,
                               const TCloudConfiguration& cloud_configuration, bool use_cache, bool* cache_hit_out) {
    RETURN_IF_ERROR(detect_java_runtime());
    auto func_cache = UserFunctionCache::instance();

    if (use_cache) {
        ASSIGN_OR_RETURN(auto result, func_cache->load_cacheable_java_udf(
                                              fid, url, checksum, TFunctionBinaryType::SRJAR,
                                              [&symbol](const std::string& libpath) -> StatusOr<std::any> {
                                                  ASSIGN_OR_RETURN(auto ctx,
                                                                   build_window_shared_context(libpath, symbol));
                                                  return std::any(std::move(ctx));
                                              },
                                              cloud_configuration));
        if (cache_hit_out != nullptr) {
            *cache_hit_out = result.first;
        }
        auto shared = std::any_cast<std::shared_ptr<JavaUDAFSharedContext>>(result.second);
        return build_window_unique_context(std::move(shared), context);
    }

    std::string libpath;
    RETURN_IF_ERROR(
            func_cache->get_libpath(fid, url, checksum, TFunctionBinaryType::SRJAR, &libpath, cloud_configuration));
    ASSIGN_OR_RETURN(auto shared_ctx, build_window_shared_context(libpath, symbol));
    return build_window_unique_context(std::move(shared_ctx), context);
}

} // namespace starrocks
