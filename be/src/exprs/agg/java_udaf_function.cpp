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

#include "exprs/agg/java_udaf_function.h"

#include <any>
#include <memory>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fmt/core.h"
#include "jni.h"
#include "runtime/user_function_cache.h"

namespace starrocks {

const int DEFAULT_UDAF_BUFFER_SIZE = 1024;

const AggregateFunction* getJavaUDAFFunction(bool input_nullable) {
    static JavaUDAFAggregateFunction no_nullable_udaf_func;
    return &no_nullable_udaf_func;
}

<<<<<<< HEAD
Status init_udaf_context(int64_t id, const std::string& url, const std::string& checksum, const std::string& symbol,
                         FunctionContext* context) {
    RETURN_IF_ERROR(detect_java_runtime());
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(id, url, checksum, &libpath));
    auto* udaf_ctx = context->udaf_ctxs();
    auto udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
=======
// Build a JavaUDAFSharedContext (class-level, shareable/cacheable).
// This is the expensive part: class loading, method introspection, and stub class generation.
// The UDAF object instance is NOT created here — it is per-aggregator (see build_udaf_unique_context).
static StatusOr<std::shared_ptr<JavaUDAFSharedContext>> build_udaf_shared_context(const std::string& libpath,
                                                                                  const std::string& symbol,
                                                                                  int num_args) {
    std::string state_cls_name = symbol + "$State";

    auto udaf_ctx = std::make_shared<JavaUDAFSharedContext>();
    udaf_ctx->udf_classloader = std::make_unique<ClassLoader>(libpath);
>>>>>>> 8c1e0cbb07 ([Enhancement] Cache UDAF for loading&initialize only once and re-use across queries (#72038))
    auto analyzer = std::make_unique<ClassAnalyzer>();
    RETURN_IF_ERROR(udaf_ctx->udf_classloader->init());

    ASSIGN_OR_RETURN(udaf_ctx->udaf_class, udaf_ctx->udf_classloader->getClass(symbol));
    ASSIGN_OR_RETURN(udaf_ctx->udaf_state_class, udaf_ctx->udf_classloader->getClass(state_cls_name));

    auto add_method = [&](const std::string& name, jclass clazz, std::unique_ptr<JavaMethodDescriptor>* res) {
        std::string method_name = name;
        std::string sign;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(analyzer->get_signature(clazz, method_name, &sign));
        RETURN_IF_ERROR(analyzer->get_udaf_method_desc(sign, &mtdesc));
        *res = std::make_unique<JavaMethodDescriptor>();
        (*res)->signature = std::move(sign);
        (*res)->name = std::move(method_name);
        (*res)->method_desc = std::move(mtdesc);
        ASSIGN_OR_RETURN((*res)->method, analyzer->get_method_object(clazz, name));
        return Status::OK();
    };

    RETURN_IF_ERROR(add_method("create", udaf_ctx->udaf_class.clazz(), &udaf_ctx->create));
    RETURN_IF_ERROR(add_method("destroy", udaf_ctx->udaf_class.clazz(), &udaf_ctx->destory));
    RETURN_IF_ERROR(add_method("update", udaf_ctx->udaf_class.clazz(), &udaf_ctx->update));
<<<<<<< HEAD
    const char* stub_clazz_name = AggBatchCallStub::stub_clazz_name;
    const char* stub_method_name = AggBatchCallStub::batch_update_method_name;
    jclass udaf_clazz = udaf_ctx->udaf_class.clazz();
    jobject update_method = udaf_ctx->update->method.handle();
    ASSIGN_OR_RETURN(auto update_stub_clazz, udf_classloader->genCallStub(stub_clazz_name, udaf_clazz, update_method,
                                                                          ClassLoader::BATCH_SINGLE_UPDATE));
    ASSIGN_OR_RETURN(auto method, analyzer->get_method_object(update_stub_clazz.clazz(), stub_method_name));
    udaf_ctx->update_batch_call_stub = std::make_unique<AggBatchCallStub>(
            context, udaf_ctx->handle.handle(), std::move(update_stub_clazz), JavaGlobalRef(method));

=======
>>>>>>> 8c1e0cbb07 ([Enhancement] Cache UDAF for loading&initialize only once and re-use across queries (#72038))
    RETURN_IF_ERROR(add_method("merge", udaf_ctx->udaf_class.clazz(), &udaf_ctx->merge));
    RETURN_IF_ERROR(add_method("finalize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->finalize));
    RETURN_IF_ERROR(add_method("serialize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->serialize));
    RETURN_IF_ERROR(add_method("serializeLength", udaf_ctx->udaf_state_class.clazz(), &udaf_ctx->serialize_size));

    // Generate and store the stub class/method — each unique context creates its own AggBatchCallStub from these
    const char* stub_clazz_name = AggBatchCallStub::stub_clazz_name;
    const char* stub_method_name = AggBatchCallStub::batch_update_method_name;
    jclass udaf_clazz = udaf_ctx->udaf_class.clazz();
    jobject update_method_obj = udaf_ctx->update->method.handle();
    ASSIGN_OR_RETURN(udaf_ctx->update_stub_clazz,
                     udaf_ctx->udf_classloader->genCallStub(stub_clazz_name, udaf_clazz, update_method_obj,
                                                            ClassLoader::BATCH_SINGLE_UPDATE, num_args));
    ASSIGN_OR_RETURN(udaf_ctx->update_stub_method,
                     analyzer->get_method_object(udaf_ctx->update_stub_clazz.clazz(), stub_method_name));

    // Look up FunctionStates method objects once — instance creation happens per aggregator
    auto& state_clazz = JVMFunctionHelper::getInstance().function_state_clazz();
    ASSIGN_OR_RETURN(udaf_ctx->states_get_method, analyzer->get_method_object(state_clazz.clazz(), "get"));
    ASSIGN_OR_RETURN(udaf_ctx->states_batch_get_method, analyzer->get_method_object(state_clazz.clazz(), "batch_get"));
    ASSIGN_OR_RETURN(udaf_ctx->states_add_method, analyzer->get_method_object(state_clazz.clazz(), "add"));
    ASSIGN_OR_RETURN(udaf_ctx->states_remove_method, analyzer->get_method_object(state_clazz.clazz(), "remove"));
    ASSIGN_OR_RETURN(udaf_ctx->states_clear_method, analyzer->get_method_object(state_clazz.clazz(), "clear"));

    return udaf_ctx;
}

// Build a per-aggregator JavaUDAFUniqueContext on top of a (possibly cached) JavaUDAFSharedContext.
static Status build_udaf_unique_context(std::shared_ptr<JavaUDAFSharedContext> udaf_ctx, FunctionContext* context) {
    auto agg_ctx = std::make_unique<JavaUDAFUniqueContext>();
    agg_ctx->ctx = std::move(udaf_ctx);

    // Create a per-aggregator UDAF object instance
    ASSIGN_OR_RETURN(agg_ctx->handle, agg_ctx->ctx->udaf_class.newInstance());

    // Create a per-aggregator AggBatchCallStub with the shared stub class/method cloned as new global refs
    JNIEnv* env = JVMFunctionHelper::getInstance().getEnv();
    JVMClass stub_clazz(env->NewGlobalRef(agg_ctx->ctx->update_stub_clazz.clazz()));
    jobject stub_method = env->NewGlobalRef(agg_ctx->ctx->update_stub_method.handle());
    agg_ctx->update_batch_call_stub = std::make_unique<AggBatchCallStub>(
            context, agg_ctx->handle.handle(), std::move(stub_clazz), JavaGlobalRef(stub_method));

    agg_ctx->buffer_data.resize(DEFAULT_UDAF_BUFFER_SIZE);
    agg_ctx->buffer = std::make_unique<DirectByteBuffer>(agg_ctx->buffer_data.data(), agg_ctx->buffer_data.size());

    // Create a new FunctionStates instance for this aggregator.
    // Method objects are cloned from the shared context (looked up only once at build time).
    auto& state_clazz = JVMFunctionHelper::getInstance().function_state_clazz();
    ASSIGN_OR_RETURN(auto instance, state_clazz.newInstance());
<<<<<<< HEAD
    ASSIGN_OR_RETURN(auto get_func, analyzer->get_method_object(state_clazz.clazz(), "get"));
    ASSIGN_OR_RETURN(auto batch_get_func, analyzer->get_method_object(state_clazz.clazz(), "batch_get"));
    ASSIGN_OR_RETURN(auto add_func, analyzer->get_method_object(state_clazz.clazz(), "add"));
    ASSIGN_OR_RETURN(auto remove_func, analyzer->get_method_object(state_clazz.clazz(), "remove"));
    ASSIGN_OR_RETURN(auto clear_func, analyzer->get_method_object(state_clazz.clazz(), "clear"));
    udaf_ctx->states = std::make_unique<UDAFStateList>(std::move(instance), get_func, batch_get_func, add_func,
                                                       remove_func, clear_func);
    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle.handle(), context, udaf_ctx);

    return Status::OK();
}

=======
    agg_ctx->states = std::make_unique<UDAFStateList>(
            std::move(instance), JavaGlobalRef(env->NewGlobalRef(agg_ctx->ctx->states_get_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(agg_ctx->ctx->states_batch_get_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(agg_ctx->ctx->states_add_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(agg_ctx->ctx->states_remove_method.handle())),
            JavaGlobalRef(env->NewGlobalRef(agg_ctx->ctx->states_clear_method.handle())));
    agg_ctx->_func = std::make_unique<UDAFFunction>(agg_ctx->handle.handle(), context, agg_ctx.get());
    attach_java_udaf_context(context, std::move(agg_ctx));
    return Status::OK();
}

Status init_udaf_context(int64_t id, const std::string& url, const std::string& checksum, const std::string& symbol,
                         FunctionContext* context, const TCloudConfiguration& cloud_configuration, bool use_cache,
                         bool* cache_hit_out) {
    RETURN_IF_ERROR(detect_java_runtime());

    int num_args = context->get_num_args();
    auto func_cache = UserFunctionCache::instance();

    if (use_cache) {
        ASSIGN_OR_RETURN(auto result,
                         func_cache->load_cacheable_java_udf(
                                 id, url, checksum, TFunctionBinaryType::SRJAR,
                                 [&symbol, num_args](const std::string& libpath) -> StatusOr<std::any> {
                                     ASSIGN_OR_RETURN(auto ctx, build_udaf_shared_context(libpath, symbol, num_args));
                                     return std::any(std::move(ctx));
                                 },
                                 cloud_configuration));
        if (cache_hit_out != nullptr) {
            *cache_hit_out = result.first;
        }
        auto shared_ctx = std::any_cast<std::shared_ptr<JavaUDAFSharedContext>>(result.second);
        return build_udaf_unique_context(std::move(shared_ctx), context);
    }

    // Cache disabled: build without caching (download JAR via get_libpath first)
    std::string libpath;
    RETURN_IF_ERROR(
            func_cache->get_libpath(id, url, checksum, TFunctionBinaryType::SRJAR, &libpath, cloud_configuration));
    ASSIGN_OR_RETURN(auto shared_ctx, build_udaf_shared_context(libpath, symbol, num_args));
    return build_udaf_unique_context(std::move(shared_ctx), context);
}

>>>>>>> 8c1e0cbb07 ([Enhancement] Cache UDAF for loading&initialize only once and re-use across queries (#72038))
} // namespace starrocks
