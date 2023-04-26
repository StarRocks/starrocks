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

Status init_udaf_context(int64_t id, const std::string& url, const std::string& checksum, const std::string& symbol,
                         FunctionContext* context) {
    RETURN_IF_ERROR(detect_java_runtime());
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(id, url, checksum, &libpath));
    auto* udaf_ctx = context->udaf_ctxs();
    auto udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    auto analyzer = std::make_unique<ClassAnalyzer>();
    RETURN_IF_ERROR(udf_classloader->init());

    ASSIGN_OR_RETURN(udaf_ctx->udaf_class, udf_classloader->getClass(symbol));
    ASSIGN_OR_RETURN(udaf_ctx->udaf_state_class, udf_classloader->getClass(state));
    ASSIGN_OR_RETURN(udaf_ctx->handle, udaf_ctx->udaf_class.newInstance());

    udaf_ctx->buffer_data.resize(DEFAULT_UDAF_BUFFER_SIZE);
    udaf_ctx->buffer = std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());

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
    const char* stub_clazz_name = AggBatchCallStub::stub_clazz_name;
    const char* stub_method_name = AggBatchCallStub::batch_update_method_name;
    jclass udaf_clazz = udaf_ctx->udaf_class.clazz();
    jobject update_method = udaf_ctx->update->method.handle();
    ASSIGN_OR_RETURN(auto update_stub_clazz, udf_classloader->genCallStub(stub_clazz_name, udaf_clazz, update_method,
                                                                          ClassLoader::BATCH_SINGLE_UPDATE));
    ASSIGN_OR_RETURN(auto method, analyzer->get_method_object(update_stub_clazz.clazz(), stub_method_name));
    udaf_ctx->update_batch_call_stub = std::make_unique<AggBatchCallStub>(
            context, udaf_ctx->handle.handle(), std::move(update_stub_clazz), JavaGlobalRef(method));

    RETURN_IF_ERROR(add_method("merge", udaf_ctx->udaf_class.clazz(), &udaf_ctx->merge));
    RETURN_IF_ERROR(add_method("finalize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->finalize));
    RETURN_IF_ERROR(add_method("serialize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->serialize));
    RETURN_IF_ERROR(add_method("serializeLength", udaf_ctx->udaf_state_class.clazz(), &udaf_ctx->serialize_size));

    auto& state_clazz = JVMFunctionHelper::getInstance().function_state_clazz();
    ASSIGN_OR_RETURN(auto instance, state_clazz.newInstance());
    ASSIGN_OR_RETURN(auto get_func, analyzer->get_method_object(state_clazz.clazz(), "get"));
    ASSIGN_OR_RETURN(auto batch_get_func, analyzer->get_method_object(state_clazz.clazz(), "batch_get"));
    ASSIGN_OR_RETURN(auto add_func, analyzer->get_method_object(state_clazz.clazz(), "add"));
    udaf_ctx->states = std::make_unique<UDAFStateList>(std::move(instance), get_func, batch_get_func, add_func);
    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle.handle(), context, udaf_ctx);

    return Status::OK();
}

} // namespace starrocks
