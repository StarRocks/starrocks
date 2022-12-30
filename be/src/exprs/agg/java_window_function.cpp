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

#include <vector>

#include "runtime/user_function_cache.h"

namespace starrocks {

const AggregateFunction* getJavaWindowFunction() {
    static JavaWindowFunction java_window_func;
    return &java_window_func;
}

Status window_init_jvm_context(int64_t fid, const std::string& url, const std::string& checksum,
                               const std::string& symbol, FunctionContext* context) {
    RETURN_IF_ERROR(detect_java_runtime());
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(fid, url, checksum, &libpath));
    auto* udaf_ctx = context->udaf_ctxs();
    auto udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    auto analyzer = std::make_unique<ClassAnalyzer>();
    RETURN_IF_ERROR(udf_classloader->init());

    ASSIGN_OR_RETURN(udaf_ctx->udaf_class, udf_classloader->getClass(symbol));
    ASSIGN_OR_RETURN(udaf_ctx->udaf_state_class, udf_classloader->getClass(state));
    ASSIGN_OR_RETURN(udaf_ctx->handle, udaf_ctx->udaf_class.newInstance());

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

    RETURN_IF_ERROR(add_method("reset", udaf_ctx->udaf_class.clazz(), &udaf_ctx->reset));
    RETURN_IF_ERROR(add_method("create", udaf_ctx->udaf_class.clazz(), &udaf_ctx->create));
    RETURN_IF_ERROR(add_method("destroy", udaf_ctx->udaf_class.clazz(), &udaf_ctx->destory));
    RETURN_IF_ERROR(add_method("finalize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->finalize));
    RETURN_IF_ERROR(add_method("windowUpdate", udaf_ctx->udaf_class.clazz(), &udaf_ctx->window_update));

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
