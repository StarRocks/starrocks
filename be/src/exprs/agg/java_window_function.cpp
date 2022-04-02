// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/agg/java_window_function.h"

#include <vector>

#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {

const AggregateFunction* getJavaWindowFunction() {
    static JavaWindowFunction java_window_func;
    return &java_window_func;
}

Status window_init_jvm_context(int64_t fid, const std::string& url, const std::string& checksum,
                               const std::string& symbol, starrocks_udf::FunctionContext* context) {
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(fid, url, checksum, &libpath));
    auto* udaf_ctx = context->impl()->udaf_ctxs();
    udaf_ctx->udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    RETURN_IF_ERROR(udaf_ctx->udf_classloader->init());
    udaf_ctx->analyzer = std::make_unique<ClassAnalyzer>();

    udaf_ctx->udaf_class = udaf_ctx->udf_classloader->getClass(symbol);
    if (udaf_ctx->udaf_class.clazz() == nullptr) {
        return Status::InternalError(fmt::format("couldn't found clazz:{}", symbol));
    }

    udaf_ctx->udaf_state_class = udaf_ctx->udf_classloader->getClass(state);
    if (udaf_ctx->udaf_state_class.clazz() == nullptr) {
        return Status::InternalError(fmt::format("couldn't found clazz:{}", state));
    }

    RETURN_IF_ERROR(udaf_ctx->udaf_class.newInstance(&udaf_ctx->handle));

    auto* analyzer = udaf_ctx->analyzer.get();

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

    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle, udaf_ctx);

    return Status::OK();
}
} // namespace starrocks::vectorized