// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/agg/java_udaf_function.h"

#include <memory>

#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {

const int DEFAULT_UDAF_BUFFER_SIZE = 1024;

const AggregateFunction* getJavaUDAFFunction(bool input_nullable) {
    static JavaUDAFAggregateFunction<false> no_nullable_udaf_func;
    return &no_nullable_udaf_func;
}

Status init_udaf_context(int64_t id, const std::string& url, const std::string& checksum, const std::string& symbol,
                         starrocks_udf::FunctionContext* context) {
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(id, url, checksum, &libpath));
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

    udaf_ctx->buffer_data.resize(DEFAULT_UDAF_BUFFER_SIZE);
    udaf_ctx->buffer = std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());

    auto* analyzer = udaf_ctx->analyzer.get();

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
    RETURN_IF_ERROR(add_method("merge", udaf_ctx->udaf_class.clazz(), &udaf_ctx->merge));
    RETURN_IF_ERROR(add_method("finalize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->finalize));
    RETURN_IF_ERROR(add_method("serialize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->serialize));
    RETURN_IF_ERROR(add_method("serializeLength", udaf_ctx->udaf_state_class.clazz(), &udaf_ctx->serialize_size));

    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle, context, udaf_ctx);

    return Status::OK();
}

} // namespace starrocks::vectorized