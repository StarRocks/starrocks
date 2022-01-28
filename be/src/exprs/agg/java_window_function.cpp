// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/agg/java_window_function.h"

#include <vector>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {

const AggregateFunction* getJavaWindowFunction() {
    static JavaWindowFunction java_window_func;
    return &java_window_func;
}

Status window_init_jvm_context(int fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                               starrocks_udf::FunctionContext* context) {
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(fid, url, checksum, &libpath));
    auto* udaf_ctx = context->impl()->udaf_ctxs();
    udaf_ctx->udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    RETURN_IF_ERROR(udaf_ctx->udf_classloader->init());
    udaf_ctx->udf_helper = std::make_unique<UDFHelper>();
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
        std::string sign;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(analyzer->get_signature(clazz, method_name, &sign));
        RETURN_IF_ERROR(analyzer->get_udaf_method_desc(sign, &mtdesc));
        *res = std::make_unique<JavaMethodDescriptor>();
        (*res)->name = std::move(method_name);
        (*res)->sign = std::move(sign);
        (*res)->method_desc = std::move(mtdesc);
        return Status::OK();
    };

    RETURN_IF_ERROR(add_method("reset", udaf_ctx->udaf_class.clazz(), &udaf_ctx->reset));
    RETURN_IF_ERROR(add_method("create", udaf_ctx->udaf_class.clazz(), &udaf_ctx->create));
    RETURN_IF_ERROR(add_method("destroy", udaf_ctx->udaf_class.clazz(), &udaf_ctx->destory));
    RETURN_IF_ERROR(add_method("getValues", udaf_ctx->udaf_class.clazz(), &udaf_ctx->get_values));
    RETURN_IF_ERROR(add_method("batchUpdate", udaf_ctx->udaf_class.clazz(), &udaf_ctx->window_update));

    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->udaf_state_class.clazz(), udaf_ctx->udaf_class.clazz(),
                                                     udaf_ctx->handle, udaf_ctx);

    return Status::OK();
}

Status ConvertDirectBufferVistor::do_visit(const NullableColumn& column) {
    const auto& null_data = column.immutable_null_column_data();
    _buffers.emplace_back((void*)null_data.data(), null_data.size());
    return column.data_column()->accept(this);
}

Status ConvertDirectBufferVistor::do_visit(const BinaryColumn& column) {
    const auto& offsets = column.get_offset();
    _buffers.emplace_back((void*)offsets.data(), offsets.size() * 4);
    const auto& bytes = column.get_bytes();
    _buffers.emplace_back((void*)bytes.data(), bytes.size());
    return Status::OK();
}

} // namespace starrocks::vectorized