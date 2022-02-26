// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstring>
#include <memory>
#include <vector>
#include <cstdint>
#include <new>
#include <ostream>
#include <string>

#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "common/status.h"
#include "glog/logging.h"
#include "gutil/strings/numbers.h"
#include "util/slice.h"

namespace starrocks::vectorized {

template <bool handle_null>
jvalue cast_to_jvalue(MethodTypeDescriptor method_type_desc, const Column* col, int row_num);
void release_jvalue(MethodTypeDescriptor method_type_desc, jvalue val);
void append_jvalue(MethodTypeDescriptor method_type_desc, Column* col, jvalue val);
Status get_java_udaf_function(int fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                              starrocks_udf::FunctionContext* context, AggregateFunction** func);

template <bool handle_null = true>
class JavaUDAFAggregateFunction : public AggregateFunction {
public:
    using State = JavaUDAFState;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override final {
        int num_args = ctx->get_num_args();
        jvalue args[num_args + 1];
        args[0].l = this->data(state).handle;
        for (int i = 0; i < num_args; ++i) {
            args[i + 1] = cast_to_jvalue<handle_null>(ctx->impl()->udaf_ctxs()->update->method_desc[i + 2], columns[i],
                                                      row_num);
        }

        ctx->impl()->udaf_ctxs()->_func->update(args);

        for (int i = 0; i < num_args; ++i) {
            release_jvalue(ctx->impl()->udaf_ctxs()->update->method_desc[i + 2], args[i + 1]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state,
               size_t row_num) const override final {
        // TODO merge
        const BinaryColumn* input_column = nullptr;
        if (column->is_nullable()) {
            auto* null_column = down_cast<const NullableColumn*>(column);
            input_column = down_cast<const BinaryColumn*>(null_column->data_column().get());
        } else {
            input_column = down_cast<const BinaryColumn*>(column);
        }
        Slice slice = input_column->get_slice(row_num);
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();

        if (udaf_ctx->buffer->capacity() < slice.get_size()) {
            udaf_ctx->buffer_data.resize(slice.get_size());
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }

        memcpy(udaf_ctx->buffer_data.data(), slice.get_data(), slice.get_size());
        udaf_ctx->_func->merge(this->data(state).handle, udaf_ctx->buffer->handle());
    }

    void serialize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                             Column* to) const override final {
        BinaryColumn* column = nullptr;
        // TODO serialize
        if (to->is_nullable()) {
            auto* null_column = down_cast<NullableColumn*>(to);
            null_column->null_column()->append(DATUM_NOT_NULL);
            column = down_cast<BinaryColumn*>(null_column->data_column().get());
        } else {
            DCHECK(to->is_binary());
            column = down_cast<BinaryColumn*>(to);
        }

        size_t old_size = column->get_bytes().size();
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();
        int serialize_size = udaf_ctx->_func->serialize_size(this->data(state).handle);
        if (udaf_ctx->buffer->capacity() < serialize_size) {
            udaf_ctx->buffer_data.resize(serialize_size);
            udaf_ctx->buffer =
                    std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());
        }

        udaf_ctx->_func->serialize(this->data(state).handle, udaf_ctx->buffer->handle());
        size_t new_size = old_size + serialize_size;
        column->get_bytes().resize(new_size);
        column->get_offset().emplace_back(new_size);
        memcpy(column->get_bytes().data(), udaf_ctx->buffer_data.data(), serialize_size);
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override final {
        // TODO finalize
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();
        jvalue val = udaf_ctx->_func->finalize(this->data(state).handle);
        append_jvalue(udaf_ctx->finalize->method_desc[0], to, val);
        release_jvalue(udaf_ctx->finalize->method_desc[0], val);
    }

    //Now Java UDAF don't Not Support Streaming Aggregate
    void convert_to_serialize_format(const Columns& src, size_t chunk_size, ColumnPtr* dst) const override final {
        DCHECK(false) << "Now Java UDAF Not Support Streaming Mode";
    }

    // State Data
    static State& data(AggDataPtr __restrict place) { return *reinterpret_cast<State*>(place); }
    static const State& data(ConstAggDataPtr __restrict place) { return *reinterpret_cast<const State*>(place); }

    // jclass
    // newInstance -> handle
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        new (ptr) State(ctx->impl()->udaf_ctxs()->_func->create());
    }

    // Call Destroy method
    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        ctx->impl()->udaf_ctxs()->_func->destroy(data(ptr).handle);
        data(ptr).~State();
    }

    size_t size() const override { return sizeof(State); }

    size_t alignof_size() const override { return alignof(State); }

    // batch interface

    void update_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        int num_cols = ctx->get_num_args();
        auto arr = JavaDataTypeConverter::convert_to_object_array(states, state_offset, batch_size);
        args.emplace_back(arr);
        JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
        helper.batch_update(ctx, ctx->impl()->udaf_ctxs()->handle, ctx->impl()->udaf_ctxs()->update->method,
                            args.data(), args.size());
        for (int i = 0; i < args.size(); ++i) {
            helper.getEnv()->DeleteLocalRef(args[i]);
        }
    }

    void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            if (filter[i] == 0) {
                this->update(ctx, columns, states[i] + state_offset, i);
            }
        }
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        std::vector<DirectByteBuffer> buffers;
        ConvertDirectBufferVistor vistor(buffers);
        int num_cols = ctx->get_num_args();
        PrimitiveType types[num_cols];
        jobject args[num_cols];
        for (int i = 0; i < num_cols; ++i) {
            types[i] = ctx->get_arg_type(i)->type;
            int buffers_idx = buffers.size();
            columns[i]->accept(&vistor);
            int buffers_sz = buffers.size() - buffers_idx;
            args[i] = helper.create_boxed_array(types[i], batch_size, columns[i]->is_nullable(), &buffers[buffers_idx],
                                                buffers_sz);
        }
        helper.batch_update_single(ctx, ctx->impl()->udaf_ctxs()->handle, ctx->impl()->udaf_ctxs()->update->method,
                                   this->data(state).handle, args, num_cols);
        for (int i = 0; i < num_cols; ++i) {
            env->DeleteLocalRef(args[i]);
        }
    }

    void merge_batch(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        for (size_t i = 0; i < batch_size; i++) {
            if (filter[i] == 0) {
                this->merge(ctx, column, states[i] + state_offset, i);
            }
        }
    }

    void merge_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column* column,
                                  AggDataPtr __restrict state) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            this->merge(ctx, column, state, i);
        }
    }

    void batch_serialize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                         size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            this->serialize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    void batch_finalize(FunctionContext* ctx, size_t batch_size, const Buffer<AggDataPtr>& agg_states,
                        size_t state_offset, Column* to) const override {
        for (size_t i = 0; i < batch_size; i++) {
            this->finalize_to_column(ctx, agg_states[i] + state_offset, to);
        }
    }

    std::string get_name() const override { return "java_udaf"; }
};
} // namespace starrocks::vectorized