// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

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

namespace starrocks::vectorized {

template <bool handle_null = true>
class JavaUDAFAggregateFunction : public AggregateFunction {
public:
    using State = JavaUDAFState;

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override final {}

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
        JVMFunctionHelper::getInstance().clear(udaf_ctx->buffer.get(), ctx);
        memcpy(udaf_ctx->buffer_data.data(), slice.get_data(), slice.get_size());
        udaf_ctx->_func->merge(this->data(state).handle, udaf_ctx->buffer->handle());
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
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
        JVMFunctionHelper::getInstance().clear(udaf_ctx->buffer.get(), ctx);

        udaf_ctx->_func->serialize(this->data(state).handle, udaf_ctx->buffer->handle());
        size_t new_size = old_size + serialize_size;
        column->get_bytes().resize(new_size);
        column->get_offset().emplace_back(new_size);
        memcpy(column->get_bytes().data() + old_size, udaf_ctx->buffer_data.data(), serialize_size);
    }

    void finalize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                            Column* to) const override final {
        auto* udaf_ctx = ctx->impl()->udaf_ctxs();
        jvalue val = udaf_ctx->_func->finalize(this->data(state).handle);
        append_jvalue(udaf_ctx->finalize->method_desc[0], to, val);
        release_jvalue(udaf_ctx->finalize->method_desc[0].is_box, val);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override final {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        auto* udf_ctxs = ctx->impl()->udaf_ctxs();
        // 1 convert input as state
        // 1.1 create state list
        auto rets = helper.batch_call(ctx, udf_ctxs->handle.handle(), udf_ctxs->create->method.handle(), chunk_size);
        // 1.2 convert input as input array
        int num_cols = ctx->get_num_args();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        args.emplace_back(rets);
        std::vector<const Column*> raw_input_ptrs(src.size());
        for (int i = 0; i < src.size(); ++i) {
            raw_input_ptrs[i] = src[i].get();
        }
        JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, raw_input_ptrs.data(), num_cols, chunk_size,
                                                      &args);
        // 2 batch call update
        helper.batch_update_state(ctx, ctx->impl()->udaf_ctxs()->handle.handle(),
                                  ctx->impl()->udaf_ctxs()->update->method.handle(), args.data(), args.size());
        // 3 get serialize size
        jintArray serialize_szs = (jintArray)helper.int_batch_call(
                ctx, rets, ctx->impl()->udaf_ctxs()->serialize_size->method.handle(), chunk_size);
        int length = env->GetArrayLength(serialize_szs);
        std::vector<int> slice_sz(length);
        helper.getEnv()->GetIntArrayRegion(serialize_szs, 0, length, slice_sz.data());
        int totalLength = std::accumulate(slice_sz.begin(), slice_sz.end(), 0, [](auto l, auto r) { return l + r; });
        // 4 prepare serialize buffer
        udf_ctxs->buffer_data.resize(totalLength);
        udf_ctxs->buffer =
                std::make_unique<DirectByteBuffer>(udf_ctxs->buffer_data.data(), udf_ctxs->buffer_data.size());
        // chunk size
        auto buffer_array = helper.create_object_array(udf_ctxs->buffer->handle(), chunk_size);
        jobject state_and_buffer[2] = {rets, buffer_array};
        helper.batch_update_state(ctx, ctx->impl()->udaf_ctxs()->handle.handle(),
                                  ctx->impl()->udaf_ctxs()->serialize->method.handle(), state_and_buffer, 2);
        std::vector<Slice> slices(chunk_size);
        // 5 ready
        int offsets = 0;
        for (int i = 0; i < chunk_size; ++i) {
            slices[i] = Slice(udf_ctxs->buffer_data.data() + offsets, slice_sz[i]);
            offsets += slice_sz[i];
        }
        // append result to dst column
        CHECK((*dst)->append_strings(slices));
        // 6 clean up arrays
        env->DeleteLocalRef(buffer_array);
        env->DeleteLocalRef(serialize_szs);
        for (int i = 0; i < args.size(); ++i) {
            env->DeleteLocalRef(args[i]);
        }
        env->DeleteLocalRef(rets);
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
        helper.getEnv()->PushLocalFrame(num_cols * 3 + 1);
        {
            auto states_arr = JavaDataTypeConverter::convert_to_states(states, state_offset, batch_size);
            JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
            helper.batch_update(ctx, ctx->impl()->udaf_ctxs()->handle.handle(),
                                ctx->impl()->udaf_ctxs()->update->method.handle(), states_arr, args.data(),
                                args.size());
        }
        helper.getEnv()->PopLocalFrame(nullptr);
    }

    void update_batch_selectively(FunctionContext* ctx, size_t batch_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const std::vector<uint8_t>& filter) const override {
        auto [env, helper] = JVMFunctionHelper::getInstanceWithEnv();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        int num_cols = ctx->get_num_args();
        helper.getEnv()->PushLocalFrame(num_cols * 3 + 1);
        {
            auto states_arr = JavaDataTypeConverter::convert_to_states_with_filter(states, state_offset, filter.data(),
                                                                                   batch_size);
            JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);
            helper.batch_update_if_not_null(ctx, ctx->impl()->udaf_ctxs()->handle.handle(),
                                            ctx->impl()->udaf_ctxs()->update->method.handle(), states_arr, args.data(),
                                            args.size());
        }
        helper.getEnv()->PopLocalFrame(nullptr);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t batch_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& helper = JVMFunctionHelper::getInstance();
        auto* env = helper.getEnv();
        std::vector<jobject> args;
        std::vector<DirectByteBuffer> buffers;
        int num_cols = ctx->get_num_args();
        env->PushLocalFrame(num_cols * 3 + 1);
        {
            JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, columns, num_cols, batch_size, &args);

            auto* stub = ctx->impl()->udaf_ctxs()->update_batch_call_stub.get();
            auto state_handle = this->data(state).handle;
            helper.batch_update_single(stub, state_handle, args.data(), num_cols, batch_size);
        }
        env->PopLocalFrame(nullptr);
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